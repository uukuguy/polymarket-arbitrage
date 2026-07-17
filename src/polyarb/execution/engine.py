"""Execution engine: orchestrates sequential execution of routed arbitrage legs.

T4 Revision 7 (2026-06-02 SESSION 36) — orchestration shell.
T5 Revision 9 (2026-06-06 SESSION 37) — close-path integration:

- **Pluggable leg executor**: caller injects `leg_executor` (async callable
  `(ExecutionLeg, attempt: int) -> tuple[bool, str | None]`) so tests can
  simulate any failure pattern. Production will inject a py-clob-client
  adapter. The default executor is a no-op simulator that always returns
  success — preserves backward compat for callers relying on stub behaviour.

- **Real abort-on-fail invariant**: when any leg's final attempt fails, no
  subsequent leg is fired (T4).

- **Per-leg retry policy**: ExecutionConfig.retry_attempts + retry_delay_seconds.

- **Structured result per leg**: ExecutionLegResult.

- **Position tracker updates only on successful legs** (T4 bug fix).

- **Close path** (T5): on successful leg, the engine either (a) calls the
  injected `fill_provider(leg)` to get a real Fill (production with real
  venue), (b) synthesizes a paper-mode Fill at `leg.estimated_price` if
  `paper_close=True` (paper mode lifecycle exercise), or (c) leaves the
  position open if neither is configured (legacy behaviour). The Fill is
  forwarded to `tracker.close_position_with_fill`.

- **Stop-loss surface** (T5): post-execution, the engine queries
  `tracker.check_stop_loss_event()` and surfaces any event on
  `ExecutionResult.stop_loss` so callers (CLI, monitor) can halt new
  signals without re-querying the tracker.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum

from polyarb.models.signal import ExecutionLeg, RoutingDecision
from polyarb.routing.config import ExecutionConfig
from polyarb.routing.position_tracker import (
    Fill,
    PositionTracker,
    StopLossEvent,
)

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PARTIAL = "partial"  # some legs succeeded, some failed without abort path
    ABORTED = "aborted"  # first-leg-fail path — subsequent legs skipped
    FAILED = "failed"    # all legs failed


@dataclass
class ExecutionLegResult:
    """Per-leg execution outcome including retry attempts."""

    leg: ExecutionLeg
    success: bool
    attempts: int  # 1 = first try succeeded; > 1 = retried
    error: str | None = None
    skipped: bool = False  # True when prior leg's abort skipped this one


@dataclass
class ExecutionResult:
    decision: RoutingDecision
    status: ExecutionStatus
    legs_executed: int  # legs that succeeded (NOT counting retries)
    legs_total: int
    realized_pnl: float
    leg_results: list[ExecutionLegResult] = field(default_factory=list)
    error_message: str | None = None
    # T5: stop-loss event from the tracker post-execution. Non-None means
    # the caller should halt new signals.
    stop_loss: StopLossEvent | None = None


# Type alias for the pluggable leg executor.
LegExecutor = Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]
# Type alias for the fill provider (T5): returns a Fill for a successful leg.
FillProvider = Callable[[ExecutionLeg], Awaitable[Fill]]


async def _default_leg_executor(
    leg: ExecutionLeg, attempt: int
) -> tuple[bool, str | None]:
    """Stub executor — succeeds after a small simulated latency.

    Replace via `ExecutionEngine(leg_executor=...)` for tests or real venue
    integration. The pre-T4 engine used this same no-op contract.
    """
    await asyncio.sleep(0.01)
    return True, None


class ExecutionEngine:
    """Executes routed arbitrage decisions across venues sequentially.

    Atomic invariant: first-leg fail → abort all subsequent legs (no
    unhedged exposure). Subsequent-leg failures yield PARTIAL status with
    explicit per-leg detail in `result.leg_results`.
    """

    def __init__(
        self,
        config: ExecutionConfig | None = None,
        tracker: PositionTracker | None = None,
        leg_executor: LegExecutor | None = None,
        fill_provider: FillProvider | None = None,
        paper_close: bool = False,
    ) -> None:
        self.config = config or ExecutionConfig()
        self.tracker = tracker or PositionTracker()
        self.leg_executor: LegExecutor = leg_executor or _default_leg_executor
        # T5: close-path configuration.
        # - `fill_provider`: production hook — given a successful leg, returns
        #   a Fill (real venue fill). Engine forwards it to
        #   tracker.close_position_with_fill.
        # - `paper_close`: when True (and no fill_provider), synthesize a Fill
        #   at the leg's estimated_price → zero-PnL lifecycle exercise.
        # - Neither set: position stays open (legacy / explicit-close mode).
        self.fill_provider: FillProvider | None = fill_provider
        self.paper_close: bool = paper_close

    async def execute(self, decision: RoutingDecision) -> ExecutionResult:
        """Execute a routing decision across all legs sequentially.

        Sequence:
          1. Iterate plan.legs in order.
          2. For each, attempt up to retry_attempts times with backoff.
          3. On leg final success → update position tracker; continue.
          4. On leg final fail:
             - If this is the first leg → abort remaining (ABORTED status).
             - If a later leg → mark this leg failed; continue to next leg
               so we record the full failure pattern (PARTIAL status if any
               other legs succeeded, FAILED if none did).
        """
        plan = decision.plan
        logger.info(
            "Executing %d legs for signal=%s (expected_profit_pct=%.2f, abs=%.2f)",
            len(plan.legs),
            decision.signal_id,
            decision.expected_profit_pct,
            decision.expected_profit_abs,
        )

        leg_results: list[ExecutionLegResult] = []
        aborted = False
        first_leg_failed = False

        for i, leg in enumerate(plan.legs):
            if aborted:
                leg_results.append(
                    ExecutionLegResult(
                        leg=leg, success=False, attempts=0, skipped=True,
                        error="aborted — prior leg failed",
                    )
                )
                continue

            leg_result = await self._execute_leg_with_retry(leg, leg_index=i)
            leg_results.append(leg_result)

            if leg_result.success:
                self._update_tracker_for_leg(decision.signal_id, leg)
                await self._maybe_close_for_leg(decision.signal_id, leg)
            else:
                if i == 0:
                    # Atomic invariant: never proceed past a failed first leg.
                    aborted = True
                    first_leg_failed = True
                    logger.warning(
                        "First leg (id=%s) failed after %d attempts — "
                        "aborting remaining %d legs",
                        leg.leg_id, leg_result.attempts, len(plan.legs) - 1,
                    )

        executed = sum(1 for r in leg_results if r.success)
        total = len(plan.legs)
        failed = sum(1 for r in leg_results if not r.success and not r.skipped)

        # Status mapping.
        status: ExecutionStatus
        if aborted and first_leg_failed:
            status = ExecutionStatus.ABORTED
        elif failed == 0:
            status = ExecutionStatus.COMPLETED
        elif executed == 0:
            status = ExecutionStatus.FAILED
        else:
            status = ExecutionStatus.PARTIAL

        realized_pnl = self._calc_realized_pnl(decision, executed, total)
        error_msg = self._summarize_errors(leg_results) if failed > 0 or aborted else None
        # T5: surface stop-loss state so callers can halt new signals.
        stop_loss_event = self.tracker.check_stop_loss_event()

        return ExecutionResult(
            decision=decision,
            status=status,
            legs_executed=executed,
            legs_total=total,
            realized_pnl=realized_pnl,
            leg_results=leg_results,
            error_message=error_msg,
            stop_loss=stop_loss_event,
        )

    async def _execute_leg_with_retry(
        self, leg: ExecutionLeg, leg_index: int
    ) -> ExecutionLegResult:
        """Attempt a single leg up to `retry_attempts` times.

        Returns ExecutionLegResult with `attempts` set to the actual attempt
        count (1 if first try succeeded). Backs off `retry_delay_seconds`
        between attempts.
        """
        max_attempts = max(1, int(self.config.retry_attempts))
        delay = max(0.0, float(self.config.retry_delay_seconds))
        last_error: str | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                success, error = await self.leg_executor(leg, attempt)
            except Exception as exc:  # noqa: BLE001 — fail-soft envelope
                success, error = False, f"executor raised: {exc!r}"

            if success:
                if attempt > 1:
                    logger.info(
                        "Leg %s (index=%d) succeeded on attempt %d/%d",
                        leg.leg_id, leg_index, attempt, max_attempts,
                    )
                return ExecutionLegResult(
                    leg=leg, success=True, attempts=attempt, error=None,
                )

            last_error = error
            logger.warning(
                "Leg %s (index=%d) attempt %d/%d failed: %s",
                leg.leg_id, leg_index, attempt, max_attempts, error,
            )
            if attempt < max_attempts and delay > 0:
                await asyncio.sleep(delay)

        return ExecutionLegResult(
            leg=leg, success=False, attempts=max_attempts, error=last_error,
        )

    def _update_tracker_for_leg(self, signal_id: str, leg: ExecutionLeg) -> None:
        """Open a position in the tracker for a successfully-executed leg only.

        Failed legs MUST NOT update the tracker — they'd corrupt downstream
        PnL accounting.
        """
        # ExecutionLeg.action is lowercase ("buy"/"sell"); tracker contract
        # is uppercase ("BUY"/"SELL"). Normalize here to keep the tracker's
        # PnL math sign-consistent.
        self.tracker.open_position(
            market_id=leg.asset,
            condition_id=leg.asset,
            side=leg.action.upper(),
            outcome="yes",  # T5+: derive from leg metadata when available
            stake=leg.size,
            price=leg.estimated_price,
            leg_id=leg.leg_id,
            operation_id=f"open:{signal_id}:{leg.leg_id}",
        )

    async def _maybe_close_for_leg(
        self, signal_id: str, leg: ExecutionLeg
    ) -> None:
        """T5 close-path hook — only fires for successfully-executed legs.

        Priority:
          1. `fill_provider` set → call it, forward returned Fill to tracker.
          2. `paper_close=True` → synthesize Fill at leg.estimated_price.
          3. Neither → leave position open (legacy / explicit-close mode).
        """
        if self.fill_provider is not None:
            try:
                fill = await self.fill_provider(leg)
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "fill_provider raised for leg %s: %r — leaving position open",
                    leg.leg_id, exc,
                )
                return
            try:
                if fill.fill_id:
                    operation_id = (
                        f"close:{signal_id}:{leg.leg_id}:fill:{fill.fill_id}"
                    )
                else:
                    logger.warning(
                        "venue fill for leg %s has no fill_id; "
                        "durable retry guarantees unavailable",
                        leg.leg_id,
                    )
                    operation_id = (
                        f"close:{signal_id}:{leg.leg_id}:"
                        f"{fill.filled_at.isoformat()}"
                    )
                self.tracker.close_position_with_fill(
                    fill,
                    operation_id=operation_id,
                )
            except ValueError as exc:
                # Partial fill or other tracker rejection — log and skip close.
                logger.warning(
                    "tracker rejected fill for leg %s: %s", leg.leg_id, exc,
                )
            return

        if self.paper_close:
            fill = Fill(
                market_id=leg.asset,
                exit_price=leg.estimated_price,
                filled_size=leg.size,
            )
            self.tracker.close_position_with_fill(
                fill,
                operation_id=f"close:{signal_id}:{leg.leg_id}:paper-close",
            )

    def _calc_realized_pnl(
        self, decision: RoutingDecision, executed: int, total: int
    ) -> float:
        """Pro-rata PnL based on legs successfully executed.

        Note: this is the SIGNAL-time expected PnL scaled by execution ratio,
        not the actual filled PnL. Real filled PnL comes from the position
        tracker post-fill — see T5. Used here for in-flight monitoring only.
        """
        if total == 0:
            return 0.0
        ratio = executed / total
        return decision.expected_profit_abs * ratio

    def _summarize_errors(self, leg_results: list[ExecutionLegResult]) -> str:
        parts: list[str] = []
        for i, r in enumerate(leg_results):
            if r.skipped:
                parts.append(f"leg[{i}] skipped (abort)")
            elif not r.success:
                parts.append(f"leg[{i}] failed after {r.attempts}: {r.error}")
        return "; ".join(parts) if parts else ""
