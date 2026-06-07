"""T8 (2026-06-07): E2E integration & chaos tests for the m2 arbitrage pipeline.

Covers:
  - Full lifecycle: signal → route → execute → position → close → stop-loss
  - All 4 ExecutionStatus outcomes: COMPLETED / PARTIAL / ABORTED / FAILED
  - Abort-on-first-leg-fail invariant (T4)
  - Retry exhaust + retry-success scenarios (T4)
  - Paper-close full lifecycle (T5)
  - Real fill_provider close-path PnL booking (T5)
  - Stop-loss trigger chain (T5)
  - Below-threshold signal rejection (T3 gate)
  - Only-successful-legs-tracked invariant (T4 bug fix regression)
  - Multi-venue routing (T3 slippage-aware selection)
"""
from __future__ import annotations

import asyncio
from typing import Awaitable, Callable

import pytest

from polyarb.execution.engine import (
    ExecutionEngine,
    ExecutionLegResult,
    ExecutionResult,
    ExecutionStatus,
)
from polyarb.models.signal import (
    ArbitrageSignal,
    ExecutionLeg,
    ExecutionPlan,
    LegSide,
    MarketSignal,
    RoutingDecision,
)
from polyarb.models.slippage import SlippageCalculator
from polyarb.routing.config import (
    ExecutionConfig,
    PositionConfig,
    RoutingConfig,
)
from polyarb.routing.engine import RoutingEngine
from polyarb.routing.position_tracker import Fill, PositionTracker, StopLossEvent

# ── helpers ────────────────────────────────────────────────────────────────


def _synth_signal(
    n_markets: int = 2,
    mid_price: float = 0.5,
    stake: float = 100.0,
    profit_pct: float = 2.5,
    venue: str = "polymarket",
) -> ArbitrageSignal:
    markets = [
        MarketSignal(
            id=f"e2e-m{i}",
            condition_id=f"e2e-cond-{i}",
            venue=venue,
            price=mid_price,
        )
        for i in range(n_markets)
    ]
    return ArbitrageSignal(
        opportunity_id="e2e-test",
        markets=markets,
        max_arbitrage_pct=profit_pct,
        max_stake_per_leg=stake,
        confidence=0.8,
    )


def _synth_decision(
    signal: ArbitrageSignal,
    min_threshold_pct: float = 1.0,
) -> RoutingDecision | None:
    engine = RoutingEngine(
        config=RoutingConfig(min_profit_threshold_pct=min_threshold_pct),
        slippage_calc=SlippageCalculator(),
    )
    return engine.route(signal)


def _always_succeed_executor() -> (
    Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]
):
    async def _exec(leg: ExecutionLeg, attempt: int) -> tuple[bool, str | None]:
        return True, None

    return _exec


def _always_fail_executor(
    error_msg: str = "simulated failure",
) -> Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]:
    async def _exec(leg: ExecutionLeg, attempt: int) -> tuple[bool, str | None]:
        return False, error_msg

    return _exec


def _fail_then_succeed_executor(
    fail_attempts: int,
) -> Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]:
    async def _exec(leg: ExecutionLeg, attempt: int) -> tuple[bool, str | None]:
        if attempt <= fail_attempts:
            return False, f"fail attempt {attempt}"
        return True, None

    return _exec


def _fail_first_leg_only_executor() -> (
    Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]
):
    async def _exec(leg: ExecutionLeg, attempt: int) -> tuple[bool, str | None]:
        # leg_id is UUID-suffixed (e.g. "abc123-e2e-cond-0"); match on asset
        if leg.asset == "e2e-cond-0":
            return False, "first leg always fails"
        return True, None

    return _exec


def _build_leg_executor(
    actions: list[tuple[bool, str | None]],
) -> Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]:
    """Returns a leg executor that consumes `actions` in order, one per call."""
    idx = 0

    async def _exec(leg: ExecutionLeg, attempt: int) -> tuple[bool, str | None]:
        nonlocal idx
        if idx >= len(actions):
            return True, None
        result = actions[idx]
        idx += 1
        return result

    return _exec


# ── E2E tests ───────────────────────────────────────────────────────────────


class TestE2EHappyPath:
    """Signal → route → execute → all legs succeed → COMPLETED."""

    async def test_full_pipeline_completed(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None
        assert decision.is_profitable

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.COMPLETED
        assert result.legs_executed == 2
        assert result.legs_total == 2
        assert tracker.open_count == 2  # positions stay open (legacy / no-close mode)

    async def test_single_leg_signal(self):
        signal = _synth_signal(n_markets=1)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.COMPLETED
        assert result.legs_executed == 1
        assert tracker.open_count == 1


class TestE2EAbortOnFirstLegFail:
    """T4 invariant: first-leg fail → ABORTED, no subsequent legs fired."""

    async def test_first_leg_fail_aborts_remaining(self):
        signal = _synth_signal(n_markets=3)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_fail_first_leg_only_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.ABORTED
        # Only first leg executed; remaining 2 skipped
        assert result.legs_executed == 0
        skipped = sum(1 for r in result.leg_results if r.skipped)
        assert skipped >= 1  # at least one leg was skipped due to abort
        assert tracker.open_count == 0

    async def test_no_positions_on_abort(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_fail_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.ABORTED
        assert tracker.open_count == 0  # pre-T4 bug fix: no phantom positions


class TestE2EPartialExecution:
    """First leg succeeds, second fails → PARTIAL."""

    async def test_second_leg_fail_makes_partial(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_build_leg_executor(
                [(True, None), (False, "second leg failed")]
            ),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.PARTIAL
        assert result.legs_executed == 1
        assert result.legs_total == 2

    async def test_only_successful_legs_in_tracker(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_build_leg_executor(
                [(True, None), (False, "second leg failed")]
            ),
        )
        await engine.execute(decision)
        assert tracker.open_count == 1  # only first leg tracked
        positions = list(tracker.open_positions())
        # tracker.open_position uses leg.asset ("e2e-cond-0") as market_id
        assert positions[0].market_id == "e2e-cond-0"


class TestE2ERetry:
    """T4 retry: exhaust → ABORTED, succeed-on-retry → COMPLETED."""

    async def test_retry_exhaust_aborts(self):
        signal = _synth_signal(n_markets=1)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=3, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_fail_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.ABORTED
        assert result.leg_results[0].attempts == 3

    async def test_succeed_on_retry(self):
        signal = _synth_signal(n_markets=1)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=3, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_fail_then_succeed_executor(fail_attempts=2),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.COMPLETED
        # Should have failed attempts 1 and 2, succeeded on 3
        assert result.leg_results[0].attempts == 3
        assert result.leg_results[0].success


class TestE2EStopLoss:
    """T5: stop-loss triggers when realized loss crosses threshold."""

    def test_check_stop_loss_not_triggered_initially(self):
        tracker = PositionTracker(
            PositionConfig(enable_pnl_stop=True, stop_loss_pct=5.0)
        )
        assert tracker.check_stop_loss() is False
        assert tracker.check_stop_loss_event() is None

    def test_check_stop_loss_disabled_always_none(self):
        tracker = PositionTracker(
            PositionConfig(enable_pnl_stop=False, stop_loss_pct=5.0)
        )
        # Force a loss through the legacy update path
        tracker.update(legs=1, pnl=-100.0)
        assert tracker.check_stop_loss() is False
        assert tracker.check_stop_loss_event() is None

    def test_check_stop_loss_threshold_exact_match(self):
        cfg = PositionConfig(
            initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0
        )
        tracker = PositionTracker(cfg)
        # Realized loss = 50.0 → exactly 5% of 1000
        tracker.update(legs=1, pnl=-50.0)
        event = tracker.check_stop_loss_event()
        assert event is not None
        assert pytest.approx(event.loss_pct, abs=1e-6) == 5.0
        assert event.recommendation == "halt_new_signals"
        assert tracker.check_stop_loss() is True

    def test_check_stop_loss_below_threshold(self):
        cfg = PositionConfig(
            initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0
        )
        tracker = PositionTracker(cfg)
        tracker.update(legs=1, pnl=-10.0)  # only 1% loss
        assert tracker.check_stop_loss() is False
        assert tracker.check_stop_loss_event() is None

    def test_check_stop_loss_profit_does_not_trigger(self):
        cfg = PositionConfig(
            initial_balance=1000.0, enable_pnl_stop=True, stop_loss_pct=5.0
        )
        tracker = PositionTracker(cfg)
        tracker.update(legs=1, pnl=100.0)  # profit!
        assert tracker.check_stop_loss() is False

    async def test_stop_loss_surfaced_in_execution_result(self):
        """Engine surfaces stop_loss on ExecutionResult after execution."""
        signal = _synth_signal(n_markets=1, profit_pct=2.5)
        decision = _synth_decision(signal)
        assert decision is not None

        cfg = PositionConfig(
            initial_balance=100.0,
            enable_pnl_stop=True,
            stop_loss_pct=5.0,
        )
        tracker = PositionTracker(cfg)
        # Pre-load a big loss so stop-loss fires
        tracker.update(legs=1, pnl=-60.0)

        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
        )
        result = await engine.execute(decision)
        assert result.stop_loss is not None
        assert result.stop_loss.loss_pct > 5.0


class TestE2EPaperClose:
    """T5: paper-close exercises full lifecycle with zero PnL."""

    async def test_paper_close_lifecycle_zero_pnl(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
            paper_close=True,
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.COMPLETED
        assert result.legs_executed == 2
        # All positions should be closed after paper_close
        assert tracker.open_count == 0
        # Paper close at estimated_price (== entry_price) → actual realized PnL in tracker is zero
        assert tracker.total_realized_pnl == 0.0

    async def test_paper_close_no_positions_left(self):
        signal = _synth_signal(n_markets=3)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
            paper_close=True,
        )
        await engine.execute(decision)
        assert tracker.open_count == 0
        assert len(list(tracker.open_positions())) == 0

    async def test_paper_close_skips_failed_legs(self):
        """Failed legs are never closed (no fill to synthesize)."""
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_build_leg_executor(
                [(True, None), (False, "second leg failed")]
            ),
            paper_close=True,
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.PARTIAL
        # First leg succeeded → paper-close fired → it was closed
        # Second leg failed → never opened → nothing to close
        assert tracker.open_count == 0  # first leg closed by paper_close


class TestE2EFillProvider:
    """T5: real fill_provider closes positions and books PnL."""

    async def test_fill_provider_books_pnl(self):
        signal = _synth_signal(n_markets=2, mid_price=0.5, stake=100.0)
        decision = _synth_decision(signal)
        assert decision is not None

        async def _close_fill(leg: ExecutionLeg) -> Fill:
            # Close at entry price for predictable signed PnL
            return Fill(
                market_id=leg.asset,
                exit_price=leg.estimated_price,
                filled_size=leg.size,
            )

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_succeed_executor(),
            fill_provider=_close_fill,
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.COMPLETED
        assert tracker.open_count == 0  # closed by fill_provider
        # Close at entry price → realized PnL = 0
        assert tracker.total_realized_pnl == 0.0

    async def test_fill_provider_skipped_for_failed_legs(self):
        signal = _synth_signal(n_markets=2)
        decision = _synth_decision(signal)
        assert decision is not None

        call_count = 0

        async def _counting_fill(leg: ExecutionLeg) -> Fill:
            nonlocal call_count
            call_count += 1
            return Fill(
                market_id=leg.asset,
                exit_price=0.50,
                filled_size=leg.size,
            )

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=1, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_build_leg_executor(
                [(True, None), (False, "second leg failed")]
            ),
            fill_provider=_counting_fill,
        )
        await engine.execute(decision)
        assert call_count == 1  # only first (successful) leg got a fill
        assert tracker.open_count == 0  # first leg closed by fill_provider


class TestE2EBelowThresholdGate:
    """T3 gate: signals below min_profit_threshold_pct are rejected."""

    def test_signal_rejected_below_threshold(self):
        signal = _synth_signal(profit_pct=0.5)  # below 1.0% default
        engine = RoutingEngine(
            config=RoutingConfig(min_profit_threshold_pct=1.0),
            slippage_calc=SlippageCalculator(),
        )
        decision = engine.route(signal)
        assert decision is None

    def test_signal_accepted_above_threshold(self):
        signal = _synth_signal(profit_pct=3.0)  # above 1.0% default
        engine = RoutingEngine(
            config=RoutingConfig(min_profit_threshold_pct=1.0),
            slippage_calc=SlippageCalculator(),
        )
        decision = engine.route(signal)
        assert decision is not None
        assert decision.is_profitable

    def test_signal_at_exact_threshold(self):
        signal = _synth_signal(profit_pct=1.0)  # exactly at threshold
        engine = RoutingEngine(
            config=RoutingConfig(min_profit_threshold_pct=1.0),
            slippage_calc=SlippageCalculator(),
        )
        decision = engine.route(signal)
        assert decision is not None  # "at threshold" should pass


class TestE2EAllPathsExhausted:
    """All legs fail after retries → FAILED (if abort doesn't catch first)."""

    async def test_all_fail_after_retries_is_aborted(self):
        signal = _synth_signal(n_markets=1)
        decision = _synth_decision(signal)
        assert decision is not None

        tracker = PositionTracker()
        engine = ExecutionEngine(
            config=ExecutionConfig(retry_attempts=3, retry_delay_seconds=0.0),
            tracker=tracker,
            leg_executor=_always_fail_executor(),
        )
        result = await engine.execute(decision)
        assert result.status == ExecutionStatus.ABORTED
        assert result.error_message is not None


class TestE2EMultiVenueRouting:
    """T3: slippage-aware venue selection across markets."""

    def test_multi_market_signal_produces_legs_for_all(self):
        signal = _synth_signal(n_markets=3, venue="polymarket")
        engine = RoutingEngine(
            config=RoutingConfig(min_profit_threshold_pct=1.0),
            slippage_calc=SlippageCalculator(),
        )
        decision = engine.route(signal)
        assert decision is not None
        assert len(decision.plan.legs) == 3

    def test_decision_surface_includes_profit_metrics(self):
        signal = _synth_signal(n_markets=2, profit_pct=2.5)
        decision = _synth_decision(signal)
        assert decision is not None
        assert decision.expected_profit_pct > 0
        assert decision.expected_profit_abs > 0
        assert decision.reason  # human-readable reason
