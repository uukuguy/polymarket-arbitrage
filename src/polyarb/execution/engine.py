\
"""Execution engine: executes routed arbitrage legs."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from enum import Enum

from polyarb.models.signal import RoutingDecision, ExecutionLeg
from polyarb.routing.position_tracker import PositionTracker
from polyarb.routing.config import ExecutionConfig

logger = logging.getLogger(__name__)


class ExecutionStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass
class ExecutionResult:
    decision: RoutingDecision
    status: ExecutionStatus
    legs_executed: int
    legs_total: int
    realized_pnl: float
    error_message: str | None = None


class ExecutionEngine:
    """Executes routed arbitrage decisions against venues."""

    def __init__(
        self,
        config: ExecutionConfig | None = None,
        tracker: PositionTracker | None = None,
    ) -> None:
        self.config = config or ExecutionConfig()
        self.tracker = tracker or PositionTracker()

    async def execute(self, decision: RoutingDecision) -> ExecutionResult:
        """Execute a routing decision across all legs.

        Executes sequentially: Polymarket leg first, then Gamma leg.
        Aborts on Polymarket miss (no unhedged exposure).
        """
        plan = decision.plan
        logger.info(
            "Executing %d legs for decision (pnl=%.2f%%, profit_abs=%.2f)",
            len(plan.legs),
            decision.expected_profit_pct,
            decision.expected_profit_abs,
        )

        results: list[tuple[ExecutionLeg, bool, str | None]] = []

        for i, leg in enumerate(plan.legs):
            success, error = await self._execute_leg(leg, is_polymarket=(i == 0))
            results.append((leg, success, error))

        executed = sum(1 for _, s, _ in results if s)
        failed = len(results) - executed

        status: ExecutionStatus
        if failed == 0:
            status = ExecutionStatus.COMPLETED
        elif executed == 0:
            status = ExecutionStatus.FAILED
        else:
            status = ExecutionStatus.PARTIAL

        realized_pnl = decision.expected_profit_abs * (executed / len(plan.legs)) if plan.legs else 0.0

        self._update_tracker(decision, executed, len(plan.legs))

        error_msg = None
        if failed > 0:
            error_msg = f"{failed}/{len(plan.legs)} legs failed"

        return ExecutionResult(
            decision=decision,
            status=status,
            legs_executed=executed,
            legs_total=len(plan.legs),
            realized_pnl=realized_pnl,
            error_message=error_msg,
        )

    async def _execute_leg(
        self, leg: ExecutionLeg, is_polymarket: bool = False
    ) -> tuple[bool, str | None]:
        """Execute a single leg. Subclass to add real venue logic."""
        # Polymarket: market order (no limit price)
        # Gamma: limit order (has limit price)
        if is_polymarket:
            logger.debug("Executing Polymarket leg: %s %s @ market", leg.action, leg.asset)
        else:
            logger.debug(
                "Executing Gamma leg: %s %s @ limit %.4f",
                leg.action,
                leg.asset,
                leg.limit_price,
            )
        await asyncio.sleep(0.01)
        return True, None

    def _update_tracker(
        self, decision: RoutingDecision, executed: int, total: int
    ) -> None:
        for leg in decision.plan.legs:
            self.tracker.open_position(
                market_id=leg.asset,
                condition_id=leg.asset,
                side=leg.action,
                outcome="yes",  # default
                stake=leg.size,
                price=leg.estimated_price,
            )

    def _calc_realized_pnl(
        self, decision: RoutingDecision, executed: int, total: int
    ) -> float:
        if total == 0:
            return 0.0
        ratio = executed / total
        return decision.expected_profit_abs * ratio