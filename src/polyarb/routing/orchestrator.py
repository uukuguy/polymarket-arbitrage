\
"""Routing orchestrator: wires together signal -> routing -> execution."""
from __future__ import annotations

import logging
from dataclasses import dataclass

from polyarb.execution.engine import ExecutionEngine, ExecutionResult
from polyarb.models.signal import ArbitrageSignal, RoutingDecision
from polyarb.routing.config import AppConfig
from polyarb.routing.engine import RoutingEngine
from polyarb.routing.position_tracker import PositionTracker

logger = logging.getLogger(__name__)


@dataclass
class OrchestrationResult:
    """Result of the full routing -> execution pipeline."""

    decision: RoutingDecision
    execution: ExecutionResult | None
    profit_realized: float
    status: str


class RoutingOrchestrator:
    """Wires signal intake -> routing decision -> execution."""

    def __init__(self, config: AppConfig | None = None) -> None:
        self.config = config or AppConfig()
        self.routing_engine = RoutingEngine(self.config.routing)
        self.position_tracker = PositionTracker(self.config.position)
        self.execution_engine = ExecutionEngine(self.config.execution)

    async def process(self, signal: ArbitrageSignal) -> OrchestrationResult | None:
        """Run the full pipeline: route -> validate position -> execute."""
        decision = self.routing_engine.route(signal)
        if decision is None:
            return None

        # Calculate total stake from execution plan
        total_stake = sum(
            leg.cost_basis_money.to_float() for leg in decision.plan.legs
        )

        position_ok, reason = self.position_tracker.can_open_position(total_stake)
        if not position_ok:
            logger.info(
                "Position check failed for %s: %s",
                signal.signal_id,
                reason,
            )
            return None

        execution = await self.execution_engine.execute(decision)
        if execution.status.name in ("COMPLETED", "PARTIAL"):
            self.position_tracker.update(
                legs=execution.legs_executed,
                pnl=execution.realized_pnl,
            )

        return OrchestrationResult(
            decision=decision,
            execution=execution,
            profit_realized=(
                execution.realized_pnl
                if execution.status.name in ("COMPLETED", "PARTIAL")
                else 0.0
            ),
            status="executed" if execution.status.name in ("COMPLETED", "PARTIAL") else "failed",
        )
