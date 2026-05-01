\
"""Routing engine: selects best arbitrage paths across venues."""
from __future__ import annotations

import logging

from polyarb.models.signal import (
    ArbitrageSignal,
    MarketSignal,
    ExecutionLeg,
    ExecutionPlan,
    RoutingDecision,
    LegSide,
)
from polyarb.routing.config import RoutingConfig

logger = logging.getLogger(__name__)


class RoutingEngine:
    """Selects the best execution path for an arbitrage signal."""

    def __init__(self, config: RoutingConfig | None = None) -> None:
        self.config = config or RoutingConfig()
        self._call_count: int = 0

    def route(self, signal: ArbitrageSignal) -> RoutingDecision | None:
        """Route an arbitrage signal to venue-executable legs.

        Returns None if the signal fails config gates or no valid path
        exists.

        Returns a RoutingDecision with ExecutionPlan using canonical
        ExecutionLeg instances from models.signal.
        """
        self._call_count += 1

        if not self._passes_gate(signal):
            return None

        execution_legs = self._build_execution_legs(signal)
        if not execution_legs:
            # Even with no markets, if signal passes gate, return a decision
            # with empty plan (useful for logging/metrics)
            plan = ExecutionPlan(
                signal_id=signal.signal_id,
                legs=[],
                total_estimated_cost=0.0,
                profit_threshold_pct=self.config.min_profit_threshold_pct,
            )
            return RoutingDecision(
                signal_id=signal.signal_id,
                plan=plan,
                is_profitable=signal.max_arbitrage_pct >= self.config.min_profit_threshold_pct,
                expected_profit_pct=signal.max_arbitrage_pct,
                expected_profit_abs=0.0,
                reason="Signal passes gate but has no routeable markets",
            )

        expected_profit_pct = signal.max_arbitrage_pct
        expected_profit_abs = expected_profit_pct / 100.0 * signal.max_stake_per_leg * len(execution_legs)

        plan = ExecutionPlan(
            signal_id=signal.signal_id,
            legs=execution_legs,
            total_estimated_cost=sum(leg.estimated_cost for leg in execution_legs),
            profit_threshold_pct=self.config.min_profit_threshold_pct,
        )

        return RoutingDecision(
            signal_id=signal.signal_id,
            plan=plan,
            is_profitable=expected_profit_pct >= self.config.min_profit_threshold_pct,
            expected_profit_pct=expected_profit_pct,
            expected_profit_abs=expected_profit_abs,
            reason="Polymarket-first routing: executing legs in venue order",
        )

    def _passes_gate(self, signal: ArbitrageSignal) -> bool:
        if signal.max_arbitrage_pct < self.config.min_profit_threshold_pct:
            logger.debug(
                "Signal %s profit %.2f%% below threshold %.2f%%",
                signal.signal_id,
                signal.max_arbitrage_pct,
                self.config.min_profit_threshold_pct,
            )
            return False
        return True

    def _build_execution_legs(self, signal: ArbitrageSignal) -> list[ExecutionLeg]:
        """Build ExecutionLeg instances for each market in the signal.

        Polymarket-first: Polymarket legs come first (market orders),
        Gamma legs follow (limit orders for hedging).
        """
        legs: list[ExecutionLeg] = []

        for market in signal.markets:
            if not isinstance(market, MarketSignal):
                continue

            # Determine side based on price deviation from 0.5
            if market.price < 0.5:
                action = LegSide.BUY.value
                estimated_price = market.price
            else:
                action = LegSide.SELL.value
                estimated_price = market.price

            # Polymarket is primary venue - market order (no limit price)
            # Gamma is hedge venue - limit order (set limit price)
            is_polymarket = market.venue.lower() in ("polymarket", "pm", "")

            leg = ExecutionLeg(
                leg_id=f"{signal.signal_id}-{market.condition_id}",
                exchange=market.venue if market.venue else "polymarket",
                action=action,
                asset=market.condition_id,
                size=signal.max_stake_per_leg,
                limit_price=None if is_polymarket else market.price * 1.001,  # 0.1% above for limit
                estimated_price=estimated_price,
                estimated_cost=estimated_price * signal.max_stake_per_leg,
                hedge_ratio=1.0,
            )
            legs.append(leg)

        return legs

    @property
    def call_count(self) -> int:
        return self._call_count