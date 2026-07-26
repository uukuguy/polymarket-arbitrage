"""Routing engine: selects best arbitrage paths across venues.

T3 Revision 6 (2026-06-02 SESSION 36): venue selection is now slippage-aware.
Each leg consults `SlippageCalculator.estimate_cross_execution_savings` and
picks the cheaper of PM vs CLOB based on net_cost_after_rebate_bps. The
prior "polymarket-first hardcoded" policy is replaced — see venue-selection
tests in tests/routing/test_engine.py for the locked behavior matrix.
"""

from __future__ import annotations

import logging

from polyarb.models.signal import (
    ArbitrageSignal,
    ExecutionLeg,
    ExecutionPlan,
    LegSide,
    MarketSignal,
    RoutingDecision,
)
from polyarb.models.slippage import SlippageCalculator, SlippageResult
from polyarb.routing.config import RoutingConfig

logger = logging.getLogger(__name__)


class RoutingEngine:
    """Selects the best execution path for an arbitrage signal."""

    def __init__(
        self,
        config: RoutingConfig | None = None,
        slippage_calc: SlippageCalculator | None = None,
    ) -> None:
        self.config = config or RoutingConfig()
        # T3 Revision 6: slippage-aware venue selection. Default-construct
        # with module defaults if caller didn't inject — backward-compatible
        # with the 6 pre-T3 tests that don't pass slippage_calc.
        self.slippage_calc = slippage_calc or SlippageCalculator()
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
        expected_profit_abs = (
            expected_profit_pct / 100.0 * signal.max_stake_per_leg * len(execution_legs)
        )

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
        """Build ExecutionLeg instances for each market — slippage-aware venue choice.

        T3 Revision 6: for each market, consult the slippage calculator to
        decide PM vs CLOB. Pick the side with lower net_cost_after_rebate_bps.
        Ties → prefer PM (single-venue baseline, less moving parts at execution).

        Legacy behaviour ("polymarket-first hardcoded") is preserved as a
        fallback when the caller pre-specified a non-default venue on the
        MarketSignal (`market.venue` is set to something other than the
        Polymarket alias set). This lets upstream callers override venue
        selection when they have richer context than the slippage model.
        """
        legs: list[ExecutionLeg] = []
        quantity = signal.max_stake_per_leg

        for market in signal.markets:
            if not isinstance(market, MarketSignal):
                continue

            # Side from price deviation (unchanged from pre-T3 behaviour).
            if market.price < 0.5:
                action = LegSide.BUY.value
            else:
                action = LegSide.SELL.value

            # Compute size_usd for slippage call. MarketSignal carries `price`
            # as a probability/USD-per-share; total notional = price × stake.
            size_usd = market.price * quantity

            chosen_venue, slippage_result = self._select_venue(
                action_side=action,
                market=market,
                size_usd=size_usd,
            )

            # `estimated_price` stays at the signal price (the model's
            # mid_price_delta_bps is a cost adjustment, not a fill-price
            # adjustment — fills land at signal price by definition; the
            # bps are a separately tracked cost). `estimated_cost` now
            # reflects net_cost_dollars from the slippage model instead of
            # naive `price × size`.
            estimated_price = market.price
            estimated_cost = abs(slippage_result.net_cost_dollars())

            # Polymarket legs ride market orders; CLOB legs ride limits.
            # Limit price stays at the previous "0.1% above signal price"
            # heuristic for CLOB BUY legs and "0.1% below" for SELL.
            limit_price: float | None
            if chosen_venue == "polymarket":
                limit_price = None
            else:
                if action == LegSide.BUY.value:
                    limit_price = market.price * 1.001
                else:
                    limit_price = market.price * 0.999

            leg = ExecutionLeg(
                leg_id=f"{signal.signal_id}-{market.condition_id}",
                exchange=chosen_venue,
                action=action,
                asset=market.condition_id,
                quantity=quantity,
                limit_price=limit_price,
                estimated_price=estimated_price,
                estimated_cost=estimated_cost,
                hedge_ratio=1.0,
            )
            legs.append(leg)

        return legs

    def _select_venue(
        self,
        action_side: str,
        market: MarketSignal,
        size_usd: float,
    ) -> tuple[str, SlippageResult]:
        """Choose PM vs CLOB for one leg via slippage estimator.

        Returns (chosen_venue, slippage_result_for_chosen_venue).

        Upstream caller can override by setting `market.venue` to a non-default
        value (anything outside the PM alias set). In that case the slippage
        call still runs (for cost accounting) but the venue is forced.
        """
        pm_aliases = ("polymarket", "pm", "")
        caller_override = market.venue.lower() not in pm_aliases and bool(market.venue)

        cross = self.slippage_calc.estimate_cross_execution_savings(
            size_usd=size_usd,
            side=action_side.upper(),
            mid_price=market.price,
            daily_volume_usd=10_000.0,  # default — T4 will pass real volume
        )

        pm_cost = cross["pm_net_cost_bps"]
        clob_cost = cross["clob_net_cost_bps"]

        if caller_override:
            chosen = market.venue
            result_dict = cross["clob_result"] if "clob" in chosen.lower() else cross["pm_result"]
        elif pm_cost <= clob_cost:
            chosen = "polymarket"
            result_dict = cross["pm_result"]
        else:
            chosen = "clob"
            result_dict = cross["clob_result"]

        # Rebuild a SlippageResult from the dict — the helper returns dicts
        # via .to_dict() so we lose the dataclass; cheap to reconstruct since
        # estimated_cost is the only consumer downstream.
        result = SlippageResult(
            side=result_dict["side"],
            venue=result_dict["venue"],
            market_impact_bps=result_dict["market_impact_bps"],
            fee_bps=result_dict["fee_bps"],
            mid_price_delta_bps=result_dict["mid_price_delta_bps"],
            total_cost_bps=result_dict["total_cost_bps"],
            net_cost_after_rebate_bps=result_dict["net_cost_after_rebate_bps"],
            size_usd=result_dict["size_usd"],
        )

        logger.debug(
            "venue selection: side=%s pm=%.2fbps clob=%.2fbps → %s",
            action_side,
            pm_cost,
            clob_cost,
            chosen,
        )
        return chosen, result

    @property
    def call_count(self) -> int:
        return self._call_count
