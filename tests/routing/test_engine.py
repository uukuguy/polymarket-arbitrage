"""Tests for routing engine."""

import pytest

from polyarb.models.signal import ArbitrageSignal, MarketOutcome, MarketSignal, Outcome
from polyarb.models.slippage import SlippageCalculator, SlippageParams
from polyarb.routing.config import RoutingConfig
from polyarb.routing.engine import RoutingEngine


def make_signal(
    profit_pct=5.0,
    max_stake=50.0,
    markets=None,
    legs=None,
    confidence=0.8,
):
    return ArbitrageSignal(
        opportunity_id="opp-1",
        markets=markets or [],
        legs=legs or [],
        max_arbitrage_pct=profit_pct,
        max_stake_per_leg=max_stake,
        confidence=confidence,
    )


class TestRoutingEngine:
    def test_empty_signal_returns_none(self):
        engine = RoutingEngine()
        # Signal with no markets should return None (fails gate)
        sig = ArbitrageSignal(opportunity_id="empty")
        result = engine.route(sig)
        assert result is None

    def test_below_threshold_returns_none(self):
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=2.0))
        sig = make_signal(profit_pct=1.0)
        result = engine.route(sig)
        assert result is None

    def test_above_threshold_returns_decision(self):
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=1.0))
        sig = make_signal(profit_pct=5.0)
        result = engine.route(sig)
        assert result is not None
        assert result.expected_profit_pct == pytest.approx(5.0)
        assert result.is_profitable is True

    def test_call_count_incremented(self):
        engine = RoutingEngine()
        engine.route(make_signal())
        engine.route(make_signal())
        assert engine.call_count == 2

    def test_routing_decision_properties(self):
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        sig = make_signal(profit_pct=3.0, max_stake=100.0)
        result = engine.route(sig)
        assert result is not None
        assert result.signal_id == sig.signal_id
        assert result.plan is not None
        assert result.expected_profit_pct == pytest.approx(3.0)

    def test_with_market_signal(self):
        """Test routing with actual market signals."""
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        market = MarketSignal(
            id="market_1",
            condition_id="cond_1",
            venue="polymarket",
            price=0.45,
            outcomes=[
                MarketOutcome(outcome=Outcome.YES, price=0.45, size=500.0),
            ],
        )
        sig = make_signal(profit_pct=3.0, max_stake=100.0, markets=[market])
        result = engine.route(sig)
        assert result is not None
        assert len(result.plan.legs) == 1
        assert result.plan.legs[0].exchange == "polymarket"
        # Polymarket legs are market orders (no limit price)
        assert result.plan.legs[0].is_market_order is True


# ──────────────────────────────────────────────────────────────────────────
# T3 Revision 6 — slippage-aware venue selection (SESSION 36)
# ──────────────────────────────────────────────────────────────────────────
# Locks the new behaviour: RoutingEngine consults SlippageCalculator and
# picks the venue with lower net_cost_after_rebate_bps. Default params land
# PM as the winner for every (side × size × mid) combination tested below
# — but the mechanism is in place so a parameter shift (e.g. raising
# clob_maker_rebate_bps) flips selection. Test 4 demonstrates the flip.


class TestVenueSelection:
    """Lock slippage-aware venue selection behaviour for T3."""

    def _make_market(self, price: float, venue: str = "polymarket") -> MarketSignal:
        return MarketSignal(
            id="m",
            condition_id="cond",
            venue=venue,
            price=price,
        )

    def test_default_params_buy_picks_polymarket(self):
        """Under default SlippageParams, BUY: PM (rebate −30bps) beats CLOB
        (taker +50bps). Locked so any param change that inverts this is loud."""
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        market = self._make_market(price=0.45)  # < 0.5 → BUY
        sig = make_signal(profit_pct=3.0, max_stake=1000.0, markets=[market])

        result = engine.route(sig)
        assert result is not None and len(result.plan.legs) == 1
        leg = result.plan.legs[0]
        assert leg.action == "buy"
        assert leg.exchange == "polymarket", (
            f"BUY under default params must route to PM; got {leg.exchange}. "
            f"Either slippage_calc was bypassed or fee params drifted."
        )
        # PM legs are market orders.
        assert leg.limit_price is None

    def test_default_params_sell_picks_polymarket_on_tie(self):
        """SELL: PM cost +50bps == CLOB cost +50bps (no maker). Tie-break
        prefers PM per engine policy (`pm_cost <= clob_cost`)."""
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        market = self._make_market(price=0.55)  # > 0.5 → SELL
        sig = make_signal(profit_pct=3.0, max_stake=1000.0, markets=[market])

        result = engine.route(sig)
        assert result is not None and len(result.plan.legs) == 1
        leg = result.plan.legs[0]
        assert leg.action == "sell"
        assert leg.exchange == "polymarket"
        assert leg.limit_price is None

    def test_estimated_cost_reflects_slippage_model(self):
        """ExecutionLeg.estimated_cost should come from
        SlippageResult.net_cost_dollars(), NOT the naive `price × size`
        formula. For a BUY $1000-notional leg under default params,
        PM net cost = −29.9bps → |−29.9 × 1000/10_000| = ~$2.99."""
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        # max_stake_per_leg = 1000 → size_usd = 0.5 × 1000 = $500 notional
        # PM net cost = −29.9bps × $500 / 10_000 = −$1.495 → |−1.495| = $1.495
        market = self._make_market(price=0.5)
        # price=0.5 produces SELL action (price >= 0.5 boundary). For SELL,
        # PM net = +50.1bps → $500 × 50.1/10_000 = ~$2.505
        sig = make_signal(profit_pct=3.0, max_stake=1000.0, markets=[market])

        result = engine.route(sig)
        assert result is not None and len(result.plan.legs) == 1
        leg = result.plan.legs[0]
        # Slippage-derived cost: SELL @ price=0.5, $500 notional, PM +50bps.
        # Allow generous tolerance — model has market_impact_bps too.
        assert 1.0 <= leg.estimated_cost <= 50.0, (
            f"estimated_cost ${leg.estimated_cost:.2f} outside slippage band; "
            f"if it's exactly 500.0 the engine is using naive price×size again."
        )
        # Critically: not the naive value.
        naive = market.price * sig.max_stake_per_leg
        assert leg.estimated_cost != pytest.approx(naive), (
            f"estimated_cost == naive {naive} → slippage model not applied"
        )

    def test_param_flip_makes_clob_cheaper_for_buy(self):
        """When CLOB maker rebate is artificially raised, BUY routes to CLOB.
        Locks the mechanism (not just the default outcome) so the engine is
        actually consulting the calculator."""
        # Raise CLOB maker rebate to dwarf PM rebate. clob_maker_avail
        # defaults to False in estimate(); the model's clob_net_cost_bps
        # path uses taker_fee_bps + market_impact. So flip taker_fee_bps to
        # be a deeper rebate (negative) and see CLOB win.
        custom_params = SlippageParams(
            maker_fee_bps=-10.0,
            taker_fee_bps=-200.0,  # absurd rebate to flip BUY selection
            pm_rebate_bps=30.0,
            clob_taker_cost_bps=50.0,
            clob_maker_rebate_bps=10.0,
            pm_taker_cost_bps=50.0,
        )
        engine = RoutingEngine(
            RoutingConfig(min_profit_threshold_pct=0.0),
            slippage_calc=SlippageCalculator(params=custom_params),
        )
        market = self._make_market(price=0.45)  # BUY
        sig = make_signal(profit_pct=3.0, max_stake=1000.0, markets=[market])

        result = engine.route(sig)
        assert result is not None and len(result.plan.legs) == 1
        leg = result.plan.legs[0]
        assert leg.exchange == "clob", (
            f"With taker_fee_bps=-200 (deep CLOB rebate), BUY must route to "
            f"CLOB; got {leg.exchange}. Slippage calculator wiring broken."
        )
        # CLOB legs are limit orders.
        assert leg.limit_price is not None
        assert leg.limit_price == pytest.approx(market.price * 1.001)

    def test_caller_venue_override_respected(self):
        """If MarketSignal.venue is set to a non-PM alias (e.g. 'gamma'),
        the engine honours the override even if slippage would prefer PM.
        Lets upstream callers carry richer info than the slippage model."""
        engine = RoutingEngine(RoutingConfig(min_profit_threshold_pct=0.0))
        market = self._make_market(price=0.45, venue="gamma")  # override
        sig = make_signal(profit_pct=3.0, max_stake=1000.0, markets=[market])

        result = engine.route(sig)
        assert result is not None and len(result.plan.legs) == 1
        leg = result.plan.legs[0]
        assert leg.exchange == "gamma", (
            f"Caller override 'gamma' must be respected; engine routed to "
            f"{leg.exchange}. Override path broken."
        )

    def test_backward_compatible_no_slippage_arg(self):
        """Pre-T3 callers don't pass slippage_calc — engine default-constructs.
        Sanity check the 6 existing tests' assumption."""
        engine = RoutingEngine()  # no slippage_calc argument
        assert engine.slippage_calc is not None
        assert isinstance(engine.slippage_calc, SlippageCalculator)
