"""Tests for routing engine."""
import pytest
from polyarb.models.signal import ArbitrageSignal, MarketSignal, MarketOutcome, Outcome
from polyarb.routing.engine import RoutingEngine
from polyarb.routing.config import RoutingConfig


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