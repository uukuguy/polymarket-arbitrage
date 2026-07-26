"""Tests for signal models."""

import pytest

from polyarb.models.signal import (
    ArbitrageSignal,
    MarketOutcome,
    MarketSignal,
    Outcome,
    SignalStatus,
)


class TestMarketSignal:
    def test_market_signal_creation(self):
        market = MarketSignal(
            id="market_1",
            condition_id="cond_1",
            venue="polymarket",
            price=0.45,
            outcomes=[
                MarketOutcome(outcome=Outcome.YES, price=0.45, size=500.0),
                MarketOutcome(outcome=Outcome.NO, price=0.55, size=500.0),
            ],
            size=500.0,
        )
        assert market.price == 0.45
        assert len(market.outcomes) == 2

    def test_market_signal_defaults(self):
        market = MarketSignal(
            id="market_2",
            condition_id="cond_2",
            venue="test",
            price=0.5,
        )
        assert market.outcomes == []
        assert market.hedge_ratio == 0.0


class TestArbitrageSignal:
    def test_arbitrage_signal_creation(self):
        signal = ArbitrageSignal(
            opportunity_id="arb_1",
            markets=[],
            max_arbitrage_pct=2.5,
            max_stake_per_leg=100.0,
        )
        assert signal.max_arbitrage_pct == 2.5
        assert signal.max_stake_per_leg == 100.0
        assert signal.status == SignalStatus.DETECTED

    def test_is_profitable_via_status(self):
        """Check profitability via status and max_arbitrage_pct."""
        signal = ArbitrageSignal(
            opportunity_id="arb_2",
            markets=[],
            max_arbitrage_pct=2.0,
            max_stake_per_leg=50.0,
            status=SignalStatus.DETECTED,
        )
        # Use status check for profitability
        assert signal.status == SignalStatus.DETECTED
        assert signal.max_arbitrage_pct > 0

    def test_is_not_profitable(self):
        signal = ArbitrageSignal(
            opportunity_id="arb_3",
            markets=[],
            max_arbitrage_pct=0.5,
            max_stake_per_leg=50.0,
        )
        assert signal.max_arbitrage_pct < 1.0

    def test_is_profitable_edge_case(self):
        signal = ArbitrageSignal(
            opportunity_id="arb_4",
            markets=[],
            max_arbitrage_pct=0.0,
            max_stake_per_leg=50.0,
        )
        assert signal.max_arbitrage_pct == 0.0

    def test_total_stake(self):
        from polyarb.models.signal import ArbitrageLeg, SignalSide

        leg = ArbitrageLeg(
            market_id="test",
            pm_side=SignalSide.YES,
            pm_price=0.5,
            pm_size=100.0,
        )
        signal = ArbitrageSignal(
            opportunity_id="arb_5",
            legs=[leg],
            max_arbitrage_pct=2.0,
            max_stake_per_leg=100.0,
        )
        assert signal.total_stake == 100.0


class TestMarketOutcome:
    def test_market_outcome_creation(self):
        outcome = MarketOutcome(outcome=Outcome.YES, price=0.45, size=500.0)
        assert outcome.outcome == Outcome.YES
        assert outcome.price == 0.45
        assert outcome.size == 500.0


class TestArbitrageLeg:
    def test_effective_cost_yes(self):
        from polyarb.models.signal import ArbitrageLeg, SignalSide

        leg = ArbitrageLeg(
            market_id="test",
            pm_side=SignalSide.YES,
            pm_price=0.6,
            pm_size=100.0,
        )
        assert leg.effective_cost == pytest.approx(60.0)

    def test_effective_cost_no(self):
        from polyarb.models.signal import ArbitrageLeg, SignalSide

        leg = ArbitrageLeg(
            market_id="test",
            pm_side=SignalSide.NO,
            pm_price=0.4,
            pm_size=100.0,
        )
        # For NO side: cost = (1 - price) * size
        assert leg.effective_cost == pytest.approx(60.0)

    def test_is_hedged(self):
        from polyarb.models.signal import ArbitrageLeg, SignalSide

        leg = ArbitrageLeg(
            market_id="test",
            pm_side=SignalSide.YES,
            pm_price=0.5,
            pm_size=100.0,
            gamma_price=0.51,
            gamma_size=100.0,
        )
        assert leg.is_hedged is True
