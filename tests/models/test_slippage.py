import pytest
from polyarb.models.slippage import SlippageCalculator, SlippageResult


class TestSlippageCalculator:
    def setup_method(self):
        self.calc = SlippageCalculator()

    def test_estimate_leg_buy_pm(self):
        result = self.calc.estimate_leg(
            side="BUY",
            venue="PM",
            mid_price=0.55,
            quantity_shares=100.0,
        )
        assert isinstance(result, SlippageResult)
        assert result.venue == "PM"
        assert result.side == "BUY"

    def test_estimate_leg_sell_clob(self):
        result = self.calc.estimate_leg(
            side="SELL",
            venue="CLOB",
            mid_price=0.45,
            quantity_shares=50.0,
            clob_maker_avail=True,
        )
        assert result.venue == "CLOB"
        assert result.side == "SELL"

    def test_compare_venues_returns_dict(self):
        result = self.calc.compare_venues(
            side="BUY",
            mid_price=0.55,
            quantity_shares=100.0,
        )
        assert isinstance(result, dict)
        assert "pm_result" in result
        assert "clob_result" in result
        assert "savings_bps" in result

    def test_compare_venues_sell(self):
        result = self.calc.compare_venues(
            side="SELL",
            mid_price=0.45,
            quantity_shares=50.0,
        )
        assert "savings_bps" in result
        assert isinstance(result["savings_bps"], float)
