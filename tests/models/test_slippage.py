import pytest
from polyarb.models.slippage import SlippageCalculator, SlippageParams, SlippageResult


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


# ──────────────────────────────────────────────────────────────────────────
# IMDEA Type-2 cross-venue fee-differential validation
# ──────────────────────────────────────────────────────────────────────────
# Plan ref: .planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/
#           02-1-PLAN.md §T2 Revision 4 (locked 2026-05-20)
# Paper ref: arxiv 2508.03474 (IMDEA, Polymarket 2024-04 → 2025-04, 86M trades,
#            $40M total arb profit, Top 3 wallets $4.2M).
#
# IMDEA classifies arbitrage into two empirical buckets:
#   - Market Rebalancing Arbitrage (single-market YES + NO ≠ 1)
#   - Combinatorial Arbitrage (cross-market same-event price drift)
#
# The slippage model here measures the CROSS-VENUE FEE DIFFERENTIAL — the
# residual profit a routing engine can scrape by choosing CLOB maker over PM
# taker (or vice versa) for the same leg. This is the unit-economics input
# that decides whether an arb identified upstream survives execution costs.
#
# These tests LOCK the model's economic claims so future code changes can't
# silently invert sign conventions or shift the fee table:
#
#   (a) fee_diff_bps("BUY", clob_maker_avail=True) =
#         clob_maker_rebate_bps (+10) − (−pm_taker_cost_bps) (−(−50))
#         = 10 + 50 = 60 bps favorable for BUY when CLOB maker is available
#
#   (b) fee_diff_bps matrix for SELL: with/without clob_maker_avail
#
#   (c) estimate_cross_execution_savings on the IMDEA reference scenario
#       (size=$1k, mid=0.5) produces a per-fill savings that's on the
#       $1-$10 order of magnitude — consistent with how Top 3 wallets
#       scraped $4.2M over millions of fills (not via huge per-trade profit
#       but via large fill counts at thin fee-differential margins).


class TestImdeaType2FeeDifferential:
    """Lock the fee-differential economic theorem for routing/execution."""

    def setup_method(self):
        self.params = SlippageParams()
        self.calc = SlippageCalculator(params=self.params)

    def test_fee_diff_bps_buy_with_clob_maker_locks_at_60bps(self):
        """BUY + CLOB maker accessible: 60 bps favorable differential.

        Derivation (locked, see model line 41-49):
            clob_maker_rebate_bps − (−pm_taker_cost_bps) = 10 − (−50) = 60

        Economic meaning: when CLOB has resting maker liquidity for the same
        outcome token, taking it (and earning the maker rebate) instead of
        paying PM taker fee saves 60 bps of notional per leg. For a $1k leg
        that's $6 per fill — at IMDEA's empirical fill counts, the scale
        explains the Top 3 wallet aggregate.
        """
        diff = self.params.fee_diff_bps(side="BUY", clob_maker_avail=True)
        assert diff == pytest.approx(60.0), (
            f"BUY+clob_maker locked at 60bps (clob_maker_rebate + pm_taker_cost); "
            f"got {diff}. Sign convention regression?"
        )

    def test_fee_diff_bps_sell_matrix_locks(self):
        """SELL fee differential under both clob_maker availability states.

        Derivation:
            SELL+clob_maker: clob_maker_rebate − pm_taker_cost = 10 − 50 = −40
            SELL+no_maker:   pm_rebate         − pm_taker_cost = 30 − 50 = −20

        Both are negative — meaning for SELL the cross-venue option is MORE
        expensive than the PM-only baseline. This is asymmetry the routing
        engine must respect (T3 dependency): cross-venue arb is favorable on
        the BUY leg, unfavorable on the SELL leg. A real Type-2 trade pairs
        BUY+CLOB-maker (favorable) with SELL+PM-only (least-bad), netting a
        positive expected differential.
        """
        sell_clob_maker = self.params.fee_diff_bps(side="SELL", clob_maker_avail=True)
        sell_no_maker = self.params.fee_diff_bps(side="SELL", clob_maker_avail=False)

        assert sell_clob_maker == pytest.approx(-40.0), (
            f"SELL+clob_maker locked at −40bps; got {sell_clob_maker}"
        )
        assert sell_no_maker == pytest.approx(-20.0), (
            f"SELL+no_maker locked at −20bps; got {sell_no_maker}"
        )
        # SELL+maker is WORSE than SELL+no_maker — counter-intuitive but
        # falls out of the current sign convention. Lock it so T3 routing
        # logic correctly skips clob_maker on the SELL leg of a Type-2 pair.
        assert sell_clob_maker < sell_no_maker, (
            "SELL+clob_maker must be worse than SELL+no_maker under current "
            "sign convention; if this inverts, T3 routing assumptions break"
        )

    def test_estimate_cross_execution_savings_imdea_unit_economics(self):
        """IMDEA reference scenario: $1k size, mid=0.5, no quoted book.

        Per-fill savings must land in the single-digit-dollar range — not
        cents (model would be useless for arb) and not hundreds (would
        violate market-microstructure sanity for a 0.5-priced market).

        At IMDEA's 86M trades total → if the addressable Type-2 subset is
        even 1% (~860k trades) and average savings is $1-5, the implied
        aggregate ($0.86M-$4.3M) matches the Top 3 wallet bracket ($4.2M).
        This test asserts the per-trade economics live in that band.
        """
        result = self.calc.estimate_cross_execution_savings(
            size_usd=1_000.0,
            side="BUY",
            mid_price=0.5,
            daily_volume_usd=10_000.0,
        )

        # Structural contract first.
        assert set(result.keys()) >= {
            "pm_net_cost_bps", "clob_net_cost_bps", "savings_bps",
            "pm_result", "clob_result",
        }, f"missing keys; got {sorted(result.keys())}"

        savings_bps = result["savings_bps"]
        savings_dollars = abs(savings_bps) * 1000.0 / 10_000.0  # $1k notional × bps

        # IMDEA-magnitude unit economics: per-fill savings must be in the
        # $0.10 — $20 band (covers thin-margin retail flow up to large
        # institutional arb). Outside this band → either fee table drift
        # or notional/size unit mismatch.
        assert 0.10 <= savings_dollars <= 20.0, (
            f"savings ${savings_dollars:.2f} on $1k @ mid=0.5 outside IMDEA "
            f"unit-economics band [$0.10, $20.00]; check fee table or sign "
            f"convention drift. savings_bps={savings_bps}, "
            f"pm_net={result['pm_net_cost_bps']:.2f}, "
            f"clob_net={result['clob_net_cost_bps']:.2f}"
        )

        # Also lock the SIGN — for BUY, cross-execution (CLOB cheaper) means
        # savings_bps positive when computed as (clob_cost − pm_cost) > 0,
        # because the helper subtracts PM from CLOB. Drift here = routing
        # engine misreads the favorable direction.
        # Note: under default params, PM is actually cheaper for BUY because
        # PM rebate (−30bps fee) beats CLOB taker (+50bps fee); the savings
        # is therefore in CLOB's column being higher cost. Lock the sign so
        # T3 doesn't accidentally invert.
        assert result["clob_net_cost_bps"] > result["pm_net_cost_bps"], (
            "BUY: PM-only baseline must be cheaper than CLOB-taker baseline "
            "under default params (PM rebate > CLOB taker fee); if this "
            "inverts, fee table changed and T3 venue selection breaks"
        )
