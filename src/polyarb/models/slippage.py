"""Slippage model for Polymarket/CLOB dual-execution legs.

Encapsulates the logic for computing:
1. CLOB taker slippage from mid-price delta + market impact
2. PM maker fee rebate
3. Effective all-in cost per leg
4. Cross-execution fee differential vs. single-venue baseline
"""

from dataclasses import dataclass, field


@dataclass
class SlippageParams:
    """Tunable slippage parameters — all values in basis points."""

    # CLOB taker fees
    maker_fee_bps: float = -10.0  # rebate
    taker_fee_bps: float = 50.0  # cost

    # Market-impact model coefficients
    impact_coef: float = 0.001  # dollar impact per dollar of notional / sqrt(daily_volume_usd)
    vol_pct: float = 2.0  # annualized vol assumption (2%)

    # PM maker rebate vs. CLOB taker baseline
    pm_rebate_bps: float = 30.0
    clob_taker_cost_bps: float = 50.0

    # CLOB maker rebate vs. PM taker baseline
    clob_maker_rebate_bps: float = 10.0
    pm_taker_cost_bps: float = 50.0

    # Execution size breakpoints (in USD notional)
    small_notional: float = 100.0
    mid_notional: float = 1000.0

    def fee_diff_bps(self, side: str, clob_maker_avail: bool) -> float:
        """Fee differential vs. single-venue baseline, in bps of notional."""
        if side.upper() == "BUY":
            if clob_maker_avail:
                # CLOB maker at -10bps vs PM taker at -50bps: 40bps cheaper
                return self.clob_maker_rebate_bps - (-self.pm_taker_cost_bps)
            else:
                # PM maker at +30bps vs PM taker at +50bps: 20bps cheaper
                return self.pm_rebate_bps - (-self.pm_taker_cost_bps)
        else:  # SELL
            if clob_maker_avail:
                return self.clob_maker_rebate_bps - self.pm_taker_cost_bps
            else:
                return self.pm_rebate_bps - self.pm_taker_cost_bps


@dataclass
class SlippageResult:
    """Breakdown of slippage costs for a single leg."""

    side: str  # "BUY" | "SELL"
    venue: str  # "PM" | "CLOB"

    # Slippage components (all in bps)
    market_impact_bps: float = 0.0
    fee_bps: float = 0.0
    mid_price_delta_bps: float = 0.0

    # Aggregates
    total_cost_bps: float = 0.0
    net_cost_after_rebate_bps: float = 0.0

    # Execution metadata
    filled_at: float | None = None  # actual fill price (None if not yet filled)
    size_usd: float = 0.0

    def net_cost_dollars(self) -> float:
        """Convert net cost to dollars."""
        return self.size_usd * self.net_cost_after_rebate_bps / 10_000

    def to_dict(self) -> dict:
        return {
            "side": self.side,
            "venue": self.venue,
            "market_impact_bps": round(self.market_impact_bps, 4),
            "fee_bps": round(self.fee_bps, 4),
            "mid_price_delta_bps": round(self.mid_price_delta_bps, 4),
            "total_cost_bps": round(self.total_cost_bps, 4),
            "net_cost_after_rebate_bps": round(self.net_cost_after_rebate_bps, 4),
            "filled_at": self.filled_at,
            "size_usd": self.size_usd,
        }


@dataclass
class SlippageCalculator:
    """Computes slippage estimates for dual-venue arbitrage legs."""

    params: SlippageParams = field(default_factory=SlippageParams)

    def estimate(
        self,
        side: str,
        venue: str,
        size_usd: float,
        mid_price: float,
        clob_bid: float | None = None,
        clob_ask: float | None = None,
        pm_bid: float | None = None,
        pm_ask: float | None = None,
        clob_maker_avail: bool = False,
        daily_volume_usd: float = 10_000.0,
    ) -> SlippageResult:
        """Estimate slippage for a single-leg fill.

        Args:
            side: "BUY" or "SELL"
            venue: "PM" or "CLOB"
            size_usd: Order size in USD notional
            mid_price: Mid-market price
            clob_bid/ask: CLOB order book levels (if venue == "CLOB")
            pm_bid/ask: PM order book levels (if venue == "PM")
            clob_maker_avail: Whether CLOB maker quotes are accessible
            daily_volume_usd: Estimated daily volume for market-impact scaling

        Returns:
            SlippageResult with cost breakdown
        """
        result = SlippageResult(side=side, venue=venue, size_usd=size_usd)

        if mid_price <= 0 or size_usd <= 0:
            return result

        # --- Market impact (Kyle's lambda approximation) ---
        notional = size_usd
        sqrt_dvol = (daily_volume_usd or 1.0) ** 0.5
        impact_dollar = self.params.impact_coef * notional / sqrt_dvol
        result.market_impact_bps = (impact_dollar / notional) * 10_000

        # --- Fee ---
        if venue == "PM":
            if side == "BUY":
                result.fee_bps = -self.params.pm_rebate_bps  # rebate = negative cost
            else:
                result.fee_bps = self.params.pm_taker_cost_bps
        else:  # CLOB
            if clob_maker_avail:
                result.fee_bps = -self.params.maker_fee_bps  # maker rebate
            else:
                result.fee_bps = self.params.taker_fee_bps  # taker cost

        # --- Mid-price delta vs. signal time ---
        if venue == "CLOB" and clob_bid is not None and clob_ask is not None:
            current_mid = (clob_bid + clob_ask) / 2
            result.mid_price_delta_bps = abs(current_mid - mid_price) / mid_price * 10_000
        elif venue == "PM" and pm_bid is not None and pm_ask is not None:
            current_mid = (pm_bid + pm_ask) / 2
            result.mid_price_delta_bps = abs(current_mid - mid_price) / mid_price * 10_000

        # --- Totals ---
        result.total_cost_bps = (
            result.market_impact_bps + abs(result.fee_bps) + result.mid_price_delta_bps
        )
        result.net_cost_after_rebate_bps = (
            result.market_impact_bps
            + result.fee_bps  # already signed
            + result.mid_price_delta_bps
        )

        return result

    def estimate_cross_execution_savings(
        self,
        size_usd: float,
        side: str,
        mid_price: float,
        clob_bid: float | None = None,
        clob_ask: float | None = None,
        pm_bid: float | None = None,
        pm_ask: float | None = None,
        daily_volume_usd: float = 10_000.0,
    ) -> dict:
        """Estimate savings from cross-execution vs. single-venue.

        Returns dict with PM-only, CLOB-only, and cross-exec costs.
        """
        pm_result = self.estimate(
            side=side,
            venue="PM",
            size_usd=size_usd,
            mid_price=mid_price,
            pm_bid=pm_bid,
            pm_ask=pm_ask,
            daily_volume_usd=daily_volume_usd,
        )
        clob_result = self.estimate(
            side=side,
            venue="CLOB",
            size_usd=size_usd,
            mid_price=mid_price,
            clob_bid=clob_bid,
            clob_ask=clob_ask,
            clob_maker_avail=False,
            daily_volume_usd=daily_volume_usd,
        )
        return {
            "pm_net_cost_bps": pm_result.net_cost_after_rebate_bps,
            "clob_net_cost_bps": clob_result.net_cost_after_rebate_bps,
            "savings_bps": clob_result.net_cost_after_rebate_bps
            - pm_result.net_cost_after_rebate_bps,
            "pm_result": pm_result.to_dict(),
            "clob_result": clob_result.to_dict(),
        }

    def estimate_leg(
        self,
        side: str,
        venue: str,
        mid_price: float,
        quantity_shares: float,
        clob_maker_avail: bool = False,
        daily_volume_usd: float = 10_000.0,
    ) -> SlippageResult:
        """Estimate slippage for a single leg (shares-based convenience wrapper).

        Args:
            side: "BUY" or "SELL"
            venue: "PM" or "CLOB"
            mid_price: Mid-market price (per share)
            quantity_shares: Number of shares
            clob_maker_avail: Whether CLOB maker quotes are accessible
            daily_volume_usd: Estimated daily volume for market-impact scaling

        Returns:
            SlippageResult with cost breakdown
        """
        size_usd = quantity_shares * mid_price
        return self.estimate(
            side=side,
            venue=venue,
            size_usd=size_usd,
            mid_price=mid_price,
            clob_maker_avail=clob_maker_avail,
            daily_volume_usd=daily_volume_usd,
        )

    def compare_venues(
        self,
        side: str,
        mid_price: float,
        quantity_shares: float,
        quantity_usd: float | None = None,
        maker_rebate_bps: float = 1.0,
        daily_volume_usd: float = 10_000.0,
    ) -> dict:
        """Compare PM vs CLOB execution costs for a given leg.

        Args:
            side: "BUY" or "SELL"
            mid_price: Mid-market price (per share)
            quantity_shares: Number of shares
            quantity_usd: Optional size in USD (computed from shares if not provided)
            maker_rebate_bps: CLOB maker rebate in bps
            daily_volume_usd: Estimated daily volume for market-impact scaling

        Returns:
            dict with pm_result, clob_result, and savings_bps
        """
        size_usd = quantity_usd if quantity_usd is not None else quantity_shares * mid_price
        pm_result = self.estimate(
            side=side,
            venue="PM",
            size_usd=size_usd,
            mid_price=mid_price,
            daily_volume_usd=daily_volume_usd,
        )
        clob_result = self.estimate(
            side=side,
            venue="CLOB",
            size_usd=size_usd,
            mid_price=mid_price,
            clob_maker_avail=False,
            daily_volume_usd=daily_volume_usd,
        )
        return {
            "pm_result": pm_result,
            "clob_result": clob_result,
            "savings_bps": clob_result.net_cost_after_rebate_bps
            - pm_result.net_cost_after_rebate_bps,
        }


# ─── Signal-layer slippage abstractions ─────────────────────────────────────────


@dataclass
class SlippageEstimate:
    """Computed slippage for a single venue leg."""

    base_bps: float
    market_impact_bps: float
    fee_bps: float
    total_bps: float

    @classmethod
    def zero(cls) -> "SlippageEstimate":
        return cls(base_bps=0.0, market_impact_bps=0.0, fee_bps=0.0, total_bps=0.0)


@dataclass
class VenueSlippageProfile:
    """Venue-specific slippage calibration."""

    venue: str
    base_spread_bps: float = 5.0
    market_impact_coef: float = 0.01
    fee_bps: float = 0.0
    maker_rebate_bps: float = 0.0
    max_impact_bps: float = 50.0


class SlippageModel:
    """Model-based slippage estimator using venue profiles."""

    DEFAULT_PROFILES: dict[str, VenueSlippageProfile] = {
        "PM": VenueSlippageProfile(
            venue="PM", base_spread_bps=3.0, market_impact_coef=0.005, fee_bps=0.0
        ),
        "CLOB": VenueSlippageProfile(
            venue="CLOB",
            base_spread_bps=2.0,
            market_impact_coef=0.008,
            fee_bps=50.0,
            maker_rebate_bps=-10.0,
        ),
        "POL": VenueSlippageProfile(
            venue="POL", base_spread_bps=4.0, market_impact_coef=0.006, fee_bps=0.0
        ),
    }

    def __init__(self, profiles: dict[str, VenueSlippageProfile] | None = None) -> None:
        self._profiles = profiles or self.DEFAULT_PROFILES

    def estimate(self, venue: str, size_usd: float, mid_price: float) -> SlippageEstimate:
        profile = self._profiles.get(venue)
        if profile is None:
            return SlippageEstimate.zero()
        base = profile.base_spread_bps / 2
        impact = min(profile.market_impact_coef * (size_usd / 1000), profile.max_impact_bps)
        total = base + impact + profile.fee_bps + profile.maker_rebate_bps
        return SlippageEstimate(
            base_bps=base, market_impact_bps=impact, fee_bps=profile.fee_bps, total_bps=total
        )
