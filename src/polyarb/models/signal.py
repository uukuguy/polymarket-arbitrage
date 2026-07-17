"""Unified signal models for the arbitrage execution engine.

Defines the canonical data structures used throughout the routing engine,
execution pipeline, and position tracker.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum

from polyarb.routing.money import Money
from polyarb.routing.quantity import Quantity


def _utcnow() -> datetime:
    return datetime.now(UTC)


class SignalStatus(Enum):
    """Lifecycle states for an ArbitrageSignal."""

    DETECTED = "detected"
    ROUTING = "routing"
    PENDING_EXECUTION = "pending_execution"
    FILLED = "filled"
    PARTIAL = "partial"
    REJECTED = "rejected"
    ABORTED = "aborted"


class SignalSide(Enum):
    """The Polymarket side taken in an arbitrage leg."""

    YES = "yes"
    NO = "no"


class LegSide(Enum):
    """Exchange-side direction for a leg."""

    BUY = "buy"
    SELL = "sell"


class PipelineOutcome(Enum):
    """Terminal outcome of the execution pipeline."""

    FILLED = "filled"
    PARTIAL = "partial"
    REJECTED = "rejected"
    ABORTED = "aborted"


# ─── Routing-layer aliases (also used by routing.engine) ───────────────────────

class Side(Enum):
    BUY = "buy"
    SELL = "sell"


class Outcome(Enum):
    YES = "yes"
    NO = "no"


@dataclass
class MarketOutcome:
    """A single outcome for a market."""

    outcome: Outcome
    price: float
    size: float = 0.0


@dataclass
class MarketSignal:
    """A Polymarket market signal used in routing decisions."""

    id: str
    condition_id: str
    venue: str
    price: float
    outcomes: list[MarketOutcome] = field(default_factory=list)
    size: float = 0.0
    hedge_ratio: float = 0.0


@dataclass
class ArbitrageLeg:
    """A single leg of an arbitrage trade.

    Attributes:
        leg_id: Unique identifier for this leg.
        market_id: Polymarket condition ID for the leg's market.
        pm_side: The Polymarket side taken (YES or NO).
        pm_price: Polymarket fill price at time of execution.
        pm_size: Size filled on Polymarket.
        gamma_side: Gamma hedge direction (BUY or SELL).
        gamma_price: Gamma fill price (None until hedged).
        gamma_size: Gamma size filled (None until hedged).
        hedge_ratio: Fraction of pm_size hedged on Gamma.
        slippage_bps: Slippage incurred vs. quoted price, in bps.
    """

    leg_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    market_id: str = ""
    pm_side: SignalSide = SignalSide.YES
    pm_price: float = 0.0
    pm_size: float = 0.0
    gamma_side: LegSide = LegSide.BUY
    gamma_price: float | None = None
    gamma_size: float | None = None
    hedge_ratio: float = 1.0
    slippage_bps: float = 0.0

    @property
    def effective_cost(self) -> float:
        """Gross cost of this leg before slippage adjustment."""
        if self.pm_side == SignalSide.YES:
            return self.pm_price * self.pm_size
        else:
            return (1.0 - self.pm_price) * self.pm_size

    @property
    def is_hedged(self) -> bool:
        """True when Gamma fill data is present."""
        return self.gamma_price is not None and self.gamma_size is not None

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON logging."""
        return {
            "leg_id": self.leg_id,
            "market_id": self.market_id,
            "pm_side": self.pm_side.value,
            "pm_price": self.pm_price,
            "pm_size": self.pm_size,
            "gamma_side": self.gamma_side.value,
            "gamma_price": self.gamma_price,
            "gamma_size": self.gamma_size,
            "hedge_ratio": self.hedge_ratio,
            "slippage_bps": self.slippage_bps,
        }


@dataclass
class ArbitrageSignal:
    """A complete arbitrage opportunity.

    Attributes:
        signal_id: Unique identifier.
        opportunity_id: Canonical ID linking legs of the same opportunity.
        legs: List of ArbitrageLeg instances.
        detected_at: When the opportunity was detected.
        signal_price: Best available price at detection.
        signal_prob: Derived probability at detection.
        expected_value_pct: Expected value as percentage of stake.
        confidence: Opportunity confidence score [0.0, 1.0].
        updated_at: Last modification timestamp.
        status: Current lifecycle state.
    """

    opportunity_id: str
    legs: list[ArbitrageLeg] = field(default_factory=list)
    signal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    detected_at: datetime = field(default_factory=_utcnow)
    signal_price: float | None = None
    signal_prob: float | None = None
    expected_value_pct: float = 0.0
    confidence: float = 0.0
    updated_at: datetime = field(default_factory=_utcnow)
    status: SignalStatus = SignalStatus.DETECTED
    markets: list[MarketSignal] = field(default_factory=list)
    max_arbitrage_pct: float = 0.0
    max_stake_per_leg: float = 0.0

    def add_leg(self, leg: ArbitrageLeg) -> None:
        """Append a leg and update timestamps."""
        self.legs.append(leg)
        self.updated_at = _utcnow()

    @property
    def total_stake(self) -> float:
        """Sum of Polymarket sizes across all legs."""
        return sum(leg.pm_size for leg in self.legs)

    @property
    def total_slippage_bps(self) -> float:
        """Mean slippage across legs."""
        if not self.legs:
            return 0.0
        return sum(leg.slippage_bps for leg in self.legs) / len(self.legs)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON logging."""
        return {
            "signal_id": self.signal_id,
            "opportunity_id": self.opportunity_id,
            "detected_at": self.detected_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "signal_price": self.signal_price,
            "signal_prob": self.signal_prob,
            "expected_value_pct": self.expected_value_pct,
            "confidence": self.confidence,
            "status": self.status.value,
            "legs": [leg.to_dict() for leg in self.legs],
        }


@dataclass(init=False)
class ExecutionLeg:
    """A leg within an execution plan ready for the pipeline.

    Attributes:
        leg_id: Matches the corresponding ArbitrageLeg.leg_id.
        exchange: Exchange identifier ("polymarket" or "gamma").
        action: "buy" or "sell".
        asset: Ticker/condition identifier.
        quantity: Outcome-token shares to execute.
        limit_price: Worst acceptable price (None = market).
        estimated_price: Expected fill price.
        estimated_cost: Expected total cost.
        hedge_ratio: Fraction of signal size this leg represents.
    """

    leg_id: str
    exchange: str
    action: str
    asset: str
    quantity_value: Quantity
    limit_price: float | None = None
    estimated_price: float = 0.0
    estimated_cost: float = 0.0
    hedge_ratio: float = 1.0

    def __init__(
        self,
        leg_id: str,
        exchange: str,
        action: str,
        asset: str,
        quantity: int | float | str | Quantity | None = None,
        limit_price: float | None = None,
        estimated_price: float = 0.0,
        estimated_cost: float = 0.0,
        hedge_ratio: float = 1.0,
        *,
        size: int | float | str | Quantity | None = None,
    ) -> None:
        if quantity is not None and size is not None:
            raise TypeError("provide quantity, not both quantity and legacy size")
        raw_quantity = quantity if quantity is not None else size
        if raw_quantity is None:
            raise TypeError("execution leg quantity is required")
        self.leg_id = leg_id
        self.exchange = exchange
        self.action = action
        self.asset = asset
        self.quantity_value = (
            raw_quantity
            if isinstance(raw_quantity, Quantity)
            else Quantity.from_value(raw_quantity)
        )
        self.limit_price = limit_price
        self.estimated_price = estimated_price
        self.estimated_cost = estimated_cost
        self.hedge_ratio = hedge_ratio

    @property
    def quantity(self) -> float:
        return self.quantity_value.to_float()

    @property
    def size(self) -> float:
        """Deprecated compatibility view; the value is shares, never pUSD."""
        return self.quantity

    @property
    def cost_basis_money(self) -> Money:
        return Money.collateral_for(
            self.quantity_value,
            self.estimated_price,
            self.action.upper(),
        )

    @property
    def notional_money(self) -> Money:
        return Money.from_value(
            self.quantity_value.to_decimal()
            * Money._price(self.estimated_price)
        )

    @property
    def is_market_order(self) -> bool:
        """True when no limit price is set."""
        return self.limit_price is None


@dataclass
class ExecutionPlan:
    """An ordered sequence of legs ready for sequential execution.

    Attributes:
        signal_id: Source signal this plan was derived from.
        legs: Ordered list of ExecutionLeg instances.
        total_estimated_cost: Pre-flight estimated total cost.
        profit_threshold_pct: Minimum profit required to proceed.
        created_at: When the plan was created.
    """

    signal_id: str
    legs: list[ExecutionLeg] = field(default_factory=list)
    total_estimated_cost: float = 0.0
    profit_threshold_pct: float = 1.0
    created_at: datetime = field(default_factory=_utcnow)

    def leg_for(self, leg_id: str) -> ExecutionLeg | None:
        """Find an ExecutionLeg by its leg_id."""
        for leg in self.legs:
            if leg.leg_id == leg_id:
                return leg
        return None


@dataclass
class PipelineResult:
    """Outcome of a single pipeline execution run.

    Attributes:
        signal_id: Source signal ID.
        outcome: Terminal pipeline outcome.
        filled_legs: List of leg IDs that were successfully filled.
        rejected_legs: List of leg IDs that were rejected.
        aborted_reason: Free-text reason if ABORTED.
        net_pnl: Estimated profit/loss in dollars.
        slippage_realized_bps: Actual slippage observed, in bps.
        executed_at: When execution completed.
    """

    signal_id: str
    outcome: PipelineOutcome
    filled_legs: list[str] = field(default_factory=list)
    rejected_legs: list[str] = field(default_factory=list)
    aborted_reason: str | None = None
    net_pnl: float = 0.0
    slippage_realized_bps: float = 0.0
    executed_at: datetime = field(default_factory=_utcnow)

    @property
    def is_success(self) -> bool:
        """True for FILLED or PARTIAL outcomes."""
        return self.outcome in (PipelineOutcome.FILLED, PipelineOutcome.PARTIAL)

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON logging."""
        return {
            "signal_id": self.signal_id,
            "outcome": self.outcome.value,
            "filled_legs": self.filled_legs,
            "rejected_legs": self.rejected_legs,
            "aborted_reason": self.aborted_reason,
            "net_pnl": self.net_pnl,
            "slippage_realized_bps": self.slippage_realized_bps,
            "executed_at": self.executed_at.isoformat(),
        }


@dataclass
class RoutingDecision:
    """Output of the routing engine.

    Attributes:
        signal_id: Source signal.
        plan: ExecutionPlan with routed legs.
        is_profitable: True when expected profit exceeds the threshold.
        expected_profit_pct: Expected profit as % of stake.
        expected_profit_abs: Expected profit in dollars.
        rejected: Set of leg IDs skipped due to insufficient margin or liquidity.
        reason: Human-readable explanation of the routing decision.
    """

    signal_id: str
    plan: ExecutionPlan
    is_profitable: bool = False
    expected_profit_pct: float = 0.0
    expected_profit_abs: float = 0.0
    rejected: set[str] = field(default_factory=set)
    reason: str = ""

    def to_dict(self) -> dict:
        """Serialize to a dict for JSON logging."""
        return {
            "signal_id": self.signal_id,
            "is_profitable": self.is_profitable,
            "expected_profit_pct": self.expected_profit_pct,
            "expected_profit_abs": self.expected_profit_abs,
            "rejected": list(self.rejected),
            "reason": self.reason,
            "plan": {
                "signal_id": self.plan.signal_id,
                "legs": [
                    {
                        "leg_id": leg.leg_id,
                        "exchange": leg.exchange,
                        "action": leg.action,
                        "asset": leg.asset,
                        "size": leg.quantity,
                        "quantity": leg.quantity,
                        "cost_basis": leg.cost_basis_money.to_float(),
                        "limit_price": leg.limit_price,
                    }
                    for leg in self.plan.legs
                ],
            },
        }
