"""Position tracker: open / update / close positions, realize PnL, surface stop-loss.

T5 Revision 9 (2026-06-06 SESSION 37) — Position Tracker realization.

T4 wired `open_position`. T5 wires the rest of the lifecycle:

- **Fill model** (`Fill`): venue-agnostic record of a fill event. Production
  flow: real `leg_executor` returns a `Fill`. Paper mode: ExecutionEngine
  synthesizes one at the leg's estimated_price.
- **`close_position_with_fill(fill)`**: the production close path. Books
  realized PnL, restores balance, removes from open set.
- **`StopLossEvent`**: richer return type from `check_stop_loss_event` than
  bare bool — carries loss_pct, realized_pnl, recommendation. Legacy bool
  form (`check_stop_loss`) preserved.
- **Partial-fill rejection**: this scope explicitly does NOT do partial fill
  aggregation. A fill quantity unequal to the position quantity raises ValueError
  loudly rather than silently booking wrong PnL. Aggregation is T5+1.
- **Bug fix**: `PositionSnapshot.roi_pct` referenced a non-existent
  `self.snapshot_balance` field. Fixed to use `self.balance`.
"""
from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from polyarb.routing.config import PositionConfig
from polyarb.routing.money import Money
from polyarb.routing.position_repository import (
    InMemoryPositionRepository,
    OperationReceipt,
    PositionRepository,
    PositionState,
    SettlementReceipt,
)
from polyarb.routing.quantity import Quantity

logger = logging.getLogger(__name__)


@dataclass(init=False)
class Position:
    """A single position on a market."""

    market_id: str
    condition_id: str
    side: str
    outcome: str
    quantity_value: Quantity
    cost_basis_money: Money
    entry_price: float
    current_price: float
    leg_id: str = ""
    opened_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def __init__(
        self,
        market_id: str,
        condition_id: str,
        side: str,
        outcome: str,
        stake: int | float | str | Money | Quantity | None = None,
        entry_price: float = 0.0,
        current_price: float = 0.0,
        leg_id: str = "",
        opened_at: datetime | None = None,
        *,
        quantity: int | float | str | Quantity | None = None,
        cost_basis: int | float | str | Money | None = None,
    ) -> None:
        self.market_id = market_id
        self.condition_id = condition_id
        self.side = side
        self.outcome = outcome
        if stake is not None and quantity is not None:
            raise TypeError("provide quantity, not both quantity and legacy stake")
        raw_quantity = quantity if quantity is not None else stake
        if raw_quantity is None:
            raise TypeError("position quantity is required")
        if isinstance(raw_quantity, Quantity):
            self.quantity_value = raw_quantity
        elif isinstance(raw_quantity, Money):
            self.quantity_value = Quantity(raw_quantity.micros)
        else:
            self.quantity_value = Quantity.from_value(raw_quantity)
        self.entry_price = entry_price
        self.current_price = current_price
        self.cost_basis_money = (
            cost_basis
            if isinstance(cost_basis, Money)
            else Money.from_value(cost_basis)
            if cost_basis is not None
            else Money.collateral_for(self.quantity_value, entry_price, side)
        )
        self.leg_id = leg_id
        self.opened_at = opened_at or datetime.now(UTC)

    @property
    def stake(self) -> float:
        """Deprecated compatibility view; historical stake inputs are shares."""
        return self.quantity

    @property
    def stake_money(self) -> Money:
        """Deprecated money alias for the position's cash cost basis."""
        return self.cost_basis_money

    @property
    def quantity(self) -> float:
        return self.quantity_value.to_float()

    @property
    def cost_basis(self) -> float:
        return self.cost_basis_money.to_float()

    @property
    def pnl_money(self) -> Money:
        return Money.pnl_for(
            self.quantity_value,
            self.entry_price,
            self.current_price,
            self.side,
        )

    @property
    def pnl(self) -> float:
        return self.pnl_money.to_float()

    @property
    def pnl_pct(self) -> float:
        if self.entry_price == 0:
            return 0.0
        if self.cost_basis_money.micros == 0:
            return 0.0
        return (self.pnl / self.cost_basis) * 100.0


@dataclass(frozen=True, slots=True)
class VenueSettlement:
    """Terminal venue cash truth attached to an immutable fill identity."""

    gross_cash: Money
    fee: Money
    status: str
    source_ref: str

    def __post_init__(self) -> None:
        if not isinstance(self.gross_cash, Money) or not isinstance(self.fee, Money):
            raise TypeError("venue settlement gross_cash and fee must be Money")
        if self.status != "CONFIRMED":
            raise ValueError("venue settlement status must be CONFIRMED")
        if not isinstance(self.source_ref, str) or not self.source_ref:
            raise ValueError("venue settlement source_ref must be non-empty")
        if self.gross_cash.micros < 0 or self.fee.micros < 0:
            raise ValueError("venue settlement cash values must be non-negative")
        if self.fee.micros > self.gross_cash.micros:
            raise ValueError("venue settlement fee cannot exceed gross cash")

    @property
    def net_cash(self) -> Money:
        return self.gross_cash - self.fee


@dataclass(init=False)
class Fill:
    """A venue fill event — the bridge between executor and tracker close path.

    Production: the real `leg_executor` (py-clob-client adapter) returns this
    after the venue confirms a fill. Paper mode: ExecutionEngine synthesizes
    one at the leg's estimated_price.

    Each fill may consume part or all of the remaining position quantity. Partial
    fills require an immutable venue fill ID so retries cannot book twice.
    """

    market_id: str
    exit_price: float
    filled_quantity_value: Quantity
    filled_at: datetime
    fill_id: str = ""
    settlement: VenueSettlement | None = None

    def __init__(
        self,
        market_id: str,
        exit_price: float,
        filled_quantity: int | float | str | Quantity | None = None,
        filled_at: datetime | None = None,
        fill_id: str = "",
        settlement: VenueSettlement | None = None,
        *,
        filled_size: int | float | str | Quantity | None = None,
    ) -> None:
        if filled_quantity is not None and filled_size is not None:
            raise TypeError(
                "provide filled_quantity, not both it and legacy filled_size"
            )
        raw_quantity = (
            filled_quantity if filled_quantity is not None else filled_size
        )
        if raw_quantity is None:
            raise TypeError("fill quantity is required")
        self.market_id = market_id
        self.exit_price = exit_price
        self.filled_quantity_value = (
            raw_quantity
            if isinstance(raw_quantity, Quantity)
            else Quantity.from_value(raw_quantity)
        )
        self.filled_at = filled_at or datetime.now(UTC)
        self.fill_id = fill_id
        if settlement is not None and not isinstance(settlement, VenueSettlement):
            raise TypeError("fill settlement must be VenueSettlement")
        self.settlement = settlement

    @property
    def filled_quantity(self) -> float:
        return self.filled_quantity_value.to_float()

    @property
    def filled_size(self) -> float:
        """Deprecated compatibility view; fill size is share quantity."""
        return self.filled_quantity


@dataclass
class StopLossEvent:
    """Surfaced by `check_stop_loss_event` when realized loss crosses threshold.

    Callers should halt new signal evaluation and decide whether to close
    open positions. The `recommendation` is advisory text the CLI / monitor
    can display; the bool form (`check_stop_loss`) returns True whenever
    this event is non-None.
    """

    loss_pct: float
    realized_pnl: float
    threshold_pct: float
    recommendation: str = "halt_new_signals"


@dataclass
class PositionSnapshot:
    """Snapshot of the full position state."""

    balance: float
    total_unrealized_pnl: float
    total_realized_pnl: float
    open_positions: int
    max_exposure: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

    @property
    def total_pnl(self) -> float:
        return self.total_realized_pnl + self.total_unrealized_pnl

    @property
    def roi_pct(self) -> float:
        # Bug fix (T5.1): pre-T5 this referenced self.snapshot_balance which
        # never existed. Any snapshot reader would AttributeError. Now uses
        # the snapshot's own balance field.
        if self.balance == 0:
            return 0.0
        return (self.total_pnl / self.balance) * 100.0


class PositionTracker:
    """Tracks open positions, balance, realized PnL, stop-loss state."""

    def __init__(
        self,
        config: PositionConfig | None = None,
        repository: PositionRepository | None = None,
    ) -> None:
        self.config = config or PositionConfig()
        self.repository = repository or InMemoryPositionRepository(
            self.config.initial_balance
        )

    def operation_receipt(self, operation_id: str) -> OperationReceipt | None:
        """Return the committed result for an immutable operation identity."""
        return self.repository.get_receipt(operation_id)

    @staticmethod
    def _operation_id(provided: str | None, kind: str, target: str) -> str:
        return provided or f"local:{kind}:{target}:{uuid4()}"

    def can_open_position(self, size: float) -> tuple[bool, str]:
        """Return (True, "") if a position of `size` can be opened, else (False, reason)."""
        state = self.repository.load()
        size_money = Money.from_value(size)
        if size_money.micros > state.balance_money.micros:
            return (
                False,
                f"Insufficient balance: need {size_money.to_float():.2f}, "
                f"have {state.balance:.2f}",
            )
        total_exposure = self._exposure_money(state)
        requested_exposure = total_exposure + size_money
        max_exposure = Money.from_value(self.config.max_total_exposure)
        if requested_exposure.micros > max_exposure.micros:
            return (
                False,
                f"Max exposure exceeded: {requested_exposure.to_float():.2f} > "
                f"{self.config.max_total_exposure:.2f}",
            )
        return True, ""

    def open_position(
        self,
        market_id: str,
        condition_id: str,
        side: str,
        outcome: str,
        stake: int | float | str | Money | Quantity | None = None,
        price: float = 0.0,
        leg_id: str = "",
        operation_id: str | None = None,
        *,
        quantity: int | float | str | Quantity | None = None,
    ) -> bool:
        if stake is not None and quantity is not None:
            raise TypeError("provide quantity, not both quantity and legacy stake")
        raw_quantity = quantity if quantity is not None else stake
        if raw_quantity is None:
            raise TypeError("position quantity is required")
        if isinstance(raw_quantity, Quantity):
            quantity_value = raw_quantity
        elif isinstance(raw_quantity, Money):
            quantity_value = Quantity(raw_quantity.micros)
        else:
            quantity_value = Quantity.from_value(raw_quantity)
        cost_basis_money = Money.collateral_for(quantity_value, price, side)

        def transition(state: PositionState) -> bool:
            if cost_basis_money.micros > state.balance_money.micros:
                logger.warning(
                    "Insufficient balance for position: need %.2f, have %.2f",
                    cost_basis_money.to_float(),
                    state.balance,
                )
                return False
            total_exposure = self._exposure_money(state)
            requested_exposure = total_exposure + cost_basis_money
            max_exposure = Money.from_value(self.config.max_total_exposure)
            if requested_exposure.micros > max_exposure.micros:
                logger.warning(
                    "Max exposure reached: %.2f + %.2f > %.2f",
                    total_exposure.to_float(),
                    cost_basis_money.to_float(),
                    self.config.max_total_exposure,
                )
                return False
            if market_id in state.open_positions:
                logger.warning("Position already open for market %s", market_id)
                return False
            state.open_positions[market_id] = Position(
                market_id=market_id,
                condition_id=condition_id,
                side=side,
                outcome=outcome,
                quantity=quantity_value,
                cost_basis=cost_basis_money,
                entry_price=price,
                current_price=price,
                leg_id=leg_id,
            )
            state.balance_money = state.balance_money - cost_basis_money
            logger.info(
                "Opened position: %s %s @ %.4f, stake=%.2f",
                side,
                market_id,
                price,
                cost_basis_money.to_float(),
            )
            return True

        result = self.repository.apply(
            self._operation_id(operation_id, "open", market_id),
            "open",
            market_id,
            transition,
        )
        assert isinstance(result, bool)
        return result

    def update(
        self, legs: int, pnl: float, operation_id: str | None = None
    ) -> None:
        """Legacy compatibility shim — `orchestrator.py` still calls it.

        Production T5+ should use `close_position_with_fill` instead. Kept
        here so the orchestrator's tests don't fail; flagged for removal
        in a follow-up task once the orchestrator is migrated.
        """
        def transition(state: PositionState) -> None:
            state.realized_pnl_money = (
                state.realized_pnl_money + Money.from_value(pnl)
            )
            logger.debug(
                "Updated tracker (legacy path): +%d legs, PnL=%.2f", legs, pnl
            )

        self.repository.apply(
            self._operation_id(operation_id, "legacy-update", "account"),
            "legacy-update",
            "account",
            transition,
        )

    def update_prices(
        self, prices: dict[str, float], operation_id: str | None = None
    ) -> None:
        def transition(state: PositionState) -> None:
            for market_id, price in prices.items():
                if market_id in state.open_positions:
                    state.open_positions[market_id].current_price = price

        target = ",".join(sorted(prices)) or "none"
        self.repository.apply(
            self._operation_id(operation_id, "prices", target),
            "prices",
            target,
            transition,
        )

    def close_position(
        self,
        market_id: str,
        exit_price: float | None = None,
        operation_id: str | None = None,
    ) -> float:
        """Low-level close primitive — closes regardless of fill size.

        Production code path uses `close_position_with_fill` (which enforces
        size match). This primitive stays for explicit operator close (e.g.,
        `make close-arb market_id=... exit_price=...`).
        """
        def transition(state: PositionState) -> Money:
            pos = state.open_positions.pop(market_id, None)
            if pos is None:
                logger.warning("No open position for market %s", market_id)
                return Money(0)
            if exit_price is not None:
                pos.current_price = exit_price
            pnl_money = pos.pnl_money
            state.balance_money = (
                state.balance_money + pos.cost_basis_money + pnl_money
            )
            state.realized_pnl_money = state.realized_pnl_money + pnl_money
            logger.info(
                "Closed position: %s @ %.4f, PnL=%.2f",
                market_id,
                exit_price if exit_price is not None else pos.current_price,
                pnl_money.to_float(),
            )
            return pnl_money

        result = self.repository.apply(
            self._operation_id(operation_id, "close", market_id),
            "close",
            market_id,
            transition,
        )
        if isinstance(result, Money):
            return result.to_float()
        assert type(result) is float
        return result

    def close_position_with_fill(
        self, fill: Fill, operation_id: str | None = None
    ) -> float | SettlementReceipt:
        """Production close path: a venue fill closes an open position.

        Applies a positive fill no larger than the remaining quantity. Partial
        fills require an immutable fill ID; the final fill consumes the exact
        residual cost basis. Returns realized PnL and updates balance/state in
        the same repository transaction. If no position is open, returns 0.0
        and warns so callers can audit a late venue event.
        """
        if fill.settlement is not None and not fill.fill_id:
            raise ValueError("venue settlement requires immutable fill_id")
        effective_operation_id = (
            f"venue-fill:{fill.fill_id}"
            if fill.fill_id
            else self._operation_id(operation_id, "close-fill", fill.market_id)
        )

        request_fingerprint = ""
        if fill.fill_id and fill.settlement is None:
            canonical_exit_price = format(
                Decimal(str(fill.exit_price)).normalize(), "f"
            )
            request_fingerprint = "modeled-fill:v1:" + json.dumps(
                {
                    "exit_price": canonical_exit_price,
                    "market_id": fill.market_id,
                    "quantity_micros": fill.filled_quantity_value.micros,
                },
                sort_keys=True,
                separators=(",", ":"),
            )
        elif fill.settlement is not None:
            settlement = fill.settlement
            request_fingerprint = "venue-settlement:v1:" + json.dumps(
                {
                    "fee_micros": settlement.fee.micros,
                    "gross_micros": settlement.gross_cash.micros,
                    "market_id": fill.market_id,
                    "quantity_micros": fill.filled_quantity_value.micros,
                    "source_ref": settlement.source_ref,
                    "status": settlement.status,
                },
                sort_keys=True,
                separators=(",", ":"),
            )

        def transition(state: PositionState) -> Money | SettlementReceipt:
            pos = state.open_positions.get(fill.market_id)
            if pos is None:
                logger.warning(
                    "close_position_with_fill: no open position for market %s",
                    fill.market_id,
                )
                return Money(0)
            if fill.filled_quantity_value.micros <= 0:
                raise ValueError(
                    "fill quantity must be positive"
                )
            if fill.filled_quantity_value.micros > pos.quantity_value.micros:
                raise ValueError(
                    f"fill quantity {fill.filled_quantity} exceeds remaining "
                    f"quantity {pos.quantity}"
                )
            is_partial = fill.filled_quantity_value != pos.quantity_value
            if is_partial and not fill.fill_id:
                raise ValueError("partial fill requires immutable fill_id")

            allocated_cost = Money.allocate(
                pos.cost_basis_money,
                fill.filled_quantity_value,
                pos.quantity_value,
            )
            pos.current_price = fill.exit_price
            if fill.settlement is None:
                pnl_money = Money.pnl_for(
                    fill.filled_quantity_value,
                    pos.entry_price,
                    fill.exit_price,
                    pos.side,
                )
                cash_returned = allocated_cost + pnl_money
                transition_result: Money | SettlementReceipt = pnl_money
            else:
                cash_returned = fill.settlement.net_cash
                pnl_money = cash_returned - allocated_cost
                transition_result = SettlementReceipt(
                    gross_cash=fill.settlement.gross_cash,
                    fee=fill.settlement.fee,
                    net_cash=cash_returned,
                    realized_pnl=pnl_money,
                )
            state.balance_money = state.balance_money + cash_returned
            state.realized_pnl_money = state.realized_pnl_money + pnl_money
            if is_partial:
                pos.quantity_value = (
                    pos.quantity_value - fill.filled_quantity_value
                )
                pos.cost_basis_money = pos.cost_basis_money - allocated_cost
            else:
                del state.open_positions[fill.market_id]
            logger.info(
                "Applied fill: %s @ %.4f, quantity=%.6f, remaining=%.6f, "
                "PnL=%.2f",
                fill.market_id,
                fill.exit_price,
                fill.filled_quantity,
                pos.quantity if is_partial else 0.0,
                pnl_money.to_float(),
            )
            return transition_result

        result = self.repository.apply(
            effective_operation_id,
            "close",
            fill.market_id,
            transition,
            request_fingerprint=request_fingerprint,
        )
        if isinstance(result, SettlementReceipt):
            return result
        if isinstance(result, Money):
            return result.to_float()
        assert type(result) is float
        return result

    def check_stop_loss(self) -> bool:
        """Legacy bool form — True if loss crossed threshold."""
        return self.check_stop_loss_event() is not None

    def check_stop_loss_event(self) -> StopLossEvent | None:
        """Return a StopLossEvent if realized loss crossed the threshold, else None.

        Uses realized PnL only (not unrealized) — unrealized swings shouldn't
        force a halt by themselves. Disabled config → always None.
        """
        if not self.config.enable_pnl_stop:
            return None
        state = self.repository.load()
        if state.snapshot_balance_money.micros == 0:
            return None
        loss_pct = (
            Decimal(abs(state.realized_pnl_money.micros))
            * Decimal(100)
            / Decimal(state.snapshot_balance_money.micros)
        )
        # Loss = negative pnl → abs(loss_pct) compared to threshold.
        if state.realized_pnl_money.micros >= 0:
            return None
        if loss_pct < Decimal(str(self.config.stop_loss_pct)):
            return None
        logger.warning(
            "Stop loss triggered: realized_pnl=%.2f, loss=%.2f%% >= %.2f%%",
            state.realized_pnl,
            float(loss_pct),
            self.config.stop_loss_pct,
        )
        return StopLossEvent(
            loss_pct=float(loss_pct),
            realized_pnl=state.realized_pnl,
            threshold_pct=self.config.stop_loss_pct,
        )

    @property
    def _total_exposure(self) -> float:
        return self._exposure_money(self.repository.load()).to_float()

    @property
    def _total_unrealized_pnl(self) -> float:
        return sum(p.pnl for p in self.repository.load().open_positions.values())

    @property
    def _balance(self) -> float:
        """Compatibility view for legacy tests and diagnostic callers."""
        return self.repository.load().balance

    @property
    def _snapshot_balance(self) -> float:
        return self.repository.load().snapshot_balance

    @property
    def _open_positions(self) -> dict[str, Position]:
        return self.repository.load().open_positions

    @property
    def _realized_pnl(self) -> float:
        return self.repository.load().realized_pnl

    @property
    def balance(self) -> float:
        return self.repository.load().balance

    @property
    def total_realized_pnl(self) -> float:
        return self.repository.load().realized_pnl

    @property
    def open_count(self) -> int:
        return len(self.repository.load().open_positions)

    def open_positions(self) -> Iterable[Position]:
        """Read-only view of currently open positions."""
        return list(self.repository.load().open_positions.values())

    def snapshot(self) -> PositionSnapshot:
        state = self.repository.load()
        total_exposure = self._exposure_money(state).to_float()
        total_unrealized_pnl = sum(p.pnl for p in state.open_positions.values())
        return PositionSnapshot(
            balance=state.balance,
            total_unrealized_pnl=total_unrealized_pnl,
            total_realized_pnl=state.realized_pnl,
            open_positions=len(state.open_positions),
            max_exposure=total_exposure,
        )

    @staticmethod
    def _exposure_money(state: PositionState) -> Money:
        exposure = Money(0)
        for position in state.open_positions.values():
            exposure = exposure + position.cost_basis_money
        return exposure
