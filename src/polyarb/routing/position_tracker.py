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
- **Partial-fill rejection**: T5 scope explicitly does NOT do partial fill
  aggregation. A fill with `filled_size != position.stake` raises ValueError
  loudly rather than silently booking wrong PnL. Aggregation is T5+1.
- **Bug fix**: `PositionSnapshot.roi_pct` referenced a non-existent
  `self.snapshot_balance` field. Fixed to use `self.balance`.
"""
from __future__ import annotations

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
)

logger = logging.getLogger(__name__)


@dataclass(init=False)
class Position:
    """A single position on a market."""

    market_id: str
    condition_id: str
    side: str
    outcome: str
    stake_money: Money
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
        stake: int | float | str | Money,
        entry_price: float,
        current_price: float,
        leg_id: str = "",
        opened_at: datetime | None = None,
    ) -> None:
        self.market_id = market_id
        self.condition_id = condition_id
        self.side = side
        self.outcome = outcome
        self.stake_money = (
            stake if isinstance(stake, Money) else Money.from_value(stake)
        )
        self.entry_price = entry_price
        self.current_price = current_price
        self.leg_id = leg_id
        self.opened_at = opened_at or datetime.now(UTC)

    @property
    def stake(self) -> float:
        return self.stake_money.to_float()

    @property
    def pnl_money(self) -> Money:
        return Money.pnl_at(
            self.stake_money,
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
        return (self.pnl / self.stake) * 100.0


@dataclass
class Fill:
    """A venue fill event — the bridge between executor and tracker close path.

    Production: the real `leg_executor` (py-clob-client adapter) returns this
    after the venue confirms a fill. Paper mode: ExecutionEngine synthesizes
    one at the leg's estimated_price.

    T5 scope: filled_size must equal the open position's stake. Partial
    fill aggregation (multiple fills per position) is T5+1.
    """

    market_id: str
    exit_price: float
    filled_size: float
    filled_at: datetime = field(default_factory=datetime.utcnow)
    fill_id: str = ""


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
        stake: float,
        price: float,
        leg_id: str = "",
        operation_id: str | None = None,
    ) -> bool:
        stake_money = Money.from_value(stake)

        def transition(state: PositionState) -> bool:
            if stake_money.micros > state.balance_money.micros:
                logger.warning(
                    "Insufficient balance for position: need %.2f, have %.2f",
                    stake_money.to_float(),
                    state.balance,
                )
                return False
            total_exposure = self._exposure_money(state)
            requested_exposure = total_exposure + stake_money
            max_exposure = Money.from_value(self.config.max_total_exposure)
            if requested_exposure.micros > max_exposure.micros:
                logger.warning(
                    "Max exposure reached: %.2f + %.2f > %.2f",
                    total_exposure.to_float(),
                    stake_money.to_float(),
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
                stake=stake_money,
                entry_price=price,
                current_price=price,
                leg_id=leg_id,
            )
            state.balance_money = state.balance_money - stake_money
            logger.info(
                "Opened position: %s %s @ %.4f, stake=%.2f",
                side,
                market_id,
                price,
                stake_money.to_float(),
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
            state.balance_money = state.balance_money + pos.stake_money + pnl_money
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
    ) -> float:
        """Production close path: a venue fill closes an open position.

        Requires `fill.filled_size == position.stake` (no partial fills in
        T5). Returns realized PnL; updates balance + realized_pnl. If no
        position is open for `fill.market_id`, returns 0.0 and warns
        (callers can audit log this).
        """
        def transition(state: PositionState) -> Money:
            pos = state.open_positions.get(fill.market_id)
            if pos is None:
                logger.warning(
                    "close_position_with_fill: no open position for market %s",
                    fill.market_id,
                )
                return Money(0)
            fill_size_money = Money.from_value(fill.filled_size)
            if fill_size_money != pos.stake_money:
                raise ValueError(
                    f"partial fill not supported (T5 scope): position stake "
                    f"{pos.stake} but fill size {fill.filled_size}"
                )
            del state.open_positions[fill.market_id]
            pos.current_price = fill.exit_price
            pnl_money = pos.pnl_money
            state.balance_money = state.balance_money + pos.stake_money + pnl_money
            state.realized_pnl_money = state.realized_pnl_money + pnl_money
            logger.info(
                "Closed via fill: %s @ %.4f, size=%.2f, PnL=%.2f",
                fill.market_id,
                fill.exit_price,
                fill.filled_size,
                pnl_money.to_float(),
            )
            return pnl_money

        result = self.repository.apply(
            self._operation_id(operation_id, "close-fill", fill.market_id),
            "close",
            fill.market_id,
            transition,
        )
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
            exposure = exposure + position.stake_money
        return exposure
