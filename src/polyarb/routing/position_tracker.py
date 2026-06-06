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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from polyarb.routing.config import PositionConfig

logger = logging.getLogger(__name__)


@dataclass
class Position:
    """A single position on a market."""

    market_id: str
    condition_id: str
    side: str
    outcome: str
    stake: float
    entry_price: float
    current_price: float
    leg_id: str = ""
    opened_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def pnl(self) -> float:
        if self.side == "BUY":
            return self.stake * (self.current_price - self.entry_price)
        else:
            return self.stake * (self.entry_price - self.current_price)

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

    def __init__(self, config: PositionConfig | None = None) -> None:
        self.config = config or PositionConfig()
        self._balance: float = self.config.initial_balance
        self._snapshot_balance: float = self.config.initial_balance
        self._open_positions: dict[str, Position] = {}
        self._realized_pnl: float = 0.0

    def can_open_position(self, size: float) -> tuple[bool, str]:
        """Return (True, "") if a position of `size` can be opened, else (False, reason)."""
        if size > self._balance:
            return False, f"Insufficient balance: need {size:.2f}, have {self._balance:.2f}"
        if self._total_exposure + size > self.config.max_total_exposure:
            return (
                False,
                f"Max exposure exceeded: {self._total_exposure + size:.2f} > "
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
    ) -> bool:
        if stake > self._balance:
            logger.warning(
                "Insufficient balance for position: need %.2f, have %.2f",
                stake,
                self._balance,
            )
            return False
        if self._total_exposure + stake > self.config.max_total_exposure:
            logger.warning(
                "Max exposure reached: %.2f + %.2f > %.2f",
                self._total_exposure,
                stake,
                self.config.max_total_exposure,
            )
            return False
        if market_id in self._open_positions:
            logger.warning("Position already open for market %s", market_id)
            return False
        pos = Position(
            market_id=market_id,
            condition_id=condition_id,
            side=side,
            outcome=outcome,
            stake=stake,
            entry_price=price,
            current_price=price,
            leg_id=leg_id,
        )
        self._open_positions[market_id] = pos
        self._balance -= stake
        logger.info(
            "Opened position: %s %s @ %.4f, stake=%.2f", side, market_id, price, stake
        )
        return True

    def update(self, legs: int, pnl: float) -> None:
        """Legacy compatibility shim — `orchestrator.py` still calls it.

        Production T5+ should use `close_position_with_fill` instead. Kept
        here so the orchestrator's tests don't fail; flagged for removal
        in a follow-up task once the orchestrator is migrated.
        """
        self._realized_pnl += pnl
        logger.debug("Updated tracker (legacy path): +%d legs, PnL=%.2f", legs, pnl)

    def update_prices(self, prices: dict[str, float]) -> None:
        for market_id, price in prices.items():
            if market_id in self._open_positions:
                self._open_positions[market_id].current_price = price

    def close_position(self, market_id: str, exit_price: float | None = None) -> float:
        """Low-level close primitive — closes regardless of fill size.

        Production code path uses `close_position_with_fill` (which enforces
        size match). This primitive stays for explicit operator close (e.g.,
        `make close-arb market_id=... exit_price=...`).
        """
        pos = self._open_positions.pop(market_id, None)
        if pos is None:
            logger.warning("No open position for market %s", market_id)
            return 0.0
        if exit_price is not None:
            pos.current_price = exit_price
        pnl = pos.pnl
        self._balance += pos.stake + pnl
        self._realized_pnl += pnl
        logger.info(
            "Closed position: %s @ %.4f, PnL=%.2f",
            market_id,
            exit_price if exit_price is not None else pos.current_price,
            pnl,
        )
        return pnl

    def close_position_with_fill(self, fill: Fill) -> float:
        """Production close path: a venue fill closes an open position.

        Requires `fill.filled_size == position.stake` (no partial fills in
        T5). Returns realized PnL; updates balance + realized_pnl. If no
        position is open for `fill.market_id`, returns 0.0 and warns
        (callers can audit log this).
        """
        pos = self._open_positions.get(fill.market_id)
        if pos is None:
            logger.warning(
                "close_position_with_fill: no open position for market %s",
                fill.market_id,
            )
            return 0.0
        if fill.filled_size != pos.stake:
            raise ValueError(
                f"partial fill not supported (T5 scope): position stake "
                f"{pos.stake} but fill size {fill.filled_size}"
            )
        # Safe to close — same arithmetic as close_position.
        del self._open_positions[fill.market_id]
        pos.current_price = fill.exit_price
        pnl = pos.pnl
        self._balance += pos.stake + pnl
        self._realized_pnl += pnl
        logger.info(
            "Closed via fill: %s @ %.4f, size=%.2f, PnL=%.2f",
            fill.market_id,
            fill.exit_price,
            fill.filled_size,
            pnl,
        )
        return pnl

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
        if self._snapshot_balance == 0:
            return None
        loss_pct = (self._realized_pnl / self._snapshot_balance) * 100.0
        # Loss = negative pnl → abs(loss_pct) compared to threshold.
        if self._realized_pnl >= 0:
            return None
        # FP-tolerant threshold compare: "at threshold" must trigger even
        # when float arithmetic produces e.g. 4.999999999 ≈ 5.0.
        if abs(loss_pct) + 1e-9 < self.config.stop_loss_pct:
            return None
        logger.warning(
            "Stop loss triggered: realized_pnl=%.2f, loss=%.2f%% >= %.2f%%",
            self._realized_pnl,
            abs(loss_pct),
            self.config.stop_loss_pct,
        )
        return StopLossEvent(
            loss_pct=abs(loss_pct),
            realized_pnl=self._realized_pnl,
            threshold_pct=self.config.stop_loss_pct,
        )

    @property
    def _total_exposure(self) -> float:
        return sum(p.stake for p in self._open_positions.values())

    @property
    def _total_unrealized_pnl(self) -> float:
        return sum(p.pnl for p in self._open_positions.values())

    @property
    def balance(self) -> float:
        return self._balance

    @property
    def total_realized_pnl(self) -> float:
        return self._realized_pnl

    @property
    def open_count(self) -> int:
        return len(self._open_positions)

    def open_positions(self) -> Iterable[Position]:
        """Read-only view of currently open positions."""
        return list(self._open_positions.values())

    def snapshot(self) -> PositionSnapshot:
        return PositionSnapshot(
            balance=self._balance,
            total_unrealized_pnl=self._total_unrealized_pnl,
            total_realized_pnl=self._realized_pnl,
            open_positions=len(self._open_positions),
            max_exposure=self._total_exposure,
        )
