\
"""Position tracker: tracks open positions and realized P&L."""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime

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
        if self.snapshot_balance == 0:
            return 0.0
        return (self.total_pnl / self.snapshot_balance) * 100.0


class PositionTracker:
    """Tracks open positions, balance, and P&L."""

    def __init__(self, config: PositionConfig | None = None) -> None:
        self.config = config or PositionConfig()
        self._balance: float = self.config.initial_balance
        self._snapshot_balance: float = self.config.initial_balance
        self._open_positions: dict[str, Position] = {}
        self._realized_pnl: float = 0.0

    def can_open_position(self, size: float) -> tuple[bool, str]:
        """Check if a position of given size can be opened.

        Returns (True, "") if OK, (False, reason) if not.
        """
        if size > self._balance:
            return False, f"Insufficient balance: need {size:.2f}, have {self._balance:.2f}"
        if self._total_exposure + size > self.config.max_total_exposure:
            return False, f"Max exposure exceeded: {self._total_exposure + size:.2f} > {self.config.max_total_exposure:.2f}"
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
            logger.warning("Insufficient balance for position: need %.2f, have %.2f", stake, self._balance)
            return False
        if self._total_exposure + stake > self.config.max_total_exposure:
            logger.warning("Max exposure reached: %.2f + %.2f > %.2f", self._total_exposure, stake, self.config.max_total_exposure)
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
        logger.info("Opened position: %s %s @ %.4f, stake=%.2f", side, market_id, price, stake)
        return True

    def update(self, legs: int, pnl: float) -> None:
        """Update tracker after execution (for compatibility)."""
        self._realized_pnl += pnl
        logger.debug("Updated tracker: +%d legs, PnL=%.2f", legs, pnl)

    def update_prices(self, prices: dict[str, float]) -> None:
        for market_id, price in prices.items():
            if market_id in self._open_positions:
                self._open_positions[market_id].current_price = price

    def close_position(self, market_id: str, exit_price: float | None = None) -> float:
        pos = self._open_positions.pop(market_id, None)
        if pos is None:
            logger.warning("No open position for market %s", market_id)
            return 0.0
        if exit_price is not None:
            pos.current_price = exit_price
        pnl = pos.pnl
        self._balance += pos.stake + pnl
        self._realized_pnl += pnl
        logger.info("Closed position: %s @ %.4f, PnL=%.2f", market_id, exit_price or pos.current_price, pnl)
        return pnl

    def check_stop_loss(self) -> bool:
        if not self.config.enable_pnl_stop:
            return False
        total_pnl = self.total_realized_pnl
        loss = (total_pnl / self._snapshot_balance) * 100.0
        if abs(loss) >= self.config.stop_loss_pct:
            logger.warning("Stop loss triggered: loss=%.2f%% >= %.2f%%", loss, self.config.stop_loss_pct)
            return True
        return False

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

    def snapshot(self) -> PositionSnapshot:
        return PositionSnapshot(
            balance=self._balance,
            total_unrealized_pnl=self._total_unrealized_pnl,
            total_realized_pnl=self._realized_pnl,
            open_positions=len(self._open_positions),
            max_exposure=self._total_exposure,
        )
