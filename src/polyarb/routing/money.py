"""Exact micro-pUSD values for the M2 paper-account ledger."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Self

MICROS_PER_PUSD = 1_000_000
SQLITE_INT_MIN = -(2**63)
SQLITE_INT_MAX = 2**63 - 1


@dataclass(frozen=True, slots=True)
class Money:
    """A canonical signed amount of pUSD stored as integer micro-units."""

    micros: int

    def __post_init__(self) -> None:
        if type(self.micros) is not int:
            raise TypeError("money micros must be an integer, not a boolean or float")
        if not SQLITE_INT_MIN <= self.micros <= SQLITE_INT_MAX:
            raise OverflowError("money micros must fit a signed 64-bit SQLite INTEGER")

    @classmethod
    def from_value(cls, value: int | float | str | Decimal) -> Self:
        """Quantize a pUSD-facing value to one micro using half-even rounding."""
        if isinstance(value, bool):
            raise TypeError("boolean is not a valid money value")
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid money value: {value!r}") from exc
        if not decimal_value.is_finite():
            raise ValueError("money value must be finite")
        micros = int(
            (decimal_value * MICROS_PER_PUSD).to_integral_value(
                rounding=ROUND_HALF_EVEN
            )
        )
        return cls(micros)

    def to_decimal(self) -> Decimal:
        return Decimal(self.micros) / Decimal(MICROS_PER_PUSD)

    def to_float(self) -> float:
        return float(self.to_decimal())

    def __add__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        return Money(self.micros + other.micros)

    def __sub__(self, other: object) -> Money:
        if not isinstance(other, Money):
            return NotImplemented
        return Money(self.micros - other.micros)

    @classmethod
    def pnl_at(
        cls,
        stake: Money,
        entry_price: int | float | str | Decimal,
        exit_price: int | float | str | Decimal,
        side: str,
    ) -> Money:
        """Calculate modeled cash PnL and round once at the ledger boundary."""
        try:
            entry = Decimal(str(entry_price))
            exit_ = Decimal(str(exit_price))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError("prices must be valid decimal-facing values") from exc
        if not entry.is_finite() or not exit_.is_finite():
            raise ValueError("prices must be finite")
        if side == "BUY":
            delta = exit_ - entry
        elif side == "SELL":
            delta = entry - exit_
        else:
            raise ValueError("side must be BUY or SELL")
        return cls.from_value(stake.to_decimal() * delta)
