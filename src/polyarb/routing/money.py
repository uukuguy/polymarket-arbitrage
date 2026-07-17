"""Exact micro-pUSD values for the M2 paper-account ledger."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Self

from polyarb.routing.quantity import Quantity

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

    @staticmethod
    def _price(value: int | float | str | Decimal) -> Decimal:
        try:
            price = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid price value: {value!r}") from exc
        if not price.is_finite():
            raise ValueError("price must be finite")
        if not Decimal(0) <= price <= Decimal(1):
            raise ValueError("price must be between 0 and 1")
        return price

    @classmethod
    def collateral_for(
        cls,
        quantity: Quantity,
        entry_price: int | float | str | Decimal,
        side: str,
    ) -> Money:
        """Return fully collateralized pUSD reserved for an execution quantity."""
        if not isinstance(quantity, Quantity):
            raise TypeError("collateral quantity must be Quantity")
        price = cls._price(entry_price)
        if side == "BUY":
            per_share = price
        elif side == "SELL":
            per_share = Decimal(1) - price
        else:
            raise ValueError("side must be BUY or SELL")
        return cls.from_value(quantity.to_decimal() * per_share)

    @classmethod
    def pnl_for(
        cls,
        quantity: Quantity,
        entry_price: int | float | str | Decimal,
        exit_price: int | float | str | Decimal,
        side: str,
    ) -> Money:
        """Return modeled pUSD PnL for an exact outcome-token quantity."""
        if not isinstance(quantity, Quantity):
            raise TypeError("PnL quantity must be Quantity")
        entry = cls._price(entry_price)
        exit_ = cls._price(exit_price)
        if side == "BUY":
            delta = exit_ - entry
        elif side == "SELL":
            delta = entry - exit_
        else:
            raise ValueError("side must be BUY or SELL")
        return cls.from_value(quantity.to_decimal() * delta)

    @classmethod
    def allocate(
        cls,
        total: Money,
        part: Quantity,
        whole: Quantity,
    ) -> Money:
        """Allocate exact cash by quantity; a final fill consumes the residual."""
        if not isinstance(total, Money):
            raise TypeError("allocation total must be Money")
        if not isinstance(part, Quantity) or not isinstance(whole, Quantity):
            raise TypeError("allocation quantities must be Quantity")
        if whole.micros <= 0 or part.micros <= 0:
            raise ValueError("allocation quantities must be positive")
        if part.micros > whole.micros:
            raise ValueError("allocation part cannot exceed whole")
        if part == whole:
            return total
        allocated_micros = int(
            (
                Decimal(total.micros)
                * Decimal(part.micros)
                / Decimal(whole.micros)
            ).to_integral_value(rounding=ROUND_HALF_EVEN)
        )
        return cls(allocated_micros)
