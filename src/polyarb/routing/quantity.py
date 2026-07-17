"""Exact outcome-token quantities for M2 execution accounting."""
from __future__ import annotations

from dataclasses import dataclass
from decimal import ROUND_HALF_EVEN, Decimal, InvalidOperation
from typing import Self

MICROS_PER_SHARE = 1_000_000
SQLITE_INT_MAX = 2**63 - 1


@dataclass(frozen=True, slots=True)
class Quantity:
    """A non-negative number of outcome-token shares in integer micro-shares."""

    micros: int

    def __post_init__(self) -> None:
        if type(self.micros) is not int:
            raise TypeError("quantity micros must be an integer, not a boolean or float")
        if self.micros < 0:
            raise ValueError("quantity cannot be negative")
        if self.micros > SQLITE_INT_MAX:
            raise OverflowError("quantity micros must fit a signed 64-bit SQLite INTEGER")

    @classmethod
    def from_value(cls, value: int | float | str | Decimal) -> Self:
        """Quantize a share-facing value to one micro-share using half-even."""
        if isinstance(value, bool):
            raise TypeError("boolean is not a valid quantity")
        try:
            decimal_value = Decimal(str(value))
        except (InvalidOperation, ValueError) as exc:
            raise ValueError(f"invalid quantity value: {value!r}") from exc
        if not decimal_value.is_finite():
            raise ValueError("quantity must be finite")
        if decimal_value < 0:
            raise ValueError("quantity cannot be negative")
        micros = int(
            (decimal_value * MICROS_PER_SHARE).to_integral_value(
                rounding=ROUND_HALF_EVEN
            )
        )
        return cls(micros)

    def to_decimal(self) -> Decimal:
        return Decimal(self.micros) / Decimal(MICROS_PER_SHARE)

    def to_float(self) -> float:
        return float(self.to_decimal())

    def __add__(self, other: object) -> Quantity:
        if not isinstance(other, Quantity):
            return NotImplemented
        return Quantity(self.micros + other.micros)

    def __sub__(self, other: object) -> Quantity:
        if not isinstance(other, Quantity):
            return NotImplemented
        return Quantity(self.micros - other.micros)
