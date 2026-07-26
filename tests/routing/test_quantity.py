from __future__ import annotations

from decimal import Decimal

import pytest

from polyarb.routing.quantity import Quantity


@pytest.mark.parametrize(
    ("value", "micros"),
    [
        ("100.25", 100_250_000),
        (0.1, 100_000),
        (Decimal("0.000001"), 1),
        (0, 0),
    ],
)
def test_quantity_quantizes_decimal_facing_shares(value, micros: int) -> None:
    quantity = Quantity.from_value(value)

    assert quantity.micros == micros
    assert quantity.to_decimal() == Decimal(micros) / Decimal(1_000_000)


@pytest.mark.parametrize(
    ("value", "micros"),
    [("0.0000005", 0), ("0.0000015", 2)],
)
def test_quantity_uses_half_even_at_one_micro_share(value: str, micros: int) -> None:
    assert Quantity.from_value(value).micros == micros


@pytest.mark.parametrize("value", [True, False, "NaN", "Infinity", -1, "-0.000001"])
def test_quantity_rejects_invalid_share_values(value) -> None:
    with pytest.raises((TypeError, ValueError)):
        Quantity.from_value(value)


@pytest.mark.parametrize("micros", [-1, 2**63])
def test_quantity_rejects_invalid_storage_units(micros: int) -> None:
    with pytest.raises((ValueError, OverflowError)):
        Quantity(micros)


def test_quantity_arithmetic_never_accepts_money_or_scalars() -> None:
    left = Quantity.from_value("2.25")
    right = Quantity.from_value("0.75")

    assert left + right == Quantity.from_value("3")
    assert left - right == Quantity.from_value("1.5")
    with pytest.raises(TypeError):
        left + 1  # type: ignore[operator]
