from __future__ import annotations

from decimal import Decimal

import pytest

from polyarb.routing.money import Money


@pytest.mark.parametrize(
    ("value", "micros"),
    [
        ("10.25", 10_250_000),
        (0.1, 100_000),
        (Decimal("-2.000001"), -2_000_001),
        (0, 0),
    ],
)
def test_money_quantizes_decimal_facing_values_to_micros(value, micros) -> None:
    money = Money.from_value(value)

    assert money.micros == micros
    assert money.to_decimal() == Decimal(micros) / Decimal(1_000_000)
    assert money.to_float() == float(money.to_decimal())


@pytest.mark.parametrize(
    ("value", "micros"),
    [
        ("0.0000005", 0),
        ("0.0000015", 2),
        ("-0.0000005", 0),
        ("-0.0000015", -2),
    ],
)
def test_money_uses_half_even_at_one_micro(value: str, micros: int) -> None:
    assert Money.from_value(value).micros == micros


@pytest.mark.parametrize("value", [True, False])
def test_money_rejects_boolean_values(value: bool) -> None:
    with pytest.raises(TypeError, match="boolean"):
        Money.from_value(value)


@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity", float("nan")])
def test_money_rejects_non_finite_values(value) -> None:
    with pytest.raises(ValueError, match="finite"):
        Money.from_value(value)


@pytest.mark.parametrize("micros", [-(2**63) - 1, 2**63])
def test_money_rejects_values_outside_sqlite_integer_range(micros: int) -> None:
    with pytest.raises(OverflowError, match="signed 64-bit"):
        Money(micros)


def test_money_arithmetic_accepts_only_money() -> None:
    left = Money.from_value("2.25")
    right = Money.from_value("0.75")

    assert left + right == Money.from_value("3")
    assert left - right == Money.from_value("1.5")
    with pytest.raises(TypeError):
        left + 1  # type: ignore[operator]


@pytest.mark.parametrize(
    ("side", "entry", "exit", "expected"),
    [
        ("BUY", 0.4, 0.5, "10"),
        ("SELL", 0.6, 0.5, "10"),
        ("BUY", 0.500000004, 0.500000009, "0.0005"),
    ],
)
def test_money_pnl_at_quantizes_once_after_price_delta(
    side: str, entry: float, exit: float, expected: str
) -> None:
    stake = Money.from_value("100")

    assert Money.pnl_at(stake, entry, exit, side) == Money.from_value(expected)


def test_money_pnl_at_rejects_unknown_side() -> None:
    with pytest.raises(ValueError, match="BUY or SELL"):
        Money.pnl_at(Money.from_value("100"), 0.4, 0.5, "HOLD")
