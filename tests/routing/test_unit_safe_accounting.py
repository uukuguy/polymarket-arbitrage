from __future__ import annotations

import pytest

from polyarb.models.signal import ExecutionLeg
from polyarb.routing.config import PositionConfig
from polyarb.routing.money import Money
from polyarb.routing.position_tracker import Fill, Position, PositionTracker
from polyarb.routing.quantity import Quantity


def test_execution_leg_names_share_quantity_and_keeps_size_alias() -> None:
    leg = ExecutionLeg(
        leg_id="l1",
        exchange="polymarket",
        action="buy",
        asset="token-1",
        quantity=100.0,
        estimated_price=0.5,
    )

    assert leg.quantity == 100.0
    assert leg.quantity_value == Quantity.from_value("100")
    assert leg.size == 100.0
    assert leg.cost_basis_money == Money.from_value("50")


def test_execution_leg_legacy_size_constructor_is_projection_only() -> None:
    leg = ExecutionLeg(
        leg_id="l1",
        exchange="polymarket",
        action="sell",
        asset="token-1",
        size=100.0,
        estimated_price=0.6,
    )

    assert leg.quantity_value == Quantity.from_value("100")
    assert leg.cost_basis_money == Money.from_value("40")


def test_position_separates_quantity_from_cash_cost_basis() -> None:
    position = Position(
        market_id="m1",
        condition_id="c1",
        side="BUY",
        outcome="YES",
        quantity=100,
        entry_price=0.5,
        current_price=0.6,
    )

    assert position.quantity_value == Quantity.from_value("100")
    assert position.quantity == 100.0
    assert position.stake == 100.0
    assert position.cost_basis_money == Money.from_value("50")
    assert position.cost_basis == 50.0
    assert position.pnl_money == Money.from_value("10")


def test_buy_lifecycle_reserves_cost_not_quantity() -> None:
    tracker = PositionTracker(PositionConfig(initial_balance=1000.0))

    assert tracker.open_position(
        "m1", "c1", "BUY", "YES", price=0.5, quantity=100
    )
    position = tracker.open_positions()[0]
    assert tracker.balance == 950.0
    assert tracker.snapshot().max_exposure == 50.0
    assert position.quantity == 100.0
    assert position.cost_basis == 50.0

    pnl = tracker.close_position_with_fill(
        Fill(market_id="m1", exit_price=0.6, filled_quantity=100)
    )

    assert pnl == 10.0
    assert tracker.balance == 1010.0
    assert tracker.total_realized_pnl == 10.0


def test_sell_lifecycle_uses_fully_collateralized_binary_short() -> None:
    tracker = PositionTracker(PositionConfig(initial_balance=1000.0))

    assert tracker.open_position(
        "m1", "c1", "SELL", "YES", price=0.6, quantity=100
    )
    assert tracker.balance == 960.0
    assert tracker.snapshot().max_exposure == 40.0

    pnl = tracker.close_position_with_fill(
        Fill(market_id="m1", exit_price=0.5, filled_quantity=100)
    )

    assert pnl == 10.0
    assert tracker.balance == 1010.0


def test_full_fill_compares_exact_quantity_and_preserves_state_on_mismatch() -> None:
    tracker = PositionTracker(PositionConfig(initial_balance=1000.0))
    assert tracker.open_position(
        "m1", "c1", "BUY", "YES", price=0.4, quantity=0.3
    )

    with pytest.raises(ValueError, match="partial fill"):
        tracker.close_position_with_fill(
            Fill("m1", 0.5, filled_quantity=0.2),
            operation_id="close:partial",
        )

    assert tracker.balance == pytest.approx(999.88)
    assert tracker.open_count == 1
    assert tracker.operation_receipt("close:partial") is None

    pnl = tracker.close_position_with_fill(
        Fill("m1", 0.5, filled_quantity=0.1 + 0.2),
        operation_id="close:full",
    )
    assert pnl == pytest.approx(0.03)
    assert tracker.balance == pytest.approx(1000.03)


def test_fill_legacy_size_alias_is_quantity_not_money() -> None:
    fill = Fill("m1", 0.5, filled_size=100)

    assert fill.filled_quantity_value == Quantity.from_value("100")
    assert fill.filled_quantity == 100.0
    assert fill.filled_size == 100.0
