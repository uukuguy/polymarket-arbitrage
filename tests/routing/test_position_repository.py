from __future__ import annotations

import sqlite3
from datetime import UTC, datetime

import pytest

from polyarb.routing.position_repository import (
    InMemoryPositionRepository,
    PositionState,
    RepositoryStateError,
    SQLitePositionRepository,
)
from polyarb.routing.position_tracker import Position


def _position(market_id: str = "m1") -> Position:
    return Position(
        market_id=market_id,
        condition_id=f"condition-{market_id}",
        side="BUY",
        outcome="YES",
        stake=100.0,
        entry_price=0.4,
        current_price=0.4,
        leg_id=f"leg-{market_id}",
        opened_at=datetime(2026, 7, 17, 8, 0, tzinfo=UTC),
    )


def _open(state: PositionState, market_id: str = "m1") -> bool:
    state.balance -= 100.0
    state.open_positions[market_id] = _position(market_id)
    return True


def test_in_memory_apply_commits_state_once() -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)
    calls = 0

    def transition(state: PositionState) -> float:
        nonlocal calls
        calls += 1
        state.balance -= 100.0
        return state.balance

    first = repository.apply("open:s1:l1", "open", "m1", transition)
    second = repository.apply("open:s1:l1", "open", "m1", transition)

    assert first == 900.0
    assert second == 900.0
    assert calls == 1
    assert repository.load().balance == 900.0


def test_in_memory_apply_rolls_back_on_exception() -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)

    def transition(state: PositionState) -> None:
        state.balance = 0.0
        state.realized_pnl = 999.0
        raise ValueError("reject")

    with pytest.raises(ValueError, match="reject"):
        repository.apply("bad", "open", "m1", transition)

    assert repository.load() == PositionState(
        balance=1000.0,
        snapshot_balance=1000.0,
    )


def test_load_returns_copy_not_mutable_repository_state() -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)

    loaded = repository.load()
    loaded.balance = 1.0
    loaded.open_positions["m1"] = object()

    assert repository.load() == PositionState(
        balance=1000.0,
        snapshot_balance=1000.0,
    )


@pytest.mark.parametrize("result", [True, False, 3.25, None])
def test_replay_preserves_json_safe_result_types(result) -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)

    first = repository.apply("op-1", "test", "m1", lambda state: result)
    replay = repository.apply(
        "op-1",
        "test",
        "m1",
        lambda state: pytest.fail("replay invoked transition"),
    )

    assert first == result
    assert replay == result


def test_operation_id_cannot_be_reused_for_different_target() -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)
    repository.apply("op-1", "open", "m1", lambda state: True)

    with pytest.raises(ValueError, match="operation identity conflict"):
        repository.apply("op-1", "close", "m2", lambda state: 0.0)


def test_sqlite_instances_share_committed_account_and_positions(tmp_path) -> None:
    path = tmp_path / "positions.db"
    left = SQLitePositionRepository(path, initial_balance=1000.0)
    right = SQLitePositionRepository(path, initial_balance=1000.0)

    assert left.apply("open:s1:l1", "open", "m1", _open) is True

    state = right.load()
    assert state.balance == 900.0
    assert state.snapshot_balance == 1000.0
    assert state.open_positions == {"m1": _position()}


def test_sqlite_duplicate_operation_returns_original_result(tmp_path) -> None:
    repository = SQLitePositionRepository(
        tmp_path / "positions.db", initial_balance=1000.0
    )
    calls = 0

    def transition(state: PositionState) -> float:
        nonlocal calls
        calls += 1
        state.realized_pnl += 5.0
        return 5.0

    assert repository.apply("close:f1", "close", "m1", transition) == 5.0
    assert repository.apply("close:f1", "close", "m1", transition) == 5.0
    assert calls == 1
    assert repository.load().realized_pnl == 5.0


def test_sqlite_apply_rolls_back_account_and_positions_on_exception(tmp_path) -> None:
    repository = SQLitePositionRepository(
        tmp_path / "positions.db", initial_balance=1000.0
    )

    def transition(state: PositionState) -> None:
        _open(state)
        state.realized_pnl = 999.0
        raise ValueError("reject")

    with pytest.raises(ValueError, match="reject"):
        repository.apply("bad", "open", "m1", transition)

    assert repository.load() == PositionState(
        balance=1000.0,
        snapshot_balance=1000.0,
    )


def test_sqlite_reopen_requires_a_new_operation_id(tmp_path) -> None:
    repository = SQLitePositionRepository(
        tmp_path / "positions.db", initial_balance=1000.0
    )
    repository.apply("open:first", "open", "m1", _open)

    def close(state: PositionState) -> float:
        position = state.open_positions.pop("m1")
        state.balance += position.stake
        return 0.0

    repository.apply("close:first", "close", "m1", close)
    assert repository.load().open_positions == {}

    assert repository.apply("open:first", "open", "m1", _open) is True
    assert repository.load().open_positions == {}

    assert repository.apply("open:second", "open", "m1", _open) is True
    assert list(repository.load().open_positions) == ["m1"]


def test_sqlite_durable_state_wins_over_new_initial_balance(tmp_path) -> None:
    path = tmp_path / "positions.db"
    first = SQLitePositionRepository(path, initial_balance=1000.0)
    first.apply("open:first", "open", "m1", _open)

    restarted = SQLitePositionRepository(path, initial_balance=9999.0)

    assert restarted.load().snapshot_balance == 1000.0
    assert restarted.load().balance == 900.0


def test_sqlite_corrupt_account_cardinality_fails_closed(tmp_path) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO m2_account_state VALUES (?, ?, ?, ?, ?)",
            ("corrupt-second-account", 1.0, 1.0, 0.0, "2026-07-17T08:00:00Z"),
        )

    with pytest.raises(RepositoryStateError, match="exactly one account"):
        repository.load()
