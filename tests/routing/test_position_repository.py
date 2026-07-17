from __future__ import annotations

import pytest

from polyarb.routing.position_repository import (
    InMemoryPositionRepository,
    PositionState,
)


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
