from __future__ import annotations

import json
import sqlite3
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime

import pytest

import polyarb.routing.position_repository as repository_module
from polyarb.routing.money import Money
from polyarb.routing.position_repository import (
    InMemoryPositionRepository,
    PositionState,
    RepositoryStateError,
    SQLitePositionRepository,
)
from polyarb.routing.position_tracker import Position

_PHASE4_SCHEMA = """
CREATE TABLE m2_account_state (
    account_id TEXT PRIMARY KEY,
    snapshot_balance REAL NOT NULL,
    balance REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE m2_open_positions (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    stake REAL NOT NULL,
    entry_price REAL NOT NULL,
    current_price REAL NOT NULL,
    leg_id TEXT NOT NULL,
    opened_at TEXT NOT NULL
);
CREATE TABLE m2_applied_operations (
    operation_id TEXT PRIMARY KEY,
    operation_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    result_json TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
"""


def _create_phase4_database(path, *, balance: float = 919.9999999999999) -> None:
    with sqlite3.connect(path) as con:
        con.executescript(_PHASE4_SCHEMA)
        con.execute(
            "INSERT INTO m2_account_state "
            "(account_id, snapshot_balance, balance, realized_pnl, updated_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("paper", 1000.0, balance, 19.999999999999996, "2026-07-17T08:00:00Z"),
        )
        con.execute(
            "INSERT INTO m2_open_positions "
            "(market_id, condition_id, side, outcome, stake, entry_price, "
            "current_price, leg_id, opened_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "m1",
                "c1",
                "BUY",
                "YES",
                100.0000004,
                0.4,
                0.5,
                "l1",
                "2026-07-17T08:00:00+00:00",
            ),
        )
        con.execute(
            "INSERT INTO m2_applied_operations "
            "(operation_id, operation_type, target_id, result_json, applied_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("legacy-close", "close", "m0", "10.0", "2026-07-17T08:00:00Z"),
        )


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


@pytest.mark.parametrize("result", [True, False, 3.25, None])
def test_in_memory_receipt_round_trips_identity_and_result(result) -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)

    assert repository.get_receipt("unknown") is None
    repository.apply("op-1", "close", "m1", lambda state: result)

    receipt = repository.get_receipt("op-1")
    assert receipt == repository_module.OperationReceipt(
        operation_id="op-1",
        operation_type="close",
        target_id="m1",
        result=result,
    )
    with pytest.raises(FrozenInstanceError):
        receipt.target_id = "m2"
    assert repository.get_receipt("op-1").target_id == "m1"


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


@pytest.mark.parametrize("result", [True, False, 3.25, None])
def test_sqlite_receipt_survives_restart_with_original_result_type(
    tmp_path, result
) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    repository.apply("op-1", "close", "m1", lambda state: result)

    restarted = SQLitePositionRepository(path, initial_balance=1000.0)
    receipt = restarted.get_receipt("op-1")

    assert receipt == repository_module.OperationReceipt(
        operation_id="op-1",
        operation_type="close",
        target_id="m1",
        result=result,
    )
    assert type(receipt.result) is type(result)


def test_sqlite_unknown_receipt_is_observational(tmp_path) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)

    assert repository.get_receipt("unknown") is None

    with sqlite3.connect(path) as con:
        operation_count = con.execute(
            "SELECT COUNT(*) FROM m2_applied_operations"
        ).fetchone()[0]
    assert operation_count == 0


def test_sqlite_receipt_lookup_propagates_storage_errors(tmp_path, monkeypatch) -> None:
    repository = SQLitePositionRepository(
        tmp_path / "positions.db", initial_balance=1000.0
    )

    def unavailable() -> sqlite3.Connection:
        raise sqlite3.DatabaseError("storage unavailable")

    monkeypatch.setattr(repository, "_connect", unavailable)

    with pytest.raises(sqlite3.DatabaseError, match="storage unavailable"):
        repository.get_receipt("op-1")


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
            "INSERT INTO m2_account_state "
            "(account_id, snapshot_balance, balance, realized_pnl, "
            "snapshot_balance_micros, balance_micros, realized_pnl_micros, "
            "updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                "corrupt-second-account",
                1.0,
                1.0,
                0.0,
                1_000_000,
                1_000_000,
                0,
                "2026-07-17T08:00:00Z",
            ),
        )

    with pytest.raises(RepositoryStateError, match="exactly one account"):
        repository.load()


def test_sqlite_fresh_schema_stores_authoritative_money_as_integer(tmp_path) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    repository.apply("open:m1", "open", "m1", _open)

    with sqlite3.connect(path) as con:
        account = con.execute(
            "SELECT snapshot_balance_micros, balance_micros, "
            "realized_pnl_micros, typeof(snapshot_balance_micros), "
            "typeof(balance_micros), typeof(realized_pnl_micros) "
            "FROM m2_account_state"
        ).fetchone()
        position = con.execute(
            "SELECT stake_micros, typeof(stake_micros) FROM m2_open_positions"
        ).fetchone()

    assert account == (
        1_000_000_000,
        900_000_000,
        0,
        "integer",
        "integer",
        "integer",
    )
    assert position == (100_000_000, "integer")


def test_sqlite_migrates_phase4_real_state_and_preserves_identity(tmp_path) -> None:
    path = tmp_path / "positions.db"
    _create_phase4_database(path)

    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    state = repository.load()

    assert state.snapshot_balance_money.micros == 1_000_000_000
    assert state.balance_money.micros == 920_000_000
    assert state.realized_pnl_money.micros == 20_000_000
    assert state.open_positions["m1"].stake_money.micros == 100_000_000
    assert repository.get_receipt("legacy-close").result == 10.0

    with sqlite3.connect(path) as con:
        raw = con.execute(
            "SELECT snapshot_balance_micros, balance_micros, "
            "realized_pnl_micros FROM m2_account_state"
        ).fetchone()
        columns = {
            row[1]
            for row in con.execute("PRAGMA table_info(m2_open_positions)").fetchall()
        }
    assert raw == (1_000_000_000, 920_000_000, 20_000_000)
    assert "stake_micros" in columns


def test_sqlite_phase4_migration_is_idempotent_on_restart(tmp_path) -> None:
    path = tmp_path / "positions.db"
    _create_phase4_database(path)
    SQLitePositionRepository(path, initial_balance=1000.0)

    with sqlite3.connect(path) as con:
        before = con.execute(
            "SELECT snapshot_balance_micros, balance_micros, "
            "realized_pnl_micros FROM m2_account_state"
        ).fetchone()

    restarted = SQLitePositionRepository(path, initial_balance=9999.0)

    with sqlite3.connect(path) as con:
        after = con.execute(
            "SELECT snapshot_balance_micros, balance_micros, "
            "realized_pnl_micros FROM m2_account_state"
        ).fetchone()
    assert after == before
    assert restarted.load().snapshot_balance_money.micros == 1_000_000_000


def test_sqlite_invalid_phase4_money_rolls_back_schema_migration(tmp_path) -> None:
    path = tmp_path / "positions.db"
    _create_phase4_database(path, balance=float("inf"))

    with pytest.raises(ValueError, match="finite"):
        SQLitePositionRepository(path, initial_balance=1000.0)

    with sqlite3.connect(path) as con:
        account_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info(m2_account_state)").fetchall()
        }
        position_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info(m2_open_positions)").fetchall()
        }
    assert "balance_micros" not in account_columns
    assert "stake_micros" not in position_columns


def test_sqlite_existing_integer_columns_with_null_authority_fail_closed(
    tmp_path,
) -> None:
    path = tmp_path / "positions.db"
    _create_phase4_database(path)
    with sqlite3.connect(path) as con:
        con.execute(
            "ALTER TABLE m2_account_state ADD COLUMN snapshot_balance_micros INTEGER"
        )
        con.execute("ALTER TABLE m2_account_state ADD COLUMN balance_micros INTEGER")
        con.execute(
            "ALTER TABLE m2_account_state ADD COLUMN realized_pnl_micros INTEGER"
        )
        con.execute("ALTER TABLE m2_open_positions ADD COLUMN stake_micros INTEGER")

    with pytest.raises(RepositoryStateError, match="authoritative money"):
        SQLitePositionRepository(path, initial_balance=1000.0)


def test_sqlite_load_rejects_runtime_corruption_of_integer_authority(
    tmp_path,
) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    with sqlite3.connect(path) as con:
        con.execute(
            "UPDATE m2_account_state SET balance_micros = ?",
            (1.5,),
        )

    with pytest.raises(RepositoryStateError, match="authoritative money"):
        repository.load()


def test_sqlite_state_write_dual_writes_legacy_projection_from_micros(
    tmp_path,
) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1.0)

    def debit(state: PositionState) -> None:
        state.balance = "0.6666665"
        state.realized_pnl = "-0.3333335"

    repository.apply("debit", "legacy-update", "paper", debit)

    with sqlite3.connect(path) as con:
        row = con.execute(
            "SELECT balance_micros, realized_pnl_micros, balance, realized_pnl "
            "FROM m2_account_state"
        ).fetchone()
    assert row == (666_666, -333_334, 0.666666, -0.333334)


@pytest.mark.parametrize(
    "repository_factory",
    [
        lambda path: InMemoryPositionRepository(initial_balance=1000.0),
        lambda path: SQLitePositionRepository(path, initial_balance=1000.0),
    ],
)
def test_money_receipt_round_trips_exact_value(repository_factory, tmp_path) -> None:
    repository = repository_factory(tmp_path / "positions.db")
    expected = Money.from_value("-0.000001")

    assert repository.apply("close:exact", "close", "m1", lambda state: expected) == expected
    assert repository.apply(
        "close:exact",
        "close",
        "m1",
        lambda state: pytest.fail("replay invoked transition"),
    ) == expected
    assert repository.get_receipt("close:exact").result == expected


def test_sqlite_money_receipt_uses_tagged_micro_json(tmp_path) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    repository.apply(
        "close:exact",
        "close",
        "m1",
        lambda state: Money.from_value("10"),
    )

    with sqlite3.connect(path) as con:
        raw = con.execute(
            "SELECT result_json FROM m2_applied_operations WHERE operation_id = ?",
            ("close:exact",),
        ).fetchone()[0]
    assert json.loads(raw) == {"kind": "money", "micros": 10_000_000}


@pytest.mark.parametrize(
    "payload",
    [
        {"kind": "unknown", "micros": 1},
        {"kind": "money", "micros": True},
        {"kind": "money", "micros": 2**63},
        {"kind": "money", "micros": 1, "extra": "forged"},
        1,
        float("nan"),
    ],
)
def test_sqlite_malformed_or_ambiguous_receipt_fails_closed(
    tmp_path, payload
) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO m2_applied_operations "
            "(operation_id, operation_type, target_id, result_json, applied_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("corrupt", "close", "m1", json.dumps(payload), "2026-07-17T08:00:00Z"),
        )

    with pytest.raises(RepositoryStateError, match="receipt"):
        repository.get_receipt("corrupt")


def test_sqlite_invalid_receipt_json_fails_as_repository_state_error(tmp_path) -> None:
    path = tmp_path / "positions.db"
    repository = SQLitePositionRepository(path, initial_balance=1000.0)
    with sqlite3.connect(path) as con:
        con.execute(
            "INSERT INTO m2_applied_operations "
            "(operation_id, operation_type, target_id, result_json, applied_at) "
            "VALUES (?, ?, ?, ?, ?)",
            ("corrupt", "close", "m1", "{", "2026-07-17T08:00:00Z"),
        )

    with pytest.raises(RepositoryStateError, match="receipt"):
        repository.get_receipt("corrupt")
