"""Persistence boundary for M2 paper-account position state."""
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any, Callable, Protocol, TypeAlias


logger = logging.getLogger(__name__)

_ACCOUNT_ID = "paper"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS m2_account_state (
    account_id TEXT PRIMARY KEY,
    snapshot_balance REAL NOT NULL,
    balance REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m2_open_positions (
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
CREATE TABLE IF NOT EXISTS m2_applied_operations (
    operation_id TEXT PRIMARY KEY,
    operation_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    result_json TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
"""
_REQUIRED_COLUMNS = {
    "m2_account_state": {
        "account_id",
        "snapshot_balance",
        "balance",
        "realized_pnl",
        "updated_at",
    },
    "m2_open_positions": {
        "market_id",
        "condition_id",
        "side",
        "outcome",
        "stake",
        "entry_price",
        "current_price",
        "leg_id",
        "opened_at",
    },
    "m2_applied_operations": {
        "operation_id",
        "operation_type",
        "target_id",
        "result_json",
        "applied_at",
    },
}


@dataclass
class PositionState:
    balance: float
    snapshot_balance: float
    realized_pnl: float = 0.0
    open_positions: dict[str, Any] = field(default_factory=dict)


TransitionResult: TypeAlias = bool | float | None
Transition: TypeAlias = Callable[[PositionState], TransitionResult]


class PositionRepository(Protocol):
    def load(self) -> PositionState: ...

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult: ...


class RepositoryStateError(RuntimeError):
    """Durable state violates repository invariants."""


@dataclass(frozen=True)
class AppliedOperation:
    operation_type: str
    target_id: str
    result: TransitionResult


class InMemoryPositionRepository:
    def __init__(self, initial_balance: float) -> None:
        self._state = PositionState(
            balance=initial_balance,
            snapshot_balance=initial_balance,
        )
        self._operations: dict[str, AppliedOperation] = {}

    def load(self) -> PositionState:
        return deepcopy(self._state)

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult:
        applied = self._operations.get(operation_id)
        if applied is not None:
            if (
                applied.operation_type != operation_type
                or applied.target_id != target_id
            ):
                raise ValueError(
                    "operation identity conflict: "
                    f"{operation_id!r} was already used for "
                    f"{applied.operation_type!r}/{applied.target_id!r}"
                )
            return deepcopy(applied.result)

        candidate = deepcopy(self._state)
        result = transition(candidate)
        if result is not None and not isinstance(result, (bool, float)):
            raise TypeError("transition result must be bool, float, or None")

        self._state = candidate
        self._operations[operation_id] = AppliedOperation(
            operation_type=operation_type,
            target_id=target_id,
            result=deepcopy(result),
        )
        return result


class SQLitePositionRepository:
    """Transactional SQLite projection for the M2 paper account."""

    def __init__(
        self,
        db_path: Path,
        initial_balance: float,
        busy_timeout_ms: int = 5000,
    ) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._initial_balance = initial_balance
        self._busy_timeout_ms = busy_timeout_ms
        self._initialize()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def load(self) -> PositionState:
        with self._connect() as con:
            return self._load_state(con)

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult:
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            applied = con.execute(
                "SELECT operation_type, target_id, result_json "
                "FROM m2_applied_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if applied is not None:
                if applied[0] != operation_type or applied[1] != target_id:
                    raise ValueError(
                        "operation identity conflict: "
                        f"{operation_id!r} was already used for "
                        f"{applied[0]!r}/{applied[1]!r}"
                    )
                result = json.loads(applied[2])
                con.commit()
                return result

            state = self._load_state(con)
            result = transition(state)
            if result is not None and not isinstance(result, (bool, float)):
                raise TypeError("transition result must be bool, float, or None")

            now = datetime.now(timezone.utc).isoformat()
            self._write_state(con, state, now)
            con.execute(
                "INSERT INTO m2_applied_operations "
                "(operation_id, operation_type, target_id, result_json, applied_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    operation_id,
                    operation_type,
                    target_id,
                    json.dumps(result),
                    now,
                ),
            )
            con.commit()
            return result
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=self._busy_timeout_ms / 1000,
        )
        con.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _initialize(self) -> None:
        con = self._connect()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("PRAGMA synchronous=NORMAL")
            con.executescript(_SCHEMA)
            self._verify_schema(con)
            con.execute("BEGIN IMMEDIATE")
            rows = con.execute(
                "SELECT snapshot_balance FROM m2_account_state"
            ).fetchall()
            if not rows:
                now = datetime.now(timezone.utc).isoformat()
                con.execute(
                    "INSERT INTO m2_account_state "
                    "(account_id, snapshot_balance, balance, realized_pnl, updated_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (
                        _ACCOUNT_ID,
                        self._initial_balance,
                        self._initial_balance,
                        0.0,
                        now,
                    ),
                )
            elif len(rows) != 1:
                raise RepositoryStateError(
                    "m2_account_state must contain exactly one account row"
                )
            elif float(rows[0][0]) != self._initial_balance:
                logger.warning(
                    "Configured initial balance %.2f differs from durable %.2f; "
                    "durable state wins",
                    self._initial_balance,
                    float(rows[0][0]),
                )
            con.commit()
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _verify_schema(con: sqlite3.Connection) -> None:
        for table, required in _REQUIRED_COLUMNS.items():
            actual = {
                row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()
            }
            if not required.issubset(actual):
                missing = ", ".join(sorted(required - actual))
                raise RepositoryStateError(
                    f"incompatible {table} schema; missing columns: {missing}"
                )

    @staticmethod
    def _load_state(con: sqlite3.Connection) -> PositionState:
        accounts = con.execute(
            "SELECT snapshot_balance, balance, realized_pnl FROM m2_account_state"
        ).fetchall()
        if len(accounts) != 1:
            raise RepositoryStateError(
                "m2_account_state must contain exactly one account row"
            )

        from polyarb.routing.position_tracker import Position

        positions: dict[str, Position] = {}
        rows = con.execute(
            "SELECT market_id, condition_id, side, outcome, stake, entry_price, "
            "current_price, leg_id, opened_at FROM m2_open_positions"
        ).fetchall()
        for row in rows:
            position = Position(
                market_id=row[0],
                condition_id=row[1],
                side=row[2],
                outcome=row[3],
                stake=float(row[4]),
                entry_price=float(row[5]),
                current_price=float(row[6]),
                leg_id=row[7],
                opened_at=datetime.fromisoformat(row[8]),
            )
            positions[position.market_id] = position

        account = accounts[0]
        return PositionState(
            snapshot_balance=float(account[0]),
            balance=float(account[1]),
            realized_pnl=float(account[2]),
            open_positions=positions,
        )

    @staticmethod
    def _write_state(
        con: sqlite3.Connection, state: PositionState, updated_at: str
    ) -> None:
        updated = con.execute(
            "UPDATE m2_account_state SET snapshot_balance = ?, balance = ?, "
            "realized_pnl = ?, updated_at = ?",
            (
                state.snapshot_balance,
                state.balance,
                state.realized_pnl,
                updated_at,
            ),
        )
        if updated.rowcount != 1:
            raise RepositoryStateError(
                "m2_account_state must contain exactly one account row"
            )

        con.execute("DELETE FROM m2_open_positions")
        for market_id, position in state.open_positions.items():
            if market_id != position.market_id:
                raise RepositoryStateError(
                    "open position key must match its market_id"
                )
            con.execute(
                "INSERT INTO m2_open_positions "
                "(market_id, condition_id, side, outcome, stake, entry_price, "
                "current_price, leg_id, opened_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position.market_id,
                    position.condition_id,
                    position.side,
                    position.outcome,
                    position.stake,
                    position.entry_price,
                    position.current_price,
                    position.leg_id,
                    position.opened_at.isoformat(),
                ),
            )
