"""Persistence boundary for M2 paper-account position state."""

from __future__ import annotations

import json
import logging
import math
import sqlite3
from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from polyarb.routing.money import Money
from polyarb.routing.quantity import Quantity

logger = logging.getLogger(__name__)

_ACCOUNT_ID = "paper"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS m2_account_state (
    account_id TEXT PRIMARY KEY,
    snapshot_balance REAL NOT NULL,
    balance REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    snapshot_balance_micros INTEGER NOT NULL,
    balance_micros INTEGER NOT NULL,
    realized_pnl_micros INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m2_open_positions (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    stake REAL NOT NULL,
    stake_micros INTEGER NOT NULL,
    quantity_micros INTEGER NOT NULL,
    cost_basis_micros INTEGER NOT NULL,
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
    request_fingerprint TEXT NOT NULL DEFAULT '',
    applied_at TEXT NOT NULL
);
"""
_REQUIRED_COLUMNS = {
    "m2_account_state": {
        "account_id",
        "snapshot_balance",
        "balance",
        "realized_pnl",
        "snapshot_balance_micros",
        "balance_micros",
        "realized_pnl_micros",
        "updated_at",
    },
    "m2_open_positions": {
        "market_id",
        "condition_id",
        "side",
        "outcome",
        "stake",
        "stake_micros",
        "quantity_micros",
        "cost_basis_micros",
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
        "request_fingerprint",
        "applied_at",
    },
}
_MONEY_COLUMNS = {
    "m2_account_state": {
        "snapshot_balance_micros": "snapshot_balance",
        "balance_micros": "balance",
        "realized_pnl_micros": "realized_pnl",
    },
    "m2_open_positions": {"stake_micros": "stake"},
}
_POSITION_UNIT_COLUMNS = {"quantity_micros", "cost_basis_micros"}
_OPERATION_FINGERPRINT_COLUMN = "request_fingerprint"
_BASE_REQUIRED_COLUMNS = {
    table: (
        required
        - set(_MONEY_COLUMNS.get(table, {}))
        - (_POSITION_UNIT_COLUMNS if table == "m2_open_positions" else set())
        - ({_OPERATION_FINGERPRINT_COLUMN} if table == "m2_applied_operations" else set())
    )
    for table, required in _REQUIRED_COLUMNS.items()
}


@dataclass(init=False)
class PositionState:
    balance_money: Money
    snapshot_balance_money: Money
    realized_pnl_money: Money
    open_positions: dict[str, Any]

    def __init__(
        self,
        balance: int | float | str | Money,
        snapshot_balance: int | float | str | Money,
        realized_pnl: int | float | str | Money = 0.0,
        open_positions: dict[str, Any] | None = None,
    ) -> None:
        self.balance_money = _as_money(balance)
        self.snapshot_balance_money = _as_money(snapshot_balance)
        self.realized_pnl_money = _as_money(realized_pnl)
        self.open_positions = open_positions or {}

    @property
    def balance(self) -> float:
        return self.balance_money.to_float()

    @balance.setter
    def balance(self, value: int | float | str | Money) -> None:
        self.balance_money = _as_money(value)

    @property
    def snapshot_balance(self) -> float:
        return self.snapshot_balance_money.to_float()

    @snapshot_balance.setter
    def snapshot_balance(self, value: int | float | str | Money) -> None:
        self.snapshot_balance_money = _as_money(value)

    @property
    def realized_pnl(self) -> float:
        return self.realized_pnl_money.to_float()

    @realized_pnl.setter
    def realized_pnl(self, value: int | float | str | Money) -> None:
        self.realized_pnl_money = _as_money(value)


def _as_money(value: int | float | str | Money) -> Money:
    return value if isinstance(value, Money) else Money.from_value(value)


@dataclass(frozen=True, slots=True)
class SettlementReceipt:
    """Exact venue-confirmed cash result stored for one immutable fill."""

    gross_cash: Money
    fee: Money
    net_cash: Money
    realized_pnl: Money
    source: str = "venue-confirmed"

    def __post_init__(self) -> None:
        for field_name in ("gross_cash", "fee", "net_cash", "realized_pnl"):
            if not isinstance(getattr(self, field_name), Money):
                raise TypeError(f"settlement {field_name} must be Money")
        if self.source != "venue-confirmed":
            raise ValueError("settlement receipt source must be venue-confirmed")
        if self.gross_cash.micros < 0 or self.fee.micros < 0:
            raise ValueError("settlement gross cash and fee must be non-negative")
        if self.fee.micros > self.gross_cash.micros:
            raise ValueError("settlement fee cannot exceed gross cash")
        if self.net_cash != self.gross_cash - self.fee:
            raise ValueError("settlement net cash must equal gross cash minus fee")


type TransitionResult = bool | float | Money | SettlementReceipt | None
type Transition = Callable[[PositionState], TransitionResult]


@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    operation_type: str
    target_id: str
    result: TransitionResult
    request_fingerprint: str = ""


class PositionRepository(Protocol):
    def load(self) -> PositionState: ...

    def get_receipt(self, operation_id: str) -> OperationReceipt | None: ...

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
        request_fingerprint: str = "",
    ) -> TransitionResult: ...


class RepositoryStateError(RuntimeError):
    """Durable state violates repository invariants."""


def _validate_transition_result(result: object) -> TransitionResult:
    if result is None or type(result) is bool or isinstance(result, (Money, SettlementReceipt)):
        return result
    if type(result) is float:
        if not math.isfinite(result):
            raise ValueError("transition float result must be finite")
        return result
    raise TypeError("transition result must be bool, float, Money, SettlementReceipt, or None")


def _validate_request_fingerprint(value: object) -> str:
    if not isinstance(value, str):
        raise TypeError("request fingerprint must be a string")
    return value


def _encode_result(result: TransitionResult) -> str:
    validated = _validate_transition_result(result)
    payload: object
    if isinstance(validated, Money):
        payload = {"kind": "money", "micros": validated.micros}
    elif isinstance(validated, SettlementReceipt):
        payload = {
            "kind": "settlement",
            "source": validated.source,
            "gross_micros": validated.gross_cash.micros,
            "fee_micros": validated.fee.micros,
            "net_micros": validated.net_cash.micros,
            "pnl_micros": validated.realized_pnl.micros,
        }
    else:
        payload = validated
    return json.dumps(payload, allow_nan=False, separators=(",", ":"))


def _decode_result(raw: str) -> TransitionResult:
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, TypeError) as exc:
        raise RepositoryStateError("invalid receipt JSON") from exc
    if payload is None or type(payload) is bool:
        return payload
    if type(payload) is float:
        if not math.isfinite(payload):
            raise RepositoryStateError("receipt float must be finite")
        return payload
    if isinstance(payload, dict) and set(payload) == {"kind", "micros"}:
        if payload["kind"] != "money" or type(payload["micros"]) is not int:
            raise RepositoryStateError("invalid tagged money receipt")
        try:
            return Money(payload["micros"])
        except (TypeError, OverflowError) as exc:
            raise RepositoryStateError("invalid tagged money receipt") from exc
    settlement_keys = {
        "kind",
        "source",
        "gross_micros",
        "fee_micros",
        "net_micros",
        "pnl_micros",
    }
    if isinstance(payload, dict) and payload.get("kind") == "settlement":
        if set(payload) != settlement_keys:
            raise RepositoryStateError("invalid settlement receipt keys")
        money_keys = ("gross_micros", "fee_micros", "net_micros", "pnl_micros")
        if payload["source"] != "venue-confirmed" or any(
            type(payload[key]) is not int for key in money_keys
        ):
            raise RepositoryStateError("invalid settlement receipt values")
        try:
            return SettlementReceipt(
                gross_cash=Money(payload["gross_micros"]),
                fee=Money(payload["fee_micros"]),
                net_cash=Money(payload["net_micros"]),
                realized_pnl=Money(payload["pnl_micros"]),
            )
        except (TypeError, ValueError, OverflowError) as exc:
            raise RepositoryStateError("invalid settlement receipt values") from exc
    raise RepositoryStateError("unsupported receipt result type")


class InMemoryPositionRepository:
    def __init__(self, initial_balance: float) -> None:
        self._state = PositionState(
            balance=initial_balance,
            snapshot_balance=initial_balance,
        )
        self._operations: dict[str, OperationReceipt] = {}

    def load(self) -> PositionState:
        return deepcopy(self._state)

    def get_receipt(self, operation_id: str) -> OperationReceipt | None:
        receipt = self._operations.get(operation_id)
        return deepcopy(receipt)

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
        request_fingerprint: str = "",
    ) -> TransitionResult:
        fingerprint = _validate_request_fingerprint(request_fingerprint)
        applied = self._operations.get(operation_id)
        if applied is not None:
            if (
                applied.operation_type != operation_type
                or applied.target_id != target_id
                or applied.request_fingerprint != fingerprint
            ):
                raise ValueError(
                    "operation identity conflict: "
                    f"{operation_id!r} was already used for "
                    f"{applied.operation_type!r}/{applied.target_id!r}"
                )
            return deepcopy(applied.result)

        candidate = deepcopy(self._state)
        result = _validate_transition_result(transition(candidate))

        self._state = candidate
        self._operations[operation_id] = OperationReceipt(
            operation_id=operation_id,
            operation_type=operation_type,
            target_id=target_id,
            result=deepcopy(result),
            request_fingerprint=fingerprint,
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
        self._initial_balance_money = Money.from_value(initial_balance)
        self._busy_timeout_ms = busy_timeout_ms
        self._initialize()

    @property
    def db_path(self) -> Path:
        return self._db_path

    def load(self) -> PositionState:
        with self._connect() as con:
            return self._load_state(con)

    def get_receipt(self, operation_id: str) -> OperationReceipt | None:
        with self._connect() as con:
            row = con.execute(
                "SELECT operation_type, target_id, result_json, request_fingerprint "
                "FROM m2_applied_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
        if row is None:
            return None
        return OperationReceipt(
            operation_id=operation_id,
            operation_type=row[0],
            target_id=row[1],
            result=_decode_result(row[2]),
            request_fingerprint=row[3],
        )

    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
        request_fingerprint: str = "",
    ) -> TransitionResult:
        fingerprint = _validate_request_fingerprint(request_fingerprint)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            applied = con.execute(
                "SELECT operation_type, target_id, result_json, request_fingerprint "
                "FROM m2_applied_operations WHERE operation_id = ?",
                (operation_id,),
            ).fetchone()
            if applied is not None:
                if (
                    applied[0] != operation_type
                    or applied[1] != target_id
                    or applied[3] != fingerprint
                ):
                    raise ValueError(
                        "operation identity conflict: "
                        f"{operation_id!r} was already used for "
                        f"{applied[0]!r}/{applied[1]!r}"
                    )
                result = _decode_result(applied[2])
                con.commit()
                return result

            state = self._load_state(con)
            result = _validate_transition_result(transition(state))

            now = datetime.now(UTC).isoformat()
            self._write_state(con, state, now)
            con.execute(
                "INSERT INTO m2_applied_operations "
                "(operation_id, operation_type, target_id, result_json, "
                "request_fingerprint, applied_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    operation_id,
                    operation_type,
                    target_id,
                    _encode_result(result),
                    fingerprint,
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
            self._verify_schema(con, _BASE_REQUIRED_COLUMNS)
            con.execute("BEGIN IMMEDIATE")
            self._migrate_money_schema(con)
            self._migrate_position_units(con)
            self._migrate_operation_fingerprints(con)
            self._verify_schema(con, _REQUIRED_COLUMNS)
            rows = con.execute("SELECT snapshot_balance_micros FROM m2_account_state").fetchall()
            if not rows:
                now = datetime.now(UTC).isoformat()
                con.execute(
                    "INSERT INTO m2_account_state "
                    "(account_id, snapshot_balance, balance, realized_pnl, "
                    "snapshot_balance_micros, balance_micros, "
                    "realized_pnl_micros, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        _ACCOUNT_ID,
                        self._initial_balance_money.to_float(),
                        self._initial_balance_money.to_float(),
                        0.0,
                        self._initial_balance_money.micros,
                        self._initial_balance_money.micros,
                        0,
                        now,
                    ),
                )
            elif len(rows) != 1:
                raise RepositoryStateError("m2_account_state must contain exactly one account row")
            elif int(rows[0][0]) != self._initial_balance_money.micros:
                logger.warning(
                    "Configured initial balance %.2f differs from durable %.2f; durable state wins",
                    self._initial_balance_money.to_float(),
                    Money(int(rows[0][0])).to_float(),
                )
            con.commit()
        except Exception:
            if con.in_transaction:
                con.rollback()
            raise
        finally:
            con.close()

    @staticmethod
    def _verify_schema(con: sqlite3.Connection, required_columns: dict[str, set[str]]) -> None:
        for table, required in required_columns.items():
            actual = {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
            if not required.issubset(actual):
                missing = ", ".join(sorted(required - actual))
                raise RepositoryStateError(
                    f"incompatible {table} schema; missing columns: {missing}"
                )

    @classmethod
    def _migrate_money_schema(cls, con: sqlite3.Connection) -> None:
        existing = {
            table: {row[1] for row in con.execute(f"PRAGMA table_info({table})").fetchall()}
            for table in _MONEY_COLUMNS
        }
        all_money_columns_existed = all(
            set(columns).issubset(existing[table]) for table, columns in _MONEY_COLUMNS.items()
        )

        for table, columns in _MONEY_COLUMNS.items():
            for money_column in columns:
                if money_column not in existing[table]:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {money_column} INTEGER")

        if not all_money_columns_existed:
            account_rows = con.execute(
                "SELECT account_id, snapshot_balance, balance, realized_pnl FROM m2_account_state"
            ).fetchall()
            for account_id, snapshot, balance, realized in account_rows:
                con.execute(
                    "UPDATE m2_account_state SET snapshot_balance_micros = ?, "
                    "balance_micros = ?, realized_pnl_micros = ? "
                    "WHERE account_id = ?",
                    (
                        Money.from_value(snapshot).micros,
                        Money.from_value(balance).micros,
                        Money.from_value(realized).micros,
                        account_id,
                    ),
                )
            position_rows = con.execute("SELECT market_id, stake FROM m2_open_positions").fetchall()
            for market_id, stake in position_rows:
                con.execute(
                    "UPDATE m2_open_positions SET stake_micros = ? WHERE market_id = ?",
                    (Money.from_value(stake).micros, market_id),
                )

        cls._validate_money_authority(con)

    @staticmethod
    def _validate_money_authority(con: sqlite3.Connection) -> None:
        for table, columns in _MONEY_COLUMNS.items():
            invalid_predicate = " OR ".join(f"typeof({column}) != 'integer'" for column in columns)
            invalid_count = con.execute(
                f"SELECT COUNT(*) FROM {table} WHERE {invalid_predicate}"
            ).fetchone()[0]
            if invalid_count:
                raise RepositoryStateError(f"{table} contains invalid authoritative money values")

    @classmethod
    def _migrate_position_units(cls, con: sqlite3.Connection) -> None:
        existing = {row[1] for row in con.execute("PRAGMA table_info(m2_open_positions)")}
        present = _POSITION_UNIT_COLUMNS & existing
        if present and present != _POSITION_UNIT_COLUMNS:
            raise RepositoryStateError("m2_open_positions contains partial v3 position authority")

        if not present:
            for column in sorted(_POSITION_UNIT_COLUMNS):
                con.execute(f"ALTER TABLE m2_open_positions ADD COLUMN {column} INTEGER")

            refund = Money(0)
            rows = con.execute(
                "SELECT market_id, side, stake_micros, entry_price FROM m2_open_positions"
            ).fetchall()
            for market_id, side, legacy_quantity_micros, entry_price in rows:
                quantity = Quantity(int(legacy_quantity_micros))
                cost_basis = Money.collateral_for(quantity, entry_price, side)
                con.execute(
                    "UPDATE m2_open_positions SET quantity_micros = ?, "
                    "cost_basis_micros = ? WHERE market_id = ?",
                    (quantity.micros, cost_basis.micros, market_id),
                )
                legacy_reserve = Money(quantity.micros)
                refund = refund + legacy_reserve - cost_basis

            if refund.micros:
                account = con.execute("SELECT balance_micros FROM m2_account_state").fetchall()
                if len(account) != 1:
                    raise RepositoryStateError(
                        "m2_account_state must contain exactly one account row"
                    )
                repaired_balance = Money(int(account[0][0])) + refund
                con.execute(
                    "UPDATE m2_account_state SET balance_micros = ?, balance = ?",
                    (repaired_balance.micros, repaired_balance.to_float()),
                )

        cls._validate_position_authority(con)

    @staticmethod
    def _validate_position_authority(con: sqlite3.Connection) -> None:
        invalid_count = con.execute(
            "SELECT COUNT(*) FROM m2_open_positions "
            "WHERE typeof(quantity_micros) != 'integer' "
            "OR typeof(cost_basis_micros) != 'integer'"
        ).fetchone()[0]
        if invalid_count:
            raise RepositoryStateError("m2_open_positions contains invalid v3 position authority")

    @classmethod
    def _migrate_operation_fingerprints(cls, con: sqlite3.Connection) -> None:
        columns = {row[1] for row in con.execute("PRAGMA table_info(m2_applied_operations)")}
        if _OPERATION_FINGERPRINT_COLUMN not in columns:
            con.execute(
                "ALTER TABLE m2_applied_operations "
                "ADD COLUMN request_fingerprint TEXT NOT NULL DEFAULT ''"
            )
        invalid_count = con.execute(
            "SELECT COUNT(*) FROM m2_applied_operations WHERE typeof(request_fingerprint) != 'text'"
        ).fetchone()[0]
        if invalid_count:
            raise RepositoryStateError(
                "m2_applied_operations contains invalid request fingerprints"
            )

    @staticmethod
    def _load_state(con: sqlite3.Connection) -> PositionState:
        SQLitePositionRepository._validate_money_authority(con)
        SQLitePositionRepository._validate_position_authority(con)
        accounts = con.execute(
            "SELECT snapshot_balance_micros, balance_micros, "
            "realized_pnl_micros FROM m2_account_state"
        ).fetchall()
        if len(accounts) != 1:
            raise RepositoryStateError("m2_account_state must contain exactly one account row")

        from polyarb.routing.position_tracker import Position

        positions: dict[str, Position] = {}
        rows = con.execute(
            "SELECT market_id, condition_id, side, outcome, quantity_micros, "
            "cost_basis_micros, entry_price, current_price, leg_id, opened_at "
            "FROM m2_open_positions"
        ).fetchall()
        for row in rows:
            position = Position(
                market_id=row[0],
                condition_id=row[1],
                side=row[2],
                outcome=row[3],
                quantity=Quantity(int(row[4])),
                cost_basis=Money(int(row[5])),
                entry_price=float(row[6]),
                current_price=float(row[7]),
                leg_id=row[8],
                opened_at=datetime.fromisoformat(row[9]),
            )
            expected_cost = Money.collateral_for(
                position.quantity_value,
                position.entry_price,
                position.side,
            )
            if position.cost_basis_money != expected_cost:
                raise RepositoryStateError(
                    f"position {position.market_id!r} cost basis is inconsistent"
                )
            positions[position.market_id] = position

        account = accounts[0]
        return PositionState(
            snapshot_balance=Money(int(account[0])),
            balance=Money(int(account[1])),
            realized_pnl=Money(int(account[2])),
            open_positions=positions,
        )

    @staticmethod
    def _write_state(con: sqlite3.Connection, state: PositionState, updated_at: str) -> None:
        updated = con.execute(
            "UPDATE m2_account_state SET snapshot_balance = ?, balance = ?, "
            "realized_pnl = ?, snapshot_balance_micros = ?, balance_micros = ?, "
            "realized_pnl_micros = ?, updated_at = ?",
            (
                state.snapshot_balance_money.to_float(),
                state.balance_money.to_float(),
                state.realized_pnl_money.to_float(),
                state.snapshot_balance_money.micros,
                state.balance_money.micros,
                state.realized_pnl_money.micros,
                updated_at,
            ),
        )
        if updated.rowcount != 1:
            raise RepositoryStateError("m2_account_state must contain exactly one account row")

        con.execute("DELETE FROM m2_open_positions")
        for market_id, position in state.open_positions.items():
            if market_id != position.market_id:
                raise RepositoryStateError("open position key must match its market_id")
            con.execute(
                "INSERT INTO m2_open_positions "
                "(market_id, condition_id, side, outcome, stake, stake_micros, "
                "quantity_micros, cost_basis_micros, entry_price, current_price, "
                "leg_id, opened_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    position.market_id,
                    position.condition_id,
                    position.side,
                    position.outcome,
                    position.quantity_value.to_float(),
                    position.quantity_value.micros,
                    position.quantity_value.micros,
                    position.cost_basis_money.micros,
                    position.entry_price,
                    position.current_price,
                    position.leg_id,
                    position.opened_at.isoformat(),
                ),
            )
