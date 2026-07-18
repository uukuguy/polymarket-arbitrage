"""Durable, atomic storage for known-universe neg-risk quote runs.

The snapshot pipeline remains responsible for producing ``snapshots`` and
``markets``.  This focused sidecar reads the latest snapshot's eligible
membership and records one all-or-nothing terminal quote set per collection.
"""

from __future__ import annotations

import math
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from polyarb.storage.sqlite_store import _rollback_without_masking

_TERMINAL_STATES = frozenset(
    {
        "executable",
        "missing-book",
        "missing-ask",
        "invalid-ask-price",
        "invalid-ask-size",
        "collector-error",
    }
)


class QuoteRunStateError(RuntimeError):
    """A quote-run state transition would violate the atomic-run contract."""


class QuoteRunBusyError(QuoteRunStateError):
    """Another quote run is still collecting in the database."""


@dataclass(frozen=True)
class UniverseLeg:
    neg_risk_market_id: str
    market_id: str
    condition_id: str
    slug: str | None
    yes_token_id: str


@dataclass(frozen=True)
class PersistedQuote:
    neg_risk_market_id: str
    market_id: str
    condition_id: str
    slug: str | None
    yes_token_id: str
    terminal_state: str
    best_ask_price: float | None
    best_ask_size: float | None


@dataclass(frozen=True)
class QuoteRun:
    run_id: int
    universe_snapshot_id: int
    universe_taken_at_ms: int
    quoted_at_ms: int
    requested_token_count: int
    successful_response_count: int
    status: str
    failure_reason: str | None
    completed_at_ms: int | None


@dataclass(frozen=True)
class CompleteQuoteProjection:
    run_id: int
    universe_snapshot_id: int
    universe_taken_at_ms: int
    quoted_at_ms: int
    requested_token_count: int
    successful_response_count: int
    quotes: tuple[PersistedQuote, ...]


class NegRiskQuoteStore:
    """SQLite API for one complete-or-failed neg-risk quote collection."""

    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def latest_universe(self) -> tuple[int, int, tuple[UniverseLeg, ...]] | None:
        """Return membership from exactly the newest snapshot, if one exists."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            snapshot = con.execute(
                "SELECT id, taken_at_ms FROM snapshots WHERE id = (SELECT MAX(id) FROM snapshots)"
            ).fetchone()
            if snapshot is None:
                return None
            rows = con.execute(
                "SELECT neg_risk_market_id, market_id, condition_id, slug, yes_token_id "
                "FROM markets WHERE snapshot_id = ? AND active = 1 AND closed = 0 "
                "AND neg_risk_market_id IS NOT NULL AND neg_risk_market_id != '' "
                "AND yes_token_id IS NOT NULL AND yes_token_id != '' "
                "ORDER BY neg_risk_market_id, market_id",
                (snapshot[0],),
            ).fetchall()
        finally:
            con.close()
        return (
            int(snapshot[0]),
            int(snapshot[1]),
            tuple(UniverseLeg(*row) for row in rows),
        )

    def begin_run(
        self,
        *,
        universe_snapshot_id: int,
        universe_taken_at_ms: int,
        legs: tuple[UniverseLeg, ...],
        quoted_at_ms: int,
    ) -> int:
        """Create a collecting run after atomically acquiring the DB lock."""
        requested_legs = _deduplicate_legs(legs)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                busy = con.execute(
                    "SELECT id FROM neg_risk_quote_runs WHERE status = 'collecting' LIMIT 1"
                ).fetchone()
                if busy is not None:
                    raise QuoteRunBusyError(
                        f"collecting quote run already exists: {int(busy[0])}"
                    )
                snapshot = con.execute(
                    "SELECT taken_at_ms FROM snapshots WHERE id = ?",
                    (universe_snapshot_id,),
                ).fetchone()
                if snapshot is None:
                    raise QuoteRunStateError(
                        f"universe snapshot {universe_snapshot_id} does not exist"
                    )
                if int(snapshot[0]) != universe_taken_at_ms:
                    raise QuoteRunStateError(
                        "universe_taken_at_ms does not match the stored snapshot"
                    )
                snapshot_legs = _snapshot_legs(con, universe_snapshot_id)
                if _legs_by_token(requested_legs) != _legs_by_token(snapshot_legs):
                    raise QuoteRunStateError(
                        "requested legs do not match snapshot membership"
                    )
                cur = con.execute(
                    "INSERT INTO neg_risk_quote_runs("
                    "universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, status"
                    ") VALUES (?, ?, ?, ?, 'collecting')",
                    (
                        universe_snapshot_id,
                        universe_taken_at_ms,
                        quoted_at_ms,
                        len(requested_legs),
                    ),
                )
                run_id = int(cur.lastrowid)
                con.executemany(
                    "INSERT INTO neg_risk_quote_run_legs("
                    "quote_run_id, neg_risk_market_id, market_id, condition_id, slug, yes_token_id"
                    ") VALUES (?, ?, ?, ?, ?, ?)",
                    [
                        (
                            run_id,
                            leg.neg_risk_market_id,
                            leg.market_id,
                            leg.condition_id,
                            leg.slug,
                            leg.yes_token_id,
                        )
                        for leg in requested_legs
                    ],
                )
                con.execute("COMMIT")
                return run_id
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def record_terminal_quotes(
        self, run_id: int, quotes: tuple[PersistedQuote, ...]
    ) -> None:
        """Append terminal observations for a collecting run without replacement."""
        _validate_quotes(quotes)
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                _require_collecting(con, run_id)
                requested = {
                    str(row[4]): UniverseLeg(*row)
                    for row in con.execute(
                        "SELECT neg_risk_market_id, market_id, condition_id, slug, yes_token_id "
                        "FROM neg_risk_quote_run_legs WHERE quote_run_id = ?",
                        (run_id,),
                    )
                }
                for quote in quotes:
                    requested_leg = requested.get(quote.yes_token_id)
                    if requested_leg is None:
                        raise ValueError(
                            "terminal quote token is not requested by this run"
                        )
                    quote_leg = UniverseLeg(
                        quote.neg_risk_market_id,
                        quote.market_id,
                        quote.condition_id,
                        quote.slug,
                        quote.yes_token_id,
                    )
                    if quote_leg != requested_leg:
                        raise ValueError(
                            "terminal quote identity does not match requested leg"
                        )
                con.executemany(
                    "INSERT INTO neg_risk_quotes("
                    "quote_run_id, neg_risk_market_id, market_id, condition_id, slug, "
                    "yes_token_id, terminal_state, best_ask_price, best_ask_size"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            run_id,
                            quote.neg_risk_market_id,
                            quote.market_id,
                            quote.condition_id,
                            quote.slug,
                            quote.yes_token_id,
                            quote.terminal_state,
                            quote.best_ask_price,
                            quote.best_ask_size,
                        )
                        for quote in quotes
                    ],
                )
                con.execute("COMMIT")
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def complete_run(self, run_id: int, *, completed_at_ms: int) -> QuoteRun:
        """Atomically promote a fully terminal collecting run to complete."""
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                _require_collecting(con, run_id)
                requested = int(
                    con.execute(
                        "SELECT COUNT(*) FROM neg_risk_quote_run_legs WHERE quote_run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                )
                persisted = int(
                    con.execute(
                        "SELECT COUNT(*) FROM neg_risk_quotes WHERE quote_run_id = ?",
                        (run_id,),
                    ).fetchone()[0]
                )
                if persisted != requested:
                    raise QuoteRunStateError(
                        "cannot complete quote run: "
                        f"requested {requested} terminal rows, found {persisted}"
                    )
                successful = int(
                    con.execute(
                        "SELECT COUNT(*) FROM neg_risk_quotes "
                        "WHERE quote_run_id = ? AND terminal_state = 'executable'",
                        (run_id,),
                    ).fetchone()[0]
                )
                con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'complete', "
                    "successful_response_count = ?, completed_at_ms = ? WHERE id = ?",
                    (successful, completed_at_ms, run_id),
                )
                row = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms FROM neg_risk_quote_runs WHERE id = ?",
                    (run_id,),
                ).fetchone()
                con.execute("COMMIT")
                return _quote_run_from_row(row)
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def fail_run(self, run_id: int, *, failure_reason: str) -> None:
        """Transition only a collecting run to failed, preserving complete runs."""
        if not isinstance(failure_reason, str) or not failure_reason:
            raise ValueError("failure_reason must be a non-empty string")
        con = self._connect()
        try:
            con.execute("BEGIN IMMEDIATE")
            try:
                cur = con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'failed', failure_reason = ? "
                    "WHERE id = ? AND status = 'collecting'",
                    (failure_reason, run_id),
                )
                if cur.rowcount != 1:
                    raise QuoteRunStateError(
                        f"quote run {run_id} is not collecting and cannot fail"
                    )
                con.execute("COMMIT")
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def latest_complete_run(self) -> QuoteRun | None:
        """Return the newest complete run's metadata, without its terminal rows."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            row = con.execute(
                "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                "requested_token_count, successful_response_count, status, failure_reason, "
                "completed_at_ms FROM neg_risk_quote_runs WHERE status = 'complete' "
                "ORDER BY quoted_at_ms DESC, id DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        return _quote_run_from_row(row) if row is not None else None

    def latest_complete_projection(self) -> CompleteQuoteProjection | None:
        """Load metadata and every terminal row from exactly one complete run."""
        run = self.latest_complete_run()
        if run is None:
            return None
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            rows = con.execute(
                "SELECT neg_risk_market_id, market_id, condition_id, slug, yes_token_id, "
                "terminal_state, best_ask_price, best_ask_size FROM neg_risk_quotes "
                "WHERE quote_run_id = ? ORDER BY neg_risk_market_id, market_id, yes_token_id",
                (run.run_id,),
            ).fetchall()
        finally:
            con.close()
        return CompleteQuoteProjection(
            run_id=run.run_id,
            universe_snapshot_id=run.universe_snapshot_id,
            universe_taken_at_ms=run.universe_taken_at_ms,
            quoted_at_ms=run.quoted_at_ms,
            requested_token_count=run.requested_token_count,
            successful_response_count=run.successful_response_count,
            quotes=tuple(PersistedQuote(*row) for row in rows),
        )


def _deduplicate_legs(legs: tuple[UniverseLeg, ...]) -> tuple[UniverseLeg, ...]:
    by_token: dict[str, UniverseLeg] = {}
    for leg in legs:
        if not leg.yes_token_id:
            raise ValueError("universe leg yes_token_id must be non-empty")
        existing = by_token.get(leg.yes_token_id)
        if existing is not None and existing != leg:
            raise ValueError(
                "duplicate yes_token_id maps to inconsistent identity: "
                f"{leg.yes_token_id!r}"
            )
        by_token[leg.yes_token_id] = leg
    return tuple(by_token.values())


def _snapshot_legs(
    con: sqlite3.Connection, universe_snapshot_id: int
) -> tuple[UniverseLeg, ...]:
    rows = con.execute(
        "SELECT neg_risk_market_id, market_id, condition_id, slug, yes_token_id "
        "FROM markets WHERE snapshot_id = ? AND active = 1 AND closed = 0 "
        "AND neg_risk_market_id IS NOT NULL AND neg_risk_market_id != '' "
        "AND yes_token_id IS NOT NULL AND yes_token_id != '' "
        "ORDER BY neg_risk_market_id, market_id",
        (universe_snapshot_id,),
    ).fetchall()
    try:
        return _deduplicate_legs(tuple(UniverseLeg(*row) for row in rows))
    except ValueError as error:
        raise QuoteRunStateError("snapshot membership contains inconsistent token IDs") from error


def _legs_by_token(legs: tuple[UniverseLeg, ...]) -> dict[str, UniverseLeg]:
    return {leg.yes_token_id: leg for leg in legs}


def _validate_quotes(quotes: tuple[PersistedQuote, ...]) -> None:
    token_ids: set[str] = set()
    for quote in quotes:
        if not quote.yes_token_id:
            raise ValueError("persisted quote yes_token_id must be non-empty")
        if quote.yes_token_id in token_ids:
            raise ValueError("terminal quote token IDs must be unique")
        token_ids.add(quote.yes_token_id)
        if quote.terminal_state not in _TERMINAL_STATES:
            raise ValueError(f"invalid terminal_state: {quote.terminal_state!r}")
        if quote.terminal_state == "executable":
            if not _is_valid_executable_value(quote.best_ask_price, lower=0, upper=1):
                raise ValueError("executable quote price must be finite and in (0, 1]")
            if not _is_valid_executable_value(quote.best_ask_size, lower=0):
                raise ValueError("executable quote size must be positive and finite")
        elif quote.best_ask_price is not None or quote.best_ask_size is not None:
            raise ValueError("non-executable quote price and size must be null")


def _is_valid_executable_value(
    value: float | None, *, lower: float, upper: float | None = None
) -> bool:
    if value is None or not isinstance(value, (int, float)) or isinstance(value, bool):
        return False
    if not math.isfinite(value) or value <= lower:
        return False
    return upper is None or value <= upper


def _require_collecting(con: sqlite3.Connection, run_id: int) -> None:
    row = con.execute(
        "SELECT status FROM neg_risk_quote_runs WHERE id = ?", (run_id,)
    ).fetchone()
    if row is None:
        raise QuoteRunStateError(f"quote run {run_id} does not exist")
    if row[0] != "collecting":
        raise QuoteRunStateError(f"quote run {run_id} is not collecting")


def _quote_run_from_row(row: tuple[object, ...]) -> QuoteRun:
    return QuoteRun(
        run_id=int(row[0]),
        universe_snapshot_id=int(row[1]),
        universe_taken_at_ms=int(row[2]),
        quoted_at_ms=int(row[3]),
        requested_token_count=int(row[4]),
        successful_response_count=int(row[5]),
        status=str(row[6]),
        failure_reason=str(row[7]) if row[7] is not None else None,
        completed_at_ms=int(row[8]) if row[8] is not None else None,
    )
