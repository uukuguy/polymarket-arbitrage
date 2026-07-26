"""Durable, atomic storage for known-universe neg-risk quote runs.

The snapshot pipeline remains responsible for producing ``snapshots`` and
``markets``.  This focused sidecar reads the latest snapshot's eligible
membership and records one all-or-nothing terminal quote set per collection.
"""

from __future__ import annotations

import hashlib
import json
import math
import sqlite3
import time
from collections.abc import Callable
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

QUOTE_RUN_LEASE_MS = 30_000


class QuoteRunStateError(RuntimeError):
    """A quote-run state transition would violate the atomic-run contract."""


class QuoteRunBusyError(QuoteRunStateError):
    """Another quote run is still collecting in the database."""


class QuoteRunLeaseLostError(QuoteRunStateError):
    """A run no longer owns a live lease for a terminal state transition."""

    def __init__(self, run_id: int | None = None) -> None:
        detail = "quote-run-lease-lost"
        if run_id is not None:
            detail += f": quote run {run_id} no longer owns a live collection lease"
        super().__init__(detail)


class QuoteUniverseUnavailableError(RuntimeError):
    """No completed source snapshot currently backs the published market view."""

    def __init__(self, detail: str = "quote-universe-unavailable") -> None:
        super().__init__(detail)


class QuoteProjectionIntegrityError(QuoteUniverseUnavailableError):
    """A complete run cannot be proven against one atomic source view."""

    def __init__(self) -> None:
        super().__init__("quote-projection-integrity-unavailable")


@dataclass(frozen=True)
class UniverseLeg:
    neg_risk_market_id: str
    market_id: str
    condition_id: str
    slug: str | None
    yes_token_id: str
    event_id: str = ""
    membership_hash: str = ""


@dataclass(frozen=True)
class GroupRejection:
    group_id: str
    quality: str
    reason: str


@dataclass(frozen=True)
class VerifiedQuoteUniverse:
    snapshot_id: int
    taken_at_ms: int
    universe_hash: str
    legs: tuple[UniverseLeg, ...]
    rejections: tuple[GroupRejection, ...]


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
    event_id: str = ""
    membership_hash: str = ""


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
    universe_hash: str = ""
    source_truth_hash: str = ""


@dataclass(frozen=True)
class CompleteQuoteProjection:
    run_id: int
    universe_snapshot_id: int
    universe_taken_at_ms: int
    quoted_at_ms: int
    requested_token_count: int
    successful_response_count: int
    run_legs: tuple[UniverseLeg, ...]
    quotes: tuple[PersistedQuote, ...]
    source_universe: VerifiedQuoteUniverse
    universe_hash: str
    source_truth_hash: str


class NegRiskQuoteStore:
    """SQLite API for one complete-or-failed neg-risk quote collection."""

    def __init__(self, db_path: Path | str, *, now_ms: Callable[[], int] | None = None) -> None:
        self._db_path = Path(db_path)
        self._now_ms = now_ms or _wall_clock_ms

    def current_time_ms(self) -> int:
        """Return this store's authoritative, injectable lease clock."""
        return self._now_ms()

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(self._db_path, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _begin_immediate(self, con: sqlite3.Connection) -> None:
        """Serialize a lease transition before taking its authoritative time."""
        con.execute("BEGIN IMMEDIATE")

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
                "SELECT m.neg_risk_market_id,m.market_id,m.condition_id,m.slug,"
                "m.yes_token_id,COALESCE(t.event_id,''),COALESCE(t.membership_hash,'') "
                "FROM markets m LEFT JOIN neg_risk_group_truth t "
                "ON t.snapshot_id=m.snapshot_id "
                "AND t.neg_risk_market_id=m.neg_risk_market_id "
                "WHERE m.snapshot_id = ? AND m.active = 1 AND m.closed = 0 "
                "AND m.neg_risk_market_id IS NOT NULL AND m.neg_risk_market_id != '' "
                "AND m.yes_token_id IS NOT NULL AND m.yes_token_id != '' "
                "ORDER BY m.neg_risk_market_id,m.market_id",
                (snapshot[0],),
            ).fetchall()
        finally:
            con.close()
        return (
            int(snapshot[0]),
            int(snapshot[1]),
            tuple(UniverseLeg(*row) for row in rows),
        )

    def latest_verified_universe(self) -> VerifiedQuoteUniverse:
        """Select only standard groups backed by complete published source truth."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            con.execute("BEGIN")
            snapshot = _latest_completed_published_snapshot(con)
            if snapshot is None:
                raise QuoteUniverseUnavailableError()
            return _verified_universe_for_snapshot(
                con,
                snapshot_id=int(snapshot[0]),
                taken_at_ms=int(snapshot[1]),
            )
        finally:
            con.close()

    def verified_universe_for_snapshot(
        self,
        snapshot_id: int,
    ) -> VerifiedQuoteUniverse:
        """Rebuild verified truth for one exact complete published snapshot."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            con.execute("BEGIN")
            snapshot = con.execute(
                "SELECT s.id,s.taken_at_ms FROM snapshots s "
                "JOIN snapshot_source_coverage c "
                "ON c.snapshot_id=s.id AND c.completed=1 "
                "WHERE s.id=? AND s.market_view_published=1",
                (snapshot_id,),
            ).fetchone()
            if snapshot is None:
                raise QuoteUniverseUnavailableError()
            return _verified_universe_for_snapshot(
                con,
                snapshot_id=int(snapshot[0]),
                taken_at_ms=int(snapshot[1]),
            )
        finally:
            con.close()

    def begin_verified_run(
        self,
        universe: VerifiedQuoteUniverse,
        *,
        quoted_at_ms: int,
    ) -> int:
        """Acquire a run lease only if the verified universe is still current."""
        if universe.universe_hash != _universe_hash(universe.legs):
            raise QuoteRunStateError("verified universe hash does not match its legs")
        return self._begin_run(
            universe_snapshot_id=universe.snapshot_id,
            universe_taken_at_ms=universe.taken_at_ms,
            legs=universe.legs,
            quoted_at_ms=quoted_at_ms,
            expected_universe_hash=universe.universe_hash,
            expected_source_truth_hash=_source_truth_hash(universe),
        )

    def begin_run(
        self,
        *,
        universe_snapshot_id: int,
        universe_taken_at_ms: int,
        legs: tuple[UniverseLeg, ...],
        quoted_at_ms: int,
    ) -> int:
        """Create a run only for the exact current verified universe."""
        return self._begin_run(
            universe_snapshot_id=universe_snapshot_id,
            universe_taken_at_ms=universe_taken_at_ms,
            legs=legs,
            quoted_at_ms=quoted_at_ms,
            expected_universe_hash=None,
            expected_source_truth_hash=None,
        )

    def _begin_run(
        self,
        *,
        universe_snapshot_id: int,
        universe_taken_at_ms: int,
        legs: tuple[UniverseLeg, ...],
        quoted_at_ms: int,
        expected_universe_hash: str | None,
        expected_source_truth_hash: str | None,
    ) -> int:
        """Revalidate verified truth while atomically acquiring the DB lease."""
        requested_legs = _deduplicate_legs(legs)
        universe_hash = _universe_hash(requested_legs)
        if expected_universe_hash is not None and universe_hash != expected_universe_hash:
            raise QuoteRunStateError("requested legs do not match verified universe hash")
        con = self._connect()
        try:
            self._begin_immediate(con)
            try:
                now_ms = self.current_time_ms()
                con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'failed', "
                    "failure_reason = 'collector-lease-expired' "
                    "WHERE status = 'collecting' "
                    "AND COALESCE(lease_expires_at_ms, 0) <= ?",
                    (now_ms,),
                )
                busy = con.execute(
                    "SELECT id FROM neg_risk_quote_runs WHERE status = 'collecting' LIMIT 1"
                ).fetchone()
                if busy is not None:
                    raise QuoteRunBusyError(f"collecting quote run already exists: {int(busy[0])}")
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
                latest = _latest_completed_published_snapshot(con)
                if latest is None or int(latest[0]) != universe_snapshot_id:
                    raise QuoteRunStateError(
                        "verified universe snapshot is no longer the latest published truth"
                    )
                verified = _verified_universe_for_snapshot(
                    con,
                    snapshot_id=universe_snapshot_id,
                    taken_at_ms=universe_taken_at_ms,
                )
                snapshot_legs = verified.legs
                if verified.universe_hash != universe_hash:
                    raise QuoteRunStateError(
                        "requested legs do not match verified snapshot membership"
                    )
                if _legs_by_token(requested_legs) != _legs_by_token(snapshot_legs):
                    raise QuoteRunStateError(
                        "requested legs do not match verified snapshot membership"
                    )
                source_truth_hash = _source_truth_hash(verified)
                if (
                    expected_source_truth_hash is not None
                    and source_truth_hash != expected_source_truth_hash
                ):
                    raise QuoteRunStateError(
                        "requested source truth does not match verified snapshot"
                    )
                cur = con.execute(
                    "INSERT INTO neg_risk_quote_runs("
                    "universe_snapshot_id, universe_taken_at_ms, universe_hash, "
                    "source_truth_hash, quoted_at_ms, requested_token_count, "
                    "lease_expires_at_ms, status"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, 'collecting')",
                    (
                        universe_snapshot_id,
                        universe_taken_at_ms,
                        universe_hash,
                        source_truth_hash,
                        quoted_at_ms,
                        len(requested_legs),
                        now_ms + QUOTE_RUN_LEASE_MS,
                    ),
                )
                run_id = int(cur.lastrowid)
                con.executemany(
                    "INSERT INTO neg_risk_quote_run_legs("
                    "quote_run_id, neg_risk_market_id, event_id, membership_hash, "
                    "market_id, condition_id, slug, yes_token_id"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            run_id,
                            leg.neg_risk_market_id,
                            leg.event_id,
                            leg.membership_hash,
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

    def renew_run_lease(self, run_id: int) -> None:
        """Extend a still-live collecting run lease, never reviving an expired one."""
        con = self._connect()
        try:
            self._begin_immediate(con)
            try:
                now_ms = self.current_time_ms()
                cur = con.execute(
                    "UPDATE neg_risk_quote_runs SET lease_expires_at_ms = ? "
                    "WHERE id = ? AND status = 'collecting' "
                    "AND lease_expires_at_ms > ?",
                    (now_ms + QUOTE_RUN_LEASE_MS, run_id, now_ms),
                )
                if cur.rowcount != 1:
                    raise QuoteRunLeaseLostError(run_id)
                con.execute("COMMIT")
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def record_terminal_quotes(
        self,
        run_id: int,
        quotes: tuple[PersistedQuote, ...],
    ) -> None:
        """Append observations only while this run still owns its live lease."""
        _validate_quotes(quotes)
        con = self._connect()
        try:
            self._begin_immediate(con)
            try:
                now_ms = self.current_time_ms()
                _require_live_collecting(con, run_id, now_ms=now_ms)
                requested = {
                    str(row[4]): UniverseLeg(*row)
                    for row in con.execute(
                        "SELECT neg_risk_market_id, market_id, condition_id, slug, yes_token_id, "
                        "event_id, membership_hash "
                        "FROM neg_risk_quote_run_legs WHERE quote_run_id = ?",
                        (run_id,),
                    )
                }
                for quote in quotes:
                    requested_leg = requested.get(quote.yes_token_id)
                    if requested_leg is None:
                        raise ValueError("terminal quote token is not requested by this run")
                    quote_leg = UniverseLeg(
                        quote.neg_risk_market_id,
                        quote.market_id,
                        quote.condition_id,
                        quote.slug,
                        quote.yes_token_id,
                        quote.event_id,
                        quote.membership_hash,
                    )
                    if quote_leg != requested_leg:
                        raise ValueError("terminal quote identity does not match requested leg")
                con.executemany(
                    "INSERT INTO neg_risk_quotes("
                    "quote_run_id, neg_risk_market_id, event_id, membership_hash, "
                    "market_id, condition_id, slug, yes_token_id, terminal_state, "
                    "best_ask_price, best_ask_size"
                    ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    [
                        (
                            run_id,
                            quote.neg_risk_market_id,
                            quote.event_id,
                            quote.membership_hash,
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

    def complete_run(
        self,
        run_id: int,
        *,
        completed_at_ms: int,
        successful_response_count: int,
    ) -> QuoteRun:
        """Atomically promote a terminal run with its accepted CLOB response count."""
        if isinstance(successful_response_count, bool) or not isinstance(
            successful_response_count, int
        ):
            raise ValueError("successful_response_count must be an integer")
        con = self._connect()
        try:
            self._begin_immediate(con)
            try:
                now_ms = self.current_time_ms()
                _require_live_collecting(con, run_id, now_ms=now_ms)
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
                if not 0 <= successful_response_count <= requested:
                    raise ValueError(
                        "successful_response_count must be between 0 and requested_token_count"
                    )
                con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'complete', "
                    "successful_response_count = ?, completed_at_ms = ? WHERE id = ?",
                    (successful_response_count, completed_at_ms, run_id),
                )
                row = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms, universe_hash, source_truth_hash "
                    "FROM neg_risk_quote_runs WHERE id = ?",
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
                "completed_at_ms, universe_hash, source_truth_hash "
                "FROM neg_risk_quote_runs r "
                "WHERE r.status = 'complete' AND length(r.universe_hash)=64 "
                "AND length(r.source_truth_hash)=64 "
                "AND EXISTS ("
                "SELECT 1 FROM snapshots s JOIN snapshot_source_coverage c "
                "ON c.snapshot_id=s.id AND c.completed=1 "
                "WHERE s.id=r.universe_snapshot_id AND s.market_view_published=1"
                ") "
                "AND r.requested_token_count=("
                "SELECT COUNT(*) FROM neg_risk_quote_run_legs l WHERE l.quote_run_id=r.id"
                ") AND r.requested_token_count=("
                "SELECT COUNT(*) FROM neg_risk_quotes q WHERE q.quote_run_id=r.id"
                ") AND NOT EXISTS ("
                "SELECT 1 FROM neg_risk_quote_run_legs l WHERE l.quote_run_id=r.id "
                "AND (trim(l.event_id)='' OR trim(l.membership_hash)='')"
                ") AND NOT EXISTS ("
                "SELECT 1 FROM neg_risk_quotes q WHERE q.quote_run_id=r.id "
                "AND (trim(q.event_id)='' OR trim(q.membership_hash)='')"
                ") ORDER BY r.quoted_at_ms DESC, r.id DESC LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        return _quote_run_from_row(row) if row is not None else None

    def latest_complete_projection(self) -> CompleteQuoteProjection | None:
        """Prove one complete run and its source chain in one read transaction."""
        con = sqlite3.connect(
            f"file:{self._db_path}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            con.execute("BEGIN")
            run_rows = con.execute(
                "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                "requested_token_count, successful_response_count, status, failure_reason, "
                "completed_at_ms, universe_hash, source_truth_hash "
                "FROM neg_risk_quote_runs "
                "WHERE status='complete' "
                "ORDER BY quoted_at_ms DESC,id DESC"
            ).fetchall()
            for run_row in run_rows:
                run = _quote_run_from_row(run_row)
                if not run.source_truth_hash:
                    continue
                leg_rows = con.execute(
                    "SELECT neg_risk_market_id,market_id,condition_id,slug,yes_token_id,"
                    "event_id,membership_hash FROM neg_risk_quote_run_legs "
                    "WHERE quote_run_id=? "
                    "ORDER BY neg_risk_market_id,market_id,yes_token_id",
                    (run.run_id,),
                ).fetchall()
                quote_rows = con.execute(
                    "SELECT neg_risk_market_id,market_id,condition_id,slug,yes_token_id,"
                    "terminal_state,best_ask_price,best_ask_size,event_id,membership_hash "
                    "FROM neg_risk_quotes WHERE quote_run_id=? "
                    "ORDER BY neg_risk_market_id,market_id,yes_token_id",
                    (run.run_id,),
                ).fetchall()
                has_blank_provenance, all_provenance_blank = (
                    _blank_provenance_state(leg_rows, quote_rows)
                )
                if has_blank_provenance and not all_provenance_blank:
                    raise QuoteProjectionIntegrityError()
                if not run.universe_hash:
                    raise QuoteProjectionIntegrityError()
                run_legs = tuple(UniverseLeg(*row) for row in leg_rows)
                quotes = tuple(PersistedQuote(*row) for row in quote_rows)
                source_row = con.execute(
                    "SELECT s.id,s.taken_at_ms FROM snapshots s "
                    "JOIN snapshot_source_coverage c "
                    "ON c.snapshot_id=s.id AND c.completed=1 "
                    "WHERE s.id=? AND s.market_view_published=1",
                    (run.universe_snapshot_id,),
                ).fetchone()
                if source_row is None:
                    raise QuoteProjectionIntegrityError()
                source_universe = _verified_universe_for_snapshot(
                    con,
                    snapshot_id=int(source_row[0]),
                    taken_at_ms=int(source_row[1]),
                )
                if all_provenance_blank:
                    raise QuoteProjectionIntegrityError()
                if any(
                    rejection.quality == "incomplete-source"
                    for rejection in source_universe.rejections
                ):
                    raise QuoteUniverseUnavailableError("incomplete-source-unavailable")
                _validate_complete_projection(
                    con,
                    run=run,
                    original_run_row=run_row,
                    run_legs=run_legs,
                    quotes=quotes,
                    source_universe=source_universe,
                )
                return CompleteQuoteProjection(
                    run_id=run.run_id,
                    universe_snapshot_id=run.universe_snapshot_id,
                    universe_taken_at_ms=run.universe_taken_at_ms,
                    quoted_at_ms=run.quoted_at_ms,
                    requested_token_count=run.requested_token_count,
                    successful_response_count=run.successful_response_count,
                    run_legs=run_legs,
                    quotes=quotes,
                    source_universe=source_universe,
                    universe_hash=run.universe_hash,
                    source_truth_hash=run.source_truth_hash,
                )
            return None
        except (TypeError, ValueError, OverflowError) as error:
            raise QuoteProjectionIntegrityError() from error
        finally:
            con.close()


def _blank_provenance_state(
    leg_rows: list[tuple[object, ...]],
    quote_rows: list[tuple[object, ...]],
) -> tuple[bool, bool]:
    provenance = tuple(
        (str(row[-2]).strip(), str(row[-1]).strip())
        for row in (*leg_rows, *quote_rows)
    )
    has_blank = any(not event_id or not membership_hash for event_id, membership_hash in provenance)
    all_blank = bool(provenance) and all(
        not event_id and not membership_hash
        for event_id, membership_hash in provenance
    )
    return has_blank, all_blank


def _validate_complete_projection(
    con: sqlite3.Connection,
    *,
    run: QuoteRun,
    original_run_row: tuple[object, ...],
    run_legs: tuple[UniverseLeg, ...],
    quotes: tuple[PersistedQuote, ...],
    source_universe: VerifiedQuoteUniverse,
) -> None:
    rechecked_run_row = con.execute(
        "SELECT id,universe_snapshot_id,universe_taken_at_ms,quoted_at_ms,"
        "requested_token_count,successful_response_count,status,failure_reason,"
        "completed_at_ms,universe_hash,source_truth_hash "
        "FROM neg_risk_quote_runs WHERE id=?",
        (run.run_id,),
    ).fetchone()
    if rechecked_run_row != original_run_row:
        raise QuoteProjectionIntegrityError()
    persisted_counts = con.execute(
        "SELECT "
        "(SELECT COUNT(*) FROM neg_risk_quote_run_legs WHERE quote_run_id=?),"
        "(SELECT COUNT(*) FROM neg_risk_quotes WHERE quote_run_id=?)",
        (run.run_id, run.run_id),
    ).fetchone()
    if persisted_counts is None:
        raise QuoteProjectionIntegrityError()
    leg_count, quote_count = (int(persisted_counts[0]), int(persisted_counts[1]))
    if (
        run.status != "complete"
        or run.failure_reason is not None
        or run.completed_at_ms is None
        or run.universe_snapshot_id != source_universe.snapshot_id
        or run.universe_taken_at_ms != source_universe.taken_at_ms
        or run.requested_token_count != leg_count
        or run.requested_token_count != quote_count
        or leg_count != len(run_legs)
        or quote_count != len(quotes)
        or not 0 <= run.successful_response_count <= run.requested_token_count
    ):
        raise QuoteProjectionIntegrityError()
    _validate_projection_legs(run_legs)
    _validate_quotes(quotes)
    source_by_token = _projection_identity_by_token(source_universe.legs)
    run_by_token = _projection_identity_by_token(run_legs)
    quote_by_token = _projection_quote_identity_by_token(quotes)
    if (
        source_by_token != run_by_token
        or run_by_token != quote_by_token
        or run.universe_hash != source_universe.universe_hash
        or run.universe_hash != _universe_hash(run_legs)
        or run.source_truth_hash != _source_truth_hash(source_universe)
    ):
        raise QuoteProjectionIntegrityError()


def _validate_projection_legs(legs: tuple[UniverseLeg, ...]) -> None:
    if any(
        not value.strip()
        for leg in legs
        for value in (
            leg.neg_risk_market_id,
            leg.event_id,
            leg.membership_hash,
            leg.market_id,
            leg.condition_id,
            leg.yes_token_id,
        )
    ):
        raise QuoteProjectionIntegrityError()
    _projection_identity_by_token(legs)


def _projection_identity_by_token(
    legs: tuple[UniverseLeg, ...],
) -> dict[str, tuple[str, ...]]:
    identities = {
        leg.yes_token_id: (
            leg.neg_risk_market_id,
            leg.event_id,
            leg.membership_hash,
            leg.market_id,
            leg.condition_id,
            leg.yes_token_id,
        )
        for leg in legs
    }
    if len(identities) != len(legs) or len(set(identities.values())) != len(legs):
        raise QuoteProjectionIntegrityError()
    return identities


def _projection_quote_identity_by_token(
    quotes: tuple[PersistedQuote, ...],
) -> dict[str, tuple[str, ...]]:
    identities = {
        quote.yes_token_id: (
            quote.neg_risk_market_id,
            quote.event_id,
            quote.membership_hash,
            quote.market_id,
            quote.condition_id,
            quote.yes_token_id,
        )
        for quote in quotes
    }
    if len(identities) != len(quotes) or len(set(identities.values())) != len(quotes):
        raise QuoteProjectionIntegrityError()
    return identities


def _latest_completed_published_snapshot(
    con: sqlite3.Connection,
) -> tuple[object, object] | None:
    return con.execute(
        "SELECT s.id,s.taken_at_ms FROM snapshots s "
        "JOIN snapshot_source_coverage c ON c.snapshot_id=s.id AND c.completed=1 "
        "WHERE s.market_view_published=1 "
        "ORDER BY s.id DESC LIMIT 1"
    ).fetchone()


def _verified_universe_for_snapshot(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    taken_at_ms: int,
) -> VerifiedQuoteUniverse:
    truth_rows = con.execute(
        "SELECT event_id,neg_risk_market_id,neg_risk_type,expected_member_count,"
        "active_named_count,membership_hash,quality,reason "
        "FROM neg_risk_group_truth WHERE snapshot_id=? "
        "ORDER BY neg_risk_market_id",
        (snapshot_id,),
    ).fetchall()
    market_rows = con.execute(
        "SELECT event_id,neg_risk_market_id,market_id,condition_id,slug,yes_token_id,"
        "active,closed,incomplete FROM markets "
        "WHERE snapshot_id=? AND neg_risk_market_id IS NOT NULL "
        "AND neg_risk_market_id!='' ORDER BY neg_risk_market_id,market_id",
        (snapshot_id,),
    ).fetchall()
    membership_rows = con.execute(
        "SELECT event_id,neg_risk_market_id,market_id,member_kind,active,closed "
        "FROM event_market_memberships WHERE snapshot_id=? "
        "ORDER BY neg_risk_market_id,market_id",
        (snapshot_id,),
    ).fetchall()
    markets_by_group: dict[str, list[tuple[object, ...]]] = {}
    for row in market_rows:
        markets_by_group.setdefault(str(row[1]), []).append(row)
    memberships_by_group: dict[str, list[tuple[object, ...]]] = {}
    for row in membership_rows:
        memberships_by_group.setdefault(str(row[1]), []).append(row)

    legs: list[UniverseLeg] = []
    rejections: list[GroupRejection] = []
    for truth in truth_rows:
        event_id = str(truth[0])
        group_id = str(truth[1])
        neg_risk_type = str(truth[2])
        expected_member_count = int(truth[3])
        active_named_count = int(truth[4])
        membership_hash = str(truth[5])
        quality = str(truth[6])
        reason = str(truth[7]) if truth[7] is not None else "neg-risk-group-not-supported"
        if neg_risk_type != "standard" or quality != "complete-supported":
            rejections.append(GroupRejection(group_id, quality, reason))
            continue

        group_markets = markets_by_group.get(group_id, [])
        group_memberships = memberships_by_group.get(group_id, [])
        market_ids = {str(row[2]) for row in group_markets}
        membership_ids = {str(row[2]) for row in group_memberships}
        membership_matches = (
            bool(event_id.strip())
            and bool(membership_hash.strip())
            and expected_member_count == active_named_count
            and len(group_markets) == expected_member_count
            and len(group_memberships) == expected_member_count
            and market_ids == membership_ids
            and all(
                str(row[0]) == event_id
                and str(row[1]) == group_id
                and row[3] == "named"
                and int(row[4]) == 1
                and int(row[5]) == 0
                for row in group_memberships
            )
            and all(
                str(row[0]) == event_id
                and str(row[1]) == group_id
                and int(row[6]) == 1
                and int(row[7]) == 0
                and int(row[8]) == 0
                and isinstance(row[5], str)
                and bool(row[5].strip())
                for row in group_markets
            )
        )
        if not membership_matches:
            rejections.append(
                GroupRejection(group_id, quality, "membership-market-mismatch")
            )
            continue
        legs.extend(
            UniverseLeg(
                neg_risk_market_id=group_id,
                market_id=str(row[2]),
                condition_id=str(row[3]),
                slug=str(row[4]) if row[4] is not None else None,
                yes_token_id=str(row[5]),
                event_id=event_id,
                membership_hash=membership_hash,
            )
            for row in group_markets
        )

    ordered_legs = tuple(
        sorted(
            legs,
            key=lambda leg: (
                leg.neg_risk_market_id,
                leg.membership_hash,
                leg.market_id,
                leg.yes_token_id,
            ),
        )
    )
    return VerifiedQuoteUniverse(
        snapshot_id=snapshot_id,
        taken_at_ms=taken_at_ms,
        universe_hash=_universe_hash(ordered_legs),
        legs=ordered_legs,
        rejections=tuple(rejections),
    )


def _universe_hash(legs: tuple[UniverseLeg, ...]) -> str:
    identity = sorted(
        (
            leg.neg_risk_market_id,
            leg.membership_hash,
            leg.market_id,
            leg.yes_token_id,
        )
        for leg in legs
    )
    canonical = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _source_truth_hash(universe: VerifiedQuoteUniverse) -> str:
    identity = [
        universe.universe_hash,
        sorted(
            (rejection.group_id, rejection.quality, rejection.reason)
            for rejection in universe.rejections
        ),
    ]
    canonical = json.dumps(identity, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _deduplicate_legs(legs: tuple[UniverseLeg, ...]) -> tuple[UniverseLeg, ...]:
    by_token: dict[str, UniverseLeg] = {}
    for leg in legs:
        if not leg.yes_token_id:
            raise ValueError("universe leg yes_token_id must be non-empty")
        existing = by_token.get(leg.yes_token_id)
        if existing is not None and existing != leg:
            raise ValueError(
                f"duplicate yes_token_id maps to inconsistent identity: {leg.yes_token_id!r}"
            )
        by_token[leg.yes_token_id] = leg
    return tuple(by_token.values())


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


def _require_live_collecting(con: sqlite3.Connection, run_id: int, *, now_ms: int) -> None:
    row = con.execute(
        "SELECT status, lease_expires_at_ms FROM neg_risk_quote_runs WHERE id = ?",
        (run_id,),
    ).fetchone()
    if row is None:
        raise QuoteRunStateError(f"quote run {run_id} does not exist")
    if row[0] != "collecting":
        raise QuoteRunStateError(f"quote run {run_id} is not collecting")
    if row[1] is None or int(row[1]) <= now_ms:
        raise QuoteRunLeaseLostError(run_id)


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
        universe_hash=str(row[9]),
        source_truth_hash=str(row[10]),
    )


def _wall_clock_ms() -> int:
    return time.time_ns() // 1_000_000
