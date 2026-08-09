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
from dataclasses import dataclass, replace
from pathlib import Path

from polyarb.storage.sqlite_store import (
    SQLITE_BUSY_TIMEOUT_S,
    StructureGenerationReadError,
    _comparison_receipt_digest,
    _rollback_without_masking,
    resolve_structure_read_context,
    structure_read_transaction,
)

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

# A complete-universe quote collection shares one vCPU with the lower-priority
# snapshot child in production.  The lease must survive a long SDK parse/GIL
# window while remaining below the canonical 300-second quote freshness SLA.
QUOTE_RUN_LEASE_MS = 180_000


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
    # Non-empty only when the immutable generation projection was authenticated
    # by the exact sealed comparison receipt inside the same read transaction.
    structure_receipt_digest: str = ""
    structure_revision: int = 0
    structure_mode: str = "legacy"


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

    def __init__(
        self,
        db_path: Path | str,
        *,
        now_ms: Callable[[], int] | None = None,
        structure_generation_read_mode: str = "legacy",
    ) -> None:
        self._db_path = Path(db_path)
        self._now_ms = now_ms or _wall_clock_ms
        self._structure_generation_read_mode = structure_generation_read_mode

    def current_time_ms(self) -> int:
        """Return this store's authoritative, injectable lease clock."""
        return self._now_ms()

    def start_collection_attempt(self, *, started_at_ms: int | None = None) -> int:
        started = self.current_time_ms() if started_at_ms is None else started_at_ms
        con = self._connect()
        try:
            self._begin_immediate(con)
            # A previous parent can die before terminalizing its attempt. A
            # checkpoint older than the absolute child limit cannot still own
            # a live child and is closed before admitting the successor.
            con.execute(
                "UPDATE neg_risk_quote_attempts SET checkpoint_at_ms=?,phase='failed',"
                "outcome='failed',failure_kind='parent-orphaned' "
                "WHERE outcome='collecting' AND checkpoint_at_ms<=?",
                (started, max(0, started - 120_000)),
            )
            # Failure-only periods never reach success-side run retention.
            # Bound terminal attempt evidence on every admission instead.
            con.execute(
                "DELETE FROM neg_risk_quote_attempts WHERE id IN ("
                "SELECT id FROM neg_risk_quote_attempts WHERE outcome IN ('complete','failed') "
                "AND id NOT IN (SELECT id FROM neg_risk_quote_attempts "
                "ORDER BY started_at_ms DESC,id DESC LIMIT 1000) "
                "ORDER BY started_at_ms,id LIMIT 20)"
            )
            cur = con.execute(
                "INSERT INTO neg_risk_quote_attempts("
                "started_at_ms,checkpoint_at_ms,phase,outcome) "
                "VALUES (?,?,'universe','collecting')",
                (started, started),
            )
            con.execute("COMMIT")
            return int(cur.lastrowid)
        except Exception:
            _rollback_without_masking(con)
            raise
        finally:
            con.close()

    def checkpoint_collection_attempt(
        self,
        attempt_id: int,
        *,
        phase: str,
        checkpoint_at_ms: int | None = None,
        quote_run_id: int | None = None,
        target_count: int | None = None,
        structure_receipt_digest: str | None = None,
        phase_timings: dict[str, int] | None = None,
        failure_kind: str | None = None,
    ) -> None:
        checkpoint = self.current_time_ms() if checkpoint_at_ms is None else checkpoint_at_ms
        outcome = (
            "complete" if phase == "complete" else "failed" if phase == "failed" else "collecting"
        )
        con = self._connect()
        try:
            cur = con.execute(
                "UPDATE neg_risk_quote_attempts SET checkpoint_at_ms=?,phase=?,outcome=?,"
                "quote_run_id=COALESCE(?,quote_run_id),"
                "quote_run_identity=COALESCE(?,quote_run_identity),"
                "target_count=COALESCE(?,target_count),"
                "structure_receipt_digest=COALESCE(?,structure_receipt_digest),"
                "phase_timings_json=COALESCE(?,phase_timings_json),"
                "failure_kind=? WHERE id=? AND outcome='collecting'",
                (
                    checkpoint,
                    phase,
                    outcome,
                    quote_run_id,
                    quote_run_id,
                    target_count,
                    structure_receipt_digest,
                    (
                        json.dumps(phase_timings, sort_keys=True, separators=(",", ":"))
                        if phase_timings is not None
                        else None
                    ),
                    failure_kind,
                    attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise QuoteRunStateError("quote collection attempt is not active")
        finally:
            con.close()

    def latest_collection_attempt(self) -> dict[str, object] | None:
        with sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True) as con:
            row = con.execute(
                "SELECT id,started_at_ms,checkpoint_at_ms,phase,outcome,"
                "COALESCE(quote_run_identity,quote_run_id),"
                "target_count,structure_receipt_digest,phase_timings_json,failure_kind "
                "FROM neg_risk_quote_attempts ORDER BY started_at_ms DESC,id DESC LIMIT 1"
            ).fetchone()
        if row is None:
            return None
        return {
            "id": int(row[0]),
            "started_at_ms": int(row[1]),
            "checkpoint_at_ms": int(row[2]),
            "phase": str(row[3]),
            "outcome": str(row[4]),
            "quote_run_id": int(row[5]) if row[5] is not None else None,
            "target_count": int(row[6]) if row[6] is not None else None,
            "structure_receipt_digest": row[7],
            "phase_timings": json.loads(str(row[8])),
            "failure_kind": row[9],
        }

    def _connect(self) -> sqlite3.Connection:
        con = sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=SQLITE_BUSY_TIMEOUT_S,
        )
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA foreign_keys=ON")
        return con

    def _begin_immediate(self, con: sqlite3.Connection) -> None:
        """Serialize a lease transition before taking its authoritative time."""
        con.execute("BEGIN IMMEDIATE")

    def latest_universe(self) -> tuple[int, int, tuple[UniverseLeg, ...]] | None:
        """Return membership from exactly the newest snapshot, if one exists."""
        try:
            with structure_read_transaction(
                self._db_path,
                mode=self._structure_generation_read_mode,
                legacy_latest_snapshot=True,
            ) as read:
                rows = read.connection.execute(
                    "SELECT m.neg_risk_market_id,m.market_id,m.condition_id,m.slug,"
                    "m.yes_token_id,COALESCE(t.event_id,''),COALESCE(t.membership_hash,'') "
                    f"FROM {read.table('markets')} m LEFT JOIN {read.table('group_truth')} t "
                    "ON t.snapshot_id=m.snapshot_id "
                    "AND t.neg_risk_market_id=m.neg_risk_market_id "
                    "WHERE m.snapshot_id = ? AND m.active = 1 AND m.closed = 0 "
                    "AND m.neg_risk_market_id IS NOT NULL AND m.neg_risk_market_id != '' "
                    "AND m.yes_token_id IS NOT NULL AND m.yes_token_id != '' "
                    "ORDER BY m.neg_risk_market_id,m.market_id",
                    (read.snapshot_id,),
                ).fetchall()
                snapshot_id = read.snapshot_id
                taken_at_ms = read.taken_at_ms
        except StructureGenerationReadError:
            return None
        return (
            snapshot_id,
            taken_at_ms,
            tuple(UniverseLeg(*row) for row in rows),
        )

    def latest_verified_universe(self) -> VerifiedQuoteUniverse:
        """Select only standard groups backed by complete published source truth."""
        try:
            with structure_read_transaction(
                self._db_path,
                mode=self._structure_generation_read_mode,
            ) as read:
                universe = _verified_universe_for_snapshot(
                    read.connection,
                    snapshot_id=read.snapshot_id,
                    taken_at_ms=read.taken_at_ms,
                    truth_table=read.table("group_truth"),
                    market_table=read.table("markets"),
                    membership_table=read.table("memberships"),
                )
                if read.mode == "generation":
                    return _bind_generation_projection_receipt(
                        read.connection,
                        universe,
                    )
                return _bind_legacy_projection_receipt(read.connection, universe)
        except StructureGenerationReadError as error:
            raise QuoteUniverseUnavailableError() from error

    def verified_universe_for_snapshot(
        self,
        snapshot_id: int,
    ) -> VerifiedQuoteUniverse:
        """Rebuild verified truth for one exact complete published snapshot."""
        try:
            with structure_read_transaction(
                self._db_path,
                mode=self._structure_generation_read_mode,
                snapshot_id=snapshot_id,
            ) as read:
                return _verified_universe_for_snapshot(
                    read.connection,
                    snapshot_id=read.snapshot_id,
                    taken_at_ms=read.taken_at_ms,
                    truth_table=read.table("group_truth"),
                    market_table=read.table("markets"),
                    membership_table=read.table("memberships"),
                )
        except StructureGenerationReadError as error:
            raise QuoteUniverseUnavailableError(str(error)) from error

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
            expected_structure_receipt_digest=universe.structure_receipt_digest,
            expected_structure_revision=universe.structure_revision,
            expected_structure_mode=universe.structure_mode,
            expected_rejections=universe.rejections,
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
            expected_structure_receipt_digest=None,
            expected_structure_revision=None,
            expected_structure_mode=None,
            expected_rejections=None,
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
        expected_structure_receipt_digest: str | None,
        expected_structure_revision: int | None,
        expected_structure_mode: str | None,
        expected_rejections: tuple[GroupRejection, ...] | None,
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
                try:
                    structure = resolve_structure_read_context(
                        con,
                        mode=self._structure_generation_read_mode,
                    )
                except StructureGenerationReadError as error:
                    raise QuoteRunStateError(str(error)) from error
                if structure.snapshot_id != universe_snapshot_id:
                    raise QuoteRunStateError(
                        "verified universe snapshot is no longer the latest published truth"
                    )
                if structure.mode == "generation":
                    source_truth_hash = _require_generation_projection_receipt(
                        con,
                        snapshot_id=universe_snapshot_id,
                        universe_hash=universe_hash,
                        source_truth_hash=expected_source_truth_hash,
                        receipt_digest=expected_structure_receipt_digest,
                    )
                    snapshot_legs = requested_legs
                elif expected_structure_receipt_digest is not None:
                    source_truth_hash = _require_legacy_projection_receipt(
                        con,
                        snapshot_id=universe_snapshot_id,
                        taken_at_ms=universe_taken_at_ms,
                        universe_hash=universe_hash,
                        source_truth_hash=expected_source_truth_hash,
                        source_revision=expected_structure_revision,
                        receipt_digest=expected_structure_receipt_digest,
                    )
                    snapshot_legs = requested_legs
                else:
                    verified = _verified_universe_for_snapshot(
                        con,
                        snapshot_id=universe_snapshot_id,
                        taken_at_ms=universe_taken_at_ms,
                        truth_table=structure.table("group_truth"),
                        market_table=structure.table("markets"),
                        membership_table=structure.table("memberships"),
                    )
                    snapshot_legs = verified.legs
                    if verified.universe_hash != universe_hash:
                        raise QuoteRunStateError(
                            "requested legs do not match verified snapshot membership"
                        )
                    source_truth_hash = _source_truth_hash(verified)
                if _legs_by_token(requested_legs) != _legs_by_token(snapshot_legs):
                    raise QuoteRunStateError(
                        "requested legs do not match verified snapshot membership"
                    )
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
                if (
                    expected_structure_receipt_digest is not None
                    and expected_structure_revision is not None
                    and expected_structure_mode is not None
                    and expected_rejections is not None
                ):
                    rejections_json = _rejections_json(expected_rejections)
                    receipt_digest = _quote_source_receipt_digest(
                        quote_run_id=run_id,
                        universe_snapshot_id=universe_snapshot_id,
                        universe_taken_at_ms=universe_taken_at_ms,
                        source_mode=expected_structure_mode,
                        source_revision=expected_structure_revision,
                        projection_receipt_digest=expected_structure_receipt_digest,
                        source_rejections_json=rejections_json,
                        universe_hash=universe_hash,
                        source_truth_hash=source_truth_hash,
                        leg_quote_digest="",
                    )
                    con.execute(
                        "INSERT INTO neg_risk_quote_source_receipts("
                        "quote_run_id,source_mode,source_revision,"
                        "projection_receipt_digest,source_rejections_json,"
                        "leg_quote_digest,receipt_digest) VALUES (?,?,?,?,?,?,?)",
                        (
                            run_id,
                            expected_structure_mode,
                            expected_structure_revision,
                            expected_structure_receipt_digest,
                            rejections_json,
                            "",
                            receipt_digest,
                        ),
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
        publish_current_generation: bool = False,
    ) -> QuoteRun:
        """Atomically certify a run and optionally switch the current feed pointer."""
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
                source_receipt = con.execute(
                    "SELECT source_mode,source_revision,projection_receipt_digest,"
                    "source_rejections_json FROM neg_risk_quote_source_receipts "
                    "WHERE quote_run_id=?",
                    (run_id,),
                ).fetchone()
                if source_receipt is not None:
                    leg_rows = con.execute(
                        "SELECT neg_risk_market_id,market_id,condition_id,slug,yes_token_id,"
                        "event_id,membership_hash FROM neg_risk_quote_run_legs "
                        "WHERE quote_run_id=? ORDER BY yes_token_id",
                        (run_id,),
                    ).fetchall()
                    quote_rows = con.execute(
                        "SELECT neg_risk_market_id,market_id,condition_id,slug,yes_token_id,"
                        "terminal_state,best_ask_price,best_ask_size,event_id,membership_hash "
                        "FROM neg_risk_quotes WHERE quote_run_id=? ORDER BY yes_token_id",
                        (run_id,),
                    ).fetchall()
                    leg_quote_digest = _leg_quote_digest(
                        tuple(UniverseLeg(*row) for row in leg_rows),
                        tuple(PersistedQuote(*row) for row in quote_rows),
                    )
                    receipt_digest = _quote_source_receipt_digest(
                        quote_run_id=run_id,
                        universe_snapshot_id=int(
                            con.execute(
                                "SELECT universe_snapshot_id FROM neg_risk_quote_runs WHERE id=?",
                                (run_id,),
                            ).fetchone()[0]
                        ),
                        universe_taken_at_ms=int(
                            con.execute(
                                "SELECT universe_taken_at_ms FROM neg_risk_quote_runs WHERE id=?",
                                (run_id,),
                            ).fetchone()[0]
                        ),
                        source_mode=str(source_receipt[0]),
                        source_revision=int(source_receipt[1]),
                        projection_receipt_digest=str(source_receipt[2]),
                        source_rejections_json=str(source_receipt[3]),
                        universe_hash=str(
                            con.execute(
                                "SELECT universe_hash FROM neg_risk_quote_runs WHERE id=?",
                                (run_id,),
                            ).fetchone()[0]
                        ),
                        source_truth_hash=str(
                            con.execute(
                                "SELECT source_truth_hash FROM neg_risk_quote_runs WHERE id=?",
                                (run_id,),
                            ).fetchone()[0]
                        ),
                        leg_quote_digest=leg_quote_digest,
                    )
                    con.execute(
                        "UPDATE neg_risk_quote_source_receipts SET leg_quote_digest=?,"
                        "receipt_digest=? WHERE quote_run_id=? AND leg_quote_digest=''",
                        (leg_quote_digest, receipt_digest, run_id),
                    )
                con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'complete', "
                    "successful_response_count = ?, completed_at_ms = ? WHERE id = ?",
                    (successful_response_count, completed_at_ms, run_id),
                )
                if publish_current_generation:
                    previous = con.execute(
                        "SELECT quote_run_id FROM neg_risk_quote_current_generation "
                        "WHERE singleton=1"
                    ).fetchone()
                    con.execute(
                        "INSERT INTO neg_risk_quote_current_generation(singleton,quote_run_id) "
                        "VALUES (1,?) ON CONFLICT(singleton) DO UPDATE SET "
                        "quote_run_id=excluded.quote_run_id",
                        (run_id,),
                    )
                    if previous is not None and int(previous[0]) != run_id:
                        self._purge_complete_run_payloads_in_transaction(
                            con,
                            (int(previous[0]),),
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

    @staticmethod
    def _purge_complete_run_payloads_in_transaction(
        con: sqlite3.Connection,
        run_ids: tuple[int, ...],
    ) -> None:
        """Delete former current generations after their pointer is switched."""
        if not run_ids:
            return
        placeholders = ",".join("?" for _ in run_ids)
        con.executemany(
            "INSERT INTO neg_risk_quote_purge_authority(quote_run_id) VALUES (?)",
            ((run_id,) for run_id in run_ids),
        )
        con.execute(
            f"DELETE FROM neg_risk_quotes WHERE quote_run_id IN ({placeholders})",
            run_ids,
        )
        con.execute(
            "UPDATE neg_risk_quote_attempts SET "
            "quote_run_identity=COALESCE(quote_run_identity,quote_run_id),"
            "quote_run_id=NULL WHERE quote_run_id IN ("
            f"{placeholders})",
            run_ids,
        )
        con.execute(
            f"DELETE FROM neg_risk_quote_source_receipts WHERE quote_run_id IN ({placeholders})",
            run_ids,
        )
        con.execute(
            f"DELETE FROM neg_risk_quote_run_legs WHERE quote_run_id IN ({placeholders})",
            run_ids,
        )
        con.execute(
            f"DELETE FROM neg_risk_quote_compact_feeds WHERE quote_run_id IN ({placeholders})",
            run_ids,
        )
        con.execute(
            f"DELETE FROM neg_risk_quote_runs WHERE id IN ({placeholders})",
            run_ids,
        )
        con.execute(
            f"DELETE FROM neg_risk_quote_purge_authority WHERE quote_run_id IN ({placeholders})",
            run_ids,
        )

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

    def fail_collecting_runs(self, *, failure_reason: str) -> int:
        """Release all in-flight leases after their sole worker has stopped.

        This is deliberately a shutdown-only primitive: the production worker
        invokes it only after its isolated child has been terminated and
        awaited, so a replacement process never waits for an orphaned lease.
        Complete runs remain immutable.
        """
        if not isinstance(failure_reason, str) or not failure_reason:
            raise ValueError("failure_reason must be a non-empty string")
        con = self._connect()
        try:
            self._begin_immediate(con)
            try:
                cur = con.execute(
                    "UPDATE neg_risk_quote_runs SET status = 'failed', failure_reason = ? "
                    "WHERE status = 'collecting'",
                    (failure_reason,),
                )
                con.execute("COMMIT")
                return int(cur.rowcount)
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def reclaim_terminal_failed_payloads(self, *, max_runs: int = 1) -> int:
        """Release unpublishable failed-run payloads while retaining diagnosis.

        A failed full-universe run can contain tens of thousands of legs and
        terminal quotes.  Those rows are never eligible for a certified feed;
        retaining them across a retry storm turns transient CLOB failures into
        persistent SQLite write amplification.  The run metadata and its
        attempt's stable ``quote_run_identity`` remain durable evidence.
        """
        if isinstance(max_runs, bool) or not isinstance(max_runs, int) or max_runs < 1:
            raise ValueError("max_runs must be positive")
        con = self._connect()
        try:
            con.execute("PRAGMA secure_delete=OFF")
            self._begin_immediate(con)
            try:
                rows = con.execute(
                    "SELECT r.id FROM neg_risk_quote_runs r "
                    "WHERE r.status='failed' AND ("
                    "EXISTS (SELECT 1 FROM neg_risk_quote_run_legs l WHERE l.quote_run_id=r.id) "
                    "OR EXISTS (SELECT 1 FROM neg_risk_quotes q WHERE q.quote_run_id=r.id) "
                    "OR EXISTS (SELECT 1 FROM neg_risk_quote_source_receipts s "
                    "WHERE s.quote_run_id=r.id)"
                    ") ORDER BY r.quoted_at_ms,r.id LIMIT ?",
                    (max_runs,),
                ).fetchall()
                run_ids = tuple(int(row[0]) for row in rows)
                if not run_ids:
                    con.execute("COMMIT")
                    return 0
                placeholders = ",".join("?" for _ in run_ids)
                con.execute(
                    "UPDATE neg_risk_quote_attempts SET "
                    "quote_run_identity=COALESCE(quote_run_identity,quote_run_id),"
                    "quote_run_id=NULL WHERE quote_run_id IN ("
                    f"{placeholders})",
                    run_ids,
                )
                con.execute(
                    "DELETE FROM neg_risk_quote_source_receipts "
                    f"WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    f"DELETE FROM neg_risk_quotes WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    f"DELETE FROM neg_risk_quote_run_legs WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute("COMMIT")
                return len(run_ids)
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def purge_old_runs(
        self,
        *,
        keep_last_per_status: int = 10,
        max_runs: int = 20,
    ) -> int:
        """Delete a bounded batch of old terminal runs and their heavy rows.

        The newest complete runs are independently protected from recent
        failures so feed restoration can never lose its last known-good input.
        Collecting runs are never eligible.
        """
        for name, value, allow_zero in (
            ("keep_last_per_status", keep_last_per_status, True),
            ("max_runs", max_runs, False),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int)
                or value < (0 if allow_zero else 1)
            ):
                raise ValueError(f"{name} must be {'non-negative' if allow_zero else 'positive'}")
        con = self._connect()
        try:
            # Quote payloads are public market data.  Securely overwriting tens
            # of thousands of deleted rows made one bounded production purge
            # take 12-129 seconds; leave pages on SQLite's freelist instead so
            # future quote runs reuse them without blocking the hot producer.
            con.execute("PRAGMA secure_delete=OFF")
            self._begin_immediate(con)
            try:
                # Spawn/protocol failures can have no run row. Retain the
                # newest 1,000 terminal attempts and trim at most 20 per call.
                con.execute(
                    "DELETE FROM neg_risk_quote_attempts WHERE id IN ("
                    "SELECT id FROM neg_risk_quote_attempts WHERE outcome IN ('complete','failed') "
                    "AND id NOT IN (SELECT id FROM neg_risk_quote_attempts "
                    "ORDER BY started_at_ms DESC,id DESC LIMIT 1000) "
                    "ORDER BY started_at_ms,id LIMIT 20)"
                )
                rows = con.execute(
                    "WITH protected AS ("
                    "SELECT id FROM neg_risk_quote_runs WHERE status='complete' "
                    "ORDER BY quoted_at_ms DESC,id DESC LIMIT ?"
                    ") SELECT id FROM neg_risk_quote_runs "
                    "WHERE status IN ('complete','failed') "
                    "AND id NOT IN (SELECT id FROM protected) "
                    "AND id NOT IN (SELECT quote_run_id FROM "
                    "neg_risk_quote_current_generation) "
                    "AND id NOT IN ("
                    "SELECT id FROM neg_risk_quote_runs WHERE status='failed' "
                    "ORDER BY quoted_at_ms DESC,id DESC LIMIT ?"
                    ") ORDER BY quoted_at_ms,id LIMIT ?",
                    (keep_last_per_status, keep_last_per_status, max_runs),
                ).fetchall()
                run_ids = tuple(int(row[0]) for row in rows)
                if not run_ids:
                    con.execute("COMMIT")
                    return 0
                placeholders = ",".join("?" for _ in run_ids)
                con.executemany(
                    "INSERT INTO neg_risk_quote_purge_authority(quote_run_id) VALUES (?)",
                    ((run_id,) for run_id in run_ids),
                )
                con.execute(
                    f"DELETE FROM neg_risk_quotes WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                # Attempts are durable operational receipts and outlive the
                # heavy run rows.  Legacy schemas retain an FK on quote_run_id;
                # copy its identity before detaching it for bounded purge.
                con.execute(
                    "UPDATE neg_risk_quote_attempts SET "
                    "quote_run_identity=COALESCE(quote_run_identity,quote_run_id),"
                    "quote_run_id=NULL WHERE quote_run_id IN ("
                    f"{placeholders})",
                    run_ids,
                )
                con.execute(
                    "DELETE FROM neg_risk_quote_source_receipts "
                    f"WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    f"DELETE FROM neg_risk_quote_run_legs WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    f"DELETE FROM neg_risk_quote_compact_feeds WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    f"DELETE FROM neg_risk_quote_runs WHERE id IN ({placeholders})",
                    run_ids,
                )
                con.execute(
                    "DELETE FROM neg_risk_quote_purge_authority "
                    f"WHERE quote_run_id IN ({placeholders})",
                    run_ids,
                )
                con.execute("COMMIT")
                return len(run_ids)
            except Exception:
                _rollback_without_masking(con)
                raise
        finally:
            con.close()

    def latest_complete_run(self) -> QuoteRun | None:
        """Return the newest complete run's metadata, without its terminal rows."""
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            pointer = con.execute(
                "SELECT quote_run_id FROM neg_risk_quote_current_generation WHERE singleton=1"
            ).fetchone()
            current_clause = "AND r.id=? " if pointer is not None else ""
            parameters = (int(pointer[0]),) if pointer is not None else ()
            row = con.execute(
                "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                "requested_token_count, successful_response_count, status, failure_reason, "
                "completed_at_ms, universe_hash, source_truth_hash "
                "FROM neg_risk_quote_runs r "
                "WHERE r.status = 'complete' AND length(r.universe_hash)=64 "
                "AND length(r.source_truth_hash)=64 " + current_clause + "AND EXISTS ("
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
                ") ORDER BY r.quoted_at_ms DESC, r.id DESC LIMIT 1",
                parameters,
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
            pointer = con.execute(
                "SELECT quote_run_id FROM neg_risk_quote_current_generation WHERE singleton=1"
            ).fetchone()
            if pointer is None:
                run_rows = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms, universe_hash, source_truth_hash "
                    "FROM neg_risk_quote_runs "
                    "WHERE status='complete' "
                    "ORDER BY quoted_at_ms DESC,id DESC"
                ).fetchall()
            else:
                run_rows = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms, universe_hash, source_truth_hash "
                    "FROM neg_risk_quote_runs WHERE id=? AND status='complete'",
                    (int(pointer[0]),),
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
                has_blank_provenance, all_provenance_blank = _blank_provenance_state(
                    leg_rows, quote_rows
                )
                if has_blank_provenance and not all_provenance_blank:
                    raise QuoteProjectionIntegrityError()
                if not run.universe_hash:
                    raise QuoteProjectionIntegrityError()
                run_legs = tuple(UniverseLeg(*row) for row in leg_rows)
                quotes = tuple(PersistedQuote(*row) for row in quote_rows)
                source_receipt = con.execute(
                    "SELECT source_mode,source_revision,projection_receipt_digest,"
                    "source_rejections_json,leg_quote_digest,receipt_digest FROM "
                    "neg_risk_quote_source_receipts WHERE quote_run_id=?",
                    (run.run_id,),
                ).fetchone()
                if source_receipt is None:
                    unsealed = con.execute(
                        "SELECT 1 FROM neg_risk_quote_unsealed_receipts WHERE quote_run_id=?",
                        (run.run_id,),
                    ).fetchone()
                    if unsealed is not None:
                        # Never let an unsealed draft regain trust through the
                        # historical no-receipt Structure compatibility path.
                        continue
                    # One-time compatibility path for runs produced before the
                    # run-bound source receipt existed. New runs never rescan.
                    try:
                        source = resolve_structure_read_context(
                            con,
                            mode=self._structure_generation_read_mode,
                            snapshot_id=run.universe_snapshot_id,
                        )
                    except StructureGenerationReadError:
                        raise QuoteProjectionIntegrityError()
                    source_universe = _verified_universe_for_snapshot(
                        con,
                        snapshot_id=source.snapshot_id,
                        taken_at_ms=source.taken_at_ms,
                        truth_table=source.table("group_truth"),
                        market_table=source.table("markets"),
                        membership_table=source.table("memberships"),
                    )
                else:
                    try:
                        rejection_values = json.loads(str(source_receipt[3]))
                        rejections = tuple(
                            GroupRejection(str(item[0]), str(item[1]), str(item[2]))
                            for item in rejection_values
                        )
                    except (TypeError, ValueError, IndexError, json.JSONDecodeError) as error:
                        raise QuoteProjectionIntegrityError() from error
                    expected_receipt = _quote_source_receipt_digest(
                        quote_run_id=run.run_id,
                        universe_snapshot_id=run.universe_snapshot_id,
                        universe_taken_at_ms=run.universe_taken_at_ms,
                        source_mode=str(source_receipt[0]),
                        source_revision=int(source_receipt[1]),
                        projection_receipt_digest=str(source_receipt[2]),
                        source_rejections_json=str(source_receipt[3]),
                        universe_hash=run.universe_hash,
                        source_truth_hash=run.source_truth_hash,
                        leg_quote_digest=str(source_receipt[4]),
                    )
                    if (
                        len(str(source_receipt[4])) != 64
                        or source_receipt[4] != _leg_quote_digest(run_legs, quotes)
                        or source_receipt[5] != expected_receipt
                    ):
                        raise QuoteProjectionIntegrityError()
                    source_universe = VerifiedQuoteUniverse(
                        snapshot_id=run.universe_snapshot_id,
                        taken_at_ms=run.universe_taken_at_ms,
                        universe_hash=run.universe_hash,
                        legs=run_legs,
                        rejections=rejections,
                        structure_receipt_digest=str(source_receipt[2]),
                        structure_revision=int(source_receipt[1]),
                        structure_mode=str(source_receipt[0]),
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

    def latest_complete_projection_metadata(self) -> QuoteRun | None:
        """Read only the current complete run identity before a large projection load."""
        con = sqlite3.connect(
            f"file:{self._db_path}?mode=ro",
            uri=True,
            isolation_level=None,
        )
        try:
            con.execute("BEGIN")
            pointer = con.execute(
                "SELECT quote_run_id FROM neg_risk_quote_current_generation WHERE singleton=1"
            ).fetchone()
            if pointer is None:
                row = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms, universe_hash, source_truth_hash "
                    "FROM neg_risk_quote_runs WHERE status='complete' "
                    "ORDER BY quoted_at_ms DESC,id DESC LIMIT 1"
                ).fetchone()
            else:
                row = con.execute(
                    "SELECT id, universe_snapshot_id, universe_taken_at_ms, quoted_at_ms, "
                    "requested_token_count, successful_response_count, status, failure_reason, "
                    "completed_at_ms, universe_hash, source_truth_hash "
                    "FROM neg_risk_quote_runs WHERE id=? AND status='complete'",
                    (int(pointer[0]),),
                ).fetchone()
            con.execute("COMMIT")
            return None if row is None else _quote_run_from_row(row)
        finally:
            con.close()

    def persist_compact_feed(self, run_id: int, payload: dict[str, object]) -> None:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        digest = hashlib.sha256(encoded.encode()).hexdigest()
        con = self._connect()
        try:
            self._begin_immediate(con)
            current = con.execute(
                "SELECT quote_run_id FROM neg_risk_quote_current_generation WHERE singleton=1"
            ).fetchone()
            if current is None or int(current[0]) != run_id:
                raise QuoteProjectionIntegrityError()
            con.execute(
                "INSERT OR REPLACE INTO neg_risk_quote_compact_feeds("
                "quote_run_id,payload_json,payload_sha256,created_at_ms) "
                "VALUES(?,?,?,?)",
                (run_id, encoded, digest, self.current_time_ms()),
            )
            con.commit()
        finally:
            con.close()

    def latest_compact_feed(self) -> tuple[QuoteRun, dict[str, object]] | None:
        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True, timeout=0.25)
        try:
            row = con.execute(
                "SELECT r.id,r.universe_snapshot_id,r.universe_taken_at_ms,"
                "r.quoted_at_ms,r.requested_token_count,"
                "r.successful_response_count,r.status,r.failure_reason,"
                "r.completed_at_ms,r.universe_hash,r.source_truth_hash,"
                "f.payload_json,f.payload_sha256 "
                "FROM neg_risk_quote_current_generation g "
                "JOIN neg_risk_quote_runs r ON r.id=g.quote_run_id "
                "JOIN neg_risk_quote_compact_feeds f ON f.quote_run_id=r.id "
                "WHERE g.singleton=1 AND r.status='complete'"
            ).fetchone()
            if row is None:
                return None
            payload_json, digest = str(row[11]), str(row[12])
            if hashlib.sha256(payload_json.encode()).hexdigest() != digest:
                raise QuoteProjectionIntegrityError()
            payload = json.loads(payload_json)
            if not isinstance(payload, dict):
                raise QuoteProjectionIntegrityError()
            return _quote_run_from_row(row[:11]), payload
        finally:
            con.close()


def _blank_provenance_state(
    leg_rows: list[tuple[object, ...]],
    quote_rows: list[tuple[object, ...]],
) -> tuple[bool, bool]:
    provenance = tuple(
        (str(row[-2]).strip(), str(row[-1]).strip()) for row in (*leg_rows, *quote_rows)
    )
    has_blank = any(not event_id or not membership_hash for event_id, membership_hash in provenance)
    all_blank = bool(provenance) and all(
        not event_id and not membership_hash for event_id, membership_hash in provenance
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


def _verified_universe_for_snapshot(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    taken_at_ms: int,
    truth_table: str = "neg_risk_group_truth",
    market_table: str = "markets",
    membership_table: str = "event_market_memberships",
) -> VerifiedQuoteUniverse:
    truth_rows = con.execute(
        "SELECT event_id,neg_risk_market_id,neg_risk_type,expected_member_count,"
        "active_named_count,membership_hash,quality,reason "
        f"FROM {truth_table} WHERE snapshot_id=? "
        "ORDER BY neg_risk_market_id",
        (snapshot_id,),
    ).fetchall()
    # Start from the small certified group authority.  Unsupported augmented
    # groups can contain most of production's 116k rows; materializing those in
    # Python on every Quote cycle caused the 300-second freshness incident.
    market_rows = con.execute(
        _supported_market_projection_sql(truth_table, market_table),
        (snapshot_id,),
    ).fetchall()
    membership_rows = con.execute(
        _supported_membership_projection_sql(truth_table, membership_table),
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
            rejections.append(GroupRejection(group_id, quality, "membership-market-mismatch"))
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


def _supported_market_projection_sql(truth_table: str, market_table: str) -> str:
    return (
        "SELECT k.event_id,k.neg_risk_market_id,k.market_id,k.condition_id,k.slug,"
        f"k.yes_token_id,k.active,k.closed,k.incomplete FROM {truth_table} t "
        f"JOIN {market_table} k ON k.snapshot_id=t.snapshot_id "
        "AND k.event_id=t.event_id AND k.neg_risk_market_id=t.neg_risk_market_id "
        "WHERE t.snapshot_id=? AND t.neg_risk_type='standard' "
        "AND t.quality='complete-supported' "
        "ORDER BY k.neg_risk_market_id,k.market_id"
    )


def _supported_membership_projection_sql(
    truth_table: str,
    membership_table: str,
) -> str:
    return (
        "SELECT m.event_id,m.neg_risk_market_id,m.market_id,m.member_kind,m.active,m.closed "
        f"FROM {truth_table} t JOIN {membership_table} m "
        "ON m.snapshot_id=t.snapshot_id AND m.event_id=t.event_id "
        "AND m.neg_risk_market_id=t.neg_risk_market_id WHERE t.snapshot_id=? "
        "AND t.neg_risk_type='standard' AND t.quality='complete-supported' "
        "ORDER BY m.neg_risk_market_id,m.market_id"
    )


def _generation_comparison_receipt(
    con: sqlite3.Connection,
    snapshot_id: int,
) -> tuple[object, ...]:
    row = con.execute(
        "SELECT r.publication_id,r.legacy_snapshot_id,r.legacy_market_count,"
        "r.generation_market_count,r.legacy_universe_hash,r.generation_universe_hash,"
        "r.legacy_source_truth_hash,r.generation_source_truth_hash,"
        "r.generation_validation_hash,r.created_at_ms,r.receipt_digest,"
        "g.comparison_receipt_digest FROM current_structure_generation g "
        "JOIN structure_generation_comparison_receipts r "
        "ON r.generation_snapshot_id=g.snapshot_id "
        "AND r.publication_id=g.publication_id WHERE g.id=1 AND g.snapshot_id=?",
        (snapshot_id,),
    ).fetchone()
    if row is None:
        raise QuoteRunStateError("generation projection receipt is unavailable")
    expected = _comparison_receipt_digest(
        generation_snapshot_id=snapshot_id,
        publication_id=str(row[0]),
        legacy_snapshot_id=int(row[1]),
        legacy_market_count=int(row[2]),
        generation_market_count=int(row[3]),
        legacy_universe_hash=str(row[4]),
        generation_universe_hash=str(row[5]),
        legacy_source_truth_hash=str(row[6]),
        generation_source_truth_hash=str(row[7]),
        generation_validation_hash=str(row[8]),
        created_at_ms=int(row[9]),
    )
    if row[10] != expected or row[11] != expected:
        raise QuoteRunStateError("generation projection receipt is unauthenticated")
    return row


def _bind_generation_projection_receipt(
    con: sqlite3.Connection,
    universe: VerifiedQuoteUniverse,
) -> VerifiedQuoteUniverse:
    row = _generation_comparison_receipt(con, universe.snapshot_id)
    if row[5] != universe.universe_hash or row[7] != _source_truth_hash(universe):
        raise QuoteUniverseUnavailableError("generation projection receipt mismatch")
    return replace(
        universe,
        structure_receipt_digest=str(row[10]),
        structure_revision=universe.snapshot_id,
        structure_mode="generation",
    )


def _projection_receipt_digest(
    *,
    source_mode: str,
    snapshot_id: int,
    taken_at_ms: int,
    source_revision: int,
    universe_hash: str,
    source_truth_hash: str,
) -> str:
    payload = [
        source_mode,
        snapshot_id,
        taken_at_ms,
        source_revision,
        universe_hash,
        source_truth_hash,
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _leg_quote_digest(
    legs: tuple[UniverseLeg, ...],
    quotes: tuple[PersistedQuote, ...],
) -> str:
    """Seal every identity and executable input consumed by opportunity scan."""
    legs_by_token = {leg.yes_token_id: leg for leg in legs}
    quotes_by_token = {quote.yes_token_id: quote for quote in quotes}
    if len(legs_by_token) != len(legs) or set(legs_by_token) != set(quotes_by_token):
        raise QuoteProjectionIntegrityError()
    canonical = []
    for token_id in sorted(legs_by_token):
        leg = legs_by_token[token_id]
        quote = quotes_by_token[token_id]
        canonical.append(
            [
                "BUY",
                leg.neg_risk_market_id,
                leg.event_id,
                leg.membership_hash,
                leg.market_id,
                leg.condition_id,
                leg.slug,
                leg.yes_token_id,
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
            ]
        )
    return hashlib.sha256(
        json.dumps(canonical, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _bind_legacy_projection_receipt(
    con: sqlite3.Connection,
    universe: VerifiedQuoteUniverse,
) -> VerifiedQuoteUniverse:
    row = con.execute(
        "SELECT revision FROM legacy_structure_revision WHERE id=1 AND NOT EXISTS "
        "(SELECT 1 FROM legacy_structure_revision_dirty WHERE id=1)"
    ).fetchone()
    if row is None:
        raise QuoteUniverseUnavailableError("legacy structure revision unavailable")
    revision = int(row[0])
    digest = _projection_receipt_digest(
        source_mode="legacy",
        snapshot_id=universe.snapshot_id,
        taken_at_ms=universe.taken_at_ms,
        source_revision=revision,
        universe_hash=universe.universe_hash,
        source_truth_hash=_source_truth_hash(universe),
    )
    return replace(
        universe,
        structure_receipt_digest=digest,
        structure_revision=revision,
        structure_mode="legacy",
    )


def _require_legacy_projection_receipt(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    taken_at_ms: int,
    universe_hash: str,
    source_truth_hash: str | None,
    source_revision: int | None,
    receipt_digest: str,
) -> str:
    row = con.execute(
        "SELECT revision FROM legacy_structure_revision WHERE id=1 AND NOT EXISTS "
        "(SELECT 1 FROM legacy_structure_revision_dirty WHERE id=1)"
    ).fetchone()
    if source_truth_hash is None or source_revision is None:
        raise QuoteRunStateError("legacy projection receipt is unavailable")
    if row is None:
        raise QuoteRunStateError("legacy projection receipt mismatch")
    if int(row[0]) != source_revision or receipt_digest != _projection_receipt_digest(
        source_mode="legacy",
        snapshot_id=snapshot_id,
        taken_at_ms=taken_at_ms,
        source_revision=source_revision,
        universe_hash=universe_hash,
        source_truth_hash=source_truth_hash,
    ):
        raise QuoteRunStateError("legacy projection receipt mismatch")
    return source_truth_hash


def _rejections_json(rejections: tuple[GroupRejection, ...]) -> str:
    return json.dumps(
        [[rejection.group_id, rejection.quality, rejection.reason] for rejection in rejections],
        ensure_ascii=False,
        separators=(",", ":"),
    )


def _quote_source_receipt_digest(
    *,
    quote_run_id: int,
    universe_snapshot_id: int,
    universe_taken_at_ms: int,
    source_mode: str,
    source_revision: int,
    projection_receipt_digest: str,
    source_rejections_json: str,
    universe_hash: str,
    source_truth_hash: str,
    leg_quote_digest: str,
) -> str:
    payload = [
        quote_run_id,
        universe_snapshot_id,
        universe_taken_at_ms,
        source_mode,
        source_revision,
        projection_receipt_digest,
        source_rejections_json,
        universe_hash,
        source_truth_hash,
        leg_quote_digest,
    ]
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _require_generation_projection_receipt(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    universe_hash: str,
    source_truth_hash: str | None,
    receipt_digest: str | None,
) -> str:
    if not source_truth_hash or not receipt_digest:
        raise QuoteRunStateError("generation projection receipt is required")
    row = _generation_comparison_receipt(con, snapshot_id)
    if row[5] != universe_hash or row[7] != source_truth_hash or row[10] != receipt_digest:
        raise QuoteRunStateError("generation projection receipt mismatch")
    return source_truth_hash


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
