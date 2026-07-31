"""SQLite writer for the snapshot pipeline.

Per CONTEXT.md D-D3, write_snapshot persists the row even when is_valid=False so
validation failures become queryable. The caller (orchestrator) is responsible for
setting a non-zero process exit code based on is_valid.

Design notes (anti-patterns avoided):
- Uses stdlib sqlite3 + parameterized DDL/SQL — NO SQLAlchemy ORM.
- BEGIN IMMEDIATE + DELETE FROM markets + executemany INSERT — never INSERT OR
  REPLACE alone, which would leak rows from prior snapshots.
- isolation_level=None gives explicit transaction control (Pitfall 4).
"""

from __future__ import annotations

import json
import sqlite3
import uuid
from collections.abc import Iterable
from pathlib import Path

from loguru import logger

from polyarb.perception.market_truth import (
    EventMember,
    GroupTruth,
    SourceCoverage,
    market_truth_mismatch_reason,
    membership_hash,
)
from polyarb.storage.schemas import (
    DDL,
    EVENT_TAGS_COLUMN_ORDER,
    EVENT_TAGS_INSERT_SQL,
    EVENTS_COLUMN_ORDER,
    EVENTS_INSERT_SQL,
    L2_MIRROR_STATE_DDL,
    MARKETS_COLUMN_ORDER,
    MARKETS_INSERT_SQL,
    SCHEDULER_STATE_DDL,
    SNAPSHOT_ATTEMPTS_DDL,
    STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL,
    STRUCTURE_SYNC_WINDOWS_DDL,
    migrate_fault_auth_finalize,
    migrate_fault_intent_status,
)
from polyarb.validator.category import Category, Issue, SnapshotStatus
from polyarb.validator.layers import determine_snapshot_status

_VALID_MODES = ("subset", "full")
# Structure publication and Quote collection share one WAL database. Their
# bounded bulk transactions can legitimately overlap for tens of seconds on
# the production volume, so SQLite's five-second default is too short.
SQLITE_BUSY_TIMEOUT_S = 120.0

# Booleans that are stored as INTEGER 0/1 in SQLite — convert before insert.
_BOOL_COLUMNS = ("active", "closed", "neg_risk", "incomplete")
# events table also has bool fields stored as INTEGER 0/1.
_EVENT_BOOL_COLUMNS = ("active", "closed")


def _backfill_structure_snapshot_statuses(con: sqlite3.Connection) -> None:
    """Derive persisted status for Structure rows created before that column existed.

    The source of truth is the same Layer-1 issue policy used by the
    orchestrator.  Re-running this is safe: Structure status is a deterministic
    projection of its immutable validation issues and ``is_valid`` result.
    Archive and legacy-combined rows intentionally remain outside this contract.
    """
    rows = con.execute(
        "SELECT s.id,s.is_valid,vi.layer,vi.category,vi.market_id,vi.detail,"
        "vi.raw_payload "
        "FROM snapshots s "
        "LEFT JOIN validation_issues vi ON vi.snapshot_id=s.id "
        "WHERE s.data_product='structure' "
        "ORDER BY s.id,vi.id"
    ).fetchall()
    if not rows:
        return

    snapshots: dict[int, tuple[bool, list[Issue]]] = {}
    for snapshot_id, is_valid, layer, category, market_id, detail, raw_payload in rows:
        if snapshot_id not in snapshots:
            snapshots[snapshot_id] = (bool(is_valid), [])
        if layer is None:
            continue
        try:
            parsed_category = Category(category)
        except ValueError:
            parsed_category = Category.UNKNOWN
        snapshots[snapshot_id][1].append(
            Issue(
                layer=int(layer),
                category=parsed_category,
                market_id=market_id,
                detail=detail or "",
                raw_payload=raw_payload,
            )
        )

    for snapshot_id, (is_valid, issues) in snapshots.items():
        status = (
            SnapshotStatus.FAILED
            if not is_valid
            else determine_snapshot_status(issues)
        )
        con.execute(
            "UPDATE snapshots SET snapshot_status=? WHERE id=?",
            (status.value, snapshot_id),
        )


def _truth_error(prefix: str, reason: str) -> ValueError:
    """Return a stable, bounded publication-boundary error."""
    return ValueError(f"{prefix}:{reason}"[:200])


def _validate_market_truth(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
) -> None:
    """Reject projections that cannot be authoritative before touching SQLite."""
    truths_by_key: dict[tuple[str, str], GroupTruth] = {}
    seen_group_ids: set[str] = set()
    for truth in group_truths:
        key = (truth.event_id, truth.group_id)
        if key in truths_by_key or truth.group_id in seen_group_ids:
            raise _truth_error(
                "market-truth-invalid",
                f"duplicate-group-identity:{truth.event_id}/{truth.group_id}",
            )
        truths_by_key[key] = truth
        seen_group_ids.add(truth.group_id)

    members_by_key: dict[tuple[str, str], list[EventMember]] = {}
    seen_member_ids: set[str] = set()
    seen_member_keys: set[tuple[str, str, str]] = set()
    for member in event_members:
        key = (member.event_id, member.group_id)
        identity = (*key, member.market_id)
        if identity in seen_member_keys or member.market_id in seen_member_ids:
            raise _truth_error(
                "market-truth-invalid",
                f"duplicate-member-identity:{member.market_id}",
            )
        if key not in truths_by_key:
            raise _truth_error(
                "market-truth-invalid",
                f"member-without-group-truth:{member.market_id}",
            )
        seen_member_keys.add(identity)
        seen_member_ids.add(member.market_id)
        members_by_key.setdefault(key, []).append(member)

    for key, truth in truths_by_key.items():
        members = members_by_key.get(key, [])
        member_count = len(members)
        if truth.expected_member_count != member_count:
            raise _truth_error(
                "market-truth-invalid",
                f"expected-member-count:{truth.group_id}:"
                f"{truth.expected_member_count}!={member_count}",
            )
        active_named_count = sum(
            member.member_kind == "named" and member.active for member in members
        )
        if truth.active_named_count != active_named_count:
            raise _truth_error(
                "market-truth-invalid",
                f"active-named-count:{truth.group_id}:"
                f"{truth.active_named_count}!={active_named_count}",
            )
        expected_hash = membership_hash(truth.event_id, truth.group_id, members)
        if truth.membership_hash != expected_hash:
            raise _truth_error(
                "market-truth-invalid",
                f"membership-hash:{truth.group_id}",
            )
        if truth.quality in ("complete-supported", "complete-unsupported"):
            if member_count == 0:
                raise _truth_error(
                    "market-truth-invalid",
                    f"expected_member_count-zero:{truth.group_id}",
                )


def _validate_publication_boundary(
    *,
    is_valid: bool,
    issues: list[Issue],
    source_coverage: SourceCoverage,
    group_truths: list[GroupTruth],
    publish_markets: bool,
) -> None:
    if not publish_markets:
        return
    if not source_coverage.completed:
        raise _truth_error(
            "market-truth-publication-rejected",
            f"source-incomplete:{source_coverage.failure_source}",
        )
    if not is_valid:
        raise _truth_error("market-truth-publication-rejected", "snapshot-invalid")
    if any(issue.category == Category.API_UNREACHABLE for issue in issues):
        raise _truth_error(
            "market-truth-publication-rejected",
            "blocking-source-issue:api-unreachable",
        )
    incomplete = next(
        (truth for truth in group_truths if truth.quality == "incomplete-source"),
        None,
    )
    if incomplete is not None:
        raise _truth_error(
            "market-truth-publication-rejected",
            f"incomplete-source:{incomplete.group_id}",
        )


def _validate_published_market_truth(
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
    market_rows: list[dict],
) -> None:
    reason = market_truth_mismatch_reason(event_members, group_truths, market_rows)
    if reason is not None:
        raise _truth_error("market-truth-invalid", reason)


def _rollback_without_masking(con: object) -> None:
    """Best-effort rollback while preserving the exception that caused it."""
    try:
        con.execute("ROLLBACK")  # type: ignore[attr-defined]
    except sqlite3.Error as rollback_error:
        logger.warning(
            f"SQLite rollback was already unavailable after the original failure: {rollback_error}"
        )


def _row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project a market dict into the column order required by MARKETS_INSERT_SQL.

    Always overrides the row's snapshot_id with the new id (the orchestrator may
    pass 0 as a placeholder before the snapshot_id is known).
    """
    out: list = []
    market_id = row.get("market_id")
    for col in MARKETS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        v = row.get(col)
        if col in ("market_id", "event_id", "neg_risk_market_id") and v is not None:
            if type(v) is not str or not v.strip() or v != v.strip():
                field = {
                    "market_id": "market-id",
                    "event_id": "event-id",
                    "neg_risk_market_id": "group-id",
                }[col]
                raise _truth_error(
                    "market-truth-invalid",
                    f"published-market-truth-mismatch:{field}:{market_id}",
                )
        if col in _BOOL_COLUMNS and v is not None:
            if type(v) is bool:
                v = int(v)
            elif type(v) is not int or v not in (0, 1):
                field = col.replace("_", "-")
                raise _truth_error(
                    "market-truth-invalid",
                    f"published-market-truth-mismatch:{field}-invalid:{market_id}",
                )
        out.append(v)
    return tuple(out)


def _event_row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project an event dict into EVENTS_COLUMN_ORDER for insert."""
    out: list = []
    for col in EVENTS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        v = row.get(col)
        if col in _EVENT_BOOL_COLUMNS and v is not None:
            v = int(bool(v))
        out.append(v)
    return tuple(out)


def _event_tag_row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project an event_tag dict into EVENT_TAGS_COLUMN_ORDER for insert."""
    out: list = []
    for col in EVENT_TAGS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        out.append(row.get(col))
    return tuple(out)


def _source_coverage_to_tuple(
    source_coverage: SourceCoverage,
    snapshot_id: int,
) -> tuple:
    return (
        snapshot_id,
        int(source_coverage.completed),
        source_coverage.market_items,
        source_coverage.event_items,
        source_coverage.failure_source,
        source_coverage.failure_reason,
    )


def _event_member_to_tuple(member: EventMember, snapshot_id: int) -> tuple:
    return (
        snapshot_id,
        member.event_id,
        member.group_id,
        member.market_id,
        member.member_kind,
        int(member.active),
        int(member.closed),
    )


def _group_truth_to_tuple(truth: GroupTruth, snapshot_id: int) -> tuple:
    if truth.expected_member_count == 0 and truth.quality != "incomplete-source":
        raise ValueError(
            "expected_member_count=0 is valid only for quality='incomplete-source'"
        )
    return (
        snapshot_id,
        truth.event_id,
        truth.group_id,
        truth.neg_risk_type,
        truth.expected_member_count,
        truth.active_named_count,
        truth.membership_hash,
        truth.quality,
        truth.reason,
    )


def _insert_market_truth(
    con: sqlite3.Connection,
    *,
    snapshot_id: int,
    source_coverage: SourceCoverage,
    event_members: list[EventMember],
    group_truths: list[GroupTruth],
) -> None:
    con.execute(
        "INSERT INTO snapshot_source_coverage("
        "snapshot_id,completed,market_items,event_items,failure_source,failure_reason"
        ") VALUES (?,?,?,?,?,?)",
        _source_coverage_to_tuple(source_coverage, snapshot_id),
    )
    if event_members:
        con.executemany(
            "INSERT INTO event_market_memberships("
            "snapshot_id,event_id,neg_risk_market_id,market_id,member_kind,active,closed"
            ") VALUES (?,?,?,?,?,?,?)",
            [_event_member_to_tuple(member, snapshot_id) for member in event_members],
        )
    if group_truths:
        con.executemany(
            "INSERT INTO neg_risk_group_truth("
            "snapshot_id,event_id,neg_risk_market_id,neg_risk_type,"
            "expected_member_count,active_named_count,membership_hash,quality,reason"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            [_group_truth_to_tuple(truth, snapshot_id) for truth in group_truths],
        )


class SQLiteStore:
    """Single-connection SQLite writer for snapshot runs.

    Usage:
        store = SQLiteStore(Path("data/state.db"))
        store.init_schema()
        store.write_snapshot(taken_at_ms=..., finished_at_ms=..., mode="subset", ...)
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    def _connect_writer(self) -> sqlite3.Connection:
        return sqlite3.connect(
            self._db_path,
            isolation_level=None,
            timeout=SQLITE_BUSY_TIMEOUT_S,
        )

    @property
    def db_path(self) -> Path:
        return self._db_path

    def init_schema(self) -> None:
        """Create tables, indexes, set WAL mode. Idempotent — safe to re-run.

        Phase 02 Plan 02-08 (F-01): after CREATE TABLE IF NOT EXISTS runs, we
        also perform a PRAGMA-driven idempotent ALTER TABLE ADD COLUMN pass
        for the snapshots table. `CREATE TABLE IF NOT EXISTS` does NOT modify
        an existing table, so legacy DBs that were initialized before Plan 03
        added supabase_mirror_at_ms + parquet_r2_url are still missing those
        columns and any UPDATE against them raises `no such column`.

        Strategy is add-only (LEARNINGS P7): we never drop, rename, or retype.
        Each (table, column, ddl) tuple is checked against PRAGMA table_info;
        the ALTER runs only if absent. Re-running init_schema after migration
        is a no-op (no duplicate-column error).
        """
        con = self._connect_writer()
        try:
            con.executescript(DDL)
            if migrate_fault_auth_finalize(con):
                con.executescript(DDL)
            if migrate_fault_intent_status(con):
                con.executescript(DDL)
            # Phase 02 Plan 02: scheduler_state singleton table
            con.executescript(SCHEDULER_STATE_DDL)
            # Parent-observed outcomes for isolated scheduler snapshot children.
            con.executescript(SNAPSHOT_ATTEMPTS_DDL)
            con.executescript(STRUCTURE_SCHEDULE_ADJUSTMENTS_DDL)
            con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
            # Phase 03.1 Plan 01: l2_mirror_state singleton (GAP-2 + GAP-3)
            con.executescript(L2_MIRROR_STATE_DDL)

            # Phase 02 Plan 02-08 (F-01): idempotent ADD COLUMN for legacy DBs.
            # Targets include snapshot archival metadata and scheduler evidence.
            # Note: SQLite ALTER TABLE ADD COLUMN cannot add NOT NULL columns
            # without a default — both targets are nullable, which is correct
            # (NULL = "never mirrored / not yet uploaded").
            def _ensure_column(table: str, column: str, ddl: str) -> None:
                rows = con.execute(f"PRAGMA table_info({table})").fetchall()
                existing = {r[1] for r in rows}
                if column not in existing:
                    con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
                    logger.info(
                        f"sqlite_store: idempotent migration — ALTER {table} ADD COLUMN {column}"
                    )

            _ensure_column("snapshots", "supabase_mirror_at_ms", "INTEGER")
            _ensure_column("snapshots", "parquet_r2_url", "TEXT")
            _ensure_column(
                "snapshots",
                "market_view_published",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                "snapshots",
                "data_product",
                "TEXT NOT NULL DEFAULT 'legacy_combined'",
            )
            _ensure_column(
                "snapshots",
                "archive_status",
                "TEXT NOT NULL DEFAULT 'legacy'",
            )
            _ensure_column(
                "snapshots",
                "snapshot_status",
                "TEXT NOT NULL DEFAULT 'ok'",
            )
            _ensure_column("snapshot_attempts", "last_stage", "TEXT")
            _ensure_column("snapshot_attempts", "elapsed_ms", "INTEGER")
            _backfill_structure_snapshot_statuses(con)
            # H-009: quote collectors lease their collecting run.  A default
            # of zero makes any legacy collecting row immediately recoverable
            # rather than leaving the single-run gate permanently wedged.
            _ensure_column(
                "neg_risk_quote_runs",
                "lease_expires_at_ms",
                "INTEGER NOT NULL DEFAULT 0",
            )
            _ensure_column(
                "neg_risk_quote_runs",
                "universe_hash",
                "TEXT NOT NULL DEFAULT ''",
            )
            _ensure_column(
                "neg_risk_quote_runs",
                "source_truth_hash",
                "TEXT NOT NULL DEFAULT ''",
            )
            for table in ("neg_risk_quote_run_legs", "neg_risk_quotes"):
                _ensure_column(table, "event_id", "TEXT NOT NULL DEFAULT ''")
                _ensure_column(table, "membership_hash", "TEXT NOT NULL DEFAULT ''")
        finally:
            con.close()
        # The base snapshot schema also contains the opportunity owner tables.
        # Finish their canonical guard/singleton bootstrap through the same
        # migration path used by opportunity-first producers.
        from polyarb.perception.store import OpportunityPerceptionStore

        OpportunityPerceptionStore(self._db_path).init_schema()

    def init_structure_sync_schema(self) -> None:
        """Prepare only the resumable Structure staging tables.

        The daemon performs the canonical full migration before it starts the
        scheduler.  Snapshot retry children must not repeat that whole scan on
        a multi-gigabyte production database.  A standalone invocation against
        a fresh database still falls back to the full bootstrap.
        """
        con = self._connect_writer()
        try:
            has_snapshot_schema = (
                con.execute(
                    "SELECT 1 FROM sqlite_master "
                    "WHERE type='table' AND name='snapshots'"
                ).fetchone()
                is not None
            )
            if has_snapshot_schema:
                con.executescript(STRUCTURE_SYNC_WINDOWS_DDL)
                return
        finally:
            con.close()
        self.init_schema()

    def write_snapshot(
        self,
        *,
        taken_at_ms: int,
        finished_at_ms: int,
        mode: str,
        parquet_path: str,
        is_valid: bool,
        market_rows: list[dict],
        issues: list[Issue],
        source_coverage: SourceCoverage,
        event_members: list[EventMember],
        group_truths: list[GroupTruth],
        publish_markets: bool,
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
        data_product: str = "legacy_combined",
        archive_status: str = "legacy",
        snapshot_status: str = "ok",
    ) -> int:
        """Persist one snapshot atomically.

        Inserts snapshot metadata, source coverage, events, memberships, group
        truth, optional current markets, and issues in one BEGIN IMMEDIATE
        transaction. ``markets`` is replaced only when ``publish_markets`` is
        true. Any exception triggers ROLLBACK and re-raises (we never swallow).

        FK ordering matters: snapshots → events → event_tags → markets. A market
        that references an event_id which is NOT in events for this snapshot is
        still inserted (markets.event_id has no FK CHECK constraint enforced —
        we want orphan-tolerance per Amendment 01 design).

        Phase 1.1 Amendment 01: events / event_tags persisted alongside markets.
        Both default to empty lists for backward compat with tests that don't
        supply them.

        Returns the new `snapshots.id`.
        """
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {_VALID_MODES}, got {mode!r}")

        event_rows = event_rows or []
        event_tag_rows = event_tag_rows or []
        _validate_market_truth(event_members, group_truths)
        _validate_publication_boundary(
            is_valid=is_valid,
            issues=issues,
            source_coverage=source_coverage,
            group_truths=group_truths,
            publish_markets=publish_markets,
        )
        if publish_markets:
            _validate_published_market_truth(
                event_members,
                group_truths,
                market_rows,
            )

        con = self._connect_writer()
        # Per-connection PRAGMAs (some are persistent like journal_mode=WAL after
        # init_schema, but setting again is cheap and safe).
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        try:
            if publish_markets:
                con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
                "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    len(market_rows),
                    int(publish_markets),
                    data_product,
                    archive_status,
                    snapshot_status,
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,  # supabase_mirror_at_ms — set after successful mirror push
                    None,  # parquet_r2_url — set after successful R2 upload
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None  # AUTOINCREMENT guarantees this

            _insert_market_truth(
                con,
                snapshot_id=snapshot_id,
                source_coverage=source_coverage,
                event_members=event_members,
                group_truths=group_truths,
            )

            # ── Amendment 01: events first (FK target for markets.event_id) ─
            event_tuples = [_event_row_to_tuple(r, snapshot_id) for r in event_rows]
            if event_tuples:
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── Amendment 01: event_tags (FK references events.id) ─────────
            event_tag_tuples = [_event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows]
            if event_tag_tuples:
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets (references events.id via event_id, FK not enforced) ─
            market_tuples = (
                [_row_to_tuple(r, snapshot_id) for r in market_rows]
                if publish_markets
                else []
            )
            if market_tuples:
                con.executemany(MARKETS_INSERT_SQL, market_tuples)

            issue_tuples = [
                (
                    snapshot_id,
                    issue.layer,
                    issue.category.value,
                    issue.market_id,
                    issue.detail,
                    issue.raw_payload,
                )
                for issue in issues
            ]
            if issue_tuples:
                con.executemany(
                    "INSERT INTO validation_issues("
                    "snapshot_id,layer,category,market_id,detail,raw_payload"
                    ") VALUES (?,?,?,?,?,?)",
                    issue_tuples,
                )

            con.execute("COMMIT")
        except Exception:
            _rollback_without_masking(con)
            logger.exception("SQLite write_snapshot rolled back")
            raise
        finally:
            con.close()

        logger.info(
            f"SQLite snapshot id={snapshot_id} mode={mode} markets={len(market_rows)} "
            f"events={len(event_rows)} event_tags={len(event_tag_rows)} "
            f"issues={len(issues)} is_valid={is_valid}"
        )
        return snapshot_id

    # Plan 02-09 (D-23): streaming variant — accepts an iterator of market dicts
    # and inserts them in batches of `batch_size` inside a single BEGIN IMMEDIATE
    # transaction. Atomicity invariant: a crash mid-batch leaves no partial
    # snapshot visible (ROLLBACK rewinds the entire write). The legacy
    # write_snapshot above is retained for callers that pass a fully-materialized
    # list (tests, one-off scripts). TODO(02-09 follow-up): remove write_snapshot
    # once orchestrator and Supabase mirror migrate to the streaming variant.
    def write_snapshot_streaming(
        self,
        *,
        taken_at_ms: int,
        finished_at_ms: int,
        mode: str,
        parquet_path: str,
        is_valid: bool,
        market_rows: Iterable[dict],
        issues: list[Issue],
        source_coverage: SourceCoverage,
        event_members: list[EventMember],
        group_truths: list[GroupTruth],
        publish_markets: bool,
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
        batch_size: int = 500,
        data_product: str = "legacy_combined",
        archive_status: str = "legacy",
        snapshot_status: str = "ok",
    ) -> tuple[int, int]:
        """Streaming variant of write_snapshot.

        Identical publication semantics to write_snapshot, but `market_rows`
        can be any iterable (list, generator). Published inserts run in batches
        of `batch_size` inside ONE BEGIN IMMEDIATE → COMMIT transaction, so
        per-snapshot atomicity is preserved exactly as the legacy path.

        Because the iterator length is unknown up front, we insert the
        snapshots row with market_count=0 as a placeholder and UPDATE it to
        the real count after the iterator is consumed — still inside the
        same transaction, so the final visible row always has the correct
        count.

        Returns:
            (snapshot_id, market_row_count) — the auto-assigned snapshot id
            and how many market rows were inserted.
        """
        if mode not in _VALID_MODES:
            raise ValueError(f"mode must be one of {_VALID_MODES}, got {mode!r}")

        event_rows = event_rows or []
        event_tag_rows = event_tag_rows or []
        _validate_market_truth(event_members, group_truths)
        _validate_publication_boundary(
            is_valid=is_valid,
            issues=issues,
            source_coverage=source_coverage,
            group_truths=group_truths,
            publish_markets=publish_markets,
        )

        con = self._connect_writer()
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        market_count = 0
        try:
            if publish_markets:
                con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
                "data_product,archive_status,snapshot_status,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    0,  # placeholder; UPDATEd after stream consumed
                    int(publish_markets),
                    data_product,
                    archive_status,
                    snapshot_status,
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,
                    None,
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None

            _insert_market_truth(
                con,
                snapshot_id=snapshot_id,
                source_coverage=source_coverage,
                event_members=event_members,
                group_truths=group_truths,
            )

            # ── events first (FK target for markets.event_id) ──────────────
            if event_rows:
                event_tuples = [_event_row_to_tuple(r, snapshot_id) for r in event_rows]
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── event_tags (FK references events.id) ───────────────────────
            if event_tag_rows:
                event_tag_tuples = [_event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows]
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets streamed in batches ────────────────────────────────
            batch: list[tuple] = []
            for row in market_rows:
                market_count += 1
                if publish_markets:
                    batch.append(_row_to_tuple(row, snapshot_id))
                    if len(batch) >= batch_size:
                        con.executemany(MARKETS_INSERT_SQL, batch)
                        batch.clear()
            if batch:
                con.executemany(MARKETS_INSERT_SQL, batch)
                batch.clear()

            if publish_markets:
                published_market_rows = [
                    {
                        "market_id": row[0],
                        "event_id": row[1],
                        "neg_risk_market_id": row[2],
                        "neg_risk": row[3],
                        "active": row[4],
                        "closed": row[5],
                    }
                    for row in con.execute(
                        "SELECT market_id,event_id,neg_risk_market_id,"
                        "neg_risk,active,closed FROM markets WHERE snapshot_id=?",
                        (snapshot_id,),
                    ).fetchall()
                ]
                _validate_published_market_truth(
                    event_members,
                    group_truths,
                    published_market_rows,
                )

            # Patch market_count to the real value (still inside same tx)
            con.execute(
                "UPDATE snapshots SET market_count=? WHERE id=?",
                (market_count, snapshot_id),
            )

            # ── issues ─────────────────────────────────────────────────────
            if issues:
                issue_tuples = [
                    (
                        snapshot_id,
                        issue.layer,
                        issue.category.value,
                        issue.market_id,
                        issue.detail,
                        issue.raw_payload,
                    )
                    for issue in issues
                ]
                con.executemany(
                    "INSERT INTO validation_issues("
                    "snapshot_id,layer,category,market_id,detail,raw_payload"
                    ") VALUES (?,?,?,?,?,?)",
                    issue_tuples,
                )

            con.execute("COMMIT")
        except Exception:
            _rollback_without_masking(con)
            logger.exception("SQLite write_snapshot_streaming rolled back")
            raise
        finally:
            con.close()

        logger.info(
            f"SQLite snapshot id={snapshot_id} mode={mode} markets={market_count} "
            f"events={len(event_rows)} event_tags={len(event_tag_rows)} "
            f"issues={len(issues)} is_valid={is_valid} "
            f"(streaming, batch_size={batch_size})"
        )
        return snapshot_id, market_count

    def update_snapshot_mirror_fields(
        self,
        snapshot_id: int,
        *,
        supabase_mirror_at_ms: int | None = None,
        parquet_r2_url: str | None = None,
    ) -> None:
        """Update supabase_mirror_at_ms and/or parquet_r2_url for a snapshot.

        Phase 02 Plan 03 — called by orchestrator after successful mirror push / R2 upload.
        Silently no-ops if snapshot_id doesn't exist (defensive; should never happen).
        """
        if supabase_mirror_at_ms is None and parquet_r2_url is None:
            return
        sets: list[str] = []
        params: list = []
        if supabase_mirror_at_ms is not None:
            sets.append("supabase_mirror_at_ms = ?")
            params.append(supabase_mirror_at_ms)
        if parquet_r2_url is not None:
            sets.append("parquet_r2_url = ?")
            params.append(parquet_r2_url)
        params.append(snapshot_id)
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.execute(
                f"UPDATE snapshots SET {', '.join(sets)} WHERE id = ?",
                tuple(params),
            )
        finally:
            con.close()

    def get_snapshot(self, snapshot_id: int) -> dict | None:
        """Return a single snapshot row by ID, or None if not found.

        Phase 02 Plan 03 — used by reconcile to retrieve snapshot metadata.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, "
                "parquet_path, notes, supabase_mirror_at_ms, parquet_r2_url "
                "FROM snapshots WHERE id = ?",
                (snapshot_id,),
            ).fetchone()
            if not row:
                return None
            return {
                "id": row[0],
                "taken_at_ms": row[1],
                "finished_at_ms": row[2],
                "mode": row[3],
                "is_valid": bool(row[4]),
                "market_count": row[5],
                "parquet_path": row[6],
                "notes": row[7],
                "supabase_mirror_at_ms": row[8],
                "parquet_r2_url": row[9],
            }
        finally:
            con.close()

    def get_markets_for_snapshot(self, snapshot_id: int) -> list[dict]:
        """Return all market rows for a given snapshot_id.

        Phase 02 Plan 03 — used by reconcile to get markets to mirror.
        Returns list of dicts with selected columns for Supabase narrow mirror.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return []
        try:
            rows = con.execute(
                "SELECT market_id, question, slug, event_id AS event_slug, "
                "mid_price, liquidity_usd, volume_usd, end_time_ms "
                "FROM markets WHERE snapshot_id = ?",
                (snapshot_id,),
            ).fetchall()
            return [
                {
                    "market_id": r[0],
                    "question": r[1],
                    "slug": r[2],
                    "event_slug": r[3],
                    "mid_price": r[4],
                    "liquidity_usd": r[5],
                    "volume_usd": r[6],
                    "end_time_ms": r[7],
                }
                for r in rows
            ]
        finally:
            con.close()

    def get_scheduler_state(self) -> dict | None:
        """Read the scheduler singleton row. Returns None if not yet written."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT state, failure_counter, updated_at_ms FROM scheduler_state WHERE id=1"
            ).fetchone()
            if not row:
                return None
            return {"state": row[0], "failure_counter": row[1], "updated_at_ms": row[2]}
        finally:
            con.close()

    def upsert_scheduler_state(self, *, state: str, failure_counter: int) -> None:
        """Write/update the scheduler singleton row."""
        import time as _time

        updated_at_ms = int(_time.time() * 1000)
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO scheduler_state(id, state, failure_counter, updated_at_ms) "
                "VALUES (1, ?, ?, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "state=excluded.state, failure_counter=excluded.failure_counter, "
                "updated_at_ms=excluded.updated_at_ms",
                (state, failure_counter, updated_at_ms),
            )
        finally:
            con.close()

    def begin_or_resume_structure_sync(self, *, started_at_ms: int) -> dict[str, object]:
        """Return the sole resumable Structure window, creating it atomically."""
        if started_at_ms < 0:
            raise ValueError("invalid-structure-sync-start")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,published_snapshot_id "
                "FROM structure_sync_windows WHERE status IN ('open','events_complete') "
                "ORDER BY started_at_ms LIMIT 1"
            ).fetchone()
            if row is None:
                window_id = uuid.uuid4().hex
                con.execute(
                    "INSERT INTO structure_sync_windows("
                    "id,status,started_at_ms,checkpoint_at_ms) VALUES (?,'open',?,?)",
                    (window_id, started_at_ms, started_at_ms),
                )
                row = con.execute(
                    "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                    "checkpoint_at_ms,event_pages,market_pages,failure_reason,"
                    "published_snapshot_id "
                    "FROM structure_sync_windows WHERE id=?",
                    (window_id,),
                ).fetchone()
            con.execute("COMMIT")
            assert row is not None
            return self._structure_sync_window_row(row)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def get_latest_structure_sync(self) -> dict[str, object] | None:
        """Read latest window progress without starting another collection window."""
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,published_snapshot_id "
                "FROM structure_sync_windows ORDER BY checkpoint_at_ms DESC LIMIT 1"
            ).fetchone()
            return None if row is None else self._structure_sync_window_row(row)
        finally:
            con.close()

    def restart_structure_sync_window(
        self,
        *,
        window_id: str,
        restarted_at_ms: int,
        failure_reason: str,
    ) -> dict[str, object]:
        """Fail one rejected-cursor window and atomically open its successor."""
        if not window_id or restarted_at_ms < 0:
            raise ValueError("invalid-structure-sync-restart")
        if not failure_reason or len(failure_reason) > 200:
            raise ValueError("invalid-structure-sync-failure-reason")
        successor_id = uuid.uuid4().hex
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            cur = con.execute(
                "UPDATE structure_sync_windows SET status='failed',"
                "failure_reason=?,checkpoint_at_ms=? "
                "WHERE id=? AND status IN ('open','events_complete')",
                (failure_reason, restarted_at_ms, window_id),
            )
            if cur.rowcount != 1:
                raise ValueError("structure-sync-window-not-restartable")
            con.execute(
                "INSERT INTO structure_sync_windows("
                "id,status,started_at_ms,checkpoint_at_ms"
                ") VALUES (?,'open',?,?)",
                (successor_id, restarted_at_ms, restarted_at_ms + 1),
            )
            row = con.execute(
                "SELECT id,status,event_cursor,market_cursor,started_at_ms,"
                "checkpoint_at_ms,event_pages,market_pages,failure_reason,"
                "published_snapshot_id FROM structure_sync_windows WHERE id=?",
                (successor_id,),
            ).fetchone()
            con.execute("COMMIT")
            return self._structure_sync_window_row(row)
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def mark_structure_sync_published(
        self, *, window_id: str, snapshot_id: int, published_at_ms: int
    ) -> None:
        """Bind a complete window to its certified snapshot exactly once."""
        con = self._connect_writer()
        try:
            cur = con.execute(
                "UPDATE structure_sync_windows SET status='published',"
                "published_snapshot_id=?,checkpoint_at_ms=? "
                "WHERE id=? AND status='complete'",
                (snapshot_id, published_at_ms, window_id),
            )
            if cur.rowcount != 1:
                raise ValueError("structure-sync-window-not-complete")
        finally:
            con.close()

    def purge_published_structure_sync_windows(
        self,
        *,
        keep_last: int = 1,
        max_windows_per_run: int = 1,
    ) -> tuple[int, list[str]]:
        """Delete a bounded batch of raw staging already bound to snapshots."""
        if keep_last < 1:
            raise ValueError("keep_last must be positive")
        if max_windows_per_run < 1:
            raise ValueError("max_windows_per_run must be positive")

        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            keep_ids = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows "
                    "WHERE status='published' "
                    "ORDER BY checkpoint_at_ms DESC,id DESC LIMIT ?",
                    (keep_last,),
                )
            ]
            placeholders = ",".join("?" for _ in keep_ids)
            to_delete = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows "
                    "WHERE status='published' "
                    f"AND id NOT IN ({placeholders}) "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (*keep_ids, max_windows_per_run),
                )
            ]
            if to_delete:
                delete_placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    "DELETE FROM structure_sync_event_staging "
                    f"WHERE window_id IN ({delete_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_market_staging "
                    f"WHERE window_id IN ({delete_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_windows "
                    f"WHERE id IN ({delete_placeholders}) AND status='published'",
                    to_delete,
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

        if to_delete:
            logger.info(
                "structure staging retention deleted "
                f"{len(to_delete)} published windows ids={to_delete}"
            )
        return len(to_delete), to_delete

    def purge_failed_structure_sync_windows(
        self,
        *,
        max_windows_per_run: int = 1,
    ) -> tuple[int, list[str]]:
        """Reclaim staging from failed windows after fresh truth is certified."""
        if max_windows_per_run < 1:
            raise ValueError("max_windows_per_run must be positive")
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            to_delete = [
                str(row[0])
                for row in con.execute(
                    "SELECT id FROM structure_sync_windows WHERE status='failed' "
                    "ORDER BY checkpoint_at_ms,id LIMIT ?",
                    (max_windows_per_run,),
                )
            ]
            if to_delete:
                placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    "DELETE FROM structure_sync_event_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_market_staging "
                    f"WHERE window_id IN ({placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM structure_sync_windows "
                    f"WHERE id IN ({placeholders}) AND status='failed'",
                    to_delete,
                )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()
        return len(to_delete), to_delete

    def read_complete_structure_sync(self, window_id: object) -> tuple[list[dict], list[dict]]:
        """Return staged facts only after both source traversals completed."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            events = [
                json.loads(str(item[0]))
                for item in con.execute(
                    "SELECT payload_json FROM structure_sync_event_staging "
                    "WHERE window_id=? ORDER BY event_id", (window_id,)
                ).fetchall()
            ]
            markets = [
                json.loads(str(item[0]))
                for item in con.execute(
                    "SELECT payload_json FROM structure_sync_market_staging "
                    "WHERE window_id=? ORDER BY market_id", (window_id,)
                ).fetchall()
            ]
            return events, markets
        finally:
            con.close()

    def get_complete_structure_sync_counts(
        self, window_id: object
    ) -> tuple[int, int]:
        """Validate a completed window and return its staged row counts."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            event_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_sync_event_staging "
                    "WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            market_count = int(
                con.execute(
                    "SELECT COUNT(*) FROM structure_sync_market_staging "
                    "WHERE window_id=?",
                    (window_id,),
                ).fetchone()[0]
            )
            return event_count, market_count
        finally:
            con.close()

    def iter_complete_structure_events(self, window_id: str):
        """Stream completed event payloads without materializing the window."""
        yield from self._iter_complete_structure_payloads(
            window_id,
            table="structure_sync_event_staging",
            id_column="event_id",
        )

    def iter_complete_structure_markets(self, window_id: str):
        """Stream completed market payloads without materializing the window."""
        yield from self._iter_complete_structure_payloads(
            window_id,
            table="structure_sync_market_staging",
            id_column="market_id",
        )

    def _iter_complete_structure_payloads(
        self,
        window_id: object,
        *,
        table: str,
        id_column: str,
    ):
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT status FROM structure_sync_windows WHERE id=?", (window_id,)
            ).fetchone()
            if row is None or row[0] != "complete":
                raise ValueError("structure-sync-window-not-complete")
            cursor = con.execute(
                f"SELECT payload_json FROM {table} "  # noqa: S608 - internal constants
                f"WHERE window_id=? ORDER BY {id_column}",
                (window_id,),
            )
            for payload_row in cursor:
                yield json.loads(str(payload_row[0]))
        finally:
            con.close()

    @staticmethod
    def _structure_sync_window_row(row: tuple[object, ...]) -> dict[str, object]:
        keys = (
            "id", "status", "event_cursor", "market_cursor", "started_at_ms",
            "checkpoint_at_ms", "event_pages", "market_pages", "failure_reason",
            "published_snapshot_id",
        )
        return dict(zip(keys, row, strict=True))

    def commit_structure_event_page(
        self,
        *,
        window_id: object,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        events: list[dict],
        finished_at_ms: int,
    ) -> None:
        """Stage one validated event page and advance its opaque cursor together."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        if finished_at_ms < 0 or completed != (next_cursor is None):
            raise ValueError("invalid-structure-event-page")
        serialized: list[tuple[str, str, str | None]] = []
        for event in events:
            event_id = event.get("id") if isinstance(event, dict) else None
            if not isinstance(event_id, str) or not event_id:
                raise ValueError("invalid-structure-event")
            serialized.append((event_id, json.dumps(event, sort_keys=True), requested_cursor))
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status,event_cursor FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row[0] != "open" or row[1] != requested_cursor:
                raise ValueError("structure-event-page-cursor-mismatch")
            con.executemany(
                "INSERT INTO structure_sync_event_staging("
                "window_id,event_id,payload_json,source_cursor) "
                "VALUES (?,?,?,?) ON CONFLICT(window_id,event_id) DO UPDATE SET "
                "payload_json=excluded.payload_json,source_cursor=excluded.source_cursor",
                [(window_id, *item) for item in serialized],
            )
            con.execute(
                "UPDATE structure_sync_windows SET status=?,event_cursor=?,"
                "checkpoint_at_ms=?,event_pages=event_pages+1 WHERE id=?",
                (
                    "events_complete" if completed else "open",
                    next_cursor,
                    finished_at_ms,
                    window_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def list_staged_structure_events(self, window_id: object) -> list[dict]:
        """Read staging only for finalizer tests; it is never online truth."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        con = sqlite3.connect(self._db_path)
        try:
            rows = con.execute(
                "SELECT payload_json FROM structure_sync_event_staging "
                "WHERE window_id=? ORDER BY event_id",
                (window_id,),
            ).fetchall()
            return [json.loads(str(row[0])) for row in rows]
        finally:
            con.close()

    def commit_structure_market_page(
        self,
        *,
        window_id: object,
        requested_cursor: str | None,
        next_cursor: str | None,
        completed: bool,
        markets: list[dict],
        finished_at_ms: int,
    ) -> None:
        """Stage one market page only after complete event coverage is durable."""
        if not isinstance(window_id, str) or not window_id:
            raise ValueError("invalid-structure-sync-window")
        if finished_at_ms < 0 or completed != (next_cursor is None):
            raise ValueError("invalid-structure-market-page")
        serialized: list[tuple[str, str, str | None]] = []
        for market in markets:
            market_id = market.get("id") if isinstance(market, dict) else None
            if not isinstance(market_id, str) or not market_id:
                raise ValueError("invalid-structure-market")
            serialized.append((market_id, json.dumps(market, sort_keys=True), requested_cursor))
        con = self._connect_writer()
        try:
            con.execute("BEGIN IMMEDIATE")
            row = con.execute(
                "SELECT status,market_cursor FROM structure_sync_windows WHERE id=?",
                (window_id,),
            ).fetchone()
            if row is None or row[0] != "events_complete" or row[1] != requested_cursor:
                raise ValueError("structure-market-page-cursor-mismatch")
            con.executemany(
                "INSERT INTO structure_sync_market_staging("
                "window_id,market_id,payload_json,source_cursor) "
                "VALUES (?,?,?,?) ON CONFLICT(window_id,market_id) DO UPDATE SET "
                "payload_json=excluded.payload_json,source_cursor=excluded.source_cursor",
                [(window_id, *item) for item in serialized],
            )
            con.execute(
                "UPDATE structure_sync_windows SET status=?,market_cursor=?,"
                "checkpoint_at_ms=?,market_pages=market_pages+1 WHERE id=?",
                (
                    "complete" if completed else "events_complete",
                    next_cursor,
                    finished_at_ms,
                    window_id,
                ),
            )
            con.execute("COMMIT")
        except BaseException:
            if con.in_transaction:
                con.execute("ROLLBACK")
            raise
        finally:
            con.close()

    def begin_snapshot_attempt(self, *, started_at_ms: int) -> int:
        """Append one running scheduler attempt before spawning its child."""
        con = self._connect_writer()
        try:
            cur = con.execute(
                "INSERT INTO snapshot_attempts(started_at_ms,outcome) "
                "VALUES (?, 'running')",
                (started_at_ms,),
            )
            assert cur.lastrowid is not None
            return int(cur.lastrowid)
        finally:
            con.close()

    def finish_snapshot_attempt(
        self,
        *,
        attempt_id: int,
        outcome: str,
        finished_at_ms: int,
        snapshot_id: int | None,
        failure_kind: str | None,
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
    ) -> None:
        """Close one running attempt exactly once with a bounded outcome."""
        if outcome not in {"succeeded", "failed", "cancelled"}:
            raise ValueError(f"invalid terminal snapshot attempt outcome: {outcome}")
        con = self._connect_writer()
        try:
            cur = con.execute(
                "UPDATE snapshot_attempts "
                "SET finished_at_ms=?, outcome=?, snapshot_id=?, failure_kind=?, "
                "last_stage=?, elapsed_ms=? "
                "WHERE id=? AND outcome='running'",
                (
                    finished_at_ms,
                    outcome,
                    snapshot_id,
                    failure_kind,
                    last_stage,
                    elapsed_ms,
                    attempt_id,
                ),
            )
            if cur.rowcount != 1:
                raise ValueError(f"snapshot attempt {attempt_id} is not running")
        finally:
            con.close()

    def get_latest_snapshot_attempt(self) -> dict[str, object] | None:
        """Read one newest scheduler attempt without mutating operational truth."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT id,started_at_ms,finished_at_ms,outcome,snapshot_id,failure_kind,"
                "last_stage,elapsed_ms "
                "FROM snapshot_attempts ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            return {
                "id": row[0],
                "started_at_ms": row[1],
                "finished_at_ms": row[2],
                "outcome": row[3],
                "snapshot_id": row[4],
                "failure_kind": row[5],
                "last_stage": row[6],
                "elapsed_ms": row[7],
            }
        finally:
            con.close()

    def get_snapshot_attempts(self, *, limit: int = 30) -> list[dict[str, object]]:
        """Read bounded newest scheduler attempt evidence for timing policy."""
        bounded_limit = max(1, min(int(limit), 100))
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return []
        try:
            rows = con.execute(
                "SELECT id,started_at_ms,finished_at_ms,outcome,snapshot_id,"
                "failure_kind,last_stage,elapsed_ms "
                "FROM snapshot_attempts ORDER BY id DESC LIMIT ?",
                (bounded_limit,),
            ).fetchall()
            keys = (
                "id",
                "started_at_ms",
                "finished_at_ms",
                "outcome",
                "snapshot_id",
                "failure_kind",
                "last_stage",
                "elapsed_ms",
            )
            return [dict(zip(keys, row, strict=True)) for row in rows]
        finally:
            con.close()

    def append_structure_schedule_adjustment(
        self,
        *,
        source_attempt_id: int,
        decided_at_ms: int,
        success_sample_count: int,
        success_p95_s: int | None,
        previous_timeout_s: int,
        previous_cadence_s: int,
        timeout_s: int,
        cadence_s: int,
        reason: str,
    ) -> None:
        """Append one auditable effective-schedule change exactly once."""
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO structure_schedule_adjustments("
                "source_attempt_id,decided_at_ms,success_sample_count,success_p95_s,"
                "previous_timeout_s,previous_cadence_s,timeout_s,cadence_s,reason"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    source_attempt_id,
                    decided_at_ms,
                    success_sample_count,
                    success_p95_s,
                    previous_timeout_s,
                    previous_cadence_s,
                    timeout_s,
                    cadence_s,
                    reason,
                ),
            )
        finally:
            con.close()

    def get_latest_structure_schedule_adjustment(
        self,
    ) -> dict[str, object] | None:
        """Read the newest persisted effective Structure schedule."""
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            row = con.execute(
                "SELECT source_attempt_id,success_sample_count,success_p95_s,"
                "previous_timeout_s,previous_cadence_s,timeout_s,cadence_s,reason "
                "FROM structure_schedule_adjustments ORDER BY id DESC LIMIT 1"
            ).fetchone()
            if row is None:
                return None
            keys = (
                "source_attempt_id",
                "success_sample_count",
                "success_p95_s",
                "previous_timeout_s",
                "previous_cadence_s",
                "timeout_s",
                "cadence_s",
                "reason",
            )
            return dict(zip(keys, row, strict=True))
        finally:
            con.close()

    def count_structure_schedule_adjustments(self) -> int:
        """Return the append-only adjustment count for invariant checks."""
        con = sqlite3.connect(self._db_path)
        try:
            row = con.execute(
                "SELECT COUNT(*) FROM structure_schedule_adjustments"
            ).fetchone()
            return int(row[0]) if row is not None else 0
        finally:
            con.close()

    # ── Phase 03.1 Plan 01: l2_mirror_state singleton (GAP-2 + GAP-3) ──────
    #
    # Freshness anchor for /health l2_tob_age_seconds sub-check. Mirror's
    # success path writes here; /health (Plan 02 wires it) reads via the
    # getter. None = cold start ("never mirrored") → /health caller maps to
    # WARN. Sub-second /health probes MUST NOT round-trip to Supabase, so
    # this lives in the local SQLite file alongside scheduler_state.

    def get_l2_tob_last_mirror_at_s(self) -> int | None:
        """Read last successful L2 mirror push wall-clock (seconds since epoch).

        Returns None when the l2_mirror_state row is absent (cold start) —
        caller treats None as "never mirrored", typically maps to /health WARN.
        Never raises on missing file or missing table; the DDL is created in
        init_schema() but read-only callers may hit a not-yet-initialized DB.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            return None
        try:
            try:
                row = con.execute(
                    "SELECT last_mirror_at_s FROM l2_mirror_state WHERE id=1"
                ).fetchone()
            except sqlite3.OperationalError:
                # Legacy DB without the l2_mirror_state table (pre-03.1).
                return None
            return int(row[0]) if row else None
        finally:
            con.close()

    def upsert_l2_tob_mirror_state(self, last_mirror_at_s: int) -> None:
        """Write singleton row with the latest successful mirror wall-clock.

        Idempotent — uses ON CONFLICT(id) DO UPDATE so repeated calls overwrite
        the single row enforced by CHECK(id=1).
        """
        con = self._connect_writer()
        try:
            con.execute(
                "INSERT INTO l2_mirror_state(id, last_mirror_at_s) "
                "VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET "
                "last_mirror_at_s=excluded.last_mirror_at_s",
                (int(last_mirror_at_s),),
            )
        finally:
            con.close()

    def get_latest_snapshot(self, *, data_product: str | None = None) -> dict | None:
        """Read the most-recent snapshot row for /health endpoint.

        Phase 02 Plan 02 — used by /health handler to determine pass/warn/fail.
        Uses read-only mode=ro URI (P3.8: HTTP server NEVER writes SQLite).
        Returns None if no snapshots exist (first deploy edge case).

        ``data_product`` narrows the result to one explicit product when a
        consumer has a production contract (for example health reads only
        Structure).  ``None`` retains the legacy all-snapshot behavior for
        historical reconciliation tooling.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            # DB file doesn't exist yet → no snapshot
            return None
        try:
            where = "WHERE data_product = ?" if data_product is not None else ""
            params = (data_product,) if data_product is not None else ()
            # Try to read the new Plan 03 columns; fall back gracefully for old DBs
            # that haven't been migrated (supabase_mirror_at_ms + parquet_r2_url).
            try:
                row = con.execute(
                    "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, notes, "
                    "snapshot_status, "
                    "supabase_mirror_at_ms, parquet_r2_url "
                    f"FROM snapshots {where} ORDER BY id DESC LIMIT 1",
                    params,
                ).fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "taken_at_ms": row[1],
                    "finished_at_ms": row[2],
                    "mode": row[3],
                    "is_valid": bool(row[4]),
                    "market_count": row[5],
                    "notes": row[6],
                    "snapshot_status": row[7],
                    "supabase_mirror_at_ms": row[8],  # Phase 02 Plan 03: nullable
                    "parquet_r2_url": row[9],  # Phase 02 Plan 03: nullable
                }
            except sqlite3.OperationalError:
                # Old DB schema without Plan 03 columns — fall back to 7-column query
                row = con.execute(
                    "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, notes "
                    "FROM snapshots ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "taken_at_ms": row[1],
                    "finished_at_ms": row[2],
                    "mode": row[3],
                    "is_valid": bool(row[4]),
                    "market_count": row[5],
                    "notes": row[6],
                    "snapshot_status": None,
                    "supabase_mirror_at_ms": None,
                    "parquet_r2_url": None,
                }
        finally:
            con.close()

    def purge_old_snapshots(
        self,
        *,
        older_than_days: int = 7,
        keep_last: int = 5,
        max_snapshots_per_run: int = 10,
        parquet_root: Path | None = None,
        dry_run: bool = False,
    ) -> tuple[int, list[int]]:
        """Delete one bounded batch older than N days, keeping M most recent.

        Deletes in FK-safe order: validation_issues → markets → event_tags →
        events → snapshots. Also deletes parquet files.

        Bounding each transaction prevents a large historical backlog from growing
        WAL for minutes and losing all progress when a deployment interrupts it.
        Returns (deleted_count, deleted_ids).
        """
        import time as _time

        if max_snapshots_per_run < 1:
            raise ValueError("max_snapshots_per_run must be positive")
        cutoff_ms = int((_time.time() - older_than_days * 86_400) * 1000)

        con = sqlite3.connect(f"file:{self._db_path}?mode=ro", uri=True)
        try:
            # Find IDs to delete: older than cutoff AND not in the last M
            keep_ids = [
                r[0]
                for r in con.execute(
                    "SELECT id FROM snapshots ORDER BY id DESC LIMIT ?",
                    (keep_last,),
                ).fetchall()
            ]
            placeholders = ",".join("?" for _ in keep_ids)
            to_delete = [
                r[0]
                for r in con.execute(
                    f"SELECT id FROM snapshots "
                    f"WHERE taken_at_ms < ? AND id NOT IN ({placeholders}) "
                    f"ORDER BY id LIMIT ?",
                    [cutoff_ms, *keep_ids, max_snapshots_per_run],
                ).fetchall()
            ]
            # Archive ownership is explicit.  A Structure snapshot carries the
            # no-archive marker in parquet_path for compatibility with the old
            # non-null column contract; that marker is not a file to unlink.
            # Never infer deletion ownership from a path-shaped string alone.
            parquet_paths = (
                [
                    r[0]
                    for r in con.execute(
                        f"SELECT parquet_path, archive_status FROM snapshots "
                        f"WHERE id IN ({','.join('?' for _ in to_delete)})",
                        to_delete,
                    ).fetchall()
                    if r[1] != "not_requested"
                ]
                if parquet_root is not None and to_delete
                else []
            )
        finally:
            con.close()

        if not to_delete:
            logger.info("purge_old_snapshots: nothing to delete")
            return (0, [])

        if dry_run:
            logger.info(
                f"purge_old_snapshots DRY-RUN: would delete {len(to_delete)} "
                f"snapshots (ids={to_delete}), {len(parquet_paths)} parquet files"
            )
            return (0, to_delete)

        # Delete in FK-safe order
        con = self._connect_writer()
        try:
            con.execute("PRAGMA journal_mode=WAL")
            con.execute("BEGIN IMMEDIATE")
            try:
                id_placeholders = ",".join("?" for _ in to_delete)
                con.execute(
                    f"DELETE FROM validation_issues WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM snapshot_source_coverage "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM event_market_memberships "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    "DELETE FROM neg_risk_group_truth "
                    f"WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM markets WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM event_tags WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM events WHERE snapshot_id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute(
                    f"DELETE FROM snapshots WHERE id IN ({id_placeholders})",
                    to_delete,
                )
                con.execute("COMMIT")
            except Exception:
                _rollback_without_masking(con)
                logger.exception("purge_old_snapshots rolled back")
                raise
        finally:
            con.close()

        # Delete parquet files (after SQLite commit succeeds)
        deleted_files = 0
        for pp in parquet_paths:
            try:
                p = Path(pp)
                if p.exists():
                    p.unlink()
                    deleted_files += 1
            except OSError:
                logger.warning(f"purge: could not delete parquet file {pp}")

        logger.info(
            f"purge_old_snapshots: deleted {len(to_delete)} snapshots "
            f"(ids={to_delete}), {deleted_files} parquet files"
        )
        return (len(to_delete), to_delete)
