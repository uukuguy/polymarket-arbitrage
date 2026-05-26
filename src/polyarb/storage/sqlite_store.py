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

import sqlite3
from pathlib import Path
from typing import Iterable

from loguru import logger

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
)
from polyarb.validator.category import Issue

_VALID_MODES = ("subset", "full")

# Booleans that are stored as INTEGER 0/1 in SQLite — convert before insert.
_BOOL_COLUMNS = ("active", "closed", "neg_risk", "incomplete")
# events table also has bool fields stored as INTEGER 0/1.
_EVENT_BOOL_COLUMNS = ("active", "closed")


def _row_to_tuple(row: dict, snapshot_id: int) -> tuple:
    """Project a market dict into the column order required by MARKETS_INSERT_SQL.

    Always overrides the row's snapshot_id with the new id (the orchestrator may
    pass 0 as a placeholder before the snapshot_id is known).
    """
    out: list = []
    for col in MARKETS_COLUMN_ORDER:
        if col == "snapshot_id":
            out.append(snapshot_id)
            continue
        v = row.get(col)
        if col in _BOOL_COLUMNS and v is not None:
            v = int(bool(v))
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
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.executescript(DDL)
            # Phase 02 Plan 02: scheduler_state singleton table
            con.executescript(SCHEDULER_STATE_DDL)
            # Phase 03.1 Plan 01: l2_mirror_state singleton (GAP-2 + GAP-3)
            con.executescript(L2_MIRROR_STATE_DDL)

            # Phase 02 Plan 02-08 (F-01): idempotent ADD COLUMN for legacy DBs.
            # Targets: snapshots.supabase_mirror_at_ms, snapshots.parquet_r2_url.
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
        finally:
            con.close()

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
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
    ) -> int:
        """Persist one snapshot atomically.

        Wraps DELETE FROM markets + INSERT snapshot meta + executemany events +
        executemany event_tags + executemany markets + executemany issues in a
        single BEGIN IMMEDIATE transaction. Any exception triggers ROLLBACK and
        re-raises (we never swallow).

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

        con = sqlite3.connect(self._db_path, isolation_level=None)
        # Per-connection PRAGMAs (some are persistent like journal_mode=WAL after
        # init_schema, but setting again is cheap and safe).
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        try:
            con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    len(market_rows),
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,  # supabase_mirror_at_ms — set after successful mirror push
                    None,  # parquet_r2_url — set after successful R2 upload
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None  # AUTOINCREMENT guarantees this

            # ── Amendment 01: events first (FK target for markets.event_id) ─
            event_tuples = [
                _event_row_to_tuple(r, snapshot_id) for r in event_rows
            ]
            if event_tuples:
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── Amendment 01: event_tags (FK references events.id) ─────────
            event_tag_tuples = [
                _event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows
            ]
            if event_tag_tuples:
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets (references events.id via event_id, FK not enforced) ─
            market_tuples = [_row_to_tuple(r, snapshot_id) for r in market_rows]
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
            con.execute("ROLLBACK")
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
        notes: str | None = None,
        event_rows: list[dict] | None = None,
        event_tag_rows: list[dict] | None = None,
        batch_size: int = 500,
    ) -> tuple[int, int]:
        """Streaming variant of write_snapshot.

        Identical semantics to write_snapshot, but `market_rows` can be any
        iterable (list, generator). Inserts run in batches of `batch_size`
        inside ONE BEGIN IMMEDIATE → COMMIT transaction, so per-snapshot
        atomicity is preserved exactly as the legacy path.

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

        con = sqlite3.connect(self._db_path, isolation_level=None)
        con.execute("PRAGMA journal_mode=WAL")
        con.execute("PRAGMA synchronous=NORMAL")
        con.execute("BEGIN IMMEDIATE")
        market_count = 0
        try:
            con.execute("DELETE FROM markets")  # full overwrite (D-C1)
            cur = con.execute(
                "INSERT INTO snapshots("
                "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes,"
                "supabase_mirror_at_ms,parquet_r2_url"
                ") VALUES (?,?,?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    0,  # placeholder; UPDATEd after stream consumed
                    int(is_valid),
                    parquet_path,
                    notes,
                    None,
                    None,
                ),
            )
            snapshot_id = cur.lastrowid
            assert snapshot_id is not None

            # ── events first (FK target for markets.event_id) ──────────────
            if event_rows:
                event_tuples = [
                    _event_row_to_tuple(r, snapshot_id) for r in event_rows
                ]
                con.executemany(EVENTS_INSERT_SQL, event_tuples)

            # ── event_tags (FK references events.id) ───────────────────────
            if event_tag_rows:
                event_tag_tuples = [
                    _event_tag_row_to_tuple(r, snapshot_id) for r in event_tag_rows
                ]
                con.executemany(EVENT_TAGS_INSERT_SQL, event_tag_tuples)

            # ── markets streamed in batches ────────────────────────────────
            batch: list[tuple] = []
            for row in market_rows:
                batch.append(_row_to_tuple(row, snapshot_id))
                if len(batch) >= batch_size:
                    con.executemany(MARKETS_INSERT_SQL, batch)
                    market_count += len(batch)
                    batch.clear()
            if batch:
                con.executemany(MARKETS_INSERT_SQL, batch)
                market_count += len(batch)
                batch.clear()

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
            con.execute("ROLLBACK")
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
        con = sqlite3.connect(self._db_path, isolation_level=None)
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
        con = sqlite3.connect(self._db_path, isolation_level=None)
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

    def get_latest_snapshot(self) -> dict | None:
        """Read the most-recent snapshot row for /health endpoint.

        Phase 02 Plan 02 — used by /health handler to determine pass/warn/fail.
        Uses read-only mode=ro URI (P3.8: HTTP server NEVER writes SQLite).
        Returns None if no snapshots exist (first deploy edge case).

        Columns returned: id, taken_at_ms, finished_at_ms, mode, status (notes field),
        market_count, is_valid.
        """
        uri = f"file:{self._db_path}?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
        except sqlite3.OperationalError:
            # DB file doesn't exist yet → no snapshot
            return None
        try:
            # Try to read the new Plan 03 columns; fall back gracefully for old DBs
            # that haven't been migrated (supabase_mirror_at_ms + parquet_r2_url).
            try:
                row = con.execute(
                    "SELECT id, taken_at_ms, finished_at_ms, mode, is_valid, market_count, notes, "
                    "supabase_mirror_at_ms, parquet_r2_url "
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
                    "supabase_mirror_at_ms": row[7],  # Phase 02 Plan 03: nullable
                    "parquet_r2_url": row[8],           # Phase 02 Plan 03: nullable
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
        parquet_root: Path | None = None,
        dry_run: bool = False,
    ) -> tuple[int, list[int]]:
        """Delete snapshots older than N days, keeping at least M most recent.

        Deletes in FK-safe order: validation_issues → markets → event_tags →
        events → snapshots. Also deletes parquet files.

        Returns (deleted_count, deleted_ids).
        """
        import time as _time

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
                    f"ORDER BY id",
                    [cutoff_ms, *keep_ids],
                ).fetchall()
            ]
            # Also fetch parquet paths for cleanup
            parquet_paths = [
                r[0]
                for r in con.execute(
                    f"SELECT parquet_path FROM snapshots "
                    f"WHERE id IN ({','.join('?' for _ in to_delete)})",
                    to_delete,
                ).fetchall()
            ] if parquet_root is not None and to_delete else []
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
        con = sqlite3.connect(self._db_path, isolation_level=None)
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
                con.execute("ROLLBACK")
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
