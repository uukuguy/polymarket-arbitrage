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

from loguru import logger

from polyarb.storage.schemas import (
    DDL,
    EVENT_TAGS_COLUMN_ORDER,
    EVENT_TAGS_INSERT_SQL,
    EVENTS_COLUMN_ORDER,
    EVENTS_INSERT_SQL,
    MARKETS_COLUMN_ORDER,
    MARKETS_INSERT_SQL,
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
        """Create tables, indexes, set WAL mode. Idempotent — safe to re-run."""
        con = sqlite3.connect(self._db_path, isolation_level=None)
        try:
            con.executescript(DDL)
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
                "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes"
                ") VALUES (?,?,?,?,?,?,?)",
                (
                    taken_at_ms,
                    finished_at_ms,
                    mode,
                    len(market_rows),
                    int(is_valid),
                    parquet_path,
                    notes,
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
