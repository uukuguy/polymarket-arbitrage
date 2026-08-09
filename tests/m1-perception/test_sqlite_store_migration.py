"""Regression tests for SQLiteStore idempotent column-add migration (Plan 02-08 F-01).

Background: Plan 03 added two columns to the `snapshots` table
(supabase_mirror_at_ms BIGINT + parquet_r2_url TEXT). However the schema DDL
uses `CREATE TABLE IF NOT EXISTS snapshots(...)`, which is a no-op on legacy
DBs that already have a `snapshots` table missing those columns. Result:
old dev/prod DBs cannot record mirror state — UPDATE statements raise
`OperationalError: no such column`.

Fix (F-01): after CREATE TABLE, `init_schema()` performs a PRAGMA-based
idempotent ALTER TABLE ADD COLUMN for any missing column. Add-only per
LEARNINGS P7 (never drop, never rename, never alter type).

Tests cover:
1. A simulated legacy snapshots table (without the two new columns) is
   migrated in-place by init_schema() — both columns appear after.
2. The migration is idempotent — calling init_schema() twice on a fresh DB
   does not produce duplicate-column errors.
3. INSERT / SELECT on the new columns works after migration.
4. Data in pre-existing rows is preserved (they get NULL for new columns).
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

from polyarb.storage import sqlite_store as sqlite_store_module
from polyarb.storage.sqlite_store import SQLiteStore


def _create_legacy_snapshots_table(db_path: Path) -> None:
    """Create a pre-Plan-03 snapshots table (missing the two new columns).

    Mirrors the DDL that existed before Phase 02 Plan 03 — no
    supabase_mirror_at_ms, no parquet_r2_url. Inserts one historical row so
    we can verify migration preserves existing data.
    """
    con = sqlite3.connect(db_path, isolation_level=None)
    try:
        con.executescript(
            """
            CREATE TABLE snapshots (
              id             INTEGER PRIMARY KEY AUTOINCREMENT,
              taken_at_ms    INTEGER NOT NULL,
              finished_at_ms INTEGER NOT NULL,
              mode           TEXT NOT NULL,
              market_count   INTEGER NOT NULL,
              is_valid       INTEGER NOT NULL,
              parquet_path   TEXT NOT NULL,
              notes          TEXT
            );
            """
        )
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes"
            ") VALUES (?,?,?,?,?,?,?)",
            (1_700_000_000_000, 1_700_000_060_000, "subset", 42, 1, "/x/y.parquet", "legacy"),
        )
    finally:
        con.close()


def _column_names(db_path: Path, table: str) -> set[str]:
    con = sqlite3.connect(db_path)
    try:
        return {r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()}
    finally:
        con.close()


def test_legacy_db_adds_supabase_mirror_at_ms_column(tmp_path: Path) -> None:
    """Legacy snapshots table (missing two new cols) → init_schema migrates in place."""
    db = tmp_path / "legacy.db"
    _create_legacy_snapshots_table(db)
    assert "supabase_mirror_at_ms" not in _column_names(db, "snapshots")

    store = SQLiteStore(db)
    store.init_schema()

    cols = _column_names(db, "snapshots")
    assert "supabase_mirror_at_ms" in cols
    assert "parquet_r2_url" in cols
    assert "market_view_published" in cols
    assert "data_product" in cols
    assert "archive_status" in cols


def test_legacy_db_preserves_existing_rows(tmp_path: Path) -> None:
    """After migration, the historical row survives; new columns are NULL."""
    db = tmp_path / "legacy.db"
    _create_legacy_snapshots_table(db)

    store = SQLiteStore(db)
    store.init_schema()

    con = sqlite3.connect(db)
    try:
        row = con.execute(
            "SELECT mode, market_count, supabase_mirror_at_ms, parquet_r2_url, "
            "data_product, archive_status "
            "FROM snapshots WHERE id = 1"
        ).fetchone()
    finally:
        con.close()

    assert row is not None, "legacy row must survive migration"
    assert row[0] == "subset"
    assert row[1] == 42
    assert row[2] is None  # new column NULL for pre-migration row
    assert row[3] is None
    assert row[4] == "legacy_combined"
    assert row[5] == "legacy"
    assert _read_market_view_published(db) == 0


def test_migration_is_idempotent(tmp_path: Path) -> None:
    """Calling init_schema repeatedly on a fresh DB never raises duplicate-column."""
    db = tmp_path / "fresh.db"
    store = SQLiteStore(db)
    store.init_schema()
    store.init_schema()  # second pass must not raise
    store.init_schema()  # third pass — defensive

    cols = _column_names(db, "snapshots")
    assert "supabase_mirror_at_ms" in cols
    assert "parquet_r2_url" in cols
    assert "data_product" in cols
    assert "archive_status" in cols


def test_legacy_db_adds_market_truth_tables(tmp_path: Path) -> None:
    db = tmp_path / "legacy.db"
    _create_legacy_snapshots_table(db)

    store = SQLiteStore(db)
    store.init_schema()

    with sqlite3.connect(db) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert {
        "snapshot_source_coverage",
        "event_market_memberships",
        "neg_risk_group_truth",
    } <= tables


def test_migration_idempotent_after_legacy_migration(tmp_path: Path) -> None:
    """Legacy DB migrated once → migrating again is a no-op (no duplicate column)."""
    db = tmp_path / "legacy.db"
    _create_legacy_snapshots_table(db)

    store = SQLiteStore(db)
    store.init_schema()
    store.init_schema()  # would raise OperationalError("duplicate column") without PRAGMA guard

    cols = _column_names(db, "snapshots")
    assert "supabase_mirror_at_ms" in cols
    assert "parquet_r2_url" in cols


def test_new_columns_usable_after_migration(tmp_path: Path) -> None:
    """INSERT and UPDATE on the new columns work after migration."""
    db = tmp_path / "legacy.db"
    _create_legacy_snapshots_table(db)

    store = SQLiteStore(db)
    store.init_schema()

    # UPDATE the new columns on the legacy row
    con = sqlite3.connect(db, isolation_level=None)
    try:
        con.execute(
            "UPDATE snapshots SET supabase_mirror_at_ms = ?, parquet_r2_url = ? WHERE id = 1",
            (1_715_500_000_000, "https://r2.example/snap.parquet"),
        )
        row = con.execute(
            "SELECT supabase_mirror_at_ms, parquet_r2_url FROM snapshots WHERE id = 1"
        ).fetchone()
    finally:
        con.close()

    assert row == (1_715_500_000_000, "https://r2.example/snap.parquet")


def test_legacy_structure_snapshot_backfills_degraded_status(tmp_path: Path) -> None:
    """A pre-status Structure revision keeps its recorded Layer-1 truth after upgrade."""
    db = tmp_path / "legacy-structure.db"
    con = sqlite3.connect(db, isolation_level=None)
    try:
        con.executescript(
            """
            CREATE TABLE snapshots (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              taken_at_ms INTEGER NOT NULL,
              finished_at_ms INTEGER NOT NULL,
              mode TEXT NOT NULL,
              market_count INTEGER NOT NULL,
              market_view_published INTEGER NOT NULL DEFAULT 0,
              data_product TEXT NOT NULL DEFAULT 'legacy_combined',
              archive_status TEXT NOT NULL DEFAULT 'legacy',
              is_valid INTEGER NOT NULL,
              parquet_path TEXT NOT NULL,
              notes TEXT
            );
            CREATE TABLE validation_issues (
              id INTEGER PRIMARY KEY AUTOINCREMENT,
              snapshot_id INTEGER NOT NULL,
              layer INTEGER NOT NULL,
              category TEXT NOT NULL,
              market_id TEXT,
              detail TEXT,
              raw_payload TEXT
            );
            """
        )
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,market_view_published,"
            "data_product,archive_status,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?,?,?,?)",
            (1_700_000_000_000, 1_700_000_060_000, "full", 75_611, 1,
             "structure", "not_requested", 1, "structure://snapshot"),
        )
        con.execute(
            "INSERT INTO validation_issues(snapshot_id,layer,category,detail) "
            "VALUES (1,1,'api_jitter',?)",
            ("Gamma reported 75793 active markets, fetched 75611",),
        )
    finally:
        con.close()

    store = SQLiteStore(db)
    store.init_schema()

    with sqlite3.connect(db) as read_con:
        status = read_con.execute(
            "SELECT snapshot_status FROM snapshots WHERE id=1"
        ).fetchone()[0]
    assert status == "degraded"


def test_structure_status_backfill_does_not_rewrite_settled_snapshots(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The legacy repair must not rescan/rewrite settled production history."""
    db = tmp_path / "settled-structure.db"
    _create_legacy_snapshots_table(db)
    store = SQLiteStore(db)
    store.init_schema()
    with sqlite3.connect(db) as con:
        con.execute(
            "UPDATE snapshots SET data_product='structure',snapshot_status='degraded' "
            "WHERE id=1"
        )

    statements: list[str] = []
    original_connect = sqlite_store_module.sqlite3.connect

    def traced_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        con = original_connect(*args, **kwargs)
        con.set_trace_callback(statements.append)
        return con

    monkeypatch.setattr(sqlite_store_module.sqlite3, "connect", traced_connect)
    SQLiteStore(db).init_schema()

    assert not [
        statement
        for statement in statements
        if statement.startswith("UPDATE snapshots SET snapshot_status")
    ]


def _read_market_view_published(db: Path) -> int:
    with sqlite3.connect(db) as con:
        return int(
            con.execute(
                "SELECT market_view_published FROM snapshots WHERE id=1"
            ).fetchone()[0]
        )
