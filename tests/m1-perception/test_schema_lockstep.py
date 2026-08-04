"""Schema lockstep invariant tests for Phase 1.1.

Phase 1.1 Plan 01 Task 1 — schemas.py is the **single source of truth** for the
markets table. The 5 sync points (DDL markets cols / DDL question_translations /
MARKETS_COLUMN_ORDER / MARKETS_INSERT_SQL placeholder count / SNAPSHOT_SCHEMA
non-parquet-only fields) MUST agree on column count and names. If they drift,
silent data corruption follows (rows inserted with wrong column mapping).

Phase 1.1 Amendment 01 (2026-05-02) reshaped this:
  * markets dropped category/tags columns (Gamma /markets never returned them)
  * markets gained event_id column (FK to events.id)
  * Two new tables: events + event_tags (sourced from Gamma /events endpoint)
  * events / event_tags are SQLite-only (NOT in SNAPSHOT_SCHEMA / parquet)

These tests are designed to fail loudly the moment a future contributor adds a
column to one place but forgets the other four.
"""

from __future__ import annotations

import os
import re
import sqlite3
from pathlib import Path

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

import pytest

from polyarb.storage.schemas import (
    DDL,
    EVENT_TAGS_COLUMN_ORDER,
    EVENT_TAGS_INSERT_SQL,
    EVENTS_COLUMN_ORDER,
    EVENTS_INSERT_SQL,
    MARKETS_COLUMN_ORDER,
    MARKETS_INSERT_SQL,
    SNAPSHOT_SCHEMA,
    STRUCTURE_GENERATIONS_DDL,
)
from polyarb.storage.sqlite_store import SQLiteStore

# Parquet-only fields (in SNAPSHOT_SCHEMA but NOT in markets DDL/COLUMN_ORDER).
# Currently just the snapshot timestamp injected by the parquet writer.
PARQUET_ONLY_FIELDS = {"snapshot_taken_at_ms"}


MARKET_TRUTH_COLUMNS = {
    "snapshot_source_coverage": (
        "snapshot_id",
        "completed",
        "market_items",
        "event_items",
        "failure_source",
        "failure_reason",
    ),
    "event_market_memberships": (
        "snapshot_id",
        "event_id",
        "neg_risk_market_id",
        "market_id",
        "member_kind",
        "active",
        "closed",
    ),
    "neg_risk_group_truth": (
        "snapshot_id",
        "event_id",
        "neg_risk_market_id",
        "neg_risk_type",
        "expected_member_count",
        "active_named_count",
        "membership_hash",
        "quality",
        "reason",
    ),
}


def _ddl_table_columns(table_name: str) -> list[str]:
    """Extract column names from CREATE TABLE <table_name> (...) block in DDL."""
    m = re.search(
        rf"CREATE TABLE IF NOT EXISTS {re.escape(table_name)}\s*\((.+?)\);",
        DDL,
        re.DOTALL,
    )
    assert m is not None, f"{table_name} CREATE TABLE block not found in DDL"
    body = m.group(1)
    cols: list[str] = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        if line.startswith("--"):
            continue
        first = line.split()[0]
        # Skip table-level constraints like PRIMARY KEY (a, b), CHECK(...), etc.
        if (
            first.upper() in {"CHECK", "FOREIGN", "PRIMARY", "UNIQUE", "CONSTRAINT"}
            or first.startswith(("'", '"', ")"))
        ):
            continue
        cols.append(first)
    return cols


@pytest.mark.parametrize(("table_name", "expected"), MARKET_TRUTH_COLUMNS.items())
def test_market_truth_table_columns_match_contract(
    table_name: str,
    expected: tuple[str, ...],
) -> None:
    assert tuple(_ddl_table_columns(table_name)) == expected


def test_market_truth_tables_are_created_by_init_schema(tmp_path: Path) -> None:
    store = SQLiteStore(tmp_path / "truth.db")
    store.init_schema()

    with sqlite3.connect(store.db_path) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

    assert set(MARKET_TRUTH_COLUMNS) <= tables


def test_structure_generation_tables_are_declared_and_created(tmp_path: Path) -> None:
    expected = {
        "structure_publications",
        "structure_generation_events",
        "structure_generation_event_tags",
        "structure_generation_memberships",
        "structure_generation_group_truth",
        "structure_generation_markets",
        "structure_generation_issues",
        "structure_generation_comparison_receipts",
        "structure_generation_comparison_progress",
        "structure_generation_drift_receipts",
        "structure_generation_drift_progress",
        "current_structure_generation",
    }
    for table in expected:
        assert f"CREATE TABLE IF NOT EXISTS {table}" in STRUCTURE_GENERATIONS_DDL

    store = SQLiteStore(tmp_path / "generations.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        actual = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert expected <= actual
    with sqlite3.connect(store.db_path) as con:
        pointer_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info(current_structure_generation)")
        }
    assert {
        "validation_hash",
        "counts_json",
        "certification_component",
        "comparison_receipt_digest",
    } <= pointer_columns
    with sqlite3.connect(store.db_path) as con:
        receipt_columns = {
            row[1]
            for row in con.execute(
                "PRAGMA table_info(structure_generation_comparison_receipts)"
            )
        }
    assert "receipt_digest" in receipt_columns

    with sqlite3.connect(store.db_path) as con:
        drift_progress_columns = {
            row[1]
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_progress)"
            )
        }
        drift_receipt_columns = {
            row[1]
            for row in con.execute(
                "PRAGMA table_info(structure_generation_drift_receipts)"
            )
        }
        drift_triggers = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='trigger' "
                "AND name LIKE 'trg_structure_drift_receipt_%'"
            )
        }
    assert {
        "comparison_id",
        "legacy_snapshot_id",
        "generation_snapshot_id",
        "publication_id",
        "window_id",
        "normalization_contract_version",
        "exact_receipt_digest",
        "pointer_validation_hash",
        "generation_certification_hash",
        "source_event_count",
        "source_market_count",
        "source_event_hash",
        "source_market_hash",
        "source_identity_hash",
        "phase",
        "row_cursor_json",
        "digest_state_json",
        "class_counts_json",
        "class_digests_json",
        "created_at_ms",
        "checkpoint_at_ms",
    } <= drift_progress_columns
    assert {
        "comparison_id",
        "legacy_snapshot_id",
        "legacy_taken_at_ms",
        "legacy_finished_at_ms",
        "legacy_market_count",
        "legacy_universe_hash",
        "legacy_source_truth_hash",
        "generation_snapshot_id",
        "publication_id",
        "window_id",
        "published_snapshot_id",
        "normalization_contract_version",
        "exact_receipt_digest",
        "pointer_validation_hash",
        "generation_certification_hash",
        "source_event_count",
        "source_market_count",
        "source_event_hash",
        "source_market_hash",
        "source_identity_hash",
        "projection_universe_hash",
        "projection_group_truth_hash",
        "generation_universe_hash",
        "generation_group_truth_hash",
        "class_counts_json",
        "class_digests_json",
        "legacy_reconstruction_root",
        "generation_reconstruction_root",
        "overlap_conflict_count",
        "unclassified_count",
        "created_at_ms",
        "receipt_digest",
    } <= drift_receipt_columns
    assert drift_triggers == {
        "trg_structure_drift_receipt_update",
        "trg_structure_drift_receipt_delete",
    }


def test_structure_window_direct_foreign_keys_have_retention_ownership(
    tmp_path: Path,
) -> None:
    """A new window child must declare whether retention deletes or keeps it."""
    heavy_reclaimed = {
        "structure_sync_event_staging",
        "structure_sync_market_staging",
        "structure_sync_event_market_staging",
        "structure_sync_event_metadata_staging",
        "structure_sync_event_member_staging",
        "structure_sync_event_group_truth_staging",
        "structure_sync_event_conflict_proofs",
        "structure_sync_event_conflict_merkle_nodes",
    }
    proof_retained = {
        "structure_sync_event_market_backfill_progress",
        "structure_sync_event_source_progress",
        "structure_sync_event_conflict_summaries",
        "structure_sync_event_source_receipts",
        "structure_sync_event_member_progress",
        "structure_sync_event_group_truth_progress",
        "structure_sync_event_member_receipts",
    }
    independently_protected = {
        "structure_publications",
        "structure_generation_drift_progress",
        "structure_generation_drift_receipts",
        "structure_generation_drift_terminal_receipts",
    }
    classifications = (
        heavy_reclaimed,
        proof_retained,
        independently_protected,
    )
    assert not any(left & right for index, left in enumerate(classifications)
                   for right in classifications[index + 1:])

    store = SQLiteStore(tmp_path / "window-fks.db")
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        tables = {
            str(row[0])
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        direct_children = {
            table
            for table in tables
            for foreign_key in con.execute(f"PRAGMA foreign_key_list({table})")
            if foreign_key[2] == "structure_sync_windows"
            and foreign_key[3] == "window_id"
            and foreign_key[4] == "id"
        }

    assert direct_children == set().union(*classifications)


def _ddl_markets_columns() -> list[str]:
    return _ddl_table_columns("markets")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Column count consistency across the 4 markets-table sync points
# ─────────────────────────────────────────────────────────────────────────────


def test_lockstep_column_count_matches() -> None:
    """The 4 markets-table sync points must agree on column count.

    Sync points:
    - DDL markets CREATE TABLE column count
    - len(MARKETS_COLUMN_ORDER)
    - MARKETS_INSERT_SQL placeholder count
    - SNAPSHOT_SCHEMA non-parquet-only field count
    """
    ddl_cols = _ddl_markets_columns()
    placeholder_count = MARKETS_INSERT_SQL.count("?")
    schema_non_parquet = [f.name for f in SNAPSHOT_SCHEMA if f.name not in PARQUET_ONLY_FIELDS]

    assert len(ddl_cols) == len(MARKETS_COLUMN_ORDER), (
        f"DDL markets column count {len(ddl_cols)} != "
        f"len(MARKETS_COLUMN_ORDER) {len(MARKETS_COLUMN_ORDER)}"
    )
    assert placeholder_count == len(MARKETS_COLUMN_ORDER), (
        f"INSERT placeholder count {placeholder_count} != "
        f"len(MARKETS_COLUMN_ORDER) {len(MARKETS_COLUMN_ORDER)}"
    )
    assert len(schema_non_parquet) == len(MARKETS_COLUMN_ORDER), (
        f"SNAPSHOT_SCHEMA non-parquet-only count {len(schema_non_parquet)} != "
        f"len(MARKETS_COLUMN_ORDER) {len(MARKETS_COLUMN_ORDER)}"
    )


def test_lockstep_column_names_match() -> None:
    """DDL column names == MARKETS_COLUMN_ORDER (same names, same order)."""
    ddl_cols = _ddl_markets_columns()
    assert tuple(ddl_cols) == MARKETS_COLUMN_ORDER, (
        f"DDL column order does not match MARKETS_COLUMN_ORDER:\n"
        f"  DDL: {ddl_cols}\n  ORDER: {list(MARKETS_COLUMN_ORDER)}"
    )


def test_snapshot_schema_includes_all_markets_columns() -> None:
    """Every column in MARKETS_COLUMN_ORDER must appear in SNAPSHOT_SCHEMA."""
    schema_names = {f.name for f in SNAPSHOT_SCHEMA}
    missing = set(MARKETS_COLUMN_ORDER) - schema_names
    assert not missing, f"SNAPSHOT_SCHEMA missing markets columns: {missing}"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Amendment 01: markets has event_id (no category/tags)
# ─────────────────────────────────────────────────────────────────────────────


def test_event_id_field_in_all_4_sync_points() -> None:
    """Phase 1.1 Amendment 01: event_id column added (replaces category/tags)."""
    ddl_cols = _ddl_markets_columns()
    schema_names = {f.name for f in SNAPSHOT_SCHEMA}

    assert "event_id" in ddl_cols, "event_id not in DDL markets CREATE TABLE"
    assert "event_id" in MARKETS_COLUMN_ORDER, "event_id not in MARKETS_COLUMN_ORDER"
    assert "event_id" in MARKETS_INSERT_SQL, "event_id not in MARKETS_INSERT_SQL"
    assert "event_id" in schema_names, "event_id not in SNAPSHOT_SCHEMA"


def test_category_and_tags_removed_from_markets() -> None:
    """Phase 1.1 Amendment 01: category and tags columns must be GONE from markets.

    They never had real data — Gamma /markets returns NULL/[] for these fields.
    They live on events instead (via event_tags many-to-many).
    """
    ddl_cols = _ddl_markets_columns()
    schema_names = {f.name for f in SNAPSHOT_SCHEMA}

    assert "category" not in ddl_cols, "category should be removed from markets DDL"
    assert "category" not in MARKETS_COLUMN_ORDER, (
        "category should be removed from MARKETS_COLUMN_ORDER"
    )
    # MARKETS_INSERT_SQL is a substring search — must NOT mention category
    assert "category" not in MARKETS_INSERT_SQL, (
        "category should be removed from MARKETS_INSERT_SQL"
    )
    assert "category" not in schema_names, "category should be removed from SNAPSHOT_SCHEMA"

    assert "tags" not in ddl_cols, "tags should be removed from markets DDL"
    assert "tags" not in MARKETS_COLUMN_ORDER, "tags should be removed from MARKETS_COLUMN_ORDER"
    assert "tags" not in MARKETS_INSERT_SQL, "tags should be removed from MARKETS_INSERT_SQL"
    assert "tags" not in schema_names, "tags should be removed from SNAPSHOT_SCHEMA"


# ─────────────────────────────────────────────────────────────────────────────
# 3. question_translations table — Phase 1.1 T2
# ─────────────────────────────────────────────────────────────────────────────


def test_question_translations_ddl_exists(tmp_path: Path) -> None:
    """init_schema() must create question_translations table (idempotent)."""
    store = SQLiteStore(tmp_path / "qt.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        con.close()
    assert "question_translations" in tables, (
        "question_translations table not created by init_schema()"
    )


def test_question_translations_columns(tmp_path: Path) -> None:
    """question_translations must have all 8 columns from CONTEXT.md T2 schema."""
    store = SQLiteStore(tmp_path / "qt.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        cur = con.execute("PRAGMA table_info(question_translations)")
        cols = {row[1]: row for row in cur.fetchall()}
    finally:
        con.close()

    expected = {
        "question_hash",
        "question_en",
        "question_zh",
        "translator_model",
        "translated_at_ms",
        "token_cost",
        "retry_count",
        "is_dead",
    }
    actual = set(cols.keys())
    missing = expected - actual
    assert not missing, f"question_translations missing columns: {missing}"

    # question_hash must be the PRIMARY KEY (PRAGMA table_info pk col is index 5).
    pk_col = cols["question_hash"][5]
    assert pk_col == 1, f"question_hash should be PK (pk={pk_col})"

    # NOT NULL invariants (PRAGMA table_info notnull col is index 3).
    assert cols["question_en"][3] == 1, "question_en must be NOT NULL"
    assert cols["question_zh"][3] == 1, "question_zh must be NOT NULL"
    assert cols["translator_model"][3] == 1, "translator_model must be NOT NULL"
    assert cols["translated_at_ms"][3] == 1, "translated_at_ms must be NOT NULL"
    assert cols["retry_count"][3] == 1, "retry_count must be NOT NULL (default 0)"
    assert cols["is_dead"][3] == 1, "is_dead must be NOT NULL (default 0)"


def test_question_translations_unique_question_en(tmp_path: Path) -> None:
    """idx_qt_question_en UNIQUE index protects plan 02 LEFT JOIN from duplicates."""
    store = SQLiteStore(tmp_path / "qt.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        con.execute(
            "INSERT INTO question_translations("
            "question_hash, question_en, question_zh, translator_model, translated_at_ms"
            ") VALUES (?,?,?,?,?)",
            ("h1", "Will X happen?", "X 会发生吗？", "deepseek-chat", 1_000_000),
        )
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(
                "INSERT INTO question_translations("
                "question_hash, question_en, question_zh, translator_model, translated_at_ms"
                ") VALUES (?,?,?,?,?)",
                ("h2", "Will X happen?", "另一翻译", "qwen-turbo", 2_000_000),
            )
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# 4. Amendment 01 — events table
# ─────────────────────────────────────────────────────────────────────────────


def test_events_table_exists(tmp_path: Path) -> None:
    """init_schema() must create events table."""
    store = SQLiteStore(tmp_path / "ev.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        con.close()
    assert "events" in tables, "events table not created by init_schema()"


def test_events_columns_match_constants() -> None:
    """events DDL columns must match EVENTS_COLUMN_ORDER + EVENTS_INSERT_SQL."""
    ddl_cols = _ddl_table_columns("events")
    placeholder_count = EVENTS_INSERT_SQL.count("?")

    assert tuple(ddl_cols) == EVENTS_COLUMN_ORDER, (
        f"events DDL columns don't match EVENTS_COLUMN_ORDER:\n"
        f"  DDL: {ddl_cols}\n  ORDER: {list(EVENTS_COLUMN_ORDER)}"
    )
    assert placeholder_count == len(EVENTS_COLUMN_ORDER), (
        f"EVENTS_INSERT_SQL placeholder count {placeholder_count} != "
        f"len(EVENTS_COLUMN_ORDER) {len(EVENTS_COLUMN_ORDER)}"
    )


def test_events_composite_primary_key(tmp_path: Path) -> None:
    """events PK is (id, snapshot_id) — same event id can recur across snapshots."""
    store = SQLiteStore(tmp_path / "ev.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        # Insert prereq snapshot row.
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?)",
            (1, 2, "subset", 0, 1, "/tmp/x.parquet"),
        )
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?)",
            (3, 4, "subset", 0, 1, "/tmp/y.parquet"),
        )
        # Same event_id "EV-1" in TWO different snapshots — must NOT collide.
        # Phase 02 Plan 01: EVENTS_INSERT_SQL now has 12 columns (added page_fetched_at_ms)
        con.execute(EVENTS_INSERT_SQL, ("EV-1", "ev-1", "T1", "TKR", 1, 0, 0, 0, 0, 0, None, 1))
        con.execute(EVENTS_INSERT_SQL, ("EV-1", "ev-1", "T1", "TKR", 1, 0, 0, 0, 0, 0, None, 2))
        # But same (id, snapshot_id) must be rejected.
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(EVENTS_INSERT_SQL, ("EV-1", "ev-1", "T1", "TKR", 1, 0, 0, 0, 0, 0, None, 1))
    finally:
        con.close()


# ─────────────────────────────────────────────────────────────────────────────
# 5. Amendment 01 — event_tags table
# ─────────────────────────────────────────────────────────────────────────────


def test_event_tags_table_exists(tmp_path: Path) -> None:
    """init_schema() must create event_tags table."""
    store = SQLiteStore(tmp_path / "et.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        }
    finally:
        con.close()
    assert "event_tags" in tables, "event_tags table not created by init_schema()"


def test_event_tags_columns_match_constants() -> None:
    """event_tags DDL columns must match EVENT_TAGS_COLUMN_ORDER + EVENT_TAGS_INSERT_SQL."""
    ddl_cols = _ddl_table_columns("event_tags")
    placeholder_count = EVENT_TAGS_INSERT_SQL.count("?")

    assert tuple(ddl_cols) == EVENT_TAGS_COLUMN_ORDER, (
        f"event_tags DDL columns don't match EVENT_TAGS_COLUMN_ORDER:\n"
        f"  DDL: {ddl_cols}\n  ORDER: {list(EVENT_TAGS_COLUMN_ORDER)}"
    )
    assert placeholder_count == len(EVENT_TAGS_COLUMN_ORDER), (
        f"EVENT_TAGS_INSERT_SQL placeholder count {placeholder_count} != "
        f"len(EVENT_TAGS_COLUMN_ORDER) {len(EVENT_TAGS_COLUMN_ORDER)}"
    )


def test_event_tags_composite_primary_key(tmp_path: Path) -> None:
    """event_tags PK = (event_id, tag_id, snapshot_id)."""
    store = SQLiteStore(tmp_path / "et.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        con.execute(
            "INSERT INTO snapshots("
            "taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path"
            ") VALUES (?,?,?,?,?,?)",
            (1, 2, "subset", 0, 1, "/tmp/x.parquet"),
        )
        # Different tag_id on same event in same snapshot — OK.
        con.execute(EVENT_TAGS_INSERT_SQL, ("EV-1", "120", "Finance", "finance", 1))
        con.execute(EVENT_TAGS_INSERT_SQL, ("EV-1", "100328", "Economy", "economy", 1))
        # Duplicate (event_id, tag_id, snapshot_id) must be rejected.
        with pytest.raises(sqlite3.IntegrityError):
            con.execute(EVENT_TAGS_INSERT_SQL, ("EV-1", "120", "Finance", "finance", 1))
    finally:
        con.close()


def test_event_tags_indexes_exist(tmp_path: Path) -> None:
    """idx_event_tags_label and idx_event_tags_slug must exist for tag lookups."""
    store = SQLiteStore(tmp_path / "et.db")
    store.init_schema()
    con = sqlite3.connect(store.db_path)
    try:
        idx_names = {
            r[0]
            for r in con.execute("SELECT name FROM sqlite_master WHERE type='index'").fetchall()
        }
    finally:
        con.close()
    assert "idx_event_tags_label" in idx_names
    assert "idx_event_tags_slug" in idx_names
    assert "idx_event_tags_snapshot" in idx_names


# ─────────────────────────────────────────────────────────────────────────────
# 6. Phase 1.1 Amendment 01 baseline — exact column count
# ─────────────────────────────────────────────────────────────────────────────


def test_markets_column_count_is_23_after_phase_02_plan_01() -> None:
    """After Phase 02 Plan 01, markets table has 23 columns.

    Phase 1: 21 columns
    Phase 1.1 pre-amendment: 23 columns (added category + tags)
    Phase 1.1 Amendment 01: 22 columns (-category -tags +event_id)
    Phase 02 Plan 01: 23 columns (+page_fetched_at_ms, fixes L2 schema misunderstanding)
    """
    assert len(MARKETS_COLUMN_ORDER) == 23, (
        f"Expected 23 columns after Phase 02 Plan 01, got {len(MARKETS_COLUMN_ORDER)}"
    )
    # event_id is last column; page_fetched_at_ms is just before snapshot_id
    assert MARKETS_COLUMN_ORDER[-1] == "event_id", (
        f"event_id must be the last markets column, got {MARKETS_COLUMN_ORDER[-1]}"
    )
    assert "page_fetched_at_ms" in MARKETS_COLUMN_ORDER, (
        "page_fetched_at_ms must be in MARKETS_COLUMN_ORDER after Phase 02 Plan 01"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Phase 02 Plan 01 — 4-point lockstep for page_fetched_at_ms (Wave 0 RED test)
# ─────────────────────────────────────────────────────────────────────────────


def test_page_fetched_at_ms_in_all_four_sync_points() -> None:
    """Phase 02 Plan 01: page_fetched_at_ms must be present in all 4 sync points
    for the markets table, and 3 sync points for the events table (no parquet schema).

    This test will fail (RED) until Task 2 implements the 4-point lockstep in schemas.py.

    Sync points for markets:
    1. DDL CREATE TABLE markets - must have page_fetched_at_ms INTEGER (nullable)
    2. MARKETS_COLUMN_ORDER - must include "page_fetched_at_ms"
    3. MARKETS_INSERT_SQL - must include column name and matching ? placeholder
    4. SNAPSHOT_SCHEMA (pyarrow) - must have nullable int64 page_fetched_at_ms

    Sync points for events (no parquet schema — 3 points only):
    1. DDL CREATE TABLE events - must have page_fetched_at_ms INTEGER (nullable)
    2. EVENTS_COLUMN_ORDER - must include "page_fetched_at_ms"
    3. EVENTS_INSERT_SQL - must include column name and matching ? placeholder
    """
    # ── 1. markets DDL ────────────────────────────────────────────────────────
    markets_ddl_cols = _ddl_markets_columns()
    assert "page_fetched_at_ms" in markets_ddl_cols, (
        "page_fetched_at_ms not found in DDL CREATE TABLE markets"
    )
    # Verify it appears as INTEGER (nullable — no NOT NULL constraint)
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS markets\s*\((.+?)\);",
        DDL,
        re.DOTALL,
    )
    assert m is not None
    ddl_body = m.group(1)
    # The column definition line must be non-comment: e.g. "  page_fetched_at_ms INTEGER,"
    # Filter out comment lines and search only in non-comment lines.
    non_comment_lines = [ln for ln in ddl_body.splitlines() if not ln.strip().startswith("--")]
    non_comment_body = "\n".join(non_comment_lines)
    page_col_match = re.search(
        r"^\s*page_fetched_at_ms\s+(\w+)(.*?)$",
        non_comment_body,
        re.MULTILINE,
    )
    assert page_col_match is not None, (
        "page_fetched_at_ms column definition not found in DDL (non-comment lines)"
    )
    assert page_col_match.group(1).upper() == "INTEGER", (
        f"page_fetched_at_ms must be INTEGER type, got {page_col_match.group(1)}"
    )
    # Must NOT have NOT NULL (it's nullable for pre-02 snapshots backward compat)
    assert "NOT NULL" not in page_col_match.group(2).upper(), (
        "page_fetched_at_ms must be nullable (no NOT NULL constraint) for backward compat"
    )

    # ── 2. MARKETS_COLUMN_ORDER ───────────────────────────────────────────────
    assert "page_fetched_at_ms" in MARKETS_COLUMN_ORDER, (
        "page_fetched_at_ms not in MARKETS_COLUMN_ORDER"
    )

    # ── 3. MARKETS_INSERT_SQL ─────────────────────────────────────────────────
    assert "page_fetched_at_ms" in MARKETS_INSERT_SQL, (
        "page_fetched_at_ms not in MARKETS_INSERT_SQL column list"
    )
    # Placeholder count must match MARKETS_COLUMN_ORDER length
    placeholder_count = MARKETS_INSERT_SQL.count("?")
    assert placeholder_count == len(MARKETS_COLUMN_ORDER), (
        f"MARKETS_INSERT_SQL placeholder count {placeholder_count} != "
        f"len(MARKETS_COLUMN_ORDER) {len(MARKETS_COLUMN_ORDER)} after adding page_fetched_at_ms"
    )

    # ── 4. SNAPSHOT_SCHEMA (pyarrow) ──────────────────────────────────────────
    import pyarrow as pa

    schema_field_names = {f.name for f in SNAPSHOT_SCHEMA}
    assert "page_fetched_at_ms" in schema_field_names, (
        "page_fetched_at_ms not found in SNAPSHOT_SCHEMA"
    )
    field = SNAPSHOT_SCHEMA.field("page_fetched_at_ms")
    assert field.type == pa.int64(), (
        f"SNAPSHOT_SCHEMA page_fetched_at_ms must be pa.int64(), got {field.type}"
    )
    assert field.nullable is True, (
        "SNAPSHOT_SCHEMA page_fetched_at_ms must be nullable=True (backward compat)"
    )

    # ── events: 3-point sync (no parquet schema for events) ───────────────────
    events_ddl_cols = _ddl_table_columns("events")
    assert "page_fetched_at_ms" in events_ddl_cols, (
        "page_fetched_at_ms not found in DDL CREATE TABLE events"
    )
    assert "page_fetched_at_ms" in EVENTS_COLUMN_ORDER, (
        "page_fetched_at_ms not in EVENTS_COLUMN_ORDER"
    )
    assert "page_fetched_at_ms" in EVENTS_INSERT_SQL, "page_fetched_at_ms not in EVENTS_INSERT_SQL"
    # events placeholder count must also match
    events_placeholder_count = EVENTS_INSERT_SQL.count("?")
    assert events_placeholder_count == len(EVENTS_COLUMN_ORDER), (
        f"EVENTS_INSERT_SQL placeholder count {events_placeholder_count} != "
        f"len(EVENTS_COLUMN_ORDER) {len(EVENTS_COLUMN_ORDER)}"
    )


# =============================================================================
# Phase 02 Plan 02: scheduler_state singleton table lockstep
# =============================================================================


# =============================================================================
# Phase 02 Plan 03: snapshots table 3-point lockstep (supabase_mirror_at_ms + parquet_r2_url)
# =============================================================================


def test_supabase_mirror_at_ms_in_snapshots_three_sync_points() -> None:
    """supabase_mirror_at_ms must appear in all 3 snapshots sync points.

    3-point lockstep (no parquet schema — snapshots table is SQLite-only):
      1. SNAPSHOTS_DDL
      2. SNAPSHOTS_COLUMN_ORDER
      3. SNAPSHOTS_INSERT_SQL
    """
    from polyarb.storage.schemas import SNAPSHOTS_COLUMN_ORDER, SNAPSHOTS_DDL, SNAPSHOTS_INSERT_SQL

    # Sync point 1: DDL string
    assert "supabase_mirror_at_ms" in SNAPSHOTS_DDL, (
        "supabase_mirror_at_ms missing from SNAPSHOTS_DDL"
    )
    # Sync point 2: column order tuple
    assert "supabase_mirror_at_ms" in SNAPSHOTS_COLUMN_ORDER, (
        "supabase_mirror_at_ms missing from SNAPSHOTS_COLUMN_ORDER"
    )
    # Sync point 3: INSERT SQL
    assert "supabase_mirror_at_ms" in SNAPSHOTS_INSERT_SQL, (
        "supabase_mirror_at_ms missing from SNAPSHOTS_INSERT_SQL"
    )
    # Placeholder count matches column count (excluding auto PK 'id')
    # SNAPSHOTS_INSERT_SQL inserts all columns except id (autoincrement)
    insert_cols = [c for c in SNAPSHOTS_COLUMN_ORDER if c != "id"]
    n_cols = len(insert_cols)
    n_placeholders = SNAPSHOTS_INSERT_SQL.count("?")
    assert n_cols == n_placeholders, f"col count {n_cols} != placeholder count {n_placeholders}"


def test_parquet_r2_url_in_snapshots_three_sync_points() -> None:
    """parquet_r2_url must appear in all 3 snapshots sync points."""
    from polyarb.storage.schemas import SNAPSHOTS_COLUMN_ORDER, SNAPSHOTS_DDL, SNAPSHOTS_INSERT_SQL

    assert "parquet_r2_url" in SNAPSHOTS_DDL, "parquet_r2_url missing from SNAPSHOTS_DDL"
    assert "parquet_r2_url" in SNAPSHOTS_COLUMN_ORDER, (
        "parquet_r2_url missing from SNAPSHOTS_COLUMN_ORDER"
    )
    assert "parquet_r2_url" in SNAPSHOTS_INSERT_SQL, (
        "parquet_r2_url missing from SNAPSHOTS_INSERT_SQL"
    )


def test_market_view_published_in_snapshots_three_sync_points() -> None:
    from polyarb.storage.schemas import SNAPSHOTS_COLUMN_ORDER, SNAPSHOTS_DDL, SNAPSHOTS_INSERT_SQL

    assert "market_view_published" in SNAPSHOTS_DDL
    assert "market_view_published" in SNAPSHOTS_COLUMN_ORDER
    assert "market_view_published" in SNAPSHOTS_INSERT_SQL
    assert SNAPSHOTS_INSERT_SQL.count("?") == len(SNAPSHOTS_COLUMN_ORDER) - 1


def test_scheduler_state_table_present_in_schema_and_executable() -> None:
    """SCHEDULER_STATE_DDL declares scheduler_state table with singleton constraint.

    Plan 02 (BLOCKER-4 fix): new scheduler_state singleton table must exist in
    schemas.py DDL + be valid SQLite + enforce CHECK (id = 1).
    This is a 1-point lockstep (no COLUMN_ORDER / INSERT_SQL / parquet schema
    because scheduler_state has no parquet mirror and is a singleton).
    """
    import sqlite3

    from polyarb.storage.schemas import SCHEDULER_STATE_DDL

    # DDL string assertions
    assert "scheduler_state" in SCHEDULER_STATE_DDL, "table name missing from SCHEDULER_STATE_DDL"
    assert "failure_counter" in SCHEDULER_STATE_DDL, "failure_counter column missing"
    assert "CHECK (id = 1)" in SCHEDULER_STATE_DDL, "singleton CHECK constraint missing"

    # Verify DDL is valid SQLite by executing it in-memory
    con = sqlite3.connect(":memory:")
    con.executescript(SCHEDULER_STATE_DDL)
    row = con.execute("SELECT name FROM sqlite_master WHERE name='scheduler_state'").fetchone()
    assert row is not None, "scheduler_state table not created by SCHEDULER_STATE_DDL"
    con.close()


def test_structure_publication_schema_persists_normalization_contract_version() -> None:
    """A publication must carry the semantic contract that produced its rows."""
    from polyarb.storage.schemas import STRUCTURE_GENERATIONS_DDL

    con = sqlite3.connect(":memory:")
    con.execute("PRAGMA foreign_keys=OFF")
    con.executescript(STRUCTURE_GENERATIONS_DDL)

    columns = {
        str(row[1]): str(row[2])
        for row in con.execute("PRAGMA table_info(structure_publications)")
    }
    con.close()

    assert columns["normalization_contract_version"] == "TEXT"
