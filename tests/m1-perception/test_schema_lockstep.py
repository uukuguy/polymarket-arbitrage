"""Schema lockstep invariant tests for Phase 1.1.

Phase 1.1 Plan 01 Task 1 — schemas.py is the **single source of truth** for the
markets table. The 5 sync points (DDL markets cols / DDL question_translations /
MARKETS_COLUMN_ORDER / MARKETS_INSERT_SQL placeholder count / SNAPSHOT_SCHEMA
non-parquet-only fields) MUST agree on column count and names. If they drift,
silent data corruption follows (rows inserted with wrong column mapping).

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
    MARKETS_COLUMN_ORDER,
    MARKETS_INSERT_SQL,
    SNAPSHOT_SCHEMA,
)
from polyarb.storage.sqlite_store import SQLiteStore


# Parquet-only fields (in SNAPSHOT_SCHEMA but NOT in markets DDL/COLUMN_ORDER).
# Currently just the snapshot timestamp injected by the parquet writer.
PARQUET_ONLY_FIELDS = {"snapshot_taken_at_ms"}


def _ddl_markets_columns() -> list[str]:
    """Extract column names from the CREATE TABLE markets (...) block in DDL."""
    m = re.search(
        r"CREATE TABLE IF NOT EXISTS markets\s*\((.+?)\);",
        DDL,
        re.DOTALL,
    )
    assert m is not None, "markets CREATE TABLE block not found in DDL"
    body = m.group(1)
    cols: list[str] = []
    for line in body.splitlines():
        line = line.strip().rstrip(",")
        if not line:
            continue
        # Skip pure comment lines (none currently, but defensive).
        if line.startswith("--"):
            continue
        # First whitespace-separated token is the column name.
        first = line.split()[0]
        # Heuristic: skip table-level constraints like CHECK(...), FOREIGN KEY, etc.
        if first.upper() in {"CHECK", "FOREIGN", "PRIMARY", "UNIQUE", "CONSTRAINT"}:
            continue
        cols.append(first)
    return cols


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
    schema_non_parquet = [
        f.name for f in SNAPSHOT_SCHEMA if f.name not in PARQUET_ONLY_FIELDS
    ]

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
# 2. category field present in all 4 sync points
# ─────────────────────────────────────────────────────────────────────────────


def test_category_field_in_all_4_sync_points() -> None:
    """Phase 1.1 T1: category column added — must appear in DDL + ORDER + SQL + schema."""
    ddl_cols = _ddl_markets_columns()
    schema_names = {f.name for f in SNAPSHOT_SCHEMA}

    assert "category" in ddl_cols, "category not in DDL markets CREATE TABLE"
    assert "category" in MARKETS_COLUMN_ORDER, "category not in MARKETS_COLUMN_ORDER"
    assert "category" in MARKETS_INSERT_SQL, "category not in MARKETS_INSERT_SQL"
    assert "category" in schema_names, "category not in SNAPSHOT_SCHEMA"


def test_tags_field_in_all_4_sync_points() -> None:
    """Phase 1.1 T1: tags column added — must appear in DDL + ORDER + SQL + schema."""
    ddl_cols = _ddl_markets_columns()
    schema_names = {f.name for f in SNAPSHOT_SCHEMA}

    assert "tags" in ddl_cols, "tags not in DDL markets CREATE TABLE"
    assert "tags" in MARKETS_COLUMN_ORDER, "tags not in MARKETS_COLUMN_ORDER"
    assert "tags" in MARKETS_INSERT_SQL, "tags not in MARKETS_INSERT_SQL"
    assert "tags" in schema_names, "tags not in SNAPSHOT_SCHEMA"


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
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
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
        # Insert one translation, then try to insert another row with the same question_en
        # but different question_hash — UNIQUE index on question_en should block it.
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
# 4. Phase 1.1 baseline numbers — exact values lock against silent reordering
# ─────────────────────────────────────────────────────────────────────────────


def test_markets_column_count_is_23_after_phase_1_1() -> None:
    """After Phase 1.1 T1, markets table has 23 columns (Phase 1 had 21 + category + tags)."""
    assert len(MARKETS_COLUMN_ORDER) == 23, (
        f"Expected 23 columns after Phase 1.1, got {len(MARKETS_COLUMN_ORDER)}"
    )
    assert MARKETS_COLUMN_ORDER[-2:] == ("category", "tags"), (
        f"category/tags must be the last two columns, got {MARKETS_COLUMN_ORDER[-2:]}"
    )
