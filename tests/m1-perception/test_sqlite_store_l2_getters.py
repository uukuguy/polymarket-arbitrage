"""Tests for SQLiteStore l2_mirror_state singleton — Phase 03.1 Plan 01 Task 1.

GAP-2 + GAP-3 mechanical fix: provide a freshness anchor for the
`/health l2_tob_age_seconds` sub-check (Plan 02 wires it).

Design (vs CONTEXT.md):
- CONTEXT.md referenced `l2_top_of_book.observed_at_ms` but that table is in
  Supabase Postgres, not in local SQLite, AND the actual schema column is `ts`
  (not `observed_at_ms`). Sub-second /health probes must NOT round-trip to
  Supabase — so the design is a LOCAL SQLite singleton-row cache
  (`l2_mirror_state`) that the mirror's success path writes after each
  successful push. /health reads this cache via SQLiteStore getter.

Singleton table:
    CREATE TABLE IF NOT EXISTS l2_mirror_state (
        id INTEGER PRIMARY KEY CHECK(id=1),
        last_mirror_at_s INTEGER NOT NULL
    )

Getter contract:
- get_l2_tob_last_mirror_at_s() -> int | None
- Returns None when row absent (cold start) WITHOUT raising
- Returns int (epoch seconds wall-clock) when row present

Upsert contract:
- upsert_l2_tob_mirror_state(last_mirror_at_s: int) -> None
- Inserts when missing; overwrites when present (CHECK(id=1) enforces singleton)
"""

from __future__ import annotations

import os
import sqlite3
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


@pytest.fixture
def store(tmp_path: Path):
    """Fresh SQLiteStore + init_schema for each test."""
    from polyarb.storage.sqlite_store import SQLiteStore

    db = tmp_path / "state.db"
    s = SQLiteStore(db)
    s.init_schema()
    return s


def test_getter_returns_none_when_no_row(store) -> None:
    """Cold start: l2_mirror_state has no row → getter returns None, not raise."""
    assert store.get_l2_tob_last_mirror_at_s() is None


def test_upsert_then_get_returns_value(store) -> None:
    """upsert writes singleton row; getter returns stored int."""
    store.upsert_l2_tob_mirror_state(last_mirror_at_s=1_234_567)
    assert store.get_l2_tob_last_mirror_at_s() == 1_234_567


def test_upsert_overwrites_existing_row(store) -> None:
    """Second upsert must overwrite (CHECK(id=1) singleton constraint)."""
    store.upsert_l2_tob_mirror_state(last_mirror_at_s=1_000)
    store.upsert_l2_tob_mirror_state(last_mirror_at_s=2_000)
    assert store.get_l2_tob_last_mirror_at_s() == 2_000

    # Belt-and-braces: confirm exactly one row exists (singleton invariant)
    con = sqlite3.connect(store.db_path)
    try:
        count = con.execute("SELECT COUNT(*) FROM l2_mirror_state").fetchone()[0]
    finally:
        con.close()
    assert count == 1


def test_getter_returns_int_type(store) -> None:
    """Getter returns int, not str — /health math depends on int subtraction."""
    store.upsert_l2_tob_mirror_state(last_mirror_at_s=9_999_999)
    v = store.get_l2_tob_last_mirror_at_s()
    assert isinstance(v, int)
    assert v == 9_999_999
