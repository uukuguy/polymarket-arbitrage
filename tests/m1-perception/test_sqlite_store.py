"""Unit tests for polyarb.storage.sqlite_store.SQLiteStore.

Verifies:
- WAL pragma + 3-table schema creation (idempotent)
- BEGIN IMMEDIATE + DELETE FROM markets overwrite semantics (anti-pattern #1 NOT used)
- snapshots table is append-only across multiple write_snapshot calls
- validation_issues records category + layer correctly
- is_valid=False still persists (D-D3)
- write_snapshot returns the new snapshots.id (FK matches markets.snapshot_id)
- ValueError on invalid mode
- uint256-style 70-char token IDs round-trip as exact strings (Pitfall 3)
- ROLLBACK on executemany failure leaves the markets table empty
"""

from __future__ import annotations

# Belt-and-suspenders for F-3 path validator (this test does not import Settings,
# but if conftest is added later in Plan 01-5 we want this to keep working).
import os

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

import sqlite3
from pathlib import Path

import pytest

from polyarb.storage.sqlite_store import SQLiteStore
from polyarb.validator.category import Category, Issue


def make_market(market_id: str, **overrides) -> dict:
    """Build a fully-populated market dict suitable for write_snapshot."""
    base = dict(
        market_id=market_id,
        condition_id=f"c-{market_id}",
        slug=None,
        question=None,
        yes_token_id="1" * 70,
        no_token_id="2" * 70,
        mid_price=0.5,
        liquidity_usd=1000.0,
        volume_usd=100.0,
        best_bid_price=0.49,
        best_bid_size=100.0,
        best_ask_price=0.51,
        best_ask_size=100.0,
        end_time_ms=2_000_000_000_000,
        active=1,
        closed=0,
        neg_risk=0,
        neg_risk_market_id=None,
        fetched_at_ms=1_714_435_200_000,
        snapshot_id=0,  # placeholder; write_snapshot overrides via _row_to_tuple
        incomplete=0,
    )
    base.update(overrides)
    return base


@pytest.fixture
def store(tmp_path: Path) -> SQLiteStore:
    s = SQLiteStore(tmp_path / "t.db")
    s.init_schema()
    return s


# ---------- 1. init_schema ----------------------------------------------------


def test_init_schema_creates_three_tables(store: SQLiteStore) -> None:
    con = sqlite3.connect(store.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"snapshots", "markets", "validation_issues"} <= tables
        # WAL pragma is persistent at the DB level; should report 'wal'.
        mode = con.execute("PRAGMA journal_mode").fetchone()[0]
        assert mode.lower() == "wal"
    finally:
        con.close()


def test_init_schema_idempotent(tmp_path: Path) -> None:
    s = SQLiteStore(tmp_path / "t.db")
    s.init_schema()
    s.init_schema()  # second call must not raise
    con = sqlite3.connect(s.db_path)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        assert {"snapshots", "markets", "validation_issues"} <= tables
    finally:
        con.close()


# ---------- 2. overwrite semantics (anti-pattern #1) -------------------------


def test_write_snapshot_overwrites_markets(store: SQLiteStore) -> None:
    """The second snapshot must REPLACE markets — never accumulate rows from snapshot 1."""
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a"), make_market("b")],
        issues=[],
    )
    store.write_snapshot(
        taken_at_ms=2_000_000,
        finished_at_ms=2_000_100,
        mode="subset",
        parquet_path="x/2.parquet",
        is_valid=True,
        market_rows=[make_market("c")],
        issues=[],
    )

    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute("SELECT market_id FROM markets").fetchall()
    finally:
        con.close()
    assert rows == [("c",)], (
        "Expected only the latest snapshot's rows. INSERT OR REPLACE alone "
        "would leak 'a' and 'b' from the first snapshot."
    )


def test_write_snapshot_appends_to_snapshots_table(store: SQLiteStore) -> None:
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a")],
        issues=[],
    )
    store.write_snapshot(
        taken_at_ms=2_000_000,
        finished_at_ms=2_000_100,
        mode="full",
        parquet_path="x/2.parquet",
        is_valid=True,
        market_rows=[make_market("b")],
        issues=[],
    )
    con = sqlite3.connect(store.db_path)
    try:
        n = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    finally:
        con.close()
    assert n == 2


# ---------- 3. validation_issues ----------------------------------------------


def test_write_snapshot_records_issues_with_category(store: SQLiteStore) -> None:
    issues = [
        Issue(layer=2, category=Category.ZOMBIE_MARKET, market_id="m1", detail="low liq"),
        Issue(layer=4, category=Category.GHOST_BOOK, market_id="m2", detail="fake book"),
    ]
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("m1"), make_market("m2")],
        issues=issues,
    )
    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute(
            "SELECT category, layer FROM validation_issues ORDER BY layer"
        ).fetchall()
    finally:
        con.close()
    assert rows == [("zombie_market", 2), ("ghost_book", 4)]


# ---------- 4. is_valid=False still persists (D-D3) ---------------------------


def test_write_snapshot_invalid_still_persists(store: SQLiteStore) -> None:
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=False,
        market_rows=[make_market("a")],
        issues=[],
    )
    con = sqlite3.connect(store.db_path)
    try:
        row = con.execute(
            "SELECT is_valid, market_count FROM snapshots"
        ).fetchone()
    finally:
        con.close()
    assert row == (0, 1), "D-D3: is_valid=False rows must be queryable"


# ---------- 5. snapshot_id ----------------------------------------------------


def test_write_snapshot_returns_snapshot_id(store: SQLiteStore) -> None:
    sid = store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a")],
        issues=[Issue(layer=2, category=Category.UNKNOWN, market_id="a", detail="x")],
    )
    assert isinstance(sid, int) and sid >= 1

    con = sqlite3.connect(store.db_path)
    try:
        market_sid = con.execute(
            "SELECT snapshot_id FROM markets WHERE market_id='a'"
        ).fetchone()[0]
        issue_sid = con.execute(
            "SELECT snapshot_id FROM validation_issues"
        ).fetchone()[0]
    finally:
        con.close()
    assert market_sid == sid
    assert issue_sid == sid


# ---------- 6. invalid mode ---------------------------------------------------


def test_write_snapshot_invalid_mode_raises(store: SQLiteStore) -> None:
    with pytest.raises(ValueError):
        store.write_snapshot(
            taken_at_ms=1,
            finished_at_ms=2,
            mode="weekly",  # not in {"subset", "full"}
            parquet_path="x/1.parquet",
            is_valid=True,
            market_rows=[],
            issues=[],
        )


# ---------- 7. uint256 token IDs ---------------------------------------------


def test_token_ids_preserve_uint256_string(store: SQLiteStore) -> None:
    """Pitfall 3: 70-char numeric token IDs must round-trip as exact strings."""
    big_token = "1" * 70  # 70 decimal digits — overflows int64
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("a", yes_token_id=big_token)],
        issues=[],
    )
    con = sqlite3.connect(store.db_path)
    try:
        got = con.execute(
            "SELECT yes_token_id FROM markets WHERE market_id='a'"
        ).fetchone()[0]
    finally:
        con.close()
    assert got == big_token


# ---------- 8. rollback on failure --------------------------------------------


def test_rollback_on_executemany_failure(store: SQLiteStore) -> None:
    """If executemany fails mid-transaction, markets must remain empty (atomicity)."""
    # First, insert a baseline so we can verify DELETE FROM markets ran but then rolled back.
    store.write_snapshot(
        taken_at_ms=1_000_000,
        finished_at_ms=1_000_100,
        mode="subset",
        parquet_path="x/1.parquet",
        is_valid=True,
        market_rows=[make_market("baseline")],
        issues=[],
    )

    # Build a row missing the NOT NULL `condition_id` to force an integrity error.
    bad = make_market("bad")
    bad["condition_id"] = None  # NOT NULL → constraint violation on insert

    with pytest.raises(sqlite3.IntegrityError):
        store.write_snapshot(
            taken_at_ms=2_000_000,
            finished_at_ms=2_000_100,
            mode="subset",
            parquet_path="x/2.parquet",
            is_valid=True,
            market_rows=[bad],
            issues=[],
        )

    # The failed transaction must roll back DELETE FROM markets too — so the
    # baseline row from the first snapshot is still present and 'bad' is absent.
    con = sqlite3.connect(store.db_path)
    try:
        rows = con.execute("SELECT market_id FROM markets").fetchall()
        n_snaps = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    finally:
        con.close()
    assert rows == [("baseline",)], (
        "Rollback must restore prior markets state — got: " + repr(rows)
    )
    assert n_snaps == 1, "Failed snapshot must NOT leave a snapshots row behind"
