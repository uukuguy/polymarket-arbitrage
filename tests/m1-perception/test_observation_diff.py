"""Tests for polyarb.observation.diff — compare_snapshots + resolve_snapshot_path.

Plan 04 Task 1 — covers:
- compare_snapshots: appeared / vanished / persistent + drift ordering
- resolve_snapshot_path: int validation + read-only sqlite lookup
- latest_snapshot_pair: two newest IDs
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polyarb.observation.diff import (
    compare_snapshots,
    latest_snapshot_pair,
    resolve_snapshot_path,
)


# =============================================================================
# Fixtures — two small parquet snapshots + SQLite snapshots table
# =============================================================================


@pytest.fixture
def snap_a(tmp_path: Path) -> Path:
    """Snapshot A: 5 markets, no category column (pre-schema-extension)."""
    root = tmp_path / "2026" / "05" / "01"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "06-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["A1", "A2", "A3", "A4", "A5"],
            "condition_id": ["c1", "c2", "c3", "c4", "c5"],
            "slug": ["slug-1", "slug-2", "slug-3", "slug-4", "slug-5"],
            "question": ["Q1", "Q2", "Q3", "Q4", "Q5"],
            "category": ["politics", "sports", "science", "politics", "tech"],
            "yes_token_id": [None] * 5,
            "no_token_id": [None] * 5,
            "mid_price": [0.40, 0.55, 0.10, 0.88, 0.30],
            "liquidity_usd": [1000.0, 2000.0, 500.0, 3000.0, 800.0],
            "volume_usd": [100.0, 200.0, 50.0, 300.0, 80.0],
            "best_bid_price": [0.38, 0.53, 0.08, 0.86, 0.28],
            "best_bid_size": [10.0] * 5,
            "best_ask_price": [0.42, 0.57, 0.12, 0.90, 0.32],
            "best_ask_size": [10.0] * 5,
            "end_time_ms": [1800000000000] * 5,
            "active": [True] * 5,
            "closed": [False] * 5,
            "neg_risk": [False] * 5,
            "neg_risk_market_id": [None] * 5,
            "fetched_at_ms": [1700000000000] * 5,
            "snapshot_taken_at_ms": [1700000000000] * 5,
            "snapshot_id": [1] * 5,
            "incomplete": [False] * 5,
            "event_id": [None] * 5,
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def snap_b(tmp_path: Path) -> Path:
    """Snapshot B: 5 markets — slug-1 drifted, slug-2 gone, slug-6 appeared."""
    root = tmp_path / "2026" / "05" / "01"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "07-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["B1", "B2", "B4", "B5", "B6"],
            "condition_id": ["c1", "c2", "c4", "c5", "c6"],
            "slug": ["slug-1", "slug-3", "slug-4", "slug-5", "slug-6"],
            "question": ["Q1", "Q3", "Q4", "Q5", "Q6"],
            "category": ["politics", "science", "science", "politics", "sports"],
            "yes_token_id": [None] * 5,
            "no_token_id": [None] * 5,
            "mid_price": [0.50, 0.10, 0.88, 0.30, 0.65],
            "liquidity_usd": [1200.0, 600.0, 3100.0, 850.0, 1500.0],
            "volume_usd": [110.0, 55.0, 310.0, 85.0, 150.0],
            "best_bid_price": [0.48, 0.08, 0.86, 0.28, 0.63],
            "best_bid_size": [10.0] * 5,
            "best_ask_price": [0.52, 0.12, 0.90, 0.32, 0.67],
            "best_ask_size": [10.0] * 5,
            "end_time_ms": [1800000000000] * 5,
            "active": [True] * 5,
            "closed": [False] * 5,
            "neg_risk": [False] * 5,
            "neg_risk_market_id": [None] * 5,
            "fetched_at_ms": [1700000100000] * 5,
            "snapshot_taken_at_ms": [1700000100000] * 5,
            "snapshot_id": [2] * 5,
            "incomplete": [False] * 5,
            "event_id": [None] * 5,
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def snap_a_no_category(tmp_path: Path) -> Path:
    """Snapshot A: 3 markets, pre-Phase-1.1 schema — no category column."""
    root = tmp_path / "2026" / "05" / "03"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "06-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["A1", "A2", "A3"],
            "condition_id": ["c1", "c2", "c3"],
            "slug": ["s1", "s2", "s3"],
            "question": ["Q1", "Q2", "Q3"],
            "yes_token_id": [None] * 3,
            "no_token_id": [None] * 3,
            "mid_price": [0.40, 0.55, 0.10],
            "liquidity_usd": [1000.0, 2000.0, 500.0],
            "volume_usd": [100.0, 200.0, 50.0],
            "best_bid_price": [0.38, 0.53, 0.08],
            "best_bid_size": [10.0] * 3,
            "best_ask_price": [0.42, 0.57, 0.12],
            "best_ask_size": [10.0] * 3,
            "end_time_ms": [1800000000000] * 3,
            "active": [True] * 3,
            "closed": [False] * 3,
            "neg_risk": [False] * 3,
            "neg_risk_market_id": [None] * 3,
            "fetched_at_ms": [1700000000000] * 3,
            "snapshot_taken_at_ms": [1700000000000] * 3,
            "snapshot_id": [10] * 3,
            "incomplete": [False] * 3,
            "event_id": [None] * 3,
            # NOTE: NO category column — pre-Phase-1.1 schema
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def snap_b_with_category(tmp_path: Path) -> Path:
    """Snapshot B: 3 markets, post-Phase-1.1 schema — has category column."""
    root = tmp_path / "2026" / "05" / "03"
    root.mkdir(parents=True, exist_ok=True)
    path = root / "07-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["B1", "B2", "B3"],
            "condition_id": ["c1", "c2", "c3"],
            "slug": ["s1", "s2", "s4"],
            "question": ["Q1", "Q2", "Q4"],
            "category": ["politics", "sports", "science"],
            "yes_token_id": [None] * 3,
            "no_token_id": [None] * 3,
            "mid_price": [0.45, 0.55, 0.20],
            "liquidity_usd": [1100.0, 2100.0, 600.0],
            "volume_usd": [110.0, 210.0, 60.0],
            "best_bid_price": [0.43, 0.53, 0.18],
            "best_bid_size": [10.0] * 3,
            "best_ask_price": [0.47, 0.57, 0.22],
            "best_ask_size": [10.0] * 3,
            "end_time_ms": [1800000000000] * 3,
            "active": [True] * 3,
            "closed": [False] * 3,
            "neg_risk": [False] * 3,
            "neg_risk_market_id": [None] * 3,
            "fetched_at_ms": [1700000100000] * 3,
            "snapshot_taken_at_ms": [1700000100000] * 3,
            "snapshot_id": [11] * 3,
            "incomplete": [False] * 3,
            "event_id": [None] * 3,
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def snap_c(tmp_path: Path) -> Path:
    """Snapshot C: 3 markets with larger drifts for ordering test."""
    root = tmp_path / "2026" / "05" / "02"
    root.mkdir(parents=True)
    path = root / "08-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["C1", "C2", "C3"],
            "condition_id": ["cc1", "cc2", "cc3"],
            "slug": ["slug-x", "slug-y", "slug-z"],
            "question": ["Qx", "Qy", "Qz"],
            "yes_token_id": [None] * 3,
            "no_token_id": [None] * 3,
            "mid_price": [0.50, 0.50, 0.50],
            "liquidity_usd": [1000.0, 2000.0, 3000.0],
            "volume_usd": [100.0, 200.0, 300.0],
            "best_bid_price": [0.48, 0.48, 0.48],
            "best_bid_size": [10.0] * 3,
            "best_ask_price": [0.52, 0.52, 0.52],
            "best_ask_size": [10.0] * 3,
            "end_time_ms": [1800000000000] * 3,
            "active": [True] * 3,
            "closed": [False] * 3,
            "neg_risk": [False] * 3,
            "neg_risk_market_id": [None] * 3,
            "fetched_at_ms": [1700000200000] * 3,
            "snapshot_taken_at_ms": [1700000200000] * 3,
            "snapshot_id": [3] * 3,
            "incomplete": [False] * 3,
            "event_id": [None] * 3,
        }
    )
    pq.write_table(table, path)
    return path


@pytest.fixture
def db_with_snapshots(
    snap_a: Path, snap_b: Path, snap_c: Path
) -> Path:
    """SQLite db with snapshots table pointing at the three fixtures."""
    db_path = snap_a.parent.parent / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE snapshots (
            id INTEGER PRIMARY KEY,
            taken_at_ms INTEGER NOT NULL,
            finished_at_ms INTEGER NOT NULL,
            mode TEXT NOT NULL,
            market_count INTEGER NOT NULL,
            is_valid INTEGER NOT NULL,
            parquet_path TEXT NOT NULL,
            notes TEXT
        );
    """)
    con.execute(
        "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
        (1, 1700000000000, 1700000060000, "subset", 5, 1, str(snap_a), None),
    )
    con.execute(
        "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
        (2, 1700000100000, 1700000160000, "subset", 5, 1, str(snap_b), None),
    )
    con.execute(
        "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
        (3, 1700000200000, 1700000260000, "subset", 3, 1, str(snap_c), None),
    )
    con.commit()
    con.close()
    return db_path


# =============================================================================
# compare_snapshots
# =============================================================================


def test_diff_appeared_marker(snap_a: Path, snap_b: Path) -> None:
    df = compare_snapshots(snap_a, snap_b)
    assert "slug-6" in df["slug"].values
    row = df[df["slug"] == "slug-6"].iloc[0]
    assert row["state"] == "appeared"


def test_diff_vanished_marker(snap_a: Path, snap_b: Path) -> None:
    df = compare_snapshots(snap_a, snap_b)
    assert "slug-2" in df["slug"].values
    row = df[df["slug"] == "slug-2"].iloc[0]
    assert row["state"] == "vanished"


def test_diff_persistent_with_mid_drift(snap_a: Path, snap_b: Path) -> None:
    df = compare_snapshots(snap_a, snap_b)
    row = df[df["slug"] == "slug-1"].iloc[0]
    assert row["state"] == "persistent"
    assert abs(float(row["mid_from"]) - 0.40) < 0.001
    assert abs(float(row["mid_to"]) - 0.50) < 0.001
    assert abs(float(row["mid_drift"]) - 0.10) < 0.001


def test_diff_orders_by_drift_magnitude(snap_a: Path, snap_b: Path) -> None:
    """slug-1 drifted 0.10 (largest in fixture)."""
    df = compare_snapshots(snap_a, snap_b)
    drift_vals = df["mid_drift"].dropna().abs().tolist()
    assert drift_vals == sorted(drift_vals, reverse=True)


# =============================================================================
# resolve_snapshot_path
# =============================================================================


def test_diff_resolve_snapshot_path(db_with_snapshots: Path, snap_a: Path) -> None:
    result = resolve_snapshot_path(1, db_with_snapshots)
    assert result == snap_a


def test_diff_resolve_snapshot_path_id_2(db_with_snapshots: Path, snap_b: Path) -> None:
    result = resolve_snapshot_path(2, db_with_snapshots)
    assert result == snap_b


def test_diff_unknown_snapshot_id_raises(db_with_snapshots: Path) -> None:
    with pytest.raises(ValueError, match="not found"):
        resolve_snapshot_path(999, db_with_snapshots)


def test_diff_resolve_snapshot_path_non_int_raises(db_with_snapshots: Path) -> None:
    with pytest.raises(ValueError, match="positive int"):
        resolve_snapshot_path(-1, db_with_snapshots)
    with pytest.raises(ValueError, match="positive int"):
        resolve_snapshot_path(0, db_with_snapshots)


# =============================================================================
# latest_snapshot_pair
# =============================================================================


def test_diff_latest_pair_skips_empty_snapshots(tmp_path: Path, snap_a: Path, snap_b: Path) -> None:
    """If the newest snapshot has market_count=0, it should be skipped."""
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE snapshots (
            id INTEGER PRIMARY KEY,
            taken_at_ms INTEGER NOT NULL,
            finished_at_ms INTEGER NOT NULL,
            mode TEXT NOT NULL,
            market_count INTEGER NOT NULL,
            is_valid INTEGER NOT NULL,
            parquet_path TEXT NOT NULL,
            notes TEXT
        );
    """)
    # id=1: valid, 5 markets
    con.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                (1, 1700000000000, 1700000060000, "subset", 5, 1, str(snap_a), None))
    # id=2: EMPTY (market_count=0 — failed run)
    con.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                (2, 1700000100000, 1700000160000, "subset", 0, 0, str(snap_b), None))
    # id=3: valid, 5 markets
    con.execute("INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
                (3, 1700000200000, 1700000260000, "subset", 5, 1, str(snap_b), None))
    con.commit()
    con.close()

    older, newer = latest_snapshot_pair(db_path)
    assert older == 1  # id=2 (empty) skipped
    assert newer == 3


def test_diff_latest_pair_selects_two_most_recent(db_with_snapshots: Path) -> None:
    older, newer = latest_snapshot_pair(db_with_snapshots)
    assert older == 2
    assert newer == 3


def test_diff_latest_pair_only_one_snapshot_raises(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE snapshots (
            id INTEGER PRIMARY KEY,
            taken_at_ms INTEGER NOT NULL,
            finished_at_ms INTEGER NOT NULL,
            mode TEXT NOT NULL,
            market_count INTEGER NOT NULL,
            is_valid INTEGER NOT NULL,
            parquet_path TEXT NOT NULL,
            notes TEXT
        );
    """)
    con.execute(
        "INSERT INTO snapshots VALUES (?,?,?,?,?,?,?,?)",
        (1, 1700000000000, 1700000060000, "subset", 5, 1, "/tmp/x.parquet", None),
    )
    con.close()
    with pytest.raises(ValueError, match="need at least 2 snapshots"):
        latest_snapshot_pair(db_path)


# =============================================================================
# Schema drift regression — issue from live smoke test
# Snapshot 1 predates Phase 1.1 category column. SELECT * on a single parquet
# returns only its columns, so COALESCE(a.category, ...) raised BinderError when
# parquet A lacked the column. Fix: explicit column list in CTEs, NULL fallback.
# =============================================================================


def test_diff_schema_drift_one_parquet_lacks_category(
    snap_a_no_category: Path, snap_b_with_category: Path
) -> None:
    """compare_snapshots must not raise when FROM has no category but TO does."""
    # The core regression: this used to throw BinderError on
    # "Values list 'a' does not have a column named 'category'"
    df = compare_snapshots(snap_a_no_category, snap_b_with_category)

    # Should return 4 rows: s1 persistent, s2 vanished, s3 vanished, s4 appeared
    assert len(df) == 4

    s1 = df[df["slug"] == "s1"].iloc[0]
    assert s1["state"] == "persistent"
    assert s1["category"] == "politics"  # from snap_b

    s2 = df[df["slug"] == "s2"].iloc[0]
    assert s2["state"] == "persistent"
    assert s2["category"] == "sports"  # from snap_b (snap_a had no category)

    s4 = df[df["slug"] == "s4"].iloc[0]
    assert s4["state"] == "appeared"
    assert s4["category"] == "science"

    # s3 vanished — came from pre-category parquet; category should be NULL
    s3 = df[df["slug"] == "s3"].iloc[0]
    assert s3["state"] == "vanished"
    assert s3["category"] is None or s3["category"] == ""
