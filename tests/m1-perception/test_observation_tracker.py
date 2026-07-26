"""Tests for polyarb.observation.tracker — track_market + DoS sanity check.

Plan 04 Task 1 — covers:
- track_market: slug filter + computed spread
- track_market: parameterized query (slug injection defense T-01.1-16)
- track_market: >200 parquet files triggers warning log (T-01.1-17)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polyarb.observation.tracker import _PARQUET_COUNT_WARN_THRESHOLD, track_market

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def single_parquet(tmp_path: Path) -> Path:
    """One parquet, two snapshots for the same slug with unique taken_at_ms values."""
    root = tmp_path / "2026" / "05" / "01"
    root.mkdir(parents=True)
    path = root / "06-00-00.parquet"
    table = pa.table(
        {
            "market_id": ["M1", "M2", "M3"],
            "condition_id": ["C1", "C2", "C3"],
            "slug": ["x-slug", "y-slug", "x-slug"],
            "question": ["Q1", "Q2", "Q3"],
            "yes_token_id": [None, None, None],
            "no_token_id": [None, None, None],
            "mid_price": [0.40, 0.55, 0.45],
            "liquidity_usd": [1000.0, 2000.0, 1100.0],
            "volume_usd": [100.0, 200.0, 110.0],
            "best_bid_price": [0.38, 0.53, 0.43],
            "best_bid_size": [10.0, 10.0, 10.0],
            "best_ask_price": [0.42, 0.57, 0.47],
            "best_ask_size": [10.0, 10.0, 10.0],
            "end_time_ms": [1800000000000] * 3,
            "active": [True] * 3,
            "closed": [False] * 3,
            "neg_risk": [False] * 3,
            "neg_risk_market_id": [None, None, None],
            "fetched_at_ms": [1700000000000] * 3,
            "snapshot_taken_at_ms": [1700000000000, 1700000100000, 1700000001000],
            "snapshot_id": [1, 2, 1],
            "incomplete": [False, False, False],
            "event_id": [None, None, None],
            "category": ["politics", "science", "politics"],
        }
    )
    pq.write_table(table, path)
    return tmp_path


# =============================================================================
# track_market
# =============================================================================


def test_track_market_returns_ordered_series(single_parquet: Path) -> None:
    df = track_market("x-slug", single_parquet)
    assert len(df) == 2
    # Ordered by taken_at_ms ASC, then snapshot_id ASC
    taken_at_ms_values = list(df["taken_at_ms"])
    assert taken_at_ms_values == sorted(taken_at_ms_values)


def test_track_market_only_returns_requested_slug(single_parquet: Path) -> None:
    df = track_market("x-slug", single_parquet)
    assert all(s == "x-slug" for s in df["slug"])


def test_track_market_empty_for_unknown_slug(single_parquet: Path) -> None:
    df = track_market("nonexistent-slug", single_parquet)
    assert df.empty


def test_track_market_spread_computed(single_parquet: Path) -> None:
    df = track_market("x-slug", single_parquet)
    row = df.iloc[0]
    expected_spread = float(row["best_ask_price"]) - float(row["best_bid_price"])
    assert abs(float(row["spread"]) - expected_spread) < 1e-9


def test_track_market_parameterized_query_no_injection(
    single_parquet: Path,
) -> None:
    """Slug with SQL-special chars passes through as data, not code."""
    df = track_market("x-slug'; DROP TABLE markets; --", single_parquet)
    assert df.empty


def test_track_market_warns_over_threshold(tmp_path: Path) -> None:
    """More than _PARQUET_COUNT_WARN_THRESHOLD files triggers warning log (T-01.1-17)."""
    threshold = _PARQUET_COUNT_WARN_THRESHOLD
    root = tmp_path / "2026" / "05" / "01"
    root.mkdir(parents=True)
    # Write threshold + 1 dummy parquet files into a clean, isolated directory
    for i in range(threshold + 1):
        p = root / f"file_{i:04d}.parquet"
        t = pa.table(
            {
                "market_id": [f"M{i}"],
                "condition_id": [f"C{i}"],
                "slug": ["x-slug"],
                "question": [f"Q{i}"],
                "yes_token_id": [None],
                "no_token_id": [None],
                "mid_price": [0.50],
                "liquidity_usd": [100.0],
                "volume_usd": [10.0],
                "best_bid_price": [0.48],
                "best_bid_size": [5.0],
                "best_ask_price": [0.52],
                "best_ask_size": [5.0],
                "end_time_ms": [1800000000000],
                "active": [True],
                "closed": [False],
                "neg_risk": [False],
                "neg_risk_market_id": [None],
                "fetched_at_ms": [1700000000000],
                "snapshot_taken_at_ms": [1700000000000],
                "snapshot_id": [1],
                "incomplete": [False],
                "event_id": [None],
                "category": ["test"],
            }
        )
        pq.write_table(t, p)
    with pytest.warns(UserWarning, match=f"{threshold + 1} parquet files"):
        track_market("x-slug", tmp_path)
