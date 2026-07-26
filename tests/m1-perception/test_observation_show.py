"""Tests for polyarb.observation.show — multi-source single-market detail.

Plan 05 Task 1 — covers:
- show_market: full dict, bilingual, neg-risk siblings, time dim, recent history
- SQL injection defense
- missing market error handling
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from polyarb.observation.show import (
    show_market,
    show_neg_risk_siblings,
    show_question_bilingual,
    show_recent_history,
    show_time_dimension,
)

# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def db_with_market(tmp_path: Path) -> Path:
    """SQLite db with one market + translation."""
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY, condition_id TEXT, slug TEXT, question TEXT,
            yes_token_id TEXT, no_token_id TEXT, mid_price REAL, liquidity_usd REAL,
            volume_usd REAL, best_bid_price REAL, best_bid_size REAL,
            best_ask_price REAL, best_ask_size REAL, end_time_ms INTEGER,
            active INTEGER, closed INTEGER, neg_risk INTEGER, neg_risk_market_id TEXT,
            fetched_at_ms INTEGER, snapshot_id INTEGER, incomplete INTEGER,
            event_id TEXT
        );
        CREATE TABLE question_translations (
            question_hash TEXT PRIMARY KEY, question_en TEXT, question_zh TEXT,
            translator_model TEXT, translated_at_ms INTEGER, token_cost INTEGER,
            retry_count INTEGER, is_dead INTEGER
        );
    """)
    con.execute(
        "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "m1",
            "c1",
            "test-slug",
            "Will X happen?",
            "tok_yes",
            "tok_no",
            0.55,
            5000.0,
            1000.0,
            0.53,
            100.0,
            0.57,
            100.0,
            1800000000000,
            1,
            0,
            0,
            None,
            1700000000000,
            1,
            0,
            None,
        ),
    )
    con.execute(
        "INSERT INTO question_translations VALUES (?,?,?,?,?,?,?,?)",
        ("h1", "Will X happen?", "X 会发生吗？", "test-model", 1700000000000, 10, 0, 0),
    )
    con.commit()
    con.close()
    return db_path


@pytest.fixture
def db_with_neg_risk_group(tmp_path: Path) -> Path:
    """SQLite db with 3 markets sharing a neg_risk_market_id."""
    db_path = tmp_path / "state.db"
    con = sqlite3.connect(str(db_path))
    con.executescript("""
        CREATE TABLE markets (
            market_id TEXT PRIMARY KEY, condition_id TEXT, slug TEXT, question TEXT,
            yes_token_id TEXT, no_token_id TEXT, mid_price REAL, liquidity_usd REAL,
            volume_usd REAL, best_bid_price REAL, best_bid_size REAL,
            best_ask_price REAL, best_ask_size REAL, end_time_ms INTEGER,
            active INTEGER, closed INTEGER, neg_risk INTEGER, neg_risk_market_id TEXT,
            fetched_at_ms INTEGER, snapshot_id INTEGER, incomplete INTEGER,
            event_id TEXT
        );
        CREATE TABLE question_translations (
            question_hash TEXT PRIMARY KEY, question_en TEXT, question_zh TEXT,
            translator_model TEXT, translated_at_ms INTEGER, token_cost INTEGER,
            retry_count INTEGER, is_dead INTEGER
        );
    """)
    for i, (mid, slug, q) in enumerate(
        [
            ("m_a", "slug-a", "QA"),
            ("m_b", "slug-b", "QB"),
            ("m_c", "slug-c", "QC"),
        ]
    ):
        con.execute(
            "INSERT INTO markets VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                mid,
                "c_neg",
                slug,
                q,
                "ty",
                "tn",
                0.50,
                1000.0,
                100.0,
                0.48,
                10.0,
                0.52,
                10.0,
                1800000000000,
                1,
                0,
                1,
                "neg_grp_1",
                1700000000000,
                1,
                0,
                None,
            ),
        )
    con.commit()
    con.close()
    return db_path


@pytest.fixture
def parquet_root(tmp_path: Path) -> Path:
    """Write 6 mini parquets for recent history test."""
    root = tmp_path / "snaps"
    for i in range(6):
        d = root / "2026" / "05" / f"{i + 1:02d}"
        d.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.table(
                {
                    "market_id": [f"m{i}"],
                    "condition_id": [f"c{i}"],
                    "slug": ["test-slug"],
                    "question": ["Q"],
                    "yes_token_id": [None],
                    "no_token_id": [None],
                    "mid_price": [0.50 + i * 0.01],
                    "liquidity_usd": [1000.0],
                    "volume_usd": [100.0],
                    "best_bid_price": [0.48],
                    "best_bid_size": [10.0],
                    "best_ask_price": [0.52],
                    "best_ask_size": [10.0],
                    "end_time_ms": [1800000000000],
                    "active": [True],
                    "closed": [False],
                    "neg_risk": [False],
                    "neg_risk_market_id": [None],
                    "fetched_at_ms": [1700000000000],
                    "snapshot_taken_at_ms": [1700000000000 + i * 1000],
                    "snapshot_id": [i + 1],
                    "incomplete": [False],
                    "event_id": [None],
                    "category": ["test"],
                }
            ),
            d / "00-00-00.parquet",
        )
    return root


# =============================================================================
# show_question_bilingual
# =============================================================================


def test_show_question_bilingual_with_translation() -> None:
    row = {"question": "Will X happen?", "question_zh": "X 会发生吗？"}
    out = show_question_bilingual(row)
    assert "EN: Will X happen?" in out
    assert "中文: X 会发生吗？" in out


def test_show_question_bilingual_without_translation() -> None:
    row = {"question": "Will X happen?"}
    out = show_question_bilingual(row)
    assert "未翻译" in out


# =============================================================================
# show_time_dimension
# =============================================================================


def test_show_time_dimension_perpetual() -> None:
    out = show_time_dimension({"end_time_ms": None})
    assert "无固定结算" in out


def test_show_time_dimension_past() -> None:
    out = show_time_dimension({"end_time_ms": 0})
    assert "已结算" in out


# =============================================================================
# show_neg_risk_siblings
# =============================================================================


def test_show_neg_risk_siblings_when_grouped(db_with_neg_risk_group: Path) -> None:
    row = {"slug": "slug-a", "neg_risk_market_id": "neg_grp_1"}
    siblings = show_neg_risk_siblings(row, db_with_neg_risk_group)
    assert len(siblings) == 2
    slugs = [s["slug"] for s in siblings]
    assert "slug-b" in slugs
    assert "slug-c" in slugs
    assert "slug-a" not in slugs


def test_show_neg_risk_siblings_when_solo(db_with_neg_risk_group: Path) -> None:
    row = {"slug": "slug-a", "neg_risk_market_id": None}
    assert show_neg_risk_siblings(row, db_with_neg_risk_group) == []


# =============================================================================
# show_recent_history
# =============================================================================


def test_show_recent_history_5_snapshots(parquet_root: Path) -> None:
    df = show_recent_history("test-slug", parquet_root)
    assert len(df) == 5
    # first row should be snapshot_id=2 (oldest of last 5)
    assert df.iloc[0]["snapshot_id"] == 2
    assert df.iloc[-1]["snapshot_id"] == 6


# =============================================================================
# show_market (integration)
# =============================================================================


def test_show_market_returns_full_dict(db_with_market: Path, parquet_root: Path) -> None:
    result = show_market("test-slug", db_with_market, parquet_root)
    assert "market" in result
    assert "bilingual" in result
    assert "time_dim" in result
    assert "neg_risk_siblings" in result
    assert "recent_history" in result
    assert result["market"]["slug"] == "test-slug"


def test_show_market_with_translation(db_with_market: Path, parquet_root: Path) -> None:
    result = show_market("test-slug", db_with_market, parquet_root)
    assert "X 会发生吗" in result["bilingual"]


def test_show_unknown_slug_raises(db_with_market: Path, parquet_root: Path) -> None:
    with pytest.raises(ValueError, match="not found"):
        show_market("nonexistent-slug", db_with_market, parquet_root)


def test_show_sql_injection_safe(db_with_market: Path, parquet_root: Path) -> None:
    with pytest.raises(ValueError, match="not found"):
        show_market("x'; DROP TABLE markets; --", db_with_market, parquet_root)
