"""Phase 02 Plan 01 — Parquet / SQLite dual-source consistency tests.

Wave 0 RED test (Test 1.3 from 02-01-PLAN.md).

Validates D-12 amendment: parquet row count == SQLite snapshots.market_count
== COUNT(*) FROM markets WHERE snapshot_id = ?.

This closes the dual-source validation gap — previously only SQLite was
checked, leaving silent parquet failures undetected (LEARNINGS L11/S5).
"""
from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from unittest.mock import AsyncMock, patch
from types import SimpleNamespace

import pyarrow.parquet as pq
import pytest

# F-3 escape hatch: tmp_path lives outside project root.
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

from polyarb.config import Settings  # noqa: E402
from polyarb.snapshot.orchestrator import run_snapshot  # noqa: E402

FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_gamma_fixture() -> list[dict]:
    return json.loads((FIXTURES_DIR / "gamma_sample.json").read_text())


def _load_clob_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "clob_sample.json").read_text())


def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        liquidity_threshold_usd=100.0,
    )


def _books_as_objects(book_dicts: list[dict]) -> list[SimpleNamespace]:
    return [SimpleNamespace(**bd) for bd in book_dicts]


def _events_for_markets(markets: list[dict]) -> list[dict]:
    return [
        {
            "id": f"EV-{m['id']}",
            "slug": f"event-{m['id']}",
            "title": f"Event for {m['id']}",
            "ticker": "TKR",
            "active": True,
            "closed": False,
            "liquidity": 1000.0,
            "volume": 5000.0,
            "endDate": "2026-12-31T00:00:00Z",
            "tags": [{"id": "120", "label": "Test", "slug": "test"}],
            "markets": [{"id": m["id"]}],
        }
        for m in markets
    ]


@pytest.mark.asyncio
async def test_parquet_row_count_matches_sqlite_market_count(tmp_path: Path) -> None:
    """D-12 dual-source consistency: parquet row count must equal:
    - snapshots.market_count in SQLite
    - COUNT(*) FROM markets WHERE snapshot_id = ?

    This test drives page_fetched_at_ms through to both SQLite and parquet,
    verifying the full end-to-end pipeline consistency.
    """
    settings = _make_settings(tmp_path)
    gamma_data = _load_gamma_fixture()
    clob_data = _load_clob_fixture()

    fake_gamma = AsyncMock()
    fake_gamma.fetch_all_active_markets.return_value = gamma_data
    fake_gamma.fetch_all_active_events.return_value = _events_for_markets(gamma_data)
    fake_gamma.aclose = AsyncMock()
    fake_gamma.__aenter__.return_value = fake_gamma
    fake_gamma.__aexit__.return_value = None

    with patch(
        "polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma
    ), patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
        clob_inst = ClobMock.return_value
        clob_inst.get_books = AsyncMock(
            return_value=_books_as_objects(clob_data["books"])
        )
        clob_inst.get_prices_buy_sell = AsyncMock(
            return_value={
                "buy": clob_data["prices_buy"],
                "sell": clob_data["prices_sell"],
            }
        )
        result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    assert result.parquet_path is not None
    parquet_path = result.parquet_path
    assert parquet_path.exists(), f"Parquet file missing: {parquet_path}"

    # Read parquet row count
    parquet_table = pq.read_table(parquet_path)
    parquet_row_count = parquet_table.num_rows

    # Read SQLite counts
    con = sqlite3.connect(settings.db_path)
    try:
        snapshot_id = result.snapshot_id
        sqlite_market_count = con.execute(
            "SELECT market_count FROM snapshots WHERE id = ?", (snapshot_id,)
        ).fetchone()[0]
        sqlite_actual_rows = con.execute(
            "SELECT COUNT(*) FROM markets WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()[0]
    finally:
        con.close()

    # D-12 dual-source consistency: all three must agree
    assert parquet_row_count == sqlite_market_count, (
        f"Parquet row count {parquet_row_count} != "
        f"snapshots.market_count {sqlite_market_count} in SQLite"
    )
    assert parquet_row_count == sqlite_actual_rows, (
        f"Parquet row count {parquet_row_count} != "
        f"COUNT(*) FROM markets {sqlite_actual_rows}"
    )
    assert sqlite_market_count == sqlite_actual_rows, (
        f"snapshots.market_count {sqlite_market_count} != "
        f"COUNT(*) FROM markets {sqlite_actual_rows}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Plan 02-09 (D-23): streaming vs legacy persistence parity
# ─────────────────────────────────────────────────────────────────────────────


def _make_storage_row(market_id: str, *, snapshot_taken_at_ms: int = 1_777_448_000_000) -> dict:
    """Build a row matching SNAPSHOT_SCHEMA exactly (for direct writer tests
    that bypass the orchestrator/normalizer pipeline)."""
    return dict(
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
        active=True,
        closed=False,
        neg_risk=False,
        neg_risk_market_id=None,
        fetched_at_ms=1_777_448_000_000,
        page_fetched_at_ms=None,
        snapshot_taken_at_ms=snapshot_taken_at_ms,
        snapshot_id=1,
        incomplete=False,
        event_id=None,
    )


def test_streaming_parquet_matches_legacy_parquet(tmp_path: Path) -> None:
    """write_parquet_streaming + write_parquet_atomic produce identical row
    content given identical input (1500-row scale)."""
    from polyarb.storage.parquet_writer import (
        write_parquet_atomic,
        write_parquet_streaming,
    )

    rows = [_make_storage_row(f"m{i:04d}") for i in range(1500)]
    p_a = tmp_path / "atomic.parquet"
    p_s = tmp_path / "stream.parquet"

    write_parquet_atomic(rows, p_a)
    write_parquet_streaming((r for r in rows), p_s, batch_size=250)

    rows_a = pq.read_table(p_a).to_pylist()
    rows_s = pq.read_table(p_s).to_pylist()
    assert len(rows_a) == 1500
    assert len(rows_s) == 1500
    # Compare element-wise (order is preserved through both writers)
    assert rows_a == rows_s


def test_streaming_sqlite_matches_legacy_sqlite(tmp_path: Path) -> None:
    """write_snapshot_streaming + write_snapshot produce identical SQLite
    snapshot state (market_count, market rows by id) given identical input."""
    from polyarb.storage.sqlite_store import SQLiteStore

    rows = [_make_storage_row(f"m{i:04d}") for i in range(1500)]

    store_legacy = SQLiteStore(tmp_path / "legacy.db")
    store_legacy.init_schema()
    legacy_id = store_legacy.write_snapshot(
        taken_at_ms=1, finished_at_ms=2, mode="subset",
        parquet_path="x.parquet", is_valid=True,
        market_rows=rows, issues=[],
    )

    store_stream = SQLiteStore(tmp_path / "stream.db")
    store_stream.init_schema()
    stream_id, count = store_stream.write_snapshot_streaming(
        taken_at_ms=1, finished_at_ms=2, mode="subset",
        parquet_path="x.parquet", is_valid=True,
        market_rows=(r for r in rows), issues=[],
        batch_size=250,
    )
    assert count == 1500

    # Compare snapshots metadata + market row sets
    con_a = sqlite3.connect(store_legacy.db_path)
    con_b = sqlite3.connect(store_stream.db_path)
    try:
        mc_a = con_a.execute(
            "SELECT market_count FROM snapshots WHERE id=?", (legacy_id,)
        ).fetchone()[0]
        mc_b = con_b.execute(
            "SELECT market_count FROM snapshots WHERE id=?", (stream_id,)
        ).fetchone()[0]
        assert mc_a == mc_b == 1500

        ids_a = sorted(
            r[0] for r in con_a.execute("SELECT market_id FROM markets").fetchall()
        )
        ids_b = sorted(
            r[0] for r in con_b.execute("SELECT market_id FROM markets").fetchall()
        )
        assert ids_a == ids_b
    finally:
        con_a.close()
        con_b.close()

