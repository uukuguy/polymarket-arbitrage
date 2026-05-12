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
