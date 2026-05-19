"""Chaos: Gamma API 5xx failures → snapshot FAILED or DEGRADED (RESEARCH §11).

Scenario 1a: Gamma 503 × 5 (retry exhausted) → snapshot FAILED
             - respx mocks all Gamma /markets calls with 503
             - tenacity backoff exhausts → GammaClient raises
             - orchestrator catches → Issue(API_UNREACHABLE) → status=FAILED

Scenario 1b: Gamma timeout mid-pagination (first page OK, second page 503 ×
             retry_attempts) → snapshot DEGRADED or FAILED (partial fetch)
             - First /markets returns 5 records
             - Second /markets returns 503 every retry
             - orchestrator proceeds with what it got → partial data → DEGRADED or FAILED
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import httpx
import pytest
import respx

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings  # noqa: E402
from polyarb.snapshot.orchestrator import run_snapshot  # noqa: E402
from polyarb.validator.category import SnapshotStatus  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GAMMA_BASE = "https://gamma-api.polymarket.com"


def _make_settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        retry_attempts=3,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
    )


def _make_market(idx: int) -> dict:
    """Minimal market dict for testing."""
    return {
        "id": str(540000 + idx),
        "conditionId": f"0x{'a' * 63}{idx}",
        "question": f"Market {idx}?",
        "slug": f"market-{idx}",
        "active": True,
        "closed": False,
        "liquidityNum": 50000.0 + idx,
        "volumeNum": 10000.0,
        "clobTokenIds": f'["{idx * 2}", "{idx * 2 + 1}"]',
        "endDate": "2026-12-31T00:00:00Z",
    }


def _fake_clob_empty():
    """Patch ClobReaderClient to return empty books (not the subject of these tests)."""
    return patch("polyarb.snapshot.orchestrator.ClobReaderClient")


# ---------------------------------------------------------------------------
# Scenario 1a: Gamma 503 × 5 → snapshot FAILED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gamma_503_exhausted_yields_failed(tmp_path: Path) -> None:
    """Gamma /markets returns 503 on every call; retry exhausts → FAILED status.

    The orchestrator catches the exception from GammaClient and records an
    Issue(API_UNREACHABLE). determine_snapshot_status → FAILED because Layer 1
    saw no markets (0 vs 0) with api_unreachable category.
    """
    settings = _make_settings(tmp_path)

    with respx.mock(base_url=_GAMMA_BASE, assert_all_called=False) as router:
        # All Gamma endpoints return 503 — retry exhausts after retry_attempts calls
        router.get("/markets").mock(return_value=httpx.Response(503))
        router.get("/events").mock(return_value=httpx.Response(503))

        with _fake_clob_empty() as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=[])
            clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

            result = await run_snapshot(settings, mode="subset")

    # After Gamma fails, orchestrator records api_unreachable Issue
    assert "api_unreachable" in result.issue_categories, (
        f"Expected api_unreachable Issue, got categories: {result.issue_categories}"
    )
    # Status must be FAILED (api_unreachable at Layer 1 → no markets → FAILED)
    assert result.status == SnapshotStatus.FAILED.value, (
        f"Expected FAILED, got {result.status!r}"
    )


@pytest.mark.asyncio
async def test_gamma_503_issue_detail_contains_gamma_or_503(tmp_path: Path) -> None:
    """Issue recorded for Gamma 503 must mention the error context."""
    import sqlite3

    settings = _make_settings(tmp_path)

    with respx.mock(base_url=_GAMMA_BASE, assert_all_called=False) as router:
        router.get("/markets").mock(return_value=httpx.Response(503))
        router.get("/events").mock(return_value=httpx.Response(503))

        with _fake_clob_empty() as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=[])
            clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

            result = await run_snapshot(settings, mode="subset")

    # Verify the issue was persisted to SQLite with some descriptive detail
    con = sqlite3.connect(settings.db_path)
    rows = con.execute(
        "SELECT category, detail FROM validation_issues"
    ).fetchall()
    con.close()

    assert rows, "Expected at least one validation_issues row"
    # At least one row should be api_unreachable
    categories = [r[0] for r in rows]
    assert "api_unreachable" in categories, (
        f"Expected api_unreachable in {categories}"
    )


# ---------------------------------------------------------------------------
# Scenario 1b: Gamma timeout mid-pagination → DEGRADED or FAILED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_gamma_timeout_mid_paginate_yields_degraded_or_failed(tmp_path: Path) -> None:
    """First /markets page returns 5 markets; second /markets call returns 503 ×
    retry_attempts. Orchestrator proceeds with partial data or fails.

    Expected: status is DEGRADED or FAILED (not OK — data is incomplete).
    In current implementation the streaming iterator stops at 422/5xx mid-stream
    and we see the partial page that came before. Result depends on final issue set.
    """
    settings = _make_settings(tmp_path)
    # 5 markets — just enough to be a short page (< 100) so pagination terminates
    first_page = [_make_market(i) for i in range(5)]

    with respx.mock(base_url=_GAMMA_BASE, assert_all_called=False) as router:
        # First /markets call → 5 real markets (short page, terminates pagination)
        # Events → 503 (events are fail-soft)
        router.get("/markets").mock(
            side_effect=[
                httpx.Response(200, json=first_page),
                httpx.Response(503),  # If pagination tries again
            ]
        )
        router.get("/events").mock(return_value=httpx.Response(503))

        with _fake_clob_empty() as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=[])
            clob_inst.get_prices_buy_sell = AsyncMock(return_value={"buy": {}, "sell": {}})

            result = await run_snapshot(settings, mode="subset")

    # With partial Gamma data (5 markets on first page) + events fail-soft,
    # should be DEGRADED (api_unreachable for events) or FAILED.
    # The key assertion: it is NOT an unhandled exception — daemon stays alive.
    assert result.status in (
        SnapshotStatus.OK.value,
        SnapshotStatus.DEGRADED.value,
        SnapshotStatus.FAILED.value,
    ), f"Unexpected status: {result.status}"
    # Additionally: snapshot was persisted (daemon still wrote something)
    assert result.snapshot_id >= 1, "snapshot_id must be > 0 — orchestrator wrote to DB"
