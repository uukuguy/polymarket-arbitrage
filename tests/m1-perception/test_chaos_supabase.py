"""Chaos: Supabase API 500 → snapshot OK or DEGRADED (not FAILED) (RESEARCH §11).

Scenario: SupabaseMirror.push_snapshot raises Exception("500 internal server").
The orchestrator's step 7.5 is fail-soft:
  - Exception is caught
  - Issue(layer=4, category=unknown) recorded with "Supabase mirror" in detail
  - Snapshot status is OK or DEGRADED (NOT FAILED — SQLite is source of truth)
  - Snapshot is still persisted to SQLite + Parquet

This mirrors RESEARCH §11 row "Supabase API 500 → mirror failure → DEGRADED but snapshot OK".
"""
from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from pydantic import SecretStr

from polyarb.config import Settings  # noqa: E402
from polyarb.snapshot.orchestrator import run_snapshot  # noqa: E402
from polyarb.validator.category import SnapshotStatus  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXTURES_DIR = Path(__file__).parent / "fixtures"


def _load_gamma() -> list[dict]:
    import json
    return json.loads((_FIXTURES_DIR / "gamma_sample.json").read_text())


def _load_clob() -> dict:
    import json
    return json.loads((_FIXTURES_DIR / "clob_sample.json").read_text())


def _make_settings_with_supabase(tmp_path: Path) -> Settings:
    """Settings with Supabase mirror enabled (mocked endpoint)."""
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        retry_attempts=2,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        supabase_url="http://localhost:0",
        supabase_service_key=SecretStr("dummy-service-key"),
    )


def _make_fake_gamma(markets: list[dict]) -> object:
    fake = AsyncMock()
    fake.fetch_all_active_markets.return_value = markets
    fake.fetch_all_active_events.return_value = []

    def _make_iter(items):
        async def _iter():
            for item in items:
                yield item
        return _iter

    fake.iter_active_markets = _make_iter(markets)
    fake.iter_active_events = _make_iter([])
    fake.aclose = AsyncMock()
    fake.__aenter__.return_value = fake
    fake.__aexit__.return_value = None
    return fake


# ---------------------------------------------------------------------------
# Scenario: Supabase push_snapshot raises → DEGRADED not FAILED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_supabase_500_yields_ok_or_degraded_not_failed(tmp_path: Path) -> None:
    """SupabaseMirror.push_snapshot raises → step 7.5 catches → snapshot NOT FAILED.

    The critical assertion is: status != FAILED. Supabase is secondary
    storage; SQLite is source of truth. Mirror failure must not kill the snapshot.
    """
    settings = _make_settings_with_supabase(tmp_path)
    gamma_data = _load_gamma()
    clob_data = _load_clob()
    fake_gamma = _make_fake_gamma(gamma_data)

    books_objs = [SimpleNamespace(**bd) for bd in clob_data["books"]]

    # Patch SupabaseMirror so push_snapshot raises a 500-like error
    mock_mirror = MagicMock()
    mock_mirror.push_snapshot.side_effect = Exception("500 internal server error")

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma):
        with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=books_objs)
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
            )
            # Patch SupabaseMirror inside orchestrator at step 7.5
            with patch(
                "polyarb.storage.supabase_mirror.SupabaseMirror",
                return_value=mock_mirror,
            ):
                result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # CRITICAL: must NOT be FAILED (Supabase failure is not a snapshot failure)
    assert result.status != SnapshotStatus.FAILED.value, (
        f"Supabase 500 must NOT cause FAILED status — got {result.status!r}. "
        "Mirror failure is fail-soft (D-12 amendment)."
    )

    # Snapshot must have been persisted
    assert result.market_count >= 1, "Markets must be persisted to SQLite even if mirror fails"
    assert result.parquet_path.exists(), "Parquet must be written even if mirror fails"


@pytest.mark.asyncio
async def test_supabase_500_records_issue(tmp_path: Path) -> None:
    """SupabaseMirror.push_snapshot raises → an Issue is recorded in result.issue_categories.

    Note: mirror/R2 Issues appended in steps 7.5/7.6 (post-SQLite-write) flow into
    SnapshotResult.issue_categories but are NOT re-written to SQLite (by design —
    SQLite write is already committed when step 7.5 runs). Verify via result object.
    """
    settings = _make_settings_with_supabase(tmp_path)
    gamma_data = _load_gamma()
    clob_data = _load_clob()
    fake_gamma = _make_fake_gamma(gamma_data)
    books_objs = [SimpleNamespace(**bd) for bd in clob_data["books"]]

    mock_mirror = MagicMock()
    mock_mirror.push_snapshot.side_effect = Exception("500 internal server error")

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma):
        with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=books_objs)
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
            )
            with patch(
                "polyarb.storage.supabase_mirror.SupabaseMirror",
                return_value=mock_mirror,
            ):
                result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # SnapshotResult.issue_categories must include "unknown" for the mirror failure
    # (mirror issues are appended in step 7.5 and flow into result.issue_categories)
    assert "unknown" in result.issue_categories, (
        f"Expected 'unknown' Issue category from mirror failure, "
        f"got issue_categories: {result.issue_categories}"
    )
    # issue_count must be > 0
    assert result.issue_count > 0, (
        f"Expected issue_count > 0 when mirror fails, got {result.issue_count}"
    )


@pytest.mark.asyncio
async def test_supabase_sqlite_still_written_on_mirror_failure(tmp_path: Path) -> None:
    """SQLite is written before step 7.5 mirror call — verified directly in DB."""
    settings = _make_settings_with_supabase(tmp_path)
    gamma_data = _load_gamma()
    clob_data = _load_clob()
    fake_gamma = _make_fake_gamma(gamma_data)
    books_objs = [SimpleNamespace(**bd) for bd in clob_data["books"]]

    mock_mirror = MagicMock()
    mock_mirror.push_snapshot.side_effect = Exception("500 internal server error")

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma):
        with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=books_objs)
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
            )
            with patch(
                "polyarb.storage.supabase_mirror.SupabaseMirror",
                return_value=mock_mirror,
            ):
                result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Directly verify SQLite has the snapshot row
    con = sqlite3.connect(settings.db_path)
    snapshot_count = con.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    market_count = con.execute("SELECT COUNT(*) FROM markets").fetchone()[0]
    con.close()

    assert snapshot_count == 1, f"Expected 1 snapshot in DB, got {snapshot_count}"
    assert market_count == len(gamma_data), (
        f"Expected {len(gamma_data)} market rows, got {market_count}"
    )
