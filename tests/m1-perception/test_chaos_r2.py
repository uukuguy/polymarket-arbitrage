"""Chaos: R2 PUT 503 → retry 3× → R2UploadError → orchestrator catches → DEGRADED (RESEARCH §11).

Scenario: R2 upload raises R2UploadError after retry exhaustion.
The orchestrator's step 7.6 is fail-soft:
  - R2UploadError is caught
  - Issue(layer=4, category=unknown) recorded with "R2" in detail
  - Snapshot status is DEGRADED (not FAILED)
  - Parquet is already written locally; SQLite row is already committed

Additionally verifies that the R2 client is configured with max_attempts=3,
mode='standard' (test_r2_retry_config_applied from test_r2_sync.py verifies the
low-level config; this test verifies the orchestrator-level behavior).
"""

from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

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


def _make_settings_with_r2(tmp_path: Path) -> Settings:
    """Settings with R2 enabled (mocked endpoint)."""
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        retry_attempts=2,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        r2_endpoint="https://test.r2.cloudflarestorage.com",
        r2_access_key_id=SecretStr("dummy-access-key"),
        r2_secret_access_key=SecretStr("dummy-secret-key"),
        r2_bucket="test-bucket",
    )


def _make_fake_gamma(markets: list[dict]) -> object:
    fake = AsyncMock()
    fake.fetch_all_active_markets.return_value = markets
    fake.fetch_all_active_events.return_value = []

    def _make_iter(items):
        async def _iter(_coverage):
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
# Scenario: R2 upload raises R2UploadError → DEGRADED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_r2_upload_failure_yields_degraded_not_failed(tmp_path: Path) -> None:
    """R2 upload fails → Issue(unknown) in validation_issues, status NOT FAILED.

    R2 is secondary storage (archive). Its failure must not kill the snapshot.
    SQLite + Parquet are already written by the time step 7.6 runs.
    """
    settings = _make_settings_with_r2(tmp_path)
    gamma_data = _load_gamma()
    clob_data = _load_clob()
    fake_gamma = _make_fake_gamma(gamma_data)
    books_objs = [SimpleNamespace(**bd) for bd in clob_data["books"]]

    from polyarb.storage.r2_sync import R2UploadError

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma):
        with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=books_objs)
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
            )
            # Make upload_parquet_to_r2 always raise R2UploadError
            with patch(
                "polyarb.storage.r2_sync.upload_parquet_to_r2",
                side_effect=R2UploadError("key=2026/01/01/00-00-00.parquet: R2 503"),
            ):
                result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # Status must NOT be FAILED due to R2 failure alone
    assert result.status != SnapshotStatus.FAILED.value, (
        f"R2 upload failure must NOT cause FAILED status, got {result.status!r}. "
        "R2 is fail-soft secondary storage."
    )

    # SQLite still written
    assert result.market_count >= 1, "Markets must be persisted even if R2 fails"
    assert result.parquet_path.exists(), "Parquet must be written locally even if R2 upload fails"


@pytest.mark.asyncio
async def test_r2_upload_failure_records_issue(tmp_path: Path) -> None:
    """R2 upload failure → an Issue is recorded in result.issue_categories.

    Note: R2 Issues appended in step 7.6 (post-SQLite-write) flow into
    SnapshotResult.issue_categories but are NOT re-written to SQLite (by design).
    Verify via result object, not SQLite rows.
    """
    settings = _make_settings_with_r2(tmp_path)
    gamma_data = _load_gamma()
    clob_data = _load_clob()
    fake_gamma = _make_fake_gamma(gamma_data)
    books_objs = [SimpleNamespace(**bd) for bd in clob_data["books"]]

    from polyarb.storage.r2_sync import R2UploadError

    with patch("polyarb.snapshot.orchestrator.GammaClient", return_value=fake_gamma):
        with patch("polyarb.snapshot.orchestrator.ClobReaderClient") as ClobMock:
            clob_inst = ClobMock.return_value
            clob_inst.get_books = AsyncMock(return_value=books_objs)
            clob_inst.get_prices_buy_sell = AsyncMock(
                return_value={"buy": clob_data["prices_buy"], "sell": clob_data["prices_sell"]}
            )
            with patch(
                "polyarb.storage.r2_sync.upload_parquet_to_r2",
                side_effect=R2UploadError("key=2026/01/01/00-00-00.parquet: R2 503"),
            ):
                result = await run_snapshot(settings, mode="subset", now_ms=1_777_448_000_000)

    # SnapshotResult.issue_categories must include "unknown" for R2 failure
    # (R2 issues are appended in step 7.6 and flow into result.issue_categories)
    assert "unknown" in result.issue_categories, (
        f"Expected 'unknown' Issue category from R2 failure, "
        f"got issue_categories: {result.issue_categories}"
    )
    assert result.issue_count > 0, (
        f"Expected issue_count > 0 when R2 fails, got {result.issue_count}"
    )


@pytest.mark.asyncio
async def test_r2_retry_config_is_applied() -> None:
    """R2 client must be configured with max_attempts=3, mode='standard' (D-10 retry policy)."""
    from polyarb.storage.r2_sync import _R2_RETRY_CONFIG

    retry_cfg = _R2_RETRY_CONFIG.retries
    assert retry_cfg.get("max_attempts") == 3, (
        f"R2 retry config: expected max_attempts=3, got {retry_cfg}"
    )
    assert retry_cfg.get("mode") == "standard", (
        f"R2 retry config: expected mode='standard', got {retry_cfg}"
    )
