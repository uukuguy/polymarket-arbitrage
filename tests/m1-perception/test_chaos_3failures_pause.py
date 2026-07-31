"""Chaos: N consecutive FAILED snapshots → scheduler RECOVERING + alert.

Phase 03.1-04 D-02: threshold raised 3 → 5 (FAILURE_THRESHOLD = 5).

Scenario: run_snapshot always raises → scheduler._failure_counter increments each tick
→ after FAILURE_THRESHOLD ticks: scheduler.state == RECOVERING and
  send_paused_alert called exactly once.

Also verifies:
  - Tick N+1+ (still RECOVERING): bounded producer continues running
  - A certified success returns scheduler.state → RUNNING automatically

This mirrors RESEARCH §11 row "consecutive FAILED → PAUSED + alert".
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.daemon.scheduler import SchedulerState, SnapshotScheduler  # noqa: E402
from polyarb.validator.category import SnapshotStatus  # noqa: E402

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeResult:
    """Minimal snapshot result stub."""

    def __init__(self, status: SnapshotStatus) -> None:
        self.status = status
        self.snapshot_id = 1


def _make_store(tmp_path: Path) -> Any:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    return store


def _make_settings(tmp_path: Path) -> Any:
    from pydantic import SecretStr

    from polyarb.config import Settings

    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        retry_attempts=1,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        scan_shared_secret=SecretStr(
            "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"
        ),
    )


# ---------------------------------------------------------------------------
# Test 1: FAILURE_THRESHOLD consecutive failures → RECOVERING
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failures_start_recovery(tmp_path: Path) -> None:
    """FAILURE_THRESHOLD ticks all raising → RECOVERING after the Nth tick.

    Phase 03.1-04 D-02: threshold is 5 (was 3). Loop reads from the class
    attribute so future threshold changes don't drift this test.
    """
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)

    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("chaos: always fail"))

    for i in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING, (
        f"Expected RECOVERING after {SnapshotScheduler.FAILURE_THRESHOLD} failures, "
        f"got {scheduler.state!r}"
    )
    assert scheduler._failure_counter >= SnapshotScheduler.FAILURE_THRESHOLD, (
        f"failure_counter must be >= {SnapshotScheduler.FAILURE_THRESHOLD}, "
        f"got {scheduler._failure_counter}"
    )


# ---------------------------------------------------------------------------
# Test 2: send_paused_alert called exactly once (dedup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovering_alert_called_once_on_threshold(tmp_path: Path) -> None:
    """send_recovering_alert is called once when scheduler enters recovery.

    At the threshold, _on_recovering() fires once.
    On later recovery failures — no extra first-incident alert.
    """
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("chaos: always fail"))

    from polyarb.daemon import alerts as _alerts_mod

    _alerts_mod._LAST_ALERT_TIME_MS.clear()  # reset dedup state

    with patch.object(
        _alerts_mod, "send_recovering_alert", new=AsyncMock(return_value=None)
    ) as mock_alert:
        # N ticks → transitions to RECOVERING, calls recovery alert once
        for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
            await scheduler._tick()

        assert mock_alert.call_count == 1, (
            f"Expected recovery alert once, got {mock_alert.call_count}"
        )

        # Next tick: scheduler is already RECOVERING → retries without a duplicate alert
        await scheduler._tick()

        assert mock_alert.call_count == 1, (
            f"send_paused_alert must NOT be called again when already RECOVERING, "
            f"got {mock_alert.call_count}"
        )


# ---------------------------------------------------------------------------
# Test 3: RECOVERING continues bounded producer ticks
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovering_scheduler_continues_ticks(tmp_path: Path) -> None:
    """RECOVERING continues bounded producer ticks rather than becoming terminal."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler.state = SchedulerState.RECOVERING

    run_mock = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    scheduler._run_snapshot = run_mock

    await scheduler._tick()
    await scheduler._tick()

    assert run_mock.await_count == 2
    assert scheduler.state == SchedulerState.RUNNING


# ---------------------------------------------------------------------------
# Test 4: recovery success resumes RUNNING automatically
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_success_resumes_running(tmp_path: Path) -> None:
    """A certified recovery tick resets the counter without manual control."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler.state = SchedulerState.RECOVERING
    scheduler._failure_counter = SnapshotScheduler.FAILURE_THRESHOLD

    ok_mock = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    scheduler._run_snapshot = ok_mock

    from polyarb.daemon import alerts as _alerts_mod

    with patch.object(_alerts_mod, "send_heartbeat_ok", new=AsyncMock(return_value=None)):
        await scheduler._tick()
    ok_mock.assert_called_once()
    assert scheduler.state == SchedulerState.RUNNING
    assert scheduler._failure_counter == 0


# ---------------------------------------------------------------------------
# Test 5: failure counter persists across restart
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failure_counter_persists_restart(tmp_path: Path) -> None:
    """Counter=N-1 from prior run → one more failure on fresh instance → RECOVERING at N.

    Phase 03.1-04 D-02: threshold is 5. Pre-shutdown counter set to 4, restart
    restores 4, one more failure transitions to PAUSED.
    """
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)

    threshold = SnapshotScheduler.FAILURE_THRESHOLD
    pre_shutdown_counter = threshold - 1  # one short of trigger

    # First instance: threshold-1 failures
    s1 = SnapshotScheduler(settings=settings, sqlite_store=store)
    s1._run_snapshot = AsyncMock(side_effect=RuntimeError("fail"))
    for _ in range(pre_shutdown_counter):
        await s1._tick()
    assert s1._failure_counter == pre_shutdown_counter

    # Second instance (simulating restart): reads counter from DB
    s2 = SnapshotScheduler(settings=settings, sqlite_store=store)
    assert s2._failure_counter == pre_shutdown_counter, (
        f"Restarted scheduler must restore counter={pre_shutdown_counter}, "
        f"got {s2._failure_counter}"
    )

    from polyarb.daemon import alerts as _alerts_mod

    _alerts_mod._LAST_ALERT_TIME_MS.clear()

    with patch.object(_alerts_mod, "send_paused_alert", new=AsyncMock(return_value=None)):
        s2._run_snapshot = AsyncMock(side_effect=RuntimeError("fail"))
        await s2._tick()

    assert s2.state == SchedulerState.RECOVERING, (
        f"After {threshold}-th failure (counter restored={pre_shutdown_counter} + 1), "
        f"must be RECOVERING, got {s2.state!r}"
    )
