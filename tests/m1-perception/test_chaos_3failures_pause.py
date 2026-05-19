"""Chaos: 3 consecutive FAILED snapshots → scheduler PAUSED + send_paused_alert called.

Scenario: run_snapshot always raises → scheduler._failure_counter increments each tick
→ after 3 ticks: scheduler.state == PAUSED, send_paused_alert called exactly once.

Also verifies:
  - Tick 4+ (still PAUSED): run_snapshot NOT called (scheduler skips ticks when PAUSED)
  - Unpause: scheduler.state → RUNNING, next tick calls run_snapshot again

This mirrors RESEARCH §11 row "3× consecutive FAILED → PAUSED + alert".
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
# Test 1: 3 consecutive failures → PAUSED
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_3_failures_cause_pause(tmp_path: Path) -> None:
    """3 ticks all raising → scheduler.state == PAUSED after 3rd tick."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)

    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("chaos: always fail"))

    for i in range(3):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.PAUSED, (
        f"Expected PAUSED after 3 failures, got {scheduler.state!r}"
    )
    assert scheduler._failure_counter >= 3, (
        f"failure_counter must be >= 3, got {scheduler._failure_counter}"
    )


# ---------------------------------------------------------------------------
# Test 2: send_paused_alert called exactly once (dedup)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paused_alert_called_once_on_pause(tmp_path: Path) -> None:
    """send_paused_alert called exactly once when scheduler transitions to PAUSED.

    After the 3rd failure, _on_paused() fires → alerts.send_paused_alert called.
    On the 4th failure (scheduler already PAUSED → skips tick) — no extra call.
    """
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("chaos: always fail"))

    from polyarb.daemon import alerts as _alerts_mod
    _alerts_mod._LAST_ALERT_TIME_MS.clear()  # reset dedup state

    with patch.object(
        _alerts_mod, "send_paused_alert", new=AsyncMock(return_value=None)
    ) as mock_alert:
        # 3 ticks → transitions to PAUSED, calls send_paused_alert once
        for _ in range(3):
            await scheduler._tick()

        assert mock_alert.call_count == 1, (
            f"Expected send_paused_alert to be called once, got {mock_alert.call_count}"
        )

        # 4th tick: scheduler is already PAUSED → skips → no additional call
        await scheduler._tick()

        assert mock_alert.call_count == 1, (
            f"send_paused_alert must NOT be called again when already PAUSED, "
            f"got {mock_alert.call_count}"
        )


# ---------------------------------------------------------------------------
# Test 3: PAUSED → run_snapshot NOT called on tick
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_paused_scheduler_skips_tick(tmp_path: Path) -> None:
    """When scheduler is PAUSED, _tick() is a no-op — run_snapshot never called."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler.state = SchedulerState.PAUSED

    run_mock = AsyncMock()
    scheduler._run_snapshot = run_mock

    await scheduler._tick()
    await scheduler._tick()

    run_mock.assert_not_called()


# ---------------------------------------------------------------------------
# Test 4: Unpause → next tick calls run_snapshot
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_unpause_resumes_ticks(tmp_path: Path) -> None:
    """After scheduler.unpause(), next tick calls run_snapshot again."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    scheduler.state = SchedulerState.PAUSED
    scheduler._failure_counter = 3

    ok_mock = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    scheduler._run_snapshot = ok_mock

    # While PAUSED: run_snapshot not called
    await scheduler._tick()
    ok_mock.assert_not_called()

    # Unpause manually
    scheduler.unpause()
    assert scheduler.state == SchedulerState.RUNNING, "After unpause, state must be RUNNING"
    assert scheduler._failure_counter == 0, "After unpause, failure_counter must reset to 0"

    # Next tick: run_snapshot is called
    from polyarb.daemon import alerts as _alerts_mod
    with patch.object(_alerts_mod, "send_heartbeat_ok", new=AsyncMock(return_value=None)):
        await scheduler._tick()
    ok_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Test 5: failure counter persists across restart
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_failure_counter_persists_restart(tmp_path: Path) -> None:
    """Counter=2 from prior run → one more failure on fresh instance → PAUSED at 3."""
    settings = _make_settings(tmp_path)
    store = _make_store(tmp_path)

    # First instance: 2 failures
    s1 = SnapshotScheduler(settings=settings, sqlite_store=store)
    s1._run_snapshot = AsyncMock(side_effect=RuntimeError("fail"))
    await s1._tick()
    await s1._tick()
    assert s1._failure_counter == 2

    # Second instance (simulating restart): reads counter from DB
    s2 = SnapshotScheduler(settings=settings, sqlite_store=store)
    assert s2._failure_counter == 2, (
        f"Restarted scheduler must restore counter=2, got {s2._failure_counter}"
    )

    from polyarb.daemon import alerts as _alerts_mod
    _alerts_mod._LAST_ALERT_TIME_MS.clear()

    with patch.object(_alerts_mod, "send_paused_alert", new=AsyncMock(return_value=None)):
        s2._run_snapshot = AsyncMock(side_effect=RuntimeError("fail"))
        await s2._tick()

    assert s2.state == SchedulerState.PAUSED, (
        f"After 3rd failure (counter restored=2 + 1), must be PAUSED, got {s2.state!r}"
    )
