"""Tests for SnapshotScheduler 3-failure-pause state machine.

Covers D-13 / T-02-04:
- scheduler.state == PAUSED after 3 consecutive FAILED results
- DEGRADED is NOT failure (D-12 amendment); 3x DEGRADED must NOT pause
- Paused scheduler skips tick (no run_snapshot called)
- Failure counter resets on OK/DEGRADED success
- Counter persists across restart (restored from DB)
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polyarb.daemon.scheduler import SchedulerState, SnapshotScheduler
from polyarb.validator.category import SnapshotStatus


# ---------------------------------------------------------------------------
# scheduler_interval_s configurability (Inj 2 P0 fix, 2026-05-20)
# ---------------------------------------------------------------------------

def test_scheduler_interval_default_3600() -> None:
    """Default Settings.scheduler_interval_s == 3600 (preserves Plan 02 behavior)."""
    from polyarb.config import Settings
    s = Settings(_env_file=None)
    assert s.scheduler_interval_s == 3600


def test_scheduler_interval_reads_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    """POLYARB_SCHEDULER_INTERVAL_S env var overrides default.

    Inj 2 P0 fix: pre-2026-05-20 the value was read via getattr fallback on a
    field that Settings did not declare, so the env var was silently ignored
    and prod was stuck at 3600s (1h) — chaos injection could not verify
    the 3-failure-pause path within a reasonable window.
    """
    from polyarb.config import Settings
    monkeypatch.setenv("POLYARB_SCHEDULER_INTERVAL_S", "60")
    s = Settings(_env_file=None)
    assert s.scheduler_interval_s == 60


# ---------------------------------------------------------------------------
# Helper result types
# ---------------------------------------------------------------------------

class _FakeResult:
    """Minimal snapshot result stub."""
    def __init__(self, status: SnapshotStatus) -> None:
        self.status = status


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_pause_after_3_failures(
    daemon_settings_for_test: Any,
    tmp_path: Path,
) -> None:
    """3 consecutive FAILED results → scheduler.state == PAUSED."""
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    # Always raise → counts as FAILED
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    for _ in range(3):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.PAUSED
    assert scheduler._failure_counter >= 3


@pytest.mark.asyncio
async def test_no_pause_after_degraded_then_ok(
    daemon_settings_for_test: Any,
) -> None:
    """DEGRADED then OK → counter resets, state stays RUNNING."""
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    # Tick 1: DEGRADED
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.DEGRADED))
    await scheduler._tick()
    assert scheduler._failure_counter == 0  # DEGRADED does not count as failure
    assert scheduler.state == SchedulerState.RUNNING

    # Tick 2: OK
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    await scheduler._tick()
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING


@pytest.mark.asyncio
async def test_paused_skips_tick(
    daemon_settings_for_test: Any,
) -> None:
    """When scheduler is PAUSED, _tick() does nothing — run_snapshot NOT called."""
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler.state = SchedulerState.PAUSED

    run_mock = AsyncMock()
    scheduler._run_snapshot = run_mock

    await scheduler._tick()

    run_mock.assert_not_called()


@pytest.mark.asyncio
async def test_degraded_does_not_count_as_failure(
    daemon_settings_for_test: Any,
) -> None:
    """Per D-12 amendment, DEGRADED is success-with-warnings.

    3 consecutive DEGRADED must NOT pause the scheduler.
    """
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.DEGRADED))

    for _ in range(5):  # More than FAILURE_THRESHOLD
        await scheduler._tick()

    # Counter should stay at 0; state should stay RUNNING
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING


@pytest.mark.asyncio
async def test_successful_tick_calls_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """Plan 02-05 fix-up: successful snapshot tick must ping Better Stack heartbeat.

    Wired via `alerts.send_heartbeat_ok(self._settings)` from _tick() success branch.
    Test patches the module attribute so we can assert the call without HTTP traffic.
    """
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_called_once_with(daemon_settings_for_test)


@pytest.mark.asyncio
async def test_degraded_tick_also_calls_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """DEGRADED is success-with-warnings (D-12) — heartbeat OK still fires.

    Better Stack monitor should stay green for DEGRADED ticks; the degradation
    is signalled through Sentry breadcrumbs + supabase mirror age check, not
    through silencing the heartbeat.
    """
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.DEGRADED))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_called_once_with(daemon_settings_for_test)


@pytest.mark.asyncio
async def test_failed_tick_does_not_call_heartbeat_ok(
    daemon_settings_for_test: Any,
) -> None:
    """FAILED tick must NOT ping heartbeat — that's how Better Stack notices the outage.

    If heartbeat OK fires on failure, the monitor stays green and 75-min grace
    never triggers, defeating the external-watcher safety net.
    """
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as hb_mock:
        await scheduler._tick()
        hb_mock.assert_not_called()


@pytest.mark.asyncio
async def test_counter_persists_across_restart(
    daemon_settings_for_test: Any,
) -> None:
    """Counter=2 from prior shutdown → one more failure on restart → PAUSED at counter=3."""
    from polyarb.storage.sqlite_store import SQLiteStore
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    # First instance: run 2 failures, then "shut down"
    scheduler1 = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler1._run_snapshot = AsyncMock(side_effect=RuntimeError("failed"))

    await scheduler1._tick()
    await scheduler1._tick()

    assert scheduler1._failure_counter == 2
    assert scheduler1.state == SchedulerState.RUNNING

    # Second instance reads from DB → restores counter=2
    scheduler2 = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    assert scheduler2._failure_counter == 2, (
        f"Expected restored counter=2, got {scheduler2._failure_counter}. "
        "Scheduler must persist counter to SQLite."
    )

    # One more failure → PAUSED
    scheduler2._run_snapshot = AsyncMock(side_effect=RuntimeError("failed again"))
    await scheduler2._tick()

    assert scheduler2.state == SchedulerState.PAUSED
