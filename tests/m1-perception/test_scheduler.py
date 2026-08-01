"""Tests for SnapshotScheduler N-failure-pause state machine.

Covers D-13 / T-02-04 (Phase 02 baseline) + D-02 (Phase 03.1-04 raises
FAILURE_THRESHOLD 3 → 5 to absorb DNS jitter via tenacity retry):

- scheduler.state == PAUSED after FAILURE_THRESHOLD consecutive FAILED results
- DEGRADED is NOT failure (D-12 amendment); N×DEGRADED must NOT pause
- Paused scheduler skips tick (no run_snapshot called)
- Failure counter resets on OK/DEGRADED success
- Counter persists across restart (restored from DB)
- 4 failures keep RUNNING; the 5th transitions to PAUSED (explicit guard)
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace
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


def test_structure_sync_enablement_reads_production_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.config import Settings

    monkeypatch.setenv("POLYARB_STRUCTURE_SYNC_ENABLED", "true")
    settings = Settings(_env_file=None)
    scheduler = SnapshotScheduler(settings=settings, sqlite_store=MagicMock())

    assert scheduler.structure_sync_enabled is True


def test_recovery_retry_delay_is_exponential_and_bounded() -> None:
    from polyarb.daemon.scheduler import recovery_retry_delay_s

    assert [recovery_retry_delay_s(counter) for counter in (1, 2, 3, 4, 5, 20)] == [
        5.0,
        10.0,
        20.0,
        40.0,
        60.0,
        60.0,
    ]


# ---------------------------------------------------------------------------
# Helper result types
# ---------------------------------------------------------------------------


class _FakeResult:
    """Minimal snapshot result stub."""

    def __init__(
        self,
        status: SnapshotStatus,
        *,
        snapshot_id: int = 1,
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
    ) -> None:
        self.status = status
        self.snapshot_id = snapshot_id
        self.last_stage = last_stage
        self.elapsed_ms = elapsed_ms


class _FakeProcess:
    def __init__(
        self,
        payload: object,
        *,
        returncode: int,
        block: bool = False,
        stderr: bytes = b"bounded stderr",
    ) -> None:
        self.stdout = (
            payload
            if isinstance(payload, bytes)
            else json.dumps(payload).encode()
        )
        self.returncode = returncode
        self.block = block
        self.stderr = stderr
        self.terminated = False
        self.killed = False
        self._reaped = asyncio.Event()

    async def communicate(self):
        if self.block and not self.killed:
            await self._reaped.wait()
        return self.stdout, self.stderr

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True
        self._reaped.set()


def test_snapshot_attempt_lifecycle_is_append_only(daemon_settings_for_test: Any) -> None:
    """A terminal scheduler attempt keeps its original OOM classification."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="failed",
        finished_at_ms=2_000,
        snapshot_id=None,
        failure_kind="snapshot-subprocess-signal-sigkill-possible-oom",
    )

    assert store.get_latest_snapshot_attempt() == {
        "id": attempt_id,
        "started_at_ms": 1_000,
        "finished_at_ms": 2_000,
        "outcome": "failed",
        "snapshot_id": None,
        "failure_kind": "snapshot-subprocess-signal-sigkill-possible-oom",
        "last_stage": None,
        "elapsed_ms": None,
    }


def test_snapshot_attempt_diagnostic_columns_migrate_legacy_rows(tmp_path: Path) -> None:
    """Fresh and pre-diagnostic attempt tables expose nullable diagnostic columns."""
    from polyarb.storage.sqlite_store import SQLiteStore

    fresh_db = tmp_path / "fresh.db"
    SQLiteStore(fresh_db).init_schema()
    with sqlite3.connect(fresh_db) as con:
        fresh_columns = {row[1] for row in con.execute("PRAGMA table_info(snapshot_attempts)")}
    assert {"elapsed_ms", "last_stage"} <= fresh_columns

    legacy_db = tmp_path / "legacy.db"
    with sqlite3.connect(legacy_db) as con:
        con.execute(
            "CREATE TABLE snapshot_attempts ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT, started_at_ms INTEGER NOT NULL, "
            "finished_at_ms INTEGER, outcome TEXT NOT NULL, snapshot_id INTEGER, "
            "failure_kind TEXT)"
        )
        con.execute(
            "INSERT INTO snapshot_attempts(started_at_ms, outcome) VALUES (?, ?)",
            (1_000, "running"),
        )

    legacy_store = SQLiteStore(legacy_db)
    legacy_store.init_schema()
    legacy_store.init_schema()

    with sqlite3.connect(legacy_db) as con:
        legacy_columns = {row[1] for row in con.execute("PRAGMA table_info(snapshot_attempts)")}
        historical_diagnostics = con.execute(
            "SELECT last_stage, elapsed_ms FROM snapshot_attempts WHERE id = 1"
        ).fetchone()
    assert {"elapsed_ms", "last_stage"} <= legacy_columns
    assert historical_diagnostics == (None, None)


def test_snapshot_attempt_terminal_row_cannot_be_rewritten(
    daemon_settings_for_test: Any,
) -> None:
    """A later writer cannot turn recorded success into a fabricated failure."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    attempt_id = store.begin_snapshot_attempt(started_at_ms=1_000)
    store.finish_snapshot_attempt(
        attempt_id=attempt_id,
        outcome="succeeded",
        finished_at_ms=2_000,
        snapshot_id=746,
        failure_kind=None,
    )

    with pytest.raises(ValueError, match="not running"):
        store.finish_snapshot_attempt(
            attempt_id=attempt_id,
            outcome="failed",
            finished_at_ms=3_000,
            snapshot_id=None,
            failure_kind="late-rewrite",
        )


@pytest.mark.asyncio
async def test_scheduler_persists_sigkill_attempt_failure(
    daemon_settings_for_test: Any,
) -> None:
    """A kernel-killed child leaves durable operational evidence behind."""
    from polyarb.daemon.scheduler import SnapshotSubprocessError
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        side_effect=SnapshotSubprocessError(
            "timeout",
            last_stage="gamma-markets",
            elapsed_ms=245_012,
        )
    )

    await scheduler._tick()

    assert store.get_latest_snapshot_attempt() == {
        "id": 1,
        "started_at_ms": pytest.approx(int(time.time() * 1000), abs=2_000),
        "finished_at_ms": pytest.approx(int(time.time() * 1000), abs=2_000),
        "outcome": "failed",
        "snapshot_id": None,
        "failure_kind": "snapshot-subprocess-timeout",
        "last_stage": "gamma-markets",
        "elapsed_ms": 245_012,
    }


@pytest.mark.asyncio
async def test_scheduler_persists_successful_attempt_diagnostics(
    daemon_settings_for_test: Any,
) -> None:
    """A terminal successful row retains parent-observed diagnostics."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(
            SnapshotStatus.OK,
            last_stage="persist",
            elapsed_ms=1_234,
        )
    )

    await scheduler._tick()

    latest_attempt = store.get_latest_snapshot_attempt()
    assert latest_attempt is not None
    assert latest_attempt["outcome"] == "succeeded"
    assert latest_attempt["last_stage"] == "persist"
    assert latest_attempt["elapsed_ms"] == 1_234


@pytest.mark.asyncio
async def test_scheduler_preserves_result_diagnostics_when_result_is_rejected(
    daemon_settings_for_test: Any,
) -> None:
    """A parent validation error retains the child facts it was handed."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=_FakeResult(
            SnapshotStatus.OK,
            snapshot_id=0,
            last_stage="persist",
            elapsed_ms=1_234,
        )
    )

    await scheduler._tick()

    latest_attempt = store.get_latest_snapshot_attempt()
    assert latest_attempt is not None
    assert latest_attempt["failure_kind"] == "snapshot-subprocess-missing-snapshot-id"
    assert latest_attempt["last_stage"] == "persist"
    assert latest_attempt["elapsed_ms"] == 1_234


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "is_valid", "returncode"),
    (
        ("ok", True, 0),
        ("degraded", True, 0),
        ("failed", False, 1),
    ),
)
async def test_snapshot_pipeline_runs_in_isolated_subprocess(
    status: str,
    is_valid: bool,
    returncode: int,
) -> None:
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess(
        {
            "status": status,
            "is_valid": is_valid,
            "snapshot_id": 746,
            "market_count": 81959,
            "issue_count": 3,
        },
        returncode=returncode,
    )
    calls = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert result.status == SnapshotStatus(status)
    assert result.snapshot_id == 746
    assert result.market_count == 81959
    assert result.issue_count == 3
    args, kwargs = calls[0]
    assert args[1:] == (
        "-m",
        "polyarb.snapshot",
        "structure-sync",
        "--json",
        "--low-priority",
        "--max-pages",
        "40",
    )
    assert kwargs["stdout"] == asyncio.subprocess.PIPE
    assert kwargs["stderr"] == asyncio.subprocess.PIPE


@pytest.mark.asyncio
async def test_snapshot_subprocess_accepts_cooperative_checkpoint() -> None:
    from polyarb.daemon.scheduler import (
        IsolatedStructureCheckpoint,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {
            "checkpointed": True,
            "window_id": "window-1",
            "stage": "markets",
            "pages_processed": 80,
        },
        returncode=0,
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert isinstance(result, IsolatedStructureCheckpoint)
    assert result.window_id == "window-1"
    assert result.stage == "markets"
    assert result.pages_processed == 80


def test_snapshot_stage_parser_keeps_only_final_allowlisted_marker() -> None:
    """Arbitrary child stderr never becomes a scheduler diagnostic."""
    from polyarb.daemon.scheduler import _parse_last_snapshot_stage

    stderr = b"\n".join(
        (
            b"network error: upstream sent a secret-looking message",
            b"snapshot-stage stage=gamma-events state=complete elapsed_ms=12",
            b"snapshot-stage stage=not-allowed state=start elapsed_ms=13",
            b"snapshot-stage stage=gamma-markets state=unexpected elapsed_ms=14",
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=-1",
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=15",
        )
    )

    assert _parse_last_snapshot_stage(stderr) == "gamma-markets"


@pytest.mark.asyncio
async def test_snapshot_subprocess_result_has_parent_elapsed_and_final_stage() -> None:
    """The parent returns bounded diagnostics, never the child's stderr payload."""
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess(
        {
            "status": "ok",
            "is_valid": True,
            "snapshot_id": 746,
            "market_count": 81959,
            "issue_count": 3,
        },
        returncode=0,
        stderr=(
            b"snapshot-stage stage=gamma-events state=complete elapsed_ms=91\n"
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=999999"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    result = await run_snapshot_in_subprocess(spawn=spawn)

    assert result.last_stage == "gamma-markets"
    assert 0 <= result.elapsed_ms < 999999
    assert not hasattr(result, "stderr")


@pytest.mark.asyncio
async def test_snapshot_waits_for_shared_producer_slot(
    daemon_settings_for_test,
    monkeypatch,
) -> None:
    from polyarb.daemon import scheduler as scheduler_module

    producer_lock = asyncio.Lock()
    await producer_lock.acquire()
    calls = 0
    observed_timeout_s = None

    async def run_snapshot(*, timeout_s: float):
        nonlocal calls, observed_timeout_s
        calls += 1
        observed_timeout_s = timeout_s
        return SimpleNamespace(status=SnapshotStatus.OK)

    monkeypatch.setattr(
        scheduler_module,
        "run_snapshot_in_subprocess",
        run_snapshot,
    )
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=MagicMock(),
        producer_lock=producer_lock,
    )
    scheduler._effective_timeout_s = 240
    running = asyncio.create_task(scheduler._run_snapshot())
    await asyncio.sleep(0)
    assert calls == 0

    producer_lock.release()
    await running

    assert calls == 1
    assert observed_timeout_s == 180


async def test_snapshot_timeout_reaps_before_reading_bounded_stage_diagnostics() -> None:
    """Timeout data arrives only from the child communicate result after reaping."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess(
        {},
        returncode=0,
        block=True,
        stderr=(
            b"arbitrary child error\n"
            b"snapshot-stage stage=gamma-markets state=start elapsed_ms=17"
        ),
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="snapshot-subprocess-timeout") as raised:
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    error = raised.value
    assert process.terminated is True
    assert process.killed is True
    assert error.last_stage == "gamma-markets"
    assert error.elapsed_ms >= 0
    assert not hasattr(error, "stderr")


@pytest.mark.asyncio
async def test_snapshot_subprocess_rejects_mismatched_exit_contract() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            {
                "status": "ok",
                "is_valid": True,
                "snapshot_id": 746,
                "market_count": 81959,
                "issue_count": 3,
            },
            returncode=1,
        )

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-invalid-json",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_classifies_sqlite_writer_contention() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(
            b"",
            returncode=1,
            stderr=b"OperationalError: database is locked",
        )

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-sqlite-busy",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_classifies_sigkill_as_possible_oom() -> None:
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(b"", returncode=-9)

    with pytest.raises(
        SnapshotSubprocessError,
        match="snapshot-subprocess-signal-sigkill-possible-oom",
    ):
        await run_snapshot_in_subprocess(spawn=spawn)


@pytest.mark.asyncio
async def test_snapshot_subprocess_cancellation_terminates_then_kills() -> None:
    from polyarb.daemon.scheduler import run_snapshot_in_subprocess

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    task = asyncio.create_task(
        run_snapshot_in_subprocess(
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_snapshot_subprocess_timeout_terminates_then_kills() -> None:
    """A CPU-bound child cannot hold the 5-minute production cadence forever."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    process = _FakeProcess({}, returncode=0, block=True)

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="snapshot-subprocess-timeout"):
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    assert process.terminated is True
    assert process.killed is True


@pytest.mark.asyncio
async def test_snapshot_timeout_keeps_one_reap_task_until_child_exit() -> None:
    """Timeout cleanup must not abandon a child after cancelling its pipe reader."""
    from polyarb.daemon.scheduler import (
        SnapshotSubprocessError,
        run_snapshot_in_subprocess,
    )

    class Process:
        def __init__(self) -> None:
            self.returncode: int | None = None
            self.calls = 0
            self.released = asyncio.Event()
            self.terminated = False
            self.killed = False

        async def communicate(self) -> tuple[bytes, bytes]:
            self.calls += 1
            if self.calls != 1:
                raise AssertionError("communicate must not be re-entered after timeout")
            await self.released.wait()
            return b"", b""

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True
            self.returncode = -9
            self.released.set()

    process = Process()

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(SnapshotSubprocessError, match="snapshot-subprocess-timeout"):
        await run_snapshot_in_subprocess(
            spawn=spawn,
            timeout_s=0.01,
            terminate_timeout_s=0.01,
        )

    assert process.terminated is True
    assert process.killed is True
    assert process.calls == 1


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_recovery_after_failure_threshold(
    daemon_settings_for_test: Any,
    tmp_path: Path,
) -> None:
    """FAILURE_THRESHOLD consecutive FAILED results → RECOVERING.

    Phase 03.1-04 D-02: threshold is 5. Loop drives the class attribute so the
    invariant ("after N failures recovery is visible") survives future tuning.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    # Always raise → counts as FAILED
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING
    assert scheduler._failure_counter >= SnapshotScheduler.FAILURE_THRESHOLD


@pytest.mark.asyncio
async def test_recovery_after_5_failures_not_3(
    daemon_settings_for_test: Any,
) -> None:
    """Four failures keep RUNNING; the fifth starts recovery.

    Pinned to literal 5 (not the class attribute) because this test's purpose
    is to catch silent threshold regressions back to 3. If someone bumps
    FAILURE_THRESHOLD to 3 (or 7), this test should fail loudly.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(side_effect=RuntimeError("snapshot failed"))

    # 4 failures: counter goes 1, 2, 3, 4 — state must remain RUNNING
    for tick in range(4):
        await scheduler._tick()
        assert scheduler.state == SchedulerState.RUNNING, (
            f"After {tick + 1} failures, expected RUNNING; got {scheduler.state}. "
            "Threshold may have regressed below 5."
        )

    # 5th failure: counter = 5 → RECOVERING
    await scheduler._tick()
    assert scheduler.state == SchedulerState.RECOVERING, (
        f"After 5 failures, expected RECOVERING; got {scheduler.state}. "
        "Threshold may have increased above 5."
    )
    assert scheduler._failure_counter == 5


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
async def test_legacy_paused_state_is_migrated_to_recovering(
    daemon_settings_for_test: Any,
) -> None:
    """An in-memory legacy PAUSED state cannot disable the producer forever."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler.state = SchedulerState.PAUSED

    run_mock = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))
    scheduler._run_snapshot = run_mock

    await scheduler._tick()

    run_mock.assert_called_once()
    assert scheduler.state == SchedulerState.RUNNING


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
async def test_structure_checkpoint_releases_slot_without_failure_or_alert(
    daemon_settings_for_test: Any,
) -> None:
    """A cooperative page slice is progress, not a degraded incident."""
    from polyarb.daemon.scheduler import IsolatedStructureCheckpoint
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        return_value=IsolatedStructureCheckpoint(
            window_id="window-1",
            stage="markets",
            pages_processed=80,
            elapsed_ms=40_000,
        )
    )

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()) as heartbeat:
        await scheduler._tick()

    attempt = store.get_latest_snapshot_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "cancelled"
    assert attempt["failure_kind"] == "structure-checkpoint"
    assert scheduler._failure_counter == 0
    assert scheduler.state == SchedulerState.RUNNING
    assert scheduler._checkpoint_pending is True
    heartbeat.assert_not_called()


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
async def test_successful_structure_publish_wakes_quote_worker(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    request_quote = MagicMock(return_value=True)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
        on_snapshot_published=request_quote,
    )
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    request_quote.assert_called_once_with()


@pytest.mark.asyncio
async def test_successful_tick_purges_expired_snapshots_on_attached_store(
    daemon_settings_for_test: Any,
) -> None:
    """Retention must run in the app process that owns the mounted volume."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    purge = MagicMock(return_value=(0, []))
    store.purge_old_snapshots = purge  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    purge.assert_called_once_with(
        older_than_days=7,
        keep_last=5,
        max_snapshots_per_run=10,
        parquet_root=daemon_settings_for_test.parquet_root,
    )


@pytest.mark.asyncio
async def test_success_persists_recovery_before_slow_retention(
    daemon_settings_for_test: Any,
) -> None:
    """Health must see recovery before bounded cleanup starts."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    store.upsert_scheduler_state(state="RECOVERING", failure_counter=7)
    observed_counters: list[int] = []

    def purge(**_kwargs):
        state = store.get_scheduler_state()
        assert state is not None
        observed_counters.append(int(state["failure_counter"]))
        return (0, [])

    store.purge_old_snapshots = purge  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    assert observed_counters == [0]


@pytest.mark.asyncio
async def test_successful_tick_purges_failed_then_old_published_structure_window(
    daemon_settings_for_test: Any,
) -> None:
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    purge_failed = MagicMock(return_value=(0, []))
    purge_published = MagicMock(return_value=(0, []))
    store.purge_failed_structure_sync_windows = purge_failed  # type: ignore[method-assign]
    store.purge_published_structure_sync_windows = purge_published  # type: ignore[method-assign]
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(return_value=_FakeResult(SnapshotStatus.OK))

    with patch("polyarb.daemon.alerts.send_heartbeat_ok", new=AsyncMock()):
        await scheduler._tick()

    purge_failed.assert_called_once_with(max_windows_per_run=1)
    purge_published.assert_called_once_with(keep_last=1, max_windows_per_run=1)


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
    """Counter=N-1 from prior shutdown → one more failure starts recovery at N.

    Phase 03.1-04 D-02: threshold is 5. We drive both halves from the class
    attribute so the persistence invariant survives future tuning.
    """
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()

    threshold = SnapshotScheduler.FAILURE_THRESHOLD
    pre_shutdown_counter = threshold - 1  # one short of trigger

    # First instance: run threshold-1 failures, then "shut down"
    scheduler1 = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler1._run_snapshot = AsyncMock(side_effect=RuntimeError("failed"))

    for _ in range(pre_shutdown_counter):
        await scheduler1._tick()

    assert scheduler1._failure_counter == pre_shutdown_counter
    assert scheduler1.state == SchedulerState.RUNNING

    # Second instance reads from DB → restores counter
    scheduler2 = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    assert scheduler2._failure_counter == pre_shutdown_counter, (
        f"Expected restored counter={pre_shutdown_counter}, "
        f"got {scheduler2._failure_counter}. Scheduler must persist counter to SQLite."
    )

    # One more failure → RECOVERING
    scheduler2._run_snapshot = AsyncMock(side_effect=RuntimeError("failed again"))
    await scheduler2._tick()

    assert scheduler2.state == SchedulerState.RECOVERING


@pytest.mark.asyncio
async def test_failure_threshold_enters_recovering_and_a_later_success_self_heals(
    daemon_settings_for_test: Any,
) -> None:
    """A repeated failure must not disable the only Structure producer forever."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._run_snapshot = AsyncMock(
        side_effect=[
            RuntimeError("upstream unavailable")
            for _ in range(SnapshotScheduler.FAILURE_THRESHOLD)
        ]
        + [_FakeResult(SnapshotStatus.OK, snapshot_id=99)]
    )

    for _ in range(SnapshotScheduler.FAILURE_THRESHOLD):
        await scheduler._tick()

    assert scheduler.state == SchedulerState.RECOVERING
    assert scheduler._failure_counter == SnapshotScheduler.FAILURE_THRESHOLD

    await scheduler._tick()

    assert scheduler.state == SchedulerState.RUNNING
    assert scheduler._failure_counter == 0
    assert scheduler._run_snapshot.await_count == SnapshotScheduler.FAILURE_THRESHOLD + 1
