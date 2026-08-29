"""Regression tests for daemon graceful shutdown (Plan 02-08 F-04).

Background: Phase 1 调试期 verification found that `pkill -INT polyarb.daemon.main`
required `pkill -9` workarounds because the scheduler loop polled stop_event
only every 10s. Wave 5 chaos test requires < 1s graceful shutdown.

Fix (F-04, hardened after the timeout-authority audit):
1. scheduler.run() inner sleep granularity 10s → 1s
2. _tick() is cancellation-aware (raises CancelledError up so the task
   actually unwinds)
3. main.py explicitly cancels scheduler_task after stop_event fires, then lets
   each task finish its own bounded recovery contract without a contradictory
   outer timeout cancelling cleanup a second time

The tests below mock _run_snapshot so we test the scheduler's responsiveness
in isolation — the orchestrator's actual runtime is out of scope (it takes
~minutes per snapshot and is itself cancellation-aware via async/await).
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
import tomllib
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def test_shutdown_authority_outlives_structure_child_cleanup() -> None:
    from polyarb.daemon import main as daemon_main
    from polyarb.daemon.scheduler import STRUCTURE_SUBPROCESS_SHUTDOWN_BUDGET_S

    source = inspect.getsource(daemon_main.main)
    fly_config = tomllib.loads(Path("fly.toml").read_text())

    assert STRUCTURE_SUBPROCESS_SHUTDOWN_BUDGET_S == 30.0
    assert "timeout=5.0" not in source
    assert "timeout_graceful_shutdown=STRUCTURE_SUBPROCESS_SHUTDOWN_BUDGET_S" in source
    assert fly_config["kill_signal"] == "SIGTERM"
    assert fly_config["kill_timeout"] > STRUCTURE_SUBPROCESS_SHUTDOWN_BUDGET_S


def test_l2_shutdown_uses_one_budget_below_the_platform_window() -> None:
    from polyarb.daemon import l2_main
    from polyarb.daemon.lifecycle import (
        DAEMON_TASK_DRAIN_BUDGET_SECONDS,
        PLATFORM_TERMINATION_WINDOW_SECONDS,
    )

    fly_config = tomllib.loads(Path("fly-l2.toml").read_text())
    timeout_default = inspect.signature(l2_main._drain_daemon_tasks).parameters["timeout_s"].default

    assert timeout_default == DAEMON_TASK_DRAIN_BUDGET_SECONDS
    assert 0 < DAEMON_TASK_DRAIN_BUDGET_SECONDS < PLATFORM_TERMINATION_WINDOW_SECONDS
    assert fly_config["kill_signal"] == "SIGTERM"
    assert fly_config["kill_timeout"] == PLATFORM_TERMINATION_WINDOW_SECONDS


@pytest.mark.asyncio
async def test_supervisor_sigkill_wait_and_output_drain_are_bounded() -> None:
    from polyarb.perception.supervisor import ProducerSupervisor

    class WedgedProcess:
        returncode = None

        def __init__(self) -> None:
            self.terminated = False
            self.killed = False

        def terminate(self) -> None:
            self.terminated = True

        def kill(self) -> None:
            self.killed = True

        async def wait(self) -> int:
            await asyncio.Event().wait()
            return -9

    process = WedgedProcess()
    await asyncio.wait_for(
        ProducerSupervisor._terminate(process, 0.001),  # type: ignore[arg-type]
        timeout=0.1,
    )
    drain_task = asyncio.create_task(asyncio.Event().wait())
    output = await asyncio.wait_for(
        ProducerSupervisor._drain_result(  # type: ignore[arg-type]
            drain_task,
            timeout_s=0.001,
        ),
        timeout=0.1,
    )

    assert process.terminated is True
    assert process.killed is True
    assert output == b"[output-read-timeout]"
    assert drain_task.done()


@pytest.mark.asyncio
@pytest.mark.parametrize("race_stage", ["terminate", "kill"])
async def test_supervisor_shutdown_accepts_already_exited_process_races(race_stage: str) -> None:
    from polyarb.perception.supervisor import ProducerSupervisor

    class ExitedProcess:
        returncode = None

        def terminate(self) -> None:
            if race_stage == "terminate":
                raise ProcessLookupError

        def kill(self) -> None:
            if race_stage == "kill":
                raise ProcessLookupError

        async def wait(self) -> int:
            await asyncio.Event().wait()
            return -9

    await asyncio.wait_for(
        ProducerSupervisor._terminate(ExitedProcess(), 0.001),  # type: ignore[arg-type]
        timeout=0.1,
    )


def _make_settings(tmp_path: Path, interval_s: int = 60) -> MagicMock:
    """Minimal Settings stub for the scheduler — uses MagicMock so
    new attributes resolve without raising AttributeError."""
    s = MagicMock()
    s.scheduler_interval_s = interval_s
    s.db_path = tmp_path / "t.db"
    return s


def _make_store(tmp_path: Path) -> object:
    """Provide a real SQLiteStore so scheduler_state persistence works."""
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "t.db")
    store.init_schema()
    return store


# ---------------------------------------------------------------------------
# F-04 test 1: scheduler.run() stops within 1.5s of stop_event being set,
# when no tick is in progress (the inter-tick wait dominates).
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scheduler_stops_within_1s_of_stop_event(tmp_path: Path) -> None:
    """run() must exit ≤ 1s after stop_event.set() during the inter-tick wait.

    Pre-F-04: inner sleep was 10s, so the loop could wait up to 10s after a
    SIGINT before checking stop_event. Wave 5 chaos test requires < 1s.
    """
    from polyarb.daemon.scheduler import SnapshotScheduler

    settings = _make_settings(tmp_path, interval_s=60)  # long interval
    store = _make_store(tmp_path)

    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
    # Replace the snapshot runner with a fast no-op so the first tick returns
    # instantly and the loop drops into the inter-tick wait.
    scheduler._run_snapshot = AsyncMock(
        return_value=type(
            "R",
            (),
            {
                "status": __import__(
                    "polyarb.validator.category", fromlist=["SnapshotStatus"]
                ).SnapshotStatus.OK
            },
        )()
    )

    stop = asyncio.Event()
    run_task = asyncio.create_task(scheduler.run(stop))

    # Let the first tick complete + scheduler drop into inter-tick wait
    await asyncio.sleep(0.1)
    assert not run_task.done(), "scheduler exited prematurely"

    t0 = time.monotonic()
    stop.set()
    try:
        await asyncio.wait_for(run_task, timeout=2.0)
    except TimeoutError:
        run_task.cancel()
        pytest.fail("scheduler.run() did not exit within 2s of stop_event")
    elapsed = time.monotonic() - t0

    assert elapsed < 1.5, (
        f"scheduler took {elapsed:.2f}s to stop after stop_event; "
        f"F-04 contract is < 1.5s (target 1s)"
    )


# ---------------------------------------------------------------------------
# F-04 test 2: cancelling the scheduler task interrupts an in-flight tick.
# A real snapshot tick takes tens of seconds; cancellation must propagate.
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_in_flight_tick_responds_to_cancellation(tmp_path: Path) -> None:
    """If a tick is mid-flight (snapshot taking ~minutes), task.cancel() must
    interrupt it within ≤ 1s.

    We mock _run_snapshot to be a long sleep — simulating the orchestrator
    holding the event loop on an await call (e.g. waiting on Gamma HTTP).
    The scheduler must propagate CancelledError so the task actually unwinds.
    """
    from polyarb.daemon.scheduler import SnapshotScheduler

    settings = _make_settings(tmp_path, interval_s=60)
    store = _make_store(tmp_path)

    scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)

    async def _slow_snapshot() -> object:
        # Simulate an in-flight orchestrator call — long await that yields
        # control back to the event loop (cancellation can land here).
        await asyncio.sleep(30)
        return type(
            "R",
            (),
            {
                "status": __import__(
                    "polyarb.validator.category", fromlist=["SnapshotStatus"]
                ).SnapshotStatus.OK
            },
        )()

    scheduler._run_snapshot = _slow_snapshot  # type: ignore[assignment]

    stop = asyncio.Event()
    run_task = asyncio.create_task(scheduler.run(stop))
    await asyncio.sleep(0.1)  # let tick begin

    t0 = time.monotonic()
    run_task.cancel()
    with pytest.raises((asyncio.CancelledError, BaseException)):
        await run_task
    elapsed = time.monotonic() - t0
    assert elapsed < 1.5, f"in-flight tick took {elapsed:.2f}s to cancel; F-04 requires < 1.5s"


# ---------------------------------------------------------------------------
# F-04 test 3: scheduler.run() inter-tick loop polls stop_event at <= 1s
# granularity (structural test on the sleep argument).
# ---------------------------------------------------------------------------


def test_run_loop_inner_sleep_at_most_1s_per_iter() -> None:
    """The inner inter-tick wait must call asyncio.sleep with arg <= 1.

    Pre-F-04 code: `await asyncio.sleep(min(10, interval_s - elapsed))`.
    Post-fix: `await asyncio.sleep(min(1, interval_s - elapsed))`.

    Structural test — assertion is on the source code so refactors don't
    silently regress the granularity.
    """
    import inspect

    from polyarb.daemon import scheduler as scheduler_mod

    src = inspect.getsource(scheduler_mod.SnapshotScheduler.run)
    # The fix uses `min(1, ...)` or equivalent — confirm no `min(10, ...)`.
    assert "min(10," not in src.replace(" ", ""), (
        "scheduler.run() still uses min(10, ...) inner sleep — F-04 not applied"
    )
    # And the loop must reference stop_event so cancellation works
    assert "stop_event" in src


@pytest.mark.asyncio
async def test_http_startup_failure_is_durable_and_cleans_server_task(tmp_path: Path) -> None:
    from polyarb.daemon.main import _abort_http_startup, _wait_for_http_startup
    from polyarb.perception.incidents import IncidentManager
    from polyarb.perception.store import OpportunityPerceptionStore

    class FailedServer:
        started = False
        should_exit = False

        async def serve(self) -> None:
            raise OSError("address already in use")

    server = FailedServer()
    server_task = asyncio.create_task(server.serve())
    with pytest.raises(RuntimeError, match="http-server-startup-failed") as caught:
        await _wait_for_http_startup(server, server_task, timeout_s=0.2)

    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    await _abort_http_startup(
        server,
        server_task,
        IncidentManager(store),
        caught.value,
    )

    incident = store.open_incidents()[0]
    assert incident.scope == "http"
    assert incident.kind == "startup-failure"
    assert incident.state == "escalated"
    assert server.should_exit is True
    assert server_task.done()
