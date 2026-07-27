"""SnapshotScheduler — 5-failure-pause state machine.

Phase 02 Plan 02 — D-13 / T-02-04.
Phase 03.1-04 — D-02: threshold raised 3 → 5 to give tenacity DNS retry (D-01 A)
room to absorb transient EAI_NODATA without prematurely flipping to PAUSED.
At ~37s snapshot cadence, 5 consecutive failures = ~3min observation window;
healthz-watcher (15-min cron) will auto-unpause before manual intervention
is required.

State machine:
  RUNNING → tick OK/DEGRADED → reset counter, stay RUNNING
  RUNNING → tick FAILED/exception → counter += 1
  RUNNING + counter >= FAILURE_THRESHOLD → transition to PAUSED
  PAUSED → tick → skip (no run_snapshot call)
  PAUSED → manual unpause (via /scan resume or SSH) → RUNNING, counter = 0

Design decisions:
- DEGRADED is NOT a failure (D-12 amendment): 3x DEGRADED does NOT pause.
  Only SnapshotStatus.FAILED and uncaught exceptions count as failures.
- Counter persists to SQLite (scheduler_state table, singleton row) so a
  restart after 2 failures still knows it's at counter=2.
- _run_snapshot is async and injectable (tests replace it with AsyncMock).
- _on_paused() is a stub alert hook — Plan 05 wires it to Sentry/Telegram.
- run() is a placeholder loop for local testing; Plan 04 uses Fly scheduled
  machines for real prod cron (not this loop).

Source: RESEARCH.md §Architecture Patterns §2.5, CONTEXT.md D-13
"""

from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from loguru import logger

from polyarb.validator.category import SnapshotStatus


class SchedulerState(StrEnum):
    """Scheduler state: RUNNING (normal) or PAUSED (5x consecutive failures)."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"


class SnapshotSubprocessError(RuntimeError):
    """The isolated snapshot process did not return one bounded result."""

    def __init__(self, reason: str) -> None:
        super().__init__(f"snapshot-subprocess-{reason}")


@dataclass(frozen=True)
class IsolatedSnapshotResult:
    status: SnapshotStatus
    snapshot_id: int
    market_count: int
    issue_count: int


async def run_snapshot_in_subprocess(
    *,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (
        asyncio.create_subprocess_exec
    ),
    terminate_timeout_s: float = 3.0,
) -> IsolatedSnapshotResult:
    """Run the CPU/GIL-heavy snapshot pipeline outside the HTTP process."""
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.snapshot",
        "snapshot",
        "--json",
        "--low-priority",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.perf_counter()
    logger.info(
        "isolated snapshot started "
        f"pid={getattr(process, 'pid', None)}"
    )
    try:
        stdout, stderr = await process.communicate()
    except asyncio.CancelledError:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            await asyncio.wait_for(
                process.communicate(),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.communicate()
        raise

    if process.returncode is not None and process.returncode < 0:
        signal_number = -process.returncode
        try:
            signal_name = signal.Signals(signal_number).name.lower()
        except ValueError:
            signal_name = str(signal_number)
        possible_oom = signal_number == signal.SIGKILL
        logger.error(
            "isolated snapshot terminated by signal "
            f"pid={getattr(process, 'pid', None)} "
            f"exit_class=signal signal={signal_name.upper()} "
            f"oom_hint={'possible-cgroup-oom' if possible_oom else 'none'} "
            f"stderr_bytes={len(stderr)}"
        )
        suffix = "-possible-oom" if possible_oom else ""
        raise SnapshotSubprocessError(
            f"signal-{signal_name}{suffix}"
        )

    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        logger.warning(
            "isolated snapshot returned invalid output "
            f"returncode={process.returncode} stderr_bytes={len(stderr)}"
        )
        raise SnapshotSubprocessError("invalid-json") from error
    if not isinstance(payload, dict):
        raise SnapshotSubprocessError("invalid-json")
    try:
        status = SnapshotStatus(str(payload.get("status", "")).lower())
    except ValueError as error:
        raise SnapshotSubprocessError("invalid-json") from error
    is_valid = payload.get("is_valid")
    snapshot_id = payload.get("snapshot_id")
    market_count = payload.get("market_count")
    issue_count = payload.get("issue_count")
    if (
        not isinstance(is_valid, bool)
        or isinstance(snapshot_id, bool)
        or not isinstance(snapshot_id, int)
        or snapshot_id <= 0
        or isinstance(market_count, bool)
        or not isinstance(market_count, int)
        or market_count < 0
        or isinstance(issue_count, bool)
        or not isinstance(issue_count, int)
        or issue_count < 0
        or (status == SnapshotStatus.FAILED) == is_valid
        or (process.returncode == 0) != is_valid
    ):
        raise SnapshotSubprocessError("invalid-json")
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    logger.info(
        "isolated snapshot complete "
        f"pid={getattr(process, 'pid', None)} "
        f"elapsed_ms={elapsed_ms} "
        f"status={status.value} "
        f"snapshot_id={snapshot_id} "
        f"market_count={market_count} "
        f"issue_count={issue_count}"
    )
    return IsolatedSnapshotResult(
        status=status,
        snapshot_id=snapshot_id,
        market_count=market_count,
        issue_count=issue_count,
    )


class SnapshotScheduler:
    """Manages snapshot scheduling with 5-failure pause protection.

    Usage:
        scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
        await scheduler._tick()       # unit-testable single tick
        await scheduler.run(stop_ev)  # long-running loop (Plan 02 placeholder)
    """

    # Phase 03.1-04 D-02: 3 → 5. Combined with DNS retry (D-01 A) the threshold
    # tolerates ~3min of bursty failure before pausing; healthz-watcher cron
    # (15-min) auto-unpauses well within human-response cadence.
    FAILURE_THRESHOLD = 5

    def __init__(self, settings: object, sqlite_store: object) -> None:
        self._settings = settings
        self._sqlite_store = sqlite_store

        # Restore state from DB (test_counter_persists_across_restart)
        self._failure_counter = 0
        self.state = SchedulerState.RUNNING
        self._restore_state()

    def _restore_state(self) -> None:
        """Read scheduler_state from SQLite and restore counter + state."""
        try:
            row = self._sqlite_store.get_scheduler_state()
            if row:
                self._failure_counter = int(row.get("failure_counter", 0))
                state_str = row.get("state", "RUNNING").upper()
                self.state = (
                    SchedulerState(state_str)
                    if state_str in SchedulerState.__members__
                    else SchedulerState.RUNNING
                )
                logger.debug(
                    f"scheduler state restored: state={self.state} "
                    f"failure_counter={self._failure_counter}"
                )
        except Exception:
            logger.warning("could not restore scheduler state from DB, starting fresh")

    def _persist_counter(self) -> None:
        """Write current state + counter to scheduler_state SQLite table."""
        try:
            self._sqlite_store.upsert_scheduler_state(
                state=self.state.value,
                failure_counter=self._failure_counter,
            )
        except Exception:
            logger.warning("could not persist scheduler state to DB")

    async def _run_snapshot(self) -> object:
        """Run a snapshot and return a result with .status attribute.

        This method is injectable — tests replace it with AsyncMock.
        Real prod wires it to the orchestrator in Plan 04.
        """
        return await run_snapshot_in_subprocess()

    async def _on_paused(self) -> None:
        """Alert hook called when scheduler transitions to PAUSED state.

        Plan 02: stub (logs only).
        Plan 05: wires to alerts.send_paused_alert (Sentry + Better Stack +
            Telegram fallback). We reference via the module attribute
            ``alerts.send_paused_alert`` (not a from-import) so tests can
            monkeypatch the function on the module.
        """
        logger.error(
            "SCHEDULER_PAUSED: consecutive failure threshold reached "
            f"(counter={self._failure_counter}). Manual restart required. "
            "Run /scan or SSH to unpause."
        )
        try:
            from polyarb.daemon import alerts as _alerts

            await _alerts.send_paused_alert(
                self._settings,
                reason=f"{self._failure_counter} consecutive FAILED snapshots",
            )
        except Exception as e:  # noqa: BLE001
            # Alerts are fail-soft: if every channel is unreachable, the
            # daemon should still pause cleanly — losing the notification
            # is bad, losing the pause-state is worse.
            logger.warning(f"send_paused_alert failed: {e!r}")

    async def _finish_attempt(
        self,
        *,
        attempt_id: int,
        outcome: str,
        snapshot_id: int | None,
        failure_kind: str | None,
    ) -> None:
        """Best-effort terminal record; scheduler behavior remains primary truth."""
        try:
            await asyncio.to_thread(
                self._sqlite_store.finish_snapshot_attempt,
                attempt_id=attempt_id,
                outcome=outcome,
                finished_at_ms=int(time.time() * 1000),
                snapshot_id=snapshot_id,
                failure_kind=failure_kind,
            )
        except Exception as error:  # noqa: BLE001 - operational evidence is fail-soft
            logger.warning(
                "could not finish snapshot attempt "
                f"attempt_id={attempt_id} kind={type(error).__name__}"
            )

    async def _tick(self) -> None:
        """Execute one scheduler tick.

        If PAUSED: skip (no-op).
        If RUNNING: run snapshot, update counter, check threshold.

        F-04 (Plan 02-08): cancellation propagates. asyncio.CancelledError
        is NOT caught by the generic Exception handler — we re-raise so
        run() can unwind. Wave 5 chaos test gates on this.
        """
        if self.state == SchedulerState.PAUSED:
            logger.info("scheduler is PAUSED, skipping tick")
            return

        attempt_id = await asyncio.to_thread(
            self._sqlite_store.begin_snapshot_attempt,
            started_at_ms=int(time.time() * 1000),
        )

        try:
            result = await self._run_snapshot()
            result_status = getattr(result, "status", None)

            if result_status in (SnapshotStatus.OK, SnapshotStatus.DEGRADED):
                snapshot_id = getattr(result, "snapshot_id", None)
                if not isinstance(snapshot_id, int) or snapshot_id <= 0:
                    raise SnapshotSubprocessError("missing-snapshot-id")
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="succeeded",
                    snapshot_id=snapshot_id,
                    failure_kind=None,
                )
                # DEGRADED is NOT a failure (D-12 amendment)
                self._failure_counter = 0
                logger.info(
                    f"snapshot tick success: status={result_status} failure_counter reset to 0"
                )
                # Plan 02-05 fix-up: Better Stack heartbeat OK pulse.
                # Reference via the module attribute (not from-import) so tests
                # can monkeypatch alerts.send_heartbeat_ok. Fail-soft already
                # encapsulated inside send_heartbeat_ok itself.
                try:
                    from polyarb.daemon import alerts as _alerts

                    await _alerts.send_heartbeat_ok(self._settings)
                except Exception as e:  # noqa: BLE001
                    logger.warning(f"send_heartbeat_ok failed: {e!r}")
                try:
                    deleted, _ = await asyncio.to_thread(
                        self._sqlite_store.purge_old_snapshots,
                        older_than_days=7,
                        keep_last=5,
                        max_snapshots_per_run=10,
                        parquet_root=self._settings.parquet_root,
                    )
                    if deleted:
                        logger.info(f"snapshot retention deleted {deleted} expired snapshots")
                except Exception as e:  # noqa: BLE001
                    # Retention is fail-soft relative to a valid fresh snapshot,
                    # but its failure remains visible in production logs.
                    logger.warning(f"snapshot retention failed: {e!r}")
            else:
                snapshot_id = getattr(result, "snapshot_id", None)
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="failed",
                    snapshot_id=(snapshot_id if isinstance(snapshot_id, int) else None),
                    failure_kind="snapshot-status-failed",
                )
                # FAILED status
                self._failure_counter += 1
                logger.warning(
                    f"snapshot tick FAILED: status={result_status} "
                    f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
                )

        except asyncio.CancelledError:
            # F-04: cancellation must propagate so run() can stop in <1s.
            # Do NOT count as a failure — this is a graceful shutdown signal.
            await self._finish_attempt(
                attempt_id=attempt_id,
                outcome="cancelled",
                snapshot_id=None,
                failure_kind="scheduler-cancelled",
            )
            logger.info("scheduler tick cancelled mid-flight; propagating CancelledError")
            raise
        except Exception as error:
            await self._finish_attempt(
                attempt_id=attempt_id,
                outcome="failed",
                snapshot_id=None,
                failure_kind=str(error),
            )
            self._failure_counter += 1
            logger.exception(
                f"snapshot tick raised exception "
                f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
            )

        # Persist counter before pause check
        self._persist_counter()

        # Transition to PAUSED if threshold reached
        if self._failure_counter >= self.FAILURE_THRESHOLD:
            self.state = SchedulerState.PAUSED
            self._persist_counter()  # persist PAUSED state
            await self._on_paused()

    def unpause(self) -> None:
        """Manually unpause the scheduler (called via /scan or SSH).

        Resets failure counter. Plan 04 exposes this via daemon management endpoint.
        """
        self.state = SchedulerState.RUNNING
        self._failure_counter = 0
        self._persist_counter()
        logger.info("scheduler unpaused manually, failure_counter reset to 0")

    async def run(self, stop_event: asyncio.Event) -> None:
        """Long-running scheduler loop (Plan 02 placeholder).

        Plan 02: sleeps between ticks using settings interval (default 1 hour).
        Plan 04: real prod uses Fly scheduled machines (not this loop).

        F-04 (Plan 02-08): inner sleep granularity dropped from 10s → 1s so
        a SIGINT-triggered stop_event.set() is observed within 1s, satisfying
        the Wave 5 chaos test "< 1s graceful shutdown" gate. Also use
        asyncio.wait_for on stop_event so cancellation interrupts the wait
        immediately rather than after the next 1s tick.
        """
        interval_s = self._settings.scheduler_interval_s
        logger.info(f"scheduler loop started, tick interval={interval_s}s")

        try:
            # Delay first tick 10s so uvicorn fully starts and Fly's health
            # check sees a live /health before the first Gamma fetch ties up
            # the event loop for 30-120s. Use wait_for(stop_event) so SIGINT
            # during startup delay is still <1s responsive.
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=10)
                # stop_event was set during delay — exit immediately
                logger.info("scheduler: stop_event during startup delay, exiting")
                return
            except TimeoutError:
                pass  # normal: 10s elapsed, proceed to first tick

            while not stop_event.is_set():
                await self._tick()
                # Wait for interval, checking stop_event at 1s granularity.
                # F-04: 10s → 1s. Wave 5 chaos test gates on <1s shutdown.
                elapsed = 0.0
                while elapsed < interval_s and not stop_event.is_set():
                    step = min(1.0, interval_s - elapsed)
                    try:
                        # Use wait_for(stop_event.wait, ...) so an external
                        # task.cancel() lands immediately rather than after
                        # the 1s sleep completes.
                        await asyncio.wait_for(stop_event.wait(), timeout=step)
                        # stop_event fired during the wait — exit inner loop
                        break
                    except TimeoutError:
                        elapsed += step
        except asyncio.CancelledError:
            # F-04: graceful cancellation path. main.py may cancel this task
            # explicitly to interrupt an in-flight tick.
            logger.info("scheduler loop received CancelledError, exiting")
            raise

        logger.info("scheduler loop stopped")
