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
import re
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

from loguru import logger

from polyarb.daemon.structure_schedule import derive_structure_schedule
from polyarb.validator.category import SnapshotStatus


class SchedulerState(StrEnum):
    """Scheduler state: RUNNING (normal) or PAUSED (5x consecutive failures)."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"


class SnapshotSubprocessError(RuntimeError):
    """The isolated snapshot process did not return one bounded result."""

    def __init__(
        self,
        reason: str,
        *,
        last_stage: str | None = None,
        elapsed_ms: int = 0,
    ) -> None:
        super().__init__(f"snapshot-subprocess-{reason}")
        self.last_stage = last_stage
        self.elapsed_ms = max(0, elapsed_ms)


SNAPSHOT_SUBPROCESS_TIMEOUT_S = 240.0

_SNAPSHOT_STAGE_MARKER_RE = re.compile(
    rb"^snapshot-stage stage="
    rb"(gamma-events|gamma-markets|membership-recheck|validate|persist) "
    rb"state=(?:start|complete) elapsed_ms=(?:0|[1-9][0-9]*)$",
    re.MULTILINE,
)


def _parse_last_snapshot_stage(stderr: bytes) -> str | None:
    """Extract only the final fixed-vocabulary stage marker from child stderr."""
    last_stage: str | None = None
    for marker in _SNAPSHOT_STAGE_MARKER_RE.finditer(stderr):
        last_stage = marker.group(1).decode("ascii")
    return last_stage


@dataclass(frozen=True)
class IsolatedSnapshotResult:
    status: SnapshotStatus
    snapshot_id: int
    market_count: int
    issue_count: int
    last_stage: str | None
    elapsed_ms: int


async def run_snapshot_in_subprocess(
    *,
    spawn: Callable[..., Awaitable[asyncio.subprocess.Process]] = (
        asyncio.create_subprocess_exec
    ),
    timeout_s: float = SNAPSHOT_SUBPROCESS_TIMEOUT_S,
    terminate_timeout_s: float = 3.0,
) -> IsolatedSnapshotResult:
    """Run the CPU/GIL-heavy snapshot pipeline outside the HTTP process."""
    process = await spawn(
        sys.executable,
        "-m",
        "polyarb.snapshot",
        "snapshot",
        "--product",
        "structure",
        "--json",
        "--low-priority",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    started = time.monotonic()
    logger.info(
        "isolated snapshot started "
        f"pid={getattr(process, 'pid', None)}"
    )
    # Own one pipe-reader task for the child lifetime.  A timeout/cancellation
    # must not cancel the reader and then attempt a second communicate(), which
    # can leave the original child alive after the scheduler records failure.
    communicate_task = asyncio.create_task(process.communicate())

    async def terminate_then_kill() -> tuple[bytes, bytes]:
        try:
            process.terminate()
        except ProcessLookupError:
            pass
        try:
            return await asyncio.wait_for(
                asyncio.shield(communicate_task),
                timeout=terminate_timeout_s,
            )
        except TimeoutError:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            return await asyncio.shield(communicate_task)

    def elapsed_ms() -> int:
        return max(0, int((time.monotonic() - started) * 1000))

    def subprocess_error(
        reason: str,
        *,
        stderr: bytes,
    ) -> SnapshotSubprocessError:
        return SnapshotSubprocessError(
            reason,
            last_stage=_parse_last_snapshot_stage(stderr),
            elapsed_ms=elapsed_ms(),
        )

    try:
        stdout, stderr = await asyncio.wait_for(
            asyncio.shield(communicate_task),
            timeout=timeout_s,
        )
    except asyncio.CancelledError:
        await terminate_then_kill()
        raise
    except TimeoutError as error:
        logger.error(
            "isolated snapshot timed out "
            f"pid={getattr(process, 'pid', None)} timeout_s={timeout_s}"
        )
        _, stderr = await terminate_then_kill()
        raise subprocess_error("timeout", stderr=stderr) from error

    last_stage = _parse_last_snapshot_stage(stderr)
    process_elapsed_ms = elapsed_ms()

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
            f"signal-{signal_name}{suffix}",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
        )

    try:
        payload = json.loads(stdout)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        logger.warning(
            "isolated snapshot returned invalid output "
            f"returncode={process.returncode} stderr_bytes={len(stderr)}"
        )
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
        ) from error
    if not isinstance(payload, dict):
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
        )
    try:
        status = SnapshotStatus(str(payload.get("status", "")).lower())
    except ValueError as error:
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
        ) from error
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
        raise SnapshotSubprocessError(
            "invalid-json",
            last_stage=last_stage,
            elapsed_ms=process_elapsed_ms,
        )
    logger.info(
        "isolated snapshot complete "
        f"pid={getattr(process, 'pid', None)} "
        f"elapsed_ms={process_elapsed_ms} "
        f"last_stage={last_stage} "
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
        last_stage=last_stage,
        elapsed_ms=process_elapsed_ms,
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
        self._request_now_event = asyncio.Event()
        self._restore_state()
        self._effective_timeout_s = int(SNAPSHOT_SUBPROCESS_TIMEOUT_S)
        self._effective_cadence_s = int(
            getattr(settings, "scheduler_interval_s", 300)
        )
        self._restore_effective_schedule()

    @property
    def effective_timeout_s(self) -> int:
        return self._effective_timeout_s

    @property
    def effective_cadence_s(self) -> int:
        return self._effective_cadence_s

    def _restore_effective_schedule(self) -> None:
        """Restore or derive one bounded schedule from durable attempt truth."""
        try:
            latest_adjustment = (
                self._sqlite_store.get_latest_structure_schedule_adjustment()
            )
            if latest_adjustment is not None:
                self._effective_timeout_s = int(latest_adjustment["timeout_s"])
                self._effective_cadence_s = int(latest_adjustment["cadence_s"])

            attempts = self._sqlite_store.get_snapshot_attempts(limit=30)
            if not attempts:
                return
            source_attempt_id = max(int(row["id"]) for row in attempts)
            latest_source_id = (
                int(latest_adjustment["source_attempt_id"])
                if latest_adjustment is not None
                else None
            )
            if latest_source_id == source_attempt_id:
                return

            attempts_since_adjustment = (
                max(0, source_attempt_id - latest_source_id)
                if latest_source_id is not None
                else 3
            )
            decision = derive_structure_schedule(
                attempts,
                configured_timeout_s=int(SNAPSHOT_SUBPROCESS_TIMEOUT_S),
                configured_cadence_s=int(
                    getattr(self._settings, "scheduler_interval_s", 300)
                ),
                previous_timeout_s=self._effective_timeout_s,
                previous_cadence_s=self._effective_cadence_s,
                attempts_since_adjustment=attempts_since_adjustment,
            )
            changed = (
                decision.timeout_s != self._effective_timeout_s
                or decision.cadence_s != self._effective_cadence_s
            )
            if not changed:
                return
            previous_timeout_s = self._effective_timeout_s
            previous_cadence_s = self._effective_cadence_s
            self._sqlite_store.append_structure_schedule_adjustment(
                source_attempt_id=source_attempt_id,
                decided_at_ms=int(time.time() * 1_000),
                success_sample_count=decision.success_sample_count,
                success_p95_s=decision.success_p95_s,
                previous_timeout_s=previous_timeout_s,
                previous_cadence_s=previous_cadence_s,
                timeout_s=decision.timeout_s,
                cadence_s=decision.cadence_s,
                reason=decision.reason,
            )
            self._effective_timeout_s = decision.timeout_s
            self._effective_cadence_s = decision.cadence_s
            logger.info(
                "structure schedule adjusted "
                f"timeout_s={decision.timeout_s} cadence_s={decision.cadence_s} "
                f"reason={decision.reason} source_attempt_id={source_attempt_id}"
            )
        except Exception as error:  # noqa: BLE001 - static config remains safe fallback
            logger.warning(
                "could not restore adaptive structure schedule "
                f"kind={type(error).__name__}"
            )

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
        return await run_snapshot_in_subprocess(timeout_s=self._effective_timeout_s)

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
        last_stage: str | None = None,
        elapsed_ms: int | None = None,
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
                last_stage=last_stage,
                elapsed_ms=elapsed_ms,
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
                    last_stage = getattr(result, "last_stage", None)
                    elapsed_ms = getattr(result, "elapsed_ms", 0)
                    raise SnapshotSubprocessError(
                        "missing-snapshot-id",
                        last_stage=last_stage if isinstance(last_stage, str) else None,
                        elapsed_ms=elapsed_ms if isinstance(elapsed_ms, int) else 0,
                    )
                await self._finish_attempt(
                    attempt_id=attempt_id,
                    outcome="succeeded",
                    snapshot_id=snapshot_id,
                    failure_kind=None,
                    last_stage=getattr(result, "last_stage", None),
                    elapsed_ms=getattr(result, "elapsed_ms", None),
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
                    last_stage=getattr(result, "last_stage", None),
                    elapsed_ms=getattr(result, "elapsed_ms", None),
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
                last_stage=getattr(error, "last_stage", None),
                elapsed_ms=getattr(error, "elapsed_ms", None),
            )
            self._failure_counter += 1
            logger.exception(
                f"snapshot tick raised exception "
                f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
            )

        # Persist counter before pause check
        self._persist_counter()
        self._restore_effective_schedule()

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

    def request_now(self) -> bool:
        """Wake the single scheduler loop for one normal tick, if available.

        This never calls ``_run_snapshot`` directly.  A paused scheduler is a
        safety boundary, and a set event records one pending request while an
        existing child owns the producer.
        """
        if self.state == SchedulerState.PAUSED or self._request_now_event.is_set():
            return False
        self._request_now_event.set()
        return True

    async def _wait_for_next_tick(self, stop_event: asyncio.Event, delay_s: float) -> bool:
        """Wait for stop, cadence, or one coalesced requested normal cycle."""
        if self._request_now_event.is_set():
            self._request_now_event.clear()
            return False
        stop_task = asyncio.create_task(stop_event.wait())
        request_task = asyncio.create_task(self._request_now_event.wait())
        try:
            done, pending = await asyncio.wait(
                (stop_task, request_task),
                timeout=delay_s,
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
            if stop_task in done and stop_task.result():
                return True
            if request_task in done and request_task.result():
                self._request_now_event.clear()
            return False
        except asyncio.CancelledError:
            stop_task.cancel()
            request_task.cancel()
            await asyncio.gather(stop_task, request_task, return_exceptions=True)
            raise

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
        logger.info(
            "scheduler loop started, "
            f"tick interval={self._effective_cadence_s}s "
            f"snapshot timeout={self._effective_timeout_s}s"
        )

        try:
            # Delay first tick 10s so uvicorn fully starts and Fly's health
            # check sees a live /health before the first Gamma fetch ties up
            # the event loop for 30-120s. Use wait_for(stop_event) so SIGINT
            # during startup delay is still <1s responsive.
            if await self._wait_for_next_tick(stop_event, 10):
                # stop_event was set during delay — exit immediately
                logger.info("scheduler: stop_event during startup delay, exiting")
                return

            while not stop_event.is_set():
                await self._tick()
                if await self._wait_for_next_tick(
                    stop_event,
                    self._effective_cadence_s,
                ):
                    break
        except asyncio.CancelledError:
            # F-04: graceful cancellation path. main.py may cancel this task
            # explicitly to interrupt an in-flight tick.
            logger.info("scheduler loop received CancelledError, exiting")
            raise

        logger.info("scheduler loop stopped")
