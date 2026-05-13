"""SnapshotScheduler — 3-failure-pause state machine.

Phase 02 Plan 02 — D-13 / T-02-04.

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
import time
from enum import Enum

from loguru import logger

from polyarb.validator.category import SnapshotStatus


class SchedulerState(str, Enum):
    """Scheduler state: RUNNING (normal) or PAUSED (3x consecutive failures)."""

    RUNNING = "RUNNING"
    PAUSED = "PAUSED"


class SnapshotScheduler:
    """Manages snapshot scheduling with 3-failure pause protection.

    Usage:
        scheduler = SnapshotScheduler(settings=settings, sqlite_store=store)
        await scheduler._tick()       # unit-testable single tick
        await scheduler.run(stop_ev)  # long-running loop (Plan 02 placeholder)
    """

    FAILURE_THRESHOLD = 3

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
                self.state = SchedulerState(state_str) if state_str in SchedulerState.__members__ else SchedulerState.RUNNING
                logger.debug(
                    f"scheduler state restored: state={self.state} failure_counter={self._failure_counter}"
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
        from polyarb.config import load_settings
        from polyarb.snapshot.orchestrator import run_snapshot

        # Result object with status attribute (matches SnapshotResult interface)
        class _Result:
            def __init__(self, is_valid: bool) -> None:
                self.status = SnapshotStatus.OK if is_valid else SnapshotStatus.FAILED

        result = await run_snapshot(self._settings)
        return _Result(is_valid=result.is_valid if hasattr(result, "is_valid") else True)

    async def _on_paused(self) -> None:
        """Alert hook called when scheduler transitions to PAUSED state.

        Plan 02: stub (logs only).
        Plan 05: wires to Sentry.capture_message + Better Stack heartbeat stop.
        """
        logger.error(
            "SCHEDULER_PAUSED: consecutive failure threshold reached "
            f"(counter={self._failure_counter}). Manual restart required. "
            "Run /scan or SSH to unpause."
        )

    async def _tick(self) -> None:
        """Execute one scheduler tick.

        If PAUSED: skip (no-op).
        If RUNNING: run snapshot, update counter, check threshold.
        """
        if self.state == SchedulerState.PAUSED:
            logger.info("scheduler is PAUSED, skipping tick")
            return

        try:
            result = await self._run_snapshot()
            result_status = getattr(result, "status", None)

            if result_status in (SnapshotStatus.OK, SnapshotStatus.DEGRADED):
                # DEGRADED is NOT a failure (D-12 amendment)
                self._failure_counter = 0
                logger.info(
                    f"snapshot tick success: status={result_status} "
                    f"failure_counter reset to 0"
                )
            else:
                # FAILED status
                self._failure_counter += 1
                logger.warning(
                    f"snapshot tick FAILED: status={result_status} "
                    f"failure_counter={self._failure_counter}/{self.FAILURE_THRESHOLD}"
                )

        except Exception:
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
        """
        interval_s = getattr(self._settings, "scheduler_interval_s", 3600)
        logger.info(f"scheduler loop started, tick interval={interval_s}s")

        while not stop_event.is_set():
            await self._tick()
            # Wait for interval, checking stop_event every 10 seconds
            elapsed = 0
            while elapsed < interval_s and not stop_event.is_set():
                await asyncio.sleep(min(10, interval_s - elapsed))
                elapsed += 10

        logger.info("scheduler loop stopped")
