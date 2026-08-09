"""Deterministic capacity-watermark policy for M1 resident maintenance."""

from __future__ import annotations

import asyncio
import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

from loguru import logger

CapacityState = Literal["normal", "pressure", "critical", "exhaustion-imminent"]


@dataclass(frozen=True)
class CapacityPolicy:
    pressure_free_percent: float
    critical_free_percent: float
    exhaustion_free_percent: float
    recovery_hold_ms: int

    def __post_init__(self) -> None:
        if not (
            0.0 < self.exhaustion_free_percent < self.critical_free_percent
            < self.pressure_free_percent < 100.0
        ):
            raise ValueError("invalid-capacity-watermarks")
        if self.recovery_hold_ms < 0:
            raise ValueError("invalid-capacity-recovery-hold")

    def transition(
        self,
        previous: CapacityState | None,
        *,
        previous_state_started_at_ms: int | None = None,
        last_recovery_receipt_at_ms: int | None = None,
        free_percent: float,
        now_ms: int,
    ) -> CapacityState:
        if not 0.0 <= free_percent <= 100.0:
            raise ValueError("invalid-capacity-free-percent")
        if now_ms < 0:
            raise ValueError("invalid-capacity-time")
        if (
            last_recovery_receipt_at_ms is not None
            and (
                type(last_recovery_receipt_at_ms) is not int
                or last_recovery_receipt_at_ms < 0
                or last_recovery_receipt_at_ms > now_ms
            )
        ):
            raise ValueError("invalid-capacity-recovery-receipt")
        if free_percent <= self.exhaustion_free_percent:
            return "exhaustion-imminent"
        if free_percent <= self.critical_free_percent:
            return "critical"
        if free_percent <= self.pressure_free_percent:
            return "pressure"
        if (
            previous in {"pressure", "critical", "exhaustion-imminent"}
            and previous_state_started_at_ms is not None
            and (
                now_ms - previous_state_started_at_ms < self.recovery_hold_ms
                or last_recovery_receipt_at_ms is None
                or last_recovery_receipt_at_ms < previous_state_started_at_ms
            )
        ):
            return previous
        return "normal"


class CapacityController:
    """Run one low-priority, Quote-aware capacity maintenance decision."""

    def __init__(
        self,
        *,
        store: object,
        policy: CapacityPolicy,
        clock_ms: Callable[[], int] | None = None,
        retry_delay_ms: int = 5_000,
    ) -> None:
        if retry_delay_ms < 1:
            raise ValueError("invalid-capacity-retry-delay")
        self._store = store
        self._policy = policy
        self._clock_ms = clock_ms or (lambda: int(time.time() * 1_000))
        self._retry_delay_ms = retry_delay_ms

    def run_once(self, *, quote_priority: bool) -> dict[str, object]:
        """Measure first; defer immediately when Quote owns the hot path."""
        usage = shutil.disk_usage(self._store.db_path.parent)
        free_percent = 100.0 * usage.free / usage.total if usage.total else 0.0
        now_ms = self._clock_ms()
        previous = self._store.capacity_controller_runtime_status()
        state = self._policy.transition(
            previous["state"],
            previous_state_started_at_ms=previous["state_started_at_ms"],
            last_recovery_receipt_at_ms=previous["last_recovery_receipt_at_ms"],
            free_percent=free_percent,
            now_ms=now_ms,
        )
        self._store.record_capacity_controller_measurement(
            state=state,
            free_bytes=usage.free,
            free_percent=free_percent,
            observed_at_ms=now_ms,
        )
        if state == "normal":
            return self._store.capacity_controller_runtime_status()
        if quote_priority:
            return self._store.defer_capacity_controller_attempt(
                action="quote-priority",
                now_ms=now_ms,
                next_attempt_at_ms=now_ms + self._retry_delay_ms,
            )
        try:
            deleted_count, deleted_ids = self._store.purge_old_snapshots(
                older_than_days=7,
                keep_last=5,
                max_snapshots_per_run=10,
            )
        except (OSError, sqlite3.Error) as error:
            error_kind = (
                "writer-busy"
                if isinstance(error, sqlite3.OperationalError)
                and any(marker in str(error).lower() for marker in ("locked", "busy"))
                else type(error).__name__[:64]
            )
            return self._store.record_capacity_controller_failure(
                error_kind=error_kind,
                now_ms=now_ms,
                next_attempt_at_ms=now_ms + self._retry_delay_ms,
            )
        return self._store.record_capacity_controller_reclaim(
            action="reclaimed-snapshots",
            deleted_count=deleted_count,
            deleted_ids=deleted_ids,
            completed_at_ms=now_ms,
        )


class CapacityMaintenanceWorker:
    """Resident capacity owner that releases the shared slot to Quote first."""

    def __init__(
        self,
        *,
        controller: CapacityController,
        producer_lock: asyncio.Lock,
        quote_worker_runtime: object | None,
        quote_interval_s: float,
        interval_s: float,
        incident_lifecycle: object | None = None,
    ) -> None:
        if quote_interval_s <= 0 or interval_s <= 0:
            raise ValueError("invalid-capacity-maintenance-interval")
        self._controller = controller
        self._producer_lock = producer_lock
        self._quote_runtime = quote_worker_runtime
        self._quote_interval_s = quote_interval_s
        self._interval_s = interval_s
        self._incident_lifecycle = incident_lifecycle

    def _quote_priority(self) -> bool:
        runtime = self._quote_runtime
        return bool(
            runtime is not None
            and (runtime.pipeline_active() or runtime.pipeline_due(self._quote_interval_s))
        )

    async def _tick(self) -> None:
        if self._quote_priority():
            runtime = await asyncio.to_thread(
                self._controller.run_once, quote_priority=True
            )
        else:
            async with self._producer_lock:
                runtime = await asyncio.to_thread(
                    self._controller.run_once,
                    quote_priority=self._quote_priority(),
                )
        if self._incident_lifecycle is not None:
            await asyncio.to_thread(self._incident_lifecycle.observe, runtime)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self._tick()
            except asyncio.CancelledError:
                raise
            except Exception as error:  # noqa: BLE001 - never kills Quote sibling
                logger.exception(
                    "capacity maintenance tick failed kind=%s",
                    type(error).__name__,
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self._interval_s)
            except TimeoutError:
                pass


__all__ = [
    "CapacityController",
    "CapacityMaintenanceWorker",
    "CapacityPolicy",
    "CapacityState",
]
