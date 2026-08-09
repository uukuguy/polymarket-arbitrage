"""Deterministic capacity-watermark policy for M1 resident maintenance."""

from __future__ import annotations

import shutil
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

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


__all__ = ["CapacityController", "CapacityPolicy", "CapacityState"]
