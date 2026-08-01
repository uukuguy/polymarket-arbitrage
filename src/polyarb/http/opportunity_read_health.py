"""Bounded execution lanes and chain truth for opportunity authority reads."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from typing import Any, TypeVar

T = TypeVar("T")


class ReadLaneSaturatedError(RuntimeError):
    """Every worker is still occupied by an earlier timed-out operation."""


class BoundedReadLane:
    """A dedicated executor whose running and queued work is strictly bounded."""

    def __init__(self, name: str, *, capacity: int = 2) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=capacity,
            thread_name_prefix=name,
        )
        self._slots = threading.BoundedSemaphore(capacity)

    async def run(
        self,
        function: Callable[..., T],
        *args: Any,
        timeout_s: float,
        **kwargs: Any,
    ) -> T:
        if not self._slots.acquire(blocking=False):
            raise ReadLaneSaturatedError("read-lane-saturated")
        try:
            future = self._executor.submit(partial(function, *args, **kwargs))
        except BaseException:
            self._slots.release()
            raise
        future.add_done_callback(lambda _future: self._slots.release())
        wrapped = asyncio.wrap_future(future)
        return await asyncio.wait_for(asyncio.shield(wrapped), timeout=timeout_s)


@dataclass(frozen=True)
class OpportunityReadHealthSnapshot:
    source_truth_status: str
    source_truth_consecutive_failures: int
    source_truth_last_error_kind: str | None
    source_truth_last_attempt_at_s: float | None
    source_truth_last_live_success_at_s: float | None
    source_truth_last_fallback_at_s: float | None
    source_truth_failure_started_at_s: float | None
    lifecycle_status: str
    lifecycle_consecutive_failures: int
    lifecycle_last_error_kind: str | None
    lifecycle_last_attempt_at_s: float | None
    lifecycle_last_success_at_s: float | None
    lifecycle_failure_started_at_s: float | None

    def to_dict(self) -> dict[str, object]:
        return {field: getattr(self, field) for field in self.__dataclass_fields__}


class OpportunityReadHealth:
    """Process-local request diagnostics consumed by payloads and strict health."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._source_truth_status = "never-attempted"
        self._source_truth_consecutive_failures = 0
        self._source_truth_last_error_kind: str | None = None
        self._source_truth_last_attempt_at_s: float | None = None
        self._source_truth_last_live_success_at_s: float | None = None
        self._source_truth_last_fallback_at_s: float | None = None
        self._source_truth_failure_started_at_s: float | None = None
        self._lifecycle_status = "never-attempted"
        self._lifecycle_consecutive_failures = 0
        self._lifecycle_last_error_kind: str | None = None
        self._lifecycle_last_attempt_at_s: float | None = None
        self._lifecycle_last_success_at_s: float | None = None
        self._lifecycle_failure_started_at_s: float | None = None

    def mark_source_live(self, now_s: float) -> None:
        with self._lock:
            self._source_truth_status = "live"
            self._source_truth_consecutive_failures = 0
            self._source_truth_last_error_kind = None
            self._source_truth_last_attempt_at_s = now_s
            self._source_truth_last_live_success_at_s = now_s
            self._source_truth_failure_started_at_s = None

    def mark_source_fallback(self, now_s: float, error_kind: str) -> None:
        with self._lock:
            self._source_truth_status = "last-known-authenticated"
            self._source_truth_consecutive_failures += 1
            self._source_truth_last_error_kind = error_kind
            self._source_truth_last_attempt_at_s = now_s
            self._source_truth_last_fallback_at_s = now_s
            if self._source_truth_failure_started_at_s is None:
                self._source_truth_failure_started_at_s = now_s

    def mark_source_unavailable(self, now_s: float, error_kind: str) -> None:
        with self._lock:
            self._source_truth_status = "unavailable"
            self._source_truth_consecutive_failures += 1
            self._source_truth_last_error_kind = error_kind
            self._source_truth_last_attempt_at_s = now_s
            if self._source_truth_failure_started_at_s is None:
                self._source_truth_failure_started_at_s = now_s

    def mark_lifecycle(self, now_s: float, status: str, error_kind: str | None) -> None:
        with self._lock:
            self._lifecycle_status = status
            self._lifecycle_last_attempt_at_s = now_s
            if error_kind is None:
                self._lifecycle_consecutive_failures = 0
                self._lifecycle_last_error_kind = None
                self._lifecycle_last_success_at_s = now_s
                self._lifecycle_failure_started_at_s = None
            else:
                self._lifecycle_consecutive_failures += 1
                self._lifecycle_last_error_kind = error_kind
                if self._lifecycle_failure_started_at_s is None:
                    self._lifecycle_failure_started_at_s = now_s

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return OpportunityReadHealthSnapshot(
                source_truth_status=self._source_truth_status,
                source_truth_consecutive_failures=(self._source_truth_consecutive_failures),
                source_truth_last_error_kind=self._source_truth_last_error_kind,
                source_truth_last_attempt_at_s=self._source_truth_last_attempt_at_s,
                source_truth_last_live_success_at_s=(self._source_truth_last_live_success_at_s),
                source_truth_last_fallback_at_s=self._source_truth_last_fallback_at_s,
                source_truth_failure_started_at_s=(self._source_truth_failure_started_at_s),
                lifecycle_status=self._lifecycle_status,
                lifecycle_consecutive_failures=self._lifecycle_consecutive_failures,
                lifecycle_last_error_kind=self._lifecycle_last_error_kind,
                lifecycle_last_attempt_at_s=self._lifecycle_last_attempt_at_s,
                lifecycle_last_success_at_s=self._lifecycle_last_success_at_s,
                lifecycle_failure_started_at_s=self._lifecycle_failure_started_at_s,
            ).to_dict()
