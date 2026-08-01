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


class ReadLaneClosedError(RuntimeError):
    """The owning application has shut this read lane down."""


class BoundedReadLane:
    """A dedicated executor whose running and queued work is strictly bounded."""

    def __init__(self, name: str, *, capacity: int = 2) -> None:
        self._executor = ThreadPoolExecutor(
            max_workers=capacity,
            thread_name_prefix=name,
        )
        self._slots = threading.BoundedSemaphore(capacity)
        self._state_lock = threading.Lock()
        self._closed = False

    async def run(
        self,
        function: Callable[..., T],
        *args: Any,
        timeout_s: float,
        **kwargs: Any,
    ) -> T:
        with self._state_lock:
            if self._closed:
                raise ReadLaneClosedError("read-lane-closed")
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

    def shutdown(self) -> None:
        """Reject new work and abandon running operations without blocking."""
        with self._state_lock:
            if self._closed:
                return
            self._closed = True
            self._executor.shutdown(wait=False, cancel_futures=True)


@dataclass(frozen=True)
class OpportunityReadHealthSnapshot:
    source_truth_status: str
    source_truth_consecutive_failures: int
    source_truth_last_error_kind: str | None
    source_truth_last_attempt_at_s: float | None
    source_truth_last_live_success_at_s: float | None
    source_truth_last_fallback_at_s: float | None
    source_truth_failure_started_at_s: float | None
    source_truth_latest_token: int
    lifecycle_status: str
    lifecycle_consecutive_failures: int
    lifecycle_last_error_kind: str | None
    lifecycle_last_attempt_at_s: float | None
    lifecycle_last_success_at_s: float | None
    lifecycle_failure_started_at_s: float | None
    lifecycle_latest_token: int

    def to_dict(self) -> dict[str, object]:
        return {field: getattr(self, field) for field in self.__dataclass_fields__}


class OpportunityReadHealth:
    """Process-local request diagnostics consumed by payloads and strict health."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._next_sequence = 0
        self._source_truth_latest_token = 0
        self._source_truth_attempt_started_at_s: float | None = None
        self._source_truth_status = "never-attempted"
        self._source_truth_consecutive_failures = 0
        self._source_truth_last_error_kind: str | None = None
        self._source_truth_last_attempt_at_s: float | None = None
        self._source_truth_last_live_success_at_s: float | None = None
        self._source_truth_last_fallback_at_s: float | None = None
        self._source_truth_failure_started_at_s: float | None = None
        self._lifecycle_latest_token = 0
        self._lifecycle_attempt_started_at_s: float | None = None
        self._lifecycle_status = "never-attempted"
        self._lifecycle_consecutive_failures = 0
        self._lifecycle_last_error_kind: str | None = None
        self._lifecycle_last_attempt_at_s: float | None = None
        self._lifecycle_last_success_at_s: float | None = None
        self._lifecycle_failure_started_at_s: float | None = None

    def _begin_attempt(self, stage: str, now_s: float) -> int:
        with self._lock:
            self._next_sequence += 1
            token = self._next_sequence
            if stage == "source":
                self._source_truth_latest_token = token
                self._source_truth_attempt_started_at_s = now_s
                self._source_truth_last_attempt_at_s = max(
                    now_s,
                    self._source_truth_last_attempt_at_s or now_s,
                )
            else:
                self._lifecycle_latest_token = token
                self._lifecycle_attempt_started_at_s = now_s
                self._lifecycle_last_attempt_at_s = max(
                    now_s,
                    self._lifecycle_last_attempt_at_s or now_s,
                )
            return token

    def begin_source_attempt(self, now_s: float) -> int:
        return self._begin_attempt("source", now_s)

    def begin_lifecycle_attempt(self, now_s: float) -> int:
        return self._begin_attempt("lifecycle", now_s)

    def mark_source_live(self, token: int, now_s: float) -> bool:
        with self._lock:
            if token != self._source_truth_latest_token:
                return False
            self._source_truth_status = "live"
            self._source_truth_consecutive_failures = 0
            self._source_truth_last_error_kind = None
            self._source_truth_last_live_success_at_s = max(
                now_s,
                self._source_truth_last_live_success_at_s or now_s,
            )
            self._source_truth_failure_started_at_s = None
            return True

    def mark_source_fallback(
        self,
        token: int,
        now_s: float,
        error_kind: str,
    ) -> bool:
        with self._lock:
            if token != self._source_truth_latest_token:
                return False
            self._source_truth_status = "last-known-authenticated"
            self._source_truth_consecutive_failures += 1
            self._source_truth_last_error_kind = error_kind
            self._source_truth_last_fallback_at_s = max(
                now_s,
                self._source_truth_last_fallback_at_s or now_s,
            )
            if self._source_truth_failure_started_at_s is None:
                self._source_truth_failure_started_at_s = (
                    self._source_truth_attempt_started_at_s
                )
            return True

    def mark_source_unavailable(
        self,
        token: int,
        now_s: float,
        error_kind: str,
        *,
        authentication_invalid: bool = False,
    ) -> bool:
        with self._lock:
            if token != self._source_truth_latest_token:
                return False
            self._source_truth_status = (
                "authentication-invalid"
                if authentication_invalid
                else "unavailable"
            )
            self._source_truth_consecutive_failures += 1
            self._source_truth_last_error_kind = error_kind
            if self._source_truth_failure_started_at_s is None:
                self._source_truth_failure_started_at_s = (
                    self._source_truth_attempt_started_at_s
                )
            return True

    def mark_lifecycle(
        self,
        token: int,
        now_s: float,
        status: str,
        error_kind: str | None,
    ) -> bool:
        with self._lock:
            if token != self._lifecycle_latest_token:
                return False
            self._lifecycle_status = status
            if error_kind is None:
                self._lifecycle_consecutive_failures = 0
                self._lifecycle_last_error_kind = None
                self._lifecycle_last_success_at_s = max(
                    now_s,
                    self._lifecycle_last_success_at_s or now_s,
                )
                self._lifecycle_failure_started_at_s = None
            else:
                self._lifecycle_consecutive_failures += 1
                self._lifecycle_last_error_kind = error_kind
                if self._lifecycle_failure_started_at_s is None:
                    self._lifecycle_failure_started_at_s = (
                        self._lifecycle_attempt_started_at_s
                    )
            return True

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
                source_truth_latest_token=self._source_truth_latest_token,
                lifecycle_status=self._lifecycle_status,
                lifecycle_consecutive_failures=self._lifecycle_consecutive_failures,
                lifecycle_last_error_kind=self._lifecycle_last_error_kind,
                lifecycle_last_attempt_at_s=self._lifecycle_last_attempt_at_s,
                lifecycle_last_success_at_s=self._lifecycle_last_success_at_s,
                lifecycle_failure_started_at_s=self._lifecycle_failure_started_at_s,
                lifecycle_latest_token=self._lifecycle_latest_token,
            ).to_dict()
