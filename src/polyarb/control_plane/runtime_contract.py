"""Shared synchronous/asynchronous runtime evidence reporters.

The reporter is deliberately small: persistence, fencing, and transaction
boundaries remain owned by :mod:`polyarb.control_plane.postgres`.  This module
only owns the in-process attempt lifecycle, monotonic progress sequence, and
the cancellation semantics needed to stop a worker immediately after a
heartbeat loses its fence.
"""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime
from types import MappingProxyType
from typing import Any, Protocol

from .models import JobLease
from .runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeProgress,
    validate_runtime_detail_bounds,
)


class RuntimeStore(Protocol):
    """The synchronous store surface needed by an attempt reporter."""

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        detail: dict[str, object] | None = None,
    ) -> object: ...

    def heartbeat_runtime_attempt(
        self,
        lease: JobLease,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> JobLease: ...


Clock = Callable[[], datetime]
Sleeper = Callable[[float], Awaitable[None]]


class AttemptDeadlineExceeded(TimeoutError):
    """The single authoritative absolute attempt deadline elapsed."""


class ProgressDeadlineExceeded(TimeoutError):
    """The attempt produced no durable progress within its policy window."""


class ServiceStopRequested(RuntimeError):
    """The service requested a cooperative stop at the next safe boundary."""


async def _await_sleep(awaitable: Awaitable[None]) -> None:
    await awaitable


# This is intentionally a closed registry.  A worker may not invent a stage
# at runtime: adding a new stage requires an explicit contract and coverage
# test in this plan.  The outer mapping is immutable, as are each stage tuple.
RUNTIME_STAGE_REGISTRY: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "structure-fetch": (
            "fetch-page",
            "validate-page",
            "upload-page",
            "commit-page",
        ),
        "structure-materialize": (
            "read-page-receipts",
            "build-bundle",
            "upload-bundle",
            "commit-bundle",
        ),
        "structure-normalize": (
            "read-range",
            "normalize-range",
            "upload-range",
            "commit-range",
        ),
        "structure-certify": (
            "verify-parity",
            "build-manifest",
            "upload-manifest",
            "commit-certification",
        ),
        "quote-admit": (
            "read-manifest",
            "read-shards",
            "build-batches",
            "upload-batches",
            "commit-admission",
        ),
        "quote-batch": (
            "read-input",
            "fetch-books",
            "build-artifact",
            "upload-artifact",
            "commit-receipt",
        ),
        "quote-certify": (
            "verify-batches",
            "publish-pointer",
        ),
        "opportunity-certify": (
            "read-current-quote",
            "compute-opportunities",
            "upload-projection",
            "publish-opportunity",
        ),
    }
)
ALL_RUNTIME_STAGES = frozenset(
    stage for stages in RUNTIME_STAGE_REGISTRY.values() for stage in stages
)
_SECRET_LIKE_DETAIL_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
)


def _read_clock(clock: Clock) -> datetime:
    """Read and validate a timezone-aware wall-clock value."""
    value = clock()
    if type(value) is not datetime:
        raise TypeError("runtime clock must return datetime")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("runtime clock must return a timezone-aware datetime")
    return value


def _validate_inputs(
    *,
    store: RuntimeStore,
    lease: JobLease,
    profile: RuntimeDeadlineProfile,
    clock: Clock,
) -> None:
    if not callable(getattr(store, "record_runtime_progress", None)):
        raise TypeError("runtime store must record progress")
    if not callable(getattr(store, "heartbeat_runtime_attempt", None)):
        raise TypeError("runtime store must heartbeat attempts")
    if type(lease) is not JobLease:
        raise TypeError("lease must be JobLease")
    if type(lease.job_type) is not str:
        raise TypeError("lease job_type must be an exact str")
    if lease.job_type not in RUNTIME_STAGE_REGISTRY:
        raise ValueError(f"unregistered runtime job type: {lease.job_type!r}")
    if type(profile) is not RuntimeDeadlineProfile:
        raise TypeError("profile must be RuntimeDeadlineProfile")
    if type(profile.policy_version) is not str:
        raise TypeError("runtime policy version must be an exact str")
    if any(
        type(getattr(profile, field)) is not int
        for field in (
            "lease_seconds",
            "heartbeat_seconds",
            "progress_seconds",
            "attempt_seconds",
        )
    ):
        raise TypeError("runtime deadline values must be exact ints")
    if not callable(clock):
        raise TypeError("runtime clock must be callable")


def _validate_progress_arguments(
    *, job_type: str, stage: str, current: int, total: int | None
) -> None:
    if type(stage) is not str:
        raise TypeError("progress stage must be a str")
    if stage not in RUNTIME_STAGE_REGISTRY[job_type]:
        raise ValueError(f"stage {stage!r} is not registered for {job_type}")
    if type(current) is not int:
        raise TypeError("progress current must be an int")
    if total is not None and type(total) is not int:
        raise TypeError("progress total must be an int or None")


def _validate_runtime_detail(detail: dict[str, object] | None) -> None:
    if detail is None:
        return
    validate_runtime_detail_bounds(detail)
    for key in detail:
        normalized = key.casefold().replace("-", "_")
        if any(part in normalized for part in _SECRET_LIKE_DETAIL_KEY_PARTS):
            raise ValueError(f"secret-like runtime detail key is forbidden: {key!r}")


def _validate_clock_progression(now: datetime, previous: datetime) -> None:
    if now < previous:
        raise ValueError("runtime clock regressed during heartbeat")


class AttemptRuntime:
    """Own one fenced lease's progress sequence and due heartbeats."""

    def __init__(
        self,
        *,
        store: RuntimeStore,
        lease: JobLease,
        profile: RuntimeDeadlineProfile,
        clock: Clock,
    ) -> None:
        _validate_inputs(store=store, lease=lease, profile=profile, clock=clock)
        self._store = store
        self._lease = lease
        self._profile = profile
        self._clock = clock
        self._sequence = 0
        self._last_heartbeat_at = _read_clock(clock)
        self._started_at = self._last_heartbeat_at

    @property
    def lease(self) -> JobLease:
        """Return the current fenced lease, including the latest renewal."""
        return self._lease

    @property
    def current_lease(self) -> JobLease:
        """Explicit alias for callers preparing a terminal fenced commit."""
        return self._lease

    @property
    def progress_sequence(self) -> int:
        return self._sequence

    @property
    def last_heartbeat_at(self) -> datetime:
        return self._last_heartbeat_at

    @property
    def profile(self) -> RuntimeDeadlineProfile:
        """Return the immutable deadline contract used by this attempt."""
        return self._profile

    def remaining_attempt_seconds(self) -> float:
        """Return the remaining absolute attempt budget or fail authoritatively."""
        now = _read_clock(self._clock)
        _validate_clock_progression(now, self._started_at)
        remaining = self._profile.attempt_seconds - (now - self._started_at).total_seconds()
        if remaining <= 0:
            raise AttemptDeadlineExceeded(f"attempt deadline exceeded for {self._lease.job_key}")
        return remaining

    def progress(
        self,
        *,
        stage: str,
        current: int,
        total: int | None,
        detail: dict[str, object] | None = None,
    ) -> None:
        """Persist one strictly increasing progress observation.

        The sequence is advanced only after persistence succeeds.  A failed
        write therefore remains retryable with the same sequence instead of
        creating a local sequence gap that the durable monotonic guard would
        reject later.
        """
        _validate_progress_arguments(
            job_type=self._lease.job_type,
            stage=stage,
            current=current,
            total=total,
        )
        _validate_runtime_detail(detail)
        sequence = self._sequence + 1
        progress = RuntimeProgress(
            sequence=sequence,
            current=current,
            total=total,
            stage=stage,
        )
        now = _read_clock(self._clock)
        self._require_attempt_live(now)
        kwargs: dict[str, Any] = {
            "progress": progress,
            "now": now,
        }
        if detail is not None:
            # Do not let a store retain a caller-owned mutable detail object.
            kwargs["detail"] = dict(detail)
        self._store.record_runtime_progress(self._lease, **kwargs)
        self._sequence = sequence

    def heartbeat_if_due(self) -> None:
        """Renew the lease only after the configured heartbeat interval."""
        now = _read_clock(self._clock)
        _validate_clock_progression(now, self._last_heartbeat_at)
        elapsed_seconds = (now - self._last_heartbeat_at).total_seconds()
        if elapsed_seconds < self._profile.heartbeat_seconds:
            return
        self._renew_heartbeat(now)

    def heartbeat(self) -> None:
        """Renew immediately at a worker-defined bounded checkpoint.

        Synchronous parity workers already have a monotonic chunk budget.  A
        forced renewal lets them retain that existing cadence while keeping
        the actual fenced store mutation in this shared reporter.
        """
        now = _read_clock(self._clock)
        _validate_clock_progression(now, self._last_heartbeat_at)
        self._renew_heartbeat(now)

    def _renew_heartbeat(self, now: datetime) -> None:
        self._require_attempt_live(now)
        renewed = self._store.heartbeat_runtime_attempt(
            self._lease,
            now=now,
            lease_seconds=self._profile.lease_seconds,
        )
        if type(renewed) is not JobLease:
            raise TypeError("heartbeat store must return JobLease")
        self._lease = renewed
        self._last_heartbeat_at = now

    def _require_attempt_live(self, now: datetime) -> None:
        _validate_clock_progression(now, self._started_at)
        if (now - self._started_at).total_seconds() >= self._profile.attempt_seconds:
            raise AttemptDeadlineExceeded(f"attempt deadline exceeded for {self._lease.job_key}")


class AsyncAttemptRuntime(AttemptRuntime):
    """Attempt reporter with an owned, cancellable asynchronous heartbeat.

    The task started here is owned by the task that called :meth:`start`.
    Losing the lease cancels that owner task and is re-raised from
    ``__aexit__`` as the original exception after the heartbeat task is
    reaped.  This prevents a worker from continuing a long blocking operation
    after it no longer has write authority.
    """

    def __init__(
        self,
        *,
        store: RuntimeStore,
        lease: JobLease,
        profile: RuntimeDeadlineProfile,
        clock: Clock,
        sleep: Sleeper | None = None,
    ) -> None:
        super().__init__(store=store, lease=lease, profile=profile, clock=clock)
        if sleep is not None and not callable(sleep):
            raise TypeError("runtime sleeper must be callable")
        self._sleep = sleep
        self._stopped = asyncio.Event()
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._watchdog_task: asyncio.Task[None] | None = None
        self._heartbeat_call_task: asyncio.Task[Any] | None = None
        self._owner_task: asyncio.Task[Any] | None = None
        self._heartbeat_error: BaseException | None = None
        self._watchdog_error: BaseException | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._started_monotonic: float | None = None
        self._last_progress_monotonic: float | None = None
        self._stop_complete = False

    @property
    def heartbeat_task(self) -> asyncio.Task[None] | None:
        """Expose the owned task for bounded lifecycle assertions."""
        return self._heartbeat_task

    @property
    def heartbeat_error(self) -> BaseException | None:
        return self._heartbeat_error

    @property
    def watchdog_error(self) -> BaseException | None:
        return self._watchdog_error

    def progress(
        self,
        *,
        stage: str,
        current: int,
        total: int | None,
        detail: dict[str, object] | None = None,
    ) -> None:
        """Persist progress and reset the in-process progress watchdog."""
        super().progress(stage=stage, current=current, total=total, detail=detail)
        loop = self._loop
        if loop is not None:
            self._last_progress_monotonic = loop.time()

    async def start(self) -> AsyncAttemptRuntime:
        """Start exactly one heartbeat task owned by the current task."""
        if self._heartbeat_task is not None or self._stop_complete:
            raise RuntimeError("attempt runtime can only be started once")
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError("attempt runtime requires an owning task")
        self._owner_task = owner
        self._loop = asyncio.get_running_loop()
        self._started_monotonic = self._loop.time()
        self._last_progress_monotonic = self._started_monotonic
        self._stopped.clear()
        try:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name=f"runtime-heartbeat:{self.lease.job_key}",
            )
            self._watchdog_task = asyncio.create_task(
                self._watchdog_loop(),
                name=f"runtime-watchdog:{self.lease.job_key}",
            )
        except BaseException:
            if self._heartbeat_task is not None:
                self._heartbeat_task.cancel()
            self._owner_task = None
            raise
        return self

    async def stop(self) -> None:
        """Stop and reap the heartbeat task; repeated calls are harmless."""
        if self._heartbeat_task is None:
            raise RuntimeError("attempt runtime has not been started")
        self._require_owner()
        if self._stop_complete:
            if self._heartbeat_error is not None:
                raise self._heartbeat_error
            return
        self._stopped.set()
        heartbeat_cancelled = await self._drain_heartbeat_task()
        watchdog_cancelled = await self._drain_watchdog_task()
        self._stop_complete = True
        if self._heartbeat_error is not None:
            raise self._heartbeat_error
        if self._watchdog_error is not None:
            raise self._watchdog_error
        if heartbeat_cancelled or watchdog_cancelled:
            raise asyncio.CancelledError

    async def __aenter__(self) -> AsyncAttemptRuntime:
        return await self.start()

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        stop_error: BaseException | None = None
        try:
            await self.stop()
        except BaseException as error:  # preserve cancellation/body error below
            stop_error = error
        if self._heartbeat_error is not None:
            # A heartbeat failure is the authoritative reason for stopping,
            # even when it reached the body as CancelledError.
            raise self._heartbeat_error
        if self._watchdog_error is not None:
            raise self._watchdog_error
        if stop_error is not None:
            raise stop_error
        return False

    def _require_owner(self) -> None:
        if asyncio.current_task() is not self._owner_task:
            raise RuntimeError("attempt runtime must be stopped by its owner task")

    async def _heartbeat_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                if not await self._wait_for_heartbeat_or_stop():
                    break
                if self._stopped.is_set():
                    break
                now = _read_clock(self._clock)
                _validate_clock_progression(now, self._last_heartbeat_at)
                renewed = await self._run_heartbeat_call(now)
                if type(renewed) is not JobLease:
                    raise TypeError("heartbeat store must return JobLease")
                self._lease = renewed
                self._last_heartbeat_at = now
                if self._stopped.is_set():
                    break
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._heartbeat_error = error
            self._stopped.set()
            owner = self._owner_task
            if owner is not None and owner is not asyncio.current_task():
                owner.cancel()
            raise
        finally:
            self._stopped.set()

    async def _watchdog_loop(self) -> None:
        try:
            loop = self._loop
            started = self._started_monotonic
            if loop is None or started is None:
                raise RuntimeError("attempt watchdog started without an event loop")
            while not self._stopped.is_set():
                now = loop.time()
                last_progress = self._last_progress_monotonic
                if last_progress is None:
                    raise RuntimeError("attempt watchdog has no progress baseline")
                attempt_remaining = self._profile.attempt_seconds - (now - started)
                progress_remaining = self._profile.progress_seconds - (now - last_progress)
                if attempt_remaining <= 0:
                    raise AttemptDeadlineExceeded(
                        f"attempt deadline exceeded for {self.lease.job_key}"
                    )
                if progress_remaining <= 0:
                    raise ProgressDeadlineExceeded(
                        f"progress deadline exceeded for {self.lease.job_key}"
                    )
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=min(attempt_remaining, progress_remaining),
                    )
                except TimeoutError:
                    continue
        except asyncio.CancelledError:
            raise
        except BaseException as error:
            self._watchdog_error = error
            self._stopped.set()
            owner = self._owner_task
            if owner is not None and owner is not asyncio.current_task():
                owner.cancel()
            raise
        finally:
            self._stopped.set()

    async def _run_heartbeat_call(self, now: datetime) -> JobLease:
        call_task = asyncio.create_task(
            asyncio.to_thread(
                self._store.heartbeat_runtime_attempt,
                self._lease,
                now=now,
                lease_seconds=self._profile.lease_seconds,
            ),
            name=f"runtime-heartbeat-call:{self.lease.job_key}",
        )
        self._heartbeat_call_task = call_task
        try:
            # Shield the executor future so cancellation of the heartbeat
            # coroutine cannot abandon a DB call that may still mutate state.
            return await asyncio.shield(call_task)
        except asyncio.CancelledError:
            # A cancelled heartbeat coroutine must still wait for the worker
            # thread to finish before propagating cancellation.
            error, _ = await self._drain_task(call_task)
            if error is not None and self._heartbeat_error is None:
                self._heartbeat_error = error
            raise
        finally:
            if self._heartbeat_call_task is call_task:
                self._heartbeat_call_task = None

    async def _wait_for_heartbeat_or_stop(self) -> bool:
        if self._sleep is None:
            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._profile.heartbeat_seconds
                )
            except TimeoutError:
                return True
            return False

        sleep_result = self._sleep(float(self._profile.heartbeat_seconds))
        if not inspect.isawaitable(sleep_result):
            raise TypeError("runtime sleeper must return an awaitable")
        sleep_task = asyncio.create_task(_await_sleep(sleep_result))
        stop_task = asyncio.create_task(self._stopped.wait())
        tasks = (sleep_task, stop_task)
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            if sleep_task in done:
                # Retrieve sleeper exceptions instead of leaving an unobserved
                # task behind.
                sleep_task.result()
                return not self._stopped.is_set()
            return False
        finally:
            pending = [task for task in tasks if not task.done()]
            for task in pending:
                task.cancel()
            if pending:
                pending_awaitables: list[Awaitable[Any]] = list(pending)
                await asyncio.gather(*pending_awaitables, return_exceptions=True)
            # A stop event and sleeper may complete in the same loop turn;
            # retrieve both results to keep task accounting clean.
            for task in tasks:
                if task.done() and not task.cancelled():
                    try:
                        task.result()
                    except BaseException:
                        if task is sleep_task:
                            raise

    async def _drain_heartbeat_task(self) -> bool:
        task = self._heartbeat_task
        if task is None or task is asyncio.current_task():
            return False
        error, was_cancelled = await self._drain_task(task)
        if error is not None and self._heartbeat_error is None:
            self._heartbeat_error = error
        return was_cancelled

    async def _drain_watchdog_task(self) -> bool:
        task = self._watchdog_task
        if task is None or task is asyncio.current_task():
            return False
        error, was_cancelled = await self._drain_task(task)
        if error is not None and self._watchdog_error is None:
            self._watchdog_error = error
        return was_cancelled

    async def _drain_task(self, task: asyncio.Task[Any]) -> tuple[BaseException | None, bool]:
        """Await a task to completion without abandoning executor work."""
        current = asyncio.current_task()
        was_cancelled = current is not None and current.cancelling() > 0
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                was_cancelled = True
                continue
        try:
            task.result()
        except asyncio.CancelledError:
            return None, was_cancelled
        except BaseException as error:
            return error, was_cancelled
        return None, was_cancelled


__all__ = [
    "ALL_RUNTIME_STAGES",
    "AsyncAttemptRuntime",
    "AttemptDeadlineExceeded",
    "AttemptRuntime",
    "ProgressDeadlineExceeded",
    "RUNTIME_STAGE_REGISTRY",
    "RuntimeStore",
    "ServiceStopRequested",
]
