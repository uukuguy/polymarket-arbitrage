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
from .runtime_models import RuntimeDeadlineProfile, RuntimeProgress


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
        if detail is not None and type(detail) is not dict:
            raise TypeError("runtime detail must be a dict or None")
        sequence = self._sequence + 1
        progress = RuntimeProgress(
            sequence=sequence,
            current=current,
            total=total,
            stage=stage,
        )
        kwargs: dict[str, Any] = {
            "progress": progress,
            "now": _read_clock(self._clock),
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
        renewed = self._store.heartbeat_runtime_attempt(
            self._lease,
            now=now,
            lease_seconds=self._profile.lease_seconds,
        )
        if type(renewed) is not JobLease:
            raise TypeError("heartbeat store must return JobLease")
        self._lease = renewed
        self._last_heartbeat_at = now


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
        self._heartbeat_call_task: asyncio.Task[Any] | None = None
        self._owner_task: asyncio.Task[Any] | None = None
        self._heartbeat_error: BaseException | None = None
        self._stop_complete = False

    @property
    def heartbeat_task(self) -> asyncio.Task[None] | None:
        """Expose the owned task for bounded lifecycle assertions."""
        return self._heartbeat_task

    @property
    def heartbeat_error(self) -> BaseException | None:
        return self._heartbeat_error

    async def start(self) -> AsyncAttemptRuntime:
        """Start exactly one heartbeat task owned by the current task."""
        if self._heartbeat_task is not None or self._stop_complete:
            raise RuntimeError("attempt runtime can only be started once")
        owner = asyncio.current_task()
        if owner is None:
            raise RuntimeError("attempt runtime requires an owning task")
        self._owner_task = owner
        self._stopped.clear()
        try:
            self._heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(),
                name=f"runtime-heartbeat:{self.lease.job_key}",
            )
        except BaseException:
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
        await self._drain_heartbeat_task()
        self._stop_complete = True
        if self._heartbeat_error is not None:
            raise self._heartbeat_error

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
                if self._stopped.is_set():
                    break
                if type(renewed) is not JobLease:
                    raise TypeError("heartbeat store must return JobLease")
                self._lease = renewed
                self._last_heartbeat_at = now
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
            await self._drain_task(call_task)
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
            done, _ = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
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

    async def _drain_heartbeat_task(self) -> None:
        task = self._heartbeat_task
        if task is None or task is asyncio.current_task():
            return
        error = await self._drain_task(task)
        if error is not None and self._heartbeat_error is None:
            self._heartbeat_error = error

    async def _drain_task(self, task: asyncio.Task[Any]) -> BaseException | None:
        """Await a task to completion without abandoning executor work."""
        while not task.done():
            try:
                await asyncio.shield(task)
            except asyncio.CancelledError:
                continue
        try:
            task.result()
        except asyncio.CancelledError:
            return None
        except BaseException as error:
            return error
        return None


__all__ = [
    "ALL_RUNTIME_STAGES",
    "AsyncAttemptRuntime",
    "AttemptRuntime",
    "RUNTIME_STAGE_REGISTRY",
    "RuntimeStore",
]
