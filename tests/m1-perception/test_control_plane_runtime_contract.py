"""Contracts for the shared M1 attempt runtime reporter."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from polyarb.control_plane.models import JobLease
from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.runtime_contract import (
    RUNTIME_STAGE_REGISTRY,
    AsyncAttemptRuntime,
    AttemptDeadlineExceeded,
    AttemptRuntime,
    ProgressDeadlineExceeded,
)
from polyarb.control_plane.runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeProgress,
)

NOW = datetime(2026, 8, 24, 12, 0, tzinfo=UTC)
PROFILE = RuntimeDeadlineProfile(
    policy_version="runtime-v1",
    lease_seconds=120,
    heartbeat_seconds=30,
    progress_seconds=60,
    attempt_seconds=120,
)
LEASE = JobLease(
    job_key="quote:test:batch:1",
    job_type="quote-admit",
    input_identity="input",
    lease_owner="worker-a",
    lease_epoch=1,
    lease_expires_at=NOW + timedelta(seconds=120),
    checkpoint_cursor=None,
    checkpoint_digest=None,
)


class VirtualClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value
        self.sleeper: VirtualSleeper | None = None

    def __call__(self) -> datetime:
        return self.value

    def advance(self, *, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        if self.sleeper is not None:
            self.sleeper.wake()

    async def advance_async(self, *, seconds: float) -> None:
        self.advance(seconds=seconds)
        await asyncio.sleep(0)


class VirtualSleeper:
    def __init__(self) -> None:
        self._wakeups: asyncio.Queue[None] = asyncio.Queue()
        self.started = asyncio.Event()

    def wake(self) -> None:
        self._wakeups.put_nowait(None)

    async def __call__(self, seconds: float) -> None:  # noqa: ARG002
        self.started.set()
        await self._wakeups.get()


class FakeStore:
    def __init__(self, *, fail_heartbeats: BaseException | None = None) -> None:
        self.progresses: list[tuple[JobLease, RuntimeProgress, datetime]] = []
        self.heartbeats: list[tuple[JobLease, datetime, int]] = []
        self.fail_heartbeats = fail_heartbeats

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        detail: dict[str, object] | None = None,  # noqa: ARG002
    ) -> None:
        self.progresses.append((lease, progress, now))

    def heartbeat_runtime_attempt(
        self,
        lease: JobLease,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> JobLease:
        self.heartbeats.append((lease, now, lease_seconds))
        if self.fail_heartbeats is not None:
            raise self.fail_heartbeats
        return replace(
            lease,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
        )


class BlockingHeartbeatStore(FakeStore):
    def __init__(self) -> None:
        super().__init__()
        self.started = threading.Event()
        self.released = threading.Event()
        self.completed = threading.Event()
        self.renewed_expires_at: datetime | None = None

    def heartbeat_runtime_attempt(
        self,
        lease: JobLease,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> JobLease:
        self.heartbeats.append((lease, now, lease_seconds))
        self.started.set()
        if not self.released.wait(timeout=2):
            raise AssertionError("blocking heartbeat was not released")
        self.completed.set()
        return replace(
            lease,
            lease_expires_at=(
                self.renewed_expires_at
                if self.renewed_expires_at is not None
                else now + timedelta(seconds=lease_seconds)
            ),
        )


async def _yield_until(predicate, *, cycles: int = 40) -> None:
    for _ in range(cycles):
        if predicate():
            return
        await asyncio.sleep(0)
    raise AssertionError("bounded virtual-time condition was not reached")


def test_stage_registry_is_closed_to_plan02_names() -> None:
    assert set(RUNTIME_STAGE_REGISTRY) == {
        "structure-fetch",
        "structure-materialize",
        "structure-normalize",
        "structure-certify",
        "quote-admit",
        "quote-batch",
        "quote-certify",
        "opportunity-certify",
    }
    assert RUNTIME_STAGE_REGISTRY["structure-fetch"] == (
        "fetch-page",
        "validate-page",
        "upload-page",
        "commit-page",
    )
    assert RUNTIME_STAGE_REGISTRY["quote-admit"] == (
        "read-manifest",
        "read-shards",
        "build-batches",
        "upload-batches",
        "commit-admission",
    )


def test_progress_is_monotonic_and_heartbeat_does_not_advance_progress() -> None:
    clock = VirtualClock()
    store = FakeStore()
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)

    runtime.progress(stage="read-shards", current=1, total=4)
    clock.advance(seconds=30)
    runtime.heartbeat_if_due()

    assert [item[1].sequence for item in store.progresses] == [1]
    assert len(store.heartbeats) == 1
    assert runtime.lease.lease_expires_at == NOW + timedelta(seconds=150)


def test_progress_rejects_unknown_stage_before_persistence() -> None:
    clock = VirtualClock()
    store = FakeStore()
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)

    with pytest.raises(ValueError, match="not registered"):
        runtime.progress(stage="unbounded-work", current=1, total=1)

    assert store.progresses == []


def test_sync_runtime_enforces_one_absolute_attempt_budget_across_calls() -> None:
    clock = VirtualClock()
    store = FakeStore()
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)

    assert runtime.remaining_attempt_seconds() == 120
    clock.advance(seconds=119)
    assert runtime.remaining_attempt_seconds() == 1
    clock.advance(seconds=1)
    with pytest.raises(AttemptDeadlineExceeded, match="attempt deadline"):
        runtime.progress(stage="read-shards", current=1, total=1)

    assert store.progresses == []


def test_progress_rejects_non_exact_runtime_values() -> None:
    clock = VirtualClock()
    runtime = AttemptRuntime(store=FakeStore(), lease=LEASE, profile=PROFILE, clock=clock)

    with pytest.raises(TypeError):
        runtime.progress(stage="read-shards", current=True, total=1)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        runtime.progress(stage="read-shards", current=1, total=False)  # type: ignore[arg-type]


def test_constructor_requires_timezone_aware_clock_and_exact_contract_types() -> None:
    lease = replace(LEASE)
    with pytest.raises(ValueError, match="timezone-aware"):
        AttemptRuntime(
            store=FakeStore(),
            lease=lease,
            profile=PROFILE,
            clock=lambda: datetime(2026, 8, 24, 12, 0),
        )

    with pytest.raises(TypeError):
        AttemptRuntime(  # type: ignore[arg-type]
            store=FakeStore(),
            lease=LEASE,
            profile=cast(Any, object()),
            clock=lambda: NOW,
        )


def test_constructor_rejects_unknown_job_type_and_cross_job_stage() -> None:
    clock = VirtualClock()
    unknown_job_lease = replace(LEASE, job_type="unknown-job")
    with pytest.raises(ValueError, match="registered runtime job type"):
        AttemptRuntime(
            store=FakeStore(),
            lease=unknown_job_lease,
            profile=PROFILE,
            clock=clock,
        )

    store = FakeStore()
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)
    with pytest.raises(ValueError, match="not registered for quote-admit"):
        runtime.progress(stage="fetch-page", current=1, total=1)
    assert store.progresses == []


def test_sync_heartbeat_rejects_wall_clock_regression_before_store_call() -> None:
    clock = VirtualClock()
    store = FakeStore()
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)

    clock.advance(seconds=30)
    runtime.heartbeat_if_due()
    clock.value -= timedelta(seconds=1)
    with pytest.raises(ValueError, match="regressed"):
        runtime.heartbeat_if_due()

    assert len(store.heartbeats) == 1


@pytest.mark.asyncio
async def test_async_runtime_heartbeats_with_bounded_virtual_time_and_exposes_lease() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    store = FakeStore()
    profile = replace(PROFILE, heartbeat_seconds=1)
    runtime = AsyncAttemptRuntime(
        store=store,
        lease=LEASE,
        profile=profile,
        clock=clock,
        sleep=sleeper,
    )

    async with runtime:
        await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
        await clock.advance_async(seconds=1)
        await _yield_until(lambda: len(store.heartbeats) >= 1)
        await clock.advance_async(seconds=1)
        await _yield_until(lambda: len(store.heartbeats) >= 2)

    assert len(store.heartbeats) == 2
    assert runtime.lease.lease_expires_at > LEASE.lease_expires_at
    assert runtime.heartbeat_task is not None
    assert runtime.heartbeat_task.done()


@pytest.mark.asyncio
async def test_heartbeat_failure_cancels_body_and_surfaces_original_error() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    failure = StaleLeaseError("lease fenced")
    runtime = AsyncAttemptRuntime(
        store=FakeStore(fail_heartbeats=failure),
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )

    with pytest.raises(StaleLeaseError) as raised:
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            sleeper.wake()
            await _yield_until(lambda: runtime.heartbeat_error is failure)

    assert raised.value is failure
    assert runtime.heartbeat_task is not None
    assert runtime.heartbeat_task.done()


@pytest.mark.asyncio
async def test_progress_watchdog_cancels_owner_with_typed_error() -> None:
    profile = RuntimeDeadlineProfile(
        policy_version="test",
        lease_seconds=3,
        heartbeat_seconds=1,
        progress_seconds=1,
        attempt_seconds=3,
    )
    runtime = AsyncAttemptRuntime(
        store=FakeStore(),
        lease=replace(LEASE, lease_expires_at=NOW + timedelta(seconds=3)),
        profile=profile,
        clock=lambda: NOW,
    )

    with pytest.raises(ProgressDeadlineExceeded, match="progress deadline"):
        async with runtime:
            await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_attempt_watchdog_wins_while_progress_remains_live() -> None:
    profile = RuntimeDeadlineProfile(
        policy_version="test",
        lease_seconds=3,
        heartbeat_seconds=1,
        progress_seconds=1,
        attempt_seconds=2,
    )
    runtime = AsyncAttemptRuntime(
        store=FakeStore(),
        lease=replace(LEASE, lease_expires_at=NOW + timedelta(seconds=3)),
        profile=profile,
        clock=lambda: NOW,
    )

    with pytest.raises(AttemptDeadlineExceeded, match="attempt deadline"):
        async with runtime:
            while True:
                runtime.progress(stage="read-shards", current=1, total=2)
                await asyncio.sleep(0.2)


@pytest.mark.asyncio
async def test_stop_drains_inflight_thread_call_before_context_exit() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    store = BlockingHeartbeatStore()
    runtime = AsyncAttemptRuntime(
        store=store,
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )

    async def owner() -> None:
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            sleeper.wake()
            assert await asyncio.to_thread(store.started.wait, 0.5)
            await runtime.stop()

    owner_task = asyncio.create_task(owner())
    try:
        assert await asyncio.to_thread(store.started.wait, 0.5)
        await asyncio.sleep(0)
        assert not owner_task.done()
        assert not store.completed.is_set()

        store.released.set()
        await asyncio.wait_for(owner_task, timeout=0.5)
        assert store.completed.is_set()
        assert runtime.heartbeat_task is not None
        assert runtime.heartbeat_task.done()

        sleeper.wake()
        await asyncio.sleep(0)
        assert len(store.heartbeats) == 1
    finally:
        store.released.set()
        if not owner_task.done():
            owner_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner_task


@pytest.mark.asyncio
async def test_stop_race_retains_completed_renewal_for_terminal_commit() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    store = BlockingHeartbeatStore()
    store.renewed_expires_at = NOW + timedelta(seconds=3)
    race_lease = replace(LEASE, lease_expires_at=NOW + timedelta(seconds=1))
    race_profile = replace(
        PROFILE,
        lease_seconds=3,
        heartbeat_seconds=1,
        progress_seconds=2,
        attempt_seconds=3,
    )
    runtime = AsyncAttemptRuntime(
        store=store,
        lease=race_lease,
        profile=race_profile,
        clock=clock,
        sleep=sleeper,
    )
    terminal_expires_at: datetime | None = None
    stop_entered = asyncio.Event()

    async def owner() -> None:
        nonlocal terminal_expires_at
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            sleeper.wake()
            assert await asyncio.to_thread(store.started.wait, 0.5)
            stop_entered.set()
            await runtime.stop()
            terminal_expires_at = runtime.lease.lease_expires_at

    owner_task = asyncio.create_task(owner())
    try:
        assert await asyncio.to_thread(store.started.wait, 0.5)
        await asyncio.wait_for(stop_entered.wait(), timeout=0.5)
        assert not owner_task.done()
        store.released.set()
        await asyncio.wait_for(owner_task, timeout=0.5)

        assert terminal_expires_at is not None
        assert terminal_expires_at == NOW + timedelta(seconds=3)
        assert terminal_expires_at > clock()
        assert len(store.heartbeats) == 1
        sleeper.wake()
        await asyncio.sleep(0)
        assert len(store.heartbeats) == 1
    finally:
        store.released.set()
        if not owner_task.done():
            owner_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner_task


@pytest.mark.asyncio
async def test_cancellation_during_stop_drains_thread_then_propagates() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    store = BlockingHeartbeatStore()
    runtime = AsyncAttemptRuntime(
        store=store,
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )
    terminal = asyncio.Event()
    stop_entered = asyncio.Event()

    async def owner() -> None:
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            sleeper.wake()
            assert await asyncio.to_thread(store.started.wait, 0.5)
            stop_entered.set()
            await runtime.stop()
            terminal.set()

    owner_task = asyncio.create_task(owner())
    try:
        assert await asyncio.to_thread(store.started.wait, 0.5)
        await asyncio.wait_for(stop_entered.wait(), timeout=0.5)
        owner_task.cancel()
        await asyncio.sleep(0)
        assert not store.completed.is_set()
        assert not terminal.is_set()

        store.released.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(owner_task, timeout=0.5)
        assert store.completed.is_set()
        assert not terminal.is_set()
        assert runtime.heartbeat_task is not None
        assert runtime.heartbeat_task.done()
    finally:
        store.released.set()
        if not owner_task.done():
            owner_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await owner_task


@pytest.mark.asyncio
async def test_async_heartbeat_rejects_wall_clock_regression_before_store_call() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    store = FakeStore()
    runtime = AsyncAttemptRuntime(
        store=store,
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )

    with pytest.raises(ValueError, match="regressed"):
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            clock.advance(seconds=1)
            await _yield_until(lambda: len(store.heartbeats) >= 1)
            clock.value -= timedelta(seconds=1)
            sleeper.wake()
            await _yield_until(lambda: runtime.heartbeat_error is not None)

    assert len(store.heartbeats) == 1


@pytest.mark.asyncio
async def test_stop_reaps_heartbeat_task_on_body_error_and_is_idempotent() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    runtime = AsyncAttemptRuntime(
        store=FakeStore(),
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )

    with pytest.raises(RuntimeError, match="body failed"):
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            raise RuntimeError("body failed")

    await runtime.stop()
    assert runtime.heartbeat_task is not None
    assert runtime.heartbeat_task.done()


@pytest.mark.asyncio
async def test_external_cancellation_reaps_heartbeat_task() -> None:
    clock = VirtualClock()
    sleeper = VirtualSleeper()
    clock.sleeper = sleeper
    runtime = AsyncAttemptRuntime(
        store=FakeStore(),
        lease=LEASE,
        profile=replace(PROFILE, heartbeat_seconds=1),
        clock=clock,
        sleep=sleeper,
    )

    async def body() -> None:
        async with runtime:
            await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
            await asyncio.Event().wait()

    task = asyncio.create_task(body())
    await asyncio.wait_for(sleeper.started.wait(), timeout=0.2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert runtime.heartbeat_task is not None
    assert runtime.heartbeat_task.done()
