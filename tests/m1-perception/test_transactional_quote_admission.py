from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import pytest

from polyarb.control_plane.models import JobLease, JobState
from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.quote_admission import (
    QuoteAdmissionError,
    QuoteAdmissionShardUnavailable,
    TransactionalQuoteAdmitter,
)
from polyarb.control_plane.quote_worker import QuoteBatchWorkerResult
from polyarb.control_plane.runtime_models import RuntimeProgress
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    StructureShardArtifact,
    StructureShardReceipt,
    canonical_structure_bundle_bytes,
    canonical_structure_shard_bytes,
    canonical_structure_shard_manifest_bytes,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class _VirtualClock:
    def __init__(self, value: datetime = NOW) -> None:
        self.value = value
        self._lock = threading.Lock()
        self._wake_callbacks: set[Callable[[], None]] = set()

    def __call__(self) -> datetime:
        with self._lock:
            return self.value

    def advance(self, seconds: float) -> None:
        with self._lock:
            self.value += timedelta(seconds=seconds)
            callbacks = tuple(self._wake_callbacks)
        for callback in callbacks:
            callback()

    def register(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._wake_callbacks.add(callback)

    def unregister(self, callback: Callable[[], None]) -> None:
        with self._lock:
            self._wake_callbacks.discard(callback)


class _VirtualSleeper:
    def __init__(self, clock: _VirtualClock) -> None:
        self._clock = clock

    async def __call__(self, seconds: float) -> None:
        loop = asyncio.get_running_loop()
        wake = asyncio.Event()

        def callback() -> None:
            loop.call_soon_threadsafe(wake.set)

        self._clock.register(callback)
        try:
            target = self._clock() + timedelta(seconds=seconds)
            while self._clock() < target:
                await wake.wait()
                wake.clear()
        finally:
            self._clock.unregister(callback)


class _SlowBody:
    def __init__(self, payload: bytes, clock: _VirtualClock) -> None:
        self._payload = payload
        self._clock = clock
        self.finished = threading.Event()

    def read(self) -> bytes:
        # Seven short slices total 207 simulated seconds.  Each real sleep
        # gives the event loop a chance to run the fenced heartbeat task.
        try:
            for seconds in (31, 31, 31, 31, 31, 31, 21):
                self._clock.advance(seconds)
                time.sleep(0.005)
            return self._payload
        finally:
            self.finished.set()


class _BlockingBody:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.started = threading.Event()
        self.release = threading.Event()
        self.finished = threading.Event()

    def read(self) -> bytes:
        self.started.set()
        try:
            if not self.release.wait(timeout=2):
                raise RuntimeError("blocking body was not released")
            return self._payload
        finally:
            self.finished.set()


class _Objects:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload
        self.puts: list[dict[str, object]] = []

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "artifacts", "Key": "bundles/current.ndjson"}
        return {"Body": _Body(self._payload)}

    def put_object(self, **kwargs: object) -> None:
        self.puts.append(kwargs)

    def head_object(self, **kwargs: object) -> dict[str, object]:
        matching = next(item for item in self.puts if item["Key"] == kwargs["Key"])
        body = matching["Body"]
        metadata = matching["Metadata"]
        assert isinstance(body, bytes)
        assert isinstance(metadata, dict)
        return {
            "ContentLength": len(body),
            "Metadata": {"sha256": metadata["sha256"]},
        }


class _SlowObjects(_Objects):
    def __init__(self, payload: bytes, clock: _VirtualClock) -> None:
        super().__init__(payload)
        self._clock = clock
        self.last_body: _SlowBody | None = None

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "artifacts", "Key": "bundles/current.ndjson"}
        self.last_body = _SlowBody(self._payload, self._clock)
        return {"Body": self.last_body}


class _BlockingReadObjects(_Objects):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.get_started = threading.Event()
        self.last_body: _BlockingBody | None = None

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "artifacts", "Key": "bundles/current.ndjson"}
        self.last_body = _BlockingBody(self._payload)
        self.get_started.set()
        return {"Body": self.last_body}


class _BlockingUploadObjects(_Objects):
    def __init__(self, payload: bytes) -> None:
        super().__init__(payload)
        self.put_started = threading.Event()
        self.release_put = threading.Event()
        self.put_finished = threading.Event()

    def put_object(self, **kwargs: object) -> None:
        self.put_started.set()
        try:
            if not self.release_put.wait(timeout=2):
                raise RuntimeError("blocking upload was not released")
            super().put_object(**kwargs)
        finally:
            self.put_finished.set()


class _ObjectMap:
    def __init__(self, payloads: dict[str, bytes]) -> None:
        self._payloads = payloads
        self.puts: list[dict[str, object]] = []

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["Bucket"] == "artifacts"
        return {"Body": _Body(self._payloads[str(kwargs["Key"])])}

    def put_object(self, **kwargs: object) -> None:
        self.puts.append(kwargs)
        body = kwargs["Body"]
        assert isinstance(body, bytes)
        self._payloads[str(kwargs["Key"])] = body

    def head_object(self, **kwargs: object) -> dict[str, object]:
        matching = next(item for item in self.puts if item["Key"] == kwargs["Key"])
        body = matching["Body"]
        metadata = matching["Metadata"]
        assert isinstance(body, bytes)
        assert isinstance(metadata, dict)
        return {"ContentLength": len(body), "Metadata": metadata}


class _ControlPlane:
    def __init__(self, digest: str) -> None:
        self.digest = digest
        self.admitted: dict[str, object] | None = None
        self.finished: list[JobState] = []
        self.retry_incidents: list[dict[str, object]] = []
        self.interruptions: list[dict[str, object]] = []
        self.recoveries: list[dict[str, object]] = []
        self.recovery_leases: list[JobLease] = []
        self.heartbeats: list[datetime] = []
        self.progress: list[RuntimeProgress] = []
        self.terminal_leases: list[JobLease] = []
        self.expired_observations: list[str] = []
        self._current_lease: JobLease | None = None
        self.checkpoints: list[tuple[str, str, str, str]] = []
        self.claim_count = 0

    def claim_job(self, **kwargs: object) -> JobLease:
        assert kwargs["job_types"] == ("quote-admit",)
        self.claim_count += 1
        latest = self.checkpoints[-1] if self.checkpoints else None
        lease = JobLease(
            job_key="structure:digest:quote-admit",
            job_type="quote-admit",
            input_identity="structure:digest:bundles/current.ndjson:" + self.digest,
            lease_owner="quote-admitter",
            lease_epoch=self.claim_count,
            lease_expires_at=NOW + timedelta(seconds=120),
            checkpoint_cursor=None if latest is None else latest[0],
            checkpoint_digest=None if latest is None else latest[1],
        )
        self._current_lease = lease
        return lease

    def record_running_checkpoint(self, lease: JobLease, **kwargs: object) -> object:
        record = (
            str(kwargs["checkpoint_cursor"]),
            str(kwargs["checkpoint_digest"]),
            str(kwargs["artifact_key"]),
            str(kwargs["idempotency_key"]),
        )
        if record not in self.checkpoints:
            self.checkpoints.append(record)
        return object()

    def running_checkpoints(self, job_key: str) -> tuple[tuple[str, str, str], ...]:
        assert job_key == "structure:digest:quote-admit"
        return tuple(record[:3] for record in self.checkpoints)

    def quote_admission_input(self, job_key: str) -> tuple[str, str, str]:
        assert job_key == "structure:digest:quote-admit"
        return ("structure:digest", "bundles/current.ndjson", self.digest)

    def admit_quote_generation(self, lease: JobLease, **kwargs: object) -> None:
        now = kwargs.get("now")
        if (
            self._current_lease is not None
            and isinstance(now, datetime)
            and lease.lease_expires_at <= now
        ):
            self.expired_observations.append("admit")
        self.terminal_leases.append(lease)
        self.admitted = kwargs

    def heartbeat_runtime_attempt(
        self, lease: JobLease, *, now: datetime, lease_seconds: int
    ) -> JobLease:
        if now >= lease.lease_expires_at:
            self.expired_observations.append("heartbeat")
        self.heartbeats.append(now)
        renewed = replace(lease, lease_expires_at=now + timedelta(seconds=lease_seconds))
        self._current_lease = renewed
        return renewed

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        detail: dict[str, object] | None = None,
    ) -> RuntimeProgress:
        if now >= lease.lease_expires_at:
            self.expired_observations.append("progress")
        self.progress.append(progress)
        return progress

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)

    def finish_interrupted(self, lease: JobLease, **kwargs: object) -> None:
        self.interruptions.append(kwargs)

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        now = kwargs.get("now")
        if (
            self._current_lease is not None
            and isinstance(now, datetime)
            and lease.lease_expires_at <= now
        ):
            self.expired_observations.append("recovery")
        self.recovery_leases.append(lease)
        self.recoveries.append(kwargs)
        return False


class _StaleControlPlane(_ControlPlane):
    def heartbeat_runtime_attempt(
        self, lease: JobLease, *, now: datetime, lease_seconds: int
    ) -> JobLease:
        self.heartbeats.append(now)
        raise StaleLeaseError("simulated lease takeover")


class _TerminalRaceControlPlane(_ControlPlane):
    def __init__(self, digest: str, clock: _VirtualClock) -> None:
        super().__init__(digest)
        self._clock = clock
        self.terminal_committed = threading.Event()
        self.heartbeat_after_terminal = 0

    def heartbeat_runtime_attempt(
        self, lease: JobLease, *, now: datetime, lease_seconds: int
    ) -> JobLease:
        if self.terminal_committed.is_set():
            self.heartbeat_after_terminal += 1
            raise StaleLeaseError("heartbeat raced terminal admission")
        return super().heartbeat_runtime_attempt(lease, now=now, lease_seconds=lease_seconds)

    def admit_quote_generation(self, lease: JobLease, **kwargs: object) -> None:
        self.terminal_committed.set()
        self._clock.advance(31)
        time.sleep(0.01)
        super().admit_quote_generation(lease, **kwargs)


class _BlockingTerminalControlPlane(_ControlPlane):
    def __init__(self, digest: str) -> None:
        super().__init__(digest)
        self.terminal_started = threading.Event()
        self.release_terminal = threading.Event()
        self.terminal_committed = threading.Event()

    def admit_quote_generation(self, lease: JobLease, **kwargs: object) -> None:
        self.terminal_started.set()
        if not self.release_terminal.wait(timeout=2):
            raise RuntimeError("blocking terminal was not released")
        self.terminal_committed.set()
        super().admit_quote_generation(lease, **kwargs)


class _BlockingRecoveryControlPlane(_ControlPlane):
    def __init__(self, digest: str) -> None:
        super().__init__(digest)
        self.recovery_started = threading.Event()
        self.release_recovery = threading.Event()

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        self.recovery_started.set()
        if not self.release_recovery.wait(timeout=2):
            raise RuntimeError("blocking recovery was not released")
        raise RuntimeError("recovery probe failed after terminal admission")


class _IdleWorker:
    def run_once(self) -> QuoteBatchWorkerResult:
        return QuoteBatchWorkerResult(job_key=None, outcome="idle")


def _bundle() -> StructureBundleArtifact:
    components = {
        "events": ({"id": "event-a"},),
        "event_tags": (),
        "memberships": (),
        "group_truth": (),
        "markets": (
            {
                "market_id": "market-active",
                "condition_id": "condition-active",
                "slug": "active-market",
                "yes_token_id": "yes-active",
                "event_id": "event-a",
                "active": True,
                "closed": False,
                "neg_risk": True,
                "neg_risk_market_id": "neg-risk-a",
            },
            {
                "market_id": "market-closed",
                "condition_id": "condition-closed",
                "slug": "closed-market",
                "yes_token_id": "yes-closed",
                "event_id": "event-a",
                "active": True,
                "closed": True,
                "neg_risk": True,
                "neg_risk_market_id": "neg-risk-a",
            },
        ),
        "issues": (),
    }
    identity = StructureBundleIdentity(
        publication_id="p",
        window_id="w",
        snapshot_id=1,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="v",
        component_counts={key: len(value) for key, value in components.items()},
    )
    return StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(identity=identity, components=components)
    )


def test_quote_admitter_derives_active_neg_risk_legs_from_authenticated_bundle() -> None:
    bundle = _bundle()
    control_plane = _ControlPlane(bundle.sha256)
    objects = _Objects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    assert asyncio.run(worker.run_once()).outcome == "admitted"
    assert control_plane.admitted is not None
    legs = cast(tuple[object, ...], control_plane.admitted["legs"])
    assert len(legs) == 1
    assert cast(Any, legs[0]).yes_token_id == "yes-active"
    assert control_plane.admitted["structure_receipt_digest"] == bundle.sha256
    assert control_plane.admitted["input_artifacts"]
    assert len(objects.puts) == 1
    assert str(objects.puts[0]["Key"]).startswith("quote-inputs/")
    assert control_plane.finished == []


def test_quote_admitter_long_runtime_keeps_lease_live_for_207_simulated_seconds() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _ControlPlane(bundle.sha256)
    objects = _SlowObjects(bundle.payload, clock)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "admitted"
    assert clock() == NOW + timedelta(seconds=207)
    assert len(control_plane.heartbeats) >= 6
    assert control_plane.expired_observations == []
    assert [progress.sequence for progress in control_plane.progress] == list(
        range(1, len(control_plane.progress) + 1)
    )
    assert {
        (progress.stage, progress.current, progress.total) for progress in control_plane.progress
    } >= {
        ("read-manifest", 1, 1),
        ("read-shards", 1, 1),
        ("build-batches", 1, 1),
        ("upload-batches", 1, 1),
        ("commit-admission", 1, 1),
    }
    assert control_plane.admitted is not None
    legs = cast(tuple[Any, ...], control_plane.admitted["legs"])
    assert [leg.yes_token_id for leg in legs] == ["yes-active"]
    assert control_plane.terminal_leases[0].lease_expires_at > clock()
    assert control_plane.recovery_leases[0].lease_expires_at > clock()
    assert objects.last_body is not None and objects.last_body.finished.wait(timeout=1)


def test_quote_admitter_stale_heartbeat_cancels_owner_without_retry_or_recovery() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _StaleControlPlane(bundle.sha256)
    objects = _SlowObjects(bundle.payload, clock)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    with pytest.raises(StaleLeaseError, match="simulated lease takeover"):
        asyncio.run(worker.run_once())

    assert control_plane.admitted is None
    assert control_plane.retry_incidents == []
    assert control_plane.recoveries == []
    assert objects.last_body is not None and objects.last_body.finished.wait(timeout=1)


def test_quote_admitter_stale_heartbeat_drains_blocking_read_before_return() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _StaleControlPlane(bundle.sha256)
    objects = _BlockingReadObjects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    async def exercise() -> None:
        task = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(asyncio.to_thread(objects.get_started.wait, 1), timeout=1)
        assert objects.last_body is not None
        assert objects.last_body.started.wait(timeout=1)
        clock.advance(31)
        for _ in range(100):
            if control_plane.heartbeats:
                break
            await asyncio.sleep(0.005)
        assert control_plane.heartbeats
        assert not task.done()
        assert objects.last_body is not None
        objects.last_body.release.set()
        with pytest.raises(StaleLeaseError, match="simulated lease takeover"):
            await asyncio.wait_for(task, timeout=1)
        assert objects.last_body.finished.is_set()

    asyncio.run(exercise())
    assert control_plane.admitted is None
    assert control_plane.retry_incidents == []
    assert control_plane.recoveries == []


def test_quote_admitter_stale_heartbeat_drains_blocking_upload_before_return() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _StaleControlPlane(bundle.sha256)
    objects = _BlockingUploadObjects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    async def exercise() -> None:
        task = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(asyncio.to_thread(objects.put_started.wait, 1), timeout=1)
        clock.advance(31)
        for _ in range(100):
            if control_plane.heartbeats:
                break
            await asyncio.sleep(0.005)
        assert control_plane.heartbeats
        objects.release_put.set()
        with pytest.raises(StaleLeaseError, match="simulated lease takeover"):
            await asyncio.wait_for(task, timeout=1)
        assert objects.put_finished.is_set()

    asyncio.run(exercise())
    assert control_plane.admitted is None
    assert control_plane.retry_incidents == []
    assert control_plane.recoveries == []


def test_quote_admitter_external_cancellation_drains_blocking_read_before_return() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _ControlPlane(bundle.sha256)
    objects = _BlockingReadObjects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    async def exercise() -> None:
        task = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(asyncio.to_thread(objects.get_started.wait, 1), timeout=1)
        assert objects.last_body is not None
        assert objects.last_body.started.wait(timeout=1)
        task.cancel()
        assert objects.last_body is not None
        objects.last_body.release.set()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=1)
        assert objects.last_body.finished.is_set()

    asyncio.run(exercise())
    assert control_plane.admitted is None
    assert control_plane.retry_incidents == []
    assert control_plane.interruptions == [{"component": "quote-admit", "now": NOW}]
    assert control_plane.recoveries == []


def test_quote_admitter_scheduler_waits_for_terminal_commit() -> None:
    bundle = _bundle()
    control_plane = _BlockingTerminalControlPlane(bundle.sha256)
    objects = _Objects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )
    idle = _IdleWorker()
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=idle,
        structure_source_worker=idle,
        structure_source_materializer=idle,
        structure_worker=idle,
        structure_certifier=idle,
        quote_admitter=worker,
        quote_worker=idle,
        quote_certifier=idle,
        max_turns=6,
        include_quote_batch=False,
    )

    async def exercise() -> dict[str, object]:
        tick = asyncio.create_task(scheduler.run_tick())
        await asyncio.wait_for(asyncio.to_thread(control_plane.terminal_started.wait, 1), timeout=1)
        await asyncio.sleep(0.08)
        assert not tick.done(), "scheduler must not invent a terminal-call timeout"
        control_plane.release_terminal.set()
        return await asyncio.wait_for(tick, timeout=1)

    result = asyncio.run(exercise())

    assert control_plane.terminal_committed.is_set()
    assert control_plane.admitted is not None
    turns = cast(list[dict[str, object]], result["turns"])
    quote_turn = next(turn for turn in turns if turn["worker"] == "quote-admit")
    assert quote_turn["outcome"] == "admitted"


def test_quote_admitter_scheduler_waits_for_bounded_recovery() -> None:
    bundle = _bundle()
    control_plane = _BlockingRecoveryControlPlane(bundle.sha256)
    objects = _Objects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )
    idle = _IdleWorker()
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=idle,
        structure_source_worker=idle,
        structure_source_materializer=idle,
        structure_worker=idle,
        structure_certifier=idle,
        quote_admitter=worker,
        quote_worker=idle,
        quote_certifier=idle,
        max_turns=6,
        include_quote_batch=False,
    )

    async def exercise() -> dict[str, object]:
        tick = asyncio.create_task(scheduler.run_tick())
        await asyncio.wait_for(asyncio.to_thread(control_plane.recovery_started.wait, 1), timeout=1)
        await asyncio.sleep(0.08)
        assert not tick.done(), "scheduler must drain the bounded recovery call"
        control_plane.release_recovery.set()
        return await asyncio.wait_for(tick, timeout=1)

    result = asyncio.run(exercise())

    assert control_plane.admitted is not None
    turns = cast(list[dict[str, object]], result["turns"])
    quote_turn = next(turn for turn in turns if turn["worker"] == "quote-admit")
    assert quote_turn["outcome"] == "admitted:recovery-pending"


def test_quote_admitter_terminal_commit_detaches_on_grace_expiry_cancellation() -> None:
    bundle = _bundle()
    control_plane = _BlockingTerminalControlPlane(bundle.sha256)
    objects = _Objects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    async def exercise() -> None:
        task = asyncio.create_task(worker.run_once())
        await asyncio.wait_for(asyncio.to_thread(control_plane.terminal_started.wait, 1), timeout=1)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)
        assert not control_plane.terminal_committed.is_set()

    try:
        asyncio.run(exercise())
    finally:
        control_plane.release_terminal.set()


def test_quote_admitter_stops_heartbeat_before_terminal_commit_race() -> None:
    bundle = _bundle()
    clock = _VirtualClock()
    control_plane = _TerminalRaceControlPlane(bundle.sha256, clock)
    objects = _SlowObjects(bundle.payload, clock)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=100,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "admitted"
    assert control_plane.terminal_committed.is_set()
    assert control_plane.heartbeat_after_terminal == 0
    assert len(control_plane.recoveries) == 1
    assert objects.last_body is not None and objects.last_body.finished.wait(timeout=1)


def test_quote_admitter_v3_manifest_reports_each_shard_and_batch() -> None:
    markets = tuple(
        {
            "market_id": f"market-{index}",
            "condition_id": f"condition-{index}",
            "slug": f"market-{index}",
            "yes_token_id": f"yes-{index}",
            "event_id": "event-a",
            "active": True,
            "closed": False,
            "neg_risk": True,
            "neg_risk_market_id": f"neg-risk-{index}",
        }
        for index in range(2)
    )
    identity = StructureBundleIdentity(
        publication_id="publication-v3",
        window_id="window-v3",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="v3",
        component_counts={
            "events": 0,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 2,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shards = tuple(
        StructureShardArtifact.from_bytes(
            canonical_structure_shard_bytes(
                window_key="window-v3",
                source_digest="b" * 64,
                component="markets",
                ordinal=index,
                rows=(market,),
            )
        )
        for index, market in enumerate(markets)
    )
    receipts = tuple(
        StructureShardReceipt("markets", index, shard.key, shard.sha256, 1)
        for index, shard in enumerate(shards)
    )
    manifest_payload = canonical_structure_shard_manifest_bytes(identity=identity, shards=receipts)
    manifest = StructureBundleArtifact.from_bytes(manifest_payload)
    clock = _VirtualClock()
    control_plane = _ControlPlane(manifest.sha256)
    objects = _ObjectMap(
        {
            "bundles/current.ndjson": manifest.payload,
            **{shard.key: shard.payload for shard in shards},
        }
    )
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=clock,
        batch_size=1,
        lease_seconds=120,
        runtime_sleep=_VirtualSleeper(clock),
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "admitted"
    assert control_plane.admitted is not None
    legs = cast(tuple[Any, ...], control_plane.admitted["legs"])
    assert [leg.yes_token_id for leg in legs] == [
        "yes-0",
        "yes-1",
    ]
    progress = control_plane.progress
    assert [(item.stage, item.current, item.total) for item in progress] == [
        ("read-manifest", 1, 1),
        ("read-shards", 1, 2),
        ("read-shards", 2, 2),
        ("build-batches", 2, 2),
        ("upload-batches", 1, 2),
        ("upload-batches", 2, 2),
        ("commit-admission", 1, 1),
    ]
    assert len(objects.puts) == 3
    assert len(control_plane.checkpoints) == 1
    assert len(control_plane.recoveries) == 1


def test_quote_admitter_missing_v3_shard_records_safe_exact_artifact_identity() -> None:
    market = {
        "market_id": "market-missing-shard",
        "condition_id": "condition-missing-shard",
        "slug": "missing-shard",
        "yes_token_id": "yes-missing-shard",
        "event_id": "event-missing-shard",
        "active": True,
        "closed": False,
        "neg_risk": True,
        "neg_risk_market_id": "neg-risk-missing-shard",
    }
    identity = StructureBundleIdentity(
        publication_id="publication-missing-shard",
        window_id="window-missing-shard",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="v3",
        component_counts={
            "events": 0,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 2,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shards = tuple(
        StructureShardArtifact.from_bytes(
            canonical_structure_shard_bytes(
                window_key=identity.window_id,
                source_digest="b" * 64,
                component="markets",
                ordinal=index,
                rows=(market | {"yes_token_id": f"yes-missing-shard-{index}"},),
            )
        )
        for index in range(2)
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(
            identity=identity,
            shards=tuple(
                StructureShardReceipt("markets", index, shard.key, shard.sha256, 1)
                for index, shard in enumerate(shards)
            ),
        )
    )
    control_plane = _ControlPlane(manifest.sha256)
    objects = _ObjectMap(
        {
            "bundles/current.ndjson": manifest.payload,
            shards[0].key: shards[0].payload,
        }
    )
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=10,
    )

    with pytest.raises(QuoteAdmissionShardUnavailable) as raised:
        asyncio.run(worker.run_once())

    assert raised.value.artifact_key == shards[1].key
    assert control_plane.admitted is None
    incident = control_plane.retry_incidents[0]
    assert incident["error_class"] == "QuoteAdmissionShardUnavailable"
    assert incident["detail"]["missing_artifact_key"] == shards[1].key
    assert "provider" not in incident["detail"]


def test_quote_admitter_resumes_231_shards_after_last_durable_checkpoint() -> None:
    markets = tuple(
        {
            "market_id": f"market-{index}",
            "condition_id": f"condition-{index}",
            "slug": f"market-{index}",
            "yes_token_id": f"yes-{index}",
            "event_id": "event-a",
            "active": True,
            "closed": False,
            "neg_risk": True,
            "neg_risk_market_id": "neg-risk-a",
        }
        for index in range(231)
    )
    identity = StructureBundleIdentity(
        publication_id="publication-resume",
        window_id="window-resume",
        snapshot_id=1,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="v3",
        component_counts={
            "events": 0,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 231,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shards = tuple(
        StructureShardArtifact.from_bytes(
            canonical_structure_shard_bytes(
                window_key="window-resume",
                source_digest="a" * 64,
                component="markets",
                ordinal=index,
                rows=(market,),
            )
        )
        for index, market in enumerate(markets)
    )
    receipts = tuple(
        StructureShardReceipt("markets", index, shard.key, shard.sha256, 1)
        for index, shard in enumerate(shards)
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(identity=identity, shards=receipts)
    )

    class _FailOnceObjectMap(_ObjectMap):
        def __init__(self, payloads: dict[str, bytes], fail_key: str) -> None:
            super().__init__(payloads)
            self.fail_key = fail_key
            self.failed = False
            self.read_counts: dict[str, int] = {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            self.read_counts[key] = self.read_counts.get(key, 0) + 1
            if key == self.fail_key and not self.failed:
                self.failed = True
                raise TimeoutError("simulated shard interruption")
            return super().get_object(**kwargs)

    control_plane = _ControlPlane(manifest.sha256)
    objects = _FailOnceObjectMap(
        {
            "bundles/current.ndjson": manifest.payload,
            **{shard.key: shard.payload for shard in shards},
        },
        shards[128].key,
    )
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=20,
    )

    with pytest.raises(QuoteAdmissionShardUnavailable) as unavailable:
        asyncio.run(worker.run_once())
    assert unavailable.value.artifact_key == shards[128].key
    assert len(control_plane.checkpoints) == 12
    assert control_plane.checkpoints[-1][0].endswith(":120")

    assert asyncio.run(worker.run_once()).outcome == "admitted"
    assert all(objects.read_counts[shard.key] == 1 for shard in shards[:120])
    assert all(objects.read_counts[shard.key] == 2 for shard in shards[120:129])
    assert all(objects.read_counts[shard.key] == 1 for shard in shards[129:])


def test_quote_admitter_retries_without_batches_when_bundle_digest_is_wrong() -> None:
    bundle = _bundle()
    control_plane = _ControlPlane("b" * 64)
    objects = _Objects(bundle.payload)
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, control_plane),
        object_client=objects,
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    with pytest.raises(QuoteAdmissionError, match="digest"):
        asyncio.run(worker.run_once())
    assert control_plane.admitted is None
    assert control_plane.finished == []
    assert control_plane.retry_incidents[0]["component"] == "quote-admit"
    assert control_plane.retry_incidents[0]["detail"] == {
        "job_key": "structure:digest:quote-admit",
        "lease_epoch": 1,
        "error_class": "StructureBundleError",
        "failure_fingerprint": control_plane.retry_incidents[0]["detail"]["failure_fingerprint"],
    }
    assert control_plane.retry_incidents[0]["detail"]["failure_fingerprint"].startswith("sha256:")
    assert objects.puts == []


def test_v3_quote_admission_rejects_even_identical_duplicate_yes_tokens() -> None:
    market = {
        "market_id": "market-active",
        "condition_id": "condition-active",
        "slug": "active-market",
        "yes_token_id": "yes-active",
        "event_id": "event-a",
        "active": True,
        "closed": False,
        "neg_risk": True,
        "neg_risk_market_id": "neg-risk-a",
    }
    first = StructureShardArtifact.from_bytes(
        canonical_structure_shard_bytes(
            window_key="window-v3",
            source_digest="a" * 64,
            component="markets",
            ordinal=0,
            rows=(market,),
        )
    )
    second = StructureShardArtifact.from_bytes(
        canonical_structure_shard_bytes(
            window_key="window-v3",
            source_digest="a" * 64,
            component="markets",
            ordinal=1,
            rows=(market,),
        )
    )
    worker = TransactionalQuoteAdmitter(
        control_plane=cast(Any, object()),
        object_client=cast(Any, _ObjectMap({first.key: first.payload, second.key: second.payload})),
        bucket="artifacts",
        worker_id="quote-admitter",
        now=lambda: NOW,
        batch_size=100,
    )

    with pytest.raises(QuoteAdmissionError, match="duplicate YES token"):
        worker._read_v3_quote_legs(
            (
                StructureShardReceipt("markets", 0, first.key, first.sha256, 1),
                StructureShardReceipt("markets", 1, second.key, second.sha256, 1),
            )
        )
