from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from polyarb.control_plane import structure_worker as structure_worker_module
from polyarb.control_plane.faults import IntentionalStagingRetryFault
from polyarb.control_plane.models import (
    JobLease,
    JobState,
    StructureRangeReceipt,
    StructureRangeSpec,
)
from polyarb.control_plane.postgres import (
    IncompleteStructureGenerationError,
    StaleLeaseError,
    StructureParityMismatchError,
)
from polyarb.control_plane.runtime_contract import RetryableHeartbeatError, ServiceStopRequested
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    StructureRangeArtifact,
    StructureShardArtifact,
    StructureShardReceipt,
    canonical_structure_bundle_bytes,
    canonical_structure_range_bytes,
    canonical_structure_shard_bytes,
    canonical_structure_shard_manifest_bytes,
)
from polyarb.control_plane.structure_worker import (
    StructureNormalizationInputInvalid,
    StructureWorkerError,
    StructureWorkerResult,
    TransactionalStructureCertifier,
    TransactionalStructureRangePool,
    TransactionalStructureWorker,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class _RangePoolLane:
    def __init__(
        self,
        *,
        ordinal: int,
        entered: list[int],
        release: asyncio.Event,
        lease_seconds: int = 120,
        error: BaseException | None = None,
    ) -> None:
        self._ordinal = ordinal
        self._entered = entered
        self._release = release
        self._lease_seconds = lease_seconds
        self._error = error

    async def run_once(self) -> StructureWorkerResult:
        self._entered.append(self._ordinal)
        await self._release.wait()
        if self._error is not None:
            raise self._error
        return StructureWorkerResult(
            job_key=f"structure:generation:range:{self._ordinal}",
            outcome="succeeded",
        )


def test_structure_range_pool_runs_all_lanes_concurrently_and_aggregates() -> None:
    async def exercise() -> None:
        entered: list[int] = []
        release = asyncio.Event()
        pool = TransactionalStructureRangePool(
            lanes=tuple(
                _RangePoolLane(ordinal=index, entered=entered, release=release)
                for index in range(12)
            )
        )

        turn = asyncio.create_task(pool.run_once())
        for _ in range(100):
            if len(entered) == 12:
                break
            await asyncio.sleep(0)
        assert sorted(entered) == list(range(12))
        release.set()

        result = await turn
        assert result.outcome == "succeeded:12/12"
        assert result.job_key is not None
        assert len(result.job_key.split(",")) == 12

    asyncio.run(exercise())


def test_structure_range_pool_rejects_mixed_lease_policies() -> None:
    release = asyncio.Event()
    with pytest.raises(ValueError, match="share one positive lease policy"):
        TransactionalStructureRangePool(
            lanes=(
                _RangePoolLane(ordinal=0, entered=[], release=release, lease_seconds=120),
                _RangePoolLane(ordinal=1, entered=[], release=release, lease_seconds=90),
            )
        )


def test_structure_range_pool_drains_siblings_before_raising() -> None:
    async def exercise() -> None:
        entered: list[int] = []
        release = asyncio.Event()
        pool = TransactionalStructureRangePool(
            lanes=(
                _RangePoolLane(
                    ordinal=0,
                    entered=entered,
                    release=release,
                    error=RuntimeError("lane failed"),
                ),
                _RangePoolLane(ordinal=1, entered=entered, release=release),
            )
        )
        turn = asyncio.create_task(pool.run_once())
        for _ in range(100):
            if len(entered) == 2:
                break
            await asyncio.sleep(0)
        assert sorted(entered) == [0, 1]
        release.set()
        with pytest.raises(RuntimeError, match="lane failed"):
            await turn

    asyncio.run(exercise())


def test_structure_bounded_sync_call_checks_heartbeat_after_fast_success() -> None:
    checks = 0

    def heartbeat() -> None:
        nonlocal checks
        checks += 1

    assert (
        structure_worker_module._run_bounded_sync_call(
            lambda: "done",
            heartbeat=heartbeat,
            heartbeat_interval_seconds=30,
            attempt_timeout_seconds=120,
        )
        == "done"
    )
    assert checks == 1


def _bundle() -> StructureBundleArtifact:
    identity = StructureBundleIdentity(
        publication_id="publication-1",
        window_id="window-1",
        snapshot_id=42,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="structure-v7",
        component_counts={
            "events": 2,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )
    return StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(
            identity=identity,
            components={
                "events": ({"id": "event-a"}, {"id": "event-b"}),
                "event_tags": (),
                "memberships": (),
                "group_truth": (),
                "markets": (),
                "issues": (),
            },
        )
    )


class _Body:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload


class FakeObjectClient:
    def __init__(self, bundle: StructureBundleArtifact) -> None:
        self.bundle = bundle
        self.upload: dict[str, object] = {}

    def get_object(self, **kwargs: object) -> dict[str, object]:
        assert kwargs["Key"] == self.bundle.key
        return {"Body": _Body(self.bundle.payload)}

    def put_object(self, **kwargs: object) -> None:
        self.upload = kwargs

    def head_object(self, **kwargs: object) -> dict[str, object]:
        return {"ContentLength": len(self.upload["Body"]), "Metadata": self.upload["Metadata"]}


class BlockingRangeObjectClient(FakeObjectClient):
    def __init__(self, bundle: StructureBundleArtifact) -> None:
        super().__init__(bundle)
        self.started = threading.Event()
        self.release = threading.Event()

    def get_object(self, **kwargs: object) -> dict[str, object]:
        self.started.set()
        if not self.release.wait(timeout=10):
            raise AssertionError("blocking range read was not released")
        return super().get_object(**kwargs)


def _elapsed_clock() -> Callable[[], datetime]:
    started = time.monotonic()
    return lambda: NOW + timedelta(seconds=time.monotonic() - started)


async def _wait_for_heartbeats(control_plane: FakeControlPlane, count: int) -> None:
    for _ in range(500):
        if len(control_plane.runtime_heartbeats) >= count:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"expected at least {count} runtime heartbeats")


class FakeControlPlane:
    def __init__(self, spec: StructureRangeSpec, *, prior: object | None = None) -> None:
        self.spec = spec
        self.prior = prior
        self.finished: list[JobState] = []
        self.recorded: dict[str, object] | None = None
        self.completed: dict[str, object] | None = None
        self.recoveries: list[dict[str, object]] = []
        self.retry_incidents: list[dict[str, object]] = []
        self.interruptions: list[dict[str, object]] = []
        self.runtime_progress: list[dict[str, object]] = []
        self.runtime_heartbeats: list[dict[str, object]] = []

    def claim_job(self, **kwargs: object) -> JobLease:
        now = kwargs["now"]
        lease_seconds = kwargs["lease_seconds"]
        assert isinstance(now, datetime)
        assert isinstance(lease_seconds, int)
        return JobLease(
            job_key=self.spec.job_key,
            job_type="structure-normalize",
            input_identity=self.spec.input_identity,
            lease_owner="worker-a",
            lease_epoch=1,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def structure_range_receipt(self, job_key: str) -> object | None:
        assert job_key == self.spec.job_key
        return self.prior

    def structure_range_spec(self, job_key: str) -> StructureRangeSpec:
        assert job_key == self.spec.job_key
        return self.spec

    def record_structure_range(self, lease: JobLease, **kwargs: object) -> None:
        self.recorded = kwargs

    def complete_structure_range(self, lease: JobLease, **kwargs: object) -> None:
        self.completed = kwargs

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        self.recoveries.append(kwargs)
        return False

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)

    def finish_interrupted(self, lease: JobLease, **kwargs: object) -> None:
        self.interruptions.append(kwargs)

    def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
        self.runtime_progress.append(kwargs)

    def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
        self.runtime_heartbeats.append(kwargs)
        now = kwargs["now"]
        lease_seconds = kwargs["lease_seconds"]
        assert isinstance(now, datetime)
        assert isinstance(lease_seconds, int)
        return JobLease(
            job_key=lease.job_key,
            job_type=lease.job_type,
            input_identity=lease.input_identity,
            lease_owner=lease.lease_owner,
            lease_epoch=lease.lease_epoch,
            lease_expires_at=now + timedelta(seconds=lease_seconds),
            checkpoint_cursor=lease.checkpoint_cursor,
            checkpoint_digest=lease.checkpoint_digest,
        )


def _spec(bundle: StructureBundleArtifact) -> StructureRangeSpec:
    return StructureRangeSpec.create(
        bundle_key=bundle.key,
        bundle_digest=bundle.sha256,
        component="events",
        ordinal=0,
        range_start="event-a",
        range_end="event-b",
    )


def test_transactional_structure_worker_reads_frozen_r2_range_then_records() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    objects = FakeObjectClient(bundle)
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert control_plane.completed is not None
    assert control_plane.completed["record_count"] == 1
    assert control_plane.completed["artifact_key"] == objects.upload["Key"]
    assert control_plane.completed["artifact_digest"] == objects.upload["Metadata"]["sha256"]
    assert b'"id":"event-a"' in objects.upload["Body"]
    assert b'"id":"event-b"' not in objects.upload["Body"]
    assert control_plane.finished == []


def test_structure_worker_renews_while_normalize_read_exceeds_lease() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    objects = BlockingRangeObjectClient(bundle)
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    async def run():
        task = asyncio.create_task(worker.run_once())
        assert await asyncio.to_thread(objects.started.wait, 2)
        await _wait_for_heartbeats(control_plane, 3)
        objects.release.set()
        return await task

    result = asyncio.run(run())

    assert result.outcome == "succeeded"
    assert len(control_plane.runtime_heartbeats) >= 3
    assert control_plane.completed is not None


def test_structure_worker_retries_transient_heartbeat_while_normalize_read_blocks() -> None:
    bundle = _bundle()
    objects = BlockingRangeObjectClient(bundle)

    class TransientHeartbeatControlPlane(FakeControlPlane):
        def __init__(self, spec: StructureRangeSpec) -> None:
            super().__init__(spec)
            self.heartbeat_calls = 0

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.heartbeat_calls += 1
            if self.heartbeat_calls == 1:
                raise RetryableHeartbeatError("transient heartbeat connection failure")
            return super().heartbeat_runtime_attempt(lease, **kwargs)

    control_plane = TransientHeartbeatControlPlane(_spec(bundle))
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    async def run() -> StructureWorkerResult:
        task = asyncio.create_task(worker.run_once())
        assert await asyncio.to_thread(objects.started.wait, 2)
        await _wait_for_heartbeats(control_plane, 1)
        objects.release.set()
        return await task

    result = asyncio.run(run())

    assert result.outcome == "succeeded"
    assert control_plane.heartbeat_calls >= 2
    assert control_plane.completed is not None


def test_stale_normalize_heartbeat_drains_read_and_prevents_terminal_commit() -> None:
    bundle = _bundle()
    objects = BlockingRangeObjectClient(bundle)

    class StaleControlPlane(FakeControlPlane):
        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.runtime_heartbeats.append(kwargs)
            objects.release.set()
            raise StaleLeaseError("normalize lease was fenced")

    control_plane = StaleControlPlane(_spec(bundle))
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    with pytest.raises(StaleLeaseError, match="fenced"):
        asyncio.run(worker.run_once())

    assert control_plane.recorded is None
    assert control_plane.finished == []
    assert control_plane.retry_incidents == []
    assert control_plane.interruptions == []
    assert objects.upload == {}


def test_cancelled_normalize_read_is_drained_without_terminal_commit() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    objects = BlockingRangeObjectClient(bundle)
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    async def run() -> None:
        task = asyncio.create_task(worker.run_once())
        assert await asyncio.to_thread(objects.started.wait, 2)
        task.cancel()
        objects.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(run())

    assert control_plane.recorded is None
    assert control_plane.finished == []
    assert control_plane.retry_incidents == []
    assert control_plane.interruptions[0]["component"] == "structure-normalize"
    assert objects.upload == {}


def test_structure_worker_reports_all_fenced_range_lifecycle_stages() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=FakeObjectClient(bundle),
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert [item["progress"].stage for item in control_plane.runtime_progress] == [
        "read-range",
        "normalize-range",
        "upload-range",
        "commit-range",
    ]


def test_structure_worker_uses_terminal_range_api_when_available() -> None:
    bundle = _bundle()

    class TerminalControlPlane(FakeControlPlane):
        def __init__(self) -> None:
            super().__init__(_spec(bundle))
            self.completed: dict[str, object] | None = None

        def complete_structure_range(self, lease: JobLease, **kwargs: object) -> None:
            self.completed = kwargs

    control_plane = TerminalControlPlane()
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=FakeObjectClient(bundle),
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert control_plane.completed is not None
    assert control_plane.completed["range_digest"] == control_plane.spec.range_digest
    assert control_plane.finished == []


def test_structure_fault_hook_crashes_after_verified_upload_before_receipt() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    objects = FakeObjectClient(bundle)
    observed: list[str] = []
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
        crash_after_r2_upload=lambda lease: (
            observed.append(lease.job_key),
            (_ for _ in ()).throw(KeyboardInterrupt("staging fault")),
        )[1],
    )

    with pytest.raises(KeyboardInterrupt, match="staging fault"):
        asyncio.run(worker.run_once())

    assert objects.upload
    assert observed == [_spec(bundle).job_key]
    assert control_plane.recorded is None
    assert control_plane.finished == []


def test_structure_retry_fault_uses_existing_retry_incident_path() -> None:
    bundle = _bundle()
    control_plane = FakeControlPlane(_spec(bundle))
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=FakeObjectClient(bundle),
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
        retry_fault_before_receipt=lambda _lease: (_ for _ in ()).throw(
            IntentionalStagingRetryFault("intentional staging retry")
        ),
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "retryable"
    assert control_plane.recorded is None
    assert control_plane.retry_incidents[0]["component"] == "structure-normalize"


def test_transactional_structure_worker_recovers_receipt_without_r2_read() -> None:
    bundle = _bundle()
    prior = StructureRangeReceipt(
        job_key=_spec(bundle).job_key,
        bundle_digest=bundle.sha256,
        component="events",
        range_digest=_spec(bundle).range_digest,
        artifact_key="structure-ranges/prior/rows.ndjson",
        artifact_digest="b" * 64,
        record_count=1,
    )
    control_plane = FakeControlPlane(_spec(bundle), prior=prior)
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=FakeObjectClient(bundle),
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "recovered"
    assert control_plane.recorded is None
    assert control_plane.completed is not None
    assert control_plane.completed["range_digest"] == prior.range_digest
    assert control_plane.completed["artifact_key"] == prior.artifact_key
    assert control_plane.completed["artifact_digest"] == prior.artifact_digest
    assert control_plane.completed["record_count"] == prior.record_count
    assert control_plane.finished == []


def test_structure_worker_reads_only_named_v3_shard() -> None:
    identity = StructureBundleIdentity(
        publication_id="source-window:window-v3",
        window_id="window-v3",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shard = StructureShardArtifact.from_bytes(
        canonical_structure_shard_bytes(
            window_key="window-v3",
            source_digest="a" * 64,
            component="events",
            ordinal=0,
            rows=({"id": "event-a"},),
        )
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(
            identity=identity,
            shards=(
                StructureShardReceipt(
                    component="events",
                    ordinal=0,
                    artifact_key=shard.key,
                    artifact_digest=shard.sha256,
                    row_count=1,
                ),
            ),
        )
    )
    spec = StructureRangeSpec.create(
        bundle_key=manifest.key,
        bundle_digest=manifest.sha256,
        component="events",
        ordinal=0,
        range_start="shard:00000000",
        range_end="shard:00000001",
    )

    class Objects(FakeObjectClient):
        def __init__(self) -> None:
            self.bundle = manifest
            self.upload = {}
            self.read_keys: list[str] = []

        def get_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            self.read_keys.append(key)
            return {"Body": _Body({manifest.key: manifest.payload, shard.key: shard.payload}[key])}

    objects = Objects()
    worker = TransactionalStructureWorker(
        control_plane=FakeControlPlane(spec),
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert objects.read_keys == [manifest.key, shard.key]


def test_structure_worker_atomically_quarantines_schema_invalid_named_v3_shard() -> None:
    identity = StructureBundleIdentity(
        publication_id="source-window:corrupt-v3",
        window_id="corrupt-v3",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    corrupt = StructureShardArtifact.from_bytes(b'{"kind":"not-a-structure-shard"}\n')
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(
            identity=identity,
            shards=(
                StructureShardReceipt("events", 0, corrupt.key, corrupt.sha256, 1),
            ),
        )
    )
    spec = StructureRangeSpec.create(
        bundle_key=manifest.key,
        bundle_digest=manifest.sha256,
        component="events",
        ordinal=0,
        range_start="shard:00000000",
        range_end="shard:00000001",
    )

    class ControlPlane(FakeControlPlane):
        def __init__(self) -> None:
            super().__init__(spec)
            self.quarantines: list[dict[str, object]] = []

        def finish_quarantined_with_incident(
            self, _lease: JobLease, **kwargs: object
        ) -> None:
            self.quarantines.append(kwargs)

    class Objects(FakeObjectClient):
        def __init__(self) -> None:
            self.bundle = manifest
            self.upload = {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            payloads = {manifest.key: manifest.payload, corrupt.key: corrupt.payload}
            return {"Body": _Body(payloads[key])}

    control_plane = ControlPlane()
    objects = Objects()
    worker = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    with pytest.raises(StructureNormalizationInputInvalid) as invalid:
        asyncio.run(worker.run_once())

    assert invalid.value.artifact_key == corrupt.key
    assert control_plane.completed is None
    assert control_plane.finished == []
    assert objects.upload == {}
    assert control_plane.quarantines == [
        {
            "error_class": "StructureNormalizationInputInvalid",
            "incident_key": f"incident:input-quarantine:{spec.job_key}",
            "dedupe_key": f"input-quarantine:{spec.job_key}",
            "component": "structure-normalize",
            "summary": "structure-normalize input quarantined",
            "detail": {
                "job_key": spec.job_key,
                "lease_epoch": 1,
                "input_artifact_key": corrupt.key,
                "bundle_digest": spec.bundle_digest,
                "component": spec.component,
                "range_digest": spec.range_digest,
                "reason_code": "failure.schema",
            },
            "channels": ("dashboard",),
            "now": NOW,
        }
    ]


def test_structure_certifier_service_stop_uses_interruption_not_defect_retry() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.interruption = None

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "a" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "a" * 64,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, _generation_key: str):
            return ((object(), object()),)

        def record_runtime_progress(self, _lease: JobLease, **_kwargs: object) -> None:
            return None

        def heartbeat_runtime_attempt(self, lease: JobLease, **_kwargs: object) -> JobLease:
            return lease

        def finish_interrupted(self, _lease: JobLease, **kwargs: object) -> None:
            self.interruption = kwargs

        def finish_retryable_with_incident(self, *_args: object, **_kwargs: object) -> None:
            raise AssertionError("service stop must not consume defect retry budget")

    control_plane = ControlPlane()
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=object(),
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
    )
    certifier.request_stop()

    with pytest.raises(ServiceStopRequested):
        certifier.run_once()

    assert control_plane.interruption == {"component": "structure-certify", "now": NOW}


def test_structure_certifier_heartbeats_during_parity_before_fenced_commit() -> None:
    bundle = _bundle()
    spec = StructureRangeSpec.create(
        bundle_key=bundle.key,
        bundle_digest=bundle.sha256,
        component="events",
        ordinal=0,
        range_start="",
        range_end="",
    )
    range_artifact = StructureRangeArtifact.from_bytes(
        canonical_structure_range_bytes(
            bundle_digest=bundle.sha256,
            component="events",
            range_digest=spec.range_digest,
            rows=({"id": "event-a"}, {"id": "event-b"}),
        )
    )

    class CertifierControlPlane:
        def __init__(self) -> None:
            self.finished: list[JobState] = []
            self.certified: dict[str, object] | None = None
            self.certified_lease: JobLease | None = None
            self.recoveries: list[dict[str, object]] = []
            self.heartbeats: list[dict[str, object]] = []
            self.runtime_progress: list[dict[str, object]] = []

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "a" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "a" * 64,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_manifest_payload(self, generation_key: str) -> bytes:
            assert generation_key == "structure:" + "a" * 64
            return b'{"kind":"structure-manifest"}\n'

        def structure_generation_receipts(self, generation_key: str):
            assert generation_key == "structure:" + "a" * 64
            return (
                (
                    spec,
                    type(
                        "Receipt",
                        (),
                        {
                            "artifact_key": range_artifact.key,
                            "artifact_digest": range_artifact.sha256,
                            "record_count": 2,
                        },
                    )(),
                ),
            )

        def certify_structure_generation(self, lease: JobLease, **kwargs: object) -> str:
            self.certified_lease = lease
            self.certified = kwargs
            return str(kwargs["artifact_digest"])

        def heartbeat(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.heartbeats.append(kwargs)
            return JobLease(
                job_key=lease.job_key,
                job_type=lease.job_type,
                input_identity=lease.input_identity,
                lease_owner=lease.lease_owner,
                lease_epoch=lease.lease_epoch,
                lease_expires_at=kwargs["now"] + timedelta(seconds=kwargs["lease_seconds"]),
                checkpoint_cursor=lease.checkpoint_cursor,
                checkpoint_digest=lease.checkpoint_digest,
            )

        def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
            self.finished.append(state)

        def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
            self.recoveries.append(kwargs)
            return False

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.heartbeats.append(kwargs)
            return JobLease(
                job_key=lease.job_key,
                job_type=lease.job_type,
                input_identity=lease.input_identity,
                lease_owner=lease.lease_owner,
                lease_epoch=lease.lease_epoch,
                lease_expires_at=kwargs["now"] + timedelta(seconds=kwargs["lease_seconds"]),
                checkpoint_cursor=lease.checkpoint_cursor,
                checkpoint_digest=lease.checkpoint_digest,
            )

    class ObjectClient:
        def __init__(self) -> None:
            self.upload: dict[str, object] = {}

        def put_object(self, **kwargs: object) -> None:
            self.upload = kwargs

        def get_object(self, **kwargs: object) -> dict[str, object]:
            payload = bundle.payload if kwargs["Key"] == bundle.key else range_artifact.payload
            return {"Body": _Body(payload)}

        def head_object(self, **kwargs: object) -> dict[str, object]:
            return {"ContentLength": len(self.upload["Body"]), "Metadata": self.upload["Metadata"]}

    control_plane = CertifierControlPlane()
    objects = ObjectClient()
    clock_values = iter((0.0, 11.0))
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
        monotonic_clock=lambda: next(clock_values, 11.0),
    )

    result = certifier.run_once()

    assert result.outcome == "certified"
    assert objects.upload["Key"] == control_plane.certified["artifact_key"]
    assert objects.upload["Metadata"]["sha256"] == control_plane.certified["artifact_digest"]
    assert control_plane.heartbeats == [{"now": NOW, "lease_seconds": 30}]
    assert control_plane.certified_lease is not None
    assert control_plane.certified_lease.lease_expires_at == NOW + timedelta(seconds=30)
    assert control_plane.finished == []
    assert [item["progress"].stage for item in control_plane.runtime_progress] == [
        "verify-parity",
        "build-manifest",
        "upload-manifest",
        "commit-certification",
    ]


def test_structure_certifier_renews_while_parity_read_exceeds_lease() -> None:
    bundle = _bundle()
    spec = StructureRangeSpec.create(
        bundle_key=bundle.key,
        bundle_digest=bundle.sha256,
        component="events",
        ordinal=0,
        range_start="",
        range_end="",
    )
    range_artifact = StructureRangeArtifact.from_bytes(
        canonical_structure_range_bytes(
            bundle_digest=bundle.sha256,
            component="events",
            range_digest=spec.range_digest,
            rows=({"id": "event-a"}, {"id": "event-b"}),
        )
    )

    class ControlPlane:
        def __init__(self) -> None:
            self.heartbeats: list[dict[str, object]] = []
            self.runtime_progress: list[dict[str, object]] = []
            self.certified = False

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "a" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "a" * 64,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, generation_key: str):
            assert generation_key == "structure:" + "a" * 64
            return (
                (
                    spec,
                    type(
                        "Receipt",
                        (),
                        {
                            "artifact_key": range_artifact.key,
                            "artifact_digest": range_artifact.sha256,
                            "record_count": 2,
                        },
                    )(),
                ),
            )

        def structure_manifest_payload(self, generation_key: str) -> bytes:
            assert generation_key == "structure:" + "a" * 64
            return b'{"kind":"structure-manifest"}\n'

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.heartbeats.append(kwargs)
            return JobLease(
                job_key=lease.job_key,
                job_type=lease.job_type,
                input_identity=lease.input_identity,
                lease_owner=lease.lease_owner,
                lease_epoch=lease.lease_epoch,
                lease_expires_at=kwargs["now"] + timedelta(seconds=kwargs["lease_seconds"]),
                checkpoint_cursor=lease.checkpoint_cursor,
                checkpoint_digest=lease.checkpoint_digest,
            )

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            self.runtime_progress.append(kwargs)

        def certify_structure_generation(self, lease: JobLease, **kwargs: object) -> str:
            self.certified = True
            return str(kwargs["artifact_digest"])

        def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
            return False

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            raise AssertionError("blocking parity read must not be retried")

    class BlockingObjects:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.block_once = True
            self.upload: dict[str, object] = {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            if self.block_once:
                self.block_once = False
                self.started.set()
                if not self.release.wait(timeout=10):
                    raise AssertionError("blocking parity read was not released")
            payload = bundle.payload if kwargs["Key"] == bundle.key else range_artifact.payload
            return {"Body": _Body(payload)}

        def put_object(self, **kwargs: object) -> None:
            self.upload = kwargs

        def head_object(self, **kwargs: object) -> dict[str, object]:
            return {
                "ContentLength": len(self.upload["Body"]),
                "Metadata": self.upload["Metadata"],
            }

    control_plane = ControlPlane()
    objects = BlockingObjects()
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="certifier-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    async def run():
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        assert await asyncio.to_thread(objects.started.wait, 2)
        for _ in range(500):
            if len(control_plane.heartbeats) >= 3:
                break
            await asyncio.sleep(0.01)
        else:
            raise AssertionError("expected parity heartbeats while R2 read blocked")
        objects.release.set()
        return await task

    result = asyncio.run(run())

    assert result.outcome == "certified"
    assert control_plane.certified
    assert len(control_plane.heartbeats) >= 3


def test_structure_certifier_renews_while_v3_parsing_exceeds_lease(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    identity = StructureBundleIdentity(
        publication_id="source-window:parse-renewal",
        window_id="parse-renewal",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shard = StructureShardArtifact.from_bytes(
        canonical_structure_shard_bytes(
            window_key="parse-renewal",
            source_digest="a" * 64,
            component="events",
            ordinal=0,
            rows=({"id": "event-a"},),
        )
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(
            identity=identity,
            shards=(StructureShardReceipt("events", 0, shard.key, shard.sha256, 1),),
        )
    )
    spec = StructureRangeSpec.create(
        bundle_key=manifest.key,
        bundle_digest=manifest.sha256,
        component="events",
        ordinal=0,
        range_start="shard:00000000",
        range_end="shard:00000001",
    )
    range_artifact = StructureRangeArtifact.from_bytes(
        canonical_structure_range_bytes(
            bundle_digest=manifest.sha256,
            component="events",
            range_digest=spec.range_digest,
            rows=({"id": "event-a"},),
        )
    )
    receipt = StructureRangeReceipt(
        job_key=spec.job_key,
        bundle_digest=spec.bundle_digest,
        component=spec.component,
        range_digest=spec.range_digest,
        artifact_key=range_artifact.key,
        artifact_digest=range_artifact.sha256,
        record_count=1,
    )

    class ControlPlane:
        def __init__(self) -> None:
            self.heartbeats: list[dict[str, object]] = []
            self.certified = False

        def claim_job(self, **_kwargs: object) -> JobLease:
            return JobLease(
                job_key=spec.generation_key + ":certify",
                job_type="structure-certify",
                input_identity=spec.generation_key,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=3),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, _generation_key: str):
            return ((spec, receipt),)

        def running_checkpoints(self, _job_key: str):
            return ()

        def record_running_checkpoint(self, _lease: JobLease, **_kwargs: object) -> object:
            return object()

        def structure_manifest_payload(self, _generation_key: str) -> bytes:
            return b'{"kind":"structure-manifest"}\n'

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            self.heartbeats.append(kwargs)
            return JobLease(
                job_key=lease.job_key,
                job_type=lease.job_type,
                input_identity=lease.input_identity,
                lease_owner=lease.lease_owner,
                lease_epoch=lease.lease_epoch,
                lease_expires_at=kwargs["now"] + timedelta(seconds=kwargs["lease_seconds"]),
                checkpoint_cursor=lease.checkpoint_cursor,
                checkpoint_digest=lease.checkpoint_digest,
            )

        def record_runtime_progress(self, _lease: JobLease, **_kwargs: object) -> None:
            return None

        def certify_structure_generation(self, _lease: JobLease, **kwargs: object) -> str:
            self.certified = True
            return str(kwargs["artifact_digest"])

        def record_job_recovery(self, _lease: JobLease, **_kwargs: object) -> bool:
            return False

        def finish_retryable_with_incident(self, _lease: JobLease, **_kwargs: object) -> None:
            raise AssertionError("blocking v3 parse must retain its lease")

    class Objects:
        def __init__(self) -> None:
            self.upload: dict[str, object] = {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            payloads = {
                manifest.key: manifest.payload,
                shard.key: shard.payload,
                range_artifact.key: range_artifact.payload,
            }
            return {"Body": _Body(payloads[str(kwargs["Key"])])}

        def put_object(self, **kwargs: object) -> None:
            self.upload = kwargs

        def head_object(self, **_kwargs: object) -> dict[str, object]:
            return {
                "ContentLength": len(self.upload["Body"]),
                "Metadata": self.upload["Metadata"],
            }

    parse_started = threading.Event()
    release_parse = threading.Event()
    original_parse = structure_worker_module.parse_structure_shard_bytes

    def blocking_parse(payload: bytes, *, expected_sha256: str):
        parse_started.set()
        if not release_parse.wait(timeout=10):
            raise AssertionError("blocking v3 parse was not released")
        return original_parse(payload, expected_sha256=expected_sha256)

    monkeypatch.setattr(structure_worker_module, "parse_structure_shard_bytes", blocking_parse)
    control_plane = ControlPlane()
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=Objects(),
        bucket="structure",
        worker_id="certifier-a",
        now=_elapsed_clock(),
        lease_seconds=3,
    )

    async def run() -> tuple[StructureWorkerResult, bool]:
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        assert await asyncio.to_thread(parse_started.wait, 2)
        renewed = False
        for _ in range(300):
            if len(control_plane.heartbeats) >= 2:
                renewed = True
                break
            await asyncio.sleep(0.01)
        release_parse.set()
        return await task, renewed

    result, renewed = asyncio.run(run())

    assert renewed, "v3 parsing must remain inside the lease-renewing call boundary"
    assert result.outcome == "certified"
    assert control_plane.certified


def test_structure_certifier_resumes_1117_ranges_after_durable_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        structure_worker_module,
        "_run_bounded_sync_call",
        lambda call, **_kwargs: call(),
    )
    identity = StructureBundleIdentity(
        publication_id="source-window:resume",
        window_id="resume",
        snapshot_id=0,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1117,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
        source_kind="gamma-source-window-events-v3-sharded",
    )
    shards = tuple(
        StructureShardArtifact.from_bytes(
            canonical_structure_shard_bytes(
                window_key="resume",
                source_digest="a" * 64,
                component="events",
                ordinal=index,
                rows=({"id": f"event-{index:04d}"},),
            )
        )
        for index in range(1117)
    )
    shard_receipts = tuple(
        StructureShardReceipt("events", index, shard.key, shard.sha256, 1)
        for index, shard in enumerate(shards)
    )
    manifest = StructureBundleArtifact.from_bytes(
        canonical_structure_shard_manifest_bytes(identity=identity, shards=shard_receipts)
    )
    ranges: list[tuple[StructureRangeSpec, StructureRangeReceipt]] = []
    payloads = {manifest.key: manifest.payload, **{shard.key: shard.payload for shard in shards}}
    for index, shard in enumerate(shards):
        spec = StructureRangeSpec.create(
            bundle_key=manifest.key,
            bundle_digest=manifest.sha256,
            component="events",
            ordinal=index,
            range_start=f"shard:{index:08d}",
            range_end=f"shard:{index + 1:08d}",
        )
        artifact = StructureRangeArtifact.from_bytes(
            canonical_structure_range_bytes(
                bundle_digest=manifest.sha256,
                component="events",
                range_digest=spec.range_digest,
                rows=({"id": f"event-{index:04d}"},),
            )
        )
        payloads[artifact.key] = artifact.payload
        ranges.append(
            (
                spec,
                StructureRangeReceipt(
                    job_key=spec.job_key,
                    bundle_digest=spec.bundle_digest,
                    component=spec.component,
                    range_digest=spec.range_digest,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    record_count=1,
                ),
            )
        )

    class ControlPlane:
        def __init__(self) -> None:
            self.claims = 0
            self.checkpoints: list[tuple[str, str, str, str]] = []
            self.certified = False

        def claim_job(self, **kwargs: object) -> JobLease:
            self.claims += 1
            latest = self.checkpoints[-1] if self.checkpoints else None
            return JobLease(
                job_key=manifest.key.replace("structure-manifests/", "structure:").replace(
                    "/manifest.ndjson", ":certify"
                ),
                job_type="structure-certify",
                input_identity="structure:" + manifest.sha256,
                lease_owner="certifier-a",
                lease_epoch=self.claims,
                lease_expires_at=NOW + timedelta(seconds=30),
                checkpoint_cursor=None if latest is None else latest[0],
                checkpoint_digest=None if latest is None else latest[1],
            )

        def structure_generation_receipts(self, generation_key: str):
            return tuple(ranges)

        def running_checkpoints(self, job_key: str):
            return tuple(record[:3] for record in self.checkpoints)

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

        def structure_manifest_payload(self, generation_key: str) -> bytes:
            return b'{"kind":"structure-manifest"}\n'

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            return None

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            return lease

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            return None

        def certify_structure_generation(self, lease: JobLease, **kwargs: object) -> str:
            self.certified = True
            return str(kwargs["artifact_digest"])

        def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
            return False

    class Objects:
        def __init__(self) -> None:
            self.failed = False
            self.read_counts: dict[str, int] = {}
            self.upload: dict[str, object] = {}

        def get_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            self.read_counts[key] = self.read_counts.get(key, 0) + 1
            if key == shards[300].key and not self.failed:
                self.failed = True
                raise TimeoutError("simulated parity interruption")
            return {"Body": _Body(payloads[key])}

        def put_object(self, **kwargs: object) -> None:
            self.upload = kwargs

        def head_object(self, **kwargs: object) -> dict[str, object]:
            return {
                "ContentLength": len(self.upload["Body"]),
                "Metadata": self.upload["Metadata"],
            }

    control_plane = ControlPlane()
    objects = Objects()
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
    )

    with pytest.raises(TimeoutError, match="parity interruption"):
        certifier.run_once()
    assert control_plane.checkpoints[-1][0].endswith(":300")

    assert certifier.run_once().outcome == "certified"
    assert all(objects.read_counts[shard.key] == 1 for shard in shards[:300])
    assert objects.read_counts[shards[300].key] == 2
    assert all(objects.read_counts[shard.key] == 1 for shard in shards[301:])


def test_structure_certifier_waits_for_missing_range_receipts_without_incident() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.finished: list[dict[str, object]] = []

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "a" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "a" * 64,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, generation_key: str):
            raise IncompleteStructureGenerationError("range receipts pending")

        def finish(self, lease: JobLease, **kwargs: object) -> None:
            self.finished.append(kwargs)

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            raise AssertionError("incomplete generation is ordinary waiting, not an incident")

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            return None

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            return lease

    certifier = TransactionalStructureCertifier(
        control_plane=ControlPlane(),
        object_client=object(),
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
    )

    assert certifier.run_once().outcome == "waiting"
    assert certifier._control_plane.finished == [
        {
            "state": JobState.WAITING,
            "now": NOW,
        }
    ]


def test_structure_certifier_quarantines_parity_mismatch_and_invalidates_qualification() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.quarantined: list[dict[str, object]] = []

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "b" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "b" * 64,
                lease_owner="certifier-a",
                lease_epoch=4,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, generation_key: str):
            raise StructureParityMismatchError("component-count parity failed")

        def finish_quarantined_with_incident(
            self, lease: JobLease, **kwargs: object
        ) -> None:
            self.quarantined.append(kwargs)

        def finish(self, lease: JobLease, **kwargs: object) -> None:
            raise AssertionError("a proved parity mismatch must not return to waiting")

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            raise AssertionError("immutable parity mismatch must not enter the retry circuit")

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            return None

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            return lease

    control_plane = ControlPlane()
    certifier = TransactionalStructureCertifier(
        control_plane=control_plane,
        object_client=object(),
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
    )

    assert certifier.run_once().outcome == "quarantined"
    assert control_plane.quarantined == [
        {
            "error_class": "StructureParityMismatchError",
            "incident_key": "incident:integrity-conflict:structure:" + "b" * 64 + ":certify",
            "dedupe_key": "integrity-conflict:structure:" + "b" * 64 + ":certify",
            "component": "structure-certify",
            "summary": "structure-certify parity mismatch quarantined",
            "detail": {
                "job_key": "structure:" + "b" * 64 + ":certify",
                "lease_epoch": 4,
                "generation_key": "structure:" + "b" * 64,
                "reason_code": "integrity.conflict",
            },
            "channels": ("dashboard",),
            "qualification_impact": "invalidated",
            "reason_code": "integrity.conflict",
            "now": NOW,
        }
    ]


def test_structure_certifier_refuses_range_content_that_does_not_reassemble_bundle() -> None:
    bundle = _bundle()
    spec = StructureRangeSpec.create(
        bundle_key=bundle.key,
        bundle_digest=bundle.sha256,
        component="events",
        ordinal=0,
        range_start="",
        range_end="",
    )
    bad_range = StructureRangeArtifact.from_bytes(
        canonical_structure_range_bytes(
            bundle_digest=bundle.sha256,
            component="events",
            range_digest=spec.range_digest,
            rows=({"id": "event-a"},),
        )
    )

    class ControlPlane:
        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key=spec.generation_key + ":certify",
                job_type="structure-certify",
                input_identity=spec.generation_key,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW + timedelta(seconds=int(kwargs["lease_seconds"])),
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, generation_key: str):
            assert generation_key == spec.generation_key
            return (
                (
                    spec,
                    type(
                        "Receipt",
                        (),
                        {
                            "artifact_key": bad_range.key,
                            "artifact_digest": bad_range.sha256,
                            "record_count": 1,
                        },
                    )(),
                ),
            )

        def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
            assert state is JobState.RETRYABLE

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            assert kwargs["component"] == "structure-certify"
            assert kwargs["error_class"] == "StructureWorkerError"

        def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
            return None

        def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
            return lease

    class ObjectClient:
        def get_object(self, **kwargs: object) -> dict[str, object]:
            payload = bundle.payload if kwargs["Key"] == bundle.key else bad_range.payload
            return {"Body": _Body(payload)}

        def put_object(self, **kwargs: object) -> None:
            raise AssertionError("must not write manifest after failed parity")

        def head_object(self, **kwargs: object) -> dict[str, object]:
            raise AssertionError("must not head after failed parity")

    certifier = TransactionalStructureCertifier(
        control_plane=ControlPlane(),
        object_client=ObjectClient(),
        bucket="structure",
        worker_id="certifier-a",
        now=lambda: NOW,
    )

    with pytest.raises(StructureWorkerError, match="content-parity"):
        certifier.run_once()
