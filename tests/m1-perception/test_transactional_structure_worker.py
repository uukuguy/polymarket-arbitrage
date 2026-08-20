from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from polyarb.control_plane.faults import IntentionalStagingRetryFault
from polyarb.control_plane.models import JobLease, JobState, StructureRangeSpec
from polyarb.control_plane.postgres import IncompleteStructureGenerationError
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
    StructureWorkerError,
    TransactionalStructureCertifier,
    TransactionalStructureWorker,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


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


class FakeControlPlane:
    def __init__(self, spec: StructureRangeSpec, *, prior: object | None = None) -> None:
        self.spec = spec
        self.prior = prior
        self.finished: list[JobState] = []
        self.recorded: dict[str, object] | None = None
        self.recoveries: list[dict[str, object]] = []
        self.retry_incidents: list[dict[str, object]] = []

    def claim_job(self, **kwargs: object) -> JobLease:
        return JobLease(
            job_key=self.spec.job_key,
            job_type="structure-normalize",
            input_identity=self.spec.input_identity,
            lease_owner="worker-a",
            lease_epoch=1,
            lease_expires_at=NOW,
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

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        self.recoveries.append(kwargs)
        return False

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)


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
    assert control_plane.recorded is not None
    assert control_plane.recorded["record_count"] == 1
    assert control_plane.recorded["artifact_key"] == objects.upload["Key"]
    assert control_plane.recorded["artifact_digest"] == objects.upload["Metadata"]["sha256"]
    assert b'"id":"event-a"' in objects.upload["Body"]
    assert b'"id":"event-b"' not in objects.upload["Body"]
    assert control_plane.finished == [JobState.SUCCEEDED]


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
    control_plane = FakeControlPlane(_spec(bundle), prior=object())
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
    assert control_plane.finished == [JobState.SUCCEEDED]


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

        def claim_job(self, **kwargs: object) -> JobLease:
            return JobLease(
                job_key="structure:" + "a" * 64 + ":certify",
                job_type="structure-certify",
                input_identity="structure:" + "a" * 64,
                lease_owner="certifier-a",
                lease_epoch=1,
                lease_expires_at=NOW,
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
                lease_expires_at=NOW,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def structure_generation_receipts(self, generation_key: str):
            raise IncompleteStructureGenerationError("range receipts pending")

        def finish(self, lease: JobLease, **kwargs: object) -> None:
            self.finished.append(kwargs)

        def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
            raise AssertionError("incomplete generation is ordinary waiting, not an incident")

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
                lease_expires_at=NOW,
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
