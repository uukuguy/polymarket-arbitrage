from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from polyarb.control_plane.models import JobLease, JobState, StructureRangeSpec
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_bundle_bytes,
)
from polyarb.control_plane.structure_worker import TransactionalStructureWorker

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
