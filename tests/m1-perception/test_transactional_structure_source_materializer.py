from __future__ import annotations

import asyncio
import threading
import time
from datetime import UTC, datetime

from polyarb.control_plane.models import JobLease, StructureSourcePageSpec
from polyarb.control_plane.structure_artifact import (
    StructureShardBatchArtifact,
    StructureShardReceipt,
    canonical_structure_shard_batch_bytes,
)
from polyarb.control_plane.structure_source import (
    StructureSourcePageArtifact,
    TransactionalStructureSourceMaterializer,
    materialize_event_page_shards,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)

type PageArtifact = tuple[StructureSourcePageSpec, StructureSourcePageArtifact]


def _artifact(
    *, stream: str, records: tuple[dict[str, object], ...], ordinal: int = 0
) -> tuple[StructureSourcePageSpec, StructureSourcePageArtifact]:
    spec = StructureSourcePageSpec(
        window_key="source-window:materializer",
        stream=stream,
        ordinal=ordinal,
        requested_cursor=None,
    )
    return (
        spec,
        StructureSourcePageArtifact.from_page(
            spec=spec,
            records=records,
            next_cursor=None,
            completed=True,
            started_at_ms=1,
            finished_at_ms=2,
        ),
    )


class FakeControlPlane:
    def __init__(self, pages: tuple[PageArtifact, ...]) -> None:
        self.pages = pages
        self.admitted: dict[str, object] | None = None
        self.retry_incidents: list[dict[str, object]] = []
        self.recoveries: list[dict[str, object]] = []
        self.checkpoints: list[dict[str, object]] = []
        self.runtime_progress: list[dict[str, object]] = []
        self.runtime_heartbeats: list[dict[str, object]] = []

    def claim_job(self, **kwargs: object) -> JobLease:
        assert kwargs["job_types"] == ("structure-materialize",)
        return JobLease(
            job_key="source-window:materializer:materialize",
            job_type="structure-materialize",
            input_identity="source-window:materializer",
            lease_owner="materializer-a",
            lease_epoch=1,
            lease_expires_at=NOW,
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def structure_source_window_pages(self, window_key: str):
        assert window_key == "source-window:materializer"
        return tuple((spec, artifact.key, artifact.sha256) for spec, artifact in self.pages)

    def structure_source_window_digest(self, window_key: str) -> str:
        assert window_key == "source-window:materializer"
        return "a" * 64

    def checkpoint(self, _lease: JobLease, **kwargs: object) -> None:
        self.checkpoints.append(kwargs)

    def admit_structure_source_bundle(self, lease: JobLease, **kwargs: object):
        self.admitted = kwargs
        return ()

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        self.recoveries.append(kwargs)
        return False

    def record_runtime_progress(self, lease: JobLease, **kwargs: object) -> None:
        self.runtime_progress.append(kwargs)

    def heartbeat_runtime_attempt(self, lease: JobLease, **kwargs: object) -> JobLease:
        self.runtime_heartbeats.append(kwargs)
        return JobLease(
            job_key=lease.job_key,
            job_type=lease.job_type,
            input_identity=lease.input_identity,
            lease_owner=lease.lease_owner,
            lease_epoch=lease.lease_epoch,
            lease_expires_at=kwargs["now"],
            checkpoint_cursor=lease.checkpoint_cursor,
            checkpoint_digest=lease.checkpoint_digest,
        )


def test_final_shard_manifest_recovery_failure_is_not_retried_after_admission() -> None:
    pages = (
        _artifact(
            stream="events",
            ordinal=0,
            records=(
                {
                    "id": "event-a",
                    "slug": "event-a",
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                    "markets": [],
                },
            ),
        ),
        _artifact(
            stream="events",
            ordinal=1,
            records=(
                {
                    "id": "event-b",
                    "slug": "event-b",
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                    "markets": [],
                },
            ),
        ),
    )
    source_digest = "a" * 64
    shard_receipts: list[StructureShardReceipt] = []
    objects = MemoryR2(pages)
    for page in pages:
        for component, artifact in materialize_event_page_shards(
            page, source_digest=source_digest
        ):
            objects.objects[artifact.key] = artifact.payload
            shard_receipts.append(
                StructureShardReceipt(
                    component=component,
                    ordinal=page[0].ordinal,
                    artifact_key=artifact.key,
                    artifact_digest=artifact.sha256,
                    row_count=1,
                )
            )
    batch = StructureShardBatchArtifact.from_bytes(
        canonical_structure_shard_batch_bytes(
            window_key="source-window:materializer",
            source_digest=source_digest,
            start_ordinal=0,
            end_ordinal=2,
            shards=shard_receipts,
        )
    )
    objects.objects[batch.key] = batch.payload

    class FinalizingControlPlane(FakeControlPlane):
        def claim_job(self, **kwargs: object) -> JobLease:
            lease = super().claim_job(**kwargs)
            return JobLease(
                job_key=lease.job_key,
                job_type=lease.job_type,
                input_identity=lease.input_identity,
                lease_owner=lease.lease_owner,
                lease_epoch=lease.lease_epoch,
                lease_expires_at=lease.lease_expires_at,
                checkpoint_cursor="shard-batch:00000001",
                checkpoint_digest=batch.sha256,
            )

        def structure_materializer_batches(self, window_key: str):
            assert window_key == "source-window:materializer"
            return (("shard-batch:00000000", batch.sha256, batch.key),)

        def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
            raise RuntimeError("recovery delivery unavailable after admission")

    control_plane = FinalizingControlPlane(pages)
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=objects,
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
        shard_page_batch_size=1,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded:recovery-pending"
    assert control_plane.admitted is not None
    assert control_plane.retry_incidents == []


class MemoryR2:
    def __init__(self, pages: tuple[PageArtifact, ...]) -> None:
        self.objects = {artifact.key: artifact.payload for _spec, artifact in pages}
        self.metadata: dict[str, dict[str, str]] = {}

    def get_object(self, **kwargs: object):
        payload = self.objects[str(kwargs["Key"])]
        return {"Body": type("Body", (), {"read": lambda _self: payload})()}

    def put_object(self, **kwargs: object) -> None:
        key = str(kwargs["Key"])
        self.objects[key] = bytes(kwargs["Body"])
        self.metadata[key] = dict(kwargs["Metadata"])

    def head_object(self, **kwargs: object):
        key = str(kwargs["Key"])
        return {"ContentLength": len(self.objects[key]), "Metadata": self.metadata[key]}


class ConcurrentReadMemoryR2(MemoryR2):
    def __init__(self, pages: tuple[PageArtifact, ...]) -> None:
        super().__init__(pages)
        self._lock = threading.Lock()
        self.active_reads = 0
        self.max_active_reads = 0

    def get_object(self, **kwargs: object):
        with self._lock:
            self.active_reads += 1
            self.max_active_reads = max(self.max_active_reads, self.active_reads)
        try:
            time.sleep(0.02)
            return super().get_object(**kwargs)
        finally:
            with self._lock:
                self.active_reads -= 1


def test_materializer_reads_only_sealed_r2_pages_then_admits_ranges() -> None:
    pages = (
        _artifact(
            stream="events",
            records=(
                {
                    "id": "event-a",
                    "slug": "event-a",
                    "active": True,
                    "closed": False,
                    "markets": [
                        {
                            "id": "market-a",
                            "active": True,
                            "closed": False,
                            "negRiskOther": False,
                        }
                    ],
                },
            ),
        ),
        _artifact(
            stream="markets",
            records=(
                {
                    "id": "market-a",
                    "conditionId": "condition-a",
                    "clobTokenIds": '["yes-a", "no-a"]',
                    "outcomePrices": '["0.4", "0.6"]',
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                },
            ),
        ),
    )
    control_plane = FakeControlPlane(pages)
    objects = MemoryR2(pages)
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=objects,
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert control_plane.admitted is not None
    assert control_plane.admitted["bundle"].key in objects.objects
    assert control_plane.admitted["ranges"] == (
        ("events", "", ""),
        ("event_tags", "", ""),
        ("memberships", "", ""),
        ("group_truth", "", ""),
        ("markets", "", ""),
        ("issues", "", ""),
    )


def test_materializer_reports_all_fenced_bundle_lifecycle_stages() -> None:
    pages = (
        _artifact(
            stream="events",
            records=(
                {
                    "id": "event-a",
                    "slug": "event-a",
                    "active": True,
                    "closed": False,
                    "markets": [],
                },
            ),
        ),
    )
    control_plane = FakeControlPlane(pages)
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=MemoryR2(pages),
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert [item["progress"].stage for item in control_plane.runtime_progress] == [
        "read-page-receipts",
        "build-bundle",
        "upload-bundle",
        "commit-bundle",
    ]


def test_materializer_reads_sealed_pages_concurrently_in_stable_source_order() -> None:
    pages = (
        _artifact(
            stream="events",
            records=(
                {
                    "id": "event-a",
                    "slug": "event-a",
                    "active": True,
                    "closed": False,
                    "markets": [
                        {
                            "id": "market-a",
                            "active": True,
                            "closed": False,
                            "negRiskOther": False,
                        }
                    ],
                },
            ),
        ),
        _artifact(
            stream="markets",
            records=(
                {
                    "id": "market-a",
                    "conditionId": "condition-a",
                    "clobTokenIds": '["yes-a", "no-a"]',
                    "outcomePrices": '["0.4", "0.6"]',
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                },
            ),
        ),
    )
    control_plane = FakeControlPlane(pages)
    objects = ConcurrentReadMemoryR2(pages)
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=objects,
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert objects.max_active_reads == 2


def test_materializer_records_retry_incident_when_sealed_page_is_unavailable() -> None:
    control_plane = FakeControlPlane((_artifact(stream="events", records=()),))
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=MemoryR2(()),
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
    )

    assert asyncio.run(worker.run_once()).outcome == "retryable"

    assert control_plane.retry_incidents[0]["component"] == "structure-materialize"
    assert control_plane.retry_incidents[0]["detail"]["lease_epoch"] == 1
    assert isinstance(control_plane.retry_incidents[0]["detail"]["error_message"], str)


def test_event_only_materializer_checkpoints_one_bounded_shard_batch() -> None:
    pages = (
        _artifact(
            stream="events",
            ordinal=0,
            records=(
                {
                    "id": "event-a",
                    "slug": "event-a",
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                    "markets": [],
                },
            ),
        ),
        _artifact(
            stream="events",
            ordinal=1,
            records=(
                {
                    "id": "event-b",
                    "slug": "event-b",
                    "active": True,
                    "closed": False,
                    "negRisk": False,
                    "markets": [],
                },
            ),
        ),
    )
    control_plane = FakeControlPlane(pages)
    objects = MemoryR2(pages)
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=objects,
        bucket="source-pages",
        worker_id="materializer-a",
        now=lambda: NOW,
        range_max_rows=100,
        shard_page_batch_size=1,
    )

    assert asyncio.run(worker.run_once()).outcome == "checkpointed"
    assert control_plane.admitted is None
    assert control_plane.checkpoints[0]["checkpoint_cursor"] == "shard-batch:00000000"
    assert str(control_plane.checkpoints[0]["artifact_key"]).startswith("structure-shard-batches/")
    assert control_plane.recoveries[0]["component"] == "structure-materialize"
