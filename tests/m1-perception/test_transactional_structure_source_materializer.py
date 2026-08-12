from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from polyarb.control_plane.models import JobLease, StructureSourcePageSpec
from polyarb.control_plane.structure_source import (
    StructureSourcePageArtifact,
    TransactionalStructureSourceMaterializer,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)

type PageArtifact = tuple[StructureSourcePageSpec, StructureSourcePageArtifact]


def _artifact(
    *, stream: str, records: tuple[dict[str, object], ...]
) -> tuple[StructureSourcePageSpec, StructureSourcePageArtifact]:
    spec = StructureSourcePageSpec(
        window_key="source-window:materializer",
        stream=stream,
        ordinal=0,
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

    def admit_structure_source_bundle(self, lease: JobLease, **kwargs: object):
        self.admitted = kwargs
        return ()

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)


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

    with pytest.raises(KeyError):
        asyncio.run(worker.run_once())

    assert control_plane.retry_incidents[0]["component"] == "structure-materialize"
    assert control_plane.retry_incidents[0]["detail"]["lease_epoch"] == 1
