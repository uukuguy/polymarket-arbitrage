from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.control_plane.models import JobLease, JobState, StructureSourcePageSpec
from polyarb.control_plane.structure_source import (
    TransactionalStructureSourceAdmitter,
    TransactionalStructureSourceWorker,
)
from polyarb.control_plane.structure_worker import StructureWorkerResult

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class FakeControlPlane:
    def __init__(self, spec: StructureSourcePageSpec) -> None:
        self.spec = spec
        self.recorded: dict[str, object] | None = None
        self.finished: list[JobState] = []
        self.retry_incidents: list[dict[str, object]] = []

    def claim_job(self, **kwargs: object) -> JobLease:
        assert kwargs["job_types"] == ("structure-fetch",)
        return JobLease(
            job_key=self.spec.job_key,
            job_type="structure-fetch",
            input_identity=self.spec.input_identity,
            lease_owner="source-worker-a",
            lease_epoch=1,
            lease_expires_at=NOW,
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def structure_source_page_spec(self, job_key: str) -> StructureSourcePageSpec:
        assert job_key == self.spec.job_key
        return self.spec

    def record_structure_source_page(self, lease: JobLease, **kwargs: object):
        assert lease.job_key == self.spec.job_key
        self.recorded = kwargs
        return None

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)


class FakeGamma:
    def __init__(self) -> None:
        self.event_calls: list[tuple[str | None, int]] = []
        self.market_calls: list[tuple[str | None, int]] = []

    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
        self.event_calls.append((cursor, limit))
        return EventPage(
            events=({"id": "event-a", "markets": []},),
            requested_cursor=cursor,
            next_cursor="event-next",
            completed=False,
            started_at_ms=10,
            finished_at_ms=20,
        )

    async def fetch_active_market_page(self, cursor: str | None, limit: int) -> MarketPage:
        self.market_calls.append((cursor, limit))
        return MarketPage(
            markets=({"id": "market-a"},),
            requested_cursor=cursor,
            next_cursor=None,
            completed=True,
            started_at_ms=30,
            finished_at_ms=40,
        )


class FailingGamma(FakeGamma):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
        raise TimeoutError("Gamma unavailable")


class FakeObjectClient:
    def __init__(self) -> None:
        self.upload: dict[str, object] = {}

    def put_object(self, **kwargs: object) -> None:
        self.upload = kwargs

    def head_object(self, **kwargs: object) -> dict[str, object]:
        return {
            "ContentLength": len(self.upload["Body"]),
            "Metadata": self.upload["Metadata"],
        }


def _event_spec() -> StructureSourcePageSpec:
    return StructureSourcePageSpec(
        window_key="source-window:one",
        stream="events",
        ordinal=0,
        requested_cursor=None,
    )


def test_source_admitter_creates_one_current_window_and_never_overlaps() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.calls: list[tuple[int, datetime]] = []

        def admit_due_structure_source_window(self, *, cadence_seconds: int, now: datetime):
            self.calls.append((cadence_seconds, now))
            if len(self.calls) == 1:
                return StructureSourcePageSpec(
                    window_key="structure-source:123",
                    stream="events",
                    ordinal=0,
                    requested_cursor=None,
                )
            return None

    now = datetime(2026, 8, 12, tzinfo=UTC)
    worker = TransactionalStructureSourceAdmitter(
        control_plane=ControlPlane(), cadence_seconds=300, now=lambda: now
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key="structure-source:123:fetch:events:0", outcome="admitted"
    )
    assert asyncio.run(worker.run_once()) == StructureWorkerResult(job_key=None, outcome="idle")


def test_source_worker_fetches_one_event_page_uploads_then_records_receipt() -> None:
    control_plane = FakeControlPlane(_event_spec())
    gamma = FakeGamma()
    objects = FakeObjectClient()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=objects,
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert gamma.event_calls == [(None, 100)]
    assert gamma.market_calls == []
    assert control_plane.recorded is not None
    assert control_plane.recorded["artifact_key"] == objects.upload["Key"]
    assert control_plane.recorded["artifact_digest"] == objects.upload["Metadata"]["sha256"]
    assert control_plane.recorded["next_cursor"] == "event-next"
    assert control_plane.recorded["completed"] is False
    assert control_plane.recorded["record_count"] == 1
    records = [json.loads(line) for line in objects.upload["Body"].splitlines()]
    assert records[0]["kind"] == "structure-source-page"
    assert records[0]["stream"] == "events"
    assert records[1] == {"row": {"id": "event-a", "markets": []}}


def test_source_worker_marks_only_current_page_retryable_when_gamma_fails() -> None:
    control_plane = FakeControlPlane(_event_spec())
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=FailingGamma(),
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    with pytest.raises(TimeoutError, match="Gamma unavailable"):
        asyncio.run(worker.run_once())

    assert control_plane.recorded is None
    assert control_plane.finished == []
    assert control_plane.retry_incidents == [
        {
            "next_attempt_at": NOW + timedelta(seconds=15),
            "error_class": "TimeoutError",
            "incident_key": "incident:job-retry:source-window:one:fetch:events:0",
            "dedupe_key": "job-retry:source-window:one:fetch:events:0",
            "component": "structure-fetch",
            "summary": "structure-fetch retryable failure",
            "detail": {
                "job_key": "source-window:one:fetch:events:0",
                "lease_epoch": 1,
                "error_class": "TimeoutError",
            },
            "channels": ("dashboard",),
            "now": NOW,
        }
    ]
