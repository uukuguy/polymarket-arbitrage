from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import pytest

from polyarb.clients.gamma_client import EventPage, MarketPage
from polyarb.control_plane.models import JobLease, JobState, StructureSourcePageSpec
from polyarb.control_plane.structure_source import (
    DEFAULT_MAX_MARKET_BATCHES,
    StructureSourcePageArtifact,
    TransactionalStructureSourceAdmitter,
    TransactionalStructureSourcePool,
    TransactionalStructureSourceWorker,
    parse_structure_source_page_bytes,
)
from polyarb.control_plane.structure_worker import StructureWorkerResult

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class FakeControlPlane:
    def __init__(self, spec: StructureSourcePageSpec) -> None:
        self.spec = spec
        self.recorded: dict[str, object] | None = None
        self.finished: list[JobState] = []
        self.quarantines: list[dict[str, object]] = []
        self.retry_incidents: list[dict[str, object]] = []
        self.recoveries: list[dict[str, object]] = []

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

    def structure_source_event_pages(self, window_key: str):
        assert window_key == self.spec.window_key
        return ()

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def quarantine_structure_source_page(self, lease: JobLease, **kwargs: object) -> None:
        self.quarantines.append(kwargs)

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)

    def record_job_recovery(self, lease: JobLease, **kwargs: object) -> bool:
        self.recoveries.append(kwargs)
        return False


class FakeGamma:
    def __init__(self) -> None:
        self.event_calls: list[tuple[str | None, int]] = []
        self.market_calls: list[tuple[str | None, int]] = []
        self.exact_market_calls: list[tuple[str, ...]] = []

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

    async def fetch_markets_by_ids(self, market_ids: tuple[str, ...]) -> tuple[dict, ...]:
        self.exact_market_calls.append(market_ids)
        return tuple(
            {"id": market_id, "active": True, "closed": False, "archived": False}
            for market_id in market_ids
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


class DelayedLane:
    def __init__(self, job_key: str | None, *, failure: BaseException | None = None) -> None:
        self.job_key = job_key
        self.failure = failure
        self.calls = 0
        self.released = asyncio.Event()

    async def run_once(self) -> StructureWorkerResult:
        self.calls += 1
        await asyncio.sleep(0)
        self.released.set()
        if self.failure is not None:
            raise self.failure
        return StructureWorkerResult(
            job_key=self.job_key,
            outcome="idle" if self.job_key is None else "succeeded",
        )

    async def aclose(self) -> None:
        return None


def _event_spec() -> StructureSourcePageSpec:
    return StructureSourcePageSpec(
        window_key="source-window:one",
        stream="events",
        ordinal=0,
        requested_cursor=None,
    )


def test_source_worker_quarantines_page_at_configured_page_limit() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:limit",
        stream="markets",
        ordinal=2,
        requested_cursor="opaque-cursor",
    )
    control_plane = FakeControlPlane(spec)
    gamma = FakeGamma()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=FakeObjectClient(),
        bucket="structure",
        worker_id="source-worker-a",
        now=lambda: NOW,
        max_pages=2,
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key=spec.job_key, outcome="quarantined"
    )
    assert control_plane.quarantines == [
        {"error_class": "StructureSourcePageLimitError", "now": NOW}
    ]
    assert gamma.event_calls == []
    assert gamma.market_calls == []


def test_scoped_market_artifact_binds_the_admitted_batch_digest() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:scoped",
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a", "market-b"),
    )
    artifact = StructureSourcePageArtifact.from_page(
        spec=spec,
        records=({"id": "market-a"}, {"id": "market-b"}),
        next_cursor=None,
        completed=True,
        started_at_ms=10,
        finished_at_ms=20,
    )

    parsed, _, _, _ = parse_structure_source_page_bytes(
        artifact.payload, expected_sha256=artifact.sha256
    )

    assert parsed == spec
    header = json.loads(artifact.payload.splitlines()[0])
    assert header["market_ids_digest"] == spec.market_ids_digest


def test_source_worker_fetches_scoped_market_batch_by_exact_ids() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:scoped-fetch",
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a", "market-b"),
    )
    control_plane = FakeControlPlane(spec)
    gamma = FakeGamma()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert gamma.exact_market_calls == [("market-a", "market-b")]
    assert gamma.market_calls == []
    assert control_plane.recorded is not None
    assert control_plane.recorded["next_cursor"] is None
    assert control_plane.recorded["completed"] is True


def test_terminal_event_worker_derives_and_commits_scoped_market_batches() -> None:
    class TerminalEventGamma(FakeGamma):
        async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
            self.event_calls.append((cursor, limit))
            return EventPage(
                events=(
                    {
                        "id": "event-a",
                        "markets": [
                            {"id": "market-b", "active": True, "closed": False},
                            {"id": "market-a", "active": True, "closed": False},
                        ],
                    },
                ),
                requested_cursor=cursor,
                next_cursor=None,
                completed=True,
                started_at_ms=10,
                finished_at_ms=20,
            )

    control_plane = FakeControlPlane(_event_spec())
    gamma = TerminalEventGamma()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
        market_batch_size=1,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert control_plane.recorded is not None
    assert control_plane.recorded["market_batches"] == (("market-a",), ("market-b",))


def test_default_scoped_market_capacity_remains_hard_but_covers_live_universe() -> None:
    assert DEFAULT_MAX_MARKET_BATCHES == 5_000


def test_source_pool_aggregates_all_concurrent_lane_results() -> None:
    lanes = (DelayedLane("market:2"), DelayedLane("market:1"), DelayedLane(None))

    result = asyncio.run(TransactionalStructureSourcePool(lanes=lanes).run_once())

    assert result == StructureWorkerResult(job_key="market:1,market:2", outcome="succeeded:2/3")
    assert [lane.calls for lane in lanes] == [1, 1, 1]


def test_source_pool_waits_for_healthy_sibling_before_propagating_lane_failure() -> None:
    failing = DelayedLane(None, failure=TimeoutError("Gamma unavailable"))
    healthy = DelayedLane("market:1")

    with pytest.raises(TimeoutError, match="Gamma unavailable"):
        asyncio.run(TransactionalStructureSourcePool(lanes=(failing, healthy)).run_once())

    assert healthy.released.is_set()


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
    assert control_plane.recoveries == [
        {"component": "structure-fetch", "channels": ("dashboard",), "now": NOW}
    ]
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
