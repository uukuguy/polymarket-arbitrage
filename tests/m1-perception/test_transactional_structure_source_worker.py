from __future__ import annotations

import asyncio
import json
import time
from datetime import UTC, datetime

import pytest

from polyarb.clients.gamma_client import EventPage, MarketPage, PaginationIntegrityError
from polyarb.control_plane.failure_identity import retry_failure_fingerprint
from polyarb.control_plane.models import (
    CloudUsageDecision,
    JobLease,
    JobState,
    SourceAdmissionDecision,
    StructureSourcePageSpec,
)
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
    def __init__(self, spec: StructureSourcePageSpec, *, lease_epoch: int = 1) -> None:
        self.spec = spec
        self.lease_epoch = lease_epoch
        self.recorded: dict[str, object] | None = None
        self.finished: list[JobState] = []
        self.quarantines: list[dict[str, object]] = []
        self.retry_incidents: list[dict[str, object]] = []
        self.interruptions: list[dict[str, object]] = []
        self.recoveries: list[dict[str, object]] = []
        self.runtime_progress: list[dict[str, object]] = []
        self.runtime_heartbeats: list[dict[str, object]] = []
        self.usage_decision = CloudUsageDecision(True, 0, 0, "usage-1")

    def claim_job(self, **kwargs: object) -> JobLease:
        assert kwargs["job_types"] == ("structure-fetch",)
        return JobLease(
            job_key=self.spec.job_key,
            job_type="structure-fetch",
            input_identity=self.spec.input_identity,
            lease_owner="source-worker-a",
            lease_epoch=self.lease_epoch,
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

    def record_cloud_usage(self, **kwargs: object) -> CloudUsageDecision:
        self.cloud_usage = kwargs
        return self.usage_decision

    def structure_source_event_pages(self, window_key: str):
        assert window_key == self.spec.window_key
        return ()

    def finish(self, lease: JobLease, *, state: JobState, **kwargs: object) -> None:
        self.finished.append(state)

    def quarantine_structure_source_page(self, lease: JobLease, **kwargs: object) -> None:
        self.quarantines.append(kwargs)

    def finish_retryable_with_incident(self, lease: JobLease, **kwargs: object) -> None:
        self.retry_incidents.append(kwargs)

    def finish_interrupted(self, lease: JobLease, **kwargs: object) -> None:
        self.interruptions.append(kwargs)

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


class FakeGamma:
    def __init__(self) -> None:
        self.event_calls: list[tuple[str | None, int]] = []
        self.market_calls: list[tuple[str | None, int]] = []
        self.exact_market_calls: list[tuple[str, ...]] = []
        self.reset_calls = 0

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

    async def reset_transport(self) -> None:
        self.reset_calls += 1


class FailingGamma(FakeGamma):
    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
        self.failure = TimeoutError("Gamma unavailable")
        raise self.failure


class ResetFailingGamma(FailingGamma):
    async def reset_transport(self) -> None:
        self.reset_calls += 1
        raise ValueError("replacement close failed")


class ClosedExactMarketGamma(FakeGamma):
    async def fetch_markets_by_ids(self, market_ids: tuple[str, ...]) -> tuple[dict, ...]:
        raise PaginationIntegrityError("exact-id market response is not open")


class MismatchedExactMarketGamma(FakeGamma):
    async def fetch_markets_by_ids(self, market_ids: tuple[str, ...]) -> tuple[dict, ...]:
        raise PaginationIntegrityError("exact-id market response identity set mismatch")


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
        stream="events",
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


def test_scoped_market_batch_ordinal_is_bounded_by_market_capacity_not_cursor_pages() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:scoped-capacity",
        stream="markets",
        ordinal=1_000,
        requested_cursor=None,
        market_ids=("market-a",),
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
        max_pages=1_000,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert control_plane.quarantines == []
    assert gamma.exact_market_calls == [("market-a",)]


def test_source_worker_refuses_receipt_when_cloud_budget_is_exhausted() -> None:
    spec = _event_spec()
    control_plane = FakeControlPlane(spec)
    control_plane.usage_decision = CloudUsageDecision(False, 90, 90, "usage-1")
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=FakeGamma(),
        object_client=FakeObjectClient(),
        bucket="structure",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )
    assert asyncio.run(worker.run_once()).outcome == "retryable"
    assert control_plane.recorded is None


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


def test_terminal_event_worker_commits_event_embedded_market_completion() -> None:
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
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert control_plane.recorded is not None
    assert control_plane.recorded["event_embedded_markets"] is True


def test_terminal_event_does_not_rebuild_a_mutable_market_batch_scope() -> None:
    class TerminalEventGamma(FakeGamma):
        async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
            return EventPage(
                events=({"id": "event-a", "markets": []},),
                requested_cursor=cursor,
                next_cursor=None,
                completed=True,
                started_at_ms=10,
                finished_at_ms=20,
            )

    class EventOnlyControlPlane(FakeControlPlane):
        def structure_source_event_pages(self, window_key: str):
            raise AssertionError("event-only source must not read prior event artifacts")

    control_plane = EventOnlyControlPlane(_event_spec())
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=TerminalEventGamma(),
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert control_plane.recorded is not None
    assert control_plane.recorded["event_embedded_markets"] is True


def test_source_artifact_upload_is_bounded_and_retryable() -> None:
    class SlowObjectClient(FakeObjectClient):
        def put_object(self, **kwargs: object) -> None:
            time.sleep(0.05)
            super().put_object(**kwargs)

    control_plane = FakeControlPlane(_event_spec())
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=FakeGamma(),
        object_client=SlowObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
        object_store_timeout_seconds=0.001,
    )

    assert asyncio.run(worker.run_once()).outcome == "retryable"

    assert control_plane.recorded is None
    assert control_plane.retry_incidents[0]["error_class"] == "TimeoutError"


def test_default_scoped_market_capacity_remains_hard_but_covers_live_universe() -> None:
    assert DEFAULT_MAX_MARKET_BATCHES == 10_000


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

        def admit_due_structure_source_window(
            self,
            *,
            cadence_seconds: int,
            structure_high_water: int,
            quote_high_water: int,
            now: datetime,
        ):
            self.calls.append((cadence_seconds, now))
            if len(self.calls) == 1:
                return SourceAdmissionDecision(
                    state="admitted",
                    job_key="structure-source:123:fetch:events:0",
                )
            return SourceAdmissionDecision(state="busy", job_key=None)

    now = datetime(2026, 8, 12, tzinfo=UTC)
    worker = TransactionalStructureSourceAdmitter(
        control_plane=ControlPlane(), cadence_seconds=300, now=lambda: now
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key="structure-source:123:fetch:events:0", outcome="admitted"
    )
    assert asyncio.run(worker.run_once()) == StructureWorkerResult(job_key=None, outcome="busy")


def test_source_admitter_returns_quote_backpressure_without_claiming_gamma_work() -> None:
    class ControlPlane:
        def admit_due_structure_source_window(self, **_kwargs):
            return type(
                "Decision",
                (),
                {"state": "backpressured:quote", "job_key": None},
            )()

    worker = TransactionalStructureSourceAdmitter(
        control_plane=ControlPlane(),
        cadence_seconds=300,
        structure_high_water=10,
        quote_high_water=2,
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key=None, outcome="backpressured:quote"
    )


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


def test_source_worker_reports_all_fenced_page_lifecycle_stages() -> None:
    control_plane = FakeControlPlane(_event_spec())
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=FakeGamma(),
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert [
        (item["progress"].stage, item["progress"].current, item["progress"].total)
        for item in control_plane.runtime_progress
    ] == [
        ("fetch-page", 0, 1),
        ("fetch-page", 1, 1),
        ("validate-page", 0, 1),
        ("validate-page", 1, 1),
        ("upload-page", 0, 1),
        ("upload-page", 1, 1),
        ("commit-page", 0, 1),
    ]


def test_source_worker_marks_only_current_page_retryable_when_gamma_fails() -> None:
    control_plane = FakeControlPlane(_event_spec())
    gamma = FailingGamma()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key="source-window:one:fetch:events:0", outcome="retryable"
    )

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
                "stage": "fetch-page",
                "failure_fingerprint": control_plane.retry_incidents[0]["detail"][
                    "failure_fingerprint"
                ],
            },
            "channels": ("dashboard",),
            "now": NOW,
        }
    ]
    assert control_plane.retry_incidents[0]["detail"]["failure_fingerprint"] == (
        retry_failure_fingerprint(
            gamma.failure,
            component="structure-fetch:fetch-page",
        )
    )
    assert [
        (item["progress"].stage, item["progress"].current)
        for item in control_plane.runtime_progress
    ] == [("fetch-page", 0)]
    assert worker._gamma.reset_calls == 1


def test_transport_reset_failure_does_not_replace_original_durable_failure() -> None:
    control_plane = FakeControlPlane(_event_spec())
    gamma = ResetFailingGamma()
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "retryable"
    assert gamma.reset_calls == 1
    assert control_plane.retry_incidents[0]["error_class"] == "TimeoutError"
    assert control_plane.retry_incidents[0]["detail"]["error_class"] == "TimeoutError"


def test_source_worker_quarantines_window_when_frozen_market_becomes_inactive() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:closed-member",
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a",),
    )
    control_plane = FakeControlPlane(spec)
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=ClosedExactMarketGamma(),
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key=spec.job_key, outcome="quarantined"
    )
    assert control_plane.quarantines == [
        {"error_class": "StructureSourceMemberBecameInactiveError", "now": NOW}
    ]
    assert control_plane.retry_incidents == []


def test_source_worker_quarantines_repeated_exact_batch_integrity_failure() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window:identity-mismatch",
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a",),
    )
    control_plane = FakeControlPlane(spec, lease_epoch=3)
    worker = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=MismatchedExactMarketGamma(),
        object_client=FakeObjectClient(),
        bucket="source-pages",
        worker_id="source-worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()) == StructureWorkerResult(
        job_key=spec.job_key, outcome="quarantined"
    )
    assert control_plane.quarantines == [
        {"error_class": "StructureSourceExactBatchIntegrityError", "now": NOW}
    ]
    assert control_plane.retry_incidents == []
