from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime

import pytest

from polyarb.control_plane.faults import IntentionalStagingRetryFault
from polyarb.control_plane.models import (
    JobLease,
    JobState,
    QuoteBatchLeg,
    QuoteBatchSpec,
)
from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.quote_artifact import QuoteBatchInputArtifact
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)

NOW = datetime(2030, 1, 1, tzinfo=UTC)


class FakeControlPlane:
    def __init__(self, batch: QuoteBatchSpec, *, prior: object | None = None) -> None:
        self.batch = batch
        self.prior = prior
        self.finished: list[JobState] = []
        self.recorded: dict[str, object] | None = None
        self.retry_incidents: list[dict[str, object]] = []
        self.recoveries: list[dict[str, object]] = []
        self.runtime_progress: list[dict[str, object]] = []
        self.runtime_heartbeats: list[dict[str, object]] = []

    def claim_job(self, **kwargs):
        return JobLease(
            job_key=self.batch.job_key,
            job_type="quote-batch",
            input_identity=self.batch.input_identity,
            lease_owner="worker-a",
            lease_epoch=1,
            lease_expires_at=NOW,
            checkpoint_cursor=None,
            checkpoint_digest=None,
        )

    def quote_batch_receipt(self, job_key: str):
        assert job_key == self.batch.job_key
        return self.prior

    def quote_batch_spec(self, job_key: str):
        assert job_key == self.batch.job_key
        return self.batch

    def record_quote_batch(self, lease, **kwargs):
        self.recorded = kwargs

    def finish(self, lease, *, state: JobState, **kwargs):
        self.finished.append(state)

    def finish_retryable_with_incident(self, lease, **kwargs):
        self.retry_incidents.append(kwargs)

    def record_job_recovery(self, lease, **kwargs):
        self.recoveries.append(kwargs)
        return False

    def record_runtime_progress(self, lease, **kwargs):
        self.runtime_progress.append(kwargs)

    def heartbeat_runtime_attempt(self, lease, **kwargs):
        self.runtime_heartbeats.append(kwargs)
        return lease


class FakeReader:
    def __init__(self) -> None:
        self.calls = 0

    async def get_books(self, token_ids: list[str], *, projection: str = "full"):
        self.calls += 1
        assert token_ids == ["token-a"]
        assert projection == "full"
        return [{"asset_id": "token-a", "asks": [{"price": "0.41", "size": "20"}]}]


class FailingReader:
    async def get_books(self, token_ids: list[str], *, projection: str = "full"):
        raise TimeoutError("clob unavailable")


class BlockingReader:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def get_books(self, token_ids: list[str], *, projection: str = "full"):
        self.started.set()
        await self.release.wait()
        return [{"asset_id": token_ids[0], "asks": [{"price": "0.41", "size": "20"}]}]


class FakeObjectClient:
    def __init__(self) -> None:
        self.object: dict[str, object] = {}

    def put_object(self, **kwargs):
        self.object = kwargs

    def head_object(self, **kwargs):
        return {
            "ContentLength": len(self.object["Body"]),
            "Metadata": self.object["Metadata"],
        }

    def get_object(self, **kwargs):
        return {"Body": type("Body", (), {"read": lambda _self: self.object["Body"]})()}


def _runtime_quote_certifier_lease() -> JobLease:
    generation_key = "quote:" + "a" * 64
    return JobLease(
        job_key=generation_key + ":certify",
        job_type="quote-certify",
        input_identity=generation_key + ":" + "b" * 64,
        lease_owner="quote-certifier",
        lease_epoch=1,
        lease_expires_at=datetime(2030, 1, 1, tzinfo=UTC),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )


async def _wait_thread_event(event: threading.Event) -> None:
    for _ in range(1_000):
        if event.is_set():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("timed out waiting for worker event")


def _batch() -> QuoteBatchSpec:
    return QuoteBatchSpec.from_legs(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        ordinal=0,
        legs=(
            QuoteBatchLeg(
                neg_risk_market_id="neg-risk-a",
                market_id="market-a",
                condition_id="condition-a",
                slug="market-a",
                yes_token_id="token-a",
                event_id="event-a",
                membership_hash="membership-a",
            ),
        ),
    )


def test_transactional_worker_uploads_then_records_and_finishes() -> None:
    control_plane = FakeControlPlane(_batch())
    reader = FakeReader()
    objects = FakeObjectClient()
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=reader,
        object_client=objects,
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert reader.calls == 1
    assert control_plane.recorded is not None
    assert control_plane.recorded["artifact_key"] == objects.object["Key"]
    assert control_plane.recorded["artifact_digest"] == objects.object["Metadata"]["sha256"]
    assert control_plane.finished == [JobState.SUCCEEDED]


def test_transactional_worker_reads_fenced_r2_input_when_reference_is_present() -> None:
    batch = _batch()
    artifact = QuoteBatchInputArtifact.from_spec(batch)
    control_plane = FakeControlPlane(batch)
    control_plane.quote_batch_input_reference = lambda job_key: (  # type: ignore[attr-defined]
        artifact.key,
        artifact.sha256,
        len(batch.legs),
    )
    reader = FakeReader()
    objects = FakeObjectClient()
    objects.object = {"Body": artifact.payload, "Metadata": {"sha256": artifact.sha256}}
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=reader,
        object_client=objects,
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert reader.calls == 1


def test_quote_fault_hook_crashes_after_verified_upload_before_receipt() -> None:
    batch = _batch()
    control_plane = FakeControlPlane(batch)
    objects = FakeObjectClient()
    observed: list[str] = []
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=FakeReader(),
        object_client=objects,
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
        crash_after_r2_upload=lambda lease: (
            observed.append(lease.job_key),
            (_ for _ in ()).throw(KeyboardInterrupt("staging fault")),
        )[1],
    )

    with pytest.raises(KeyboardInterrupt, match="staging fault"):
        asyncio.run(worker.run_once())

    assert objects.object
    assert observed == [batch.job_key]
    assert control_plane.recorded is None
    assert control_plane.finished == []


def test_quote_retry_fault_uses_existing_retry_incident_path() -> None:
    control_plane = FakeControlPlane(_batch())
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=FakeReader(),
        object_client=FakeObjectClient(),
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
        retry_fault_before_receipt=lambda _lease: (_ for _ in ()).throw(
                IntentionalStagingRetryFault("intentional staging retry")
        ),
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "retryable"
    assert control_plane.recorded is None
    assert control_plane.retry_incidents[0]["component"] == "quote-batch"


def test_transactional_worker_finishes_existing_receipt_without_refetch() -> None:
    control_plane = FakeControlPlane(_batch(), prior=object())
    reader = FakeReader()
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=reader,
        object_client=FakeObjectClient(),
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "recovered"
    assert reader.calls == 0
    assert control_plane.recorded is None
    assert control_plane.finished == [JobState.SUCCEEDED]


def test_transactional_worker_marks_only_its_batch_retryable_on_fetch_failure() -> None:
    control_plane = FakeControlPlane(_batch())
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=FailingReader(),
        object_client=FakeObjectClient(),
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    with pytest.raises(TimeoutError, match="clob unavailable"):
        asyncio.run(worker.run_once())

    assert control_plane.recorded is None
    assert control_plane.finished == []
    assert control_plane.retry_incidents[0]["component"] == "quote-batch"


def test_transactional_worker_reports_each_quote_batch_stage() -> None:
    control_plane = FakeControlPlane(_batch())
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=FakeReader(),
        object_client=FakeObjectClient(),
        bucket="quotes",
        worker_id="worker-a",
        now=lambda: NOW,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    assert [item["progress"].stage for item in control_plane.runtime_progress] == [
        "read-input",
        "fetch-books",
        "build-artifact",
        "upload-artifact",
        "commit-receipt",
    ]


def test_quote_batch_stale_heartbeat_cancels_owner_and_drains_reader() -> None:
    class StaleControlPlane(FakeControlPlane):
        def heartbeat_runtime_attempt(self, lease, **kwargs):
            self.runtime_heartbeats.append(kwargs)
            raise StaleLeaseError("quote heartbeat fenced")

    async def scenario() -> None:
        control_plane = StaleControlPlane(_batch())
        reader = BlockingReader()
        worker = TransactionalQuoteBatchWorker(
            control_plane=control_plane,
            reader=reader,
            object_client=FakeObjectClient(),
            bucket="quotes",
            worker_id="worker-a",
            now=lambda: NOW,
            lease_seconds=3,
            runtime_sleep=lambda _seconds: asyncio.sleep(0.001),
        )
        task = asyncio.create_task(worker.run_once())
        await reader.started.wait()
        await asyncio.sleep(0.01)
        reader.release.set()
        with pytest.raises(StaleLeaseError, match="quote heartbeat fenced"):
            await task
        assert control_plane.retry_incidents == []
        assert control_plane.recorded is None
        assert control_plane.finished == []
        assert not any(
            item.get_name().startswith("runtime-heartbeat")
            for item in asyncio.all_tasks()
            if item is not asyncio.current_task()
        )

    asyncio.run(scenario())


def test_quote_batch_scheduler_cancellation_drains_reader_without_late_receipt() -> None:
    async def scenario() -> None:
        control_plane = FakeControlPlane(_batch())
        reader = BlockingReader()
        worker = TransactionalQuoteBatchWorker(
            control_plane=control_plane,
            reader=reader,
            object_client=FakeObjectClient(),
            bucket="quotes",
            worker_id="worker-a",
            now=lambda: NOW,
            lease_seconds=3,
        )
        task = asyncio.create_task(worker.run_once())
        await reader.started.wait()
        task.cancel()
        reader.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert control_plane.recorded is None
        assert control_plane.finished == []
        assert control_plane.retry_incidents == []
        assert not any(
            item.get_name().startswith("runtime-heartbeat")
            for item in asyncio.all_tasks()
            if item is not asyncio.current_task()
        )

    asyncio.run(scenario())


def test_quote_batch_terminal_commit_has_no_heartbeat_race() -> None:
    class TerminalProbeControlPlane(FakeControlPlane):
        terminal_heartbeat_count: int | None = None

        def record_quote_batch(self, lease, **kwargs):
            self.terminal_heartbeat_count = len(self.runtime_heartbeats)
            super().record_quote_batch(lease, **kwargs)

    async def fast_sleep(_seconds: float) -> None:
        await asyncio.sleep(0.001)

    async def scenario() -> None:
        control_plane = TerminalProbeControlPlane(_batch())
        worker = TransactionalQuoteBatchWorker(
            control_plane=control_plane,
            reader=FakeReader(),
            object_client=FakeObjectClient(),
            bucket="quotes",
            worker_id="worker-a",
            now=lambda: NOW,
            lease_seconds=3,
            runtime_sleep=fast_sleep,
        )
        result = await worker.run_once()
        assert result.outcome == "succeeded"
        assert control_plane.terminal_heartbeat_count == len(control_plane.runtime_heartbeats)

    asyncio.run(scenario())


def test_quote_certifier_stale_progress_fences_before_terminal_call() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.certified = False

        def claim_job(self, **kwargs):
            return _runtime_quote_certifier_lease()

        def record_runtime_progress(self, lease, **kwargs):
            raise StaleLeaseError("quote certifier progress fenced")

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            return lease

        def certify_quote_generation(self, lease, **kwargs):
            self.certified = True
            return "d" * 64

    control_plane = ControlPlane()
    certifier = TransactionalQuoteCertifier(
        control_plane=control_plane,
        worker_id="quote-certifier",
        now=lambda: NOW,
        lease_seconds=3,
    )
    with pytest.raises(StaleLeaseError, match="quote certifier progress fenced"):
        certifier.run_once()
    assert not control_plane.certified


def test_quote_certifier_terminal_commit_wins_heartbeat_race_and_drains_thread() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.heartbeat_seen = threading.Event()
            self.release = threading.Event()
            self.heartbeats = 0
            self.runtime_progress: list[object] = []

        def claim_job(self, **kwargs):
            return _runtime_quote_certifier_lease()

        def record_runtime_progress(self, lease, **kwargs):
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            self.heartbeats += 1
            self.heartbeat_seen.set()
            raise StaleLeaseError("quote certifier terminal race fenced")

        def certify_quote_generation(self, lease, **kwargs):
            self.started.set()
            self.release.wait(timeout=5)
            return "d" * 64

        def record_job_recovery(self, lease, **kwargs):
            return False

    async def scenario() -> None:
        control_plane = ControlPlane()
        certifier = TransactionalQuoteCertifier(
            control_plane=control_plane,
            worker_id="quote-certifier",
            now=lambda: datetime.now(UTC),
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        await _wait_thread_event(control_plane.heartbeat_seen)
        control_plane.release.set()
        result = await task
        assert result.outcome == "certified"
        assert control_plane.heartbeats >= 1
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())


def test_quote_certifier_scheduler_cancellation_drains_terminal_thread() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.recovered = threading.Event()

        def claim_job(self, **kwargs):
            return _runtime_quote_certifier_lease()

        def record_runtime_progress(self, lease, **kwargs):
            return None

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            return lease

        def certify_quote_generation(self, lease, **kwargs):
            self.started.set()
            self.release.wait(timeout=5)
            return "d" * 64

        def record_job_recovery(self, lease, **kwargs):
            self.recovered.set()
            return False

    async def scenario() -> None:
        control_plane = ControlPlane()
        certifier = TransactionalQuoteCertifier(
            control_plane=control_plane,
            worker_id="quote-certifier",
            now=lambda: NOW,
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        task.cancel()
        control_plane.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _wait_thread_event(control_plane.recovered)
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())
