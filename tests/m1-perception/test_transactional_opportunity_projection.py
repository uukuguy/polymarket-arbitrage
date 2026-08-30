import asyncio
import hashlib
import threading
from datetime import UTC, datetime, timedelta

import pytest

from polyarb.control_plane.models import JobLease, QuoteBatchLeg, QuoteBatchReceipt, QuoteBatchSpec
from polyarb.control_plane.opportunity_projection import build_opportunity_rows
from polyarb.control_plane.opportunity_worker import (
    StaleQuoteGenerationError,
    TransactionalOpportunityCertifier,
)
from polyarb.control_plane.postgres import (
    IncompleteQuoteGenerationError,
    OpportunityProjectionCurrentError,
    PublicationPointerConflictError,
    StaleLeaseError,
)
from polyarb.control_plane.quote_artifact import QuoteBatchInputArtifact
from polyarb.control_plane.runtime_contract import ServiceStopRequested


def test_complete_authenticated_quote_group_projects_only_positive_buy_all_edge() -> None:
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    rows = build_opportunity_rows(
        legs=legs,
        quotes=(
            {
                "yes_token_id": "token-a",
                "terminal_state": "executable",
                "best_ask_price": 0.4,
                "best_ask_size": 4,
            },
            {
                "yes_token_id": "token-b",
                "terminal_state": "executable",
                "best_ask_price": 0.5,
                "best_ask_size": 3,
            },
        ),
        structure_observed_at_ms=1,
        quote_started_at_ms=2,
        quote_quoted_at_ms=3,
    )

    assert rows == (
        {
            "group_id": "group-a",
            "event_id": "event-a",
            "membership_hash": "m-a",
            "bundle_cost": 0.9,
            "gross_edge_bps": 1000.0,
            "max_bundle_size": 3.0,
            "legs": [
                {
                    "market_id": "market-a",
                    "condition_id": "condition-a",
                    "slug": "a",
                    "yes_token_id": "token-a",
                    "ask_price": 0.4,
                    "ask_size": 4.0,
                },
                {
                    "market_id": "market-b",
                    "condition_id": "condition-b",
                    "slug": "b",
                    "yes_token_id": "token-b",
                    "ask_price": 0.5,
                    "ask_size": 3.0,
                },
            ],
            "structure_observed_at_ms": 1,
            "quote_started_at_ms": 2,
            "quote_quoted_at_ms": 3,
        },
    )


def test_certifier_authenticates_r2_quote_payload_before_atomic_publish() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + b"b" * 64
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
        + (
            b'{"best_ask_price":0.4,"best_ask_size":4,'
            b'"terminal_state":"executable","yes_token_id":"token-a"}\n'
        )
        + (
            b'{"best_ask_price":0.5,"best_ask_size":3,'
            b'"terminal_state":"executable","yes_token_id":"token-b"}\n'
        )
    )

    class ControlPlane:
        def claim_job(self, **kwargs):
            self.claim = kwargs
            return type(
                "Lease",
                (),
                {
                    "job_key": "quote:job:opportunity-certify",
                    "input_identity": "quote:" + "a" * 64,
                },
            )()

        def finish(self, *args, **kwargs):
            self.finished = kwargs

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                "job", "d" * 64, "quotes/key", hashlib.sha256(payload).hexdigest(), 2
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, ((legs, receipt, quoted_at),)

        def publish_opportunity_projection(self, **kwargs):
            self.published = kwargs
            return "e" * 64

    class Client:
        def get_object(self, **kwargs):
            assert kwargs == {"Bucket": "bucket", "Key": "quotes/key"}
            return {"Body": type("Body", (), {"read": lambda self: payload})()}

    control_plane = ControlPlane()
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane, object_client=Client(), bucket="bucket", now=lambda: quoted_at
    ).run_once()
    assert result.job_key == "quote:job:opportunity-certify"
    assert result.outcome == "certified:" + "e" * 64
    assert control_plane.published["rows"][0]["gross_edge_bps"] == 1000.0


def test_certifier_skips_r2_when_current_quote_is_already_projected() -> None:
    class ControlPlane:
        def claim_job(self, **kwargs):
            return type(
                "Lease",
                (),
                {
                    "job_key": "quote:job:opportunity-certify",
                    "input_identity": "quote:" + "a" * 64,
                },
            )()

        def finish(self, *args, **kwargs):
            self.finished = kwargs

        def current_quote_projection_inputs(self):
            raise OpportunityProjectionCurrentError("already projected")

    class Client:
        def get_object(self, **kwargs):
            raise AssertionError(f"R2 must not be read: {kwargs}")

    result = TransactionalOpportunityCertifier(
        control_plane=ControlPlane(),
        object_client=Client(),
        bucket="bucket",
        now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
    ).run_once()

    assert result.outcome == "current"


def test_opportunity_incomplete_input_uses_durable_retry_circuit() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)

    class ControlPlane:
        def __init__(self) -> None:
            self.retry = None

        def claim_job(self, **_kwargs):
            return JobLease(
                job_key="quote:old:opportunity-certify",
                job_type="opportunity-certify",
                input_identity="quote:old",
                lease_owner="opportunity-worker",
                lease_epoch=4,
                lease_expires_at=now,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def current_quote_projection_inputs(self):
            raise IncompleteQuoteGenerationError("current generation is unavailable")

        def finish(self, *_args, **_kwargs):
            raise AssertionError("incomplete input must not use an ungoverned timed retry")

        def finish_retryable_with_incident(self, _lease, **kwargs):
            self.retry = kwargs

    control_plane = ControlPlane()
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane,
        object_client=object(),
        bucket="bucket",
        now=lambda: now,
    ).run_once()

    assert result.outcome == "retryable"
    assert control_plane.retry["component"] == "opportunity-certify"
    assert control_plane.retry["error_class"] == "IncompleteQuoteGenerationError"


def test_opportunity_stale_quote_is_blocked_before_r2_or_publish() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)
    quoted_at = now - timedelta(seconds=301)

    class ControlPlane:
        def __init__(self) -> None:
            self.quarantine = None

        def claim_job(self, **_kwargs):
            return JobLease(
                job_key="quote:old:opportunity-certify",
                job_type="opportunity-certify",
                input_identity="quote:old",
                lease_owner="opportunity-worker",
                lease_epoch=4,
                lease_expires_at=now,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt("quote:old:batch:0", "a" * 64, "quotes/key", "b" * 64, 1)
            return "quote:old", "structure:old", (((object(),), receipt, quoted_at),)

        def finish_quarantined_with_incident(self, _lease, **kwargs):
            self.quarantine = kwargs

        def publish_opportunity_projection(self, **_kwargs):
            raise AssertionError("stale Quote must not publish an Opportunity projection")

    class Client:
        def get_object(self, **kwargs):
            raise AssertionError(f"stale Quote must be rejected before R2 read: {kwargs}")

    control_plane = ControlPlane()
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane,
        object_client=Client(),
        bucket="bucket",
        now=lambda: now,
    ).run_once()

    assert result.outcome == "stale"
    assert control_plane.quarantine is not None
    assert control_plane.quarantine["error_class"] == "StaleQuoteGenerationError"
    assert control_plane.quarantine["incident_key"] == "incident:freshness:quote"
    assert control_plane.quarantine["dedupe_key"] == "freshness:quote"
    assert control_plane.quarantine["reason_code"] == "freshness.quote"
    assert control_plane.quarantine["qualification_impact"] == "blocked"
    assert control_plane.quarantine["detail"]["qualification_impact"] == "blocked"
    assert control_plane.quarantine["qualification_breaking"] is True


def test_opportunity_quote_freshness_uses_oldest_batch_and_canonical_sla() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)

    with pytest.raises(StaleQuoteGenerationError, match="301.0s exceeds 300.0s"):
        TransactionalOpportunityCertifier._require_fresh_quote_generation(
            (
                ((), object(), now - timedelta(seconds=299)),
                ((), object(), now - timedelta(seconds=301)),
            ),
            now=now,
        )

    TransactionalOpportunityCertifier._require_fresh_quote_generation(
        (((), object(), now - timedelta(seconds=300)),),
        now=now,
    )


def test_opportunity_pointer_conflict_is_visible_and_never_retried() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)

    class ControlPlane:
        def __init__(self) -> None:
            self.superseded: dict[str, object] | None = None

        def claim_job(self, **_kwargs):
            return JobLease(
                job_key="quote:old:opportunity-certify",
                job_type="opportunity-certify",
                input_identity="quote:old",
                lease_owner="opportunity-worker",
                lease_epoch=4,
                lease_expires_at=now,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def current_quote_projection_inputs(self):
            raise PublicationPointerConflictError("stale opportunity lineage")

        def finish_quarantined_with_incident(self, _lease, **kwargs):
            self.superseded = kwargs

        def finish_retryable_with_incident(self, *_args, **_kwargs):
            raise AssertionError("superseded publication must not consume retry budget")

    control_plane = ControlPlane()
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane,
        object_client=object(),
        bucket="bucket",
        now=lambda: now,
    ).run_once()

    assert result.outcome == "superseded"
    assert control_plane.superseded is not None
    assert control_plane.superseded["reason_code"] == "publication.superseded"
    assert control_plane.superseded["qualification_impact"] == "delayed"
    assert control_plane.superseded["severity"] == "warning"
    assert control_plane.superseded["qualification_breaking"] is False


def test_opportunity_service_stop_uses_interruption_not_defect_retry() -> None:
    now = datetime(2030, 1, 1, tzinfo=UTC)

    class ControlPlane:
        def __init__(self) -> None:
            self.interruption = None

        def claim_job(self, **_kwargs):
            return JobLease(
                job_key="quote:old:opportunity-certify",
                job_type="opportunity-certify",
                input_identity="quote:old",
                lease_owner="opportunity-worker",
                lease_epoch=4,
                lease_expires_at=now,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def finish_interrupted(self, _lease, **kwargs):
            self.interruption = kwargs

        def finish_retryable_with_incident(self, *_args, **_kwargs):
            raise AssertionError("service stop must not consume defect retry budget")

    control_plane = ControlPlane()
    certifier = TransactionalOpportunityCertifier(
        control_plane=control_plane,
        object_client=object(),
        bucket="bucket",
        now=lambda: now,
    )
    certifier.request_stop()

    with pytest.raises(ServiceStopRequested):
        certifier.run_once()

    assert control_plane.interruption == {
        "component": "opportunity-certify",
        "now": now,
    }


def test_certifier_uses_fenced_r2_input_when_postgres_legs_are_compacted() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    legs = (
        QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),
        QuoteBatchLeg("group-a", "market-b", "condition-b", "b", "token-b", "event-a", "m-a"),
    )
    batch = QuoteBatchSpec.from_legs(
        structure_receipt_digest="a" * 64,
        universe_hash="c" * 64,
        ordinal=0,
        legs=legs,
    )
    input_artifact = QuoteBatchInputArtifact.from_spec(batch)
    quote_payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + batch.token_range_digest.encode()
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
        + (
            b'{"best_ask_price":0.4,"best_ask_size":4,'
            b'"terminal_state":"executable","yes_token_id":"token-a"}\n'
        )
        + (
            b'{"best_ask_price":0.5,"best_ask_size":3,'
            b'"terminal_state":"executable","yes_token_id":"token-b"}\n'
        )
    )

    class ControlPlane:
        def claim_job(self, **kwargs):
            return type(
                "Lease",
                (),
                {
                    "job_key": "quote:job:opportunity-certify",
                    "input_identity": "quote:" + "a" * 64,
                },
            )()

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                batch.job_key,
                "d" * 64,
                "quotes/key",
                hashlib.sha256(quote_payload).hexdigest(),
                2,
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, (((), receipt, quoted_at),)

        def quote_batch_input_reference(self, job_key):
            assert job_key == batch.job_key
            return input_artifact.key, input_artifact.sha256, len(batch.legs)

        def publish_opportunity_projection(self, **kwargs):
            self.published = kwargs
            return "e" * 64

        def finish(self, *args, **kwargs):
            pass

    class Client:
        def get_object(self, **kwargs):
            payloads = {input_artifact.key: input_artifact.payload, "quotes/key": quote_payload}
            return {"Body": type("Body", (), {"read": lambda self: payloads[kwargs["Key"]]})()}

    control_plane = ControlPlane()
    TransactionalOpportunityCertifier(
        control_plane=control_plane, object_client=Client(), bucket="bucket", now=lambda: quoted_at
    ).run_once()

    assert control_plane.published["rows"][0]["gross_edge_bps"] == 1000.0


def test_opportunity_certifier_reports_projection_stages() -> None:
    quoted_at = datetime(2030, 1, 1, tzinfo=UTC)
    payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + b"b" * 64
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
        + b'{"best_ask_price":0.4,"best_ask_size":4,"terminal_state":"executable",'
        + b'"yes_token_id":"token-a"}\n'
    )
    legs = (QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),)

    class ControlPlane:
        def __init__(self) -> None:
            self.runtime_progress: list[dict[str, object]] = []

        def claim_job(self, **kwargs):
            return JobLease(
                job_key="quote:job:opportunity-certify",
                job_type="opportunity-certify",
                input_identity="quote:" + "a" * 64,
                lease_owner="worker-a",
                lease_epoch=1,
                lease_expires_at=quoted_at,
                checkpoint_cursor=None,
                checkpoint_digest=None,
            )

        def finish(self, *args, **kwargs):
            pass

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                "job", "d" * 64, "quotes/key", hashlib.sha256(payload).hexdigest(), 1
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, ((legs, receipt, quoted_at),)

        def publish_opportunity_projection(self, **kwargs):
            return "e" * 64

        def record_runtime_progress(self, lease, **kwargs):
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            return lease

    class Client:
        def get_object(self, **kwargs):
            return {"Body": type("Body", (), {"read": lambda self: payload})()}

    control_plane = ControlPlane()
    result = TransactionalOpportunityCertifier(
        control_plane=control_plane,
        object_client=Client(),
        bucket="bucket",
        now=lambda: quoted_at,
    ).run_once()

    assert result.outcome == "certified:" + "e" * 64
    assert [item["progress"].stage for item in control_plane.runtime_progress] == [
        "read-current-quote",
        "compute-opportunities",
        "upload-projection",
        "publish-opportunity",
    ]


def _runtime_opportunity_lease() -> JobLease:
    return JobLease(
        job_key="quote:" + "a" * 64 + ":opportunity-certify",
        job_type="opportunity-certify",
        input_identity="quote:" + "a" * 64,
        lease_owner="opportunity-worker",
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


def test_opportunity_stale_heartbeat_drains_blocking_db_call_without_retry() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.heartbeat_seen = threading.Event()
            self.release = threading.Event()
            self.finished = threading.Event()
            self.runtime_progress: list[object] = []

        def claim_job(self, **kwargs):
            return _runtime_opportunity_lease()

        def record_runtime_progress(self, lease, **kwargs):
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            self.heartbeat_seen.set()
            raise StaleLeaseError("opportunity heartbeat fenced")

        def current_quote_projection_inputs(self):
            self.started.set()
            self.release.wait(timeout=5)
            return "quote:" + "a" * 64, "structure:" + "b" * 64, ()

    async def scenario() -> None:
        control_plane = ControlPlane()
        certifier = TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=object(),  # type: ignore[arg-type]
            bucket="bucket",
            now=lambda: datetime.now(UTC),
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        await _wait_thread_event(control_plane.heartbeat_seen)
        control_plane.release.set()
        with pytest.raises(StaleLeaseError, match="opportunity heartbeat fenced"):
            await task
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())


def test_opportunity_stale_heartbeat_drains_blocking_r2_body_without_publish() -> None:
    quoted_at = datetime.now(UTC)
    payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + b"b" * 64
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
        + b'{"best_ask_price":0.4,"best_ask_size":4,'
        + b'"terminal_state":"executable","yes_token_id":"token-a"}\n'
    )
    legs = (QuoteBatchLeg("group-a", "market-a", "condition-a", "a", "token-a", "event-a", "m-a"),)

    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.heartbeat_seen = threading.Event()
            self.release = threading.Event()
            self.published = False

        def claim_job(self, **kwargs):
            return _runtime_opportunity_lease()

        def record_runtime_progress(self, lease, **kwargs):
            return None

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            self.heartbeat_seen.set()
            raise StaleLeaseError("opportunity body heartbeat fenced")

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                "batch-job", "d" * 64, "quotes/key", hashlib.sha256(payload).hexdigest(), 1
            )
            return "quote:" + "a" * 64, "structure:" + "b" * 64, ((legs, receipt, quoted_at),)

        def publish_opportunity_projection(self, **kwargs):
            self.published = True
            return "e" * 64

    class Body:
        def __init__(self, owner: ControlPlane) -> None:
            self.owner = owner

        def read(self) -> bytes:
            self.owner.started.set()
            self.owner.release.wait(timeout=5)
            return payload

    class Client:
        def __init__(self, owner: ControlPlane) -> None:
            self.owner = owner

        def get_object(self, **kwargs):
            return {"Body": Body(self.owner)}

    async def scenario() -> None:
        control_plane = ControlPlane()
        certifier = TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=Client(control_plane),
            bucket="bucket",
            now=lambda: datetime.now(UTC),
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        await _wait_thread_event(control_plane.heartbeat_seen)
        control_plane.release.set()
        with pytest.raises(StaleLeaseError, match="opportunity body heartbeat fenced"):
            await task
        assert not control_plane.published
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())


def test_opportunity_scheduler_cancellation_drains_db_call_without_late_publish() -> None:
    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.release = threading.Event()
            self.finished = threading.Event()
            self.done = threading.Event()
            self.runtime_progress: list[object] = []
            self.published = False

        def claim_job(self, **kwargs):
            return _runtime_opportunity_lease()

        def record_runtime_progress(self, lease, **kwargs):
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            return lease

        def current_quote_projection_inputs(self):
            self.started.set()
            self.release.wait(timeout=5)
            self.finished.set()
            raise RuntimeError("scheduler cancellation drained")

        def finish_retryable_with_incident(self, lease, **kwargs):
            self.done.set()
            return None

    async def scenario() -> None:
        control_plane = ControlPlane()
        certifier = TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=object(),  # type: ignore[arg-type]
            bucket="bucket",
            now=lambda: datetime.now(UTC),
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        task.cancel()
        control_plane.release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        await _wait_thread_event(control_plane.done)
        assert not control_plane.published
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())


def test_opportunity_terminal_commit_wins_heartbeat_race_without_pending_thread() -> None:
    payload = (
        b'{"structure_receipt_digest":"'
        + b"a" * 64
        + b'","token_range_digest":"'
        + b"b" * 64
        + b'","universe_hash":"'
        + b"c" * 64
        + b'"}\n'
        + b'{"yes_token_id":"token-a"}\n'
    )

    class ControlPlane:
        def __init__(self) -> None:
            self.started = threading.Event()
            self.heartbeat_seen = threading.Event()
            self.release = threading.Event()
            self.runtime_progress: list[object] = []
            self.heartbeats = 0

        def claim_job(self, **kwargs):
            return _runtime_opportunity_lease()

        def record_runtime_progress(self, lease, **kwargs):
            self.runtime_progress.append(kwargs)

        def heartbeat_runtime_attempt(self, lease, **kwargs):
            self.heartbeats += 1
            self.heartbeat_seen.set()
            raise StaleLeaseError("terminal race heartbeat fenced")

        def current_quote_projection_inputs(self):
            receipt = QuoteBatchReceipt(
                "job", "d" * 64, "quotes/key", hashlib.sha256(payload).hexdigest(), 1
            )
            return (
                "quote:" + "a" * 64,
                "structure:" + "b" * 64,
                (((), receipt, datetime.now(UTC)),),
            )

        def publish_opportunity_projection(self, **kwargs):
            self.started.set()
            self.release.wait(timeout=5)
            return "d" * 64

        def record_job_recovery(self, lease, **kwargs):
            return False

    async def scenario() -> None:
        class Client:
            def get_object(self, **_kwargs):
                return {"Body": type("Body", (), {"read": lambda self: payload})()}

        control_plane = ControlPlane()
        certifier = TransactionalOpportunityCertifier(
            control_plane=control_plane,
            object_client=Client(),
            bucket="bucket",
            now=lambda: datetime.now(UTC),
            lease_seconds=3,
        )
        task = asyncio.create_task(asyncio.to_thread(certifier.run_once))
        await _wait_thread_event(control_plane.started)
        await _wait_thread_event(control_plane.heartbeat_seen)
        control_plane.release.set()
        result = await task
        assert result.outcome == "certified:" + "d" * 64
        assert control_plane.heartbeats >= 1
        assert not any(thread.name.startswith("quote-sync") for thread in threading.enumerate())

    asyncio.run(scenario())
