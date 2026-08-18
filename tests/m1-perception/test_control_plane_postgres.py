"""Real-Postgres contracts for fenced M1 job coordination."""

from __future__ import annotations

import asyncio
import os
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256

import psycopg
import pytest

from polyarb.control_plane.models import (
    JobState,
    QuoteBatchLeg,
    QuoteBatchSpec,
    StructureSourcePageSpec,
)
from polyarb.control_plane.postgres import (
    CheckpointConflictError,
    IncompleteQuoteGenerationError,
    IncompleteStructureGenerationError,
    PostgresControlPlane,
    StaleLeaseError,
)
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_bundle_bytes,
    canonical_structure_manifest_bytes,
)
from polyarb.control_plane.structure_source import (
    StructureSourcePageArtifact,
    TransactionalStructureSourceMaterializer,
    TransactionalStructureSourceWorker,
)
from polyarb.control_plane.structure_worker import TransactionalStructureWorker


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
            == 0
        )
    except OSError:
        return False


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; control-plane integration tests skipped")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if dsn.startswith(prefix):
                dsn = "postgresql://" + dsn[len(prefix) :]
        with psycopg.connect(dsn, autocommit=True) as connection:
            for role in ("anon", "authenticated", "service_role"):
                connection.execute(f"CREATE ROLE {role} NOLOGIN")
        result = subprocess.run(
            ["uv", "run", "alembic", "upgrade", "017"],
            env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
            capture_output=True,
            text=True,
            timeout=120,
        )
        assert result.returncode == 0, result.stderr
        yield dsn


@pytest.fixture()
def control_plane(postgres_dsn: str) -> Iterator[PostgresControlPlane]:
    def connect() -> psycopg.Connection:
        return psycopg.connect(postgres_dsn)

    with connect() as connection:
        for table in (
            "m1_soak_observations",
            "m1_soak_runs",
            "m1_structure_source_window_bundles",
            "m1_structure_source_page_receipts",
            "m1_structure_source_page_inputs",
            "m1_structure_source_windows",
            "m1_alert_deliveries",
            "m1_alert_outbox",
            "m1_incident_events",
            "m1_incidents",
            "m1_opportunity_publication_pointers",
            "m1_opportunity_projection_rows",
            "m1_opportunity_projections",
            "m1_publication_pointers",
            "m1_generation_manifests",
            "m1_structure_range_receipts",
            "m1_structure_range_inputs",
            "m1_structure_generation_inputs",
            "m1_quote_batch_receipts",
            "m1_quote_batch_inputs",
            "m1_quote_admission_inputs",
            "m1_checkpoint_receipts",
            "m1_job_attempts",
            "m1_job_circuits",
            "m1_jobs",
        ):
            connection.execute(f"TRUNCATE {table} CASCADE")
    yield PostgresControlPlane(connect)


def test_cloud_soak_ledger_is_append_only_and_idempotent(
    control_plane: PostgresControlPlane,
) -> None:
    from polyarb.control_plane.soak_evidence import create_record

    first = create_record(
        observed_at="2030-01-01T00:00:00+00:00",
        control_api_url="https://control.example/perception/control-plane",
        machine_states={"machine-a": "started"},
        control_snapshot={
            "status": "available",
            "expired_leases": 0,
            "open_circuit_count": 0,
            "queue_health": {},
            "job_counts": {"succeeded": 1},
        },
    )
    control_plane.start_soak_run(run_id="formal-cloud-v1", baseline_record=first)
    assert control_plane.read_soak_observations("formal-cloud-v1") == (first,)
    control_plane.append_soak_observation(run_id="formal-cloud-v1", record=first)
    control_plane.append_soak_observation(run_id="formal-cloud-v1", record=first)

    assert control_plane.read_soak_observations("formal-cloud-v1") == (first,)
    assert control_plane.operational_snapshot(now=_now())["soak_evidence"] == {
        "latest_run_id": "formal-cloud-v1",
        "latest_observed_at": "2030-01-01T00:00:00+00:00",
    }


def _now() -> datetime:
    return datetime(2030, 1, 1, 12, tzinfo=UTC)


def _leg(token_id: str, *, suffix: str = "") -> QuoteBatchLeg:
    return QuoteBatchLeg(
        neg_risk_market_id=f"neg-risk-{token_id}",
        market_id=f"market-{token_id}",
        condition_id=f"condition-{token_id}",
        slug=f"slug-{token_id}",
        yes_token_id=token_id,
        event_id=f"event-{token_id}",
        membership_hash=f"membership-{suffix or token_id}",
    )


def _structure_identity() -> StructureBundleIdentity:
    return StructureBundleIdentity(
        publication_id="publication-1",
        window_id="window-1",
        snapshot_id=42,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="structure-v7",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 1,
            "issues": 0,
        },
    )


class _OneBookReader:
    async def get_books(self, token_ids: list[str], *, projection: str = "full"):
        return [
            {
                "asset_id": token_id,
                "asks": [{"price": "0.41", "size": "20"}],
            }
            for token_id in token_ids
        ]


class _MemoryObjects:
    def __init__(self) -> None:
        self.object: dict[str, object] = {}

    def put_object(self, **kwargs: object) -> None:
        self.object = kwargs

    def head_object(self, **kwargs: object) -> dict[str, object]:
        return {
            "ContentLength": len(self.object["Body"]),
            "Metadata": self.object["Metadata"],
        }


def test_structure_worker_takeover_after_upload_before_receipt_has_one_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    """A crash after deterministic R2 upload leaves only a reclaimable lease."""
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(
            identity=_structure_identity(),
            components={
                "events": ({"id": "event-a"},),
                "event_tags": (),
                "memberships": (),
                "group_truth": (),
                "markets": ({"market_id": "market-a"},),
                "issues": (),
            },
        )
    )
    admitted = control_plane.enqueue_structure_generation(
        identity=_structure_identity(), bundle=bundle, ranges=(("events", "", ""),), now=now
    )

    class MemoryR2:
        def __init__(self) -> None:
            self.objects = {bundle.key: bundle.payload}
            self.metadata: dict[str, dict[str, object]] = {}
            self.put_calls = 0

        def get_object(self, **kwargs: object) -> dict[str, object]:
            return {"Body": type("Body", (), {"read": lambda _self: self.objects[kwargs["Key"]]})()}

        def put_object(self, **kwargs: object) -> None:
            self.put_calls += 1
            key = str(kwargs["Key"])
            self.objects[key] = bytes(kwargs["Body"])
            self.metadata[key] = dict(kwargs["Metadata"])

        def head_object(self, **kwargs: object) -> dict[str, object]:
            payload = self.objects[str(kwargs["Key"])]
            return {
                "ContentLength": len(payload),
                "Metadata": self.metadata.get(str(kwargs["Key"]), {}),
            }

    class CrashBeforeReceipt:
        def __init__(self, delegate: PostgresControlPlane) -> None:
            self._delegate = delegate
            self.crash = True

        def __getattr__(self, name: str):
            return getattr(self._delegate, name)

        def record_structure_range(self, *args: object, **kwargs: object):
            if self.crash:
                self.crash = False
                raise KeyboardInterrupt("simulated process death after R2 upload")
            return self._delegate.record_structure_range(*args, **kwargs)

    objects = MemoryR2()
    crashing = CrashBeforeReceipt(control_plane)
    first = TransactionalStructureWorker(
        control_plane=crashing,  # type: ignore[arg-type]
        object_client=objects,
        bucket="structure",
        worker_id="crashed-worker",
        now=lambda: now,
        lease_seconds=1,
    )
    with pytest.raises(KeyboardInterrupt, match="after R2 upload"):
        asyncio.run(first.run_once())
    assert control_plane.structure_range_receipt(admitted[0].job_key) is None

    recovered = TransactionalStructureWorker(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="replacement-worker",
        now=lambda: now + timedelta(seconds=2),
        lease_seconds=30,
    )
    assert asyncio.run(recovered.run_once()).outcome == "succeeded"
    receipts = control_plane.structure_generation_receipts(admitted[0].generation_key)
    assert len(receipts) == 1
    assert receipts[0][0].job_key == admitted[0].job_key
    assert objects.put_calls == 2


def test_source_window_page_receipt_fences_cursor_and_advances_event_stream(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    admitted = control_plane.admit_structure_source_window(window_key="source-window:one", now=now)
    assert len(admitted) == 1
    assert admitted[0].stream == "events"
    assert admitted[0].ordinal == 0
    assert admitted[0].requested_cursor is None

    first = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert first is not None
    successor = control_plane.record_structure_source_page(
        first,
        artifact_key="m1/structure/source/window-one/events-0.json",
        artifact_digest="a" * 64,
        next_cursor="event-cursor-1",
        completed=False,
        record_count=100,
        now=now,
    )
    assert successor is not None
    assert successor.stream == "events"
    assert successor.ordinal == 1
    assert successor.requested_cursor == "event-cursor-1"
    receipt = control_plane.structure_source_page_receipt(first.job_key)
    assert receipt == {
        "artifact_key": "m1/structure/source/window-one/events-0.json",
        "artifact_digest": "a" * 64,
        "next_cursor": "event-cursor-1",
        "completed": False,
        "record_count": 100,
    }
    assert control_plane.structure_source_event_pages("source-window:one") == (
        (
            StructureSourcePageSpec(
                window_key="source-window:one",
                stream="events",
                ordinal=0,
                requested_cursor=None,
            ),
            "m1/structure/source/window-one/events-0.json",
            "a" * 64,
        ),
    )


def test_due_source_window_admission_is_bucket_idempotent_and_never_overlaps(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    first = control_plane.admit_due_structure_source_window(cadence_seconds=300, now=now)
    assert first.state == "admitted"
    assert first.job_key == "structure-source:300:6311664:fetch:events:0"
    assert (
        control_plane.admit_due_structure_source_window(
            cadence_seconds=300, now=now + timedelta(seconds=1)
        ).state
        == "busy"
    )
    # Even a later cadence bucket cannot overlap the unfinished traversal.
    assert (
        control_plane.admit_due_structure_source_window(
            cadence_seconds=300, now=now + timedelta(seconds=301)
        ).state
        == "busy"
    )


def test_source_page_limit_quarantine_releases_later_admission_bucket(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:limit", now=now)
    lease = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None

    control_plane.quarantine_structure_source_page(
        lease,
        error_class="StructureSourcePageLimitError",
        now=now,
    )

    assert (
        control_plane.claim_job(
            worker_id="source-worker-b",
            job_types=("structure-fetch",),
            lease_seconds=30,
            now=now + timedelta(seconds=1),
        )
        is None
    )
    successor = control_plane.admit_due_structure_source_window(
        cadence_seconds=300, now=now + timedelta(seconds=301)
    )
    assert successor.state == "admitted"
    assert successor.job_key == "structure-source:300:6311665:fetch:events:0"


def test_source_quarantine_marks_unleased_sibling_pages_terminal(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:cleanup", now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/cleanup/events-0.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=2,
        market_batches=(("market-a",), ("market-b",)),
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="source-worker-b",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now + timedelta(seconds=1),
    )
    assert lease is not None

    control_plane.quarantine_structure_source_page(
        lease,
        error_class="StructureSourceExactBatchIntegrityError",
        now=now + timedelta(seconds=2),
    )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM m1_jobs WHERE job_key = %s",
            ("source-window:cleanup:fetch:markets:1",),
        )
        assert cursor.fetchone() == ("quarantined",)


def test_source_claim_skips_orphaned_jobs_from_a_quarantined_window(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:orphaned", now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/orphaned/events-0.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        market_batches=(("market-a",),),
        now=now,
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "UPDATE m1_structure_source_windows SET state = 'quarantined' WHERE window_key = %s",
            ("source-window:orphaned",),
        )
    successor = control_plane.admit_due_structure_source_window(
        cadence_seconds=300, now=now + timedelta(seconds=301)
    )
    assert successor.state == "admitted"

    claimed = control_plane.claim_job(
        worker_id="source-worker-b",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now + timedelta(seconds=301),
    )

    assert claimed is not None
    assert claimed.job_key == successor.job_key


def test_terminal_event_page_creates_first_market_page(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:two", now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert event is not None

    market = control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/window-two/events-0.json",
        artifact_digest="b" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )

    assert market is not None
    assert market.stream == "markets"
    assert market.ordinal == 0
    assert market.requested_cursor is None


def test_due_source_admission_backpressures_before_window_insert_when_range_queue_is_full(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:backpressure:normalize:0",
        job_type="structure-normalize",
        input_identity="backpressure",
        now=now,
    )

    decision = control_plane.admit_due_structure_source_window(
        cadence_seconds=300,
        now=now,
        structure_high_water=1,
        quote_high_water=10,
    )

    assert decision.state == "backpressured:structure"
    assert decision.job_key is None


def test_terminal_event_page_atomically_admits_immutable_market_batches(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:batches", now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert event is not None

    first = control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/batches/events-0.json",
        artifact_digest="c" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        market_batches=(("market-a", "market-b"), ("market-c",)),
        now=now,
    )

    assert first == StructureSourcePageSpec(
        window_key="source-window:batches",
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a", "market-b"),
    )
    assert control_plane.structure_source_page_spec(
        "source-window:batches:fetch:markets:1"
    ) == StructureSourcePageSpec(
        window_key="source-window:batches",
        stream="markets",
        ordinal=1,
        requested_cursor=None,
        market_ids=("market-c",),
    )


def test_only_last_scoped_market_batch_releases_materializer(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    window_key = "source-window:batch-completion"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/batch-completion/events-0.json",
        artifact_digest="d" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        market_batches=(("market-a",), ("market-b",)),
        now=now,
    )

    first = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert first is not None
    control_plane.record_structure_source_page(
        first,
        artifact_key="m1/structure/source/batch-completion/markets-0.json",
        artifact_digest="e" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    assert (
        control_plane.claim_job(
            worker_id="materializer-a",
            job_types=("structure-materialize",),
            lease_seconds=30,
            now=now,
        )
        is None
    )

    second = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert second is not None
    control_plane.record_structure_source_page(
        second,
        artifact_key="m1/structure/source/batch-completion/markets-1.json",
        artifact_digest="f" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    persisted_pages = control_plane.structure_source_window_pages(window_key)
    assert persisted_pages[1][0] == StructureSourcePageSpec(
        window_key=window_key,
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a",),
    )
    materializer = control_plane.claim_job(
        worker_id="materializer-a",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    assert materializer.job_key == f"{window_key}:materialize"


def test_terminal_event_with_embedded_markets_releases_materializer_without_market_jobs(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    window_key = "source-window:event-embedded"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    event = control_plane.claim_job(
        worker_id="event-lane", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert event is not None

    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/event-embedded/events-0.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )

    pages = control_plane.structure_source_window_pages(window_key)
    assert len(pages) == 1
    assert pages[0][0].stream == "events"
    materializer = control_plane.claim_job(
        worker_id="materializer", job_types=("structure-materialize",), lease_seconds=30, now=now
    )
    assert materializer is not None
    assert materializer.job_key == f"{window_key}:materialize"


def test_parallel_scoped_batch_leases_release_materializer_only_after_last_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    window_key = "source-window:parallel-batches"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    event = control_plane.claim_job(
        worker_id="event-lane", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/parallel/events-0.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        market_batches=(("market-a",), ("market-b",), ("market-c",)),
        now=now,
    )

    leases = tuple(
        control_plane.claim_job(
            worker_id=f"market-lane:{ordinal}",
            job_types=("structure-fetch",),
            lease_seconds=30,
            now=now,
        )
        for ordinal in range(3)
    )
    assert all(lease is not None for lease in leases)
    source_leases = tuple(lease for lease in leases if lease is not None)
    assert {lease.job_key for lease in source_leases} == {
        f"{window_key}:fetch:markets:{ordinal}" for ordinal in range(3)
    }
    assert {lease.lease_owner for lease in source_leases} == {
        f"market-lane:{ordinal}" for ordinal in range(3)
    }

    for ordinal, lease in enumerate(source_leases[:2]):
        control_plane.record_structure_source_page(
            lease,
            artifact_key=f"m1/structure/source/parallel/markets-{ordinal}.json",
            artifact_digest=chr(ord("b") + ordinal) * 64,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now,
        )
    assert (
        control_plane.claim_job(
            worker_id="materializer",
            job_types=("structure-materialize",),
            lease_seconds=30,
            now=now,
        )
        is None
    )

    control_plane.record_structure_source_page(
        source_leases[2],
        artifact_key="m1/structure/source/parallel/markets-2.json",
        artifact_digest="d" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    materializer = control_plane.claim_job(
        worker_id="materializer", job_types=("structure-materialize",), lease_seconds=30, now=now
    )
    assert materializer is not None
    assert materializer.job_key == f"{window_key}:materialize"


def test_terminal_market_page_releases_one_fenced_materializer_job(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:materialize", now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/materialize/events-0.json",
        artifact_digest="d" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    market = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert market is not None
    control_plane.record_structure_source_page(
        market,
        artifact_key="m1/structure/source/materialize/markets-0.json",
        artifact_digest="e" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )

    materializer = control_plane.claim_job(
        worker_id="materializer-a",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    assert materializer.job_key == "source-window:materialize:materialize"
    assert materializer.input_identity == "source-window:materialize"


def test_materializer_lease_atomically_records_bundle_and_admits_ranges(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    window_key = "source-window:admit-bundle"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key="m1/structure/source/admit/events-0.json",
        artifact_digest="f" * 64,
        next_cursor=None,
        completed=True,
        record_count=0,
        now=now,
    )
    market = control_plane.claim_job(
        worker_id="source-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert market is not None
    control_plane.record_structure_source_page(
        market,
        artifact_key="m1/structure/source/admit/markets-0.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=0,
        now=now,
    )
    materializer = control_plane.claim_job(
        worker_id="materializer-a",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    source_digest = control_plane.structure_source_window_digest(window_key)
    identity = StructureBundleIdentity(
        publication_id=f"source-window:{window_key}",
        window_id=window_key,
        snapshot_id=0,
        comparison_receipt_digest=source_digest,
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
    bundle = StructureBundleArtifact.from_bytes(
        canonical_structure_bundle_bytes(
            identity=identity,
            components={
                "events": ({"id": "event-a"},),
                "event_tags": (),
                "memberships": (),
                "group_truth": (),
                "markets": (),
                "issues": (),
            },
        )
    )

    admitted = control_plane.admit_structure_source_bundle(
        materializer,
        identity=identity,
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )

    assert len(admitted) == 1
    assert admitted[0].bundle_digest == bundle.sha256
    assert control_plane.structure_source_window_bundle(window_key) == {
        "source_digest": source_digest,
        "bundle_key": bundle.key,
        "bundle_digest": bundle.sha256,
    }
    range_lease = control_plane.claim_job(
        worker_id="normalizer-a",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert range_lease is not None
    assert range_lease.job_key == admitted[0].job_key


def test_real_source_window_materializer_turn_admits_normalizer_work(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    window_key = "source-window:real-materializer"
    event_spec = StructureSourcePageSpec(
        window_key=window_key, stream="events", ordinal=0, requested_cursor=None
    )
    market_spec = StructureSourcePageSpec(
        window_key=window_key,
        stream="markets",
        ordinal=0,
        requested_cursor=None,
        market_ids=("market-a",),
    )
    event_artifact = StructureSourcePageArtifact.from_page(
        spec=event_spec,
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
        next_cursor=None,
        completed=True,
        started_at_ms=1,
        finished_at_ms=2,
    )
    market_artifact = StructureSourcePageArtifact.from_page(
        spec=market_spec,
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
        next_cursor=None,
        completed=True,
        started_at_ms=3,
        finished_at_ms=4,
    )

    class MemoryR2:
        def __init__(self) -> None:
            self.objects = {
                event_artifact.key: event_artifact.payload,
                market_artifact.key: market_artifact.payload,
            }
            self.metadata = {
                event_artifact.key: {"sha256": event_artifact.sha256},
                market_artifact.key: {"sha256": market_artifact.sha256},
            }

        def get_object(self, **kwargs: object) -> dict[str, object]:
            payload = self.objects[str(kwargs["Key"])]
            return {"Body": type("Body", (), {"read": lambda _self: payload})()}

        def put_object(self, **kwargs: object) -> None:
            key = str(kwargs["Key"])
            self.objects[key] = bytes(kwargs["Body"])
            self.metadata[key] = dict(kwargs["Metadata"])

        def head_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            return {
                "ContentLength": len(self.objects[key]),
                "Metadata": self.metadata[key],
            }

    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    event = control_plane.claim_job(
        worker_id="source-worker-a", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert event is not None
    control_plane.record_structure_source_page(
        event,
        artifact_key=event_artifact.key,
        artifact_digest=event_artifact.sha256,
        next_cursor=None,
        completed=True,
        record_count=1,
        market_batches=(("market-a",),),
        now=now,
    )
    market = control_plane.claim_job(
        worker_id="source-worker-a", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert market is not None
    control_plane.record_structure_source_page(
        market,
        artifact_key=market_artifact.key,
        artifact_digest=market_artifact.sha256,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )

    objects = MemoryR2()
    worker = TransactionalStructureSourceMaterializer(
        control_plane=control_plane,
        object_client=objects,
        bucket="structure",
        worker_id="materializer-a",
        now=lambda: now,
        range_max_rows=100,
    )
    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    bundle = control_plane.structure_source_window_bundle(window_key)
    assert bundle is not None
    assert bundle["bundle_key"] in objects.objects
    normalizer = control_plane.claim_job(
        worker_id="normalizer-a",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert normalizer is not None
    assert normalizer.job_key.startswith(f"structure:{bundle['bundle_digest']}:normalize:")


def test_stale_source_page_lease_cannot_advance_cursor(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:stale", now=now)
    old_lease = control_plane.claim_job(
        worker_id="source-worker-old",
        job_types=("structure-fetch",),
        lease_seconds=1,
        now=now,
    )
    assert old_lease is not None
    replacement = control_plane.claim_job(
        worker_id="source-worker-new",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert replacement is not None

    with pytest.raises(StaleLeaseError, match="no longer current"):
        control_plane.record_structure_source_page(
            old_lease,
            artifact_key="m1/structure/source/stale/events-0.json",
            artifact_digest="c" * 64,
            next_cursor="forbidden-cursor",
            completed=False,
            record_count=1,
            now=now + timedelta(seconds=2),
        )

    assert control_plane.structure_source_page_receipt(old_lease.job_key) is None
    control_plane.record_structure_source_page(
        replacement,
        artifact_key="m1/structure/source/stale/events-0.json",
        artifact_digest="c" * 64,
        next_cursor="replacement-cursor",
        completed=False,
        record_count=1,
        now=now + timedelta(seconds=2),
    )
    assert (
        control_plane.structure_source_page_spec(
            "source-window:stale:fetch:events:1"
        ).requested_cursor
        == "replacement-cursor"
    )


def test_source_worker_takeover_after_upload_before_receipt_has_one_page_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    """A dead process after R2 authentication cannot skip the source cursor."""
    from polyarb.clients.gamma_client import EventPage

    now = _now()
    control_plane.admit_structure_source_window(window_key="source-window:crash", now=now)

    class Gamma:
        def __init__(self) -> None:
            self.calls = 0

        async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
            self.calls += 1
            assert cursor is None
            assert limit == 100
            return EventPage(
                events=({"id": "event-a", "markets": []},),
                requested_cursor=cursor,
                next_cursor="event-next",
                completed=False,
                started_at_ms=1,
                finished_at_ms=2,
            )

        async def fetch_active_market_page(self, cursor: str | None, limit: int):
            raise AssertionError("market page must not run before event completion")

    class MemoryR2:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}
            self.metadata: dict[str, dict[str, str]] = {}
            self.put_calls = 0

        def put_object(self, **kwargs: object) -> None:
            self.put_calls += 1
            key = str(kwargs["Key"])
            self.objects[key] = bytes(kwargs["Body"])
            self.metadata[key] = dict(kwargs["Metadata"])

        def head_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            return {
                "ContentLength": len(self.objects[key]),
                "Metadata": self.metadata[key],
            }

    class CrashBeforeReceipt:
        def __init__(self, delegate: PostgresControlPlane) -> None:
            self._delegate = delegate
            self.crash = True

        def __getattr__(self, name: str):
            return getattr(self._delegate, name)

        def record_structure_source_page(self, *args: object, **kwargs: object):
            if self.crash:
                self.crash = False
                raise KeyboardInterrupt("simulated source process death after R2 upload")
            return self._delegate.record_structure_source_page(*args, **kwargs)

    gamma = Gamma()
    objects = MemoryR2()
    crashing = TransactionalStructureSourceWorker(
        control_plane=CrashBeforeReceipt(control_plane),  # type: ignore[arg-type]
        gamma=gamma,
        object_client=objects,
        bucket="structure",
        worker_id="source-worker-crashed",
        now=lambda: now,
        lease_seconds=1,
    )
    with pytest.raises(KeyboardInterrupt, match="after R2 upload"):
        asyncio.run(crashing.run_once())
    job_key = "source-window:crash:fetch:events:0"
    assert control_plane.structure_source_page_receipt(job_key) is None

    recovered = TransactionalStructureSourceWorker(
        control_plane=control_plane,
        gamma=gamma,
        object_client=objects,
        bucket="structure",
        worker_id="source-worker-replacement",
        now=lambda: now + timedelta(seconds=2),
        lease_seconds=30,
    )
    assert asyncio.run(recovered.run_once()).outcome == "succeeded"
    assert gamma.calls == 2
    assert objects.put_calls == 2
    assert control_plane.structure_source_page_receipt(job_key) == {
        "artifact_key": next(iter(objects.objects)),
        "artifact_digest": next(iter(objects.metadata.values()))["sha256"],
        "next_cursor": "event-next",
        "completed": False,
        "record_count": 1,
    }


def test_quote_batch_spec_normalizes_one_immutable_token_range() -> None:
    batch = QuoteBatchSpec.from_tokens(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        ordinal=2,
        token_ids=("token-c", "token-a", "token-b", "token-a"),
    )

    assert batch.token_ids == ("token-a", "token-b", "token-c")
    assert batch.job_key == f"quote:{'a' * 64}:batch:2"
    assert batch.input_identity.startswith(f"quote:{'a' * 64}:{'b' * 64}:2:")


def test_deployment_preflight_requires_named_database_and_all_014_tables(
    control_plane: PostgresControlPlane,
) -> None:
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        database_name = connection.execute("SELECT current_database()").fetchone()
    assert database_name is not None
    result = control_plane.deployment_preflight(expected_database=str(database_name[0]))
    assert result["database_name"] == database_name[0]
    assert result["revision_014_tables"] == 20
    with pytest.raises(Exception, match="database identity mismatch"):
        control_plane.deployment_preflight(expected_database="not-the-control-plane")


def test_enqueue_quote_generation_is_deterministic(control_plane: PostgresControlPlane) -> None:
    now = _now()
    first = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-3", "token-1", "token-2", "token-1"),
        batch_size=2,
        now=now,
    )
    second = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-2", "token-3", "token-1"),
        batch_size=2,
        now=now,
    )

    assert first == second
    assert [batch.token_ids for batch in first] == [("token-1", "token-2"), ("token-3",)]
    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT job_key,job_type FROM m1_jobs ORDER BY job_key")
            assert cursor.fetchall() == [
                (f"quote:{'a' * 64}:batch:0", "quote-batch"),
                (f"quote:{'a' * 64}:batch:1", "quote-batch"),
                (f"quote:{'a' * 64}:certify", "quote-certify"),
            ]
    finally:
        connection.close()


def test_quote_batch_input_survives_admission_for_worker_takeover(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    admitted = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-3", "token-1", "token-2"),
        batch_size=2,
        now=now,
    )

    assert control_plane.quote_batch_spec(admitted[0].job_key) == admitted[0]


def test_structure_range_input_survives_admission_for_worker_takeover(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    artifact = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    admitted = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=artifact,
        ranges=(("events", "", "m"), ("markets", "", "")),
        now=now,
    )

    first = control_plane.structure_range_spec(admitted[0].job_key)

    assert first.bundle_digest == artifact.sha256
    assert first.bundle_key == artifact.key
    assert first.component == "events"
    assert first.range_start == ""
    assert first.range_end == "m"


def test_structure_range_receipt_is_fenced_and_idempotent(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    artifact = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=artifact,
        ranges=(("events", "", "m"),),
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="structure-a", job_types=("structure-normalize",), lease_seconds=1, now=now
    )
    assert lease is not None
    receipt = control_plane.record_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/c/rows.ndjson",
        artifact_digest="c" * 64,
        record_count=3,
        now=now,
    )
    persisted = control_plane.structure_range_receipt(spec.job_key)
    assert persisted is not None
    assert persisted.artifact_digest == "c" * 64
    assert persisted.record_count == 3
    assert (
        control_plane.record_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key="structure-ranges/c/rows.ndjson",
            artifact_digest="c" * 64,
            record_count=3,
            now=now + timedelta(seconds=1),
        )
        == receipt
    )
    replacement = control_plane.claim_job(
        worker_id="structure-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert replacement is not None
    with pytest.raises(StaleLeaseError):
        control_plane.record_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key="structure-ranges/d/rows.ndjson",
            artifact_digest="d" * 64,
            record_count=3,
            now=now + timedelta(seconds=2),
        )


def test_checkpointed_range_stays_with_original_lease_until_expiry(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    artifact = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=artifact,
        ranges=(("events", "", "m"),),
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="structure-a", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert lease is not None
    control_plane.record_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/c/rows.ndjson",
        artifact_digest="c" * 64,
        record_count=3,
        now=now,
    )

    assert (
        control_plane.claim_job(
            worker_id="structure-b",
            job_types=("structure-normalize",),
            lease_seconds=30,
            now=now + timedelta(seconds=1),
        )
        is None
    )

    replacement = control_plane.claim_job(
        worker_id="structure-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=31),
    )
    assert replacement is not None
    assert replacement.lease_epoch == lease.lease_epoch + 1


def test_structure_certification_requires_complete_matching_range_receipts(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    specs = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", "m"), ("markets", "", "")),
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="structure-a", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert first is not None
    first_spec = control_plane.structure_range_spec(first.job_key)
    control_plane.record_structure_range(
        first,
        range_digest=first_spec.range_digest,
        artifact_key="structure-ranges/a/rows.ndjson",
        artifact_digest="a" * 64,
        record_count=1,
        now=now,
    )
    control_plane.finish(first, state=JobState.SUCCEEDED, now=now)
    certifier = control_plane.claim_job(
        worker_id="structure-certifier",
        job_types=("structure-certify",),
        lease_seconds=30,
        now=now,
    )
    assert certifier is not None
    with pytest.raises(IncompleteStructureGenerationError):
        control_plane.certify_structure_generation(
            certifier,
            generation_key=specs[0].generation_key,
            artifact_key="structure-manifests/a/manifest.ndjson",
            artifact_digest="a" * 64,
            now=now,
        )
    second = control_plane.claim_job(
        worker_id="structure-b", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert second is not None
    second_spec = control_plane.structure_range_spec(second.job_key)
    control_plane.record_structure_range(
        second,
        range_digest=second_spec.range_digest,
        artifact_key="structure-ranges/b/rows.ndjson",
        artifact_digest="b" * 64,
        record_count=1,
        now=now,
    )
    control_plane.finish(second, state=JobState.SUCCEEDED, now=now)
    expected_manifest = sha256(
        canonical_structure_manifest_bytes(
            generation_key=specs[0].generation_key,
            bundle_digest=bundle.sha256,
            receipts=(
                {
                    "job_key": first_spec.job_key,
                    "component": "events",
                    "ordinal": 0,
                    "range_digest": first_spec.range_digest,
                    "artifact_key": "structure-ranges/a/rows.ndjson",
                    "artifact_digest": "a" * 64,
                    "record_count": 1,
                },
                {
                    "job_key": second_spec.job_key,
                    "component": "markets",
                    "ordinal": 1,
                    "range_digest": second_spec.range_digest,
                    "artifact_key": "structure-ranges/b/rows.ndjson",
                    "artifact_digest": "b" * 64,
                    "record_count": 1,
                },
            ),
        )
    ).hexdigest()
    assert (
        control_plane.certify_structure_generation(
            certifier,
            generation_key=specs[0].generation_key,
            artifact_key=f"structure-manifests/{expected_manifest}/manifest.ndjson",
            artifact_digest=expected_manifest,
            now=now,
        )
        == expected_manifest
    )
    quote_admit = control_plane.claim_job(
        worker_id="quote-admitter",
        job_types=("quote-admit",),
        lease_seconds=30,
        now=now,
    )
    assert quote_admit is not None
    assert control_plane.quote_admission_input(quote_admit.job_key) == (
        specs[0].generation_key,
        bundle.key,
        bundle.sha256,
    )
    quote_batches = control_plane.admit_quote_generation(
        quote_admit,
        structure_receipt_digest=bundle.sha256,
        universe_hash="c" * 64,
        legs=(_leg("quote-token"),),
        batch_size=100,
        now=now,
    )
    assert control_plane.quote_batch_spec(quote_batches[0].job_key).legs == (_leg("quote-token"),)


def test_structure_certification_refuses_component_count_parity_mismatch(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    identity = StructureBundleIdentity(
        publication_id="publication-counts",
        window_id="window-1",
        snapshot_id=42,
        comparison_receipt_digest="a" * 64,
        normalization_contract_version="structure-v7",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=identity,
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )[0]
    worker = control_plane.claim_job(
        worker_id="structure-a", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert worker is not None
    control_plane.record_structure_range(
        worker,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/a/rows.ndjson",
        artifact_digest="a" * 64,
        record_count=0,
        now=now,
    )
    control_plane.finish(worker, state=JobState.SUCCEEDED, now=now)
    certifier = control_plane.claim_job(
        worker_id="structure-certifier",
        job_types=("structure-certify",),
        lease_seconds=30,
        now=now,
    )
    assert certifier is not None
    with pytest.raises(IncompleteStructureGenerationError, match="component-count"):
        control_plane.certify_structure_generation(
            certifier,
            generation_key=spec.generation_key,
            artifact_key="structure-manifests/a/manifest.ndjson",
            artifact_digest="a" * 64,
            now=now,
        )


def test_structure_shadow_pointer_requires_certified_manifest_and_preserves_legacy_truth(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"structure-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""), ("markets", "", "")),
        now=now,
    )[0]
    with pytest.raises(IncompleteStructureGenerationError):
        control_plane.publish_structure_shadow(generation_key=spec.generation_key, now=now)

    # A certifier's durable manifest is the only accepted shadow-pointer source.
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        connection.execute(
            """
            INSERT INTO m1_generation_manifests (
                generation_key, producer_job_key, input_digest, artifact_key,
                artifact_digest, record_count, published_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                spec.generation_key,
                f"{spec.generation_key}:certify",
                bundle.sha256,
                "structure-manifests/a/manifest.ndjson",
                "a" * 64,
                2,
                now,
            ),
        )
    assert (
        control_plane.publish_structure_shadow(generation_key=spec.generation_key, now=now)
        == spec.generation_key
    )
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        pointer = connection.execute(
            """
            SELECT generation_key, expected_generation_key
            FROM m1_publication_pointers WHERE pointer_key = 'structure:current:shadow'
            """
        ).fetchone()
        legacy = connection.execute(
            "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key = 'structure:current'"
        ).fetchone()
    assert pointer == (spec.generation_key, None)
    assert legacy == (0,)


def test_quote_batch_input_preserves_leg_identity_for_worker_takeover(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    admitted = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        legs=(_leg("token-2"), _leg("token-1")),
        batch_size=1,
        now=now,
    )

    replacement_input = control_plane.quote_batch_spec(admitted[0].job_key)

    assert replacement_input.legs == (_leg("token-1"),)
    assert replacement_input.token_ids == ("token-1",)


def test_quote_batch_receipt_is_fenced_and_idempotent(control_plane: PostgresControlPlane) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1",),
        batch_size=1,
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="worker-a", job_types=("quote-batch",), lease_seconds=1, now=now
    )
    assert lease is not None
    first = control_plane.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )
    assert (
        control_plane.record_quote_batch(
            lease,
            token_range_digest=batch.token_range_digest,
            quote_digest="c" * 64,
            artifact_key="quote-batches/c/batch.ndjson",
            artifact_digest="c" * 64,
            successful_response_count=1,
            quoted_at=now,
            now=now + timedelta(seconds=1),
        )
        == first
    )
    replacement = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("quote-batch",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert replacement is not None
    with pytest.raises(StaleLeaseError):
        control_plane.record_quote_batch(
            lease,
            token_range_digest=batch.token_range_digest,
            quote_digest="d" * 64,
            artifact_key="quote-batches/d/batch.ndjson",
            artifact_digest="d" * 64,
            successful_response_count=1,
            quoted_at=now,
            now=now + timedelta(seconds=3),
        )


def test_checkpointed_quote_batch_stays_with_original_lease_until_expiry(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1",),
        batch_size=1,
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="worker-a", job_types=("quote-batch",), lease_seconds=30, now=now
    )
    assert lease is not None
    control_plane.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )

    assert (
        control_plane.claim_job(
            worker_id="worker-b",
            job_types=("quote-batch",),
            lease_seconds=30,
            now=now + timedelta(seconds=1),
        )
        is None
    )

    replacement = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("quote-batch",),
        lease_seconds=30,
        now=now + timedelta(seconds=31),
    )
    assert replacement is not None
    assert replacement.lease_epoch == lease.lease_epoch + 1


def test_replacement_lease_can_finish_an_already_recorded_quote_batch(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1",),
        batch_size=1,
        now=now,
    )[0]
    first = control_plane.claim_job(
        worker_id="worker-a", job_types=("quote-batch",), lease_seconds=1, now=now
    )
    assert first is not None
    receipt = control_plane.record_quote_batch(
        first,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )

    replacement = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("quote-batch",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert replacement is not None
    assert replacement.lease_epoch == 2
    assert (
        control_plane.record_quote_batch(
            replacement,
            token_range_digest=batch.token_range_digest,
            quote_digest="c" * 64,
            artifact_key="quote-batches/c/batch.ndjson",
            artifact_digest="c" * 64,
            successful_response_count=1,
            quoted_at=now,
            now=now + timedelta(seconds=2),
        )
        == receipt
    )
    control_plane.finish(replacement, state=JobState.SUCCEEDED, now=now + timedelta(seconds=3))
    prior = control_plane.quote_batch_receipt(batch.job_key)
    assert prior is not None
    assert prior.artifact_digest == "c" * 64
    assert prior.successful_response_count == 1


def test_transactional_quote_worker_commits_fenced_artifact_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        legs=(_leg("token-1"),),
        batch_size=1,
        now=now,
    )[0]
    objects = _MemoryObjects()
    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=_OneBookReader(),
        object_client=objects,
        bucket="quotes",
        worker_id="quote-worker",
        now=lambda: now,
    )

    result = asyncio.run(worker.run_once())

    assert result.outcome == "succeeded"
    receipt = control_plane.quote_batch_receipt(batch.job_key)
    assert receipt is not None
    assert receipt.artifact_key == objects.object["Key"]
    assert receipt.artifact_digest == objects.object["Metadata"]["sha256"]


def test_quote_worker_takeover_after_upload_before_receipt_has_one_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    """Quote retry reuses frozen batch input and creates one durable effect."""
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        legs=(_leg("token-1"),),
        batch_size=1,
        now=now,
    )[0]

    class MemoryR2:
        def __init__(self) -> None:
            self.objects: dict[str, bytes] = {}
            self.metadata: dict[str, dict[str, object]] = {}
            self.put_calls = 0

        def put_object(self, **kwargs: object) -> None:
            self.put_calls += 1
            key = str(kwargs["Key"])
            self.objects[key] = bytes(kwargs["Body"])
            self.metadata[key] = dict(kwargs["Metadata"])

        def head_object(self, **kwargs: object) -> dict[str, object]:
            key = str(kwargs["Key"])
            return {
                "ContentLength": len(self.objects[key]),
                "Metadata": self.metadata[key],
            }

    class CrashBeforeReceipt:
        def __init__(self, delegate: PostgresControlPlane) -> None:
            self._delegate = delegate
            self.crash = True

        def __getattr__(self, name: str):
            return getattr(self._delegate, name)

        def record_quote_batch(self, *args: object, **kwargs: object):
            if self.crash:
                self.crash = False
                raise KeyboardInterrupt("simulated process death after R2 upload")
            return self._delegate.record_quote_batch(*args, **kwargs)

    objects = MemoryR2()
    first = TransactionalQuoteBatchWorker(
        control_plane=CrashBeforeReceipt(control_plane),  # type: ignore[arg-type]
        reader=_OneBookReader(),
        object_client=objects,
        bucket="quotes",
        worker_id="crashed-worker",
        now=lambda: now,
        lease_seconds=1,
    )
    with pytest.raises(KeyboardInterrupt, match="after R2 upload"):
        asyncio.run(first.run_once())
    assert control_plane.quote_batch_receipt(batch.job_key) is None

    replacement = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=_OneBookReader(),
        object_client=objects,
        bucket="quotes",
        worker_id="replacement-worker",
        now=lambda: now + timedelta(seconds=2),
        lease_seconds=30,
    )
    assert asyncio.run(replacement.run_once()).outcome == "succeeded"
    receipt = control_plane.quote_batch_receipt(batch.job_key)
    assert receipt is not None
    assert objects.put_calls == 2
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        count = connection.execute(
            "SELECT count(*) FROM m1_quote_batch_receipts WHERE job_key = %s", (batch.job_key,)
        ).fetchone()
        pointer = connection.execute(
            "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key = 'quote:current'"
        ).fetchone()
    assert count == (1,)
    assert pointer == (0,)


def test_transactional_quote_certifier_waits_then_publishes_complete_generation(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        legs=(_leg("token-1"), _leg("token-2")),
        batch_size=1,
        now=now,
    )
    clock = [now]
    certifier = TransactionalQuoteCertifier(
        control_plane=control_plane,
        worker_id="certifier",
        now=lambda: clock[0],
    )
    assert certifier.run_once().outcome == "waiting"

    worker = TransactionalQuoteBatchWorker(
        control_plane=control_plane,
        reader=_OneBookReader(),
        object_client=_MemoryObjects(),
        bucket="quotes",
        worker_id="quote-worker",
        now=lambda: now,
    )
    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    assert asyncio.run(worker.run_once()).outcome == "succeeded"
    clock[0] = now + timedelta(seconds=6)
    assert certifier.run_once().outcome == "certified"

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key = 'quote:current'"
            )
            assert cursor.fetchone() == (batches[0].generation_key,)
    finally:
        connection.close()
    quote_status = control_plane.operational_snapshot(now=clock[0])["quote"]
    assert quote_status["batch_job_states"] == {"succeeded": 2}
    assert quote_status["certifier_job_states"] == {"succeeded": 1}
    assert quote_status["current_pointer"] is not None
    assert quote_status["current_pointer"]["generation_key"] == batches[0].generation_key


def test_incomplete_quote_generation_cannot_switch_current_pointer(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1", "token-2", "token-3"),
        batch_size=1,
        now=now,
    )
    batch_lease = control_plane.claim_job(
        worker_id="quote-worker", job_types=("quote-batch",), lease_seconds=30, now=now
    )
    assert batch_lease is not None
    control_plane.record_quote_batch(
        batch_lease,
        token_range_digest=batch_lease.input_identity.rsplit(":", maxsplit=1)[1],
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )
    certifier = control_plane.claim_job(
        worker_id="certifier", job_types=("quote-certify",), lease_seconds=30, now=now
    )
    assert certifier is not None

    with pytest.raises(IncompleteQuoteGenerationError):
        control_plane.certify_quote_generation(
            certifier,
            generation_key=batches[0].generation_key,
            now=now + timedelta(seconds=1),
        )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM m1_publication_pointers")
            assert cursor.fetchone() == (0,)
    finally:
        connection.close()


def test_complete_quote_generation_certifies_and_publishes_one_pointer(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1", "token-2"),
        batch_size=1,
        now=now,
    )
    for ordinal, batch in enumerate(batches):
        batch_now = now + timedelta(seconds=ordinal)
        lease = control_plane.claim_job(
            worker_id=f"quote-worker-{ordinal}",
            job_types=("quote-batch",),
            lease_seconds=30,
            now=batch_now,
        )
        assert lease is not None
        control_plane.record_quote_batch(
            lease,
            token_range_digest=batch.token_range_digest,
            quote_digest=chr(ord("c") + ordinal) * 64,
            artifact_key=f"quote-batches/{ordinal}/batch.ndjson",
            artifact_digest=chr(ord("c") + ordinal) * 64,
            successful_response_count=1,
            quoted_at=batch_now,
            now=batch_now,
        )
        control_plane.finish(
            lease,
            state=JobState.SUCCEEDED,
            now=batch_now + timedelta(milliseconds=1),
        )
    certifier = control_plane.claim_job(
        worker_id="certifier", job_types=("quote-certify",), lease_seconds=30, now=now
    )
    assert certifier is not None

    artifact_digest = control_plane.certify_quote_generation(
        certifier,
        generation_key=batches[0].generation_key,
        now=now + timedelta(seconds=3),
    )

    assert len(artifact_digest) == 64
    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT generation_key FROM m1_publication_pointers "
                "WHERE pointer_key='quote:current'"
            )
            assert cursor.fetchone() == (batches[0].generation_key,)
            cursor.execute(
                "SELECT job_type, input_identity, state FROM m1_jobs WHERE job_key=%s",
                (f"{batches[0].generation_key}:opportunity-certify",),
            )
            assert cursor.fetchone() == (
                "opportunity-certify",
                batches[0].generation_key,
                "runnable",
            )
    finally:
        connection.close()


def test_opportunity_projection_publish_is_atomic_and_current_pointer_is_pageable(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    quote_generation = "quote:" + "a" * 64
    structure_generation = "structure:" + "b" * 64
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        for generation_key in (quote_generation, structure_generation):
            job_key = f"{generation_key}:certify"
            connection.execute(
                """INSERT INTO m1_jobs(job_key,job_type,input_identity,state,created_at,updated_at)
                   VALUES (%s,'certify',%s,'succeeded',%s,%s)""",
                (job_key, generation_key, now, now),
            )
            connection.execute(
                """INSERT INTO m1_generation_manifests
                   (generation_key,producer_job_key,input_digest,artifact_key,artifact_digest,record_count,published_at)
                   VALUES (%s,%s,%s,%s,%s,1,%s)""",
                (generation_key, job_key, "c" * 64, "artifact", "d" * 64, now),
            )
        connection.execute(
            """INSERT INTO m1_publication_pointers
               (pointer_key,generation_key,expected_generation_key,lease_epoch,published_at)
               VALUES ('quote:current',%s,NULL,1,%s)""",
            (quote_generation, now),
        )

    row = {
        "group_id": "group-a",
        "event_id": "event-a",
        "membership_hash": "membership-a",
        "bundle_cost": 0.91,
        "gross_edge_bps": 900.0,
        "max_bundle_size": 4.0,
        "legs": [{"yes_token_id": "token-a", "ask_price": 0.91, "ask_size": 4.0}],
        "structure_observed_at_ms": 1,
        "quote_started_at_ms": 2,
        "quote_quoted_at_ms": 3,
    }
    digest = control_plane.publish_opportunity_projection(
        quote_generation_key=quote_generation,
        structure_generation_key=structure_generation,
        rows=(row,),
        now=now,
    )

    assert len(digest) == 64
    assert control_plane.current_opportunities(limit=1, after_group_id="") == {
        "status": "available",
        "current_opportunity_count": 1,
        "items": [row],
        "limit": 1,
        "next_after_group_id": None,
    }
    with pytest.raises(ValueError, match="invalid-opportunity-projection-row"):
        control_plane.publish_opportunity_projection(
            quote_generation_key=quote_generation,
            structure_generation_key=structure_generation,
            rows=({"group_id": "bad"},),
            now=now,
        )
    assert control_plane.current_opportunities(limit=1, after_group_id="")["items"] == [row]


def test_current_quote_projection_inputs_follows_quote_to_structure_admission_contract(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    structure_generation = "structure:" + "a" * 64
    quote_generation = "quote:" + "a" * 64
    batch_key = f"{quote_generation}:batch:0"
    leg_payload = psycopg.types.json.Jsonb(
        [
            {
                "neg_risk_market_id": "neg-risk-token-a",
                "market_id": "market-token-a",
                "condition_id": "condition-token-a",
                "slug": "slug-token-a",
                "yes_token_id": "token-a",
                "event_id": "event-token-a",
                "membership_hash": "membership-token-a",
            }
        ]
    )
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        connection.execute(
            """INSERT INTO m1_structure_generation_inputs
               (generation_key,bundle_key,bundle_digest,identity,admitted_at)
               VALUES (%s,'bundle',%s,%s,%s)""",
            (structure_generation, "a" * 64, psycopg.types.json.Jsonb({}), now),
        )
        for generation_key in (structure_generation, quote_generation):
            job_key = f"{generation_key}:certify"
            connection.execute(
                """INSERT INTO m1_jobs(job_key,job_type,input_identity,state,created_at,updated_at)
                   VALUES (%s,'certify',%s,'succeeded',%s,%s)""",
                (job_key, generation_key, now, now),
            )
            connection.execute(
                """INSERT INTO m1_generation_manifests
                   (generation_key,producer_job_key,input_digest,artifact_key,artifact_digest,record_count,published_at)
                   VALUES (%s,%s,%s,'artifact',%s,1,%s)""",
                (generation_key, job_key, "b" * 64, "c" * 64, now),
            )
        for job_key, job_type in (
            (f"{structure_generation}:quote-admit", "quote-admit"),
            (batch_key, "quote-batch"),
        ):
            connection.execute(
                """INSERT INTO m1_jobs(job_key,job_type,input_identity,state,created_at,updated_at)
                   VALUES (%s,%s,%s,'succeeded',%s,%s)""",
                (job_key, job_type, job_key, now, now),
            )
        connection.execute(
            """INSERT INTO m1_publication_pointers
               (pointer_key,generation_key,expected_generation_key,lease_epoch,published_at)
               VALUES ('quote:current',%s,NULL,1,%s)""",
            (quote_generation, now),
        )
        connection.execute(
            """INSERT INTO m1_quote_admission_inputs
               (job_key,generation_key,bundle_key,bundle_digest,admitted_at)
               VALUES (%s,%s,'bundle',%s,%s)""",
            (f"{structure_generation}:quote-admit", structure_generation, "b" * 64, now),
        )
        connection.execute(
            """INSERT INTO m1_quote_batch_inputs
               (job_key,structure_receipt_digest,universe_hash,token_range_digest,token_ids,legs,admitted_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                batch_key,
                "a" * 64,
                "b" * 64,
                "c" * 64,
                psycopg.types.json.Jsonb(["token-a"]),
                leg_payload,
                now,
            ),
        )
        connection.execute(
            """INSERT INTO m1_quote_batch_receipts
               (job_key,structure_receipt_digest,universe_hash,token_range_digest,quote_digest,
                artifact_key,artifact_digest,successful_response_count,quoted_at,committed_at)
               VALUES (%s,%s,%s,%s,%s,'quotes/key',%s,1,%s,%s)""",
            (batch_key, "a" * 64, "b" * 64, "c" * 64, "d" * 64, "e" * 64, now, now),
        )

    actual_quote, actual_structure, batches = control_plane.current_quote_projection_inputs()

    assert (actual_quote, actual_structure) == (quote_generation, structure_generation)
    assert batches[0][0] == (_leg("token-a"),)
    assert batches[0][1].artifact_key == "quotes/key"


def test_claim_reclaim_and_epoch_fencing(control_plane: PostgresControlPlane) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:alpha",
        job_type="structure-normalize",
        input_identity="alpha",
        now=now,
    )

    first = control_plane.claim_job(
        worker_id="worker-a", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert first is not None
    assert first.lease_epoch == 1
    assert first.state == JobState.LEASED
    assert (
        control_plane.claim_job(
            worker_id="worker-b", job_types=("structure-normalize",), lease_seconds=30, now=now
        )
        is None
    )
    second = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=31),
    )
    assert second is not None
    assert second.lease_epoch == 2
    with pytest.raises(StaleLeaseError):
        control_plane.heartbeat(first, now=now + timedelta(seconds=31))


def test_checkpoint_is_idempotent_and_fenced(control_plane: PostgresControlPlane) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="quote:alpha", job_type="quote-scan", input_identity="alpha", now=now
    )
    lease = control_plane.claim_job(
        worker_id="worker-a", job_types=("quote-scan",), lease_seconds=30, now=now
    )
    assert lease is not None
    receipt = control_plane.checkpoint(
        lease,
        checkpoint_cursor="page:1",
        checkpoint_digest="a" * 64,
        idempotency_key="checkpoint:quote:alpha:1",
        now=now + timedelta(seconds=1),
    )
    duplicate = control_plane.checkpoint(
        lease,
        checkpoint_cursor="page:1",
        checkpoint_digest="a" * 64,
        idempotency_key="checkpoint:quote:alpha:1",
        now=now + timedelta(seconds=2),
    )
    assert duplicate == receipt
    with pytest.raises(CheckpointConflictError):
        control_plane.checkpoint(
            lease,
            checkpoint_cursor="page:2",
            checkpoint_digest="b" * 64,
            idempotency_key="checkpoint:quote:alpha:1",
            now=now + timedelta(seconds=2),
        )

    later = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("quote-scan",),
        lease_seconds=30,
        now=now + timedelta(seconds=3),
    )
    assert later is not None
    assert later.checkpoint_cursor == "page:1"
    with pytest.raises(StaleLeaseError):
        control_plane.checkpoint(
            lease,
            checkpoint_cursor="page:2",
            checkpoint_digest="b" * 64,
            idempotency_key="checkpoint:quote:alpha:2",
            now=now + timedelta(seconds=4),
        )


def test_materializer_shard_checkpoints_are_ordered_and_fenced(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="source-window:1:materialize",
        job_type="structure-materialize",
        input_identity="source-window:1",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="worker-a", job_types=("structure-materialize",), lease_seconds=30, now=now
    )
    assert lease is not None
    control_plane.checkpoint(
        lease,
        checkpoint_cursor="shard:00000001",
        checkpoint_digest="a" * 64,
        artifact_key="structure-shards/a/rows.ndjson",
        idempotency_key="structure-materializer:source-window:1:1:a",
        now=now + timedelta(seconds=1),
    )

    assert control_plane.structure_materializer_shards("source-window:1") == (
        ("shard:00000001", "a" * 64, "structure-shards/a/rows.ndjson"),
    )
    resumed = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert resumed is not None
    assert resumed.checkpoint_cursor == "shard:00000001"
    with pytest.raises(StaleLeaseError):
        control_plane.checkpoint(
            lease,
            checkpoint_cursor="shard:00000002",
            checkpoint_digest="b" * 64,
            artifact_key="structure-shards/b/rows.ndjson",
            idempotency_key="structure-materializer:source-window:1:2:b",
            now=now + timedelta(seconds=3),
        )


def test_materializer_batch_receipts_are_ordered_by_checkpoint_cursor(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="source-window:2:materialize",
        job_type="structure-materialize",
        input_identity="source-window:2",
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="worker-a", job_types=("structure-materialize",), lease_seconds=30, now=now
    )
    assert first is not None
    control_plane.checkpoint(
        first,
        checkpoint_cursor="shard-batch:00000000",
        checkpoint_digest="a" * 64,
        artifact_key="structure-shard-batches/a/batch.ndjson",
        idempotency_key="batch:0",
        now=now + timedelta(seconds=1),
    )
    second = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert second is not None
    control_plane.checkpoint(
        second,
        checkpoint_cursor="shard-batch:00000004",
        checkpoint_digest="b" * 64,
        artifact_key="structure-shard-batches/b/batch.ndjson",
        idempotency_key="batch:4",
        now=now + timedelta(seconds=3),
    )

    assert control_plane.structure_materializer_batches("source-window:2") == (
        ("shard-batch:00000000", "a" * 64, "structure-shard-batches/a/batch.ndjson"),
        ("shard-batch:00000004", "b" * 64, "structure-shard-batches/b/batch.ndjson"),
    )


def test_retry_preserves_checkpoint_and_quarantine_stops_claims(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:retry", job_type="structure-normalize", input_identity="retry", now=now
    )
    lease = control_plane.claim_job(
        worker_id="worker-a", job_types=("structure-normalize",), lease_seconds=30, now=now
    )
    assert lease is not None
    control_plane.checkpoint(
        lease,
        checkpoint_cursor="chunk:7",
        checkpoint_digest="c" * 64,
        idempotency_key="checkpoint:structure:retry:7",
        now=now + timedelta(seconds=1),
    )
    resumed = control_plane.claim_job(
        worker_id="worker-a",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert resumed is not None
    control_plane.finish(
        resumed,
        state=JobState.RETRYABLE,
        now=now + timedelta(seconds=3),
        next_attempt_at=now + timedelta(minutes=1),
        error_class="upstream-timeout",
    )
    assert (
        control_plane.claim_job(
            worker_id="worker-b",
            job_types=("structure-normalize",),
            lease_seconds=30,
            now=now + timedelta(seconds=4),
        )
        is None
    )
    retry = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(minutes=1),
    )
    assert retry is not None
    assert retry.checkpoint_cursor == "chunk:7"
    control_plane.finish(
        retry, state=JobState.QUARANTINED, now=now + timedelta(minutes=1, seconds=1)
    )
    assert (
        control_plane.claim_job(
            worker_id="worker-c",
            job_types=("structure-normalize",),
            lease_seconds=30,
            now=now + timedelta(days=1),
        )
        is None
    )


def test_due_retryable_job_claims_before_new_runnable_work(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:retry-first",
        job_type="structure-normalize",
        input_identity="retry-first",
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="worker-a",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert first is not None
    control_plane.finish(
        first,
        state=JobState.RETRYABLE,
        now=now + timedelta(seconds=1),
        next_attempt_at=now + timedelta(seconds=5),
        error_class="upstream-timeout",
    )
    control_plane.enqueue_job(
        job_key="structure:new-work",
        job_type="structure-normalize",
        input_identity="new-work",
        now=now + timedelta(seconds=3),
    )

    claimed = control_plane.claim_job(
        worker_id="worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=6),
    )

    assert claimed is not None
    assert claimed.job_key == "structure:retry-first"


def test_incident_event_and_alert_outbox_are_one_idempotent_transaction(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    event_id = control_plane.record_incident_event(
        incident_key="incident:structure-timeout",
        dedupe_key="structure-timeout:892",
        component="structure-publication",
        severity="critical",
        summary="Structure publication child exceeded its budget",
        kind="attempt-failed",
        detail={"generation": 892, "reason": "snapshot-subprocess-timeout"},
        idempotency_key="incident-event:structure-timeout:892:1",
        channels=("dashboard", "webhook"),
        now=now,
    )
    assert event_id == control_plane.record_incident_event(
        incident_key="incident:structure-timeout",
        dedupe_key="structure-timeout:892",
        component="structure-publication",
        severity="critical",
        summary="Structure publication child exceeded its budget",
        kind="attempt-failed",
        detail={"generation": 892, "reason": "snapshot-subprocess-timeout"},
        idempotency_key="incident-event:structure-timeout:892:1",
        channels=("dashboard", "webhook"),
        now=now,
    )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM m1_incident_events")
            assert cursor.fetchone() == (1,)
            cursor.execute("SELECT channel FROM m1_alert_outbox ORDER BY channel")
            assert cursor.fetchall() == [("dashboard",), ("webhook",)]
    finally:
        connection.close()


def test_retryable_finish_creates_one_durable_incident_and_alert_intent(
    control_plane: PostgresControlPlane,
) -> None:
    """A retry cannot become invisible between job mutation and alert intent."""
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:window-a:fetch:events:0",
        job_type="structure-fetch",
        input_identity="window-a:events:0:<start>",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="structure-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None

    control_plane.finish_retryable_with_incident(
        lease,
        error_class="TimeoutError",
        incident_key="incident:job-retry:structure:window-a:fetch:events:0",
        dedupe_key="job-retry:structure:window-a:fetch:events:0",
        component="structure-fetch",
        summary="structure-fetch retryable failure",
        detail={"job_key": lease.job_key, "lease_epoch": lease.lease_epoch},
        channels=("dashboard",),
        now=now,
    )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT state, last_error_class FROM m1_jobs WHERE job_key = %s",
                (lease.job_key,),
            )
            assert cursor.fetchone() == ("retryable", "TimeoutError")
            cursor.execute("SELECT count(*) FROM m1_incidents")
            assert cursor.fetchone() == (1,)
            cursor.execute("SELECT kind FROM m1_incident_events")
            assert cursor.fetchone() == ("attempt-failed",)
            cursor.execute("SELECT channel, state FROM m1_alert_outbox")
            assert cursor.fetchone() == ("dashboard", "pending")
    finally:
        connection.close()


def test_retry_circuit_opens_on_third_failure_with_bounded_probe_delay(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:window-a:fetch:events:0",
        job_type="structure-fetch",
        input_identity="window-a:events:0:<start>",
        now=now,
    )
    delays: list[timedelta] = []
    for attempt in range(1, 8):
        attempted_at = now + sum(delays, timedelta())
        lease = control_plane.claim_job(
            worker_id=f"worker-{attempt}",
            job_types=("structure-fetch",),
            lease_seconds=30,
            now=attempted_at,
        )
        assert lease is not None
        next_attempt_at = control_plane.finish_retryable_with_incident(
            lease,
            error_class="TimeoutError",
            incident_key="incident:job-retry:structure:window-a:fetch:events:0",
            dedupe_key="job-retry:structure:window-a:fetch:events:0",
            component="structure-fetch",
            summary="structure-fetch retryable failure",
            detail={"job_key": lease.job_key},
            channels=("dashboard",),
            now=attempted_at,
        )
        delays.append(next_attempt_at - attempted_at)

    assert delays == [
        timedelta(seconds=15),
        timedelta(seconds=30),
        timedelta(seconds=60),
        timedelta(seconds=120),
        timedelta(seconds=240),
        timedelta(seconds=300),
        timedelta(seconds=300),
    ]
    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT consecutive_failures, state FROM m1_job_circuits WHERE job_key = %s",
                ("structure:window-a:fetch:events:0",),
            )
            assert cursor.fetchone() == (7, "open")
            cursor.execute(
                "SELECT kind FROM m1_incident_events ORDER BY occurred_at, incident_event_id"
            )
            assert [row[0] for row in cursor.fetchall()] == [
                "attempt-failed",
                "attempt-failed",
                "circuit-opened",
                "circuit-probe-failed",
                "circuit-probe-failed",
                "circuit-probe-failed",
                "circuit-probe-failed",
            ]
    finally:
        connection.close()


def test_successful_terminal_job_closes_circuit_and_resolves_retry_incident(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    job_key = "structure:window-a:fetch:events:recovery"
    control_plane.enqueue_job(
        job_key=job_key,
        job_type="structure-fetch",
        input_identity="window-a:events:recovery:<start>",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="worker-failure", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert lease is not None
    control_plane.finish_retryable_with_incident(
        lease,
        error_class="TimeoutError",
        incident_key=f"incident:job-retry:{job_key}",
        dedupe_key=f"job-retry:{job_key}",
        component="structure-fetch",
        summary="structure-fetch retryable failure",
        detail={"job_key": job_key},
        channels=("dashboard",),
        now=now,
    )
    recovered = control_plane.claim_job(
        worker_id="worker-recovery",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now + timedelta(seconds=15),
    )
    assert recovered is not None
    control_plane.finish(recovered, state=JobState.SUCCEEDED, now=now + timedelta(seconds=16))
    assert control_plane.record_job_recovery(
        recovered,
        component="structure-fetch",
        channels=("dashboard",),
        now=now + timedelta(seconds=16),
        acceptance_run_id="staging-retry-fault-20260815",
    )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT consecutive_failures, state, next_probe_at "
                "FROM m1_job_circuits WHERE job_key = %s",
                (job_key,),
            )
            assert cursor.fetchone() == (0, "closed", None)
            cursor.execute("SELECT state, resolved_at IS NOT NULL FROM m1_incidents")
            assert cursor.fetchone() == ("resolved", True)
            cursor.execute(
                "SELECT kind FROM m1_incident_events ORDER BY occurred_at, incident_event_id"
            )
            assert [row[0] for row in cursor.fetchall()] == ["attempt-failed", "recovered"]
            cursor.execute(
                "SELECT outbox.payload->>'acceptance_run_id' "
                "FROM m1_alert_outbox AS outbox "
                "JOIN m1_incident_events AS event "
                "ON event.incident_event_id = outbox.incident_event_id "
                "WHERE event.idempotency_key = %s",
                (f"job-recovery:{job_key}:{recovered.lease_epoch}",),
            )
            assert cursor.fetchone() == ("staging-retry-fault-20260815",)
    finally:
        connection.close()


def test_checkpointed_job_closes_circuit_and_resolves_retry_incident(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    job_key = "structure:window-a:materialize:checkpoint-recovery"
    control_plane.enqueue_job(
        job_key=job_key,
        job_type="structure-materialize",
        input_identity="window-a:materialize",
        now=now,
    )
    failed = control_plane.claim_job(
        worker_id="worker-failure", job_types=("structure-materialize",), lease_seconds=30, now=now
    )
    assert failed is not None
    control_plane.finish_retryable_with_incident(
        failed,
        error_class="StructureSourceError",
        incident_key=f"incident:job-retry:{job_key}",
        dedupe_key=f"job-retry:{job_key}",
        component="structure-materialize",
        summary="structure-materialize retryable failure",
        detail={"job_key": job_key},
        channels=("dashboard",),
        now=now,
    )
    recovered = control_plane.claim_job(
        worker_id="worker-recovery",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now + timedelta(seconds=15),
    )
    assert recovered is not None
    control_plane.checkpoint(
        recovered,
        checkpoint_cursor="shard-batch:00000003",
        checkpoint_digest="a" * 64,
        idempotency_key=f"checkpoint:{job_key}:1",
        now=now + timedelta(seconds=16),
    )
    assert control_plane.record_job_recovery(
        recovered,
        component="structure-materialize",
        channels=("dashboard",),
        now=now + timedelta(seconds=16),
    )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT consecutive_failures, state, next_probe_at "
                "FROM m1_job_circuits WHERE job_key = %s",
                (job_key,),
            )
            assert cursor.fetchone() == (0, "closed", None)
            cursor.execute("SELECT state, resolved_at IS NOT NULL FROM m1_incidents")
            assert cursor.fetchone() == ("resolved", True)
            cursor.execute(
                "SELECT kind FROM m1_incident_events ORDER BY occurred_at, incident_event_id"
            )
            assert [row[0] for row in cursor.fetchall()] == ["attempt-failed", "recovered"]
    finally:
        connection.close()


def test_alert_delivery_lease_records_one_receipt_and_fences_stale_worker(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.record_incident_event(
        incident_key="incident:source-timeout",
        dedupe_key="source-timeout",
        component="structure-fetch",
        severity="warning",
        summary="source fetch timed out",
        kind="attempt-failed",
        detail={"job_key": "source-window:one:fetch:events:0"},
        idempotency_key="source-timeout:1",
        channels=("dashboard",),
        now=now,
    )

    lease = control_plane.claim_alert_delivery(
        worker_id="alert-worker-a", lease_seconds=30, now=now
    )
    assert lease is not None
    assert lease.channel == "dashboard"
    assert lease.attempt_number == 1
    control_plane.finish_alert_delivery(
        lease,
        state="delivered",
        provider_receipt="dashboard-visible",
        now=now + timedelta(seconds=1),
    )
    assert (
        control_plane.claim_alert_delivery(
            worker_id="alert-worker-b", lease_seconds=30, now=now + timedelta(minutes=1)
        )
        is None
    )

    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT state, attempt_count FROM m1_alert_outbox")
            assert cursor.fetchone() == ("delivered", 1)
            cursor.execute(
                "SELECT attempt_number, state, provider_receipt FROM m1_alert_deliveries"
            )
            assert cursor.fetchone() == (1, "delivered", "dashboard-visible")
    finally:
        connection.close()


def test_scoped_alert_claim_never_claims_historical_outbox(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    old_event = control_plane.record_incident_event(
        incident_key="incident:old",
        dedupe_key="old",
        component="structure-fetch",
        severity="warning",
        summary="old",
        kind="attempt-failed",
        detail={},
        idempotency_key="old:1",
        channels=("dashboard",),
        now=now,
    )
    scoped_event = control_plane.record_incident_event(
        incident_key="incident:new",
        dedupe_key="new",
        component="structure-fetch",
        severity="warning",
        summary="new",
        kind="attempt-failed",
        detail={},
        idempotency_key="new:1",
        channels=("dashboard",),
        now=now,
    )
    connection = control_plane._connection_factory()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                "UPDATE m1_alert_outbox SET payload = payload || %s::jsonb "
                "WHERE incident_event_id = %s",
                ('{"acceptance_run_id":"run-a"}', scoped_event),
            )
        connection.commit()
    finally:
        connection.close()

    lease = control_plane.claim_alert_delivery(
        worker_id="alert-worker-a", lease_seconds=30, now=now, acceptance_run_id="run-a"
    )
    assert lease is not None
    assert lease.incident_event_id == scoped_event
    assert lease.incident_event_id != old_event


def test_operational_snapshot_reads_fenced_work_and_alert_intent(
    control_plane: PostgresControlPlane,
) -> None:
    """The operator plane reads Postgres evidence without SQLite authority."""
    now = datetime(2026, 8, 12, tzinfo=UTC)
    control_plane.enqueue_job(
        job_key="structure:window-a",
        job_type="structure-fetch",
        input_identity="window-a",
        now=now - timedelta(seconds=90),
    )
    control_plane.enqueue_job(
        job_key="quote:generation-a:batch-0001",
        job_type="quote-batch",
        input_identity="generation-a:batch-0001",
        now=now,
    )
    control_plane.enqueue_job(
        job_key="structure:window-a:materialize",
        job_type="structure-materialize",
        input_identity="window-a",
        now=now,
    )
    control_plane.enqueue_job(
        job_key="structure:generation-a:quote-admit",
        job_type="quote-admit",
        input_identity="structure:generation-a:bundle:abc",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="structure-worker-a",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now - timedelta(seconds=60),
    )
    assert lease is not None
    control_plane.record_incident_event(
        incident_key="quote-unavailable-a",
        dedupe_key="quote-unavailable-a",
        component="quote",
        severity="critical",
        summary="certified quote projection unavailable",
        kind="detected",
        detail={"run_id": "3035"},
        idempotency_key="quote-unavailable-a:detected",
        channels=("telegram",),
        now=now,
    )

    snapshot = control_plane.operational_snapshot(now=now, sample_limit=20)

    assert snapshot["job_counts"] == {"leased": 1, "runnable": 3}
    assert snapshot["oldest_runnable_age_seconds"] == 0.0
    assert snapshot["expired_leases"] == 1
    assert snapshot["open_circuit_count"] == 0
    assert snapshot["open_circuits"] == []
    assert snapshot["quote"] == {
        "admission_job_states": {"runnable": 1},
        "oldest_retryable_admission_age_seconds": None,
        "batch_job_states": {"runnable": 1},
        "certifier_job_states": {},
        "oldest_retryable_batch_age_seconds": None,
        "current_pointer": None,
    }
    assert snapshot["structure"] == {
        "source_fetch_job_states": {"leased": 1},
        "oldest_retryable_source_age_seconds": None,
        "source_materializer_job_states": {"runnable": 1},
        "range_job_states": {},
        "certifier_job_states": {},
        "oldest_retryable_range_age_seconds": None,
        "latest_manifest": None,
        "shadow_pointer": None,
    }
    assert snapshot["recent_attempts"] == [
        {
            "job_key": "structure:window-a",
            "lease_epoch": 1,
            "worker_id": "structure-worker-a",
            "state": "running",
        }
    ]
    assert snapshot["open_incidents"] == [
        {
            "incident_key": "quote-unavailable-a",
            "component": "quote",
            "severity": "critical",
            "summary": "certified quote projection unavailable",
        }
    ]
    assert snapshot["pending_alert_outbox"] == [
        {
            "incident_key": "quote-unavailable-a",
            "channel": "telegram",
            "state": "pending",
        }
    ]


def test_operational_snapshot_projects_bounded_next_claimable_per_pool(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="structure:one:normalize:0",
        job_type="structure-normalize",
        input_identity="structure-one",
        now=now - timedelta(seconds=90),
    )
    control_plane.enqueue_job(
        job_key="quote:one:batch:0",
        job_type="quote-batch",
        input_identity="quote-one",
        now=now - timedelta(seconds=30),
    )

    snapshot = control_plane.operational_snapshot(now=now)

    assert snapshot["queue_health"] == {
        "structure-range": {
            "unfinished_count": 1,
            "oldest_age_seconds": 90.0,
            "next_job_key": "structure:one:normalize:0",
        },
        "quote-batch": {
            "unfinished_count": 1,
            "oldest_age_seconds": 30.0,
            "next_job_key": "quote:one:batch:0",
        },
    }


def test_operational_snapshot_reports_retry_age_for_source_and_quote_admission(
    control_plane: PostgresControlPlane,
) -> None:
    now = datetime(2026, 8, 12, tzinfo=UTC)
    control_plane.enqueue_job(
        job_key="source-retry", job_type="structure-fetch", input_identity="source", now=now
    )
    source_lease = control_plane.claim_job(
        worker_id="source", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert source_lease is not None
    control_plane.finish(
        source_lease,
        state=JobState.RETRYABLE,
        next_attempt_at=now + timedelta(seconds=15),
        error_class="TimeoutError",
        now=now,
    )
    control_plane.enqueue_job(
        job_key="quote-admit-retry", job_type="quote-admit", input_identity="quote", now=now
    )
    quote_lease = control_plane.claim_job(
        worker_id="quote", job_types=("quote-admit",), lease_seconds=30, now=now
    )
    assert quote_lease is not None
    control_plane.finish(
        quote_lease,
        state=JobState.RETRYABLE,
        next_attempt_at=now + timedelta(seconds=15),
        error_class="QuoteAdmissionError",
        now=now,
    )

    snapshot = control_plane.operational_snapshot(now=now + timedelta(seconds=45))

    assert snapshot["structure"]["oldest_retryable_source_age_seconds"] == 45.0
    assert snapshot["quote"]["oldest_retryable_admission_age_seconds"] == 45.0
