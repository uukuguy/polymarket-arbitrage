"""Real-Postgres contracts for fenced M1 job coordination."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from threading import Barrier
from typing import Any, LiteralString, cast

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from polyarb.control_plane import postgres as postgres_module
from polyarb.control_plane.models import (
    JobLease,
    JobState,
    QuoteBatchLeg,
    QuoteBatchSpec,
    StructureSourcePageSpec,
)
from polyarb.control_plane.postgres import (
    CheckpointConflictError,
    ControlPlaneError,
    IncompleteQuoteGenerationError,
    IncompleteStructureGenerationError,
    PostgresControlPlane,
    RuntimeEventConflictError,
    RuntimeProgressConflictError,
    StaleLeaseError,
)
from polyarb.control_plane.qualification import (
    QualificationDecision,
    QualificationFact,
    QualificationState,
    RollingQualificationPolicy,
)
from polyarb.control_plane.qualification_service import (
    PostgresQualificationFactSource,
    PostgresQualificationServiceStore,
    QualificationService,
)
from polyarb.control_plane.qualification_store import (
    QualificationCertificateConflict,
    QualificationEpochConflict,
    canonical_certificate_bytes,
    certificate_digest,
    insert_qualification_certificate,
    list_qualification_certificates,
    qualification_certificate_payload,
    read_qualification_certificate,
    read_qualification_epoch,
    start_qualification_epoch,
    transition_qualification_epoch,
)
from polyarb.control_plane.quote_worker import (
    TransactionalQuoteBatchWorker,
    TransactionalQuoteCertifier,
)
from polyarb.control_plane.recovery_executor import RecoveryExecutor
from polyarb.control_plane.recovery_models import RecoveryActionType, RecoveryDecision
from polyarb.control_plane.recovery_records import RecoveryActionRecord
from polyarb.control_plane.recovery_store import (
    RecoveryActionConflict,
    claim_action,
    claim_controller,
    execute_claimed_action,
    finish_action,
    read_runtime_controller_status,
    read_runtime_reconcile_states,
    schedule_action,
)
from polyarb.control_plane.runtime_models import RuntimeEvent, RuntimeEventKind, RuntimeProgress
from polyarb.control_plane.runtime_store import (
    RuntimeEventConflict,
    RuntimeFenceError,
    _event_from_row,
    append_runtime_event_cursor,
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
            ["uv", "run", "alembic", "upgrade", "head"],
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
            "m1_qualification_certificates",
            "m1_qualification_epochs",
            "m1_qualification_source_cursors",
            "m1_qualification_ingress_ledger",
            "m1_soak_observations",
            "m1_soak_runs",
            "m1_cloud_usage_observations",
            "m1_recovery_actions",
            "m1_runtime_controller_leases",
            "m1_job_runtime_events",
            "m1_job_runtime_state",
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


def test_cloud_usage_budget_refuses_ninety_percent_without_an_artifact_bypass(
    control_plane: PostgresControlPlane,
) -> None:
    decision = control_plane.record_cloud_usage(
        source="gamma",
        operation="structure-page",
        bytes_received=90,
        item_count=1,
        artifact_key="structure-inputs/a.json",
        artifact_digest="a" * 64,
        daily_budget_bytes=100,
        now=_now(),
    )
    assert decision.allowed is False
    assert decision.used_bytes == 90
    assert decision.threshold_percent == 90
    snapshot = control_plane.operational_snapshot(now=_now())
    assert snapshot["open_incidents"][0]["component"] == "cloud-egress"
    assert {row["channel"] for row in snapshot["pending_alert_outbox"]} == {"dashboard", "telegram"}
    assert snapshot["cloud_usage"]["used_bytes"] == 90
    assert snapshot["cloud_usage"]["daily_budget_bytes"] == 100
    assert snapshot["cloud_usage"]["latest_observation"]["artifact_digest"] == "a" * 64


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


def test_read_soak_observations_uses_bounded_read_only_transaction() -> None:
    commands: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, sql: str, params: object = None) -> None:
            commands.append(" ".join(sql.split()))
            if "FROM m1_soak_observations" in sql:
                assert params == ("formal-cloud-v1",)

        def fetchall(self):
            return [
                {"record": {"observed_at": "2030-01-01T00:00:00+00:00", "sample": 1}},
                {"record": {"observed_at": "2030-01-01T00:05:00+00:00", "sample": 2}},
            ]

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, **kwargs: object):
            assert kwargs == {"row_factory": dict_row}
            return Cursor()

    factory = cast(Callable[[], psycopg.Connection[Any]], lambda: Connection())
    observations = PostgresControlPlane(factory).read_soak_observations("formal-cloud-v1")

    assert observations == (
        {"observed_at": "2030-01-01T00:00:00+00:00", "sample": 1},
        {"observed_at": "2030-01-01T00:05:00+00:00", "sample": 2},
    )
    assert commands[:3] == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = '5000ms'",
        "SELECT record FROM m1_soak_observations WHERE run_id = %s ORDER BY observed_at ASC",
    ]


def test_quote_admission_input_uses_bounded_read_only_transaction() -> None:
    commands: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: object, params: object = None) -> None:
            commands.append(" ".join(str(query).split()))
            if "FROM m1_quote_admission_inputs" in str(query):
                assert params == ("generation:quote-admit",)

        def fetchone(self):
            return {
                "generation_key": "generation-1",
                "bundle_key": "bundles/current.ndjson",
                "bundle_digest": "a" * 64,
            }

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, **kwargs: object):
            assert kwargs == {"row_factory": dict_row}
            return Cursor()

    factory = cast(Callable[[], psycopg.Connection[Any]], lambda: Connection())
    result = PostgresControlPlane(factory).quote_admission_input("generation:quote-admit")

    assert result == ("generation-1", "bundles/current.ndjson", "a" * 64)
    assert commands[:4] == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
        (
            "SELECT generation_key, bundle_key, bundle_digest "
            "FROM m1_quote_admission_inputs WHERE job_key = %s"
        ),
    ]


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


def _seed_quote_admission_job(
    control_plane: PostgresControlPlane,
    *,
    generation_key: str,
    now: datetime,
    lease_seconds: int,
) -> tuple[JobLease, str, tuple[QuoteBatchSpec, ...]]:
    bundle_digest = "a" * 64
    job_key = f"{generation_key}:quote-admit"
    control_plane.enqueue_job(
        job_key=job_key,
        job_type="quote-admit",
        input_identity=f"{generation_key}:bundles/current.ndjson:{bundle_digest}",
        now=now,
    )
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_structure_generation_inputs "
            "(generation_key, bundle_key, bundle_digest, identity, admitted_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (generation_key, "bundles/current.ndjson", bundle_digest, Jsonb({}), now),
        )
        connection.execute(
            "INSERT INTO m1_quote_admission_inputs "
            "(job_key, generation_key, bundle_key, bundle_digest, admitted_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (job_key, generation_key, "bundles/current.ndjson", bundle_digest, now),
        )
    lease = control_plane.claim_job(
        worker_id="quote-admitter",
        job_types=("quote-admit",),
        lease_seconds=lease_seconds,
        now=now,
    )
    assert lease is not None
    batches = control_plane.quote_batches_from_legs(
        structure_receipt_digest=bundle_digest,
        universe_hash="b" * 64,
        legs=(_leg(f"token-{generation_key}"),),
        batch_size=100,
    )
    return lease, bundle_digest, batches


def _seed_claimed_job(
    control_plane: PostgresControlPlane,
    *,
    job_key: str,
    job_type: str,
    input_identity: str,
    now: datetime,
    lease_seconds: int = 30,
) -> JobLease:
    control_plane.enqueue_job(
        job_key=job_key,
        job_type=job_type,
        input_identity=input_identity,
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id=f"worker:{job_key}",
        job_types=(job_type,),
        lease_seconds=lease_seconds,
        now=now,
    )
    assert lease is not None
    return lease


def _runtime_attempt_id(control_plane: PostgresControlPlane, job_key: str) -> str:
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT attempt_id FROM m1_job_runtime_state WHERE job_key = %s", (job_key,))
        row = cursor.fetchone()
        assert row is not None
        return str(row[0])


def _recovery_decision(now: datetime) -> RecoveryDecision:
    return RecoveryDecision(
        action=RecoveryActionType.RECLAIM_JOB,
        reason_code="job.lease-expired",
        incident_severity="critical",
        qualification_breaking=True,
        next_check_at=now + timedelta(seconds=30),
    )


def _progress_stalled_decision(now: datetime) -> RecoveryDecision:
    return RecoveryDecision(
        action=RecoveryActionType.CANCEL_JOB,
        reason_code="job.progress-stalled",
        incident_severity="warning",
        qualification_breaking=False,
        next_check_at=now + timedelta(seconds=30),
    )


def _heartbeat_missing_decision(now: datetime) -> RecoveryDecision:
    return RecoveryDecision(
        action=RecoveryActionType.RECLAIM_JOB,
        reason_code="job.heartbeat-missing",
        incident_severity="critical",
        qualification_breaking=True,
        next_check_at=now + timedelta(seconds=30),
    )


def _seed_succeeded_recovery_job(
    control_plane: PostgresControlPlane,
    *,
    job_key: str,
    job_type: str,
    now: datetime,
    recovery_lease_seconds: int = 30,
) -> JobLease:
    failed = _seed_claimed_job(
        control_plane,
        job_key=job_key,
        job_type=job_type,
        input_identity=f"{job_key}:input",
        now=now,
    )
    control_plane.finish_retryable_with_incident(
        failed,
        error_class="TimeoutError",
        incident_key=f"incident:job-retry:{job_key}",
        dedupe_key=f"job-retry:{job_key}",
        component=job_type,
        summary=f"{job_type} retryable failure",
        detail={"job_key": job_key},
        channels=("dashboard",),
        now=now,
    )
    recovered = control_plane.claim_job(
        worker_id=f"recovery:{job_key}",
        job_types=(job_type,),
        lease_seconds=recovery_lease_seconds,
        now=now + timedelta(seconds=15),
    )
    assert recovered is not None
    control_plane.finish(recovered, state=JobState.SUCCEEDED, now=now + timedelta(seconds=16))
    return recovered


def _install_sleep_trigger(
    control_plane: PostgresControlPlane,
    *,
    function_name: str,
    trigger_name: str,
    table_name: str,
    when_clause: str,
) -> None:
    with control_plane._connection_factory() as connection:
        connection.execute(
            sql.SQL(
                """
            CREATE OR REPLACE FUNCTION {function_name}() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                PERFORM pg_sleep(10);
                RETURN NEW;
            END;
            $$
            """
            ).format(function_name=sql.Identifier(function_name))
        )
        connection.execute(
            sql.SQL(
                """
            CREATE TRIGGER {trigger_name}
            BEFORE UPDATE ON {table_name}
            FOR EACH ROW
            WHEN ({when_clause})
            EXECUTE FUNCTION {function_name}()
            """
            ).format(
                trigger_name=sql.Identifier(trigger_name),
                table_name=sql.Identifier(table_name),
                when_clause=sql.SQL(when_clause),
                function_name=sql.Identifier(function_name),
            )
        )


def _remove_sleep_trigger(
    control_plane: PostgresControlPlane,
    *,
    function_name: str,
    trigger_name: str,
    table_name: str,
) -> None:
    with control_plane._connection_factory() as connection:
        connection.execute(
            sql.SQL("DROP TRIGGER IF EXISTS {} ON {}").format(
                sql.Identifier(trigger_name), sql.Identifier(table_name)
            )
        )
        connection.execute(
            sql.SQL("DROP FUNCTION IF EXISTS {}()").format(sql.Identifier(function_name))
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


def _mark_structure_job_succeeded_without_runtime_event(
    control_plane: PostgresControlPlane,
    lease: JobLease,
    *,
    checkpoint_cursor: str,
    checkpoint_digest: str,
    now: datetime,
) -> None:
    """Build the historical half-complete state left by the old terminal path."""
    with control_plane._connection_factory() as connection:
        job = connection.execute(
            """
            UPDATE m1_jobs
            SET state = 'succeeded', checkpoint_cursor = %s, checkpoint_digest = %s,
                lease_owner = NULL, lease_expires_at = NULL, updated_at = %s
            WHERE job_key = %s AND lease_epoch = %s AND state = 'leased'
            """,
            (
                checkpoint_cursor,
                checkpoint_digest,
                now,
                lease.job_key,
                lease.lease_epoch,
            ),
        )
        assert job.rowcount == 1
        attempt = connection.execute(
            """
            UPDATE m1_job_attempts
            SET state = 'succeeded', finished_at = %s
            WHERE job_key = %s AND lease_epoch = %s AND worker_id = %s
              AND state = 'running'
            """,
            (now, lease.job_key, lease.lease_epoch, lease.lease_owner),
        )
        assert attempt.rowcount == 1


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

        def complete_structure_range(self, *args: object, **kwargs: object):
            if self.crash:
                self.crash = False
                raise KeyboardInterrupt("simulated process death after R2 upload")
            return self._delegate.complete_structure_range(*args, **kwargs)

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


def test_deployment_preflight_requires_named_database_and_all_022_runtime_invariants(
    control_plane: PostgresControlPlane,
) -> None:
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        database_name = connection.execute("SELECT current_database()").fetchone()
    assert database_name is not None
    result = control_plane.deployment_preflight(expected_database=str(database_name[0]))
    assert result["database_name"] == database_name[0]
    assert result["revision_022_tables"] == 23
    assert result["runtime_event_invariants"] == [
        "append_only_function",
        "append_only_trigger",
        "unique_attempt_event_sequence",
        "unique_idempotency_key",
    ]
    with pytest.raises(Exception, match="database identity mismatch"):
        control_plane.deployment_preflight(expected_database="not-the-control-plane")


def _run_alembic(postgres_dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": postgres_dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def test_deployment_preflight_rejects_021_database_before_runtime_workers(
    postgres_dsn: str,
) -> None:
    _run_alembic(postgres_dsn, "downgrade", "021")
    try:
        control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
        with psycopg.connect(postgres_dsn) as connection:
            database_name = connection.execute("SELECT current_database()").fetchone()
        assert database_name is not None
        with pytest.raises(Exception, match="revision 022 runtime schema is incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        _run_alembic(postgres_dsn, "upgrade", "head")


@pytest.mark.parametrize(
    ("break_sql", "restore_sql"),
    (
        (
            "ALTER TABLE m1_job_runtime_events "
            "DROP CONSTRAINT uq_m1_runtime_events_attempt_sequence",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "uq_m1_runtime_events_attempt_sequence UNIQUE (attempt_id, event_sequence)",
        ),
        (
            "ALTER TABLE m1_job_runtime_events DROP CONSTRAINT uq_m1_runtime_events_idempotency",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "uq_m1_runtime_events_idempotency UNIQUE (idempotency_key)",
        ),
        (
            "DROP TRIGGER m1_runtime_events_immutable ON m1_job_runtime_events",
            "CREATE TRIGGER m1_runtime_events_immutable BEFORE UPDATE OR DELETE "
            "ON m1_job_runtime_events FOR EACH ROW EXECUTE FUNCTION "
            "m1_reject_runtime_event_mutation()",
        ),
    ),
)
def test_deployment_preflight_rejects_runtime_event_invariant_drift(
    postgres_dsn: str, break_sql: LiteralString, restore_sql: LiteralString
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute(sql.SQL(break_sql))
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime event invariants are incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute(sql.SQL(restore_sql))


def test_deployment_preflight_rejects_replaced_runtime_append_only_function(
    postgres_dsn: str,
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute(
            """
            CREATE OR REPLACE FUNCTION m1_reject_runtime_event_mutation() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RETURN NEW;
            END;
            $$;
            """
        )
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime event invariants are incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute(
                "CREATE OR REPLACE FUNCTION m1_reject_runtime_event_mutation() "
                "RETURNS trigger LANGUAGE plpgsql AS $$\n"
                "        BEGIN\n"
                "            RAISE EXCEPTION 'runtime events are append-only';\n"
                "        END;\n"
                "        $$;"
            )


def test_deployment_preflight_rejects_replica_only_runtime_append_only_trigger(
    postgres_dsn: str,
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute(
            "ALTER TABLE m1_job_runtime_events ENABLE REPLICA TRIGGER m1_runtime_events_immutable"
        )
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime event invariants are incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute(
                "ALTER TABLE m1_job_runtime_events ENABLE TRIGGER m1_runtime_events_immutable"
            )


def test_deployment_preflight_rejects_column_scoped_runtime_append_only_trigger(
    postgres_dsn: str,
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute("DROP TRIGGER m1_runtime_events_immutable ON m1_job_runtime_events")
        connection.execute(
            """
            CREATE TRIGGER m1_runtime_events_immutable
            BEFORE UPDATE OF detail OR DELETE ON m1_job_runtime_events
            FOR EACH ROW EXECUTE FUNCTION m1_reject_runtime_event_mutation()
            """
        )
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime event invariants are incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute("DROP TRIGGER m1_runtime_events_immutable ON m1_job_runtime_events")
            connection.execute(
                """
                CREATE TRIGGER m1_runtime_events_immutable
                BEFORE UPDATE OR DELETE ON m1_job_runtime_events
                FOR EACH ROW EXECUTE FUNCTION m1_reject_runtime_event_mutation()
                """
            )


def test_deployment_preflight_rejects_missing_runtime_state_deadline_column(
    postgres_dsn: str,
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute("ALTER TABLE m1_job_runtime_state DROP COLUMN heartbeat_deadline_at")
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime schema fingerprint is incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute(
                """
                ALTER TABLE m1_job_runtime_state
                ADD COLUMN heartbeat_deadline_at TIMESTAMP WITH TIME ZONE NOT NULL
                """
            )
            connection.execute(
                """
                CREATE INDEX m1_job_runtime_state_deadlines
                ON m1_job_runtime_state (
                    lease_deadline_at, heartbeat_deadline_at, progress_deadline_at
                )
                """
            )


@pytest.mark.parametrize(
    ("table_name", "constraint_name", "restore_sql"),
    (
        (
            "m1_job_runtime_state",
            "ck_m1_runtime_state_epoch",
            "ALTER TABLE m1_job_runtime_state ADD CONSTRAINT "
            "ck_m1_runtime_state_epoch CHECK (lease_epoch > 0)",
        ),
        (
            "m1_job_runtime_state",
            "ck_m1_runtime_state_progress",
            "ALTER TABLE m1_job_runtime_state ADD CONSTRAINT "
            "ck_m1_runtime_state_progress CHECK ("
            "progress_sequence >= 0 AND progress_current >= 0 AND "
            "(progress_total IS NULL OR (progress_total >= 0 AND "
            "progress_current <= progress_total)))",
        ),
        (
            "m1_job_runtime_state",
            "ck_m1_runtime_state_recovery",
            "ALTER TABLE m1_job_runtime_state ADD CONSTRAINT "
            "ck_m1_runtime_state_recovery CHECK ("
            "recovery_state IN ('active', 'suspect', 'recovering', 'recovered', 'terminal'))",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_detail_size",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_detail_size CHECK ("
            "jsonb_typeof(detail) = 'object' AND octet_length(detail::text) <= 4096 "
            "AND pg_column_size(detail) <= 4096)",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_epoch",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_epoch CHECK (lease_epoch > 0)",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_kind",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_kind CHECK (kind IN ("
            "'job.started', 'job.stage-changed', 'job.lease-at-risk', "
            "'job.progress-stalled', 'job.retryable-failed', 'job.retry-scheduled', "
            "'job.recovery-started', 'job.recovered', 'job.terminal-failed', "
            "'job.succeeded'))",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_progress_current",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_progress_current CHECK ("
            "progress_current IS NULL OR progress_current >= 0)",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_progress_pair",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_progress_pair CHECK ("
            "(progress_sequence IS NULL) = (progress_current IS NULL))",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_progress_sequence",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_progress_sequence CHECK ("
            "progress_sequence IS NULL OR progress_sequence >= 0)",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_progress_total",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_progress_total CHECK ("
            "progress_total IS NULL OR (progress_total >= 0 AND "
            "progress_current IS NOT NULL AND progress_current <= progress_total))",
        ),
        (
            "m1_job_runtime_events",
            "ck_m1_runtime_events_sequence",
            "ALTER TABLE m1_job_runtime_events ADD CONSTRAINT "
            "ck_m1_runtime_events_sequence CHECK (event_sequence > 0)",
        ),
    ),
)
def test_deployment_preflight_rejects_missing_runtime_check_constraint(
    postgres_dsn: str,
    table_name: LiteralString,
    constraint_name: LiteralString,
    restore_sql: LiteralString,
) -> None:
    control_plane = PostgresControlPlane(lambda: psycopg.connect(postgres_dsn))
    with psycopg.connect(postgres_dsn) as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
        connection.execute(
            sql.SQL("ALTER TABLE {} DROP CONSTRAINT {}").format(
                sql.Identifier(table_name),
                sql.Identifier(constraint_name),
            )
        )
    assert database_name is not None
    try:
        with pytest.raises(Exception, match="runtime schema fingerprint is incomplete"):
            control_plane.deployment_preflight(expected_database=str(database_name[0]))
    finally:
        with psycopg.connect(postgres_dsn) as connection:
            connection.execute(sql.SQL(restore_sql))


def test_deployment_preflight_is_independent_of_connection_search_path(
    postgres_dsn: str,
) -> None:
    def connect() -> psycopg.Connection:
        return psycopg.connect(postgres_dsn, options="-c search_path=pg_catalog")

    control_plane = PostgresControlPlane(connect)
    with connect() as connection:
        database_name = connection.execute("SELECT current_database()").fetchone()
    assert database_name is not None

    result = control_plane.deployment_preflight(expected_database=str(database_name[0]))

    assert result["database_name"] == database_name[0]
    assert result["revision_022_tables"] == 23


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
            now=now + timedelta(milliseconds=500),
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
    control_plane.finish(certifier, state=JobState.WAITING, now=now)
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
    awakened = control_plane.claim_job(
        worker_id="structure-certifier-awakened",
        job_types=("structure-certify",),
        lease_seconds=30,
        now=now,
    )
    assert awakened is not None
    assert awakened.job_key == certifier.job_key
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
            awakened,
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
    prepared_batches = control_plane.quote_batches_from_legs(
        structure_receipt_digest=bundle.sha256,
        universe_hash="c" * 64,
        legs=(_leg("quote-token"),),
        batch_size=100,
    )
    quote_batches = control_plane.admit_quote_generation(
        quote_admit,
        structure_receipt_digest=bundle.sha256,
        universe_hash="c" * 64,
        legs=(_leg("quote-token"),),
        batch_size=100,
        input_artifacts={
            prepared_batches[0].job_key: (
                f"quote-inputs/{'d' * 64}/batch.ndjson",
                "d" * 64,
                1,
            )
        },
        now=now,
    )
    assert control_plane.quote_batch_input_reference(quote_batches[0].job_key) == (
        f"quote-inputs/{'d' * 64}/batch.ndjson",
        "d" * 64,
        1,
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id, lease_epoch, worker_id, event_sequence, kind, stage, "
            "detail, "
            "idempotency_key FROM m1_job_runtime_events "
            "WHERE job_key = %s AND kind = %s",
            (quote_admit.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        success_event = cursor.fetchone()
    assert success_event is not None
    assert success_event[0:5] == (
        success_event[0],
        quote_admit.lease_epoch,
        quote_admit.lease_owner,
        2,
        RuntimeEventKind.SUCCEEDED.value,
    )
    assert success_event[5] == "commit-admission"
    assert success_event[6] == {
        "component": "control-plane",
        "data_product": "market-snapshot",
        "qualification_impact": "qualified",
        "result_code": "ok",
    }
    assert success_event[7] == f"runtime:{success_event[0]}:succeeded"


def test_quote_admission_success_event_failure_rolls_back_terminal_rows(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    generation_key = "quote:atomic-success"
    job_key = f"{generation_key}:quote-admit"
    bundle_digest = "a" * 64
    control_plane.enqueue_job(
        job_key=job_key,
        job_type="quote-admit",
        input_identity=f"{generation_key}:bundles/current.ndjson:{bundle_digest}",
        now=now,
    )
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_structure_generation_inputs "
            "(generation_key, bundle_key, bundle_digest, identity, admitted_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (generation_key, "bundles/current.ndjson", bundle_digest, Jsonb({}), now),
        )
        connection.execute(
            "INSERT INTO m1_quote_admission_inputs "
            "(job_key, generation_key, bundle_key, bundle_digest, admitted_at) "
            "VALUES (%s, %s, %s, %s, %s)",
            (job_key, generation_key, "bundles/current.ndjson", bundle_digest, now),
        )
    lease = control_plane.claim_job(
        worker_id="quote-admitter", job_types=("quote-admit",), lease_seconds=120, now=now
    )
    assert lease is not None
    batches = control_plane.quote_batches_from_legs(
        structure_receipt_digest=bundle_digest,
        universe_hash="b" * 64,
        legs=(_leg("token-atomic"),),
        batch_size=100,
    )

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected success event failure"):
        control_plane.admit_quote_generation(
            lease,
            structure_receipt_digest=bundle_digest,
            universe_hash="b" * 64,
            legs=(_leg("token-atomic"),),
            batch_size=100,
            input_artifacts={
                batches[0].job_key: (
                    f"quote-inputs/{'c' * 64}/batch.ndjson",
                    "c" * 64,
                    1,
                )
            },
            now=now,
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_quote_batch_inputs WHERE job_key LIKE %s",
            (f"{generation_key}:%",),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)


def test_quote_admission_rejects_expired_terminal_budget_before_mutation(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease, bundle_digest, batches = _seed_quote_admission_job(
        control_plane,
        generation_key="quote:expired-terminal-budget",
        now=now,
        lease_seconds=3,
    )

    with pytest.raises(StaleLeaseError, match="no safe terminal budget"):
        control_plane.admit_quote_generation(
            lease,
            structure_receipt_digest=bundle_digest,
            universe_hash="b" * 64,
            legs=(_leg("token-quote:expired-terminal-budget"),),
            batch_size=100,
            input_artifacts={
                batches[0].job_key: (
                    f"quote-inputs/{'c' * 64}/batch.ndjson",
                    "c" * 64,
                    1,
                )
            },
            now=now + timedelta(seconds=3),
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_quote_batch_inputs WHERE job_key LIKE %s",
            ("quote:expired-terminal-budget:%",),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)


def test_quote_admission_statement_timeout_rolls_back_terminal_transaction(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    lease, bundle_digest, batches = _seed_quote_admission_job(
        control_plane,
        generation_key="quote:statement-timeout",
        now=now,
        lease_seconds=2,
    )

    def sleep_until_statement_timeout(cursor: object, event: object) -> object:
        assert event is not None
        cast(Any, cursor).execute("SELECT pg_sleep(10)")
        return event

    monkeypatch.setattr(
        postgres_module, "append_runtime_event_cursor", sleep_until_statement_timeout
    )
    started = time.monotonic()
    with pytest.raises(psycopg.errors.QueryCanceled):
        control_plane.admit_quote_generation(
            lease,
            structure_receipt_digest=bundle_digest,
            universe_hash="b" * 64,
            legs=(_leg("token-quote:statement-timeout"),),
            batch_size=100,
            input_artifacts={
                batches[0].job_key: (
                    f"quote-inputs/{'c' * 64}/batch.ndjson",
                    "c" * 64,
                    1,
                )
            },
            now=now,
        )
    assert time.monotonic() - started < 3

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_quote_batch_inputs WHERE job_key LIKE %s",
            ("quote:statement-timeout:%",),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)


def test_quote_admission_lock_timeout_rolls_back_terminal_transaction(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease, bundle_digest, batches = _seed_quote_admission_job(
        control_plane,
        generation_key="quote:lock-timeout",
        now=now,
        lease_seconds=3,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute(
                "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE",
                (lease.job_key,),
            )
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            control_plane.admit_quote_generation(
                lease,
                structure_receipt_digest=bundle_digest,
                universe_hash="b" * 64,
                legs=(_leg("token-quote:lock-timeout"),),
                batch_size=100,
                input_artifacts={
                    batches[0].job_key: (
                        f"quote-inputs/{'c' * 64}/batch.ndjson",
                        "c" * 64,
                        1,
                    )
                },
                now=now,
            )
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_quote_batch_inputs WHERE job_key LIKE %s",
            ("quote:lock-timeout:%",),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)


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


def test_quote_batch_terminal_success_event_rolls_back_receipt(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
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
        worker_id="quote-terminal", job_types=("quote-batch",), lease_seconds=30, now=now
    )
    assert lease is not None
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected quote success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected quote success event failure"):
        control_plane.record_quote_batch(
            lease,
            token_range_digest=batch.token_range_digest,
            quote_digest="c" * 64,
            artifact_key="quote-batches/c/batch.ndjson",
            artifact_digest="c" * 64,
            successful_response_count=1,
            quoted_at=now,
            now=now,
            terminal=True,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_quote_batch_receipts WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    receipt = control_plane.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
        terminal=True,
    )
    assert receipt.job_key == lease.job_key
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


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


def test_quote_certifier_success_event_rolls_back_manifest_and_pointer(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-1",),
        batch_size=1,
        now=now,
    )[0]
    batch_lease = control_plane.claim_job(
        worker_id="quote-cert-batch", job_types=("quote-batch",), lease_seconds=30, now=now
    )
    assert batch_lease is not None
    control_plane.record_quote_batch(
        batch_lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )
    control_plane.finish(batch_lease, state=JobState.SUCCEEDED, now=now)
    certifier = control_plane.claim_job(
        worker_id="quote-certifier", job_types=("quote-certify",), lease_seconds=30, now=now
    )
    assert certifier is not None
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected quote certification success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected quote certification success event failure"):
        control_plane.certify_quote_generation(
            certifier,
            generation_key=batch.generation_key,
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (certifier.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_generation_manifests WHERE generation_key = %s",
            (batch.generation_key,),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key = 'quote:current'"
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    assert (
        control_plane.certify_quote_generation(
            certifier,
            generation_key=batch.generation_key,
            now=now,
        )
        == sha256(
            f"{batch.job_key}:{batch.token_range_digest}:"
            f"{'c' * 64}:quote-batches/c/batch.ndjson:{'c' * 64}".encode()
        ).hexdigest()
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (certifier.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


def test_quote_certification_terminal_transaction_has_bounded_timeout(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="a" * 64,
        universe_hash="b" * 64,
        token_ids=("token-timeout",),
        batch_size=1,
        now=now,
    )[0]
    batch_lease = control_plane.claim_job(
        worker_id="quote-timeout-batch", job_types=("quote-batch",), lease_seconds=30, now=now
    )
    assert batch_lease is not None
    control_plane.record_quote_batch(
        batch_lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/timeout.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now,
    )
    control_plane.finish(batch_lease, state=JobState.SUCCEEDED, now=now)
    certifier = control_plane.claim_job(
        worker_id="quote-timeout-certifier",
        job_types=("quote-certify",),
        lease_seconds=2,
        now=now,
    )
    assert certifier is not None
    function_name = "m1_test_quote_certify_timeout_fn"
    trigger_name = "m1_test_quote_certify_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_jobs",
        when_clause="OLD.state = 'leased' AND NEW.state = 'succeeded'",
    )
    started = time.monotonic()
    try:
        with pytest.raises(psycopg.errors.QueryCanceled):
            control_plane.certify_quote_generation(
                certifier,
                generation_key=batch.generation_key,
                now=now,
            )
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_jobs",
        )
    assert time.monotonic() - started < 4
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (certifier.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_generation_manifests WHERE generation_key = %s",
            (batch.generation_key,),
        )
        assert cursor.fetchone() == (0,)


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


def test_opportunity_terminal_success_event_rolls_back_projection_and_pointer(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    quote_generation = "quote:" + "a" * 64
    structure_generation = "structure:" + "b" * 64
    with control_plane._connection_factory() as connection:
        for generation_key in (quote_generation, structure_generation):
            job_key = f"{generation_key}:certify"
            connection.execute(
                """
                INSERT INTO m1_jobs(
                    job_key, job_type, input_identity, state, created_at, updated_at
                )
                VALUES (%s, 'certify', %s, 'succeeded', %s, %s)
                """,
                (job_key, generation_key, now, now),
            )
            connection.execute(
                """
                INSERT INTO m1_generation_manifests
                    (generation_key, producer_job_key, input_digest, artifact_key,
                     artifact_digest, record_count, published_at)
                VALUES (%s, %s, %s, %s, %s, 1, %s)
                """,
                (generation_key, job_key, "c" * 64, "artifact", "d" * 64, now),
            )
        connection.execute(
            """
            INSERT INTO m1_publication_pointers
                (pointer_key, generation_key, expected_generation_key, lease_epoch, published_at)
            VALUES ('quote:current', %s, NULL, 1, %s)
            """,
            (quote_generation, now),
        )
    control_plane.enqueue_job(
        job_key=f"{quote_generation}:opportunity-certify",
        job_type="opportunity-certify",
        input_identity=quote_generation,
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="opportunity-terminal",
        job_types=("opportunity-certify",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
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
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected opportunity success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected opportunity success event failure"):
        control_plane.publish_opportunity_projection(
            quote_generation_key=quote_generation,
            structure_generation_key=structure_generation,
            rows=(row,),
            now=now,
            lease=lease,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_opportunity_projections WHERE generation_key = %s",
            (quote_generation,),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_opportunity_publication_pointers")
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    control_plane.publish_opportunity_projection(
        quote_generation_key=quote_generation,
        structure_generation_key=structure_generation,
        rows=(row,),
        now=now,
        lease=lease,
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


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


def test_claim_commits_runtime_state_and_started_event_together(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:claim",
        job_type="structure-normalize",
        input_identity="runtime-claim",
        now=now,
    )

    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, stage, progress_sequence,
                   last_heartbeat_at, last_progress_at, lease_deadline_at,
                   heartbeat_deadline_at, progress_deadline_at, attempt_deadline_at
            FROM m1_job_runtime_state WHERE job_key = %s
            """,
            (lease.job_key,),
        )
        state = cursor.fetchone()
        cursor.execute(
            """
            SELECT attempt_id, lease_epoch, worker_id, event_sequence, kind,
                   stage, idempotency_key
            FROM m1_job_runtime_events
            WHERE job_key = %s ORDER BY event_sequence
            """,
            (lease.job_key,),
        )
        events = cursor.fetchall()

    assert state is not None
    assert isinstance(state[0], str) and state[0]
    assert state[1:4] == (lease.lease_epoch, lease.lease_owner, "started")
    assert state[4] == 0
    assert state[5] == state[6] == now
    assert all(value is not None for value in state[7:])
    assert len(events) == 1
    assert events[0][0:5] == (
        state[0],
        lease.lease_epoch,
        lease.lease_owner,
        1,
        RuntimeEventKind.STARTED.value,
    )
    assert events[0][5] == "started"


def test_runtime_progress_is_fenced_monotonic_and_idempotent(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:progress",
        job_type="structure-normalize",
        input_identity="runtime-progress",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None

    first = control_plane.record_runtime_progress(
        lease,
        progress=RuntimeProgress(sequence=1, current=2, total=5, stage="upload"),
        now=now + timedelta(seconds=1),
        idempotency_key="runtime-progress:1",
    )
    duplicate = control_plane.record_runtime_progress(
        lease,
        progress=RuntimeProgress(sequence=1, current=2, total=5, stage="upload"),
        now=now + timedelta(seconds=1),
        idempotency_key="runtime-progress:1",
    )
    assert duplicate == first
    with pytest.raises(RuntimeProgressConflictError, match="progress sequence"):
        control_plane.record_runtime_progress(
            lease,
            progress=RuntimeProgress(sequence=1, current=3, total=5, stage="upload"),
            now=now + timedelta(seconds=2),
            idempotency_key="runtime-progress:conflict",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT progress_sequence, progress_current, last_progress_at "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1, 2, now + timedelta(seconds=1))
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (2,)


def test_runtime_progress_lock_timeout_rolls_back_state_and_event(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime:progress-lock-timeout",
        job_type="structure-normalize",
        input_identity="runtime-progress-lock-timeout",
        now=now,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute(
                "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE",
                (lease.job_key,),
            )
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            control_plane.record_runtime_progress(
                lease,
                progress=RuntimeProgress(sequence=1, current=1, total=1, stage="upload"),
                now=now,
                idempotency_key="runtime:progress-lock-timeout:1",
            )
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT progress_sequence, progress_current FROM m1_job_runtime_state "
            "WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (0, 0)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)


def test_runtime_progress_statement_timeout_rolls_back_state_and_event(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime:progress-statement-timeout",
        job_type="structure-normalize",
        input_identity="runtime-progress-statement-timeout",
        now=now,
        lease_seconds=2,
    )
    function_name = "m1_test_progress_timeout_fn"
    trigger_name = "m1_test_progress_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_job_runtime_state",
        when_clause="NEW.progress_sequence > OLD.progress_sequence",
    )
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            control_plane.record_runtime_progress(
                lease,
                progress=RuntimeProgress(sequence=1, current=1, total=1, stage="upload"),
                now=now,
                idempotency_key="runtime:progress-statement-timeout:1",
            )
        assert time.monotonic() - started < 3
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_job_runtime_state",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT progress_sequence, progress_current FROM m1_job_runtime_state "
            "WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (0, 0)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)


def test_runtime_heartbeat_updates_liveness_without_progress(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:heartbeat",
        job_type="structure-normalize",
        input_identity="runtime-heartbeat",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    renewed = control_plane.heartbeat_runtime_attempt(
        lease, now=now + timedelta(seconds=5), lease_seconds=30
    )
    assert renewed.lease_expires_at == now + timedelta(seconds=35)

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT last_heartbeat_at, last_progress_at, progress_sequence, "
            "lease_deadline_at, heartbeat_deadline_at "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        row = cursor.fetchone()
        assert row is not None
        assert row[0] == now + timedelta(seconds=5)
        assert row[1] == now
        assert row[2] == 0
        assert row[3] == now + timedelta(seconds=35)
        assert row[4] > row[0]


def test_runtime_heartbeat_sets_fenced_timeouts_before_first_query(monkeypatch) -> None:
    now = _now()
    lease = JobLease(
        job_key="runtime:heartbeat-contract",
        job_type="structure-normalize",
        input_identity="runtime-heartbeat-contract",
        lease_owner="runtime-worker",
        lease_epoch=1,
        lease_expires_at=now + timedelta(seconds=30),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )
    commands: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: object, params: object = None) -> None:
            as_string = getattr(query, "as_string", None)
            rendered = str(as_string(None) if callable(as_string) else query)
            commands.append(" ".join(rendered.split()))

        def fetchone(self):
            return {"attempt_id": "attempt-1"}

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, **kwargs: object):
            assert kwargs == {"row_factory": dict_row}
            return Cursor()

    monkeypatch.setattr(
        postgres_module,
        "update_runtime_heartbeat_cursor",
        lambda *args, **kwargs: {"lease_deadline_at": now + timedelta(seconds=31)},
    )
    factory = cast(Callable[[], psycopg.Connection[Any]], lambda: Connection())
    renewed = PostgresControlPlane(factory).heartbeat_runtime_attempt(
        lease, now=now + timedelta(seconds=1), lease_seconds=30
    )

    assert renewed.lease_expires_at == now + timedelta(seconds=31)
    assert commands[:3] == [
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
        (
            "SELECT attempt_id FROM m1_job_runtime_state "
            "WHERE job_key = %s AND lease_epoch = %s AND worker_id = %s"
        ),
    ]


def test_runtime_heartbeat_lock_timeout_rolls_back_liveness(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime:heartbeat-lock-timeout",
        job_type="structure-normalize",
        input_identity="runtime-heartbeat-lock-timeout",
        now=now,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute(
                "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE",
                (lease.job_key,),
            )
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            control_plane.heartbeat_runtime_attempt(lease, now=now, lease_seconds=30)
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT lease_expires_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (now + timedelta(seconds=30),)
        cursor.execute(
            "SELECT last_heartbeat_at, lease_deadline_at, heartbeat_deadline_at "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        state = cursor.fetchone()
        assert state is not None
        assert state[0] == now
        assert state[1] == now + timedelta(seconds=30)
        assert state[2] == now + timedelta(seconds=10)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)


def test_runtime_heartbeat_statement_timeout_rolls_back_liveness(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime:heartbeat-statement-timeout",
        job_type="structure-normalize",
        input_identity="runtime-heartbeat-statement-timeout",
        now=now,
        lease_seconds=2,
    )
    function_name = "m1_test_heartbeat_timeout_fn"
    trigger_name = "m1_test_heartbeat_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_jobs",
        when_clause="NEW.lease_expires_at IS DISTINCT FROM OLD.lease_expires_at",
    )
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            control_plane.heartbeat_runtime_attempt(lease, now=now, lease_seconds=30)
        assert time.monotonic() - started < 3
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_jobs",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT lease_expires_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (now + timedelta(seconds=2),)
        cursor.execute(
            "SELECT last_heartbeat_at, lease_deadline_at, heartbeat_deadline_at "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        state = cursor.fetchone()
        assert state is not None
        assert state[0] == now
        assert state[1] == now + timedelta(seconds=2)
        assert state[2] == now + timedelta(seconds=1)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)


def test_runtime_expired_lease_rejects_heartbeat_and_progress_without_mutation(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:expired",
        job_type="structure-normalize",
        input_identity="runtime-expired",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=3,
        now=now,
    )
    assert lease is not None
    expired_at = now + timedelta(seconds=4)

    with pytest.raises(StaleLeaseError):
        control_plane.heartbeat_runtime_attempt(lease, now=expired_at, lease_seconds=30)
    with pytest.raises(StaleLeaseError):
        control_plane.record_runtime_progress(
            lease,
            progress=RuntimeProgress(sequence=1, current=1, total=1, stage="upload"),
            now=expired_at,
            idempotency_key="runtime:expired:1",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT lease_expires_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (now + timedelta(seconds=3),)
        cursor.execute(
            "SELECT last_heartbeat_at, progress_sequence, last_progress_at "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (now, 0, now)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)


def test_runtime_direct_append_fences_new_events_but_replays_exact_expired_event(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:append-expired",
        job_type="structure-normalize",
        input_identity="runtime-append-expired",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=3,
        now=now,
    )
    assert lease is not None

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        attempt_row = cursor.fetchone()
        assert attempt_row is not None
        attempt_id = str(attempt_row[0])

    replay = RuntimeEvent(
        job_key=lease.job_key,
        attempt_id=attempt_id,
        lease_epoch=lease.lease_epoch,
        worker_id=lease.lease_owner,
        event_sequence=2,
        kind=RuntimeEventKind.LEASE_AT_RISK,
        stage="upload",
        progress=None,
        detail={
            "component": "control-plane",
            "deadline_kind": "lease",
            "deadline_at": (now + timedelta(seconds=3)).isoformat(),
            "recovery_policy": "retry-job",
        },
        occurred_at=now + timedelta(seconds=1),
        idempotency_key="runtime:append-expired:replay",
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        assert append_runtime_event_cursor(cursor, replay) == replay

    expired_new_event = RuntimeEvent(
        job_key=replay.job_key,
        attempt_id=replay.attempt_id,
        lease_epoch=replay.lease_epoch,
        worker_id=replay.worker_id,
        event_sequence=3,
        kind=RuntimeEventKind.LEASE_AT_RISK,
        stage=replay.stage,
        progress=None,
        detail=dict(replay.detail),
        occurred_at=now + timedelta(seconds=4),
        idempotency_key="runtime:append-expired:new",
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        with pytest.raises(RuntimeFenceError, match="lease is no longer current"):
            append_runtime_event_cursor(cursor, expired_new_event)

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (2,)


def test_runtime_default_progress_idempotency_is_attempt_scoped(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:attempt-key",
        job_type="structure-normalize",
        input_identity="runtime-attempt-key",
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="runtime-worker-a",
        job_types=("structure-normalize",),
        lease_seconds=3,
        now=now,
    )
    assert first is not None
    first_progress = control_plane.record_runtime_progress(
        first,
        progress=RuntimeProgress(sequence=1, current=1, total=2, stage="upload"),
        now=now + timedelta(seconds=1),
    )

    second = control_plane.claim_job(
        worker_id="runtime-worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert second is not None
    second_progress = control_plane.record_runtime_progress(
        second,
        progress=RuntimeProgress(sequence=1, current=1, total=2, stage="upload"),
        now=now + timedelta(seconds=5),
    )
    assert second_progress.attempt_id != first_progress.attempt_id
    assert second_progress.idempotency_key != first_progress.idempotency_key

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM m1_job_attempts WHERE job_key = %s ORDER BY lease_epoch",
            (second.job_key,),
        )
        attempt_ids = tuple(row[0] for row in cursor.fetchall())
        assert len(attempt_ids) == 2
        cursor.execute(
            "SELECT attempt_id, event_sequence, idempotency_key "
            "FROM m1_job_runtime_events WHERE job_key = %s "
            "AND kind = %s ORDER BY attempt_id",
            (second.job_key, RuntimeEventKind.STAGE_CHANGED.value),
        )
        progress_events = cursor.fetchall()
        assert len(progress_events) == 2
        assert all(row[1] == 2 for row in progress_events)
        assert len({row[2] for row in progress_events}) == 2


def test_runtime_stale_exact_replay_is_read_only_and_conflicts_do_not_mutate(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:stale-replay",
        job_type="structure-normalize",
        input_identity="runtime-stale-replay",
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="runtime-worker-a",
        job_types=("structure-normalize",),
        lease_seconds=3,
        now=now,
    )
    assert first is not None
    first_progress = control_plane.record_runtime_progress(
        first,
        progress=RuntimeProgress(sequence=1, current=1, total=2, stage="upload"),
        now=now + timedelta(seconds=1),
        idempotency_key="runtime:stale-replay:1",
    )
    second = control_plane.claim_job(
        worker_id="runtime-worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert second is not None

    replay = control_plane.record_runtime_progress(
        first,
        progress=RuntimeProgress(sequence=1, current=1, total=2, stage="upload"),
        now=now + timedelta(seconds=1),
        idempotency_key="runtime:stale-replay:1",
    )
    assert replay == first_progress

    with pytest.raises(RuntimeEventConflictError, match="idempotency key conflicts"):
        control_plane.record_runtime_progress(
            first,
            progress=RuntimeProgress(sequence=1, current=2, total=2, stage="upload"),
            now=now + timedelta(seconds=1),
            idempotency_key="runtime:stale-replay:1",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id, worker_id, progress_sequence, progress_current "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (second.job_key,),
        )
        current = cursor.fetchone()
        assert current is not None
        assert current[0] != first_progress.attempt_id
        assert current[1:] == (second.lease_owner, 0, 0)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (second.job_key,),
        )
        assert cursor.fetchone() == (3,)


def test_stale_runtime_update_does_not_mutate_current_attempt(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:fence",
        job_type="structure-normalize",
        input_identity="runtime-fence",
        now=now,
    )
    first = control_plane.claim_job(
        worker_id="runtime-worker-a",
        job_types=("structure-normalize",),
        lease_seconds=3,
        now=now,
    )
    assert first is not None
    second = control_plane.claim_job(
        worker_id="runtime-worker-b",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert second is not None
    with pytest.raises(StaleLeaseError):
        control_plane.record_runtime_progress(
            first,
            progress=RuntimeProgress(sequence=1, current=1, total=1, stage="upload"),
            now=now + timedelta(seconds=5),
            idempotency_key="runtime-stale:1",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id, worker_id, progress_sequence "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (second.job_key,),
        )
        current = cursor.fetchone()
        assert current is not None
        assert isinstance(current[0], str) and current[0]
        assert current[1:] == (second.lease_owner, 0)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (second.job_key,),
        )
        assert cursor.fetchone() == (2,)


def test_runtime_events_are_immutable_and_timestamp_detail_is_utc(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:append-only",
        job_type="structure-normalize",
        input_identity="runtime-append-only",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        attempt_row = cursor.fetchone()
        assert attempt_row is not None
        attempt_id = attempt_row[0]
        event = RuntimeEvent(
            job_key=lease.job_key,
            attempt_id=attempt_id,
            lease_epoch=lease.lease_epoch,
            worker_id=lease.lease_owner,
            event_sequence=2,
            kind=RuntimeEventKind.LEASE_AT_RISK,
            stage="upload",
            progress=None,
            detail={
                "component": "control-plane",
                "deadline_at": "2030-01-01T13:00:00+01:00",
            },
            occurred_at=now + timedelta(seconds=1),
            idempotency_key="runtime:append-only:2",
        )
        persisted = append_runtime_event_cursor(cursor, event)
        assert persisted.detail["deadline_at"] == "2030-01-01T12:00:00+00:00"
        cursor.execute(
            "SELECT detail->>'deadline_at' FROM m1_job_runtime_events WHERE idempotency_key = %s",
            (event.idempotency_key,),
        )
        assert cursor.fetchone() == ("2030-01-01T12:00:00+00:00",)

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            cursor.execute(
                "UPDATE m1_job_runtime_events SET stage = 'forged' WHERE idempotency_key = %s",
                (event.idempotency_key,),
            )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            cursor.execute(
                "DELETE FROM m1_job_runtime_events WHERE idempotency_key = %s",
                (event.idempotency_key,),
            )


def test_runtime_event_replay_is_exact_and_conflicts_are_rejected(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    control_plane.enqueue_job(
        job_key="runtime:replay",
        job_type="structure-normalize",
        input_identity="runtime-replay",
        now=now,
    )
    lease = control_plane.claim_job(
        worker_id="runtime-worker",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        attempt_row = cursor.fetchone()
        assert attempt_row is not None
        attempt_id = attempt_row[0]
        event = RuntimeEvent(
            job_key=lease.job_key,
            attempt_id=attempt_id,
            lease_epoch=lease.lease_epoch,
            worker_id=lease.lease_owner,
            event_sequence=2,
            kind=RuntimeEventKind.STAGE_CHANGED,
            stage="upload",
            progress=RuntimeProgress(sequence=1, current=1, total=2, stage="upload"),
            detail={"component": "control-plane"},
            occurred_at=now + timedelta(seconds=1),
            idempotency_key="runtime:replay:2",
        )
        assert append_runtime_event_cursor(cursor, event) == event
        assert append_runtime_event_cursor(cursor, event) == event
        with pytest.raises(RuntimeEventConflict):
            append_runtime_event_cursor(
                cursor,
                RuntimeEvent(
                    job_key=event.job_key,
                    attempt_id=event.attempt_id,
                    lease_epoch=event.lease_epoch,
                    worker_id=event.worker_id,
                    event_sequence=event.event_sequence,
                    kind=event.kind,
                    stage="parse",
                    progress=RuntimeProgress(sequence=1, current=1, total=2, stage="parse"),
                    detail=dict(event.detail),
                    occurred_at=event.occurred_at,
                    idempotency_key=event.idempotency_key,
                ),
            )


def test_controller_claims_are_monotonic_and_only_latest_schedules_recovery_action(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:controller-fence",
        job_type="structure-normalize",
        input_identity="recovery-action:controller-fence",
        now=now,
        lease_seconds=30,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = tuple(
            pool.map(
                lambda owner: claim_controller(
                    control_plane._connection_factory,
                    controller_id="m1-runtime-reconciler",
                    owner_id=owner,
                    lease_seconds=30,
                    now=now,
                ),
                ("controller-a", "controller-b"),
            )
        )

    epochs = sorted(claim.lease_epoch for claim in claims)
    assert epochs == [1, 2]
    stale_controller = min(claims, key=lambda claim: claim.lease_epoch)
    current_controller = max(claims, key=lambda claim: claim.lease_epoch)

    stale = schedule_action(
        control_plane._connection_factory,
        controller=stale_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    assert stale.state == "completed"
    assert stale.result_code == "stale-noop"

    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=current_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )
    assert scheduled.state == "pending"
    assert scheduled.result_code is None
    assert scheduled.expected_controller_epoch == current_controller.lease_epoch

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT state, result_code, expected_controller_epoch
            FROM m1_recovery_actions
            WHERE target_id = %s
            ORDER BY requested_at, action_id
            """,
            (lease.job_key,),
        )
        assert cursor.fetchall() == [
            ("completed", "stale-noop", stale_controller.lease_epoch),
            ("pending", None, current_controller.lease_epoch),
        ]
        cursor.execute(
            "SELECT recovery_state FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("recovering",)
        cursor.execute(
            "SELECT kind FROM m1_job_runtime_events WHERE job_key = %s ORDER BY event_sequence",
            (lease.job_key,),
        )
        assert [row[0] for row in cursor.fetchall()] == [
            RuntimeEventKind.STARTED.value,
            RuntimeEventKind.RECOVERY_STARTED.value,
        ]
        cursor.execute("SELECT kind FROM m1_incident_events ORDER BY occurred_at")
        assert [row[0] for row in cursor.fetchall()] == ["recovery-started"]
        cursor.execute("SELECT channel, state FROM m1_alert_outbox")
        assert cursor.fetchone() == ("dashboard", "pending")


def test_recovery_action_active_target_race_persists_one_stale_noop(
    control_plane: PostgresControlPlane,
) -> None:
    """Concurrent valid controllers produce one active action and one durable stale result."""
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:active-target-race",
        job_type="structure-normalize",
        input_identity="recovery-action:active-target-race",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controllers = tuple(
        claim_controller(
            control_plane._connection_factory,
            controller_id=f"m1-runtime-race-{suffix}",
            owner_id=f"owner-{suffix}",
            lease_seconds=30,
            now=now,
        )
        for suffix in ("a", "b")
    )
    barrier = Barrier(2)

    def schedule(controller):
        barrier.wait()
        return schedule_action(
            control_plane._connection_factory,
            controller=controller,
            decision=_recovery_decision(now),
            incident_key=f"incident:{lease.job_key}:{controller.controller_id}",
            component="structure-normalize",
            target_type="job",
            target_id=lease.job_key,
            expected_attempt_id=attempt_id,
            expected_lease_epoch=lease.lease_epoch,
            recovery_budget_remaining=1,
            cooldown_seconds=60,
            channels=("dashboard",),
            now=now + timedelta(seconds=1),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(schedule, controllers))

    stale_controller, stale = next(
        (controller, result)
        for controller, result in zip(controllers, results, strict=True)
        if result.result_code == "stale-noop"
    )
    active = next(result for result in results if result.state == "pending")
    assert stale.state == "completed"
    assert stale.detail["stale_reason"] == "active-target-authoritative"
    assert active.result_code is None
    assert active.target_id == stale.target_id == lease.job_key

    replay = schedule_action(
        control_plane._connection_factory,
        controller=stale_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}:{stale_controller.controller_id}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    assert replay == stale

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, result_code, detail FROM m1_recovery_actions "
            "WHERE target_id = %s ORDER BY requested_at, action_id",
            (lease.job_key,),
        )
        rows = cursor.fetchall()
        assert len(rows) == 2
        assert sorted((row[0], row[1]) for row in rows) == [
            ("completed", "stale-noop"),
            ("pending", None),
        ]
        assert next(row[2]["stale_reason"] for row in rows if row[1] == "stale-noop") == (
            "active-target-authoritative"
        )


def test_runtime_controller_status_and_facts_are_read_only(
    control_plane: PostgresControlPlane,
) -> None:
    """The dashboard/fact projection cannot create controller/action mutations."""
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime-controller-status:read-only",
        job_type="structure-normalize",
        input_identity="runtime-controller-status:read-only",
        now=now,
    )
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler-status",
        owner_id="status-reader",
        lease_seconds=60,
        now=now,
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_recovery_actions WHERE controller_id = %s",
            (controller.controller_id,),
        )
        before_actions_row = cursor.fetchone()
        assert before_actions_row is not None
        before_actions = before_actions_row[0]
        cursor.execute(
            "SELECT count(*) FROM m1_recovery_target_budgets WHERE controller_id = %s",
            (controller.controller_id,),
        )
        before_budgets_row = cursor.fetchone()
        assert before_budgets_row is not None
        before_budgets = before_budgets_row[0]

    status = read_runtime_controller_status(
        control_plane._connection_factory,
        controller_id=controller.controller_id,
        now=now,
        sample_limit=10,
    )
    facts = read_runtime_reconcile_states(
        control_plane._connection_factory,
        controller_id=controller.controller_id,
        now=now,
        sample_limit=10,
    )
    status_controller = status.get("controller")
    assert isinstance(status_controller, dict)
    status_controller = cast(dict[str, object], status_controller)
    assert "lease_epoch" in status_controller
    assert status_controller["lease_epoch"] == controller.lease_epoch
    assert status["actions"] == {"pending": [], "running": [], "recent_completed": []}
    assert facts and facts[0].target_id == lease.job_key
    assert facts[0].runtime_state.owner_is_current is True

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_recovery_actions WHERE controller_id = %s",
            (controller.controller_id,),
        )
        after_actions_row = cursor.fetchone()
        assert after_actions_row is not None
        assert after_actions_row[0] == before_actions
        cursor.execute(
            "SELECT count(*) FROM m1_recovery_target_budgets WHERE controller_id = %s",
            (controller.controller_id,),
        )
        after_budgets_row = cursor.fetchone()
        assert after_budgets_row is not None
        assert after_budgets_row[0] == before_budgets


def test_recovery_action_stale_controller_does_not_create_budget_or_poison_schedule(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:stale-controller-budget",
        job_type="structure-normalize",
        input_identity="recovery-action:stale-controller-budget",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    stale_controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-stale-budget",
        lease_seconds=30,
        now=now,
    )
    current_controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-current-budget",
        lease_seconds=30,
        now=now + timedelta(seconds=1),
    )

    stale = schedule_action(
        control_plane._connection_factory,
        controller=stale_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=0,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )
    assert stale.state == "completed"
    assert stale.result_code == "stale-noop"

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*)
            FROM m1_recovery_target_budgets
            WHERE controller_id = %s AND target_type = 'job' AND target_id = %s
            """,
            (stale_controller.controller_id, lease.job_key),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (0,)

    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=current_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=3),
    )
    assert scheduled.state == "pending"
    assert scheduled.result_code is None

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT remaining_actions
            FROM m1_recovery_target_budgets
            WHERE controller_id = %s AND target_type = 'job' AND target_id = %s
            """,
            (current_controller.controller_id, lease.job_key),
        )
        assert cursor.fetchone() == (0,)


def test_recovery_action_schedule_is_idempotent_and_conflicting_replay_fails_closed(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:idempotent",
        job_type="structure-normalize",
        input_identity="recovery-action:idempotent",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-idempotent",
        lease_seconds=30,
        now=now,
    )

    first = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
        detail={"job_key": lease.job_key, "bounded": True},
    )
    replay = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
        detail={"job_key": lease.job_key, "bounded": True},
    )
    assert replay == first

    conflicting_replays: tuple[dict[str, object], ...] = (
        {"cooldown_seconds": 90},
        {"component": "quote-batch"},
        {"channels": ("telegram", "dashboard")},
        {"decision": _heartbeat_missing_decision(now)},
        {"decision": _progress_stalled_decision(now)},
        {"recovery_budget_remaining": 1},
        {"incident_key": f"incident:{lease.job_key}:other"},
        {"detail": {"job_key": lease.job_key, "bounded": False}},
    )
    for overrides in conflicting_replays:
        kwargs: dict[str, object] = {
            "controller": controller,
            "decision": _recovery_decision(now),
            "incident_key": f"incident:{lease.job_key}",
            "component": "structure-normalize",
            "target_type": "job",
            "target_id": lease.job_key,
            "expected_attempt_id": attempt_id,
            "expected_lease_epoch": lease.lease_epoch,
            "recovery_budget_remaining": 2,
            "cooldown_seconds": 60,
            "channels": ("dashboard",),
            "now": now + timedelta(seconds=1),
            "detail": {"job_key": lease.job_key, "bounded": True},
        }
        kwargs.update(overrides)
        with pytest.raises(RecoveryActionConflict, match="idempotency"):
            schedule_action(control_plane._connection_factory, **kwargs)  # type: ignore[arg-type]

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_recovery_actions")
        assert cursor.fetchone() == (1,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.RECOVERY_STARTED.value),
        )
        assert cursor.fetchone() == (1,)


def test_recovery_action_channel_replay_encoding_is_unambiguous(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:channel-canonical",
        job_type="structure-normalize",
        input_identity="recovery-action:channel-canonical",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-channel-canonical",
        lease_seconds=30,
        now=now,
    )

    first = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=60,
        channels=("a,b", "c"),
        now=now + timedelta(seconds=1),
    )
    same_set_replay = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=60,
        channels=("c", "a,b"),
        now=now + timedelta(seconds=1),
    )
    assert same_set_replay == first

    with pytest.raises(RecoveryActionConflict, match="idempotency"):
        schedule_action(
            control_plane._connection_factory,
            controller=controller,
            decision=_recovery_decision(now),
            incident_key=f"incident:{lease.job_key}",
            component="structure-normalize",
            target_type="job",
            target_id=lease.job_key,
            expected_attempt_id=attempt_id,
            expected_lease_epoch=lease.lease_epoch,
            recovery_budget_remaining=2,
            cooldown_seconds=60,
            channels=("a", "b,c"),
            now=now + timedelta(seconds=1),
        )

    for channels in (("a", "a"), ("a", "")):
        with pytest.raises(ValueError):
            schedule_action(
                control_plane._connection_factory,
                controller=controller,
                decision=_recovery_decision(now),
                incident_key=f"incident:{lease.job_key}",
                component="structure-normalize",
                target_type="job",
                target_id=lease.job_key,
                expected_attempt_id=attempt_id,
                expected_lease_epoch=lease.lease_epoch,
                recovery_budget_remaining=2,
                cooldown_seconds=60,
                channels=channels,
                now=now + timedelta(seconds=1),
            )


def test_recovery_action_stale_attempt_lease_is_completed_noop_without_mutation(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:stale-attempt",
        job_type="structure-normalize",
        input_identity="recovery-action:stale-attempt",
        now=now,
        lease_seconds=3,
    )
    old_attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    current_lease = control_plane.claim_job(
        worker_id="worker:recovery-action:stale-attempt:current",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert current_lease is not None
    current_attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-stale-attempt",
        lease_seconds=30,
        now=now,
    )

    stale = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=old_attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=5),
    )

    assert stale.state == "completed"
    assert stale.result_code == "stale-noop"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id, lease_epoch, recovery_state "
            "FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (current_attempt_id, current_lease.lease_epoch, "active")
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (0,)


def test_recovery_action_single_active_target_claim_and_finish_are_fenced(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:claim-finish",
        job_type="structure-normalize",
        input_identity="recovery-action:claim-finish",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-claim",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    duplicate = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    assert duplicate == scheduled

    claim = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert claim is not None
    assert claim.state == "running"
    assert claim.worker_id == "action-worker"
    assert claim.worker_epoch == 1
    assert (
        claim_action(
            control_plane._connection_factory,
            worker_id="another-worker",
            controller=controller,
            lease_seconds=30,
            now=now + timedelta(seconds=3),
        )
        is None
    )

    stale_finish = finish_action(
        control_plane._connection_factory,
        action_id=claim.action_id,
        worker_id="another-worker",
        worker_epoch=claim.worker_epoch,
        result_code="succeeded",
        now=now + timedelta(seconds=4),
    )
    assert stale_finish.state == "running"
    assert stale_finish.result_code is None

    finished = finish_action(
        control_plane._connection_factory,
        action_id=claim.action_id,
        worker_id=claim.worker_id,
        worker_epoch=claim.worker_epoch,
        result_code="disabled-action",
        now=now + timedelta(seconds=5),
        detail={"postcondition": "not executed in tests"},
    )
    assert finished.state == "completed"
    assert finished.result_code == "disabled-action"


def test_recovery_action_statement_timeout_rolls_back_action_event_incident_and_alert(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:statement-timeout",
        job_type="structure-normalize",
        input_identity="recovery-action:statement-timeout",
        now=now,
        lease_seconds=4,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-timeout",
        lease_seconds=30,
        now=now,
    )
    function_name = "m1_test_recovery_action_timeout_fn"
    trigger_name = "m1_test_recovery_action_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_job_runtime_state",
        when_clause="NEW.recovery_state = 'recovering'",
    )
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            schedule_action(
                control_plane._connection_factory,
                controller=controller,
                decision=_recovery_decision(now),
                incident_key=f"incident:{lease.job_key}",
                component="structure-normalize",
                target_type="job",
                target_id=lease.job_key,
                expected_attempt_id=attempt_id,
                expected_lease_epoch=lease.lease_epoch,
                recovery_budget_remaining=1,
                cooldown_seconds=60,
                channels=("dashboard",),
                now=now + timedelta(seconds=1),
            )
        assert time.monotonic() - started < 3
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_job_runtime_state",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_recovery_actions")
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT recovery_state FROM m1_job_runtime_state WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("active",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1,)
        cursor.execute("SELECT count(*) FROM m1_incidents")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (0,)


def test_recovery_action_schedules_expired_job_without_old_worker_write_authority(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:expired-job",
        job_type="structure-normalize",
        input_identity="recovery-action:expired-job",
        now=now,
        lease_seconds=1,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-expired-job",
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )

    action = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )

    assert action.state == "pending"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT state, lease_owner, lease_epoch, lease_expires_at
            FROM m1_jobs
            WHERE job_key = %s
            """,
            (lease.job_key,),
        )
        assert cursor.fetchone() == (
            "leased",
            lease.lease_owner,
            lease.lease_epoch,
            lease.lease_expires_at,
        )
        cursor.execute(
            """
            SELECT event_sequence, kind, detail->>'reason_code', detail->>'action_type'
            FROM m1_job_runtime_events
            WHERE job_key = %s
            ORDER BY event_sequence
            """,
            (lease.job_key,),
        )
        assert cursor.fetchall() == [
            (1, RuntimeEventKind.STARTED.value, None, None),
            (
                2,
                RuntimeEventKind.RECOVERY_STARTED.value,
                "job.lease-expired",
                RecoveryActionType.RECLAIM_JOB.value,
            ),
        ]
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (1,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (1,)


def test_recovery_action_claim_reclaims_expired_worker_lease_and_unwedges_active_index(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:reclaim-worker",
        job_type="structure-normalize",
        input_identity="recovery-action:reclaim-worker",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-reclaim-worker",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    first_claim = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker-a",
        controller=controller,
        lease_seconds=1,
        now=now + timedelta(seconds=2),
    )
    assert first_claim is not None
    assert first_claim.action_id == scheduled.action_id

    reclaimed = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker-b",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert reclaimed is not None
    assert reclaimed.action_id == scheduled.action_id
    assert reclaimed.worker_id == "action-worker-b"
    assert reclaimed.worker_epoch == first_claim.worker_epoch + 1

    stale_finish = finish_action(
        control_plane._connection_factory,
        action_id=scheduled.action_id,
        worker_id=first_claim.worker_id or "",
        worker_epoch=first_claim.worker_epoch,
        result_code="succeeded",
        now=now + timedelta(seconds=5),
        detail={"worker": "old"},
    )
    assert stale_finish.state == "running"
    assert stale_finish.worker_id == "action-worker-b"
    assert stale_finish.result_code is None

    finished = finish_action(
        control_plane._connection_factory,
        action_id=scheduled.action_id,
        worker_id=reclaimed.worker_id or "",
        worker_epoch=reclaimed.worker_epoch,
        result_code="failed",
        now=now + timedelta(seconds=5),
        detail={"worker": "new"},
    )
    assert finished.state == "completed"
    assert finished.result_code == "failed"

    next_controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-reclaim-worker-next",
        lease_seconds=30,
        now=now + timedelta(seconds=6),
    )
    next_action = schedule_action(
        control_plane._connection_factory,
        controller=next_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=7),
    )
    assert next_action.state == "pending"
    assert next_action.action_id != scheduled.action_id


def test_recovery_action_finish_requires_unexpired_worker_lease_and_exact_replay(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:finish-replay",
        job_type="structure-normalize",
        input_identity="recovery-action:finish-replay",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-finish-replay",
        lease_seconds=30,
        now=now,
    )
    schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    claim = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker",
        controller=controller,
        lease_seconds=1,
        now=now + timedelta(seconds=2),
    )
    assert claim is not None

    expired_finish = finish_action(
        control_plane._connection_factory,
        action_id=claim.action_id,
        worker_id=claim.worker_id or "",
        worker_epoch=claim.worker_epoch,
        result_code="succeeded",
        now=now + timedelta(seconds=4),
        detail={"result": "late"},
    )
    assert expired_finish.state == "running"
    assert expired_finish.result_code is None

    reclaimed = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker-reclaim",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=5),
    )
    assert reclaimed is not None
    finished = finish_action(
        control_plane._connection_factory,
        action_id=claim.action_id,
        worker_id=reclaimed.worker_id or "",
        worker_epoch=reclaimed.worker_epoch,
        result_code="succeeded",
        now=now + timedelta(seconds=6),
        detail={"result": "ok"},
    )
    assert (
        finish_action(
            control_plane._connection_factory,
            action_id=claim.action_id,
            worker_id=reclaimed.worker_id or "",
            worker_epoch=reclaimed.worker_epoch,
            result_code="succeeded",
            now=now + timedelta(seconds=7),
            detail={"result": "ok"},
        )
        == finished
    )
    with pytest.raises(RecoveryActionConflict, match="finish replay"):
        finish_action(
            control_plane._connection_factory,
            action_id=claim.action_id,
            worker_id=reclaimed.worker_id or "",
            worker_epoch=reclaimed.worker_epoch,
            result_code="failed",
            now=now + timedelta(seconds=7),
            detail={"result": "ok"},
        )
    with pytest.raises(RecoveryActionConflict, match="finish replay"):
        finish_action(
            control_plane._connection_factory,
            action_id=claim.action_id,
            worker_id=reclaimed.worker_id or "",
            worker_epoch=reclaimed.worker_epoch,
            result_code="succeeded",
            now=now + timedelta(seconds=7),
            detail={"result": "changed"},
        )


def test_recovery_action_claim_is_scoped_by_controller_id_and_epoch(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:controller-scope",
        job_type="structure-normalize",
        input_identity="recovery-action:controller-scope",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller_a = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler-a",
        owner_id="controller-a",
        lease_seconds=30,
        now=now,
    )
    controller_b = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler-b",
        owner_id="controller-b",
        lease_seconds=30,
        now=now,
    )
    assert controller_a.lease_epoch == controller_b.lease_epoch == 1
    action = schedule_action(
        control_plane._connection_factory,
        controller=controller_a,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )

    assert (
        claim_action(
            control_plane._connection_factory,
            worker_id="wrong-controller-worker",
            controller=controller_b,
            lease_seconds=30,
            now=now + timedelta(seconds=2),
        )
        is None
    )
    claimed = claim_action(
        control_plane._connection_factory,
        worker_id="right-controller-worker",
        controller=controller_a,
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert claimed is not None
    assert claimed.action_id == action.action_id


def test_recovery_action_concurrent_exact_schedule_replay_is_atomic(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:concurrent-replay",
        job_type="structure-normalize",
        input_identity="recovery-action:concurrent-replay",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-concurrent-replay",
        lease_seconds=30,
        now=now,
    )
    barrier = Barrier(2)

    def schedule_from_thread() -> object:
        barrier.wait(timeout=5)
        return schedule_action(
            control_plane._connection_factory,
            controller=controller,
            decision=_recovery_decision(now),
            incident_key=f"incident:{lease.job_key}",
            component="structure-normalize",
            target_type="job",
            target_id=lease.job_key,
            expected_attempt_id=attempt_id,
            expected_lease_epoch=lease.lease_epoch,
            recovery_budget_remaining=1,
            cooldown_seconds=0,
            channels=("dashboard",),
            now=now + timedelta(seconds=1),
            detail={"concurrent": "exact"},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: schedule_from_thread(), range(2)))

    assert results[0] == results[1]
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_recovery_actions")
        assert cursor.fetchone() == (1,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.RECOVERY_STARTED.value),
        )
        assert cursor.fetchone() == (1,)
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (1,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (1,)


def test_recovery_action_persisted_budget_does_not_reset_on_controller_reclaim(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:budget-reclaim",
        job_type="structure-normalize",
        input_identity="recovery-action:budget-reclaim",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-budget",
        lease_seconds=30,
        now=now,
    )
    first = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    claim = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert claim is not None
    finish_action(
        control_plane._connection_factory,
        action_id=first.action_id,
        worker_id=claim.worker_id or "",
        worker_epoch=claim.worker_epoch,
        result_code="failed",
        now=now + timedelta(seconds=3),
    )

    reclaimed_controller = claim_controller(
        control_plane._connection_factory,
        controller_id=controller.controller_id,
        owner_id="controller-budget-reclaimed",
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    exhausted = schedule_action(
        control_plane._connection_factory,
        controller=reclaimed_controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=99,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=5),
    )
    assert exhausted.state == "completed"
    assert exhausted.result_code == "disabled-action"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT remaining_actions
            FROM m1_recovery_target_budgets
            WHERE controller_id = %s AND target_type = 'job' AND target_id = %s
            """,
            (controller.controller_id, lease.job_key),
        )
        assert cursor.fetchone() == (0,)


def test_recovery_action_concurrent_last_budget_unit_is_consumed_once(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:budget-concurrent",
        job_type="structure-normalize",
        input_identity="recovery-action:budget-concurrent",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-budget-concurrent",
        lease_seconds=30,
        now=now,
    )
    barrier = Barrier(2)

    def schedule_from_thread() -> object:
        barrier.wait(timeout=5)
        return schedule_action(
            control_plane._connection_factory,
            controller=controller,
            decision=_recovery_decision(now),
            incident_key=f"incident:{lease.job_key}",
            component="structure-normalize",
            target_type="job",
            target_id=lease.job_key,
            expected_attempt_id=attempt_id,
            expected_lease_epoch=lease.lease_epoch,
            recovery_budget_remaining=1,
            cooldown_seconds=0,
            channels=("dashboard",),
            now=now + timedelta(seconds=1),
            detail={"budget": "last-unit"},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(lambda _index: schedule_from_thread(), range(2)))

    assert results[0] == results[1]
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT remaining_actions
            FROM m1_recovery_target_budgets
            WHERE controller_id = %s AND target_type = 'job' AND target_id = %s
            """,
            (controller.controller_id, lease.job_key),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_recovery_actions WHERE state = 'pending'")
        assert cursor.fetchone() == (1,)


def test_recovery_action_cooldown_is_checked_from_persisted_target_state(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:cooldown",
        job_type="structure-normalize",
        input_identity="recovery-action:cooldown",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-cooldown",
        lease_seconds=30,
        now=now,
    )
    first = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=2,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    claim = claim_action(
        control_plane._connection_factory,
        worker_id="action-worker",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert claim is not None
    finish_action(
        control_plane._connection_factory,
        action_id=first.action_id,
        worker_id=claim.worker_id or "",
        worker_epoch=claim.worker_epoch,
        result_code="failed",
        now=now + timedelta(seconds=3),
    )
    controller_next = claim_controller(
        control_plane._connection_factory,
        controller_id=controller.controller_id,
        owner_id="controller-cooldown-next",
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )

    blocked = schedule_action(
        control_plane._connection_factory,
        controller=controller_next,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=99,
        cooldown_seconds=60,
        channels=("dashboard",),
        now=now + timedelta(seconds=10),
    )
    assert blocked.state == "completed"
    assert blocked.result_code == "disabled-action"


def test_recovery_started_detail_uses_actual_progress_stalled_decision(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-action:progress-detail",
        job_type="structure-normalize",
        input_identity="recovery-action:progress-detail",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="controller-progress-detail",
        lease_seconds=30,
        now=now,
    )

    schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_progress_stalled_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )

    with (
        control_plane._connection_factory() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        cursor.execute(
            """
            SELECT event_id, job_key, attempt_id, lease_epoch, worker_id,
                   event_sequence, kind, stage, progress_sequence, progress_current,
                   progress_total, detail, occurred_at, idempotency_key
            FROM m1_job_runtime_events
            WHERE job_key = %s AND kind = %s
            """,
            (lease.job_key, RuntimeEventKind.RECOVERY_STARTED.value),
        )
        row = cursor.fetchone()
    assert row is not None
    event = _event_from_row(row)
    assert event.kind is RuntimeEventKind.RECOVERY_STARTED
    assert event.detail["reason_code"] == "job.progress-stalled"
    assert event.detail["action_type"] == RecoveryActionType.CANCEL_JOB.value
    detail = event.detail
    assert "Authorization" not in str(detail)
    assert len(str(detail)) < 4096


def test_recovery_executor_heartbeats_exact_attempt_without_business_receipt(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:heartbeat",
        job_type="structure-normalize",
        input_identity="recovery-executor:heartbeat",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-heartbeat",
        lease_seconds=30,
        now=now,
    )
    decision = RecoveryDecision(
        action=RecoveryActionType.HEARTBEAT_JOB,
        reason_code="job.lease-at-risk",
        incident_severity="warning",
        qualification_breaking=False,
        next_check_at=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=decision,
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    result = RecoveryExecutor(
        connection_factory=control_plane._connection_factory,
        control_plane=control_plane,
        controller=controller,
        worker_id="executor-heartbeat-worker",
        heartbeat_lease_seconds=77,
    ).run_once(now=now + timedelta(seconds=2))

    assert result is not None and result.outcome == "succeeded"
    assert result.action_id == scheduled.action_id
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("completed", "succeeded")
        cursor.execute("SELECT count(*) FROM m1_checkpoint_receipts")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_publication_pointers")
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT lease_expires_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (now + timedelta(seconds=2 + 77),)


def test_recovery_executor_cancel_is_cooperative_retry_and_exactly_fenced(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:cancel",
        job_type="structure-normalize",
        input_identity="recovery-executor:cancel",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-cancel",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_progress_stalled_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    result = RecoveryExecutor(
        connection_factory=control_plane._connection_factory,
        control_plane=control_plane,
        controller=controller,
        worker_id="executor-cancel-worker",
    ).run_once(now=now + timedelta(seconds=2))

    assert result is not None and result.outcome == "succeeded"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, lease_owner FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("retryable", None)
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("completed", "succeeded")
        cursor.execute(
            "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key LIKE %s",
            (f"%{lease.job_key}%",),
        )
        assert cursor.fetchone() == (0,)


def test_recovery_executor_reclaims_expired_lease_without_claiming_another_job(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:reclaim",
        job_type="structure-normalize",
        input_identity="recovery-executor:reclaim",
        now=now,
        lease_seconds=1,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-reclaim",
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_recovery_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=2),
    )
    result = RecoveryExecutor(
        connection_factory=control_plane._connection_factory,
        control_plane=control_plane,
        controller=controller,
        worker_id="executor-reclaim-worker",
    ).run_once(now=now + timedelta(seconds=3))

    assert result is not None and result.outcome == "succeeded"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, lease_owner, lease_expires_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        state, owner, expires_at = cursor.fetchone()
        assert state == "retryable"
        assert owner is None and expires_at is None
        cursor.execute(
            "SELECT state, recovery_state FROM m1_job_attempts AS a "
            "JOIN m1_job_runtime_state AS r USING (job_key, lease_epoch) "
            "WHERE a.attempt_id = %s",
            (attempt_id,),
        )
        assert cursor.fetchone() == ("retryable", "recovered")
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("completed", "succeeded")


def test_recovery_executor_releases_one_due_circuit_probe(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:probe",
        job_type="structure-normalize",
        input_identity="recovery-executor:probe",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    with control_plane._connection_factory() as connection:
        connection.execute(
            "UPDATE m1_jobs SET state = 'retryable', lease_owner = NULL, "
            "lease_expires_at = NULL, next_attempt_at = %s WHERE job_key = %s",
            (now + timedelta(seconds=30), lease.job_key),
        )
        connection.execute(
            "INSERT INTO m1_job_circuits "
            "(job_key, consecutive_failures, state, opened_at, next_probe_at, updated_at) "
            "VALUES (%s, 3, 'open', %s, %s, %s)",
            (lease.job_key, now - timedelta(minutes=5), now - timedelta(seconds=1), now),
        )
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-probe",
        lease_seconds=30,
        now=now,
    )
    decision = RecoveryDecision(
        action=RecoveryActionType.PROBE_CIRCUIT,
        reason_code="circuit.probe-due",
        incident_severity="warning",
        qualification_breaking=False,
        next_check_at=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=decision,
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="circuit",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    result = RecoveryExecutor(
        connection_factory=control_plane._connection_factory,
        control_plane=control_plane,
        controller=controller,
        worker_id="executor-probe-worker",
    ).run_once(now=now + timedelta(seconds=2))

    assert result is not None and result.outcome == "succeeded"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, next_attempt_at FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("retryable", now + timedelta(seconds=2))
        cursor.execute(
            "SELECT state, next_probe_at > %s FROM m1_job_circuits WHERE job_key = %s",
            (now + timedelta(seconds=2), lease.job_key),
        )
        assert cursor.fetchone() == ("open", True)
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("completed", "succeeded")


def test_recovery_action_old_worker_cannot_mutate_after_action_lease_reclaim(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:action-fence",
        job_type="structure-normalize",
        input_identity="recovery-executor:action-fence",
        now=now,
        lease_seconds=30,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-action-fence",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_progress_stalled_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )
    old_claim = claim_action(
        control_plane._connection_factory,
        worker_id="old-action-worker",
        controller=controller,
        lease_seconds=1,
        now=now + timedelta(seconds=2),
    )
    assert old_claim is not None
    new_claim = claim_action(
        control_plane._connection_factory,
        worker_id="new-action-worker",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=4),
    )
    assert new_claim is not None
    assert new_claim.worker_epoch == old_claim.worker_epoch + 1

    def dispatch(cursor: psycopg.Cursor[Any], action: RecoveryActionRecord) -> str:
        return control_plane._execute_recovery_action_cursor(
            cursor,
            action,
            now=now + timedelta(seconds=4),
        )

    stale = execute_claimed_action(
        control_plane._connection_factory,
        action_id=scheduled.action_id,
        worker_id=old_claim.worker_id or "",
        worker_epoch=old_claim.worker_epoch,
        controller=controller,
        now=now + timedelta(seconds=4),
        callback=dispatch,
    )
    assert stale.state == "running"
    assert stale.worker_id == "new-action-worker"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, lease_owner FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased", lease.lease_owner)

    finished = execute_claimed_action(
        control_plane._connection_factory,
        action_id=scheduled.action_id,
        worker_id=new_claim.worker_id or "",
        worker_epoch=new_claim.worker_epoch,
        controller=controller,
        now=now + timedelta(seconds=4),
        callback=dispatch,
    )
    assert finished.state == "completed"
    assert finished.result_code == "succeeded"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, lease_owner FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("retryable", None)


def test_recovery_action_atomic_rollback_keeps_business_and_action_running(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:atomic-rollback",
        job_type="structure-normalize",
        input_identity="recovery-executor:atomic-rollback",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-atomic-rollback",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_progress_stalled_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now + timedelta(seconds=1),
    )

    def fail_after_business_write(
        cursor: psycopg.Cursor[Any],
        _action: RecoveryActionRecord,
        *,
        now: datetime,
        heartbeat_lease_seconds: int,
    ) -> str:
        cursor.execute(
            "UPDATE m1_jobs SET last_error_class = %s WHERE job_key = %s",
            ("injected-partial-write", lease.job_key),
        )
        raise RuntimeError("injected action-terminal failure")

    monkeypatch.setattr(control_plane, "_execute_recovery_action_cursor", fail_after_business_write)
    executor = RecoveryExecutor(
        connection_factory=control_plane._connection_factory,
        control_plane=control_plane,
        controller=controller,
        worker_id="executor-atomic-worker",
    )
    with pytest.raises(RuntimeError, match="injected action-terminal failure"):
        executor.run_once(now=now + timedelta(seconds=2))

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, lease_owner, last_error_class FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased", lease.lease_owner, None)
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("running", None)


def test_recovery_business_mutations_have_no_public_standalone_bypass() -> None:
    for method_name in (
        "heartbeat_recovering_job",
        "cancel_stalled_job",
        "release_retryable_job",
        "reclaim_expired_job",
        "release_one_circuit_probe",
    ):
        assert not hasattr(PostgresControlPlane, method_name)
    assert hasattr(PostgresControlPlane, "_execute_recovery_action_cursor")


def test_action_terminal_uses_db_clock_and_rolls_back_after_worker_lease_expires(
    control_plane: PostgresControlPlane,
) -> None:
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT clock_timestamp()")
        row = cursor.fetchone()
    assert row is not None
    now = cast(datetime, row[0])
    lease = _seed_claimed_job(
        control_plane,
        job_key="recovery-executor:terminal-clock",
        job_type="structure-normalize",
        input_identity="recovery-executor:terminal-clock",
        now=now,
    )
    attempt_id = _runtime_attempt_id(control_plane, lease.job_key)
    controller = claim_controller(
        control_plane._connection_factory,
        controller_id="m1-runtime-reconciler",
        owner_id="executor-terminal-clock",
        lease_seconds=30,
        now=now,
    )
    scheduled = schedule_action(
        control_plane._connection_factory,
        controller=controller,
        decision=_progress_stalled_decision(now),
        incident_key=f"incident:{lease.job_key}",
        component="structure-normalize",
        target_type="job",
        target_id=lease.job_key,
        expected_attempt_id=attempt_id,
        expected_lease_epoch=lease.lease_epoch,
        recovery_budget_remaining=1,
        cooldown_seconds=0,
        channels=("dashboard",),
        now=now,
    )
    claimed = claim_action(
        control_plane._connection_factory,
        worker_id="terminal-clock-worker",
        controller=controller,
        lease_seconds=1,
        now=now,
    )
    assert claimed is not None

    def callback(cursor: psycopg.Cursor[Any], _action: RecoveryActionRecord) -> str:
        cursor.execute(
            "UPDATE m1_jobs SET last_error_class = %s WHERE job_key = %s",
            ("injected-terminal-window", lease.job_key),
        )
        for _ in range(4):
            cursor.execute("SELECT pg_sleep(0.3)")
        return "succeeded"

    with pytest.raises(RecoveryActionConflict, match="worker lease"):
        execute_claimed_action(
            control_plane._connection_factory,
            action_id=scheduled.action_id,
            worker_id=claimed.worker_id or "",
            worker_epoch=claimed.worker_epoch,
            controller=controller,
            now=now,
            callback=callback,
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, result_code FROM m1_recovery_actions WHERE action_id = %s",
            (scheduled.action_id,),
        )
        assert cursor.fetchone() == ("running", None)
        cursor.execute(
            "SELECT state, last_error_class FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased", None)

    reclaimed = claim_action(
        control_plane._connection_factory,
        worker_id="terminal-clock-reclaimer",
        controller=controller,
        lease_seconds=30,
        now=now + timedelta(seconds=2),
    )
    assert reclaimed is not None
    assert reclaimed.worker_epoch == claimed.worker_epoch + 1


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


def test_retryable_finish_lock_timeout_rolls_back_job_circuit_incident_and_alert(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="retry:lock-timeout",
        job_type="structure-fetch",
        input_identity="retry-lock-timeout",
        now=now,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute(
                "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE",
                (lease.job_key,),
            )
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            control_plane.finish_retryable_with_incident(
                lease,
                error_class="TimeoutError",
                incident_key="incident:retry:lock-timeout",
                dedupe_key="job-retry:retry:lock-timeout",
                component="structure-fetch",
                summary="retry lock timeout",
                detail={"job_key": lease.job_key},
                channels=("dashboard",),
                now=now,
            )
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, last_error_class FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased", None)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute("SELECT count(*) FROM m1_job_circuits")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_incidents")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (0,)


def test_retryable_finish_statement_timeout_rolls_back_job_circuit_incident_and_alert(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="retry:statement-timeout",
        job_type="structure-fetch",
        input_identity="retry-statement-timeout",
        now=now,
        lease_seconds=2,
    )
    function_name = "m1_test_retry_timeout_fn"
    trigger_name = "m1_test_retry_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_jobs",
        when_clause="OLD.state = 'leased' AND NEW.state = 'retryable'",
    )
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            control_plane.finish_retryable_with_incident(
                lease,
                error_class="TimeoutError",
                incident_key="incident:retry:statement-timeout",
                dedupe_key="job-retry:retry:statement-timeout",
                component="structure-fetch",
                summary="retry statement timeout",
                detail={"job_key": lease.job_key},
                channels=("dashboard",),
                now=now,
            )
        assert time.monotonic() - started < 3
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_jobs",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state, last_error_class FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("leased", None)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute("SELECT count(*) FROM m1_job_circuits")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_incidents")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_incident_events")
        assert cursor.fetchone() == (0,)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (0,)


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


def test_job_recovery_lock_timeout_rolls_back_circuit_incident_event_and_alert(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_succeeded_recovery_job(
        control_plane,
        job_key="recovery:lock-timeout",
        job_type="structure-fetch",
        now=now,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute(
                "SELECT job_key FROM m1_jobs WHERE job_key = %s FOR UPDATE",
                (lease.job_key,),
            )
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            control_plane.record_job_recovery(
                lease,
                component="structure-fetch",
                channels=("dashboard",),
                now=now + timedelta(seconds=16),
            )
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT consecutive_failures, state FROM m1_job_circuits WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1, "closed")
        cursor.execute(
            "SELECT state, resolved_at IS NOT NULL FROM m1_incidents WHERE dedupe_key = %s",
            (f"job-retry:{lease.job_key}",),
        )
        assert cursor.fetchone() == ("open", False)
        cursor.execute("SELECT kind FROM m1_incident_events")
        assert cursor.fetchone() == ("attempt-failed",)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (1,)


def test_job_recovery_statement_timeout_rolls_back_circuit_incident_event_and_alert(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    lease = _seed_succeeded_recovery_job(
        control_plane,
        job_key="recovery:statement-timeout",
        job_type="structure-fetch",
        now=now,
        recovery_lease_seconds=4,
    )
    function_name = "m1_test_recovery_timeout_fn"
    trigger_name = "m1_test_recovery_timeout_trigger"
    _install_sleep_trigger(
        control_plane,
        function_name=function_name,
        trigger_name=trigger_name,
        table_name="m1_incidents",
        when_clause="OLD.state = 'open' AND NEW.state = 'resolved'",
    )
    try:
        started = time.monotonic()
        with pytest.raises(psycopg.errors.QueryCanceled):
            control_plane.record_job_recovery(
                lease,
                component="structure-fetch",
                channels=("dashboard",),
                now=now + timedelta(seconds=16),
            )
        assert time.monotonic() - started < 4
    finally:
        _remove_sleep_trigger(
            control_plane,
            function_name=function_name,
            trigger_name=trigger_name,
            table_name="m1_incidents",
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT state FROM m1_jobs WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT consecutive_failures, state FROM m1_job_circuits WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (1, "closed")
        cursor.execute(
            "SELECT state, resolved_at IS NOT NULL FROM m1_incidents WHERE dedupe_key = %s",
            (f"job-retry:{lease.job_key}",),
        )
        assert cursor.fetchone() == ("open", False)
        cursor.execute("SELECT kind FROM m1_incident_events")
        assert cursor.fetchone() == ("attempt-failed",)
        cursor.execute("SELECT count(*) FROM m1_alert_outbox")
        assert cursor.fetchone() == (1,)


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


def test_operational_snapshot_projects_external_runtime_watchdog_source(
    control_plane: PostgresControlPlane,
) -> None:
    """Dashboard must not hide an open external monitor incident."""
    now = datetime(2026, 8, 18, tzinfo=UTC)
    control_plane.record_incident_event(
        incident_key="external-watchdog-a",
        dedupe_key="runtime-watchdog:cloudflare-watchdog-supervisor",
        component="runtime-watchdog",
        severity="critical",
        summary="External watchdog found alert unavailable",
        kind="detected",
        detail={"source": "cloudflare-watchdog-supervisor", "failures": ["machine:alert:stopped"]},
        idempotency_key="external-watchdog-a:detected",
        channels=("telegram",),
        now=now,
    )

    runtime_watchdog = control_plane.operational_snapshot(now=now)["runtime_watchdog"]

    assert runtime_watchdog == {
        "current": {
            "incident_key": "external-watchdog-a",
            "severity": "critical",
            "summary": "External watchdog found alert unavailable",
            "opened_at": "2026-08-18T00:00:00+00:00",
            "source": "cloudflare-watchdog-supervisor",
            "failures": ["machine:alert:stopped"],
        },
        "recent_events": [
            {
                "incident_key": "external-watchdog-a",
                "severity": "critical",
                "summary": "External watchdog found alert unavailable",
                "kind": "detected",
                "occurred_at": "2026-08-18T00:00:00+00:00",
                "detail": {
                    "source": "cloudflare-watchdog-supervisor",
                    "failures": ["machine:alert:stopped"],
                },
            }
        ],
    }


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


def test_structure_source_existing_receipt_recovers_terminal_runtime_atomically(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-recovery:source"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    lease = control_plane.claim_job(
        worker_id="source-recovery",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    artifact_key = "structure-source/runtime-recovery/source.json"
    artifact_digest = "a" * 64
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_checkpoint_receipts "
            "(receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor, "
            "checkpoint_digest, artifact_key, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                f"checkpoint:{lease.job_key}",
                lease.job_key,
                lease.lease_epoch,
                f"structure-source-page:{lease.job_key}:{artifact_digest}",
                "events:0",
                artifact_digest,
                artifact_key,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO m1_structure_source_page_receipts "
            "(job_key, artifact_key, artifact_digest, next_cursor, completed, "
            "record_count, committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (lease.job_key, artifact_key, artifact_digest, None, True, 1, now),
        )

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected source recovery event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected source recovery event failure"):
        control_plane.record_structure_source_page(
            lease,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    successor = control_plane.record_structure_source_page(
        lease,
        artifact_key=artifact_key,
        artifact_digest=artifact_digest,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    assert successor is not None
    assert successor.stream == "markets"
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)

    # The exact replay is read-only even after the original lease expires.
    assert (
        control_plane.record_structure_source_page(
            lease,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now + timedelta(seconds=31),
        )
        is None
    )


def test_structure_bundle_existing_receipt_recovers_terminal_runtime_atomically(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-recovery:bundle"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    source_lease = control_plane.claim_job(
        worker_id="source-recovery",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert source_lease is not None
    control_plane.record_structure_source_page(
        source_lease,
        artifact_key="structure-source/runtime-recovery/bundle-source.json",
        artifact_digest="b" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )
    materializer = control_plane.claim_job(
        worker_id="materializer-recovery",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    source_digest = control_plane.structure_source_window_digest(window_key)
    identity = StructureBundleIdentity(
        publication_id="runtime-recovery-bundle",
        window_id=window_key,
        snapshot_id=7,
        comparison_receipt_digest=source_digest,
        normalization_contract_version="structure-v7",
        source_kind="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-recovery-bundle"}\n')
    specs = control_plane.enqueue_structure_generation(
        identity=identity,
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_structure_source_window_bundles "
            "(window_key, producer_job_key, source_digest, bundle_key, "
            "bundle_digest, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (window_key, materializer.job_key, source_digest, bundle.key, bundle.sha256, now),
        )

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected bundle recovery event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected bundle recovery event failure"):
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (materializer.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (materializer.job_key, materializer.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (materializer.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    assert (
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now,
        )
        == specs
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (materializer.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (materializer.job_key, materializer.lease_epoch),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (materializer.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)
    assert (
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now + timedelta(seconds=31),
        )
        == specs
    )


def test_structure_range_existing_receipt_recovers_terminal_runtime_atomically(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-recovery-range"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="normalizer-recovery",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    artifact_key = "structure-ranges/runtime-recovery/rows.ndjson"
    artifact_digest = "c" * 64
    idempotency_key = f"structure-range:{lease.job_key}:{artifact_digest}"
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_checkpoint_receipts "
            "(receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor, "
            "checkpoint_digest, artifact_key, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                f"checkpoint:{lease.job_key}",
                lease.job_key,
                lease.lease_epoch,
                idempotency_key,
                f"{spec.component}:{spec.ordinal}",
                artifact_digest,
                artifact_key,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO m1_structure_range_receipts "
            "(job_key, bundle_digest, component, range_digest, artifact_key, "
            "artifact_digest, record_count, committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                lease.job_key,
                spec.bundle_digest,
                spec.component,
                spec.range_digest,
                artifact_key,
                artifact_digest,
                1,
                now,
            ),
        )

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected range recovery event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected range recovery event failure"):
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    assert (
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now,
        ).job_key
        == lease.job_key
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)
    assert (
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now + timedelta(seconds=31),
        ).job_key
        == lease.job_key
    )


def test_structure_source_succeeded_without_event_repairs_only_proven_attempt(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-repair:source"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    lease = control_plane.claim_job(
        worker_id="source-repair",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    artifact_key = "structure-source/runtime-repair/source.json"
    artifact_digest = "d" * 64
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_checkpoint_receipts "
            "(receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor, "
            "checkpoint_digest, artifact_key, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                f"checkpoint:{lease.job_key}",
                lease.job_key,
                lease.lease_epoch,
                f"structure-source-page:{lease.job_key}:{artifact_digest}",
                "events:0",
                artifact_digest,
                artifact_key,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO m1_structure_source_page_receipts "
            "(job_key, artifact_key, artifact_digest, next_cursor, completed, "
            "record_count, committed_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (lease.job_key, artifact_key, artifact_digest, None, True, 1, now),
        )
    _mark_structure_job_succeeded_without_runtime_event(
        control_plane,
        lease,
        checkpoint_cursor="events:0",
        checkpoint_digest=artifact_digest,
        now=now,
    )

    def fail_repair_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected source repair event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_repair_event)
    with pytest.raises(RuntimeError, match="injected source repair event failure"):
        control_plane.record_structure_source_page(
            lease,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now + timedelta(seconds=31),
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    assert (
        control_plane.record_structure_source_page(
            lease,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now + timedelta(seconds=31),
        )
        is None
    )
    assert (
        control_plane.record_structure_source_page(
            lease,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now + timedelta(seconds=32),
        )
        is None
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


def test_structure_bundle_succeeded_without_event_repairs_only_proven_attempt(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-repair:bundle"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    source_lease = control_plane.claim_job(
        worker_id="source-repair",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert source_lease is not None
    control_plane.record_structure_source_page(
        source_lease,
        artifact_key="structure-source/runtime-repair/bundle-source.json",
        artifact_digest="e" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )
    materializer = control_plane.claim_job(
        worker_id="materializer-repair",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    source_digest = control_plane.structure_source_window_digest(window_key)
    identity = StructureBundleIdentity(
        publication_id="runtime-repair-bundle",
        window_id=window_key,
        snapshot_id=8,
        comparison_receipt_digest=source_digest,
        normalization_contract_version="structure-v7",
        source_kind="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-repair-bundle"}\n')
    specs = control_plane.enqueue_structure_generation(
        identity=identity,
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_structure_source_window_bundles "
            "(window_key, producer_job_key, source_digest, bundle_key, "
            "bundle_digest, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (window_key, materializer.job_key, source_digest, bundle.key, bundle.sha256, now),
        )
    _mark_structure_job_succeeded_without_runtime_event(
        control_plane,
        materializer,
        checkpoint_cursor="bundle",
        checkpoint_digest=bundle.sha256,
        now=now,
    )

    def fail_repair_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected bundle repair event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_repair_event)
    with pytest.raises(RuntimeError, match="injected bundle repair event failure"):
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now + timedelta(seconds=31),
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (materializer.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (materializer.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    assert (
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now + timedelta(seconds=31),
        )
        == specs
    )
    assert (
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now + timedelta(seconds=32),
        )
        == specs
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (materializer.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


def test_structure_range_succeeded_without_event_repairs_only_proven_attempt(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-repair-range"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="normalizer-repair",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    artifact_key = "structure-ranges/runtime-repair/rows.ndjson"
    artifact_digest = "f" * 64
    idempotency_key = f"structure-range:{lease.job_key}:{artifact_digest}"
    with control_plane._connection_factory() as connection:
        connection.execute(
            "INSERT INTO m1_checkpoint_receipts "
            "(receipt_id, job_key, lease_epoch, idempotency_key, checkpoint_cursor, "
            "checkpoint_digest, artifact_key, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                f"checkpoint:{lease.job_key}",
                lease.job_key,
                lease.lease_epoch,
                idempotency_key,
                f"{spec.component}:{spec.ordinal}",
                artifact_digest,
                artifact_key,
                now,
            ),
        )
        connection.execute(
            "INSERT INTO m1_structure_range_receipts "
            "(job_key, bundle_digest, component, range_digest, artifact_key, "
            "artifact_digest, record_count, committed_at) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (
                lease.job_key,
                spec.bundle_digest,
                spec.component,
                spec.range_digest,
                artifact_key,
                artifact_digest,
                1,
                now,
            ),
        )
    _mark_structure_job_succeeded_without_runtime_event(
        control_plane,
        lease,
        checkpoint_cursor=f"{spec.component}:{spec.ordinal}",
        checkpoint_digest=artifact_digest,
        now=now,
    )

    def fail_repair_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected range repair event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_repair_event)
    with pytest.raises(RuntimeError, match="injected range repair event failure"):
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now + timedelta(seconds=31),
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", append_runtime_event_cursor)
    assert (
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now + timedelta(seconds=31),
        ).job_key
        == lease.job_key
    )
    assert (
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key=artifact_key,
            artifact_digest=artifact_digest,
            record_count=1,
            now=now + timedelta(seconds=32),
        ).job_key
        == lease.job_key
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (1,)


@pytest.mark.parametrize(
    "reader_name",
    (
        "structure_source_page_spec",
        "structure_source_page_receipt",
        "structure_range_spec",
        "structure_manifest_payload",
        "structure_generation_receipts",
    ),
)
def test_structure_reads_set_bounded_read_timeouts(
    reader_name: str,
) -> None:
    commands: list[str] = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: object, params: object = None) -> None:
            as_string = getattr(query, "as_string", None)
            rendered = str(as_string(None) if callable(as_string) else query)
            commands.append(" ".join(rendered.split()))

        def fetchone(self):
            return None

        def fetchall(self):
            return []

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self, **kwargs: object):
            assert kwargs == {"row_factory": dict_row}
            return Cursor()

    control_plane = PostgresControlPlane(
        cast(Callable[[], psycopg.Connection[Any]], lambda: Connection())
    )
    reader = getattr(control_plane, reader_name)
    try:
        reader("read-timeout")
    except (ControlPlaneError, IncompleteStructureGenerationError):
        pass
    assert commands[:3] == [
        "SET TRANSACTION READ ONLY",
        "SET LOCAL statement_timeout = '5000ms'",
        "SET LOCAL lock_timeout = '1000ms'",
    ]


@pytest.mark.parametrize(
    "reader_name", ("structure_generation_receipts", "structure_manifest_payload")
)
def test_structure_generation_reads_respect_real_lock_timeout(
    control_plane: PostgresControlPlane,
    reader_name: str,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-read-lock"}\n')
    control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    blocker = control_plane._connection_factory()
    try:
        with blocker.cursor() as cursor:
            cursor.execute("LOCK TABLE m1_structure_range_inputs IN ACCESS EXCLUSIVE MODE")
        started = time.monotonic()
        with pytest.raises(psycopg.errors.LockNotAvailable):
            getattr(control_plane, reader_name)("structure-read-lock")
        assert time.monotonic() - started < 3
    finally:
        blocker.rollback()
        blocker.close()


def test_structure_source_success_event_is_atomic_and_fenced(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-success:source"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    lease = control_plane.claim_job(
        worker_id="source-runtime",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None

    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected source success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected source success event failure"):
        control_plane.record_structure_source_page(
            lease,
            artifact_key="structure-source/runtime-success/source.json",
            artifact_digest="a" * 64,
            next_cursor=None,
            completed=True,
            record_count=1,
            now=now,
        )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("running",)
        cursor.execute(
            "SELECT count(*) FROM m1_structure_source_page_receipts WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    successor = control_plane.record_structure_source_page(
        lease,
        artifact_key="structure-source/runtime-success/source.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now,
    )
    assert successor is not None
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT attempt_id FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        attempt_row = cursor.fetchone()
        assert attempt_row is not None
        attempt_id = attempt_row[0]
        cursor.execute(
            "SELECT event_sequence, stage, detail, idempotency_key "
            "FROM m1_job_runtime_events WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (
            2,
            "commit-page",
            {
                "component": "structure-fetch",
                "data_product": "structure-sync",
                "qualification_impact": "qualified",
                "result_code": "ok",
            },
            f"runtime:{attempt_id}:succeeded",
        )


def test_structure_bundle_success_event_rolls_back_bundle_and_generation(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    window_key = "runtime-success:bundle"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    source_lease = control_plane.claim_job(
        worker_id="source-runtime",
        job_types=("structure-fetch",),
        lease_seconds=30,
        now=now,
    )
    assert source_lease is not None
    control_plane.record_structure_source_page(
        source_lease,
        artifact_key="structure-source/runtime-success/bundle-source.json",
        artifact_digest="b" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )
    materializer = control_plane.claim_job(
        worker_id="materializer-runtime",
        job_types=("structure-materialize",),
        lease_seconds=30,
        now=now,
    )
    assert materializer is not None
    source_digest = control_plane.structure_source_window_digest(window_key)
    identity = StructureBundleIdentity(
        publication_id="runtime-success-publication",
        window_id=window_key,
        snapshot_id=7,
        comparison_receipt_digest=source_digest,
        normalization_contract_version="structure-v7",
        source_kind="gamma-source-window-events-v3-sharded",
        component_counts={
            "events": 1,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": 0,
            "issues": 0,
        },
    )
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-success-bundle"}\n')
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected bundle success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected bundle success event failure"):
        control_plane.admit_structure_source_bundle(
            materializer,
            identity=identity,
            bundle=bundle,
            ranges=(("events", "", ""),),
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (materializer.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_structure_source_window_bundles WHERE window_key = %s",
            (window_key,),
        )
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_structure_generation_inputs WHERE generation_key LIKE %s",
            (f"{window_key}:%",),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    specs = control_plane.admit_structure_source_bundle(
        materializer,
        identity=identity,
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    assert len(specs) == 1
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT event_sequence, stage FROM m1_job_runtime_events "
            "WHERE job_key = %s AND kind = %s",
            (materializer.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (2, "commit-bundle")


def test_complete_structure_range_success_event_is_atomic(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-range-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )[0]
    lease = control_plane.claim_job(
        worker_id="normalize-runtime",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert lease is not None
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected range success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected range success event failure"):
        control_plane.complete_structure_range(
            lease,
            range_digest=spec.range_digest,
            artifact_key="structure-ranges/runtime-range/rows.ndjson",
            artifact_digest="c" * 64,
            record_count=1,
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_structure_range_receipts WHERE job_key = %s",
            (lease.job_key,),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    receipt = control_plane.complete_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/runtime-range/rows.ndjson",
        artifact_digest="c" * 64,
        record_count=1,
        now=now,
    )
    assert receipt.job_key == lease.job_key
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT state FROM m1_job_attempts WHERE job_key = %s AND lease_epoch = %s",
            (lease.job_key, lease.lease_epoch),
        )
        assert cursor.fetchone() == ("succeeded",)
        cursor.execute(
            "SELECT event_sequence, stage FROM m1_job_runtime_events "
            "WHERE job_key = %s AND kind = %s",
            (lease.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (2, "commit-range")


def test_structure_certification_success_event_rolls_back_manifest(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-certify-bundle"}\n')
    spec = control_plane.enqueue_structure_generation(
        identity=_structure_identity(),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )[0]
    range_lease = control_plane.claim_job(
        worker_id="normalize-runtime",
        job_types=("structure-normalize",),
        lease_seconds=30,
        now=now,
    )
    assert range_lease is not None
    control_plane.record_structure_range(
        range_lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/runtime-certify/rows.ndjson",
        artifact_digest="d" * 64,
        record_count=1,
        now=now,
    )
    control_plane.finish(range_lease, state=JobState.SUCCEEDED, now=now)
    certifier = control_plane.claim_job(
        worker_id="certifier-runtime",
        job_types=("structure-certify",),
        lease_seconds=30,
        now=now,
    )
    assert certifier is not None
    manifest_digest = sha256(
        canonical_structure_manifest_bytes(
            generation_key=spec.generation_key,
            bundle_digest=bundle.sha256,
            receipts=(
                {
                    "job_key": spec.job_key,
                    "component": "events",
                    "ordinal": 0,
                    "range_digest": spec.range_digest,
                    "artifact_key": "structure-ranges/runtime-certify/rows.ndjson",
                    "artifact_digest": "d" * 64,
                    "record_count": 1,
                },
            ),
        )
    ).hexdigest()
    original_append = postgres_module.append_runtime_event_cursor

    def fail_success_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected certification success event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_success_event)
    with pytest.raises(RuntimeError, match="injected certification success event failure"):
        control_plane.certify_structure_generation(
            certifier,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{manifest_digest}/manifest.ndjson",
            artifact_digest=manifest_digest,
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (certifier.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute(
            "SELECT count(*) FROM m1_generation_manifests WHERE generation_key = %s",
            (spec.generation_key,),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    assert (
        control_plane.certify_structure_generation(
            certifier,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{manifest_digest}/manifest.ndjson",
            artifact_digest=manifest_digest,
            now=now,
        )
        == manifest_digest
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT event_sequence, stage FROM m1_job_runtime_events "
            "WHERE job_key = %s AND kind = %s",
            (certifier.job_key, RuntimeEventKind.SUCCEEDED.value),
        )
        assert cursor.fetchone() == (2, "commit-certification")


def test_retry_runtime_events_are_atomic_and_do_not_copy_error_text(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime-retry:source",
        job_type="structure-fetch",
        input_identity="runtime-retry:source-input",
        now=now,
    )
    original_append = postgres_module.append_runtime_event_cursor

    def fail_retry_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected retry event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_retry_event)
    with pytest.raises(RuntimeError, match="injected retry event failure"):
        control_plane.finish_retryable_with_incident(
            lease,
            error_class="TimeoutError",
            incident_key=f"incident:job-retry:{lease.job_key}",
            dedupe_key=f"job-retry:{lease.job_key}",
            component="structure-fetch",
            summary="runtime retry failure",
            detail={"error_message": "Authorization:Bearer secret", "job_key": lease.job_key},
            channels=("dashboard",),
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute("SELECT count(*) FROM m1_job_circuits WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind IN (%s, %s)",
            (
                lease.job_key,
                RuntimeEventKind.RETRYABLE_FAILED.value,
                RuntimeEventKind.RETRY_SCHEDULED.value,
            ),
        )
        assert cursor.fetchone() == (0,)

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", original_append)
    control_plane.finish_retryable_with_incident(
        lease,
        error_class="TimeoutError",
        incident_key=f"incident:job-retry:{lease.job_key}",
        dedupe_key=f"job-retry:{lease.job_key}",
        component="structure-fetch",
        summary="runtime retry failure",
        detail={"error_message": "Authorization:Bearer secret", "job_key": lease.job_key},
        channels=("dashboard",),
        now=now,
    )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT event_sequence, kind, detail FROM m1_job_runtime_events "
            "WHERE job_key = %s ORDER BY event_sequence",
            (lease.job_key,),
        )
        events = cursor.fetchall()
        assert [row[0] for row in events] == [1, 2, 3]
        assert [row[1] for row in events] == [
            RuntimeEventKind.STARTED.value,
            RuntimeEventKind.RETRYABLE_FAILED.value,
            RuntimeEventKind.RETRY_SCHEDULED.value,
        ]
        assert "error_message" not in events[1][2]
        assert "error_class" not in events[1][2]
        assert "secret" not in str(events[1][2]).lower()
        assert "error_message" not in events[2][2]
        assert "error_class" not in events[2][2]
        assert "secret" not in str(events[2][2]).lower()


def test_quote_retry_event_injection_rolls_back_circuit_and_job_transition(
    control_plane: PostgresControlPlane,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = _now()
    lease = _seed_claimed_job(
        control_plane,
        job_key="runtime-retry:quote-batch",
        job_type="quote-batch",
        input_identity="runtime-retry:quote-batch-input",
        now=now,
    )

    def fail_retry_event(*args: object, **kwargs: object) -> object:
        raise RuntimeError("injected quote retry event failure")

    monkeypatch.setattr(postgres_module, "append_runtime_event_cursor", fail_retry_event)
    with pytest.raises(RuntimeError, match="injected quote retry event failure"):
        control_plane.finish_retryable_with_incident(
            lease,
            error_class="TimeoutError",
            incident_key=f"incident:job-retry:{lease.job_key}",
            dedupe_key=f"job-retry:{lease.job_key}",
            component="quote-batch",
            summary="quote retry failure",
            detail={"error_message": "Authorization:Bearer secret"},
            channels=("dashboard",),
            now=now,
        )
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT state FROM m1_jobs WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == ("leased",)
        cursor.execute("SELECT count(*) FROM m1_job_circuits WHERE job_key = %s", (lease.job_key,))
        assert cursor.fetchone() == (0,)
        cursor.execute(
            "SELECT count(*) FROM m1_job_runtime_events WHERE job_key = %s AND kind IN (%s, %s)",
            (
                lease.job_key,
                RuntimeEventKind.RETRYABLE_FAILED.value,
                RuntimeEventKind.RETRY_SCHEDULED.value,
            ),
        )
        assert cursor.fetchone() == (0,)


def test_qualification_epoch_transition_is_state_version_cas_and_rolls_back_old_writers(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    policy = _qualification_policy()
    initial = policy.new_epoch(
        started_at=now,
        epoch_id="qualification-epoch-cas",
    )
    persisted = start_qualification_epoch(control_plane._connection_factory, initial)
    assert persisted.version == 1
    breaker = policy.apply(
        initial,
        QualificationFact.breaking(
            "fact-breaker",
            now + timedelta(hours=1),
            "lease.expired",
            policy_version=policy.policy_version,
            release_id=policy.release_id,
            config_id=policy.config_id,
            role_identity=policy.role_identity,
        ),
    )
    barrier = Barrier(2)

    def race(expected_owner: str) -> object:
        barrier.wait(timeout=10)
        try:
            return transition_qualification_epoch(
                control_plane._connection_factory,
                expected_epoch_id=initial.epoch_id,
                expected_state=initial.state,
                expected_version=1,
                next_decision=breaker,
                writer_id=expected_owner,
            )
        except BaseException as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = tuple(pool.map(race, ("writer-a", "writer-b")))

    successes = [result for result in results if not isinstance(result, BaseException)]
    failures = [result for result in results if isinstance(result, BaseException)]
    assert len(successes) == 1
    assert len(failures) == 1
    assert isinstance(failures[0], QualificationEpochConflict)

    after_race = read_qualification_epoch(
        control_plane._connection_factory,
        epoch_id=initial.epoch_id,
    )
    assert after_race is not None
    assert after_race.state == "invalidated"
    assert after_race.version == 2
    assert after_race.invalidated_at == now + timedelta(hours=1)
    assert after_race.invalidation_reason == "lease.expired"
    with pytest.raises(QualificationEpochConflict, match="state/version"):
        transition_qualification_epoch(
            control_plane._connection_factory,
            expected_epoch_id=initial.epoch_id,
            expected_state=initial.state,
            expected_version=1,
            next_decision=breaker,
            writer_id="stale-writer",
        )
    unchanged = read_qualification_epoch(
        control_plane._connection_factory,
        epoch_id=initial.epoch_id,
    )
    assert unchanged == after_race
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SELECT count(*) FROM m1_qualification_certificates")
        assert cursor.fetchone() == (0,)


def test_qualification_service_first_tick_initializes_sql_null_cursor(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    _seed_freshness_pointers(control_plane, published_at=now)
    service = _qualification_service(control_plane, batch_size=20)

    result = service.tick(now)

    assert result.applied == 3
    assert result.state is QualificationState.ACCUMULATING
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT source_cursor, jsonb_typeof(source_cursor), jsonb_array_length(fact_records)
            FROM m1_qualification_epochs
            WHERE epoch_id = %s
            """,
            (result.epoch_id,),
        )
        row = cursor.fetchone()
    assert row is not None
    assert row[0] is None
    assert row[1] is None
    assert row[2] == 3


def test_qualification_ingress_late_runtime_commit_is_consumed_after_cursor(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    _seed_freshness_pointers(control_plane, published_at=now)
    service = _qualification_service(control_plane, batch_size=20)
    first = service.tick(now)
    assert first.applied == 3

    _insert_runtime_event(
        control_plane,
        job_key="qualification:late-runtime",
        kind="job.terminal-failed",
        reason_code="lease.expired",
        occurred_at=now - timedelta(hours=3),
        sequence=2,
    )
    restarted = _qualification_service(control_plane, batch_size=20)
    second = restarted.tick(now + timedelta(seconds=1))

    assert second.state is QualificationState.RECOVERING
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*)
            FROM m1_qualification_ingress_ledger
            WHERE source = 'runtime'
            """
        )
        assert cursor.fetchone() == (2,)
        cursor.execute(
            """
            SELECT state, invalidation_reason
            FROM m1_qualification_epochs
            WHERE state = 'invalidated'
            """
        )
        assert cursor.fetchone() == ("invalidated", "lease.expired")


def test_qualification_recovery_restart_keeps_epoch_fact_history_local(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    _seed_freshness_pointers(control_plane, published_at=now)
    terminal = _insert_runtime_event(
        control_plane,
        job_key="qualification:cross-batch",
        kind="job.terminal-failed",
        reason_code="lease.expired",
        occurred_at=now,
        sequence=2,
    )
    service = _qualification_service(control_plane, batch_size=10)
    invalidated = service.tick(now + timedelta(seconds=1))
    assert invalidated.state is QualificationState.RECOVERING

    _insert_runtime_event(
        control_plane,
        job_key=terminal.job_key,
        kind="job.recovered",
        reason_code="job.recovered",
        occurred_at=now + timedelta(seconds=2),
        sequence=3,
    )
    restarted = _qualification_service(control_plane, batch_size=10)
    recovered = restarted.tick(now + timedelta(seconds=3))
    assert recovered.state is QualificationState.ACCUMULATING

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT state, jsonb_array_length(fact_records), started_at, last_fact_at
            FROM m1_qualification_epochs
            ORDER BY started_at, state
            """
        )
        rows = cursor.fetchall()
    states = [row[0] for row in rows]
    assert states == ["invalidated", "recovering", "accumulating"]
    invalidated_row = next(row for row in rows if row[0] == "invalidated")
    accumulating_row = next(row for row in rows if row[0] == "accumulating")
    recovering_row = next(row for row in rows if row[0] == "recovering")
    assert invalidated_row[1] == 2
    assert recovering_row[1] == 0
    assert accumulating_row[1] == 1
    assert accumulating_row[3] >= accumulating_row[2]


def test_qualification_same_batch_recovery_keeps_recovering_epoch_empty(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    _seed_freshness_pointers(control_plane, published_at=now)
    lease = _insert_runtime_event(
        control_plane,
        job_key="qualification:same-batch",
        kind="job.terminal-failed",
        reason_code="lease.expired",
        occurred_at=now,
        sequence=2,
    )
    _insert_runtime_event(
        control_plane,
        job_key=lease.job_key,
        kind="job.recovered",
        reason_code="job.recovered",
        occurred_at=now + timedelta(seconds=1),
        sequence=3,
    )

    result = _qualification_service(control_plane, batch_size=20).tick(now + timedelta(seconds=2))

    assert result.state is QualificationState.ACCUMULATING
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT state, jsonb_array_length(fact_records)
            FROM m1_qualification_epochs
            ORDER BY started_at, state
            """
        )
        rows = cursor.fetchall()
    assert ("recovering", 0) in rows
    assert ("accumulating", 1) in rows


def test_qualification_freshness_reobserves_same_pointer_and_invalidates_on_aging(
    control_plane: PostgresControlPlane,
) -> None:
    now = _now()
    _seed_freshness_pointers(control_plane, published_at=now - timedelta(seconds=800))
    service = _qualification_service(control_plane, batch_size=20)

    first = service.tick(now)
    assert first.state is QualificationState.ACCUMULATING
    _insert_runtime_event(
        control_plane,
        job_key="qualification:healthy-runtime",
        kind="job.succeeded",
        reason_code="",
        occurred_at=now + timedelta(seconds=50),
        sequence=2,
    )
    second = service.tick(now + timedelta(seconds=200))

    assert second.state is QualificationState.RECOVERING
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT count(*)
            FROM m1_qualification_ingress_ledger
            WHERE source = 'freshness' AND source_id LIKE 'freshness:quote:%'
            """
        )
        assert cursor.fetchone() == (2,)
        cursor.execute(
            """
            SELECT invalidation_reason
            FROM m1_qualification_epochs
            WHERE state = 'invalidated'
            """
        )
        assert cursor.fetchone() == ("freshness.structure",)


def test_qualification_certificate_is_canonical_idempotent_and_conflict_loud(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(control_plane, epoch_id="qualification-epoch-cert")
    payload = qualification_certificate_payload(qualified)
    evidence_digest = payload["evidence_digest"]
    assert isinstance(evidence_digest, str)
    assert canonical_certificate_bytes(payload) == (
        b'{"bounds":{"max_gap_seconds":900,"qualified_at":"2030-01-02T12:00:00+00:00",'
        b'"required_seconds":86400,"started_at":"2030-01-01T12:00:00+00:00"},'
        b'"contained_incidents":[],"counts":{"progress_count":12,"successful_count":12},'
        b'"evidence_digest":"' + evidence_digest.encode("ascii") + b'",'
        b'"identity":{"config_id":"config-a","epoch_id":"qualification-epoch-cert",'
        b'"policy_version":"m1-rolling-qualification-v1","release_id":"release-a",'
        b'"role_identity":["m1","structure"]},"policy_version":"m1-rolling-qualification-v1",'
        b'"recovery_actions":[],"slo":{"evidence_gap_seconds":900,"evidence_gap_status":"pass",'
        b'"freshness":"pass","recovery":"pass","required_seconds":86400}}'
    )
    first = insert_qualification_certificate(
        control_plane._connection_factory,
        decision=qualified,
    )
    replay = insert_qualification_certificate(
        control_plane._connection_factory,
        decision=qualified,
    )
    assert replay == first
    assert first.certificate_digest == certificate_digest(payload)
    assert first.certificate_id == f"qualification-certificate:{first.certificate_digest}"
    assert first.identity_key == _certificate_identity_key_for_test(payload)
    assert first.canonical_payload.encode("utf-8") == canonical_certificate_bytes(payload)
    assert first.created_at.tzinfo is not None
    assert first.created_at.utcoffset() == timedelta(0)
    assert (
        read_qualification_certificate(
            control_plane._connection_factory,
            certificate_id=first.certificate_id,
        )
        == first
    )
    assert list_qualification_certificates(control_plane._connection_factory) == (first,)

    conflict_payload = {
        **payload,
        "counts": {"progress_count": 13, "successful_count": 12},
    }
    with pytest.raises(psycopg.Error, match="qualification certificate"):
        _direct_insert_qualification_certificate(control_plane, conflict_payload)
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT count(*), min(certificate_digest) FROM m1_qualification_certificates"
        )
        assert cursor.fetchone() == (1, first.certificate_digest)
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            cursor.execute(
                "UPDATE m1_qualification_certificates SET evidence_digest = %s",
                ("f" * 64,),
            )
        connection.rollback()
        with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
            cursor.execute(
                "DELETE FROM m1_qualification_certificates WHERE certificate_id = %s",
                (first.certificate_id,),
            )
        connection.rollback()

    with pytest.raises(ValueError, match="JSON-safe"):
        canonical_certificate_bytes(
            {
                **payload,
                "identity": {
                    **cast(dict[str, object], payload["identity"]),
                    "role_identity": ("tuple-is-not-canonical-json",),
                },
            }
        )
    with pytest.raises(ValueError, match="must include"):
        certificate_digest({"identity": {"epoch_id": qualified.epoch_id}})


def test_qualification_certificate_api_rejects_forged_payload_and_bad_decision_types(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(control_plane, epoch_id="qualification-epoch-api")
    payload = qualification_certificate_payload(qualified)

    with pytest.raises(TypeError):
        cast(Any, insert_qualification_certificate)(
            control_plane._connection_factory,
            epoch_id=qualified.epoch_id,
            payload={**payload, "slo": {"freshness": "forged-pass"}},
        )
    with pytest.raises(QualificationCertificateConflict, match="qualified counts"):
        insert_qualification_certificate(
            control_plane._connection_factory,
            decision=QualificationDecision(
                state=QualificationState.QUALIFIED,
                epoch_id="qualification-epoch-bad-counts",
                started_at=_now(),
                policy_version=qualified.policy_version,
                release_id=qualified.release_id,
                config_id=qualified.config_id,
                role_identity=qualified.role_identity,
                last_fact_at=_now() + timedelta(days=1),
                qualified_at=_now() + timedelta(days=1),
                max_gap_seconds=900,
                coverage_seconds=86_400,
                progress_count=None,
                successful_count=1,
            ),
        )


def test_qualification_certificate_db_rejects_direct_forgery_and_app_role_insert(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(control_plane, epoch_id="qualification-epoch-db")
    legitimate = insert_qualification_certificate(
        control_plane._connection_factory,
        decision=qualified,
    )
    forged = {
        **qualification_certificate_payload(qualified),
        "bounds": {
            **cast(dict[str, object], qualification_certificate_payload(qualified)["bounds"]),
            "required_seconds": 1,
        },
    }
    with pytest.raises(psycopg.Error, match="qualification certificate"):
        _direct_insert_qualification_certificate(control_plane, forged)

    app_forged = {
        **qualification_certificate_payload(qualified),
        "identity": {
            **cast(dict[str, object], qualification_certificate_payload(qualified)["identity"]),
            "epoch_id": "qualification-epoch-db-forged-app",
        },
    }
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            cursor.execute("SET ROLE authenticated")
            _execute_direct_certificate_insert(cursor, app_forged)
        connection.rollback()
    assert list_qualification_certificates(control_plane._connection_factory) == (legitimate,)


def test_qualification_certificate_function_privileges_and_derived_ids(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(
        control_plane,
        epoch_id="qualification-epoch-function",
    )
    payload = qualification_certificate_payload(qualified)
    digest = certificate_digest(payload)
    expected_certificate_id = f"qualification-certificate:{digest}"
    expected_identity_key = _certificate_identity_key_for_test(payload)

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SET ROLE authenticated")
        with pytest.raises(psycopg.errors.InsufficientPrivilege):
            _execute_certificate_function(cursor, payload)

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute("SET ROLE service_role")
        service_row = _execute_certificate_function(cursor, payload)
        replay_row = _execute_certificate_function(cursor, payload)
        assert service_row == replay_row
        assert service_row == (expected_certificate_id, expected_identity_key, digest)
        cursor.execute("RESET ROLE")

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        with pytest.raises(psycopg.errors.UndefinedFunction):
            cursor.execute(
                """
                SELECT certificate_id
                FROM m1_insert_qualification_certificate(
                    'attacker-certificate', %s, 'attacker-identity',
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                """,
                (
                    qualified.epoch_id,
                    qualified.policy_version,
                    qualified.release_id,
                    qualified.config_id,
                    Jsonb(list(qualified.role_identity)),
                    qualified.started_at,
                    qualified.qualified_at,
                    Jsonb(payload),
                    canonical_certificate_bytes(payload).decode("utf-8"),
                    digest,
                    digest,
                    cast(str, payload["evidence_digest"]),
                ),
            )
        connection.rollback()
        with pytest.raises(psycopg.errors.RaiseException, match="id"):
            _execute_direct_certificate_insert(
                cursor,
                payload,
                certificate_id="attacker-certificate",
                identity_key="attacker-identity",
            )
    assert list_qualification_certificates(control_plane._connection_factory)[0] == (
        read_qualification_certificate(
            control_plane._connection_factory,
            certificate_id=expected_certificate_id,
        )
    )


def test_read_qualification_certificate_recomputes_canonical_digest_and_fails_on_tamper(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(control_plane, epoch_id="qualification-epoch-read")
    record = insert_qualification_certificate(
        control_plane._connection_factory,
        decision=qualified,
    )
    assert (
        read_qualification_certificate(
            control_plane._connection_factory,
            certificate_id=record.certificate_id,
        )
        == record
    )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "ALTER TABLE m1_qualification_certificates "
            "DISABLE TRIGGER m1_qualification_certificates_immutable"
        )
        cursor.execute(
            "UPDATE m1_qualification_certificates SET canonical_payload = %s "
            "WHERE certificate_id = %s",
            (
                canonical_certificate_bytes(
                    {
                        **record.payload,
                        "counts": {"progress_count": 999, "successful_count": 12},
                    }
                ).decode("utf-8"),
                record.certificate_id,
            ),
        )
        cursor.execute(
            "ALTER TABLE m1_qualification_certificates "
            "ENABLE TRIGGER m1_qualification_certificates_immutable"
        )

    with pytest.raises(QualificationCertificateConflict, match="digest"):
        read_qualification_certificate(
            control_plane._connection_factory,
            certificate_id=record.certificate_id,
        )


def test_read_qualification_certificate_rejects_tampered_ids(
    control_plane: PostgresControlPlane,
) -> None:
    qualified = _persist_qualified_epoch(
        control_plane,
        epoch_id="qualification-epoch-read-ids",
    )
    record = insert_qualification_certificate(
        control_plane._connection_factory,
        decision=qualified,
    )

    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            "ALTER TABLE m1_qualification_certificates "
            "DISABLE TRIGGER m1_qualification_certificates_immutable"
        )
        cursor.execute(
            "UPDATE m1_qualification_certificates SET identity_key = %s WHERE certificate_id = %s",
            ("attacker-identity", record.certificate_id),
        )
        cursor.execute(
            "ALTER TABLE m1_qualification_certificates "
            "ENABLE TRIGGER m1_qualification_certificates_immutable"
        )

    with pytest.raises(QualificationCertificateConflict, match="identity key"):
        read_qualification_certificate(
            control_plane._connection_factory,
            certificate_id=record.certificate_id,
        )


def _qualification_policy() -> RollingQualificationPolicy:
    return RollingQualificationPolicy(
        policy_version="m1-rolling-qualification-v1",
        release_id="release-a",
        config_id="config-a",
        role_identity=("m1", "structure"),
        max_gap_seconds=900,
    )


def _persist_qualified_epoch(
    control_plane: PostgresControlPlane,
    *,
    epoch_id: str,
) -> QualificationDecision:
    policy = _qualification_policy()
    started_at = _now()
    qualified_at = started_at + timedelta(days=1)
    initial = policy.new_epoch(started_at=started_at, epoch_id=epoch_id)
    qualified = QualificationDecision(
        state=QualificationState.QUALIFIED,
        epoch_id=epoch_id,
        started_at=started_at,
        policy_version=policy.policy_version,
        release_id=policy.release_id,
        config_id=policy.config_id,
        role_identity=policy.role_identity,
        last_fact_at=qualified_at,
        qualified_at=qualified_at,
        max_gap_seconds=900,
        coverage_seconds=86_400,
        progress_count=12,
        successful_count=12,
    )
    start_qualification_epoch(control_plane._connection_factory, initial)
    transition_qualification_epoch(
        control_plane._connection_factory,
        expected_epoch_id=qualified.epoch_id,
        expected_state=initial.state,
        expected_version=1,
        next_decision=qualified,
        writer_id="qualifier",
    )
    return qualified


def _qualification_service(
    control_plane: PostgresControlPlane,
    *,
    batch_size: int = 100,
) -> QualificationService:
    return QualificationService(
        policy=_qualification_policy(),
        fact_source=PostgresQualificationFactSource(control_plane._connection_factory),
        state_store=PostgresQualificationServiceStore(control_plane._connection_factory),
        writer_id="qualification-test",
        batch_size=batch_size,
    )


def _insert_runtime_event(
    control_plane: PostgresControlPlane,
    *,
    job_key: str,
    kind: str,
    reason_code: str,
    occurred_at: datetime,
    sequence: int,
) -> JobLease:
    lease = _read_runtime_lease(control_plane, job_key)
    if lease is None:
        lease = _seed_claimed_job(
            control_plane,
            job_key=job_key,
            job_type="quote-batch",
            input_identity=f"{job_key}:input",
            now=occurred_at - timedelta(seconds=1),
        )
    with control_plane._connection_factory() as connection:
        connection.execute(
            """
            INSERT INTO m1_job_runtime_events (
                event_id, job_key, attempt_id, lease_epoch, worker_id,
                event_sequence, kind, stage, progress_sequence, progress_current,
                progress_total, detail, occurred_at, idempotency_key
            )
            SELECT %s, state.job_key, state.attempt_id, state.lease_epoch,
                   state.worker_id, %s, %s, state.stage, %s, %s, NULL, %s, %s, %s
            FROM m1_job_runtime_state AS state
            WHERE state.job_key = %s
            """,
            (
                f"qualification-runtime:{job_key}:{sequence}",
                sequence,
                kind,
                sequence,
                sequence,
                Jsonb({"reason_code": reason_code} if reason_code else {}),
                occurred_at,
                f"qualification-runtime:{job_key}:{sequence}",
                job_key,
            ),
        )
    return lease


def _read_runtime_lease(
    control_plane: PostgresControlPlane,
    job_key: str,
) -> JobLease | None:
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT job.job_key, job.job_type, job.input_identity, state.worker_id,
                   state.lease_epoch, state.lease_deadline_at
            FROM m1_job_runtime_state AS state
            JOIN m1_jobs AS job ON job.job_key = state.job_key
            WHERE state.job_key = %s
            """,
            (job_key,),
        )
        row = cursor.fetchone()
    if row is None:
        return None
    return JobLease(
        job_key=str(row[0]),
        job_type=str(row[1]),
        input_identity=str(row[2]),
        lease_owner=str(row[3]),
        lease_epoch=int(cast(int, row[4])),
        lease_expires_at=cast(datetime, row[5]),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )


def _seed_freshness_pointers(
    control_plane: PostgresControlPlane,
    *,
    published_at: datetime,
) -> None:
    structure_key = "structure:qualification-freshness"
    quote_key = "quote:qualification-freshness"
    with control_plane._connection_factory() as connection:
        for job_key, job_type in (
            ("job:qualification-structure", "structure"),
            ("job:qualification-quote", "quote-certify"),
        ):
            connection.execute(
                """
                INSERT INTO m1_jobs(
                    job_key, job_type, input_identity, state, created_at, updated_at
                )
                VALUES (%s, %s, %s, 'succeeded', %s, %s)
                ON CONFLICT (job_key) DO NOTHING
                """,
                (job_key, job_type, f"{job_key}:input", published_at, published_at),
            )
        connection.execute(
            """
            INSERT INTO m1_generation_manifests (
                generation_key, producer_job_key, input_digest, artifact_key,
                artifact_digest, record_count, published_at
            ) VALUES
                (%s, 'job:qualification-structure', %s, %s, %s, 3, %s),
                (%s, 'job:qualification-quote', %s, %s, %s, 5, %s)
            ON CONFLICT (generation_key) DO NOTHING
            """,
            (
                structure_key,
                "a" * 64,
                "structure.ndjson",
                "b" * 64,
                published_at,
                quote_key,
                "c" * 64,
                "quote.ndjson",
                "d" * 64,
                published_at,
            ),
        )
        connection.execute(
            """
            INSERT INTO m1_publication_pointers (
                pointer_key, generation_key, expected_generation_key, lease_epoch, published_at
            ) VALUES
                ('structure:current', %s, NULL, 1, %s),
                ('quote:current', %s, NULL, 1, %s)
            ON CONFLICT (pointer_key) DO UPDATE
            SET generation_key = EXCLUDED.generation_key,
                lease_epoch = EXCLUDED.lease_epoch,
                published_at = EXCLUDED.published_at
            """,
            (structure_key, published_at, quote_key, published_at),
        )
        connection.execute(
            """
            INSERT INTO m1_opportunity_projections (
                generation_key, structure_generation_key, projection_digest,
                record_count, certified_at
            ) VALUES (%s, %s, %s, 7, %s)
            ON CONFLICT (generation_key) DO NOTHING
            """,
            (quote_key, structure_key, "e" * 64, published_at),
        )
        connection.execute(
            """
            INSERT INTO m1_opportunity_publication_pointers (
                pointer_key, generation_key, published_at
            ) VALUES ('opportunity:current', %s, %s)
            ON CONFLICT (pointer_key) DO UPDATE
            SET generation_key = EXCLUDED.generation_key,
                published_at = EXCLUDED.published_at
            """,
            (quote_key, published_at),
        )


def _direct_insert_qualification_certificate(
    control_plane: PostgresControlPlane,
    payload: dict[str, object],
) -> None:
    with control_plane._connection_factory() as connection, connection.cursor() as cursor:
        _execute_direct_certificate_insert(cursor, payload)


def _execute_direct_certificate_insert(
    cursor: psycopg.Cursor[object],
    payload: dict[str, object],
    *,
    certificate_id: str | None = None,
    identity_key: str | None = None,
) -> None:
    digest = certificate_digest(payload)
    identity = cast(dict[str, object], payload["identity"])
    bounds = cast(dict[str, object], payload["bounds"])
    cursor.execute(
        """
        INSERT INTO m1_qualification_certificates (
            certificate_id, epoch_id, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, qualified_at, payload,
            canonical_payload, payload_sha256, certificate_digest, evidence_digest
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            certificate_id or f"qualification-certificate:{digest}",
            cast(str, identity["epoch_id"]),
            identity_key or _certificate_identity_key_for_test(payload),
            cast(str, identity["policy_version"]),
            cast(str, identity["release_id"]),
            cast(str, identity["config_id"]),
            Jsonb(cast(list[object], identity["role_identity"])),
            cast(str, bounds["started_at"]),
            cast(str, bounds["qualified_at"]),
            Jsonb(payload),
            canonical_certificate_bytes(payload).decode("utf-8"),
            digest,
            digest,
            cast(str, payload["evidence_digest"]),
        ),
    )


def _execute_certificate_function(
    cursor: psycopg.Cursor[object],
    payload: dict[str, object],
) -> tuple[str, str, str]:
    digest = certificate_digest(payload)
    identity = cast(dict[str, object], payload["identity"])
    bounds = cast(dict[str, object], payload["bounds"])
    cursor.execute(
        """
        SELECT certificate_id, identity_key, certificate_digest
        FROM m1_insert_qualification_certificate(
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """,
        (
            cast(str, identity["epoch_id"]),
            cast(str, identity["policy_version"]),
            cast(str, identity["release_id"]),
            cast(str, identity["config_id"]),
            Jsonb(cast(list[object], identity["role_identity"])),
            cast(str, bounds["started_at"]),
            cast(str, bounds["qualified_at"]),
            Jsonb(payload),
            canonical_certificate_bytes(payload).decode("utf-8"),
            digest,
            digest,
            cast(str, payload["evidence_digest"]),
        ),
    )
    row = cursor.fetchone()
    assert row is not None
    return cast(tuple[str, str, str], row)


def _certificate_identity_key_for_test(payload: dict[str, object]) -> str:
    return sha256(
        json.dumps(
            {
                "bounds": payload["bounds"],
                "identity": payload["identity"],
            },
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    ).hexdigest()
