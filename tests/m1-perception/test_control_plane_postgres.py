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

from polyarb.control_plane.models import JobState, QuoteBatchLeg, QuoteBatchSpec
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
            ["uv", "run", "alembic", "upgrade", "010"],
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
            "m1_structure_source_page_receipts",
            "m1_structure_source_page_inputs",
            "m1_structure_source_windows",
            "m1_alert_deliveries",
            "m1_alert_outbox",
            "m1_incident_events",
            "m1_incidents",
            "m1_publication_pointers",
            "m1_generation_manifests",
            "m1_structure_range_receipts",
            "m1_structure_range_inputs",
            "m1_structure_generation_inputs",
            "m1_quote_batch_receipts",
            "m1_quote_batch_inputs",
            "m1_checkpoint_receipts",
            "m1_job_attempts",
            "m1_jobs",
        ):
            connection.execute(f"TRUNCATE {table} CASCADE")
    yield PostgresControlPlane(connect)


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
    admitted = control_plane.admit_structure_source_window(
        window_key="source-window:one", now=now
    )
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
    assert control_plane.structure_source_page_spec(
        "source-window:stale:fetch:events:1"
    ).requested_cursor == "replacement-cursor"


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


def test_deployment_preflight_requires_named_database_and_all_010_tables(
    control_plane: PostgresControlPlane,
) -> None:
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        database_name = connection.execute("SELECT current_database()").fetchone()
    assert database_name is not None
    result = control_plane.deployment_preflight(expected_database=str(database_name[0]))
    assert result["database_name"] == database_name[0]
    assert result["revision_010_tables"] == 17
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
    assert control_plane.record_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/c/rows.ndjson",
        artifact_digest="c" * 64,
        record_count=3,
        now=now + timedelta(seconds=1),
    ) == receipt
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
    assert control_plane.certify_structure_generation(
        certifier,
        generation_key=specs[0].generation_key,
        artifact_key=f"structure-manifests/{expected_manifest}/manifest.ndjson",
        artifact_digest=expected_manifest,
        now=now,
    ) == expected_manifest


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
        worker_id="worker-a", job_types=("quote-batch",), lease_seconds=30, now=now
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
    assert control_plane.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now + timedelta(seconds=1),
    ) == first
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
    assert control_plane.record_quote_batch(
        replacement,
        token_range_digest=batch.token_range_digest,
        quote_digest="c" * 64,
        artifact_key="quote-batches/c/batch.ndjson",
        artifact_digest="c" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now + timedelta(seconds=2),
    ) == receipt
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
        resumed = control_plane.claim_job(
            worker_id=f"quote-worker-{ordinal}",
            job_types=("quote-batch",),
            lease_seconds=30,
            now=batch_now + timedelta(milliseconds=1),
        )
        assert resumed is not None
        control_plane.finish(
            resumed,
            state=JobState.SUCCEEDED,
            now=batch_now + timedelta(milliseconds=2),
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
    finally:
        connection.close()


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

    assert snapshot["job_counts"] == {"leased": 1, "runnable": 1}
    assert snapshot["oldest_runnable_age_seconds"] == 0.0
    assert snapshot["expired_leases"] == 1
    assert snapshot["quote"] == {
        "batch_job_states": {"runnable": 1},
        "certifier_job_states": {},
        "oldest_retryable_batch_age_seconds": None,
        "current_pointer": None,
    }
    assert snapshot["structure"] == {
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
