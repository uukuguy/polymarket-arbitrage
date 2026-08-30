"""Cross-job runtime coverage contracts for M1 transactional workers."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import psycopg
import pytest
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from polyarb.control_plane.models import JobLease, QuoteBatchLeg
from polyarb.control_plane.postgres import PostgresControlPlane, StaleLeaseError
from polyarb.control_plane.production_commissioning_disposable import (
    HeartbeatOutageCommissioningAdapter,
    NormalizationPayloadCorruptCommissioningAdapter,
    ProgressStallCommissioningAdapter,
    QuoteAdmissionMissingShardCommissioningAdapter,
    QuoteBatchIncompleteCommissioningAdapter,
    RetryBudgetCommissioningAdapter,
    SourceReceiptGapCommissioningAdapter,
    StaleOwnerCommissioningAdapter,
    WorkerExitCommissioningAdapter,
    complete_normal_turn,
    prepare_normal_turn,
)
from polyarb.control_plane.production_commissioning_runner import (
    AttackIdentity,
    run_disposable_attack,
)
from polyarb.control_plane.recovery_store import _runtime_deadline_profile
from polyarb.control_plane.runtime_contract import RUNTIME_STAGE_REGISTRY, AttemptRuntime
from polyarb.control_plane.runtime_deadlines import (
    RUNTIME_JOB_ORDER,
    RUNTIME_JOB_SUCCESSORS,
    runtime_deadline_profile,
    runtime_policy,
    runtime_retry_policy,
)
from polyarb.control_plane.runtime_models import (
    RuntimeDeadlineProfile,
    RuntimeEventKind,
    RuntimeProgress,
)
from polyarb.control_plane.structure_artifact import (
    StructureBundleArtifact,
    StructureBundleIdentity,
    canonical_structure_manifest_bytes,
)

REQUIRED_JOB_TYPES = (
    "structure-fetch",
    "structure-materialize",
    "structure-normalize",
    "structure-certify",
    "quote-admit",
    "quote-batch",
    "quote-certify",
    "opportunity-certify",
)
SECRET_LIKE_DETAIL_KEY_PARTS = (
    "api_key",
    "apikey",
    "authorization",
    "credential",
    "password",
    "secret",
    "token",
)
PROFILE = RuntimeDeadlineProfile(
    policy_version="runtime-v1",
    lease_seconds=120,
    heartbeat_seconds=30,
    progress_seconds=120,
    attempt_seconds=1200,
)
NOW = datetime(2030, 1, 1, tzinfo=UTC)


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            ).returncode
            == 0
        )
    except OSError:
        return False


def _require_docker_available() -> None:
    if not _docker_available():
        pytest.fail(
            "Docker daemon unavailable; runtime coverage requires Docker/Testcontainers "
            "Postgres. Start Docker and rerun the transactional runtime gate.",
            pytrace=False,
        )


@pytest.fixture(scope="module")
def postgres_dsn() -> Iterator[str]:
    _require_docker_available()
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
            check=False,
        )
        assert result.returncode == 0, result.stderr
        yield dsn


@pytest.fixture()
def control_plane(postgres_dsn: str) -> Iterator[PostgresControlPlane]:
    def connect() -> psycopg.Connection[Any]:
        return psycopg.connect(postgres_dsn)

    with connect() as connection:
        for table in (
            "m1_soak_observations",
            "m1_soak_runs",
            "m1_cloud_usage_observations",
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


class _CapturingStore:
    def __init__(self) -> None:
        self.progress: list[dict[str, object]] = []

    def record_runtime_progress(
        self,
        lease: JobLease,
        *,
        progress: RuntimeProgress,
        now: datetime,
        detail: dict[str, object] | None = None,
    ) -> object:
        self.progress.append({"lease": lease, "progress": progress, "now": now, "detail": detail})
        return object()

    def heartbeat_runtime_attempt(
        self,
        lease: JobLease,
        *,
        now: datetime,  # noqa: ARG002
        lease_seconds: int,  # noqa: ARG002
    ) -> JobLease:
        return lease


def test_runtime_registry_has_exact_eight_job_types_with_meaningful_stage_names() -> None:
    assert tuple(RUNTIME_STAGE_REGISTRY) == REQUIRED_JOB_TYPES
    for job_type, stages in RUNTIME_STAGE_REGISTRY.items():
        assert stages
        assert len(stages) == len(set(stages))
        assert all(
            stage and stage != "started" and not stage.startswith("job.") for stage in stages
        )


def test_structure_certifier_gets_bounded_long_attempt_without_weakening_liveness() -> None:
    certifier = runtime_deadline_profile("structure-certify", 30)
    certifier_with_longer_lease = runtime_deadline_profile("structure-certify", 120)
    normalizer = runtime_deadline_profile("structure-normalize", 30)

    assert (certifier.heartbeat_seconds, certifier.progress_seconds) == (10, 30)
    assert certifier.attempt_seconds == 3_600
    assert certifier_with_longer_lease.attempt_seconds == 3_600
    assert normalizer.attempt_seconds == 300


def test_runtime_policy_is_closed_and_orders_every_timeout() -> None:
    assert tuple(RUNTIME_STAGE_REGISTRY) == REQUIRED_JOB_TYPES
    for job_type in REQUIRED_JOB_TYPES:
        policy = runtime_policy(job_type, 120)
        assert policy.job_type == job_type
        assert policy.policy_version == "runtime-v2"
        assert 3 * policy.deadlines.heartbeat_seconds <= policy.deadlines.lease_seconds
        assert policy.io_timeout_seconds < policy.deadlines.progress_seconds
        assert policy.io_timeout_seconds < policy.terminal_grace_seconds
        assert policy.provider_attempts == 1
        assert 0 < policy.provider_timeout_seconds < policy.io_timeout_seconds
        assert policy.deadlines.progress_seconds <= policy.deadlines.attempt_seconds
        assert policy.terminal_grace_seconds > 0
        assert policy.retry_budget > 0
        assert policy.retry_backoff_seconds(1) == 15
        assert policy.retry_backoff_seconds(2) == 30
        assert policy.retry_backoff_seconds(99) == 300
        retry_policy = runtime_retry_policy(job_type)
        assert retry_policy.retry_budget == policy.retry_budget
        assert retry_policy.retry_backoff_seconds(3) == 60
        assert policy.checkpoint_interval > 0

    with pytest.raises(ValueError, match="unknown runtime job type"):
        runtime_policy("quote-adimt", 120)


def test_runtime_job_dag_is_closed_acyclic_and_topologically_ordered() -> None:
    assert set(RUNTIME_JOB_SUCCESSORS) == set(RUNTIME_STAGE_REGISTRY)
    assert set(RUNTIME_JOB_ORDER) == set(RUNTIME_STAGE_REGISTRY)
    position = {job_type: index for index, job_type in enumerate(RUNTIME_JOB_ORDER)}
    for job_type, successors in RUNTIME_JOB_SUCCESSORS.items():
        assert all(position[job_type] < position[successor] for successor in successors)


def test_worker_modules_do_not_define_private_runtime_profiles() -> None:
    root = Path(__file__).parents[2] / "src" / "polyarb" / "control_plane"
    offenders = []
    for path in root.glob("*.py"):
        if path.name == "runtime_deadlines.py":
            continue
        if "def _runtime_profile(" in path.read_text():
            offenders.append(path.name)
    assert offenders == []


def test_retry_circuit_budget_and_backoff_have_one_runtime_policy_authority() -> None:
    source = (Path(__file__).parents[2] / "src/polyarb/control_plane/postgres.py").read_text()

    policy_lookup_count = source.count("retry_policy = runtime_retry_policy(")
    backoff_use_count = source.count("retry_policy.retry_backoff_seconds(")
    assert policy_lookup_count >= 3
    assert backoff_use_count == policy_lookup_count
    assert 'circuit_state = "open" if failures >= 3' not in source
    assert "now if failures == 3" not in source
    assert "min(15 * (2 ** (failures - 1)), 300)" not in source


def test_database_deadlines_have_one_registry_and_no_private_copies() -> None:
    from polyarb.control_plane.db_deadlines import (
        CONTROL_PLANE_DB_POLICY,
        CONTROL_PLANE_HEALTH_DB_POLICY,
        MIGRATION_DB_POLICY,
        RECOVERY_DB_POLICY,
    )

    assert CONTROL_PLANE_DB_POLICY.lock_timeout_ms < CONTROL_PLANE_DB_POLICY.statement_timeout_ms
    assert CONTROL_PLANE_DB_POLICY.request_timeout_seconds > (
        CONTROL_PLANE_DB_POLICY.connect_timeout_seconds
        + 2 * CONTROL_PLANE_DB_POLICY.statement_timeout_ms / 1_000
    )
    assert CONTROL_PLANE_HEALTH_DB_POLICY.request_timeout_seconds < 5
    assert (
        CONTROL_PLANE_HEALTH_DB_POLICY.statement_timeout_ms
        < CONTROL_PLANE_DB_POLICY.statement_timeout_ms
    )
    assert RECOVERY_DB_POLICY.lock_timeout_ms <= RECOVERY_DB_POLICY.statement_timeout_ms
    assert RECOVERY_DB_POLICY.statement_timeout_ms < CONTROL_PLANE_DB_POLICY.statement_timeout_ms
    assert MIGRATION_DB_POLICY.lock_timeout_ms < MIGRATION_DB_POLICY.statement_timeout_ms
    assert MIGRATION_DB_POLICY.statement_timeout_ms >= CONTROL_PLANE_DB_POLICY.statement_timeout_ms

    alembic_env = (Path(__file__).parents[2] / "alembic" / "env.py").read_text()
    assert "MIGRATION_DB_POLICY" in alembic_env
    assert '"options": MIGRATION_DB_POLICY.connection_options' in alembic_env
    assert '"connect_timeout": MIGRATION_DB_POLICY.connect_timeout_seconds' in alembic_env
    assert "statement_timeout=30000" not in alembic_env

    root = Path(__file__).parents[2] / "src" / "polyarb" / "control_plane"
    formal_consumers = (
        "db_role_contract.py",
        "postgres.py",
        "qualification_store.py",
        "qualification_service.py",
        "recovery_store.py",
        "runtime_event_writer.py",
        "api.py",
    )
    offenders: list[str] = []
    for name in formal_consumers:
        source = (root / name).read_text()
        if any(
            private_copy in source
            for private_copy in (
                "_STATEMENT_TIMEOUT_MS =",
                "_LOCK_TIMEOUT_MS =",
                "_FENCED_MAX_STATEMENT_TIMEOUT_MS =",
                "_FENCED_MAX_LOCK_TIMEOUT_MS =",
                "_RECOVERY_STATEMENT_TIMEOUT_MS =",
                "_RECOVERY_LOCK_TIMEOUT_MS =",
                "'5000ms'",
                "'1000ms'",
            )
        ):
            offenders.append(name)
    assert offenders == []


def test_recovery_uses_persisted_policy_instead_of_renewed_deadline_arithmetic() -> None:
    row = {
        "policy_version": "runtime-v2",
        "profile_lease_seconds": 30,
        "profile_heartbeat_seconds": 10,
        "profile_progress_seconds": 30,
        "profile_attempt_seconds": 3600,
        "started_at": NOW,
        "last_heartbeat_at": NOW + timedelta(seconds=1200),
        "last_progress_at": NOW + timedelta(seconds=1200),
        "lease_deadline_at": NOW + timedelta(seconds=1230),
        "heartbeat_deadline_at": NOW + timedelta(seconds=1210),
        "progress_deadline_at": NOW + timedelta(seconds=1230),
        "attempt_deadline_at": NOW + timedelta(seconds=3600),
    }

    assert _runtime_deadline_profile(row) == RuntimeDeadlineProfile(
        policy_version="runtime-v2",
        lease_seconds=30,
        heartbeat_seconds=10,
        progress_seconds=30,
        attempt_seconds=3600,
    )


def test_runtime_coverage_gate_uses_real_terminal_boundaries_and_fails_closed() -> None:
    source = Path(__file__).read_text()

    assert "_append_job_" + "succeeded_cursor" not in source
    assert "pytest." + "skip(" not in source
    assert "UPDATE m1_" + "jobs" not in source
    assert "UPDATE m1_" + "job_attempts" not in source


@pytest.mark.parametrize("secret_key", SECRET_LIKE_DETAIL_KEY_PARTS)
def test_runtime_reporter_rejects_secret_like_detail_keys_before_persistence(
    secret_key: str,
) -> None:
    lease = JobLease(
        job_key=f"runtime-secret:{secret_key}",
        job_type="quote-batch",
        input_identity=f"runtime-secret:{secret_key}",
        lease_owner="runtime-worker",
        lease_epoch=1,
        lease_expires_at=NOW + timedelta(seconds=PROFILE.lease_seconds),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )
    store = _CapturingStore()
    runtime = AttemptRuntime(store=store, lease=lease, profile=PROFILE, clock=lambda: NOW)

    with pytest.raises(ValueError, match="secret-like runtime detail key"):
        runtime.progress(
            stage="read-input",
            current=1,
            total=1,
            detail={secret_key: "redacted"},
        )

    assert store.progress == []


def test_runtime_reporter_rejects_unbounded_detail_before_persistence() -> None:
    lease = JobLease(
        job_key="runtime-unbounded-detail",
        job_type="quote-batch",
        input_identity="runtime-unbounded-detail",
        lease_owner="runtime-worker",
        lease_epoch=1,
        lease_expires_at=NOW + timedelta(seconds=PROFILE.lease_seconds),
        checkpoint_cursor=None,
        checkpoint_digest=None,
    )
    store = _CapturingStore()
    runtime = AttemptRuntime(store=store, lease=lease, profile=PROFILE, clock=lambda: NOW)

    with pytest.raises(ValueError, match="runtime event detail is not bounded"):
        runtime.progress(
            stage="read-input",
            current=1,
            total=1,
            detail={f"component_{index}": "control-plane" for index in range(21)},
        )

    assert store.progress == []


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_transactional_job_type_persists_one_start_progress_chain_and_terminal_event(
    control_plane: PostgresControlPlane,
    job_type: str,
) -> None:
    lease = _claim_progress_and_complete(control_plane, job_type=job_type)

    events = _runtime_event_rows(control_plane, job_keys=(lease.job_key,))
    stages = RUNTIME_STAGE_REGISTRY[job_type]
    kinds = [row["kind"] for row in events]
    assert kinds.count(RuntimeEventKind.STARTED.value) == 1
    assert kinds.count(RuntimeEventKind.SUCCEEDED.value) == 1
    assert kinds.count(RuntimeEventKind.STAGE_CHANGED.value) == len(stages)
    assert len(events) == len(stages) + 2
    assert [row["event_sequence"] for row in events] == list(range(1, len(events) + 1))
    assert events[0]["kind"] == RuntimeEventKind.STARTED.value
    assert events[0]["stage"] == "started"

    progress_events = [row for row in events if row["kind"] == RuntimeEventKind.STAGE_CHANGED.value]
    assert [row["stage"] for row in progress_events] == list(stages)
    assert [row["progress_sequence"] for row in progress_events] == list(range(1, len(stages) + 1))
    assert all(row["progress_current"] >= 1 for row in progress_events)
    assert all(row["progress_total"] >= row["progress_current"] for row in progress_events)

    terminal = events[-1]
    assert terminal["kind"] == RuntimeEventKind.SUCCEEDED.value
    assert terminal["stage"] == stages[-1]
    assert terminal["progress_sequence"] == len(stages)
    assert terminal["progress_current"] == len(stages)
    assert terminal["progress_total"] == len(stages)

    for event in events:
        detail = cast(dict[str, object], event["detail"])
        assert not _secret_like_detail_keys(detail)


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_disposable_commissioning_normal_turn_references_real_durable_facts(
    control_plane: PostgresControlPlane,
    job_type: str,
) -> None:
    proof = complete_normal_turn(
        control_plane,
        node_id=job_type,
        experiment_id=f"normal-turn:{job_type}",
        now=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 1),
    )

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempt = connection.execute(
            "SELECT state FROM m1_job_attempts WHERE attempt_id = %s",
            (proof["attempt_id"],),
        ).fetchone()
        event = connection.execute(
            "SELECT kind FROM m1_job_runtime_events WHERE event_id = %s",
            (proof["success_fact_id"],),
        ).fetchone()

    assert attempt == ("succeeded",)
    assert event == (RuntimeEventKind.SUCCEEDED.value,)
    assert proof["terminal_fact_id"] == f"attempt:{proof['attempt_id']}"
    assert proof["postcondition_fact_id"].startswith("postgres:")
    assert proof["succeeded_at"].endswith("+00:00")


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_disposable_commissioning_stale_owner_is_fenced_and_replacement_completes(
    control_plane: PostgresControlPlane,
    job_type: str,
) -> None:
    started_at = NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 1)
    prepared = prepare_normal_turn(
        control_plane,
        node_id=job_type,
        experiment_id=f"stale-owner:{job_type}",
        now=started_at,
    )
    replacement = control_plane.claim_job(
        worker_id=f"commissioning:replacement:{job_type}",
        job_types=(job_type,),
        lease_seconds=120,
        now=prepared.lease.lease_expires_at + timedelta(microseconds=1),
    )

    assert replacement is not None
    assert replacement.job_key == prepared.lease.job_key
    assert replacement.lease_epoch == prepared.lease.lease_epoch + 1
    with pytest.raises(StaleLeaseError):
        prepared.complete(
            lease=prepared.lease,
            now=replacement.lease_expires_at - timedelta(seconds=2),
        )

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        stale_attempts = connection.execute(
            """
            SELECT count(*) FROM m1_job_attempts
            WHERE job_key = %s AND lease_epoch = %s AND state = 'succeeded'
            """,
            (prepared.lease.job_key, prepared.lease.lease_epoch),
        ).fetchone()
        stale_events = connection.execute(
            """
            SELECT count(*) FROM m1_job_runtime_events
            WHERE job_key = %s AND lease_epoch = %s AND kind = %s
            """,
            (
                prepared.lease.job_key,
                prepared.lease.lease_epoch,
                RuntimeEventKind.SUCCEEDED.value,
            ),
        ).fetchone()

    assert stale_attempts == (0,)
    assert stale_events == (0,)

    proof = prepared.complete(
        lease=replacement,
        now=replacement.lease_expires_at - timedelta(seconds=1),
    )

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        replacement_attempt = connection.execute(
            "SELECT lease_epoch FROM m1_job_attempts WHERE attempt_id = %s",
            (proof["attempt_id"],),
        ).fetchone()

    assert proof["attempt_id"]
    assert replacement_attempt == (replacement.lease_epoch,)
    assert proof["terminal_fact_id"] == f"attempt:{proof['attempt_id']}"
    assert proof["postcondition_fact_id"].startswith("postgres:")


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_stale_owner_adapter_writes_real_cleanup_safe_attack_proof(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
    job_type: str,
) -> None:
    identity = AttackIdentity(
        experiment_id=f"commission:{job_type}:stale-owner-terminal-write",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id=job_type,
        attack_id="stale-owner-terminal-write",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=StaleOwnerCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 1),
        ),
        evidence_dir=tmp_path / job_type,
    )

    assert proof["detector_fact_id"].startswith("attempt:")
    assert proof["recovery_action_id"].startswith("attempt:")
    assert proof["recovery_fact_id"].startswith("event:")
    assert proof["postcondition_fact_id"].startswith("postgres:")
    assert proof["cleanup_verified"] is True
    assert sorted(path.name for path in (tmp_path / job_type).iterdir()) == [
        "00-intent.json",
        "10-preflight.json",
        "20-injected.json",
        "30-detected.json",
        "40-recovery-started.json",
        "50-cleanup.json",
        "60-recovered.json",
        "70-verified.json",
        "proof.json",
    ]


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_progress_stall_adapter_executes_real_policy_recovery_and_successor_turn(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
    job_type: str,
) -> None:
    identity = AttackIdentity(
        experiment_id=f"commission:{job_type}:progress-stall",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id=job_type,
        attack_id="progress-stall",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=ProgressStallCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 20),
        ),
        evidence_dir=tmp_path / job_type,
    )

    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("action:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith("postgres:")
    assert proof["cleanup_verified"] is True

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempts = connection.execute(
            """
            SELECT lease_epoch, state, error_class
            FROM m1_job_attempts
            WHERE job_key = (
                SELECT target_id FROM m1_recovery_actions
                WHERE action_id = %s
            )
            ORDER BY lease_epoch
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchall()
        action = connection.execute(
            """
            SELECT action_type, state, result_code
            FROM m1_recovery_actions WHERE action_id = %s
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchone()
        incident = connection.execute(
            """
            SELECT severity, state, resolved_at IS NOT NULL
            FROM m1_incidents WHERE incident_key = %s
            """,
            (str(proof["detector_fact_id"]).removeprefix("incident:"),),
        ).fetchone()
        recovery_started = connection.execute(
            """
            SELECT count(*) FROM m1_job_runtime_events
            WHERE kind = 'job.recovery-started'
              AND detail->>'reason_code' = 'job.progress-stalled'
            """
        ).fetchone()
        incident_recovered = connection.execute(
            """
            SELECT count(*) FROM m1_incident_events
            WHERE incident_key = %s AND kind = 'recovered'
            """,
            (str(proof["detector_fact_id"]).removeprefix("incident:"),),
        ).fetchone()

    assert attempts == [
        (1, "retryable", "RecoveryProgressStalled"),
        (2, "succeeded", None),
    ]
    assert action == ("cancel-job", "completed", "succeeded")
    assert incident == ("warning", "resolved", True)
    assert recovery_started == (1,)
    assert incident_recovered == (1,)


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_heartbeat_outage_adapter_renews_and_completes_the_same_attempt(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
    job_type: str,
) -> None:
    identity = AttackIdentity(
        experiment_id=f"commission:{job_type}:heartbeat-outage",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id=job_type,
        attack_id="heartbeat-outage",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=HeartbeatOutageCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 30),
        ),
        evidence_dir=tmp_path / job_type,
    )

    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("action:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith("postgres:")
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempts = connection.execute(
            """
            SELECT lease_epoch, state, error_class
            FROM m1_job_attempts
            WHERE job_key = (
                SELECT target_id FROM m1_recovery_actions WHERE action_id = %s
            )
            ORDER BY lease_epoch
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchall()
        recovery = connection.execute(
            """
            SELECT a.action_type, a.state, a.result_code,
                   i.severity, i.state, i.resolved_at IS NOT NULL,
                   a.finished_at IS NOT NULL,
                   r.last_heartbeat_at = a.started_at,
                   r.lease_deadline_at = a.started_at
                       + make_interval(secs => r.profile_lease_seconds)
            FROM m1_recovery_actions AS a
            JOIN m1_incidents AS i ON i.incident_key = a.incident_key
            JOIN m1_job_runtime_state AS r ON r.job_key = a.target_id
            WHERE a.action_id = %s
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchone()
        recovery_started = connection.execute(
            """
            SELECT count(*) FROM m1_job_runtime_events
            WHERE kind = 'job.recovery-started'
              AND detail->>'reason_code' = 'job.lease-at-risk'
            """
        ).fetchone()

    assert attempts == [(1, "succeeded", None)]
    assert recovery == (
        "heartbeat-job",
        "completed",
        "succeeded",
        "warning",
        "resolved",
        True,
        True,
        True,
        True,
    )
    assert recovery_started == (1,)


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_worker_exit_adapter_reclaims_only_after_expiry_and_fences_old_owner(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
    job_type: str,
) -> None:
    identity = AttackIdentity(
        experiment_id=f"commission:{job_type}:worker-exit",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id=job_type,
        attack_id="worker-exit",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=WorkerExitCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 50),
        ),
        evidence_dir=tmp_path / job_type,
    )

    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("action:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith("postgres:")
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempts = connection.execute(
            """
            SELECT lease_epoch, state, error_class
            FROM m1_job_attempts
            WHERE job_key = (
                SELECT target_id FROM m1_recovery_actions WHERE action_id = %s
            )
            ORDER BY lease_epoch
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchall()
        action = connection.execute(
            """
            SELECT action_type, state, result_code
            FROM m1_recovery_actions WHERE action_id = %s
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchone()
        incident = connection.execute(
            """
            SELECT i.severity, i.state, i.resolved_at IS NOT NULL,
                   e.detail->>'reason_code',
                   (e.detail->>'qualification_breaking')::boolean
            FROM m1_incidents AS i
            JOIN m1_incident_events AS e ON e.incident_key = i.incident_key
            WHERE i.incident_key = %s AND e.kind = 'recovery-started'
            """,
            (str(proof["detector_fact_id"]).removeprefix("incident:"),),
        ).fetchone()
        recovery_started = connection.execute(
            """
            SELECT count(*) FROM m1_job_runtime_events
            WHERE kind = 'job.recovery-started'
              AND detail->>'reason_code' = 'job.heartbeat-missing'
            """
        ).fetchone()

    assert attempts == [
        (1, "retryable", "RecoveryLeaseExpired"),
        (2, "succeeded", None),
    ]
    assert action == (
        "reclaim-job",
        "completed",
        "succeeded",
    )
    assert incident == (
        "critical",
        "resolved",
        True,
        "job.heartbeat-missing",
        True,
    )
    assert recovery_started == (1,)


@pytest.mark.parametrize("job_type", REQUIRED_JOB_TYPES)
def test_retry_budget_adapter_opens_one_incident_and_releases_one_successful_probe(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
    job_type: str,
) -> None:
    identity = AttackIdentity(
        experiment_id=f"commission:{job_type}:retry-budget-exhaustion",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id=job_type,
        attack_id="retry-budget-exhaustion",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=RetryBudgetCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 40),
        ),
        evidence_dir=tmp_path / job_type,
    )

    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("action:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith("postgres:")
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        attempts = connection.execute(
            """
            SELECT lease_epoch, state, error_class
            FROM m1_job_attempts
            WHERE job_key = (
                SELECT target_id FROM m1_recovery_actions WHERE action_id = %s
            )
            ORDER BY lease_epoch
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchall()
        circuit = connection.execute(
            """
            SELECT consecutive_failures, state, next_probe_at, failure_fingerprint
            FROM m1_job_circuits
            WHERE job_key = (
                SELECT target_id FROM m1_recovery_actions WHERE action_id = %s
            )
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchone()
        retry_incidents = connection.execute(
            """
            SELECT count(*), min(state), bool_and(resolved_at IS NOT NULL)
            FROM m1_incidents WHERE dedupe_key LIKE 'job-retry:%'
            """
        ).fetchone()
        retry_events = connection.execute(
            """
            SELECT kind FROM m1_incident_events
            WHERE incident_key = %s ORDER BY occurred_at, incident_event_id
            """,
            (str(proof["detector_fact_id"]).removeprefix("incident:"),),
        ).fetchall()
        probe_action = connection.execute(
            """
            SELECT action_type, state, result_code FROM m1_recovery_actions
            WHERE action_id = %s
            """,
            (str(proof["recovery_action_id"]).removeprefix("action:"),),
        ).fetchone()

    assert attempts == [
        (1, "retryable", "CommissioningValidationFault"),
        (2, "retryable", "CommissioningValidationFault"),
        (3, "retryable", "CommissioningValidationFault"),
        (4, "succeeded", None),
    ]
    assert circuit is not None
    assert circuit[:3] == (0, "closed", None)
    assert circuit[3] is None
    assert retry_incidents == (1, "resolved", True)
    assert [row[0] for row in retry_events] == [
        "attempt-failed",
        "attempt-failed",
        "circuit-opened",
        "recovered",
    ]
    assert probe_action == ("probe-circuit", "completed", "succeeded")


def test_source_receipt_gap_adapter_releases_one_complete_materializer_turn(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
) -> None:
    identity = AttackIdentity(
        experiment_id="commission:structure-materialize:source-receipt-gap",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id="structure-materialize",
        attack_id="source-receipt-gap",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=SourceReceiptGapCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=60),
        ),
        evidence_dir=tmp_path / "structure-materialize",
    )

    assert str(proof["detector_fact_id"]).startswith("barrier:")
    assert str(proof["recovery_action_id"]).startswith("attempt:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith(
        "postgres:m1_structure_source_window_bundles:"
    )
    assert proof["cleanup_verified"] is True

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        shape = connection.execute(
            """
            SELECT source_window.state,
                   count(DISTINCT input.job_key),
                   count(DISTINCT receipt.job_key),
                   count(DISTINCT bundle.window_key),
                   count(DISTINCT range_input.job_key)
            FROM m1_structure_source_windows AS source_window
            JOIN m1_structure_source_page_inputs AS input
              ON input.window_key = source_window.window_key
            LEFT JOIN m1_structure_source_page_receipts AS receipt
              ON receipt.job_key = input.job_key
            LEFT JOIN m1_structure_source_window_bundles AS bundle
              ON bundle.window_key = source_window.window_key
            LEFT JOIN m1_structure_range_inputs AS range_input
              ON range_input.bundle_digest = bundle.bundle_digest
            WHERE source_window.window_key = %s
            GROUP BY source_window.state
            """,
            (identity.experiment_id,),
        ).fetchone()
        materializer = connection.execute(
            """
            SELECT job.state, count(attempt.attempt_id), min(attempt.state)
            FROM m1_jobs AS job
            JOIN m1_job_attempts AS attempt ON attempt.job_key = job.job_key
            WHERE job.job_key = %s
            GROUP BY job.state
            """,
            (f"{identity.experiment_id}:materialize",),
        ).fetchone()
        incidents = connection.execute("SELECT count(*) FROM m1_incidents").fetchone()
        recovery_actions = connection.execute(
            "SELECT count(*) FROM m1_recovery_actions"
        ).fetchone()

    assert shape == ("complete", 3, 3, 1, 1)
    assert materializer == ("succeeded", 1, "succeeded")
    assert incidents == (0,)
    assert recovery_actions == (0,)


def test_quote_batch_incomplete_adapter_blocks_partial_pointer_then_certifies(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
) -> None:
    identity = AttackIdentity(
        experiment_id="commission:quote-certify:quote-batch-incomplete",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id="quote-certify",
        attack_id="quote-batch-incomplete",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=QuoteBatchIncompleteCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=61),
        ),
        evidence_dir=tmp_path / "quote-certify",
    )

    assert proof["qualification_impact"] == "block"
    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("incident-event:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith(
        "postgres:m1_publication_pointers:quote:"
    )
    assert proof["cleanup_verified"] is True

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        shape = connection.execute(
            """
            SELECT count(DISTINCT input.job_key),
                   count(DISTINCT receipt.job_key),
                   count(DISTINCT batch.job_key) FILTER (WHERE batch.state = 'succeeded'),
                   count(DISTINCT certifier.job_key)
                       FILTER (WHERE certifier.state = 'succeeded'),
                   count(DISTINCT manifest.generation_key),
                   count(DISTINCT pointer.pointer_key),
                   count(DISTINCT opportunity.job_key)
            FROM m1_quote_batch_inputs AS input
            LEFT JOIN m1_quote_batch_receipts AS receipt
              ON receipt.job_key = input.job_key
            LEFT JOIN m1_jobs AS batch ON batch.job_key = input.job_key
            LEFT JOIN m1_jobs AS certifier
              ON certifier.job_key = 'quote:' || input.structure_receipt_digest || ':certify'
            LEFT JOIN m1_generation_manifests AS manifest
              ON manifest.generation_key = 'quote:' || input.structure_receipt_digest
            LEFT JOIN m1_publication_pointers AS pointer
              ON pointer.pointer_key = 'quote:current'
             AND pointer.generation_key = 'quote:' || input.structure_receipt_digest
            LEFT JOIN m1_jobs AS opportunity
              ON opportunity.job_key =
                 'quote:' || input.structure_receipt_digest || ':opportunity-certify'
            """
        ).fetchone()
        incident = connection.execute(
            """
            SELECT incident.state, circuit.consecutive_failures, circuit.state,
                   array_agg(event.kind ORDER BY event.occurred_at, event.kind)
            FROM m1_incidents AS incident
            JOIN m1_job_circuits AS circuit
              ON incident.dedupe_key = 'job-retry:' || circuit.job_key
            JOIN m1_incident_events AS event
              ON event.incident_key = incident.incident_key
            GROUP BY incident.state, circuit.consecutive_failures, circuit.state
            """
        ).fetchone()

    assert shape == (2, 2, 2, 1, 1, 1, 1)
    assert incident == ("resolved", 0, "closed", ["attempt-failed", "recovered"])


def test_quote_admission_missing_shard_adapter_restores_exact_input_then_admits(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
) -> None:
    identity = AttackIdentity(
        experiment_id="commission:quote-admit:quote-admission-missing-shard",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id="quote-admit",
        attack_id="quote-admission-missing-shard",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=QuoteAdmissionMissingShardCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=62),
        ),
        evidence_dir=tmp_path / "quote-admit",
    )

    assert proof["qualification_impact"] == "pause"
    assert str(proof["detector_fact_id"]).startswith("incident:")
    assert str(proof["recovery_action_id"]).startswith("artifact-restored:")
    assert str(proof["recovery_fact_id"]).startswith("event:")
    assert str(proof["postcondition_fact_id"]).startswith(
        "postgres:m1_quote_batch_inputs:quote:"
    )
    assert proof["cleanup_verified"] is True

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        admission = connection.execute(
            """
            SELECT job.state, count(attempt.attempt_id),
                   array_agg(attempt.state ORDER BY attempt.lease_epoch)
            FROM m1_jobs AS job
            JOIN m1_job_attempts AS attempt ON attempt.job_key = job.job_key
            WHERE job.job_type = 'quote-admit'
            GROUP BY job.state
            """
        ).fetchone()
        batch = connection.execute(
            """
            SELECT job.state, jsonb_array_length(input.legs),
                   jsonb_array_length(input.token_ids),
                   input.input_artifact_key IS NOT NULL,
                   input.input_artifact_digest IS NOT NULL,
                   input.leg_count
            FROM m1_quote_batch_inputs AS input
            JOIN m1_jobs AS job ON job.job_key = input.job_key
            """
        ).fetchone()
        incident = connection.execute(
            """
            SELECT incident.state, circuit.consecutive_failures, circuit.state,
                   array_agg(event.kind ORDER BY event.occurred_at, event.kind)
            FROM m1_incidents AS incident
            JOIN m1_job_circuits AS circuit
              ON incident.dedupe_key = 'job-retry:' || circuit.job_key
            JOIN m1_incident_events AS event
              ON event.incident_key = incident.incident_key
            WHERE incident.component = 'quote-admit'
            GROUP BY incident.state, circuit.consecutive_failures, circuit.state
            """
        ).fetchone()
        quote_pointer = connection.execute(
            "SELECT count(*) FROM m1_publication_pointers WHERE pointer_key = 'quote:current'"
        ).fetchone()

    assert admission == ("succeeded", 2, ["retryable", "succeeded"])
    assert batch == ("runnable", None, None, True, True, 2)
    assert incident == ("resolved", 0, "closed", ["attempt-failed", "recovered"])
    assert quote_pointer == (0,)


def test_normalization_payload_corrupt_adapter_quarantines_and_preserves_pointer(
    control_plane: PostgresControlPlane,
    tmp_path: Path,
) -> None:
    identity = AttackIdentity(
        experiment_id="commission:structure-normalize:normalization-payload-corrupt",
        release_id="a" * 40,
        config_id=f"sha256:{'b' * 64}",
        node_id="structure-normalize",
        attack_id="normalization-payload-corrupt",
    )

    proof = run_disposable_attack(
        identity=identity,
        adapter=NormalizationPayloadCorruptCommissioningAdapter(
            control_plane=control_plane,
            started_at=NOW + timedelta(minutes=64),
        ),
        evidence_dir=tmp_path / "structure-normalize-corrupt",
    )

    assert proof["qualification_impact"] == "block"
    assert str(proof["detector_fact_id"]).startswith("event:")
    assert str(proof["recovery_action_id"]).startswith("operator-action:")
    assert str(proof["recovery_fact_id"]).endswith(":operator-action-required")
    assert str(proof["postcondition_fact_id"]).startswith(
        "pointer:structure:current:shadow:structure:"
    )
    assert proof["cleanup_verified"] is True

    with control_plane._connection_factory() as connection:  # noqa: SLF001
        shape = connection.execute(
            """
            SELECT job.state, attempt.state, incident.state, incident.severity,
                   event.kind, outbox.state,
                   (SELECT count(*) FROM m1_structure_range_receipts
                    WHERE job_key = job.job_key),
                   (SELECT count(*) FROM m1_job_circuits WHERE job_key = job.job_key)
            FROM m1_jobs AS job
            JOIN m1_job_attempts AS attempt ON attempt.job_key = job.job_key
            JOIN m1_incidents AS incident
              ON incident.dedupe_key = 'input-quarantine:' || job.job_key
            JOIN m1_incident_events AS event USING (incident_key)
            JOIN m1_alert_outbox AS outbox USING (incident_event_id)
            WHERE job.job_type = 'structure-normalize' AND job.state = 'quarantined'
            """
        ).fetchone()

    assert shape == (
        "quarantined",
        "quarantined",
        "open",
        "critical",
        "escalated",
        "pending",
        0,
        0,
    )
def _claim_progress_and_complete(control_plane: PostgresControlPlane, *, job_type: str) -> JobLease:
    now = NOW + timedelta(minutes=REQUIRED_JOB_TYPES.index(job_type) + 1)
    if job_type == "structure-fetch":
        return _complete_structure_fetch(control_plane, now=now)
    if job_type == "structure-materialize":
        return _complete_structure_materialize(control_plane, now=now)
    if job_type == "structure-normalize":
        return _complete_structure_normalize(control_plane, now=now)
    if job_type == "structure-certify":
        return _complete_structure_certify(control_plane, now=now)
    if job_type == "quote-admit":
        return _complete_quote_admit(control_plane, now=now)
    if job_type == "quote-batch":
        return _complete_quote_batch(control_plane, now=now)
    if job_type == "quote-certify":
        return _complete_quote_certify(control_plane, now=now)
    if job_type == "opportunity-certify":
        return _complete_opportunity_certify(control_plane, now=now)
    raise AssertionError(f"unhandled runtime job type: {job_type}")


def _record_all_progress(
    control_plane: PostgresControlPlane, *, lease: JobLease, now: datetime
) -> None:
    runtime = AttemptRuntime(
        store=control_plane,
        lease=lease,
        profile=PROFILE,
        clock=lambda: now + timedelta(seconds=1),
    )
    stages = RUNTIME_STAGE_REGISTRY[lease.job_type]
    for current, stage in enumerate(stages, start=1):
        runtime.progress(
            stage=stage,
            current=current,
            total=len(stages),
            detail={"component": lease.job_type},
        )


def _complete_structure_fetch(control_plane: PostgresControlPlane, *, now: datetime) -> JobLease:
    window_key = "runtime-coverage:source"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    lease = _claim(control_plane, "structure-fetch", now=now)
    _record_all_progress(control_plane, lease=lease, now=now)
    successor = control_plane.record_structure_source_page(
        lease,
        artifact_key="structure-source/runtime-coverage/source.json",
        artifact_digest="a" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        now=now + timedelta(seconds=2),
    )
    assert successor is not None
    return lease


def _complete_structure_materialize(
    control_plane: PostgresControlPlane, *, now: datetime
) -> JobLease:
    window_key = "runtime-coverage:materialize"
    control_plane.admit_structure_source_window(window_key=window_key, now=now)
    source = _claim(control_plane, "structure-fetch", now=now)
    control_plane.record_structure_source_page(
        source,
        artifact_key="structure-source/runtime-coverage/materialize-source.json",
        artifact_digest="b" * 64,
        next_cursor=None,
        completed=True,
        record_count=1,
        event_embedded_markets=True,
        now=now,
    )
    lease = _claim(control_plane, "structure-materialize", now=now)
    source_digest = control_plane.structure_source_window_digest(window_key)
    bundle = StructureBundleArtifact.from_bytes(b'{"kind":"runtime-materialize"}\n')
    _record_all_progress(control_plane, lease=lease, now=now)
    specs = control_plane.admit_structure_source_bundle(
        lease,
        identity=_structure_identity(
            suffix="materialize",
            window_id=window_key,
            comparison_receipt_digest=source_digest,
            source_kind="gamma-source-window-events-v3-sharded",
            events=1,
            markets=0,
        ),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now + timedelta(seconds=2),
    )
    assert len(specs) == 1
    return lease


def _complete_structure_normalize(
    control_plane: PostgresControlPlane, *, now: datetime
) -> JobLease:
    spec, _bundle = _enqueue_structure_generation(control_plane, suffix="normalize", now=now)
    lease = _claim(control_plane, "structure-normalize", now=now)
    _record_all_progress(control_plane, lease=lease, now=now)
    receipt = control_plane.complete_structure_range(
        lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/runtime-coverage/normalize.ndjson",
        artifact_digest="c" * 64,
        record_count=1,
        now=now + timedelta(seconds=2),
    )
    assert receipt.job_key == lease.job_key
    return lease


def _complete_structure_certify(control_plane: PostgresControlPlane, *, now: datetime) -> JobLease:
    spec, bundle = _enqueue_structure_generation(control_plane, suffix="certify", now=now)
    range_artifact = "d" * 64
    range_lease = _claim(control_plane, "structure-normalize", now=now)
    control_plane.complete_structure_range(
        range_lease,
        range_digest=spec.range_digest,
        artifact_key="structure-ranges/runtime-coverage/certify.ndjson",
        artifact_digest=range_artifact,
        record_count=1,
        now=now,
    )
    lease = _claim(control_plane, "structure-certify", now=now)
    _record_all_progress(control_plane, lease=lease, now=now)
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
                    "artifact_key": "structure-ranges/runtime-coverage/certify.ndjson",
                    "artifact_digest": range_artifact,
                    "record_count": 1,
                },
            ),
        )
    ).hexdigest()
    assert (
        control_plane.certify_structure_generation(
            lease,
            generation_key=spec.generation_key,
            artifact_key=f"structure-manifests/{manifest_digest}/manifest.ndjson",
            artifact_digest=manifest_digest,
            now=now + timedelta(seconds=2),
        )
        == manifest_digest
    )
    return lease


def _complete_quote_admit(control_plane: PostgresControlPlane, *, now: datetime) -> JobLease:
    structure_digest = "e" * 64
    universe_hash = "f" * 64
    generation_key = f"structure:{structure_digest}"
    job_key = f"{generation_key}:quote-admit"
    bundle_key = "bundles/runtime-coverage.ndjson"
    control_plane.enqueue_job(
        job_key=job_key,
        job_type="quote-admit",
        input_identity=f"{generation_key}:{bundle_key}:{structure_digest}",
        now=now,
    )
    with control_plane._connection_factory() as connection:  # noqa: SLF001
        connection.execute(
            """
            INSERT INTO m1_structure_generation_inputs
                (generation_key, bundle_key, bundle_digest, identity, admitted_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (generation_key, bundle_key, structure_digest, Jsonb({}), now),
        )
        connection.execute(
            """
            INSERT INTO m1_quote_admission_inputs
                (job_key, generation_key, bundle_key, bundle_digest, admitted_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (job_key, generation_key, bundle_key, structure_digest, now),
        )
    lease = _claim(control_plane, "quote-admit", now=now)
    _record_all_progress(control_plane, lease=lease, now=now)
    legs = (_leg("quote-admit-token"),)
    batches = control_plane.quote_batches_from_legs(
        structure_receipt_digest=structure_digest,
        universe_hash=universe_hash,
        legs=legs,
        batch_size=1,
    )
    artifacts = {
        batch.job_key: (
            f"quote-inputs/{artifact_digest}/batch.ndjson",
            artifact_digest,
            len(batch.legs),
        )
        for batch in batches
        for artifact_digest in (sha256(batch.job_key.encode()).hexdigest(),)
    }
    assert (
        control_plane.admit_quote_generation(
            lease,
            structure_receipt_digest=structure_digest,
            universe_hash=universe_hash,
            legs=legs,
            batch_size=1,
            input_artifacts=artifacts,
            now=now + timedelta(seconds=2),
        )
        == batches
    )
    return lease


def _complete_quote_batch(control_plane: PostgresControlPlane, *, now: datetime) -> JobLease:
    batch = control_plane.enqueue_quote_generation(
        structure_receipt_digest="1" * 64,
        universe_hash="2" * 64,
        legs=(_leg("quote-batch-token"),),
        batch_size=1,
        now=now,
    )[0]
    lease = _claim(control_plane, "quote-batch", now=now)
    _record_all_progress(control_plane, lease=lease, now=now)
    receipt = control_plane.record_quote_batch(
        lease,
        token_range_digest=batch.token_range_digest,
        quote_digest="3" * 64,
        artifact_key="quote-batches/runtime-coverage/batch.ndjson",
        artifact_digest="4" * 64,
        successful_response_count=1,
        quoted_at=now,
        now=now + timedelta(seconds=2),
        terminal=True,
    )
    assert receipt.job_key == lease.job_key
    return lease


def _complete_quote_certify(control_plane: PostgresControlPlane, *, now: datetime) -> JobLease:
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest="5" * 64,
        universe_hash="6" * 64,
        legs=(_leg("quote-certify-token-a"), _leg("quote-certify-token-b")),
        batch_size=1,
        now=now,
    )
    for index, batch in enumerate(batches):
        batch_lease = _claim(control_plane, "quote-batch", now=now + timedelta(seconds=index))
        control_plane.record_quote_batch(
            batch_lease,
            token_range_digest=batch.token_range_digest,
            quote_digest=str(7 + index) * 64,
            artifact_key=f"quote-batches/runtime-coverage/certify-{index}.ndjson",
            artifact_digest=str(8 + index) * 64,
            successful_response_count=1,
            quoted_at=now,
            now=now + timedelta(seconds=index),
            terminal=True,
        )
    certification_now = now + timedelta(seconds=len(batches))
    lease = _claim(control_plane, "quote-certify", now=certification_now)
    _record_all_progress(control_plane, lease=lease, now=certification_now)
    artifact_digest = control_plane.certify_quote_generation(
        lease,
        generation_key=batches[0].generation_key,
        now=certification_now + timedelta(seconds=1),
    )
    assert len(artifact_digest) == 64
    return lease


def _complete_opportunity_certify(
    control_plane: PostgresControlPlane, *, now: datetime
) -> JobLease:
    structure_generation_key, structure_digest = _certify_structure_prerequisite(
        control_plane, suffix="opportunity", now=now
    )
    quote_generation_key = _certify_quote_prerequisite(
        control_plane,
        suffix="opportunity",
        structure_digest=structure_digest,
        now=now + timedelta(seconds=10),
    )
    lease = _claim(control_plane, "opportunity-certify", now=now + timedelta(seconds=20))
    _record_all_progress(control_plane, lease=lease, now=now + timedelta(seconds=20))
    digest = control_plane.publish_opportunity_projection(
        quote_generation_key=quote_generation_key,
        structure_generation_key=structure_generation_key,
        rows=(
            {
                "group_id": "runtime-coverage-group",
                "event_id": "runtime-coverage-event",
                "membership_hash": "runtime-coverage-membership",
                "bundle_cost": 0.91,
                "gross_edge_bps": 900.0,
                "max_bundle_size": 4.0,
                "legs": [
                    {
                        "yes_token_id": "opportunity-token",
                        "ask_price": 0.91,
                        "ask_size": 4.0,
                    }
                ],
                "structure_observed_at_ms": 1,
                "quote_started_at_ms": 2,
                "quote_quoted_at_ms": 3,
            },
        ),
        now=now + timedelta(seconds=22),
        lease=lease,
    )
    assert len(digest) == 64
    return lease


def _certify_structure_prerequisite(
    control_plane: PostgresControlPlane, *, suffix: str, now: datetime
) -> tuple[str, str]:
    spec, bundle = _enqueue_structure_generation(control_plane, suffix=suffix, now=now)
    range_artifact = sha256(f"{suffix}:range".encode()).hexdigest()
    range_lease = _claim(control_plane, "structure-normalize", now=now)
    control_plane.complete_structure_range(
        range_lease,
        range_digest=spec.range_digest,
        artifact_key=f"structure-ranges/runtime-coverage/{suffix}.ndjson",
        artifact_digest=range_artifact,
        record_count=1,
        now=now,
    )
    certifier = _claim(control_plane, "structure-certify", now=now)
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
                    "artifact_key": f"structure-ranges/runtime-coverage/{suffix}.ndjson",
                    "artifact_digest": range_artifact,
                    "record_count": 1,
                },
            ),
        )
    ).hexdigest()
    control_plane.certify_structure_generation(
        certifier,
        generation_key=spec.generation_key,
        artifact_key=f"structure-manifests/{manifest_digest}/manifest.ndjson",
        artifact_digest=manifest_digest,
        now=now,
    )
    return spec.generation_key, bundle.sha256


def _certify_quote_prerequisite(
    control_plane: PostgresControlPlane,
    *,
    suffix: str,
    structure_digest: str,
    now: datetime,
) -> str:
    batches = control_plane.enqueue_quote_generation(
        structure_receipt_digest=structure_digest,
        universe_hash=sha256(f"{suffix}:universe".encode()).hexdigest(),
        legs=(_leg(f"{suffix}-quote-token"),),
        batch_size=1,
        now=now,
    )
    batch = batches[0]
    batch_lease = _claim(control_plane, "quote-batch", now=now)
    control_plane.record_quote_batch(
        batch_lease,
        token_range_digest=batch.token_range_digest,
        quote_digest=sha256(f"{suffix}:quote".encode()).hexdigest(),
        artifact_key=f"quote-batches/runtime-coverage/{suffix}.ndjson",
        artifact_digest=sha256(f"{suffix}:quote-artifact".encode()).hexdigest(),
        successful_response_count=1,
        quoted_at=now,
        now=now,
        terminal=True,
    )
    certifier = _claim(control_plane, "quote-certify", now=now)
    control_plane.certify_quote_generation(
        certifier,
        generation_key=batch.generation_key,
        now=now,
    )
    return batch.generation_key


def _enqueue_structure_generation(
    control_plane: PostgresControlPlane, *, suffix: str, now: datetime
):
    bundle = StructureBundleArtifact.from_bytes(f'{{"kind":"runtime-{suffix}-bundle"}}\n'.encode())
    specs = control_plane.enqueue_structure_generation(
        identity=_structure_identity(
            suffix=suffix,
            window_id=f"runtime-coverage:{suffix}",
            comparison_receipt_digest=sha256(f"{suffix}:comparison".encode()).hexdigest(),
            source_kind="legacy-publication-v1",
            events=1,
            markets=0,
        ),
        bundle=bundle,
        ranges=(("events", "", ""),),
        now=now,
    )
    assert len(specs) == 1
    return specs[0], bundle


def _structure_identity(
    *,
    suffix: str,
    window_id: str,
    comparison_receipt_digest: str,
    source_kind: str,
    events: int,
    markets: int,
) -> StructureBundleIdentity:
    return StructureBundleIdentity(
        publication_id=f"runtime-coverage:{suffix}",
        window_id=window_id,
        snapshot_id=42,
        comparison_receipt_digest=comparison_receipt_digest,
        normalization_contract_version="structure-v7",
        source_kind=source_kind,
        component_counts={
            "events": events,
            "event_tags": 0,
            "memberships": 0,
            "group_truth": 0,
            "markets": markets,
            "issues": 0,
        },
    )


def _leg(token_id: str) -> QuoteBatchLeg:
    return QuoteBatchLeg(
        neg_risk_market_id=f"neg-risk-{token_id}",
        market_id=f"market-{token_id}",
        condition_id=f"condition-{token_id}",
        slug=f"slug-{token_id}",
        yes_token_id=token_id,
        event_id=f"event-{token_id}",
        membership_hash=f"membership-{token_id}",
    )


def _claim(control_plane: PostgresControlPlane, job_type: str, *, now: datetime) -> JobLease:
    lease = control_plane.claim_job(
        worker_id=f"runtime-coverage:{job_type}",
        job_types=(job_type,),
        lease_seconds=PROFILE.lease_seconds,
        now=now,
    )
    assert lease is not None
    return lease


def _runtime_event_rows(
    control_plane: PostgresControlPlane, *, job_keys: tuple[str, ...]
) -> list[dict[str, object]]:
    with (
        control_plane._connection_factory() as connection,
        connection.cursor(  # noqa: SLF001
            row_factory=dict_row
        ) as cursor,
    ):
        cursor.execute(
            """
            SELECT job.job_type, event.job_key, event.event_sequence, event.kind,
                   event.stage, event.progress_sequence, event.progress_current,
                   event.progress_total, event.detail
            FROM m1_job_runtime_events AS event
            JOIN m1_jobs AS job ON job.job_key = event.job_key
            WHERE event.job_key = ANY(%s)
            ORDER BY event.event_sequence
            """,
            (list(job_keys),),
        )
        return list(cursor.fetchall())


def _secret_like_detail_keys(detail: dict[str, object]) -> set[str]:
    found: set[str] = set()
    for key in detail:
        normalized = key.casefold().replace("-", "_")
        if any(part in normalized for part in SECRET_LIKE_DETAIL_KEY_PARTS):
            found.add(key)
    return found
