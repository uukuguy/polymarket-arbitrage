"""Deterministic local runtime fault matrix qualification gate."""

from __future__ import annotations

import json
import os
import subprocess
from collections.abc import Iterator
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql

from polyarb import cli_control_plane
from polyarb.control_plane import runtime_fault_matrix as matrix_module
from polyarb.control_plane.db_role_admin import provision_login_roles
from polyarb.control_plane.runtime_fault_matrix import (
    RuntimeFaultMatrixError,
    canonical_fault_matrix_bytes,
    run_fault_matrix,
)

FAULT_CLASSES = (
    "task-exception",
    "transport-generation-replacement",
    "pre-io-stage-timeout",
    "r2-timeout-hang",
    "heartbeat-loss",
    "progress-stall",
    "stale-owner",
    "circuit-probe",
    "recovery-episode-isolation",
    "service-interruption",
    "process-exit",
    "machine-restart-decision",
    "database-event-writer-failure",
    "watchdog-failure",
    "duplicate-delivery",
    "stale-action",
)


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
            == 0
        )
    except OSError:
        return False


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


@pytest.fixture(scope="module")
def control_plane_test_dsn() -> Iterator[str]:
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; runtime fault matrix requires real Postgres")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as connection:
            for role in ("anon", "authenticated", "service_role"):
                connection.execute(f"CREATE ROLE {role} NOLOGIN")
        yield dsn


def test_runtime_fault_matrix_requires_explicit_test_dsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)

    with pytest.raises(RuntimeFaultMatrixError, match="POLYARB_CONTROL_PLANE_TEST_DSN"):
        run_fault_matrix()


@pytest.mark.parametrize(
    "dsn",
    (
        "postgresql://postgres:secret@db.prod.supabase.co:5432/postgres",
        "postgresql://postgres:secret@aws-0-us-east-1.pooler.supabase.com:5432/postgres",
        "postgresql://postgres:secret@10.1.2.3:5432/test",
        "postgresql://postgres:secret@localhost:5432/test?options=-csearch_path%3Dpublic",
        "postgresql://postgres:secret@localhost:5432/test?search_path=public",
    ),
)
def test_runtime_fault_matrix_rejects_non_loopback_or_injected_dsn(
    monkeypatch: pytest.MonkeyPatch,
    dsn: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", dsn)

    with pytest.raises(RuntimeFaultMatrixError, match="non-production"):
        run_fault_matrix()


def test_runtime_fault_matrix_is_canonical_ordered_and_cleans_temp_database(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)

    first = run_fault_matrix()
    second = run_fault_matrix()

    assert canonical_fault_matrix_bytes(first) == canonical_fault_matrix_bytes(second)
    assert first == second
    assert first["status"] == "pass"
    assert first["schema_version"] == "m1-runtime-fault-matrix-v4"
    assert first["scoped_roles"] == {
        "qualification_worker": {
            "facts_consumed": first["qualification_fact_count"],
            "profile": "qualification-worker",
            "status": "pass",
        },
        "runtime_controller": {
            "observe_decisions": first["observe_decision_count"],
            "profile": "runtime-controller",
            "recovery_actions_created": 0,
            "status": "pass",
        },
    }
    qualification_fact_count = first["qualification_fact_count"]
    observe_decision_count = first["observe_decision_count"]
    assert isinstance(qualification_fact_count, int)
    assert isinstance(observe_decision_count, int)
    assert qualification_fact_count >= len(FAULT_CLASSES)
    assert observe_decision_count >= 1
    cases = first["cases"]
    assert isinstance(cases, list)
    assert [case["fault_class"] for case in cases] == list(FAULT_CLASSES)
    assert first["case_count"] == len(FAULT_CLASSES)

    for case in cases:
        assert case["status"] == "pass"
        assert isinstance(case["detection_latency_seconds"], int)
        assert case["detection_latency_seconds"] >= 0
        assert set(case) == {
            "action",
            "dashboard_projection",
            "detection_latency_seconds",
            "fault_class",
            "fence_result",
            "incident_transition",
            "qualification_impact",
            "qualification_projection",
            "recovery",
            "status",
        }
        assert case["incident_transition"]["state"] in {"open", "resolved", "not-required"}
        assert case["action"]["result"] in {
            "scheduled",
            "succeeded",
            "stale-noop",
            "idempotent-replay",
            "recorded",
            "observe-only",
            "not-required",
        }
        assert case["recovery"]["state"] in {
            "contained",
            "recovered",
            "observe-only",
            "stale-noop",
            "not-required",
        }
        assert case["dashboard_projection"]["source"] == "operational_snapshot"
        assert case["qualification_impact"] in {
            "breaking",
            "delayed",
            "none",
            "restored",
        }
        assert case["qualification_impact"] == case["qualification_projection"]["impact"]
        assert case["qualification_projection"]["source"] == "m1_qualification_ingress_ledger"
        assert case["qualification_projection"]["ingress_count"] > 0
        assert case["qualification_projection"]["decode_error_count"] == 0
        assert case["qualification_projection"]["replay_error_count"] == 0
        assert (
            case["qualification_projection"]["decoded_count"]
            == case["qualification_projection"]["ingress_count"]
        )
        assert case["qualification_projection"]["latest"]["payload_anchor"]
        assert isinstance(case["qualification_projection"]["sources"], dict)

    by_fault = {case["fault_class"]: case for case in cases}
    assert by_fault["task-exception"]["incident_transition"]["source"] == (
        "finish_retryable_with_incident"
    )
    assert "runtime" in by_fault["task-exception"]["qualification_projection"]["sources"]
    assert by_fault["r2-timeout-hang"]["incident_transition"]["outbox_pending"] >= 1
    assert by_fault["transport-generation-replacement"]["fence_result"] == (
        "transport-generation-retired"
    )
    assert by_fault["pre-io-stage-timeout"]["recovery"]["stage"] == "fetch-page"
    assert by_fault["service-interruption"]["fence_result"] == "defect-streak-preserved"
    assert by_fault["recovery-episode-isolation"]["recovery"]["episode_count"] == 3
    assert by_fault["heartbeat-loss"]["action"]["type"] == "reclaim-job"
    assert by_fault["progress-stall"]["action"]["type"] == "cancel-job"
    assert by_fault["duplicate-delivery"]["fence_result"] == "idempotent-replay"
    assert "recovery" in by_fault["duplicate-delivery"]["qualification_projection"]["sources"]
    assert by_fault["stale-action"]["action"]["result"] == "stale-noop"
    assert "recovery" in by_fault["stale-action"]["qualification_projection"]["sources"]
    assert by_fault["database-event-writer-failure"]["recovery"]["rollback_verified"] is True
    assert by_fault["database-event-writer-failure"]["qualification_impact"] == "delayed"
    assert by_fault["watchdog-failure"]["incident_transition"]["source"] == "record_incident_event"
    assert "incident" in by_fault["watchdog-failure"]["qualification_projection"]["sources"]

    with psycopg.connect(control_plane_test_dsn) as connection:
        rows = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'runtime_fault_matrix_%'"
        ).fetchall()
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability',
                'm1_runtime_controller_login',
                'm1_qualification_worker_login'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert rows == []
    assert roles == []


def test_runtime_fault_matrix_cleans_database_when_alembic_upgrade_fails(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://localhost/original")

    def fail_after_database(_dsn: str) -> None:
        raise RuntimeFaultMatrixError("injected alembic upgrade failure")

    monkeypatch.setattr(matrix_module, "_run_alembic_upgrade", fail_after_database)

    with pytest.raises(RuntimeFaultMatrixError, match="injected alembic upgrade failure"):
        run_fault_matrix()

    assert os.environ["POLYARB_SUPABASE_DB_DSN"] == "postgresql://localhost/original"
    with psycopg.connect(control_plane_test_dsn) as connection:
        rows = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'runtime_fault_matrix_%'"
        ).fetchall()
    assert rows == []


@pytest.mark.parametrize(
    "role_name",
    (
        "l3_evidence_daemon",
        "m1_runtime_controller_capability",
        "m1_qualification_worker_capability",
    ),
)
def test_runtime_fault_matrix_refuses_preexisting_migration_capability_roles(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
    role_name: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)
    with psycopg.connect(control_plane_test_dsn, autocommit=True) as connection:
        connection.execute(
            sql.SQL("CREATE ROLE {} LOGIN SUPERUSER").format(sql.Identifier(role_name))
        )
    try:
        with pytest.raises(RuntimeFaultMatrixError, match="pre-existing cluster role"):
            run_fault_matrix()
    finally:
        with psycopg.connect(control_plane_test_dsn, autocommit=True) as connection:
            connection.execute(sql.SQL("DROP ROLE {}").format(sql.Identifier(role_name)))

    with psycopg.connect(control_plane_test_dsn) as connection:
        leaked = connection.execute(
            "SELECT datname FROM pg_database WHERE datname LIKE 'runtime_fault_matrix_%'"
        ).fetchall()
    assert leaked == []


def test_runtime_fault_matrix_cleans_migration_created_roles_for_replay(
    monkeypatch: pytest.MonkeyPatch,
    control_plane_test_dsn: str,
) -> None:
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", control_plane_test_dsn)

    run_fault_matrix()

    with psycopg.connect(control_plane_test_dsn) as connection:
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability',
                'm1_runtime_controller_login',
                'm1_qualification_worker_login'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert roles == []

    run_fault_matrix()

    with psycopg.connect(control_plane_test_dsn) as connection:
        roles = connection.execute(
            """
            SELECT rolname FROM pg_roles
            WHERE rolname IN (
                'l3_evidence_daemon',
                'l3_retention_operator',
                'm1_runtime_controller_capability',
                'm1_qualification_worker_capability',
                'm1_runtime_controller_login',
                'm1_qualification_worker_login'
            )
            ORDER BY rolname
            """
        ).fetchall()
    assert roles == []


def test_runtime_fault_matrix_exercises_real_migrated_authority_paths(
    control_plane_test_dsn: str,
) -> None:
    database_name = "runtime_fault_matrix_" + "a" * 32
    matrix_dsn = matrix_module._dsn_with_database(control_plane_test_dsn, database_name)
    matrix_module._drop_isolated_database_if_exists(control_plane_test_dsn, database_name)
    matrix_module._create_isolated_database(control_plane_test_dsn, matrix_dsn, database_name)
    try:
        with psycopg.connect(matrix_dsn) as connection:
            trigger_count = connection.execute(
                """
                SELECT count(*) FROM information_schema.triggers
                WHERE trigger_schema = 'public' AND event_object_table LIKE 'm1_%%'
                """,
            ).fetchone()
            fk_count = connection.execute(
                """
                SELECT count(*) FROM pg_constraint
                WHERE connamespace = 'public'::regnamespace AND contype = 'f'
                """,
            ).fetchone()
            unique_index_count = connection.execute(
                """
                SELECT count(*) FROM pg_indexes
                WHERE schemaname = 'public'
                  AND tablename LIKE 'm1_%%'
                  AND indexdef LIKE '%%UNIQUE%%'
                """,
            ).fetchone()
        assert trigger_count is not None and int(trigger_count[0]) > 0
        assert fk_count is not None and int(fk_count[0]) > 0
        assert unique_index_count is not None and int(unique_index_count[0]) > 0

        runtime_password = f"runtime-controller-{uuid4().hex}"
        qualification_password = f"qualification-worker-{uuid4().hex}"
        provision_login_roles(
            lambda: psycopg.connect(matrix_dsn),
            expected_database=database_name,
            runtime_password=runtime_password,
            qualification_password=qualification_password,
        )
        context = matrix_module._context(
            matrix_dsn,
            runtime_password=runtime_password,
            qualification_password=qualification_password,
        )
        for index, fault_class in enumerate(
            (
                "task-exception",
                "r2-timeout-hang",
                "progress-stall",
                "database-event-writer-failure",
                "duplicate-delivery",
                "stale-action",
            )
        ):
            result = matrix_module._run_case(context, index, fault_class).case
            assert result["status"] == "pass"

        with psycopg.connect(matrix_dsn) as connection:
            facts = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM m1_jobs) AS jobs,
                    (SELECT count(*) FROM m1_job_runtime_events) AS runtime_events,
                    (SELECT count(*) FROM m1_incidents) AS incidents,
                    (SELECT count(*) FROM m1_recovery_actions) AS recovery_actions,
                    (SELECT count(*) FROM m1_alert_outbox) AS outbox
                """
            ).fetchone()
        assert facts is not None
        jobs, runtime_events, incidents, recovery_actions, outbox = map(int, facts)
        assert jobs >= 6
        assert runtime_events >= 1
        assert incidents >= 3
        assert recovery_actions >= 2
        assert outbox >= 1
    finally:
        maintenance_dsn = matrix_module._dsn_with_database_unchecked(
            control_plane_test_dsn,
            "postgres",
        )
        with psycopg.connect(maintenance_dsn, autocommit=True) as connection:
            matrix_module._drop_disposable_login_roles_if_safe(connection)
        matrix_module._drop_isolated_database_if_exists(control_plane_test_dsn, database_name)
        with psycopg.connect(maintenance_dsn, autocommit=True) as connection:
            matrix_module._lock_runtime_fault_matrix_cluster(connection)
            try:
                matrix_module._drop_migration_created_cluster_roles_if_safe(connection)
            finally:
                matrix_module._unlock_runtime_fault_matrix_cluster(connection)
        with psycopg.connect(control_plane_test_dsn) as connection:
            roles = connection.execute(
                """
                SELECT 1 FROM pg_roles
                WHERE rolname IN ('l3_evidence_daemon','l3_retention_operator')
                """
            ).fetchall()
        assert roles == []


def test_runtime_fault_matrix_cli_rejects_missing_test_dsn_before_default_dsn(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.delenv("POLYARB_CONTROL_PLANE_TEST_DSN", raising=False)
    monkeypatch.setenv("POLYARB_SUPABASE_DB_DSN", "postgresql://prod.example/prod")

    assert cli_control_plane.main(["runtime-fault-matrix", "--json"]) == 2
    captured = capsys.readouterr()
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in captured.err
    assert captured.out == ""


def test_runtime_fault_matrix_cli_emits_canonical_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    payload = {
        "case_count": 0,
        "cases": [],
        "observe_decision_count": 0,
        "qualification_fact_count": 0,
        "schema_version": "m1-runtime-fault-matrix-v2",
        "scoped_roles": {
            "qualification_worker": {
                "facts_consumed": 0,
                "profile": "qualification-worker",
                "status": "pass",
            },
            "runtime_controller": {
                "observe_decisions": 0,
                "profile": "runtime-controller",
                "recovery_actions_created": 0,
                "status": "pass",
            },
        },
        "status": "pass",
    }
    monkeypatch.setenv("POLYARB_CONTROL_PLANE_TEST_DSN", "postgresql://localhost/test")
    monkeypatch.setattr(cli_control_plane, "run_fault_matrix", lambda: payload)

    assert cli_control_plane.main(["runtime-fault-matrix", "--json"]) == 0
    captured = capsys.readouterr()
    assert json.loads(captured.out) == payload
    assert captured.err == ""
