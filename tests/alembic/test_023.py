"""Contracts for the additive runtime recovery action migration."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest

MIGRATION_PATH = Path("alembic/versions/023_m1_runtime_recovery.py")


def test_023_chains_after_022_and_declares_recovery_tables() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "023"' in text
    assert 'down_revision = "022"' in text
    assert '"m1_runtime_controller_leases"' in text
    assert '"m1_recovery_actions"' in text
    assert '"m1_recovery_target_budgets"' in text
    assert '"m1_incidents.incident_key"' in text
    assert '"m1_job_attempts.attempt_id"' in text


def test_023_recovery_actions_are_fenced_bounded_and_single_active_per_target() -> None:
    text = MIGRATION_PATH.read_text()

    assert "expected_controller_epoch" in text
    assert "expected_attempt_id" in text
    assert "expected_lease_epoch" in text
    assert "idempotency_key" in text
    assert "pg_column_size(detail) <= 4096" in text
    assert "ck_m1_recovery_actions_state" in text
    assert "ck_m1_recovery_actions_result_code" in text
    assert "CREATE UNIQUE INDEX uq_m1_recovery_action_active_target" in text
    assert "ON m1_recovery_actions(target_type, target_id)" in text
    assert "WHERE state IN ('pending', 'running')" in text


def test_023_controller_lease_epoch_is_singleton_and_monotonic() -> None:
    text = MIGRATION_PATH.read_text()

    assert '"controller_id"' in text
    assert '"lease_epoch"' in text
    assert "lease_epoch > 0" in text
    assert "uq_m1_runtime_controller_leases_owner_epoch" in text


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
            == 0
        )
    except OSError:
        return False


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def _create_supabase_roles(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        for role in ("anon", "authenticated", "service_role"):
            connection.execute(f"CREATE ROLE {role} NOLOGIN")


def test_023_upgrades_from_022_downgrades_and_reupgrades_with_expected_schema() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 022↔023 migration contract")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if dsn.startswith(prefix):
                dsn = "postgresql://" + dsn[len(prefix) :]
        _create_supabase_roles(dsn)

        _run_alembic(dsn, "upgrade", "022")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_recovery_actions")

        _run_alembic(dsn, "upgrade", "023")
        with psycopg.connect(dsn) as connection:
            assert _table_exists(connection, "m1_runtime_controller_leases")
            assert _table_exists(connection, "m1_recovery_actions")
            assert _table_exists(connection, "m1_recovery_target_budgets")
            assert _check_exists(connection, "m1_recovery_actions", "ck_m1_recovery_actions_state")
            assert _check_exists(
                connection,
                "m1_recovery_actions",
                "ck_m1_recovery_actions_result_code",
            )
            assert _fk_exists(
                connection,
                "m1_recovery_actions",
                "fk_m1_recovery_actions_controller",
            )
            assert _fk_exists(connection, "m1_recovery_actions", "fk_m1_recovery_actions_attempt")
            assert _partial_index_predicate(
                connection,
                "uq_m1_recovery_action_active_target",
            ) == "(state = ANY (ARRAY['pending'::text, 'running'::text]))"

        _run_alembic(dsn, "downgrade", "022")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_runtime_controller_leases")
            assert not _table_exists(connection, "m1_recovery_actions")
            assert not _table_exists(connection, "m1_recovery_target_budgets")
            assert _table_exists(connection, "m1_job_runtime_state")

        _run_alembic(dsn, "upgrade", "023")
        with psycopg.connect(dsn) as connection:
            assert _table_exists(connection, "m1_recovery_actions")
            assert _partial_index_predicate(
                connection,
                "uq_m1_recovery_action_active_target",
            ) == "(state = ANY (ARRAY['pending'::text, 'running'::text]))"


def _table_exists(connection: psycopg.Connection, table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
        row = cursor.fetchone()
        return bool(row and row[0])


def _check_exists(connection: psycopg.Connection, table_name: str, constraint: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass
              AND conname = %s
              AND contype = 'c'
            """,
            (f"public.{table_name}", constraint),
        )
        return cursor.fetchone() is not None


def _fk_exists(connection: psycopg.Connection, table_name: str, constraint: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass
              AND conname = %s
              AND contype = 'f'
            """,
            (f"public.{table_name}", constraint),
        )
        return cursor.fetchone() is not None


def _partial_index_predicate(connection: psycopg.Connection, index_name: str) -> str:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT pg_get_expr(pg_index.indpred, pg_index.indrelid)
            FROM pg_catalog.pg_index
            JOIN pg_catalog.pg_class ON pg_class.oid = pg_index.indexrelid
            WHERE pg_class.relname = %s
            """,
            (index_name,),
        )
        row = cursor.fetchone()
        assert row is not None
        return str(row[0])
