"""Contracts for the runtime observe-only decision ledger."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg.types.json import Jsonb

MIGRATION_PATH = Path("alembic/versions/025_m1_runtime_observe_decisions.py")


def test_025_chains_after_024_and_declares_append_only_observe_ledger() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "025"' in text
    assert 'down_revision = "024"' in text
    assert '"m1_runtime_observe_decisions"' in text
    assert '"controller_owner_id"' in text
    assert '"controller_epoch"' in text
    assert "ck_m1_runtime_observe_controller_identity" in text
    assert "decision_kind IN ('decision', 'idle')" in text
    assert "reason_code IN (" in text
    assert "action_type IS NULL OR action_type IN (" in text
    assert "payload_sha256 ~ '^[0-9a-f]{64}$'" in text
    assert "runtime_state_digest IS NULL OR runtime_state_digest ~ '^[0-9a-f]{64}$'" in text
    assert "decision_digest ~ '^[0-9a-f]{64}$'" in text
    assert "uq_m1_runtime_observe_idempotency" in text
    assert "m1_runtime_observe_decisions_immutable" in text
    assert "ForeignKeyConstraint" not in text
    assert "uq_m1_runtime_controller_leases_identity_epoch" not in text
    assert "m1_qualification" not in text


def test_025_declares_scoped_role_grants_without_recovery_mutation_grant() -> None:
    text = MIGRATION_PATH.read_text()

    assert "REVOKE ALL ON TABLE m1_runtime_observe_decisions FROM PUBLIC" in text
    assert "GRANT SELECT, INSERT ON TABLE m1_runtime_observe_decisions TO service_role" in text
    assert "GRANT SELECT ON TABLE m1_runtime_observe_decisions TO authenticated" in text
    assert "GRANT UPDATE" not in text
    assert "GRANT DELETE" not in text
    assert "GRANT INSERT ON TABLE m1_recovery_actions" not in text
    assert "CREATE TRIGGER m1_runtime_observe_decisions_immutable" in text
    assert 'op.drop_table("m1_runtime_observe_decisions")' in text
    assert "DROP FUNCTION IF EXISTS m1_runtime_observe_decisions_reject_mutation" in text


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


def test_025_upgrades_from_024_downgrades_and_preserves_recovery_actions() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 024<->025 migration contract")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if dsn.startswith(prefix):
                dsn = "postgresql://" + dsn[len(prefix) :]
        _create_supabase_roles(dsn)

        _run_alembic(dsn, "upgrade", "024")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_runtime_observe_decisions")
            assert _table_exists(connection, "m1_recovery_actions")

        _run_alembic(dsn, "upgrade", "025")
        with psycopg.connect(dsn) as connection:
            assert _table_exists(connection, "m1_runtime_observe_decisions")
            assert _trigger_exists(
                connection,
                "m1_runtime_observe_decisions",
                "m1_runtime_observe_decisions_immutable",
            )
            assert _grant_exists(
                connection,
                "m1_runtime_observe_decisions",
                "authenticated",
                "SELECT",
            )
            assert _grant_exists(
                connection,
                "m1_runtime_observe_decisions",
                "service_role",
                "INSERT",
            )
            assert not _grant_exists(
                connection,
                "m1_runtime_observe_decisions",
                "authenticated",
                "INSERT",
            )
            _insert_controller_lease(connection)
            _insert_observe_row(connection)
            _advance_controller_lease(connection)
            assert _observe_row_count(connection) == 1
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                connection.execute(
                    "UPDATE m1_runtime_observe_decisions SET reason_code = 'job.healthy'"
                )
            connection.rollback()
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                connection.execute("DELETE FROM m1_runtime_observe_decisions")
            connection.rollback()

        _run_alembic(dsn, "downgrade", "024")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_runtime_observe_decisions")
            assert _table_exists(connection, "m1_recovery_actions")


def _insert_observe_row(connection: psycopg.Connection) -> None:
    payload = {
        "controller_id": "m1-runtime-reconciler",
        "controller_owner_id": "runtime-controller-observe",
        "controller_epoch": 3,
        "decision_kind": "idle",
        "observed_at": "2026-08-25T00:00:00+00:00",
        "reason_code": "job.healthy",
    }
    connection.execute(
        """
        INSERT INTO m1_runtime_observe_decisions (
            decision_id, idempotency_key, controller_id, controller_owner_id,
            controller_epoch, observed_at, decision_kind, target_type, target_id,
            action_type, reason_code, incident_severity, qualification_breaking,
            next_check_at, runtime_state_digest, decision_digest, payload, payload_sha256
        ) VALUES (
            'runtime-observe:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'runtime-observe-idempotency:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'm1-runtime-reconciler',
            'runtime-controller-observe',
            3,
            '2026-08-25T00:00:00+00:00',
            'idle',
            NULL,
            NULL,
            NULL,
            'job.healthy',
            'warning',
            false,
            '2026-08-25T00:01:00+00:00',
            NULL,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            %s,
            'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
        )
        """,
        (Jsonb(payload),),
    )
    connection.commit()


def _insert_controller_lease(connection: psycopg.Connection) -> None:
    connection.execute(
        """
        INSERT INTO m1_runtime_controller_leases (
            controller_id, owner_id, lease_epoch, lease_expires_at, claimed_at, updated_at
        ) VALUES (
            'm1-runtime-reconciler',
            'runtime-controller-observe',
            3,
            '2026-08-25T00:05:00+00:00',
            '2026-08-25T00:00:00+00:00',
            '2026-08-25T00:00:00+00:00'
        )
        """
    )
    connection.commit()


def _advance_controller_lease(connection: psycopg.Connection) -> None:
    connection.execute(
        """
        UPDATE m1_runtime_controller_leases
        SET owner_id = 'runtime-controller-next',
            lease_epoch = 4,
            lease_expires_at = '2026-08-25T00:06:00+00:00',
            updated_at = '2026-08-25T00:01:00+00:00'
        WHERE controller_id = 'm1-runtime-reconciler'
        """
    )
    connection.commit()


def _observe_row_count(connection: psycopg.Connection) -> int:
    with connection.cursor() as cursor:
        cursor.execute("SELECT COUNT(*) FROM m1_runtime_observe_decisions")
        row = cursor.fetchone()
        assert row is not None
        return int(row[0])


def _table_exists(connection: psycopg.Connection, table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
        row = cursor.fetchone()
        return bool(row and row[0])


def _trigger_exists(connection: psycopg.Connection, table_name: str, trigger_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_trigger
            WHERE tgrelid = %s::regclass
              AND tgname = %s
              AND NOT tgisinternal
            """,
            (f"public.{table_name}", trigger_name),
        )
        return cursor.fetchone() is not None


def _grant_exists(
    connection: psycopg.Connection,
    table_name: str,
    grantee: str,
    privilege: str,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT has_table_privilege(%s, %s, %s)
            """,
            (grantee, f"public.{table_name}", privilege),
        )
        row = cursor.fetchone()
        return bool(row and row[0])
