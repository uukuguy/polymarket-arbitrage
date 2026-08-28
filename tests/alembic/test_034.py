"""Contracts for revision 034 single-authority circuit timing."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/034_m1_single_authority_circuit_timing.py")


def test_revision_034_clears_only_duplicated_circuit_budget_deadlines() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "034"' in text
    assert 'down_revision = "033"' in text
    assert "target_type = 'circuit'" in text
    assert "last_next_allowed_at = NULL" in text


def test_revision_034_preserves_job_cooldown_on_real_postgres() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 034")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "033")
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_runtime_controller_leases (
                    controller_id, owner_id, lease_epoch, lease_expires_at
                ) VALUES ('controller', 'owner', 1, clock_timestamp() + interval '1 minute')
                """
            )
            connection.execute(
                """
                INSERT INTO m1_recovery_target_budgets (
                    controller_id, target_type, target_id, max_actions,
                    remaining_actions, last_next_allowed_at, created_at, updated_at
                ) VALUES
                    ('controller', 'circuit', 'circuit-target', 3, 1,
                     clock_timestamp() + interval '16 hours', clock_timestamp(), clock_timestamp()),
                    ('controller', 'job', 'job-target', 3, 1,
                     clock_timestamp() + interval '1 minute', clock_timestamp(), clock_timestamp())
                """
            )

        _run_alembic(dsn, "upgrade", "034")
        with psycopg.connect(dsn) as connection:
            rows = connection.execute(
                """
                SELECT target_type, last_next_allowed_at IS NULL
                FROM m1_recovery_target_budgets
                ORDER BY target_type
                """
            ).fetchall()
            assert rows == [("circuit", True), ("job", False)]

        _run_alembic(dsn, "downgrade", "033")


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
