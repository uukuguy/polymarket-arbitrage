"""Contracts for revision 033 failure-identity circuit semantics."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/033_m1_failure_identity_circuits.py")


def test_revision_033_adds_and_backfills_failure_identity() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "033"' in text
    assert 'down_revision = "032"' in text
    assert '"failure_fingerprint"' in text
    assert "legacy:" in text
    assert "m1_job_circuits_failure_identity" in text
    assert "drop_constraint" in text
    assert "drop_column" in text


def test_revision_033_backfills_and_downgrades_on_real_postgres() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 033")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "032")
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_jobs (
                    job_key, job_type, input_identity, state, last_error_class,
                    created_at, updated_at
                ) VALUES (
                    'revision-033-job', 'structure-fetch', 'revision-033-input',
                    'retryable', 'TimeoutError', clock_timestamp(), clock_timestamp()
                )
                """
            )
            connection.execute(
                """
                INSERT INTO m1_job_circuits (
                    job_key, consecutive_failures, state, opened_at,
                    next_probe_at, updated_at
                ) VALUES (
                    'revision-033-job', 3, 'open', clock_timestamp(),
                    clock_timestamp(), clock_timestamp()
                )
                """
            )

        _run_alembic(dsn, "upgrade", "033")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT consecutive_failures, state, failure_fingerprint
                FROM m1_job_circuits WHERE job_key = 'revision-033-job'
                """
            ).fetchone() == (3, "open", "legacy:TimeoutError")

        _run_alembic(dsn, "downgrade", "032")
        with psycopg.connect(dsn) as connection:
            columns = connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'm1_job_circuits'
                """
            ).fetchall()
            assert ("failure_fingerprint",) not in columns


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
