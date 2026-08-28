"""Contracts for revision 028 runtime policy snapshots."""

from __future__ import annotations

import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION_PATH = Path("alembic/versions/028_m1_runtime_policy_snapshot.py")


def test_028_adds_and_removes_exact_runtime_policy_snapshot() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "028"' in text
    assert 'down_revision = "027"' in text
    for column in (
        "policy_version",
        "profile_lease_seconds",
        "profile_heartbeat_seconds",
        "profile_progress_seconds",
        "profile_attempt_seconds",
    ):
        assert f'"{column}"' in text
        assert 'drop_column("m1_job_runtime_state", name)' in text
    assert "ck_m1_runtime_state_policy_profile" in text
    assert "lease_deadline_at - last_heartbeat_at" in text
    assert 'alter_column("m1_job_runtime_state", "policy_version", server_default=None)' in text
    assert 'alter_column("m1_job_runtime_state", name, server_default=None)' in text
    assert "checkpoint_sequence" in text
    assert "uq_m1_checkpoint_receipts_job_sequence" in text
    assert "ck_m1_checkpoint_receipts_running_sequence" in text


def test_028_closes_only_superseded_running_attempts() -> None:
    text = MIGRATION_PATH.read_text()

    assert "SupersededLeaseBackfill" in text
    assert "job.superseded-running-attempt" in text
    assert "attempt.state = 'running'" in text
    assert "runtime.attempt_id = attempt.attempt_id" in text
    assert "NOT EXISTS" in text
    assert "UPDATE m1_jobs AS job" in text
    assert "runtime.lease_epoch = job.lease_epoch" in text
    assert "lease_owner = NULL" in text
    assert "lease_expires_at = NULL" in text


def test_028_real_upgrade_releases_current_orphan_and_drops_policy_defaults() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 027<->028 migration")

    from testcontainers.postgres import PostgresContainer

    now = datetime(2026, 8, 28, 0, 0, tzinfo=UTC)
    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))

        _run_alembic(dsn, "upgrade", "027")
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_jobs (
                    job_key, job_type, input_identity, state, lease_owner,
                    lease_epoch, lease_expires_at, attempt_count, created_at, updated_at
                ) VALUES (
                    'orphan-current-028', 'quote-batch', 'orphan-current-028',
                    'leased', 'old-worker', 1, %s, 1, %s, %s
                )
                """,
                (now - timedelta(seconds=1), now, now),
            )
            connection.execute(
                """
                INSERT INTO m1_job_attempts (
                    attempt_id, job_key, lease_epoch, worker_id, state, started_at
                ) VALUES (
                    'orphan-attempt-028', 'orphan-current-028', 1,
                    'old-worker', 'running', %s
                )
                """,
                (now,),
            )

        _run_alembic(dsn, "upgrade", "028")
        with psycopg.connect(dsn) as connection:
            job = connection.execute(
                """
                SELECT state, lease_owner, lease_expires_at, last_error_class
                FROM m1_jobs WHERE job_key = 'orphan-current-028'
                """
            ).fetchone()
            assert job == ("retryable", None, None, "SupersededLeaseBackfill")
            attempt = connection.execute(
                """
                SELECT state, finished_at IS NOT NULL, error_class, error_detail
                FROM m1_job_attempts WHERE attempt_id = 'orphan-attempt-028'
                """
            ).fetchone()
            assert attempt is not None
            assert attempt[:3] == ("retryable", True, "SupersededLeaseBackfill")
            assert attempt[3]["reason_code"] == "job.superseded-running-attempt"
            defaults = connection.execute(
                """
                SELECT column_name, column_default
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'm1_job_runtime_state'
                  AND column_name IN (
                      'policy_version', 'profile_lease_seconds',
                      'profile_heartbeat_seconds', 'profile_progress_seconds',
                      'profile_attempt_seconds'
                  )
                ORDER BY column_name
                """
            ).fetchall()
            assert len(defaults) == 5
            assert all(default is None for _column, default in defaults)

        _run_alembic(dsn, "downgrade", "027")
        _run_alembic(dsn, "upgrade", "028")


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


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
