"""Contracts for revision 036 bounded operator-snapshot indexes."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/036_m1_operator_snapshot_indexes.py")


def test_revision_036_builds_snapshot_indexes_without_blocking_writers() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "036"' in text
    assert 'down_revision = "035"' in text
    assert "autocommit_block" in text
    assert "CREATE INDEX CONCURRENTLY m1_job_attempts_latest" in text
    assert "CREATE INDEX CONCURRENTLY m1_alert_outbox_pending_latest" in text
    assert "DROP INDEX CONCURRENTLY IF EXISTS" in text


def test_revision_036_round_trips_bounded_snapshot_indexes_on_real_postgres() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 036")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "035")
        _run_alembic(dsn, "upgrade", "036")

        with psycopg.connect(dsn) as connection:
            indexes = dict(
                connection.execute(
                    """
                    SELECT indexname, indexdef
                    FROM pg_indexes
                    WHERE schemaname = 'public'
                      AND indexname IN (
                        'm1_job_attempts_latest',
                        'm1_alert_outbox_pending_latest'
                      )
                    ORDER BY indexname
                    """
                ).fetchall()
            )
        assert "started_at DESC, attempt_id DESC" in indexes["m1_job_attempts_latest"]
        assert "created_at DESC, outbox_id DESC" in indexes[
            "m1_alert_outbox_pending_latest"
        ]
        assert "WHERE (state = 'pending'::text)" in indexes[
            "m1_alert_outbox_pending_latest"
        ]

        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_jobs (job_key, job_type, input_identity, state)
                VALUES ('job:index-proof', 'structure-normalize', 'input:index-proof',
                        'succeeded')
                """
            )
            connection.execute(
                """
                INSERT INTO m1_job_attempts (
                    attempt_id, job_key, lease_epoch, worker_id, state, started_at
                )
                SELECT 'attempt:' || value, 'job:index-proof', value, 'worker',
                       'succeeded', clock_timestamp() - make_interval(secs => value)
                FROM generate_series(1, 2000) AS value
                """
            )
            connection.execute(
                """
                INSERT INTO m1_incidents (
                    incident_key, dedupe_key, component, severity, state, summary
                ) VALUES ('incident:index-proof', 'index-proof', 'runtime', 'warning',
                          'open', 'index proof')
                """
            )
            connection.execute(
                """
                INSERT INTO m1_incident_events (
                    incident_event_id, incident_key, kind, detail, idempotency_key,
                    occurred_at
                )
                SELECT 'event:' || value, 'incident:index-proof', 'opened', '{}'::jsonb,
                       'event:' || value, clock_timestamp() - make_interval(secs => value)
                FROM generate_series(1, 2000) AS value
                """
            )
            connection.execute(
                """
                INSERT INTO m1_alert_outbox (
                    outbox_id, incident_event_id, channel, payload, state, created_at
                )
                SELECT 'outbox:' || value, 'event:' || value, 'dashboard', '{}'::jsonb,
                       'pending', clock_timestamp() - make_interval(secs => value)
                FROM generate_series(1, 2000) AS value
                """
            )
            connection.execute("ANALYZE m1_job_attempts")
            connection.execute("ANALYZE m1_alert_outbox")
            attempts_row = connection.execute(
                """
                EXPLAIN (FORMAT JSON)
                SELECT job_key, lease_epoch, worker_id, state, error_class, error_detail
                FROM m1_job_attempts
                ORDER BY started_at DESC, attempt_id DESC LIMIT 5
                """
            ).fetchone()
            assert attempts_row is not None
            attempts_plan = attempts_row[0][0]["Plan"]
            outbox_row = connection.execute(
                """
                EXPLAIN (FORMAT JSON)
                SELECT i.incident_key, o.channel, o.state
                FROM m1_alert_outbox o
                JOIN m1_incident_events e
                  ON e.incident_event_id = o.incident_event_id
                JOIN m1_incidents i ON i.incident_key = e.incident_key
                WHERE o.state = 'pending'
                ORDER BY o.created_at DESC, o.outbox_id DESC LIMIT 5
                """
            ).fetchone()
            assert outbox_row is not None
            outbox_plan = outbox_row[0][0]["Plan"]
        assert "m1_job_attempts_latest" in _plan_indexes(attempts_plan)
        assert "m1_alert_outbox_pending_latest" in _plan_indexes(outbox_plan)

        _run_alembic(dsn, "downgrade", "035")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT count(*) FROM pg_indexes
                WHERE schemaname = 'public'
                  AND indexname IN (
                    'm1_job_attempts_latest',
                    'm1_alert_outbox_pending_latest'
                  )
                """
            ).fetchone() == (0,)


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def _plan_indexes(plan: dict[str, object]) -> set[str]:
    indexes: set[str] = set()
    index_name = plan.get("Index Name")
    if isinstance(index_name, str):
        indexes.add(index_name)
    children = plan.get("Plans", [])
    assert isinstance(children, list)
    for child in children:
        assert isinstance(child, dict)
        indexes.update(_plan_indexes(child))
    return indexes


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, result.stderr
