"""Contracts for revision 037 runtime-event-writer outbox authority."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/037_m1_runtime_event_writer_outbox.py")


def test_revision_037_grants_only_the_missing_outbox_authority() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "037"' in text
    assert 'down_revision = "036"' in text
    assert "m1_runtime_event_writer" in text
    assert "GRANT SELECT, INSERT ON TABLE public.m1_alert_outbox" in text
    assert "REVOKE SELECT, INSERT ON TABLE public.m1_alert_outbox" in text
    assert "GRANT UPDATE" not in text
    assert "GRANT DELETE" not in text


def test_revision_037_round_trips_writer_authority_on_real_postgres() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 037")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "036")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute("CREATE ROLE m1_runtime_event_writer LOGIN")

        _run_alembic(dsn, "upgrade", "037")
        with psycopg.connect(dsn) as connection:
            assert _privileges(connection) == (True, True, False, False)

        _run_alembic(dsn, "downgrade", "036")
        with psycopg.connect(dsn) as connection:
            assert _privileges(connection) == (False, False, False, False)


def _privileges(connection: psycopg.Connection[object]) -> tuple[bool, bool, bool, bool]:
    row = connection.execute(
        """
        SELECT has_table_privilege('m1_runtime_event_writer', 'public.m1_alert_outbox', 'SELECT'),
               has_table_privilege('m1_runtime_event_writer', 'public.m1_alert_outbox', 'INSERT'),
               has_table_privilege('m1_runtime_event_writer', 'public.m1_alert_outbox', 'UPDATE'),
               has_table_privilege('m1_runtime_event_writer', 'public.m1_alert_outbox', 'DELETE')
        """
    ).fetchone()
    assert row is not None
    return row


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
