"""Contracts for revision 035 episode-scoped recovery budgets."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/035_m1_recovery_budget_episodes.py")


def test_revision_035_keys_recovery_budgets_by_episode() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "035"' in text
    assert 'down_revision = "034"' in text
    assert '"episode_key"' in text
    assert 'server_default="legacy"' in text
    assert '"controller_id", "target_type", "target_id", "episode_key"' in text
    assert "MIN(remaining_actions)" in text


def test_revision_035_preserves_legacy_exhaustion_and_round_trips_on_real_postgres() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 035")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "034")
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
                ) VALUES (
                    'controller', 'circuit', 'target', 3, 0, NULL,
                    clock_timestamp(), clock_timestamp()
                )
                """
            )

        _run_alembic(dsn, "upgrade", "035")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT episode_key, remaining_actions
                FROM m1_recovery_target_budgets
                WHERE controller_id = 'controller' AND target_id = 'target'
                """
            ).fetchone() == ("legacy", 0)
            connection.execute(
                """
                INSERT INTO m1_recovery_target_budgets (
                    controller_id, target_type, target_id, episode_key,
                    max_actions, remaining_actions
                ) VALUES ('controller', 'circuit', 'target', %s, 3, 2)
                """,
                ("sha256:" + "a" * 64,),
            )
            assert connection.execute(
                """
                SELECT privilege_type
                FROM information_schema.role_table_grants
                WHERE grantee = 'm1_runtime_controller_capability'
                  AND table_name = 'm1_recovery_target_budgets'
                ORDER BY privilege_type
                """
            ).fetchall() == [("INSERT",), ("SELECT",), ("UPDATE",)]

        _run_alembic(dsn, "downgrade", "034")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT remaining_actions
                FROM m1_recovery_target_budgets
                WHERE controller_id = 'controller' AND target_id = 'target'
                """
            ).fetchone() == (0,)
            assert connection.execute(
                """
                SELECT count(*) FROM information_schema.columns
                WHERE table_name = 'm1_recovery_target_budgets'
                  AND column_name = 'episode_key'
                """
            ).fetchone() == (0,)

        _run_alembic(dsn, "upgrade", "035")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT episode_key, remaining_actions
                FROM m1_recovery_target_budgets
                WHERE controller_id = 'controller' AND target_id = 'target'
                """
            ).fetchone() == ("legacy", 0)


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
