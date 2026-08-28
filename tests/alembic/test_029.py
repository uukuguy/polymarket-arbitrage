"""Contracts for revision 029 bounded qualification status projections."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

MIGRATION_PATH = Path("alembic/versions/029_m1_bounded_qualification_status.py")


def test_029_persists_only_bounded_generated_status_projections() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "029"' in text
    assert 'down_revision = "028"' in text
    assert "status_last_fact_record" in text
    assert "status_recovery_count" in text
    assert "status_recent_recoveries" in text
    assert "GENERATED ALWAYS AS" in text
    assert "fact_records -> -1" in text
    assert "jsonb_array_length(contained_recoveries)" in text
    assert "$[last - 19 to last]" in text
    assert "STORED" in text


def test_029_real_upgrade_backfills_and_tracks_bounded_status_columns() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 028<->029 migration")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))

        _run_alembic(dsn, "upgrade", "028")
        recoveries = [f"recovery-{index:02d}" for index in range(30)]
        fact_records = [{"fact": index} for index in range(50)]
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_qualification_epochs (
                    epoch_id, state, identity_key, policy_version, release_id,
                    config_id, role_identity, started_at, contained_recoveries,
                    fact_records
                ) VALUES (
                    'epoch-029', 'accumulating', 'identity-029', 'policy-029',
                    'release-029', 'config-029', '["structure"]'::jsonb,
                    clock_timestamp(), %s, %s
                )
                """,
                (Jsonb(recoveries), Jsonb(fact_records)),
            )

        _run_alembic(dsn, "upgrade", "029")
        with psycopg.connect(dsn) as connection:
            row = connection.execute(
                """
                SELECT status_last_fact_record, status_recovery_count,
                       status_recent_recoveries
                FROM m1_qualification_epochs
                WHERE epoch_id = 'epoch-029'
                """
            ).fetchone()
            assert row == (fact_records[-1], 30, recoveries[-20:])
            connection.execute(
                """
                UPDATE m1_qualification_epochs
                SET fact_records = fact_records || %s,
                    contained_recoveries = contained_recoveries || %s
                WHERE epoch_id = 'epoch-029'
                """,
                (Jsonb([{"fact": 50}]), Jsonb(["recovery-30"])),
            )
            row = connection.execute(
                """
                SELECT status_last_fact_record, status_recovery_count,
                       status_recent_recoveries
                FROM m1_qualification_epochs
                WHERE epoch_id = 'epoch-029'
                """
            ).fetchone()
            assert row == ({"fact": 50}, 31, recoveries[-19:] + ["recovery-30"])

        _run_alembic(dsn, "downgrade", "028")
        _run_alembic(dsn, "upgrade", "029")


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
