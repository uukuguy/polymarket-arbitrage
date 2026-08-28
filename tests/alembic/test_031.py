"""Contracts for revision 031 fixed-size qualification epoch rows."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

MIGRATION = Path("alembic/versions/031_m1_compact_qualification_epochs.py")


def test_revision_031_validates_before_compacting_and_fences_regrowth() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "031"' in text
    assert 'down_revision = "030"' in text
    assert "runtime_fact_count" in text
    assert "runtime_contained_recovery_count" in text
    assert "IS DISTINCT FROM" in text
    assert "jsonb_array_length(fact_records) = 0" in text
    assert "jsonb_array_length(fact_digests) = 0" in text
    assert "jsonb_array_length(contained_recoveries) = 0" in text
    assert "digest(" not in text
    assert "public.digest" not in text


def test_revision_031_fails_closed_before_clearing_then_compacts() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 031")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "029")
        records = [
            {
                "cursor": {
                    "observed_at": f"2026-08-28T00:00:0{index}+00:00",
                    "source_rank": 40,
                    "stable_id": f"fact-{index}",
                    "ingest_seq": index + 1,
                },
                "fact": {
                    "fact_id": f"fact-{index}",
                    "observed_at": f"2026-08-28T00:00:0{index}+00:00",
                    "reason": "recovery.retry" if index == 1 else "healthy",
                },
                "source": "freshness",
            }
            for index in range(3)
        ]
        fact_digests = [[f"fact-{index}", f"digest-{index}"] for index in range(3)]
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_qualification_epochs (
                    epoch_id, state, identity_key, policy_version, release_id,
                    config_id, role_identity, started_at, fact_records,
                    fact_digests, contained_recoveries
                ) VALUES (
                    'epoch-031', 'accumulating', 'identity-031', 'policy-031',
                    'release-031', 'config-031', '["structure"]'::jsonb,
                    '2026-08-28T00:00:00+00:00', %s, %s, '["fact-1"]'::jsonb
                )
                """,
                (Jsonb(records), Jsonb(fact_digests)),
            )
        _run_alembic(dsn, "upgrade", "030")
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                UPDATE m1_qualification_epochs
                SET runtime_fact_count = 2
                WHERE epoch_id = 'epoch-031'
                """
            )

        failed = _run_alembic(dsn, "upgrade", "031", expect_success=False)
        assert "qualification epoch normalized fact count conflicts" in failed.stderr
        with psycopg.connect(dsn) as connection:
            assert connection.execute("SELECT version_num FROM alembic_version").fetchone() == (
                "030",
            )
            assert connection.execute(
                """
                SELECT jsonb_array_length(fact_records),
                       jsonb_array_length(fact_digests),
                       jsonb_array_length(contained_recoveries)
                FROM m1_qualification_epochs WHERE epoch_id = 'epoch-031'
                """
            ).fetchone() == (3, 3, 1)
            connection.execute(
                """
                UPDATE m1_qualification_epochs
                SET runtime_fact_count = 3
                WHERE epoch_id = 'epoch-031'
                """
            )

        _run_alembic(dsn, "upgrade", "031")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT jsonb_array_length(fact_records),
                       jsonb_array_length(fact_digests),
                       jsonb_array_length(contained_recoveries),
                       runtime_fact_count, runtime_contained_recovery_count
                FROM m1_qualification_epochs WHERE epoch_id = 'epoch-031'
                """
            ).fetchone() == (0, 0, 0, 3, 1)
            assert connection.execute(
                """
                SELECT jsonb_agg(fact_record ORDER BY ordinal)
                FROM m1_qualification_epoch_facts WHERE epoch_id = 'epoch-031'
                """
            ).fetchone() == (records,)
            with pytest.raises(psycopg.errors.CheckViolation):
                connection.execute(
                    """
                    UPDATE m1_qualification_epochs
                    SET fact_records = '[{}]'::jsonb
                    WHERE epoch_id = 'epoch-031'
                    """
                )
            connection.rollback()
        _run_alembic(dsn, "downgrade", "030")
        _run_alembic(dsn, "upgrade", "031")


def _normalize_dsn(dsn: str) -> str:
    for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
        if dsn.startswith(prefix):
            return "postgresql://" + dsn[len(prefix) :]
    return dsn


def _run_alembic(
    dsn: str, *args: str, expect_success: bool = True
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=180,
    )
    if expect_success:
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode != 0
    return result
