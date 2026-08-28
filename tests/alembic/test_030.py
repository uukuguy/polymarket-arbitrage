"""Contracts for revision 030 normalized qualification facts."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

MIGRATION = Path("alembic/versions/030_m1_normalized_qualification_facts.py")


def test_revision_030_is_append_only_and_avoids_digest_extension_calls() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "030"' in text
    assert 'down_revision = "029"' in text
    assert "m1_qualification_epoch_facts" in text
    assert "WITH ORDINALITY" in text
    assert "runtime_fact_count" in text
    assert "runtime_contained_recovery_count" in text
    assert "append-only" in text
    assert "GRANT SELECT, INSERT" in text
    assert "digest(" not in text
    assert "public.digest" not in text


def test_revision_030_backfills_ordered_facts_and_rejects_mutation() -> None:
    if (
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
        != 0
    ):
        pytest.fail("Docker daemon unavailable; cannot prove revision 030")
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
                    "observed_at": f"2026-08-28T00:{index // 60:02d}:{index % 60:02d}+00:00",
                    "source_rank": 40,
                    "stable_id": f"fact-{index}",
                    "ingest_seq": index + 1,
                },
                "fact": {
                    "fact_id": f"fact-{index}",
                    "observed_at": f"2026-08-28T00:{index // 60:02d}:{index % 60:02d}+00:00",
                    "reason": "recovery.retry" if index in {1, 500} else "healthy",
                },
                "source": "freshness",
            }
            for index in range(501)
        ]
        with psycopg.connect(dsn) as connection:
            connection.execute(
                """
                INSERT INTO m1_qualification_epochs (
                    epoch_id, state, identity_key, policy_version, release_id,
                    config_id, role_identity, started_at, fact_records,
                    contained_recoveries
                ) VALUES (
                    'epoch-030', 'accumulating', 'identity-030', 'policy-030',
                    'release-030', 'config-030', '["structure"]'::jsonb,
                    '2026-08-28T00:00:00+00:00', %s, %s
                )
                """,
                (Jsonb(records), Jsonb(["fact-1", "fact-500"])),
            )
        _run_alembic(dsn, "upgrade", "030")
        with psycopg.connect(dsn) as connection:
            assert connection.execute(
                """
                SELECT runtime_fact_count, runtime_contained_recovery_count
                FROM m1_qualification_epochs WHERE epoch_id = 'epoch-030'
                """
            ).fetchone() == (501, 2)
            assert connection.execute(
                """
                SELECT min(ordinal), max(ordinal), count(*)
                FROM m1_qualification_epoch_facts WHERE epoch_id = 'epoch-030'
                """
            ).fetchone() == (1, 501, 501)
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                connection.execute(
                    """
                    UPDATE m1_qualification_epoch_facts SET reason = 'healthy'
                    WHERE epoch_id = 'epoch-030' AND ordinal = 2
                    """
                )
            connection.rollback()
        _run_alembic(dsn, "downgrade", "029")
        _run_alembic(dsn, "upgrade", "030")


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
