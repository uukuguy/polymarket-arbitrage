"""Contracts for revision 038 recurring Quote generation lineage."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/038_m1_recurring_quote_generations.py")


def test_revision_038_declares_exact_lineage_and_narrow_qualification_grant() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "038"' in text
    assert 'down_revision = "037"' in text
    assert "m1_quote_generation_inputs" in text
    assert "fk_m1_quote_generation_inputs_structure" in text
    assert "legacy Quote manifest lacks exact Structure lineage" in text
    assert "GRANT SELECT ON TABLE public.m1_quote_generation_inputs" in text
    assert "GRANT INSERT" not in text
    assert "GRANT UPDATE" not in text
    assert "GRANT DELETE" not in text


def test_revision_038_backfills_legacy_lineage_and_round_trips() -> None:
    if (
        subprocess.run(
            ["docker", "--context", "orbstack", "info"],
            capture_output=True,
            text=True,
            timeout=5,
        ).returncode
        != 0
    ):
        pytest.fail("OrbStack Docker daemon unavailable; cannot prove revision 038")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "037")
        structure_digest = "a" * 64
        universe_hash = "b" * 64
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                """
                INSERT INTO public.m1_jobs (
                    job_key, job_type, input_identity, state, created_at, updated_at
                ) VALUES
                    ('structure-job', 'structure-certify', 'structure-input',
                     'succeeded', now(), now()),
                    ('quote-job', 'quote-certify', 'quote-input',
                     'succeeded', now(), now())
                """
            )
            admin.execute(
                """
                INSERT INTO public.m1_generation_manifests (
                    generation_key, producer_job_key, input_digest, artifact_key,
                    artifact_digest, record_count, published_at
                ) VALUES
                    (%s, %s, %s, %s, %s, 1, now()),
                    (%s, %s, %s, %s, %s, 1, now())
                """,
                (
                    f"structure:{structure_digest}",
                    "structure-job",
                    "c" * 64,
                    "structure-artifact",
                    "d" * 64,
                    f"quote:{structure_digest}",
                    "quote-job",
                    universe_hash,
                    "quote-artifact",
                    "e" * 64,
                ),
            )

        _run_alembic(dsn, "upgrade", "038")
        with psycopg.connect(dsn) as connection:
            row = connection.execute(
                """
                SELECT generation_key, structure_generation_key, universe_hash,
                       cadence_seconds, cadence_bucket
                FROM public.m1_quote_generation_inputs
                """
            ).fetchone()
            assert row == (
                f"quote:{structure_digest}",
                f"structure:{structure_digest}",
                universe_hash,
                None,
                None,
            )
            privilege = connection.execute(
                """
                SELECT has_table_privilege(
                    'm1_qualification_worker_capability',
                    'public.m1_quote_generation_inputs',
                    'SELECT'
                )
                """
            ).fetchone()
            assert privilege == (True,)

        _run_alembic(dsn, "downgrade", "037")
        with psycopg.connect(dsn) as connection:
            exists = connection.execute(
                "SELECT to_regclass('public.m1_quote_generation_inputs')"
            ).fetchone()
            assert exists == (None,)


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
