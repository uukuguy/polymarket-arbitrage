"""Contracts for revision 039 run-scoped Quote admission identity."""

import os
import subprocess
from pathlib import Path

import psycopg
import pytest
from psycopg import sql

MIGRATION = Path("alembic/versions/039_m1_quote_run_admission_identity.py")


def test_revision_039_rekeys_admission_uniqueness_from_structure_to_quote_run() -> None:
    text = MIGRATION.read_text()

    assert 'revision = "039"' in text
    assert 'down_revision = "038"' in text
    assert "quote_generation_key" in text
    assert "DROP CONSTRAINT" not in text.upper()
    assert "uq_m1_quote_admission_input_generation" in text
    assert "uq_m1_quote_admission_input_quote_generation" in text
    assert "m1_quote_admission_inputs_structure" in text
    assert "'^quote:[0-9a-f]{64}$'" in text


def test_revision_039_backfills_and_allows_many_quote_runs_per_structure() -> None:
    if (
        subprocess.run(
            ["docker", "--context", "orbstack", "info"],
            capture_output=True,
            text=True,
            timeout=5,
        ).returncode
        != 0
    ):
        pytest.fail("OrbStack Docker daemon unavailable; cannot prove revision 039")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        _run_alembic(dsn, "upgrade", "038")
        structure_digest = "a" * 64
        structure_generation = f"structure:{structure_digest}"
        legacy_quote = f"quote:{structure_digest}"
        recurring_quote = f"quote:{'b' * 64}"
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                """
                INSERT INTO public.m1_jobs (
                    job_key, job_type, input_identity, state, created_at, updated_at
                ) VALUES
                    ('structure-job', 'structure-certify', 'structure-input',
                     'succeeded', now(), now()),
                    ('legacy-admit', 'quote-admit', 'legacy-input',
                     'succeeded', now(), now())
                """
            )
            admin.execute(
                """
                INSERT INTO public.m1_structure_generation_inputs (
                    generation_key, bundle_key, bundle_digest, identity, admitted_at
                ) VALUES (%s, 'bundle', %s, '{}'::jsonb, now())
                """,
                (structure_generation, structure_digest),
            )
            admin.execute(
                """
                INSERT INTO public.m1_generation_manifests (
                    generation_key, producer_job_key, input_digest, artifact_key,
                    artifact_digest, record_count, published_at
                ) VALUES (%s, 'structure-job', %s, 'artifact', %s, 1, now())
                """,
                (structure_generation, "c" * 64, "d" * 64),
            )
            admin.execute(
                """
                INSERT INTO public.m1_quote_admission_inputs (
                    job_key, generation_key, bundle_key, bundle_digest, admitted_at
                ) VALUES ('legacy-admit', %s, 'bundle', %s, now())
                """,
                (structure_generation, structure_digest),
            )

        _run_alembic(dsn, "upgrade", "039")
        with psycopg.connect(dsn, autocommit=True) as admin:
            assert admin.execute(
                """
                SELECT quote_generation_key
                FROM public.m1_quote_admission_inputs
                WHERE job_key = 'legacy-admit'
                """
            ).fetchone() == (legacy_quote,)
            admin.execute(
                """
                INSERT INTO public.m1_jobs (
                    job_key, job_type, input_identity, state, created_at, updated_at
                ) VALUES ('recurring-admit', 'quote-admit', 'recurring-input',
                          'runnable', now(), now())
                """
            )
            admin.execute(
                """
                INSERT INTO public.m1_quote_admission_inputs (
                    job_key, generation_key, bundle_key, bundle_digest,
                    quote_generation_key, admitted_at
                ) VALUES ('recurring-admit', %s, 'bundle', %s, %s, now())
                """,
                (structure_generation, structure_digest, recurring_quote),
            )
            rows = admin.execute(
                """
                SELECT generation_key, quote_generation_key
                FROM public.m1_quote_admission_inputs
                ORDER BY job_key
                """
            ).fetchall()
            assert rows == [
                (structure_generation, legacy_quote),
                (structure_generation, recurring_quote),
            ]
            admin.execute(
                "DELETE FROM public.m1_quote_admission_inputs WHERE job_key = 'recurring-admit'"
            )

        _run_alembic(dsn, "downgrade", "038")
        with psycopg.connect(dsn) as connection:
            columns = connection.execute(
                """
                SELECT column_name FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'm1_quote_admission_inputs'
                  AND column_name = 'quote_generation_key'
                """
            ).fetchall()
            assert columns == []


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
