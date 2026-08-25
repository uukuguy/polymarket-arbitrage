"""Contracts for immutable rolling qualification persistence."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import cast

import psycopg
import pytest
from psycopg.types.json import Jsonb

MIGRATION_PATH = Path("alembic/versions/024_m1_rolling_qualification.py")


def test_024_chains_after_023_and_declares_qualification_tables() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "024"' in text
    assert 'down_revision = "023"' in text
    assert '"m1_qualification_epochs"' in text
    assert '"m1_qualification_certificates"' in text
    assert "ck_m1_qualification_epochs_state" in text
    assert "m1_qualification_certificates_immutable" in text


def test_024_schema_declares_state_version_cas_and_certificate_uniqueness() -> None:
    text = MIGRATION_PATH.read_text()

    assert '"version"' in text
    assert "version > 0" in text
    assert "ACCUMULATING" not in text
    assert "state IN ('accumulating', 'invalidated', 'recovering', 'qualified')" in text
    assert "uq_m1_qualification_active_identity" in text
    assert "uq_m1_qualification_certificates_identity" in text
    assert "uq_m1_qualification_certificates_digest" in text
    assert "clock_timestamp()" in text


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5).returncode
            == 0
        )
    except OSError:
        return False


def _run_alembic(dsn: str, *args: str) -> None:
    result = subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr


def _create_supabase_roles(dsn: str) -> None:
    with psycopg.connect(dsn, autocommit=True) as connection:
        for role in ("anon", "authenticated", "service_role"):
            connection.execute(f"CREATE ROLE {role} NOLOGIN")


def test_024_upgrades_from_023_downgrades_and_reupgrades_with_append_only_trigger() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove real 023<->024 migration contract")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = postgres.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if dsn.startswith(prefix):
                dsn = "postgresql://" + dsn[len(prefix) :]
        _create_supabase_roles(dsn)

        _run_alembic(dsn, "upgrade", "023")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_qualification_epochs")

        _run_alembic(dsn, "upgrade", "024")
        with psycopg.connect(dsn) as connection:
            assert _table_exists(connection, "m1_qualification_epochs")
            assert _table_exists(connection, "m1_qualification_certificates")
            assert _check_exists(
                connection,
                "m1_qualification_epochs",
                "ck_m1_qualification_epochs_state",
            )
            assert _check_exists(
                connection,
                "m1_qualification_epochs",
                "ck_m1_qualification_epochs_terminal_fields",
            )
            assert _trigger_exists(
                connection,
                "m1_qualification_certificates",
                "m1_qualification_certificates_immutable",
            )
            _insert_epoch_and_certificate(connection)
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                connection.execute(
                    "UPDATE m1_qualification_certificates SET evidence_digest = %s",
                    ("b" * 64,),
                )
            connection.rollback()
            with pytest.raises(psycopg.errors.RaiseException, match="append-only"):
                connection.execute(
                    "DELETE FROM m1_qualification_certificates WHERE epoch_id = %s",
                    ("epoch-024",),
                )
            connection.rollback()

        _run_alembic(dsn, "downgrade", "023")
        with psycopg.connect(dsn) as connection:
            assert not _table_exists(connection, "m1_qualification_certificates")
            assert not _table_exists(connection, "m1_qualification_epochs")
            assert _table_exists(connection, "m1_recovery_actions")

        _run_alembic(dsn, "upgrade", "024")
        with psycopg.connect(dsn) as connection:
            assert _table_exists(connection, "m1_qualification_epochs")
            assert _trigger_exists(
                connection,
                "m1_qualification_certificates",
                "m1_qualification_certificates_immutable",
            )


def _insert_epoch_and_certificate(connection: psycopg.Connection[object]) -> None:
    identity = {
        "config_id": "config-a",
        "epoch_id": "epoch-024",
        "policy_version": "m1-rolling-qualification-v1",
        "release_id": "release-a",
        "role_identity": ["m1"],
    }
    bounds = {
        "max_gap_seconds": 900,
        "qualified_at": "2030-01-02T00:00:00+00:00",
        "required_seconds": 86400,
        "started_at": "2030-01-01T00:00:00+00:00",
    }
    payload = {
        "bounds": bounds,
        "contained_incidents": [],
        "counts": {"progress_count": 10, "successful_count": 10},
        "evidence_digest": "e" * 64,
        "identity": identity,
        "policy_version": "m1-rolling-qualification-v1",
        "recovery_actions": [],
        "slo": {"freshness": "pass"},
    }
    connection.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, last_fact_at, qualified_at,
            fact_digests, contained_recoveries, coverage_seconds, max_gap_seconds
        ) VALUES (
            'epoch-024', 'qualified', 2, 'identity-024',
            'm1-rolling-qualification-v1', 'release-a', 'config-a',
            %s, '2030-01-01T00:00:00+00:00', '2030-01-02T00:00:00+00:00',
            '2030-01-02T00:00:00+00:00', %s, %s, 86400, 900
        )
        """,
        (Jsonb(["m1"]), Jsonb([]), Jsonb([])),
    )
    connection.execute(
        """
        INSERT INTO m1_qualification_certificates (
            certificate_id, epoch_id, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, qualified_at, payload,
            payload_sha256, certificate_digest, evidence_digest
        ) VALUES (
            'certificate-024', 'epoch-024', 'certificate-identity-024',
            'm1-rolling-qualification-v1', 'release-a', 'config-a', %s,
            '2030-01-01T00:00:00+00:00', '2030-01-02T00:00:00+00:00',
            %s, %s, %s, %s
        )
        """,
        (Jsonb(["m1"]), Jsonb(payload), "a" * 64, "a" * 64, "e" * 64),
    )
    connection.commit()


def _table_exists(connection: psycopg.Connection[object], table_name: str) -> bool:
    with connection.cursor() as cursor:
        cursor.execute("SELECT to_regclass(%s) IS NOT NULL", (f"public.{table_name}",))
        row = cursor.fetchone()
        if row is None:
            return False
        return bool(cast(tuple[object], row)[0])


def _check_exists(
    connection: psycopg.Connection[object],
    table_name: str,
    constraint: str,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_constraint
            WHERE conrelid = %s::regclass
              AND conname = %s
              AND contype = 'c'
            """,
            (f"public.{table_name}", constraint),
        )
        return cursor.fetchone() is not None


def _trigger_exists(
    connection: psycopg.Connection[object],
    table_name: str,
    trigger_name: str,
) -> bool:
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT 1
            FROM pg_catalog.pg_trigger
            WHERE tgrelid = %s::regclass
              AND tgname = %s
              AND NOT tgisinternal
            """,
            (f"public.{table_name}", trigger_name),
        )
        return cursor.fetchone() is not None
