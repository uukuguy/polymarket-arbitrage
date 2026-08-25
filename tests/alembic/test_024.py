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
    assert '"canonical_payload"' in text
    assert "CREATE EXTENSION IF NOT EXISTS pgcrypto" in text
    assert "m1_verify_qualification_certificate_insert" in text
    assert "m1_insert_qualification_certificate" in text
    assert "p_certificate_id text" not in text
    assert "p_identity_key text" not in text
    assert "REVOKE ALL ON FUNCTION m1_insert_qualification_certificate" in text
    assert "FROM PUBLIC" in text
    assert "REVOKE ALL ON TABLE m1_qualification_certificates" in text
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
                "m1_qualification_certificates_verify_insert",
            )
            assert _trigger_exists(
                connection,
                "m1_qualification_certificates",
                "m1_qualification_certificates_immutable",
            )
            certificate_id, identity_key = _insert_epoch_and_certificate(connection)
            assert certificate_id.startswith("qualification-certificate:")
            assert len(certificate_id) == len("qualification-certificate:") + 64
            assert identity_key == _sha256(
                _canonical({"bounds": _bounds(), "identity": _identity()})
            )
            with pytest.raises(psycopg.errors.RaiseException, match="digest"):
                connection.execute(
                    """
                    INSERT INTO m1_qualification_certificates (
                        certificate_id, epoch_id, identity_key, policy_version, release_id,
                        config_id, role_identity, started_at, qualified_at, payload,
                        canonical_payload, payload_sha256, certificate_digest, evidence_digest
                    )
                    SELECT 'certificate-024-forged', epoch_id, 'forged-identity',
                           policy_version, release_id, config_id, role_identity,
                           started_at, qualified_at, payload, canonical_payload,
                           repeat('0', 64), repeat('0', 64), evidence_digest
                    FROM m1_qualification_certificates
                    WHERE certificate_id = %s
                    """,
                    (certificate_id,),
                )
            connection.rollback()
            with pytest.raises(psycopg.errors.RaiseException, match="id"):
                connection.execute(
                    """
                    INSERT INTO m1_qualification_certificates (
                        certificate_id, epoch_id, identity_key, policy_version, release_id,
                        config_id, role_identity, started_at, qualified_at, payload,
                        canonical_payload, payload_sha256, certificate_digest, evidence_digest
                    )
                    SELECT 'attacker-chosen-certificate', epoch_id, 'attacker-chosen-identity',
                           policy_version, release_id, config_id, role_identity,
                           started_at, qualified_at, payload, canonical_payload,
                           payload_sha256, certificate_digest, evidence_digest
                    FROM m1_qualification_certificates
                    WHERE certificate_id = %s
                    """,
                    (certificate_id,),
                )
            connection.rollback()
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


def _identity() -> dict[str, object]:
    return {
        "config_id": "config-a",
        "epoch_id": "epoch-024",
        "policy_version": "m1-rolling-qualification-v1",
        "release_id": "release-a",
        "role_identity": ["m1"],
    }


def _bounds() -> dict[str, object]:
    return {
        "max_gap_seconds": 900,
        "qualified_at": "2030-01-02T00:00:00+00:00",
        "required_seconds": 86400,
        "started_at": "2030-01-01T00:00:00+00:00",
    }


def _insert_epoch_and_certificate(
    connection: psycopg.Connection[object],
) -> tuple[str, str]:
    identity = _identity()
    bounds = _bounds()
    payload = {
        "bounds": bounds,
        "contained_incidents": [],
        "counts": {"progress_count": 10, "successful_count": 10},
        "identity": identity,
        "policy_version": "m1-rolling-qualification-v1",
        "recovery_actions": [],
        "slo": {
            "evidence_gap_seconds": 900,
            "evidence_gap_status": "pass",
            "freshness": "pass",
            "recovery": "pass",
            "required_seconds": 86400,
        },
    }
    evidence_digest = _sha256(
        _canonical(
            {
                "contained_incidents": [],
                "epoch_id": "epoch-024",
                "fact_digests": [],
                "recovery_actions": [],
            }
        )
    )
    payload["evidence_digest"] = evidence_digest
    canonical_payload = _canonical(payload)
    digest = _sha256(canonical_payload)
    connection.execute(
        """
        INSERT INTO m1_qualification_epochs (
            epoch_id, state, version, identity_key, policy_version, release_id,
            config_id, role_identity, started_at, last_fact_at, qualified_at,
            fact_digests, contained_recoveries, coverage_seconds, max_gap_seconds,
            progress_count, successful_count, evidence_digest, required_seconds,
            slo, contained_incident_details, recovery_action_details
        ) VALUES (
            'epoch-024', 'qualified', 2, 'identity-024',
            'm1-rolling-qualification-v1', 'release-a', 'config-a',
            %s, '2030-01-01T00:00:00+00:00', '2030-01-02T00:00:00+00:00',
            '2030-01-02T00:00:00+00:00', %s, %s, 86400, 900, 10, 10,
            %s, 86400, %s, %s, %s
        )
        """,
        (
            Jsonb(["m1"]),
            Jsonb([]),
            Jsonb([]),
            evidence_digest,
            Jsonb(payload["slo"]),
            Jsonb([]),
            Jsonb([]),
        ),
    )
    cursor = connection.execute(
        """
        SELECT certificate_id, identity_key
        FROM m1_insert_qualification_certificate(
            'epoch-024', 'm1-rolling-qualification-v1', 'release-a', 'config-a', %s,
            '2030-01-01T00:00:00+00:00', '2030-01-02T00:00:00+00:00',
            %s, %s, %s, %s, %s
        )
        """,
        (
            Jsonb(["m1"]),
            Jsonb(payload),
            canonical_payload.decode("utf-8"),
            digest,
            digest,
            evidence_digest,
        ),
    )
    row = cursor.fetchone()
    assert row is not None
    connection.commit()
    return cast(tuple[str, str], row)


def _canonical(payload: object) -> bytes:
    import json

    return json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def _sha256(payload: bytes) -> str:
    from hashlib import sha256

    return sha256(payload).hexdigest()


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
