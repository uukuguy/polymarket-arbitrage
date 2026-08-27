"""Contracts for revision 027 pgcrypto namespace repair."""

from __future__ import annotations

import os
import subprocess
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast
from urllib.parse import quote, urlsplit, urlunsplit

import psycopg
import pytest
from psycopg import sql
from psycopg.types.json import Jsonb

MIGRATION_PATH = Path("alembic/versions/027_m1_qualification_pgcrypto_namespace.py")
QUALIFICATION_ROLE = "m1_qualification_worker_capability"
QUALIFICATION_LOGIN = "m1_qualification_worker_027"
NOW = datetime(2026, 8, 27, 13, 45, tzinfo=UTC)
FUNCTION_SIGNATURES = (
    "public.m1_record_qualification_ingress(text,text,text,timestamp with time zone,jsonb)",
    "public.m1_verify_qualification_certificate_insert()",
    "public.m1_insert_qualification_certificate("
    "text,text,text,text,jsonb,timestamp with time zone,timestamp with time zone,"
    "jsonb,text,text,text,text)",
)


def test_027_declares_chain_and_resolves_pgcrypto_extension_namespace() -> None:
    text = MIGRATION_PATH.read_text()

    assert 'revision = "027"' in text
    assert 'down_revision = "026"' in text
    assert "pg_catalog.pg_extension" in text
    assert "extension.extname = 'pgcrypto'" in text
    assert "pg_catalog.pg_get_functiondef" in text
    assert 'source_schema="public"' in text
    assert "pg_catalog.quote_ident" in text
    for function_name in (
        "m1_record_qualification_ingress",
        "m1_verify_qualification_certificate_insert",
        "m1_insert_qualification_certificate",
    ):
        assert function_name in text


def test_027_repairs_supabase_extensions_schema_without_authority_drift() -> None:
    if not _docker_available():
        pytest.fail("Docker daemon unavailable; cannot prove pgcrypto namespace repair")

    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as postgres:
        dsn = _normalize_dsn(postgres.get_connection_url())
        with psycopg.connect(dsn, autocommit=True) as admin:
            for role in ("anon", "authenticated", "service_role"):
                admin.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
            admin.execute("CREATE SCHEMA extensions")
            admin.execute("CREATE EXTENSION pgcrypto WITH SCHEMA extensions")

        _run_alembic(dsn, "upgrade", "026")
        with psycopg.connect(dsn, autocommit=True) as admin:
            admin.execute(
                sql.SQL("CREATE ROLE {} LOGIN INHERIT PASSWORD {}").format(
                    sql.Identifier(QUALIFICATION_LOGIN),
                    sql.Literal("qualification-027-test"),
                )
            )
            admin.execute(
                sql.SQL("GRANT {} TO {}").format(
                    sql.Identifier(QUALIFICATION_ROLE),
                    sql.Identifier(QUALIFICATION_LOGIN),
                )
            )

        qualification_dsn = _role_dsn(
            dsn,
            QUALIFICATION_LOGIN,
            "qualification-027-test",
        )
        with pytest.raises(psycopg.errors.UndefinedFunction, match="public.digest"):
            _record_freshness(qualification_dsn, "freshness:structure:027:red")

        before = _function_authority_projection(dsn)
        _run_alembic(dsn, "upgrade", "027")
        assert _current_revision(dsn) == "027"
        assert _function_authority_projection(dsn) == before

        _record_freshness(qualification_dsn, "freshness:structure:027:green")
        _assert_freshness_row(dsn, "freshness:structure:027:green")
        _assert_digest_namespace(dsn, expected="extensions.digest(")

        _run_alembic(dsn, "downgrade", "026")
        assert _current_revision(dsn) == "026"
        assert _function_authority_projection(dsn) == before
        with pytest.raises(psycopg.errors.UndefinedFunction, match="public.digest"):
            _record_freshness(qualification_dsn, "freshness:structure:027:down")

        _run_alembic(dsn, "upgrade", "027")
        _record_freshness(qualification_dsn, "freshness:structure:027:roundtrip")
        _assert_freshness_row(dsn, "freshness:structure:027:roundtrip")


def _record_freshness(dsn: str, fact_id: str) -> None:
    payload = {
        "data_product": "structure",
        "fact_id": fact_id,
        "freshness_seconds": 1,
        "freshness_slo_seconds": 900,
        "observed_at": NOW.isoformat(),
        "progress_count": 1,
        "successful_count": 1,
    }
    with psycopg.connect(dsn) as connection:
        connection.execute(
            "SELECT public.m1_record_qualification_freshness_ingress(%s, %s, %s, %s)",
            (fact_id, "structure", NOW, Jsonb(payload)),
        )


def _assert_freshness_row(dsn: str, fact_id: str) -> None:
    with psycopg.connect(dsn) as connection:
        row = connection.execute(
            "SELECT source, source_version, payload_sha256 "
            "FROM public.m1_qualification_ingress_ledger WHERE source_id = %s",
            (fact_id,),
        ).fetchone()
    assert row is not None
    assert row[0:2] == ("freshness", "structure")
    assert isinstance(row[2], str) and len(row[2]) == 64


def _assert_digest_namespace(dsn: str, *, expected: str) -> None:
    with psycopg.connect(dsn) as connection:
        definitions = [
            connection.execute(
                "SELECT pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(%s))",
                (signature,),
            ).fetchone()
            for signature in FUNCTION_SIGNATURES
        ]
    for row in definitions:
        assert row is not None
        definition = str(row[0])
        assert expected in definition
        assert "public.digest(" not in definition


def _function_authority_projection(
    dsn: str,
) -> dict[str, tuple[str, bool, tuple[str, ...], tuple[str, ...]]]:
    with psycopg.connect(dsn) as connection:
        rows = connection.execute(
            """
            SELECT pg_catalog.format(
                       '%%I.%%I(%%s)', namespace.nspname, routine.proname,
                       pg_catalog.pg_get_function_identity_arguments(routine.oid)
                   ),
                   owner.rolname,
                   routine.prosecdef,
                   COALESCE(routine.proconfig, ARRAY[]::text[]),
                   ARRAY(
                       SELECT pg_catalog.format(
                                  '%%s:%%s:%%s',
                                  CASE WHEN acl.grantee = 0 THEN 'PUBLIC'
                                       ELSE grantee.rolname END,
                                  acl.privilege_type,
                                  acl.is_grantable
                              )
                       FROM pg_catalog.aclexplode(
                           COALESCE(
                               routine.proacl,
                               pg_catalog.acldefault('f', routine.proowner)
                           )
                       ) AS acl
                       LEFT JOIN pg_catalog.pg_roles AS grantee
                         ON grantee.oid = acl.grantee
                       ORDER BY 1
                   )
            FROM pg_catalog.pg_proc AS routine
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = routine.pronamespace
            JOIN pg_catalog.pg_roles AS owner
              ON owner.oid = routine.proowner
            WHERE routine.oid = ANY(
                ARRAY[
                    pg_catalog.to_regprocedure(%s),
                    pg_catalog.to_regprocedure(%s),
                    pg_catalog.to_regprocedure(%s)
                ]
            )
            ORDER BY 1
            """,
            FUNCTION_SIGNATURES,
        ).fetchall()
    return {
        str(row[0]): (
            str(row[1]),
            bool(row[2]),
            tuple(str(value) for value in cast(Sequence[object], row[3])),
            tuple(str(value) for value in cast(Sequence[object], row[4])),
        )
        for row in rows
    }


def _docker_available() -> bool:
    try:
        return (
            subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=5,
            ).returncode
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


def _current_revision(dsn: str) -> str:
    with psycopg.connect(dsn) as connection:
        row = connection.execute("SELECT version_num FROM alembic_version").fetchone()
    assert row is not None
    return str(row[0])


def _role_dsn(dsn: str, username: str, password: str) -> str:
    parts = urlsplit(dsn)
    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"
    return urlunsplit(
        (
            parts.scheme,
            f"{quote(username)}:{quote(password)}@{host}",
            parts.path,
            parts.query,
            parts.fragment,
        )
    )
