"""Repair qualification digest calls for the installed pgcrypto namespace.

Revision ID: 027
Revises: 026

Supabase installs pgcrypto in ``extensions`` while ordinary PostgreSQL installs
it in the current creation schema (normally ``public``).  Revision 026 closed
the qualification SECURITY DEFINER search paths correctly, but accidentally
hard-coded ``public.digest``.  This migration rewrites only that qualified
routine token in the three existing function definitions, preserving their
owners, signatures, ACLs, SECURITY mode, and configured search paths.
"""

from __future__ import annotations

import re

import sqlalchemy as sa

from alembic import op

revision = "027"
down_revision = "026"
branch_labels = None
depends_on = None

FUNCTION_DIGEST_COUNTS = {
    "public.m1_record_qualification_ingress(text,text,text,timestamp with time zone,jsonb)": 1,
    "public.m1_verify_qualification_certificate_insert()": 2,
    "public.m1_insert_qualification_certificate("
    "text,text,text,text,jsonb,timestamp with time zone,timestamp with time zone,"
    "jsonb,text,text,text,text)": 1,
}
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_$]*$")


def upgrade() -> None:
    _rewrite_digest_namespace(source_schema="public", target_schema=_pgcrypto_schema())


def downgrade() -> None:
    _rewrite_digest_namespace(source_schema=_pgcrypto_schema(), target_schema="public")


def _pgcrypto_schema() -> str:
    connection = op.get_bind()
    schema = connection.execute(
        sa.text(
            """
            SELECT namespace.nspname
            FROM pg_catalog.pg_extension AS extension
            JOIN pg_catalog.pg_namespace AS namespace
              ON namespace.oid = extension.extnamespace
            WHERE extension.extname = 'pgcrypto'
            """
        )
    ).scalar_one_or_none()
    if not isinstance(schema, str) or _IDENTIFIER.fullmatch(schema) is None:
        raise RuntimeError("pgcrypto extension namespace is missing or invalid")
    digest = connection.execute(
        sa.text(
            "SELECT pg_catalog.to_regprocedure("
            "pg_catalog.quote_ident(:schema) || '.digest(bytea,text)')"
        ),
        {"schema": schema},
    ).scalar_one_or_none()
    if digest is None:
        raise RuntimeError("pgcrypto digest(bytea,text) is missing")
    return schema


def _rewrite_digest_namespace(*, source_schema: str, target_schema: str) -> None:
    connection = op.get_bind()
    source = _quoted_identifier(source_schema)
    target = _quoted_identifier(target_schema)
    source_call = f"{source}.digest("
    target_call = f"{target}.digest("

    for signature, expected_count in FUNCTION_DIGEST_COUNTS.items():
        definition = connection.execute(
            sa.text("SELECT pg_catalog.pg_get_functiondef(pg_catalog.to_regprocedure(:signature))"),
            {"signature": signature},
        ).scalar_one_or_none()
        if not isinstance(definition, str):
            raise RuntimeError(f"qualification function is missing: {signature}")
        if definition.count(source_call) != expected_count:
            raise RuntimeError(f"qualification digest projection drifted: {signature}")
        rewritten = definition.replace(source_call, target_call)
        if rewritten.count(target_call) != expected_count:
            raise RuntimeError(f"qualification digest rewrite failed: {signature}")
        # pg_get_functiondef() may contain PL/pgSQL tokens such as %ROWTYPE.
        # Force a parameterless DBAPI execute so psycopg does not parse those
        # percent signs as client-side placeholders.
        connection.execution_options(no_parameters=True).exec_driver_sql(rewritten)


def _quoted_identifier(value: str) -> str:
    quoted = (
        op.get_bind()
        .execute(
            sa.text("SELECT pg_catalog.quote_ident(:identifier)"),
            {"identifier": value},
        )
        .scalar_one()
    )
    if not isinstance(quoted, str):
        raise RuntimeError("database identifier quoting failed")
    return quoted
