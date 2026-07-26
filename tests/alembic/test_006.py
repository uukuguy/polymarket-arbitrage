"""Alembic 006 contract tests for markets_latest.no_token_id."""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")

MIGRATION_PATH = Path("alembic/versions/006_add_no_token_id.py")


def _migration_text() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def test_006_revision_chains_to_005() -> None:
    text = _migration_text()
    assert 'revision = "006"' in text
    assert 'down_revision = "005"' in text


def test_006_upgrade_is_add_only_nullable_text() -> None:
    text = _migration_text()
    upgrade = text[text.index("def upgrade(") : text.index("def downgrade(")]
    assert "op.drop_" not in upgrade
    assert 'op.add_column(\n        "markets_latest"' in upgrade
    assert 'sa.Column("no_token_id", sa.Text, nullable=True)' in upgrade


def _docker_available() -> bool:
    try:
        result = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
        return result.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def pg_dsn():
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; live-DB alembic tests skipped")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url()
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if url.startswith(prefix):
                url = "postgresql://" + url[len(prefix) :]
                break
        asyncio.run(_create_supabase_roles(url))
        yield url


async def _create_supabase_roles(dsn: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("CREATE ROLE anon NOLOGIN")
    finally:
        await conn.close()


def _run_alembic(dsn: str, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "alembic", *args],
        env={**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn},
        capture_output=True,
        text=True,
        timeout=120,
    )


async def _column_rows(dsn: str) -> list[dict]:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        rows = await conn.fetch(
            "SELECT is_nullable, data_type FROM information_schema.columns "
            "WHERE table_name='markets_latest' AND column_name='no_token_id'"
        )
        return [dict(row) for row in rows]
    finally:
        await conn.close()


@pytest.mark.slow
def test_006_upgrade_and_replay(pg_dsn: str) -> None:
    first = _run_alembic(pg_dsn, "upgrade", "006")
    assert first.returncode == 0, first.stderr
    assert asyncio.run(_column_rows(pg_dsn)) == [{"is_nullable": "YES", "data_type": "text"}]

    down = _run_alembic(pg_dsn, "downgrade", "-1")
    assert down.returncode == 0, down.stderr
    assert asyncio.run(_column_rows(pg_dsn)) == []

    second = _run_alembic(pg_dsn, "upgrade", "006")
    assert second.returncode == 0, second.stderr
    assert asyncio.run(_column_rows(pg_dsn)) == [{"is_nullable": "YES", "data_type": "text"}]
