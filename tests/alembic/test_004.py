"""Alembic 004 tests — yes_token_id column on markets_latest + add-only discipline.

Phase 04 Plan 01 Task 1 (Wave 1 RED → GREEN).

Static checks (no Docker): revision chain, no op.drop_* in upgrade(), revision id.
Live-DB checks (require Docker testcontainer): column exists in markets_latest and
is nullable after `alembic upgrade 004`.

Skip gracefully when Docker is unavailable on the host (developer laptop without
OrbStack/Docker Desktop). Static text-grep tests run regardless.

The REAL live verification of Phase 04 D-07 is Task 2's `make supabase-migrate`
against the production `POLYARB_SUPABASE_DB_DSN`. The Docker testcontainer here
is a CI safety net only.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Static text checks (no Docker required) ──────────────────────────────────


MIGRATION_PATH = Path("alembic/versions/004_add_yes_token_id.py")


def test_down_revision_chain_to_003() -> None:
    """004 must chain after 003_l2_tables (Phase 03 Plan 06 lock)."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    assert 'down_revision = "003"' in content, (
        "must chain after 003_l2_tables (Phase 03 Plan 06); current down_revision is incorrect"
    )


def test_no_drop_in_upgrade() -> None:
    """upgrade() body must not contain op.drop_* — Phase 02 L15 add-only discipline."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    upgrade_start = content.find("def upgrade(")
    downgrade_start = content.find("def downgrade(")
    assert 0 < upgrade_start < downgrade_start, "upgrade() must precede downgrade() in source order"
    upgrade_body = content[upgrade_start:downgrade_start]
    assert "op.drop_" not in upgrade_body, (
        "upgrade() must not contain op.drop_* (Phase 02 L15 — schema-add discipline)"
    )


def test_revision_id_is_004() -> None:
    """revision = '004' literal must be present."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    assert 'revision = "004"' in content, "revision must be '004'"


# ── Live-DB checks (require Docker) ──────────────────────────────────────────


def _docker_available() -> bool:
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


@pytest.fixture(scope="module")
def pg_dsn():
    """Spin up Postgres 16 with the Supabase base role expected by migrations."""
    if not _docker_available():
        pytest.skip("Docker daemon unavailable; live-DB alembic tests skipped")
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        url = pg.get_connection_url()
        # Strip +psycopg2 / +psycopg suffix so libpq DSN works with both
        # alembic (psycopg) and asyncpg here.
        for prefix in ("postgresql+psycopg2://", "postgresql+psycopg://"):
            if url.startswith(prefix):
                url = "postgresql://" + url[len(prefix) :]
                break
        asyncio.run(_create_supabase_roles(url))
        yield url


async def _create_supabase_roles(dsn: str) -> None:
    """Make vanilla Postgres match Supabase's pre-existing role surface."""
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        await conn.execute("CREATE ROLE anon NOLOGIN")
    finally:
        await conn.close()


def _run_alembic(dsn: str, cmd: str) -> subprocess.CompletedProcess:
    env = {**os.environ, "POLYARB_SUPABASE_DB_DSN": dsn}
    return subprocess.run(
        ["uv", "run", "alembic"] + cmd.split(),
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


async def _fetch(dsn: str, query: str) -> list[dict]:
    import asyncpg

    conn = await asyncpg.connect(dsn=dsn)
    try:
        rows = await conn.fetch(query)
        return [dict(r) for r in rows]
    finally:
        await conn.close()


def _q(dsn: str, query: str) -> list[dict]:
    return asyncio.run(_fetch(dsn, query))


@pytest.mark.slow
def test_004_up(pg_dsn):
    """After `upgrade 004`, markets_latest must have yes_token_id (nullable TEXT)."""
    r = _run_alembic(pg_dsn, "upgrade 004")
    assert r.returncode == 0, f"alembic upgrade failed:\nSTDOUT={r.stdout}\nSTDERR={r.stderr}"
    rows = _q(
        pg_dsn,
        "SELECT column_name, is_nullable, data_type "
        "FROM information_schema.columns "
        "WHERE table_name='markets_latest' AND column_name='yes_token_id'",
    )
    assert rows, "yes_token_id column missing from markets_latest after migration 004"
    assert rows[0]["is_nullable"] == "YES", (
        f"yes_token_id must be nullable; got is_nullable={rows[0]['is_nullable']!r}"
    )
    # SQLAlchemy sa.Text maps to PostgreSQL `text` data type.
    assert rows[0]["data_type"] == "text", (
        f"yes_token_id must be TEXT (sa.Text → text); got {rows[0]['data_type']!r}"
    )


@pytest.mark.slow
def test_004_idempotent_replay(pg_dsn):
    """upgrade 004 → downgrade -1 → upgrade 004 must succeed end-to-end."""
    r1 = _run_alembic(pg_dsn, "upgrade 004")
    assert r1.returncode == 0, f"first upgrade failed: {r1.stderr}"
    r2 = _run_alembic(pg_dsn, "downgrade -1")
    assert r2.returncode == 0, f"downgrade failed: {r2.stderr}"
    # After downgrade-1, the column should be gone.
    rows_after_down = _q(
        pg_dsn,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='markets_latest' AND column_name='yes_token_id'",
    )
    assert not rows_after_down, "yes_token_id should be absent after downgrade -1 from 004"
    r3 = _run_alembic(pg_dsn, "upgrade 004")
    assert r3.returncode == 0, f"second upgrade failed: {r3.stderr}"
    rows_after_up = _q(
        pg_dsn,
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='markets_latest' AND column_name='yes_token_id'",
    )
    assert rows_after_up, "yes_token_id should reappear after second upgrade"
