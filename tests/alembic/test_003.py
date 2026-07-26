"""Alembic 003 tests — 5 L2 tables + RLS anon_read + BRIN indexes + idempotent replay.

Phase 03 Plan 06 Task 1 (Wave 0 RED).

These tests spin a real Postgres 16 via testcontainers, run `alembic upgrade 003`
against it, and assert the schema shape promised by D-07. Marked `slow` because
container startup is ~5-15s; CI excludes via `-m 'not slow'` for the fast suite.

Skip gracefully when Docker is not available on the host (developer laptop without
OrbStack/Docker Desktop). The migration FILE is still validated by the static
text-grep tests (test_no_drop_in_upgrade + test_down_revision_chain_to_002),
which run regardless of Docker.
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import time
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


# ── Static text checks (no Docker required) ──────────────────────────────────


MIGRATION_PATH = Path("alembic/versions/003_l2_tables.py")


def test_down_revision_chain_to_002() -> None:
    """003 must chain after 002_add_top_movers_view (Plan 02-08 lock)."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    assert 'down_revision = "002"' in content, (
        "must chain after 002_add_top_movers_view (Plan 02-08); current down_revision is incorrect"
    )


def test_no_drop_in_upgrade() -> None:
    """upgrade() body must not contain op.drop_* — schema-add discipline (L15)."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    upgrade_start = content.find("def upgrade(")
    downgrade_start = content.find("def downgrade(")
    assert 0 < upgrade_start < downgrade_start, "upgrade() must precede downgrade() in source order"
    upgrade_body = content[upgrade_start:downgrade_start]
    assert "op.drop_" not in upgrade_body, (
        "upgrade() must not contain op.drop_* (Phase 02 L15 — schema-add discipline)"
    )


def test_revision_id_is_003() -> None:
    """revision = '003' literal must be present."""
    assert MIGRATION_PATH.exists(), f"missing {MIGRATION_PATH}"
    content = MIGRATION_PATH.read_text()
    assert 'revision = "003"' in content, "revision must be '003'"


# ── Live-DB checks (require Docker) ──────────────────────────────────────────


pytestmark_slow = pytest.mark.slow


def _docker_available() -> bool:
    try:
        r = subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=5)
        return r.returncode == 0
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker daemon unavailable; live-DB alembic tests skipped",
)


@pytest.fixture(scope="module")
def pg_dsn():
    """Spin up Postgres 16 with the Supabase base role expected by migrations."""
    from testcontainers.postgres import PostgresContainer

    with PostgresContainer("postgres:16-alpine") as pg:
        # default driver in testcontainers is psycopg2 — strip the +psycopg2
        # suffix so the bare libpq DSN works with both alembic (psycopg) and
        # asyncpg (via direct asyncpg.connect call below).
        url = pg.get_connection_url()
        # Examples:
        #   postgresql+psycopg2://test:test@localhost:32781/test  → strip +psycopg2
        #   postgresql+psycopg://...
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
def test_003_creates_all_tables(pg_dsn):
    """After upgrade 003, 5 l2_* tables must exist."""
    r = _run_alembic(pg_dsn, "upgrade 003")
    assert r.returncode == 0, f"alembic upgrade failed:\nSTDOUT={r.stdout}\nSTDERR={r.stderr}"
    rows = _q(
        pg_dsn,
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename",
    )
    names = {row["tablename"] for row in rows}
    expected_tables = (
        "l2_candidates",
        "l2_top_of_book",
        "l2_trades",
        "l2_signals",
        "l2_event_cursor",
    )
    for expected in expected_tables:
        assert expected in names, f"missing table {expected}; got {sorted(names)}"


@pytest.mark.slow
def test_003_rls_enabled(pg_dsn):
    """All 5 l2_* tables must have row-level security enabled."""
    _run_alembic(pg_dsn, "upgrade 003")
    rows = _q(
        pg_dsn,
        "SELECT relname FROM pg_class WHERE relrowsecurity = true AND relname LIKE 'l2_%'",
    )
    names = {row["relname"] for row in rows}
    assert len(names) == 5, f"expected 5 RLS-enabled l2_* tables; got {sorted(names)}"


@pytest.mark.slow
def test_003_anon_read_policies(pg_dsn):
    """Each of the 5 l2_* tables must have an anon_read SELECT policy."""
    _run_alembic(pg_dsn, "upgrade 003")
    rows = _q(
        pg_dsn,
        "SELECT tablename FROM pg_policies "
        "WHERE schemaname='public' AND policyname='anon_read' AND tablename LIKE 'l2_%'",
    )
    names = {row["tablename"] for row in rows}
    assert len(names) == 5, f"expected 5 anon_read policies; got {sorted(names)}"


@pytest.mark.slow
def test_003_brin_indexes_present(pg_dsn):
    """l2_top_of_book and l2_trades must have BRIN indexes on ts column."""
    _run_alembic(pg_dsn, "upgrade 003")
    rows = _q(
        pg_dsn,
        "SELECT indexname, indexdef FROM pg_indexes "
        "WHERE schemaname='public' AND tablename LIKE 'l2_%' AND indexdef ILIKE '%using brin%'",
    )
    # Expect at least one BRIN index on l2_top_of_book and one on l2_trades.
    brin_tables = {row["indexname"] for row in rows}
    assert len(brin_tables) >= 2, (
        f"expected >=2 BRIN indexes on l2_top_of_book / l2_trades ts cols; got {brin_tables}"
    )


@pytest.mark.slow
def test_idempotent_replay(pg_dsn):
    """upgrade 003 → downgrade base → upgrade 003 must succeed end-to-end."""
    r1 = _run_alembic(pg_dsn, "upgrade 003")
    assert r1.returncode == 0, f"first upgrade failed: {r1.stderr}"
    r2 = _run_alembic(pg_dsn, "downgrade base")
    assert r2.returncode == 0, f"downgrade failed: {r2.stderr}"
    r3 = _run_alembic(pg_dsn, "upgrade 003")
    assert r3.returncode == 0, f"second upgrade failed: {r3.stderr}"
    # Verify tables exist again
    rows = _q(
        pg_dsn,
        "SELECT tablename FROM pg_tables WHERE schemaname='public' AND tablename LIKE 'l2_%'",
    )
    assert len({r["tablename"] for r in rows}) == 5


@pytest.mark.slow
def test_trade_hash_unique(pg_dsn):
    """l2_trades.trade_hash must reject duplicate inserts (idempotent backfill)."""
    import asyncpg

    _run_alembic(pg_dsn, "upgrade 003")

    async def _insert_twice() -> tuple[bool, str]:
        conn = await asyncpg.connect(dsn=pg_dsn)
        try:
            now = int(time.time())
            await conn.execute(
                "INSERT INTO l2_trades "
                "(asset_id, market_id, ts, price, size, side, taker_address, trade_hash, source) "
                "VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7, $8, $9)",
                "asset-A",
                "mkt-A",
                now,
                0.5,
                10.0,
                "BUY",
                "0xabc",
                "0xdup",
                "test",
            )
            try:
                await conn.execute(
                    "INSERT INTO l2_trades "
                    "(asset_id, market_id, ts, price, size, side, taker_address, "
                    "trade_hash, source) "
                    "VALUES ($1, $2, to_timestamp($3), $4, $5, $6, $7, $8, $9)",
                    "asset-A",
                    "mkt-A",
                    now,
                    0.5,
                    10.0,
                    "BUY",
                    "0xabc",
                    "0xdup",
                    "test",
                )
                return False, "duplicate insert did not raise UniqueViolation"
            except asyncpg.exceptions.UniqueViolationError as e:
                return True, str(e)
        finally:
            await conn.close()

    ok, msg = asyncio.run(_insert_twice())
    assert ok, msg
