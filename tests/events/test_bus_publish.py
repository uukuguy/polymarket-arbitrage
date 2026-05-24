"""Tests for polyarb.events.bus.publish_snapshot_complete.

Plan 03-05 Task 1 — fail-soft envelope + breadcrumb + payload shape.
Patterns reused:
- Phase 02 L9 — patch at IMPORT SITE (polyarb.events.bus.asyncpg.connect)
- Phase 02.1 L4 — loguru StringIO sink (not caplog)
- Phase 02.2 backlog preemptive — success path also emits breadcrumb (Open Q 9)
"""
from __future__ import annotations

import io
import json
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from loguru import logger

# F-3 escape: keep test settings constructible without scan_shared_secret
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")


@pytest.fixture
def loguru_sink():
    """Loguru StringIO sink — Phase 02.1 L4 (caplog doesn't capture loguru)."""
    buf = io.StringIO()
    sink_id = logger.add(buf, format="{message}", level="DEBUG")
    yield buf
    logger.remove(sink_id)


@pytest.fixture
def fake_settings():
    """Settings stub with a usable supabase_db_dsn SecretStr."""
    from pydantic import SecretStr

    class _S:
        supabase_db_dsn = SecretStr("postgresql://test:test@localhost:5432/db")

    return _S()


def _make_breadcrumb_recorder():
    recorded: list[dict] = []

    def _rec(**kwargs):
        recorded.append(kwargs)

    return recorded, _rec


@pytest.mark.asyncio
async def test_publish_success_emits_breadcrumb_info(loguru_sink, fake_settings):
    """Phase 02.2 preemptive (RESEARCH Open Q 9): success path emits info breadcrumb."""
    from polyarb.events import bus

    fake_conn = MagicMock()
    fake_conn.execute = AsyncMock(return_value="NOTIFY")
    fake_conn.close = AsyncMock()
    recorded, rec = _make_breadcrumb_recorder()

    with patch.object(bus.asyncpg, "connect", AsyncMock(return_value=fake_conn)), \
         patch.object(bus.sentry_sdk, "add_breadcrumb", side_effect=rec):
        ok = await bus.publish_snapshot_complete(
            fake_settings, snapshot_id=42, taken_at_ms=1234567890
        )

    assert ok is True
    assert any(
        b.get("category") == "event-bus" and b.get("level") == "info"
        for b in recorded
    ), f"expected info breadcrumb category=event-bus, got {recorded}"


@pytest.mark.asyncio
async def test_publish_failsoft_logs(loguru_sink, fake_settings):
    """asyncpg.connect raises → return False, log warning, warning breadcrumb."""
    from polyarb.events import bus

    recorded, rec = _make_breadcrumb_recorder()
    with patch.object(bus.asyncpg, "connect", AsyncMock(side_effect=RuntimeError("conn refused"))), \
         patch.object(bus.sentry_sdk, "add_breadcrumb", side_effect=rec):
        ok = await bus.publish_snapshot_complete(
            fake_settings, snapshot_id=1, taken_at_ms=2
        )

    assert ok is False
    assert "event bus publish failed" in loguru_sink.getvalue()
    assert any(
        b.get("category") == "event-bus" and b.get("level") == "warning"
        for b in recorded
    )


@pytest.mark.asyncio
async def test_publish_payload_shape(fake_settings):
    """SQL = pg_notify; payload JSON encodes snapshot_id + taken_at_ms."""
    from polyarb.events import bus

    fake_conn = MagicMock()
    fake_conn.execute = AsyncMock(return_value="NOTIFY")
    fake_conn.close = AsyncMock()

    with patch.object(bus.asyncpg, "connect", AsyncMock(return_value=fake_conn)), \
         patch.object(bus.sentry_sdk, "add_breadcrumb"):
        await bus.publish_snapshot_complete(
            fake_settings, snapshot_id=42, taken_at_ms=1234567890
        )

    # spy on execute call args
    assert fake_conn.execute.call_count == 1
    args, _kwargs = fake_conn.execute.call_args
    sql, payload = args[0], args[1]
    assert "pg_notify" in sql
    assert "snapshot_complete" in sql
    parsed = json.loads(payload)
    assert parsed == {"snapshot_id": 42, "taken_at_ms": 1234567890}


@pytest.mark.asyncio
async def test_connection_closed_in_finally(fake_settings):
    """If execute raises, conn.close() still runs (finally block)."""
    from polyarb.events import bus

    fake_conn = MagicMock()
    fake_conn.execute = AsyncMock(side_effect=RuntimeError("exec failed"))
    fake_conn.close = AsyncMock()

    with patch.object(bus.asyncpg, "connect", AsyncMock(return_value=fake_conn)), \
         patch.object(bus.sentry_sdk, "add_breadcrumb"):
        ok = await bus.publish_snapshot_complete(
            fake_settings, snapshot_id=1, taken_at_ms=2
        )

    assert ok is False
    assert fake_conn.close.await_count == 1, "conn.close() must run in finally"


@pytest.mark.asyncio
async def test_payload_size_under_8000_bytes(fake_settings):
    """Postgres NOTIFY hard limit ≈8000 bytes; ours is ~80 bytes."""
    from polyarb.events import bus

    fake_conn = MagicMock()
    fake_conn.execute = AsyncMock(return_value="NOTIFY")
    fake_conn.close = AsyncMock()

    with patch.object(bus.asyncpg, "connect", AsyncMock(return_value=fake_conn)), \
         patch.object(bus.sentry_sdk, "add_breadcrumb"):
        # max realistic: 10-digit snapshot id, 13-digit ms
        await bus.publish_snapshot_complete(
            fake_settings, snapshot_id=9_999_999_999, taken_at_ms=9_999_999_999_999
        )
    args, _ = fake_conn.execute.call_args
    payload = args[1]
    assert len(payload.encode("utf-8")) < 8000
