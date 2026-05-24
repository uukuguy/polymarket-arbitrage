"""Tests for polyarb.events.listener.

Plan 03-05 Task 2 — reconnect loop + cursor catch-up + cancellation.
"""
from __future__ import annotations

import asyncio
import io
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from loguru import logger

os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


@pytest.fixture
def loguru_sink():
    buf = io.StringIO()
    sink_id = logger.add(buf, format="{message}", level="DEBUG")
    yield buf
    logger.remove(sink_id)


def _make_fake_conn(*, fetchrow_val=None, fetch_val=None, fetchrow_exc=None, fetch_exc=None):
    conn = MagicMock()
    conn.add_listener = AsyncMock()
    conn.execute = AsyncMock(return_value="LISTEN")
    conn.close = AsyncMock()
    if fetchrow_exc:
        conn.fetchrow = AsyncMock(side_effect=fetchrow_exc)
    else:
        conn.fetchrow = AsyncMock(return_value=fetchrow_val)
    if fetch_exc:
        conn.fetch = AsyncMock(side_effect=fetch_exc)
    else:
        conn.fetch = AsyncMock(return_value=fetch_val or [])
    return conn


@pytest.mark.asyncio
async def test_listener_calls_on_event_with_parsed_payload():
    """add_listener callback parses JSON payload and dispatches to on_event."""
    from polyarb.events import listener

    received: list[dict] = []
    cb = listener._make_callback(lambda d: received.append(d))
    cb(None, 0, "snapshot_complete", '{"snapshot_id": 42, "taken_at_ms": 12345}')

    assert received == [{"snapshot_id": 42, "taken_at_ms": 12345}]


@pytest.mark.asyncio
async def test_listener_reconnects_after_connection_loss(loguru_sink):
    """First connect raises, second succeeds; sleep called between attempts."""
    from polyarb.events import listener

    sleep_calls: list[float] = []
    real_sleep = asyncio.sleep

    async def _fake_sleep(s):
        sleep_calls.append(s)
        # yield to scheduler so loop doesn't hot-spin
        await real_sleep(0)

    fake_conn = _make_fake_conn()
    connect_mock = AsyncMock(side_effect=[RuntimeError("conn lost"), fake_conn])
    stop_event = asyncio.Event()

    with patch.object(listener.asyncpg, "connect", connect_mock), \
         patch.object(listener.asyncio, "sleep", _fake_sleep):
        task = asyncio.create_task(
            listener.listen_snapshot_complete(
                dsn="postgresql://test",
                on_event=lambda d: None,
                stop_event=stop_event,
            )
        )
        # let listener fail once + reconnect (poll up to 1s)
        for _ in range(50):
            await real_sleep(0.01)
            if connect_mock.await_count >= 2:
                break
        stop_event.set()
        await asyncio.wait_for(task, timeout=2.0)

    assert connect_mock.await_count >= 2, "must have reconnected after failure"
    assert sleep_calls, "must sleep before reconnecting"
    assert "reconnecting" in loguru_sink.getvalue().lower()


@pytest.mark.asyncio
async def test_listener_handles_malformed_payload(loguru_sink):
    """Non-JSON payload → log error, do NOT crash."""
    from polyarb.events import listener

    received: list[dict] = []
    cb = listener._make_callback(lambda d: received.append(d))
    # should not raise
    cb(None, 0, "snapshot_complete", "GARBAGE NOT JSON")
    assert received == []
    out = loguru_sink.getvalue().lower()
    assert "json" in out or "decode" in out or "malformed" in out or "parse" in out


@pytest.mark.asyncio
async def test_catchup_replays_missed():
    """fetchrow returns cursor; fetch returns missed snapshots."""
    from polyarb.events import listener

    fake_conn = _make_fake_conn(
        fetchrow_val={"last_snapshot_id": 9},
        fetch_val=[
            {"id": 10, "taken_at_ms": 1000},
            {"id": 11, "taken_at_ms": 2000},
            {"id": 12, "taken_at_ms": 3000},
        ],
    )
    with patch.object(listener.asyncpg, "connect", AsyncMock(return_value=fake_conn)):
        missed = await listener.catchup_from_cursor("postgresql://test")

    assert len(missed) == 3
    assert [m["id"] for m in missed] == [10, 11, 12]


@pytest.mark.asyncio
async def test_catchup_empty_when_no_cursor():
    """fetchrow returns None → last_seen=0 fallback; all snapshots returned."""
    from polyarb.events import listener

    fake_conn = _make_fake_conn(
        fetchrow_val=None,
        fetch_val=[{"id": 1, "taken_at_ms": 100}, {"id": 2, "taken_at_ms": 200}],
    )
    with patch.object(listener.asyncpg, "connect", AsyncMock(return_value=fake_conn)):
        missed = await listener.catchup_from_cursor("postgresql://test")

    assert len(missed) == 2


@pytest.mark.asyncio
async def test_catchup_handles_missing_table(loguru_sink):
    """UndefinedTableError → return [] (Plan 06 hasn't created the table yet)."""
    import asyncpg.exceptions
    from polyarb.events import listener

    fake_conn = _make_fake_conn(
        fetchrow_exc=asyncpg.exceptions.UndefinedTableError("relation l2_event_cursor does not exist"),
    )
    with patch.object(listener.asyncpg, "connect", AsyncMock(return_value=fake_conn)):
        missed = await listener.catchup_from_cursor("postgresql://test")

    assert missed == []


@pytest.mark.asyncio
async def test_listener_stop_event_clean_shutdown():
    """stop_event set before start → returns promptly without hanging."""
    from polyarb.events import listener

    fake_conn = _make_fake_conn()
    stop_event = asyncio.Event()
    stop_event.set()  # already stopped

    with patch.object(listener.asyncpg, "connect", AsyncMock(return_value=fake_conn)):
        await asyncio.wait_for(
            listener.listen_snapshot_complete(
                dsn="postgresql://test", on_event=lambda d: None, stop_event=stop_event
            ),
            timeout=1.0,
        )


@pytest.mark.asyncio
async def test_listener_propagates_cancellederror():
    """F-04 contract — CancelledError MUST propagate, not be swallowed."""
    from polyarb.events import listener

    fake_conn = _make_fake_conn()
    stop_event = asyncio.Event()

    with patch.object(listener.asyncpg, "connect", AsyncMock(return_value=fake_conn)):
        task = asyncio.create_task(
            listener.listen_snapshot_complete(
                dsn="postgresql://test", on_event=lambda d: None, stop_event=stop_event
            )
        )
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
