"""Real production mirror callback → consumer receive → quiet waiter chain."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.l2_main import make_l2_event_handler
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog


async def _wait_until(predicate, *, timeout: float = 0.2) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


def _consumer(mirror_succeeded: bool) -> tuple[WsConsumer, MagicMock, MagicMock]:
    mirror = MagicMock()
    mirror.push_top_of_book.return_value = mirror_succeeded
    mirror.push_book_levels.return_value = True
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=make_l2_event_handler(mirror),
        initial_assets=["asset-a"],
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 4
    return consumer, mirror, ws


@pytest.mark.asyncio
async def test_real_production_mirror_success_resolves_receive_path_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, mirror, ws = _consumer(True)

    async def _stream(*args, **kwargs):
        yield {
            "event_type": "book",
            "asset_id": "asset-a",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.6", "size": "10"}],
            "timestamp": "1",
        }

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _stream)
    quiet = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count >= 2)
    await asyncio.wait_for(consumer.run(asyncio.Event()), timeout=0.2)

    assert await asyncio.wait_for(quiet, timeout=0.2) is True
    mirror.push_top_of_book.assert_called_once()
    assert consumer._book_evidence_waiters == {}


@pytest.mark.asyncio
async def test_real_production_mirror_false_does_not_resolve_and_cleans_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, mirror, ws = _consumer(False)
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.01)

    async def _stream(*args, **kwargs):
        yield {
            "event_type": "book",
            "asset_id": "asset-a",
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.6", "size": "10"}],
            "timestamp": "1",
        }

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _stream)
    quiet = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count >= 2)
    await asyncio.wait_for(consumer.run(asyncio.Event()), timeout=0.2)

    assert await asyncio.wait_for(quiet, timeout=0.2) is False
    mirror.push_top_of_book.assert_called_once()
    ws.close.assert_awaited_once()
    assert consumer._book_evidence_waiters == {}
