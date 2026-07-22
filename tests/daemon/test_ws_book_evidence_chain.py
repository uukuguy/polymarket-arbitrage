"""Real production mirror callback → consumer receive → quiet waiter chain."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.l2_main import make_l2_event_handler
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog


@pytest.fixture(autouse=True)
def _l3_active_asset() -> None:
    from polyarb.observation import l3_promote

    prior = set(l3_promote._l3_active_set)
    l3_promote._l3_active_set = {"asset-a"}
    try:
        yield
    finally:
        l3_promote._l3_active_set = prior


async def _wait_until(predicate, *, timeout: float = 0.2) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0)


def _consumer(
    *,
    tob_written: bool,
    book_levels_written: bool,
) -> tuple[WsConsumer, MagicMock, MagicMock]:
    mirror = MagicMock()
    mirror.push_top_of_book.return_value = tob_written
    mirror.push_book_levels.return_value = book_levels_written
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=make_l2_event_handler(mirror),
        initial_assets=["asset-a"],
    )
    consumer.set_l3_desired(["asset-a"])
    consumer._l3_committed_set = {"asset-a"}
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
    consumer, mirror, ws = _consumer(tob_written=True, book_levels_written=True)

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
async def test_tob_true_depth_false_does_not_resolve_and_cleans_waiter(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, mirror, ws = _consumer(tob_written=True, book_levels_written=False)
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
    mirror.push_book_levels.assert_called_once()
    ws.close.assert_awaited_once()
    assert consumer._book_evidence_waiters == {}
