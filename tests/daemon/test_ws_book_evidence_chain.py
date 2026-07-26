"""Real production mirror callback → consumer receive → quiet waiter chain."""

from __future__ import annotations

import asyncio
import threading
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


@pytest.mark.asyncio
async def test_production_mirror_call_does_not_block_event_loop() -> None:
    mirror = MagicMock()
    started = threading.Event()
    release = threading.Event()

    def _blocking_write(_rows) -> bool:
        started.set()
        release.wait(timeout=1)
        return True

    mirror.push_top_of_book.side_effect = _blocking_write
    handler = make_l2_event_handler(mirror)
    dispatch = asyncio.create_task(
        handler(
            {
                "event_type": "best_bid_ask",
                "asset_id": "asset-a",
                "best_bid": "0.4",
                "best_ask": "0.6",
                "timestamp": "2026-07-24T06:00:00Z",
            }
        )
    )
    try:
        await asyncio.wait_for(asyncio.to_thread(started.wait, 0.2), timeout=0.3)
        await asyncio.wait_for(asyncio.sleep(0.01), timeout=0.05)
        assert dispatch.done() is False
    finally:
        release.set()

    assert (await asyncio.wait_for(dispatch, timeout=0.2)).tob_written is True


def _consumer(
    *,
    tob_written: bool,
    book_levels_written: bool,
) -> tuple[WsConsumer, MagicMock, MagicMock]:
    mirror = MagicMock()
    mirror.push_top_of_book.return_value = tob_written
    mirror.push_book_levels.return_value = book_levels_written
    holder: dict[str, WsConsumer] = {}
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=make_l2_event_handler(
            mirror,
            book_levels_required=lambda asset_id: holder["consumer"].requires_book_levels(asset_id),
        ),
        initial_assets=["asset-a"],
    )
    holder["consumer"] = consumer
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
async def test_no_l3_candidate_refresh_uses_real_handler_consumer_barrier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_ids = [f"candidate-{index}" for index in range(10)]
    outsider = "unsubscribed-private-token"
    mirror = MagicMock()
    mirror.push_top_of_book.return_value = True
    mirror.push_book_levels.return_value = True
    holder: dict[str, WsConsumer] = {}
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=make_l2_event_handler(
            mirror,
            book_levels_required=lambda asset_id: holder["consumer"].requires_book_levels(asset_id),
        ),
        initial_assets=candidate_ids,
    )
    holder["consumer"] = consumer
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 8
    release_last = asyncio.Event()

    def _book(asset_id: str) -> dict:
        return {
            "event_type": "book",
            "asset_id": asset_id,
            "bids": [{"price": "0.4", "size": "10"}],
            "asks": [{"price": "0.6", "size": "10"}],
            "timestamp": "1",
        }

    async def _stream(*args, **kwargs):
        yield _book(outsider)
        for asset_id in candidate_ids[:9]:
            yield _book(asset_id)
        await release_last.wait()
        yield _book(candidate_ids[9])

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _stream)
    quiet = asyncio.create_task(consumer.request_book_refresh())
    await _wait_until(lambda: ws.send.await_count >= 2)
    run = asyncio.create_task(consumer.run(asyncio.Event()))
    await _wait_until(lambda: mirror.push_book_levels.call_count == 9)

    assert quiet.done() is False
    assert consumer.requires_book_levels(outsider) is False
    assert consumer.requires_book_levels(candidate_ids[9]) is True
    assert consumer.last_quiet_refresh_missing_assets == frozenset()

    release_last.set()
    await asyncio.wait_for(run, timeout=0.2)
    assert await asyncio.wait_for(quiet, timeout=0.2) is True
    written_assets = [
        call.args[0][0]["asset_id"] for call in mirror.push_book_levels.call_args_list
    ]
    assert written_assets == candidate_ids
    assert outsider not in written_assets
    assert all(consumer.requires_book_levels(asset_id) is False for asset_id in candidate_ids)
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
