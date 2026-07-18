"""Production amendment: subscription control is one fenced transaction."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog


def _consumer() -> tuple[WsConsumer, MagicMock]:
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=lambda event: True,
        initial_assets=["a", "b"],
    )
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    ws.close = AsyncMock(return_value=None)
    consumer._current_ws = ws
    consumer._connection_generation = 7
    return consumer, ws


@pytest.mark.asyncio
async def test_quiet_refresh_requires_exact_pair_and_matching_mirror_evidence() -> None:
    consumer, ws = _consumer()

    task = asyncio.create_task(consumer.request_book_refresh())
    async with asyncio.timeout(0.1):
        while ws.send.await_count < 2:
            await asyncio.sleep(0)

    payloads = [json.loads(call.args[0]) for call in ws.send.await_args_list]
    assert payloads == [
        {"operation": "unsubscribe", "assets_ids": ["a", "b"]},
        {
            "operation": "subscribe",
            "assets_ids": ["a", "b"],
            "initial_dump": True,
        },
    ]
    assert task.done() is False

    consumer.record_book_evidence(asset_id="a", generation=6, mirror_succeeded=True)
    consumer.record_book_evidence(asset_id="a", generation=7, mirror_succeeded=False)
    await asyncio.sleep(0)
    assert task.done() is False

    consumer.record_book_evidence(asset_id="a", generation=7, mirror_succeeded=True)
    assert await task is True


@pytest.mark.asyncio
async def test_quiet_refresh_evidence_timeout_closes_only_captured_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, old_ws = _consumer()
    new_ws = MagicMock()
    new_ws.close = AsyncMock(return_value=None)
    monkeypatch.setattr(ws_consumer_module, "_BOOK_EVIDENCE_TIMEOUT_S", 0.01, raising=False)

    task = asyncio.create_task(consumer.request_book_refresh())
    async with asyncio.timeout(0.1):
        while old_ws.send.await_count < 2:
            await asyncio.sleep(0)
    consumer._current_ws = new_ws
    consumer._connection_generation = 8

    assert await task is False
    old_ws.close.assert_awaited_once()
    new_ws.close.assert_not_awaited()


@pytest.mark.asyncio
async def test_candidate_replacement_commits_only_after_serialized_control_pair() -> None:
    consumer, ws = _consumer()

    assert await consumer.replace_candidate_set(["b", "c"]) is True

    assert [json.loads(call.args[0]) for call in ws.send.await_args_list] == [
        {
            "operation": "subscribe",
            "assets_ids": ["c"],
            "initial_dump": True,
        },
        {"operation": "unsubscribe", "assets_ids": ["a"]},
    ]
    assert consumer._candidate_set == {"b", "c"}


@pytest.mark.asyncio
async def test_candidate_partial_send_failure_does_not_commit_and_compensates() -> None:
    consumer, ws = _consumer()
    ws.send.side_effect = [None, RuntimeError("ambiguous transport")]

    assert await consumer.replace_candidate_set(["b", "c"]) is False

    assert consumer._candidate_set == {"a", "b"}
    ws.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_candidate_replacement_without_socket_publishes_reconnect_desire() -> None:
    consumer, _ws = _consumer()
    consumer._current_ws = None

    assert await consumer.replace_candidate_set(["cold-start"]) is False
    assert consumer._candidate_set == {"cold-start"}
    assert consumer._compute_active_assets() == ["cold-start"]
