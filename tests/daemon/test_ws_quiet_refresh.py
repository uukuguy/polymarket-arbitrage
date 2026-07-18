"""Truthful quiet-market book refresh contracts for ``WsConsumer``."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog

BASE_S = 1_000.0


def _make_consumer() -> tuple[WsConsumer, WsWatchdog, MagicMock]:
    """Build real consumer/watchdog state and mock only the live transport."""
    watchdog = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=watchdog,
        on_event=lambda event: None,
        initial_assets=None,
    )
    consumer._candidate_set = {"candidate-b", "candidate-a"}
    consumer._l3_active_set = {"candidate-b", "l3-c"}
    consumer._last_event_at_s = BASE_S
    ws = MagicMock()
    ws.send = AsyncMock(return_value=None)
    consumer._current_ws = ws
    return consumer, watchdog, ws


@pytest.mark.asyncio
async def test_request_book_refresh_sends_sorted_union_without_mutating_truth() -> None:
    consumer, watchdog, ws = _make_consumer()
    candidates_before = set(consumer._candidate_set)
    l3_before = set(consumer._l3_active_set)
    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    result = await consumer.request_book_refresh()

    assert result is True
    ws.send.assert_awaited_once()
    assert json.loads(ws.send.await_args.args[0]) == {
        "operation": "subscribe",
        "assets_ids": ["candidate-a", "candidate-b", "l3-c"],
        "initial_dump": True,
    }
    assert consumer._candidate_set == candidates_before
    assert consumer._l3_active_set == l3_before
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before

@pytest.mark.asyncio
async def test_refresh_if_quiet_obeys_quiet_boundary_and_retry_cooldown() -> None:
    consumer, _watchdog, ws = _make_consumer()

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 59) is None
    assert consumer._last_quiet_refresh_attempt_at_s == 0.0

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 60) is True
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert ws.send.await_count == 1

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 89) is None
    assert ws.send.await_count == 1

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 90) is True
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 90
    assert ws.send.await_count == 2


@pytest.mark.asyncio
async def test_new_business_frame_suppresses_refresh_after_cooldown() -> None:
    consumer, _watchdog, ws = _make_consumer()

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 60) is True
    consumer._last_event_at_s = BASE_S + 85

    assert await consumer.refresh_if_quiet(now_s=BASE_S + 100) is None
    assert ws.send.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["empty", "no-ws", "closed"])
async def test_due_refresh_failures_do_not_forge_freshness(failure: str) -> None:
    consumer, watchdog, ws = _make_consumer()
    if failure == "empty":
        consumer._candidate_set.clear()
        consumer._l3_active_set.clear()
    elif failure == "no-ws":
        consumer._current_ws = None
    else:
        ws.send = AsyncMock(side_effect=RuntimeError("closed"))

    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    result = await consumer.refresh_if_quiet(now_s=BASE_S + 60)

    assert result is False
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before

    # A failed attempt still owns the retry cooldown. In particular, the
    # closed transport must not be hammered once per scheduler tick.
    assert await consumer.refresh_if_quiet(now_s=BASE_S + 89) is None
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 60
    assert ws.send.await_count == (1 if failure == "closed" else 0)
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before

    # The exact cooldown boundary permits another attempt. Empty/no-WS paths
    # remain transport-free; a closed live transport observes the retry.
    assert await consumer.refresh_if_quiet(now_s=BASE_S + 90) is False
    assert consumer._last_quiet_refresh_attempt_at_s == BASE_S + 90
    assert ws.send.await_count == (2 if failure == "closed" else 0)
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before


@pytest.mark.asyncio
async def test_run_quiet_refresh_sends_once_then_stops_cleanly() -> None:
    consumer, watchdog, ws = _make_consumer()
    stop_event = asyncio.Event()
    event_before = consumer.last_event_at_s
    watchdog_before = watchdog.last_event_at_s

    async def _send_then_stop(_payload: str) -> None:
        stop_event.set()

    ws.send.side_effect = _send_then_stop

    await asyncio.wait_for(
        consumer.run_quiet_refresh(
            stop_event,
            quiet_after_s=0,
            retry_s=30,
            check_interval_s=0.01,
        ),
        timeout=0.2,
    )

    ws.send.assert_awaited_once()
    assert consumer.last_event_at_s == event_before
    assert watchdog.last_event_at_s == watchdog_before


@pytest.mark.asyncio
async def test_run_quiet_refresh_propagates_cancellation() -> None:
    consumer, _watchdog, _ws = _make_consumer()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        consumer.run_quiet_refresh(
            stop_event,
            quiet_after_s=10_000,
            retry_s=30,
            check_interval_s=30,
        )
    )
    await asyncio.sleep(0)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
