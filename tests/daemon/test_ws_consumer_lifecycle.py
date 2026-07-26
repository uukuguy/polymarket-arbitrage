"""WsConsumer.run lifecycle convergence contracts."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest

from polyarb.daemon import ws_consumer as ws_consumer_module
from polyarb.daemon.ws_consumer import WsConsumer
from polyarb.daemon.ws_watchdog import WsWatchdog


def _consumer(*, on_event=lambda event: True):
    publications = []
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=WsWatchdog(stale_s=30),
        on_event=on_event,
        initial_assets=["candidate"],
        membership_observer=publications.append,
    )
    consumer._current_ws = object()
    consumer._connection_generation = 3
    consumer._l3_committed_set = {"l3-token"}
    consumer._l3_business_evidence = {"l3-token": (3, datetime(2026, 7, 23, tzinfo=UTC))}
    return consumer, publications


def _assert_disconnected_publication(consumer, publications) -> None:
    assert consumer.current_state == "DISCONNECTED"
    assert consumer._current_ws is None
    status = consumer.l3_membership_snapshot()
    assert status.committed == frozenset()
    assert status.evidenced == frozenset()
    assert publications[-1] == status


@pytest.mark.asyncio
async def test_run_normal_stream_exhaustion_converges_to_disconnected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, publications = _consumer()

    async def _exhausted_stream(*args, **kwargs):
        if False:
            yield {}

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _exhausted_stream)

    await consumer.run(asyncio.Event())

    _assert_disconnected_publication(consumer, publications)


@pytest.mark.asyncio
async def test_run_stop_break_converges_to_disconnected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop_event = asyncio.Event()
    consumer, publications = _consumer(on_event=lambda event: stop_event.set() or True)

    class _TwoEventStream:
        def __init__(self) -> None:
            self._remaining = 2

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self._remaining == 0:
                raise StopAsyncIteration
            self._remaining -= 1
            return {"event_type": "price_change", "asset_id": "candidate"}

    def _two_event_stream(*args, **kwargs):
        return _TwoEventStream()

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _two_event_stream)

    await consumer.run(stop_event)

    assert consumer.frame_count == 1
    _assert_disconnected_publication(consumer, publications)


@pytest.mark.asyncio
async def test_run_unexpected_exception_propagates_after_disconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer, publications = _consumer()

    async def _broken_stream(*args, **kwargs):
        raise RuntimeError("stream exploded")
        yield  # pragma: no cover

    monkeypatch.setattr(ws_consumer_module, "stream_market_events", _broken_stream)

    with pytest.raises(RuntimeError, match="stream exploded"):
        await consumer.run(asyncio.Event())

    _assert_disconnected_publication(consumer, publications)
