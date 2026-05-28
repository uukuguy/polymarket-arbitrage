"""RED tests for WsConsumer dropped-frame counter (Phase 04 Plan 04 Task 1, D-06 indicator 1).

Phase 03.1 Inj L2-4 left an unrepaid debt: the WS storm test only verified
LOGIC correctness (daemon survived, watchdog reconnected) — it did not verify
THROUGHPUT (zero dropped frames during real candidate-scale operation). The
on_event callback currently logs a warning on exception but does not COUNT
the failure, so there is no number we can measure during the throughput
chaos run (Task 3).

This module drives the addition of:
- `_dropped_frame_count: int` instance counter (init 0)
- `dropped_frame_count` property (mirrors `frame_count` shape)
- Increment at the on_event callback raise site (~ws_consumer.py:158)

Critical semantic: a RECEIVED frame whose on_event RAISES still counts as
received (frame_count += 1) AND as dropped (dropped_frame_count += 1). This
distinguishes "frames the WS delivered" from "frames the downstream actually
processed", which is exactly what D-06 indicator 1 needs.
"""
from __future__ import annotations

import asyncio
import os
from typing import Any
from unittest.mock import MagicMock

import pytest

# F-3 SECURITY ESCAPE HATCH (Phase 02.1 — pytest tmp_path lives outside project root)
os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


def _make_consumer(
    on_event,
    *,
    initial_assets: list[str] | None = None,
):
    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    return WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=on_event,
        initial_assets=list(initial_assets or []),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — dropped_frame_count starts at zero
# ─────────────────────────────────────────────────────────────────────────────


def test_dropped_frame_count_starts_zero() -> None:
    """A freshly-constructed WsConsumer has dropped_frame_count == 0."""
    consumer = _make_consumer(on_event=lambda ev: None)
    assert consumer.dropped_frame_count == 0, (
        f"expected 0 at construction, got {consumer.dropped_frame_count!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — dropped_frame_count increments when on_event callback raises;
#          frame_count ALSO increments (frame was received, processing failed)
# ─────────────────────────────────────────────────────────────────────────────


def test_dropped_frame_increments_on_callback_raise(monkeypatch) -> None:
    """Feed one frame whose on_event raises → dropped += 1 AND frame_count += 1."""

    def raising_callback(_event):
        raise RuntimeError("downstream dispatch failed")

    consumer = _make_consumer(
        on_event=raising_callback,
        initial_assets=["0xabc"],
    )

    # Inject a one-frame fake stream and run the consumer briefly.
    async def fake_stream(asset_ids, initial_dump=True):  # noqa: ARG001
        yield {"asset_id": "0xabc", "type": "tob", "ts_ms": 1}

    monkeypatch.setattr(
        "polyarb.daemon.ws_consumer.stream_market_events",
        fake_stream,
    )

    async def _run() -> None:
        stop_event = asyncio.Event()
        # Schedule a stop after the fake stream exhausts (after first frame).
        # The fake_stream has only one frame; the loop exits naturally when
        # the async generator is exhausted, so just await with a small grace.
        await asyncio.wait_for(consumer.run(stop_event), timeout=2.0)

    asyncio.run(_run())

    assert consumer.frame_count == 1, (
        f"frame_count: a received frame must increment regardless of dispatch outcome, got {consumer.frame_count}"
    )
    assert consumer.dropped_frame_count == 1, (
        f"dropped_frame_count: on_event raised → expected 1, got {consumer.dropped_frame_count}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — dropped_frame_count stays at zero on successful on_event
# ─────────────────────────────────────────────────────────────────────────────


def test_dropped_frame_not_incremented_on_success(monkeypatch) -> None:
    """Feed one frame whose on_event succeeds → dropped stays 0; frame_count == 1."""

    received: list[Any] = []

    def good_callback(ev):
        received.append(ev)

    consumer = _make_consumer(
        on_event=good_callback,
        initial_assets=["0xabc"],
    )

    async def fake_stream(asset_ids, initial_dump=True):  # noqa: ARG001
        yield {"asset_id": "0xabc", "type": "tob", "ts_ms": 2}

    monkeypatch.setattr(
        "polyarb.daemon.ws_consumer.stream_market_events",
        fake_stream,
    )

    async def _run() -> None:
        stop_event = asyncio.Event()
        await asyncio.wait_for(consumer.run(stop_event), timeout=2.0)

    asyncio.run(_run())

    assert consumer.frame_count == 1, (
        f"frame_count should be 1, got {consumer.frame_count}"
    )
    assert consumer.dropped_frame_count == 0, (
        f"dropped_frame_count must stay 0 when on_event succeeds, got {consumer.dropped_frame_count}"
    )
    assert received == [{"asset_id": "0xabc", "type": "tob", "ts_ms": 2}]
