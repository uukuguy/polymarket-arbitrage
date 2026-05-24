"""RED tests for polyarb.daemon.ws_watchdog.WsWatchdog state machine.

Plan 04 Wave 0. Drives implementation of:
- 30s silence → state transitions CONNECTED → RECONNECTING + on_reconnect fires
- Exp backoff sequence (1, 2, 4, 8, 16, 30) — D-03 LOCKED + R5 cap
- touch() resets reconnect_attempt to 0
- touch() sets state to WAITING_FOR_EVENT
- Reconnect storm cap: >10 reconnects/hour → DEGRADED_REST_POLLING + Sentry warning
- stop_event cancels watch() within 1.5s
- CancelledError propagates (Phase 02 F-04 invariant — NEVER swallow)
- No false positive when 29s elapsed (just under threshold)
- current_state property surfaces watchdog state to health endpoint

Monkeypatch strategy:
- time.monotonic via monkeypatch.setattr at ws_watchdog import site
- asyncio.sleep via monkeypatch.setattr at ws_watchdog import site (records waits)
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock, patch

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — 30s timeout triggers RECONNECTING + on_reconnect fires
# ─────────────────────────────────────────────────────────────────────────────


async def test_30s_timeout_triggers_RECONNECTING(monkeypatch: pytest.MonkeyPatch) -> None:
    """31s elapsed since touch() → state becomes RECONNECTING; callback invoked."""
    from polyarb.daemon import ws_watchdog as wd_mod

    base = 1000.0
    times = iter([base, base, base, base + 31.0, base + 31.0, base + 31.0, base + 31.0])
    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: next(times, base + 9999.0))

    recorded_sleeps: list[float] = []

    async def _fake_sleep(s: float) -> None:
        recorded_sleeps.append(s)

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    callback_fires: list[int] = []

    def _on_reconnect() -> None:
        callback_fires.append(1)

    watchdog = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect)
    watchdog.touch()

    stop_event = asyncio.Event()

    async def _stopper():
        await asyncio.sleep(0)  # let watch() iterate at least once
        await asyncio.sleep(0)
        stop_event.set()

    async def _short_watch():
        try:
            await asyncio.wait_for(watchdog.watch(stop_event), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(_short_watch(), _stopper())

    assert callback_fires, "on_reconnect was never called"
    assert watchdog.current_state in ("RECONNECTING", "WAITING_FOR_EVENT"), (
        f"unexpected state={watchdog.current_state}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — backoff sequence 1, 2, 4, 8, 16, 30
# ─────────────────────────────────────────────────────────────────────────────


async def test_backoff_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    """6 consecutive stalls → recorded sleep values include 1, 2, 4, 8, 16, 30 in order."""
    from polyarb.daemon import ws_watchdog as wd_mod

    base = 1000.0
    # Always stale: every monotonic() call returns base + huge offset
    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: base + 9999.0)

    recorded_sleeps: list[float] = []

    async def _fake_sleep(s: float) -> None:
        recorded_sleeps.append(s)

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    watchdog = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=None)
    watchdog.touch()  # prime

    stop_event = asyncio.Event()

    async def _stop_after_iters():
        # let watch() loop iterate several times; the fake sleep is instantaneous
        for _ in range(60):
            await asyncio.sleep(0)
        stop_event.set()

    async def _bounded_watch():
        try:
            await asyncio.wait_for(watchdog.watch(stop_event), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(_bounded_watch(), _stop_after_iters())

    # Filter to known backoff values; first 6 backoff sleeps must be [1, 2, 4, 8, 16, 30]
    backoff_only = [s for s in recorded_sleeps if s in (1, 2, 4, 8, 16, 30)]
    assert backoff_only[:6] == [1, 2, 4, 8, 16, 30], (
        f"backoff sequence wrong, got: {backoff_only[:6]}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — touch() resets reconnect_attempt to 0
# ─────────────────────────────────────────────────────────────────────────────


def test_touch_resets_attempt_counter() -> None:
    """3 simulated reconnects (attempt=3), then touch() → attempt back to 0."""
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    # Manually bump attempt as if 3 stalls happened
    wd._state.reconnect_attempt = 3
    assert wd._state.reconnect_attempt == 3
    wd.touch()
    assert wd._state.reconnect_attempt == 0, (
        f"touch() did NOT reset attempt; got {wd._state.reconnect_attempt}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — touch() sets state to WAITING_FOR_EVENT
# ─────────────────────────────────────────────────────────────────────────────


def test_touch_sets_state_waiting_for_event() -> None:
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    wd.touch()
    assert wd.current_state == "WAITING_FOR_EVENT", f"got {wd.current_state!r}"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — reconnect storm cap (>10/hour → DEGRADED_REST_POLLING + Sentry warning)
# ─────────────────────────────────────────────────────────────────────────────


async def test_reconnect_storm_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pre-populate 11 recent reconnect timestamps → next stale triggers DEGRADED + breadcrumb."""
    from polyarb.daemon import ws_watchdog as wd_mod

    base = 1000.0
    # Always stale so the watch loop tries to reconnect
    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: base + 9999.0)

    sleeps: list[float] = []

    async def _fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    breadcrumb_calls: list[dict] = []
    capture_msg_calls: list[tuple[str, dict]] = []

    def _add_breadcrumb(**kw):
        breadcrumb_calls.append(kw)

    def _capture_message(msg, **kw):
        capture_msg_calls.append((msg, kw))

    monkeypatch.setattr(wd_mod.sentry_sdk, "add_breadcrumb", _add_breadcrumb)
    monkeypatch.setattr(wd_mod.sentry_sdk, "capture_message", _capture_message)

    wd = wd_mod.WsWatchdog(stale_s=30.0)
    wd.touch()
    # Pre-populate 11 recent timestamps in the deque window
    for _ in range(11):
        wd._reconnect_timestamps.append(base + 9999.0)

    stop_event = asyncio.Event()

    async def _stopper():
        for _ in range(20):
            await asyncio.sleep(0)
        stop_event.set()

    async def _bounded():
        try:
            await asyncio.wait_for(wd.watch(stop_event), timeout=2.0)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(_bounded(), _stopper())

    assert wd.current_state == "DEGRADED_REST_POLLING", (
        f"storm cap did NOT trigger; state={wd.current_state}"
    )
    # Sentry breadcrumb category=l2-ws level=warning
    found_warn_breadcrumb = any(
        bc.get("category") == "l2-ws" and bc.get("level") == "warning"
        for bc in breadcrumb_calls
    )
    assert found_warn_breadcrumb, f"no l2-ws warning breadcrumb; got: {breadcrumb_calls}"
    assert capture_msg_calls, "capture_message never called on storm cap"


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — stop_event cancels mid-sleep within 1.5s
# ─────────────────────────────────────────────────────────────────────────────


async def test_stop_event_cancels_watch_within_1s() -> None:
    """Set stop_event after 100ms; watch() returns within 1.5s (no swallow)."""
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    wd.touch()
    stop_event = asyncio.Event()

    async def _setter():
        await asyncio.sleep(0.1)
        stop_event.set()

    async def _bounded():
        await asyncio.wait_for(wd.watch(stop_event), timeout=1.5)

    await asyncio.gather(_bounded(), _setter())


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — CancelledError propagates (Phase 02 F-04)
# ─────────────────────────────────────────────────────────────────────────────


async def test_cancelledError_propagates() -> None:
    """task.cancel() mid-watch → CancelledError raised at await site."""
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    wd.touch()
    stop_event = asyncio.Event()

    task = asyncio.create_task(wd.watch(stop_event))
    await asyncio.sleep(0.05)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — no false positive when 29s elapsed (just under threshold)
# ─────────────────────────────────────────────────────────────────────────────


async def test_low_traffic_asset_no_false_positive(monkeypatch: pytest.MonkeyPatch) -> None:
    """29s elapsed (just under 30s) → state stays WAITING_FOR_EVENT, NO reconnect."""
    from polyarb.daemon import ws_watchdog as wd_mod

    base = 1000.0
    # First monotonic = base (for touch), subsequent = base + 29.0 (under stale_s)
    call_count = {"n": 0}

    def _monotonic():
        call_count["n"] += 1
        if call_count["n"] == 1:
            return base
        return base + 29.0

    monkeypatch.setattr(wd_mod.time, "monotonic", _monotonic)

    callback_fires: list[int] = []

    def _on_reconnect():
        callback_fires.append(1)

    wd = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect)
    wd.touch()

    stop_event = asyncio.Event()

    async def _setter():
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        stop_event.set()

    async def _bounded():
        try:
            await asyncio.wait_for(wd.watch(stop_event), timeout=0.5)
        except asyncio.TimeoutError:
            pass

    await asyncio.gather(_bounded(), _setter())

    assert not callback_fires, "false-positive reconnect fired at 29s"
    assert wd.current_state in ("CONNECTED", "WAITING_FOR_EVENT"), (
        f"unexpected state at 29s: {wd.current_state}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 9 — current_state property surfaces internal state
# ─────────────────────────────────────────────────────────────────────────────


def test_current_state_property_exposed() -> None:
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    assert wd.current_state == "CONNECTED"  # initial
    wd.touch()
    assert wd.current_state == "WAITING_FOR_EVENT"
