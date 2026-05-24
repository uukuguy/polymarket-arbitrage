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
    """When elapsed > stale_s, _on_stale() runs reconnect path: callback fires + state RECONNECTING.

    Direct test of the stale-handling path. Equivalent to the 30s threshold
    semantic since watch() always calls _on_stale() once elapsed > stale_s
    (verified separately by test_low_traffic_asset_no_false_positive).
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    callback_fires: list[int] = []

    def _on_reconnect() -> None:
        callback_fires.append(1)

    wd = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect)
    wd.touch()
    await wd._on_stale()

    assert callback_fires, "on_reconnect was never called"
    assert wd.current_state == "RECONNECTING", (
        f"unexpected state={wd.current_state}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — backoff sequence 1, 2, 4, 8, 16, 30
# ─────────────────────────────────────────────────────────────────────────────


async def test_backoff_sequence(monkeypatch: pytest.MonkeyPatch) -> None:
    """Direct test of _on_stale backoff progression: 1, 2, 4, 8, 16, 30 (capped).

    Bypasses watch() loop; calls _on_stale() 6 times directly and records
    the values passed to asyncio.sleep. Watchdog's reconnect_attempt counter
    advances on each call.
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    # Stable monotonic — _on_stale's sleep duration depends only on
    # reconnect_attempt + _BACKOFF_S table, not on monotonic.
    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    recorded_sleeps: list[float] = []

    async def _fake_sleep(s: float) -> None:
        recorded_sleeps.append(s)

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    wd = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=None)
    wd.touch()  # prime

    # Drive 6 consecutive stalls — _on_stale advances reconnect_attempt each call
    for _ in range(6):
        await wd._on_stale()

    backoff_only = [s for s in recorded_sleeps if s in (1, 2, 4, 8, 16, 30)]
    assert backoff_only == [1, 2, 4, 8, 16, 30], (
        f"backoff sequence wrong, got: {backoff_only}"
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
    """11 recent reconnects in window → _on_stale takes the DEGRADED branch.

    Pre-populates the sliding-window deque so the next _on_stale() invocation
    exceeds _STORM_THRESHOLD and short-circuits to DEGRADED_REST_POLLING.
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    breadcrumb_calls: list[dict] = []
    capture_msg_calls: list[tuple[str, dict]] = []

    def _add_breadcrumb(**kw):
        breadcrumb_calls.append(kw)

    def _capture_message(msg, **kw):
        capture_msg_calls.append((msg, kw))

    monkeypatch.setattr(wd_mod.sentry_sdk, "add_breadcrumb", _add_breadcrumb)
    monkeypatch.setattr(wd_mod.sentry_sdk, "capture_message", _capture_message)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    wd = wd_mod.WsWatchdog(stale_s=30.0)
    wd.touch()
    # Pre-populate 11 recent timestamps within the storm window (cutoff = 1000 - 3600 = -2600)
    for _ in range(11):
        wd._reconnect_timestamps.append(1000.0)

    await wd._on_stale()

    assert wd.current_state == "DEGRADED_REST_POLLING", (
        f"storm cap did NOT trigger; state={wd.current_state}"
    )
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


async def test_low_traffic_asset_no_false_positive() -> None:
    """29s elapsed (just under 30s) → state stays WAITING_FOR_EVENT, NO reconnect.

    Hand-set the watchdog's last_event_time_s to (real monotonic - 29) so
    real-time elapsed reads just under stale_s. Stop after 0.7s and assert
    on_reconnect was NEVER called. Does NOT patch time.monotonic — that
    would break asyncio's internal loop.time() which is also time.monotonic.
    """
    import time as _time
    from polyarb.daemon.ws_watchdog import WsWatchdog

    callback_fires: list[int] = []

    def _on_reconnect():
        callback_fires.append(1)

    wd = WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect)
    # Force last_event_time_s to look like 29s ago — well under the 30s threshold
    wd._state.last_event_time_s = _time.monotonic() - 29.0
    wd._state.state = "WAITING_FOR_EVENT"

    stop_event = asyncio.Event()

    async def _setter():
        await asyncio.sleep(0.7)
        stop_event.set()

    async def _bounded():
        try:
            await asyncio.wait_for(wd.watch(stop_event), timeout=2.0)
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
