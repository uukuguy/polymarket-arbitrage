"""RED → GREEN tests for WsWatchdog liveness_check gate.

GAP-401 fix (2026-05-31): watchdog must not false-trip on a healthy-but-quiet
WS connection. When `liveness_check` returns True (socket open + pong seen),
the watchdog treats the silence as benign and resets the baseline instead of
reconnecting.

Tests:
1. liveness_check=lambda:True  → no RECONNECTING after stale_s silence
2. liveness_check=lambda:False → RECONNECTING fires (existing path preserved)
3. liveness_check=None         → RECONNECTING fires (backward compat)
4. liveness mid-watch alive→dead → eventually reconnects
5. liveness_check=True → state stays/becomes WAITING_FOR_EVENT; reconnect_attempt stays 0
6. liveness_check=True → no storm-cap timestamps appended (false-trips don't burn budget)
7. Consumer liveness closure: returns False when ws is None or not OPEN
8. Consumer liveness closure: returns True when ws.state==OPEN and ws.latency>0
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# ─────────────────────────────────────────────────────────────────────────────
# Test 1 — liveness_check=True: no RECONNECTING after stale_s silence
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_alive_no_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """liveness_check=lambda:True — watchdog does NOT enter RECONNECTING after silence.

    Simulates elapsed > stale_s (direct _on_stale call) with liveness_check
    returning True. Expects: state stays WAITING_FOR_EVENT, reconnect_attempt stays 0,
    no storm timestamps appended.
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    reconnect_calls: list[int] = []

    def _on_reconnect() -> None:
        reconnect_calls.append(1)

    wd = wd_mod.WsWatchdog(
        stale_s=30.0,
        on_reconnect=_on_reconnect,
        liveness_check=lambda: True,
    )
    wd.touch()  # → WAITING_FOR_EVENT, attempt=0
    await wd._on_stale()

    assert not reconnect_calls, "on_reconnect MUST NOT fire when socket is alive"
    assert wd.current_state == "WAITING_FOR_EVENT", (
        f"state should stay WAITING_FOR_EVENT when alive; got {wd.current_state!r}"
    )
    assert wd.reconnect_attempt == 0, f"reconnect_attempt should stay 0; got {wd.reconnect_attempt}"
    assert len(wd._reconnect_timestamps) == 0, (
        "no storm-cap timestamp should be appended for a benign silence"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 2 — liveness_check=False: RECONNECTING fires (existing path)
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_dead_triggers_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """liveness_check=lambda:False — existing RECONNECTING path is preserved."""
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    reconnect_calls: list[int] = []

    def _on_reconnect() -> None:
        reconnect_calls.append(1)

    wd = wd_mod.WsWatchdog(
        stale_s=30.0,
        on_reconnect=_on_reconnect,
        liveness_check=lambda: False,
    )
    wd.touch()
    await wd._on_stale()

    assert reconnect_calls, "on_reconnect MUST fire when liveness_check=False"
    assert wd.current_state == "RECONNECTING", (
        f"state should be RECONNECTING when liveness=False; got {wd.current_state!r}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3 — liveness_check=None: RECONNECTING fires (backward compat)
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_none_triggers_reconnect(monkeypatch: pytest.MonkeyPatch) -> None:
    """liveness_check=None (default) — watchdog behaves exactly as before this fix."""
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    reconnect_calls: list[int] = []

    def _on_reconnect() -> None:
        reconnect_calls.append(1)

    wd = wd_mod.WsWatchdog(
        stale_s=30.0,
        on_reconnect=_on_reconnect,
        # liveness_check not passed (None is default)
    )
    wd.touch()
    await wd._on_stale()

    assert reconnect_calls, "on_reconnect MUST fire when liveness_check=None (default)"
    assert wd.current_state == "RECONNECTING"


# ─────────────────────────────────────────────────────────────────────────────
# Test 4 — liveness mid-watch alive→dead → eventually reconnects
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_flip_alive_to_dead_reconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First stale event: liveness alive (no reconnect). Second: liveness dead → reconnects.

    Directly drives _on_stale() twice with a monotonic that advances between calls
    to keep elapsed > stale_s on both calls.
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    _tick = [1000.0]

    def _mono() -> float:
        return _tick[0]

    monkeypatch.setattr(wd_mod.time, "monotonic", _mono)

    slept: list[float] = []

    async def _fake_sleep(s: float) -> None:
        slept.append(s)

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    # Flip: first call alive, second call dead
    _alive = [True, False]
    _call_idx = [0]

    def _liveness() -> bool:
        v = _alive[_call_idx[0] % len(_alive)]
        _call_idx[0] += 1
        return v

    reconnect_calls: list[int] = []

    def _on_reconnect() -> None:
        reconnect_calls.append(1)

    wd = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect, liveness_check=_liveness)
    wd.touch()

    # First stale: liveness=True → no reconnect; baseline resets to monotonic() value (1000.0)
    await wd._on_stale()
    assert not reconnect_calls, "alive branch must not reconnect"

    # Advance tick so elapsed > stale_s on second call too
    _tick[0] = 2000.0
    # Force last_event_time_s back so it's still "stale"
    wd._state.last_event_time_s = 1000.0

    # Second stale: liveness=False → reconnect
    await wd._on_stale()
    assert reconnect_calls, "dead branch must reconnect"
    assert wd.current_state == "RECONNECTING"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5 — liveness_check=True resets baseline (last_event_time_s advances)
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_alive_resets_baseline(monkeypatch: pytest.MonkeyPatch) -> None:
    """When alive branch fires, last_event_time_s must be reset to current monotonic.

    Without this reset, the next watch() iteration would immediately re-enter
    _on_stale() (elapsed would still be > stale_s).
    """
    from polyarb.daemon import ws_watchdog as wd_mod

    _tick = [1000.0]

    def _mono() -> float:
        return _tick[0]

    monkeypatch.setattr(wd_mod.time, "monotonic", _mono)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    wd = wd_mod.WsWatchdog(stale_s=30.0, liveness_check=lambda: True)
    wd._state.last_event_time_s = 900.0  # looks stale relative to 1000.0

    await wd._on_stale()

    # After alive-branch, last_event_time_s must equal the current monotonic()
    assert wd._state.last_event_time_s == 1000.0, (
        f"baseline not reset; last_event_time_s={wd._state.last_event_time_s}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 6 — liveness alive: watch() loop runs multiple stale ticks without reconnect
#          (integration: multiple consecutive benign silence windows)
# ─────────────────────────────────────────────────────────────────────────────


async def test_liveness_alive_multiple_stale_ticks_no_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Three consecutive stale ticks with liveness=True → zero reconnects, 0 storm timestamps."""
    from polyarb.daemon import ws_watchdog as wd_mod

    monkeypatch.setattr(wd_mod.time, "monotonic", lambda: 1000.0)

    async def _fake_sleep(s: float) -> None:
        return

    monkeypatch.setattr(wd_mod.asyncio, "sleep", _fake_sleep)

    reconnect_calls: list[int] = []

    def _on_reconnect() -> None:
        reconnect_calls.append(1)

    wd = wd_mod.WsWatchdog(stale_s=30.0, on_reconnect=_on_reconnect, liveness_check=lambda: True)
    wd.touch()

    for _ in range(3):
        await wd._on_stale()

    assert not reconnect_calls, "no reconnects on 3 benign silence windows"
    assert len(wd._reconnect_timestamps) == 0, "storm-cap budget must not be consumed by silence"
    assert wd.reconnect_attempt == 0, "reconnect_attempt must stay 0"


# ─────────────────────────────────────────────────────────────────────────────
# Test 7 — consumer liveness closure: False when ws is None
# ─────────────────────────────────────────────────────────────────────────────


def test_consumer_liveness_closure_none_ws() -> None:
    """WsConsumer liveness closure returns False when _current_ws is None."""
    from unittest.mock import MagicMock

    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc"],
    )
    # _current_ws must be None initially (not yet connected)
    assert consumer._current_ws is None, "_current_ws must start None"
    # liveness closure must return False
    assert consumer._liveness_check() is False, (
        "_liveness_check() must return False when _current_ws is None"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 8 — consumer liveness closure: False when ws.state != OPEN
# ─────────────────────────────────────────────────────────────────────────────


def test_consumer_liveness_closure_closed_ws() -> None:
    """WsConsumer liveness closure returns False when ws.state is CLOSED."""
    from websockets.protocol import State

    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc"],
    )
    mock_ws = MagicMock()
    mock_ws.state = State.CLOSED
    mock_ws.latency = 0.05
    consumer._current_ws = mock_ws

    assert consumer._liveness_check() is False, (
        "_liveness_check() must return False for CLOSED socket"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 9 — consumer liveness closure: False when ws.latency == 0 (no pong yet)
# ─────────────────────────────────────────────────────────────────────────────


def test_consumer_liveness_closure_no_pong_yet() -> None:
    """WsConsumer liveness closure returns False when latency==0 (pong not yet seen)."""
    from websockets.protocol import State

    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc"],
    )
    mock_ws = MagicMock()
    mock_ws.state = State.OPEN
    mock_ws.latency = 0.0  # not yet pong'd
    consumer._current_ws = mock_ws

    assert consumer._liveness_check() is False, (
        "_liveness_check() must return False when latency==0 (no pong received yet)"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 10 — consumer liveness closure: True when OPEN + latency > 0
# ─────────────────────────────────────────────────────────────────────────────


def test_consumer_liveness_closure_open_with_pong() -> None:
    """WsConsumer liveness closure returns True when OPEN and latency > 0."""
    from websockets.protocol import State

    from polyarb.daemon.ws_consumer import WsConsumer
    from polyarb.daemon.ws_watchdog import WsWatchdog

    wd = WsWatchdog(stale_s=30.0)
    consumer = WsConsumer(
        settings=MagicMock(),
        watchdog=wd,
        on_event=lambda ev: None,
        initial_assets=["0xabc"],
    )
    mock_ws = MagicMock()
    mock_ws.state = State.OPEN
    mock_ws.latency = 0.012  # ~12ms RTT, realistic keepalive pong
    consumer._current_ws = mock_ws

    assert consumer._liveness_check() is True, (
        "_liveness_check() must return True when OPEN and latency > 0"
    )
