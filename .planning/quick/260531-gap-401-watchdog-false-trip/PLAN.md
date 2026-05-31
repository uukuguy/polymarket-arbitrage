---
quick_id: 260531-gap-401-watchdog-false-trip
type: quick
mode: tdd
ws: m1-perception
created: 2026-05-31
files_modified:
  - src/polyarb/daemon/ws_watchdog.py
  - src/polyarb/daemon/ws_consumer.py
  - src/polyarb/clients/ws_market_client.py
  - tests/m1-perception/test_ws_watchdog_liveness.py
---

# Quick Task: GAP-401 watchdog false-trip fix

## Problem (root cause, confirmed 2026-05-30 prod chaos — see 04.1-SOAK-LOG.md Finding 2)

`WsWatchdog.touch()` is called ONLY on incoming WS **data frames** (`ws_consumer.py:208`).
In a quiet market (no events), no frames arrive → `last_event_time_s` never updates →
`watch()` sees `elapsed > stale_s (30s)` → `_on_stale()` fires a RECONNECTING even though
the socket is perfectly alive. These false-trips during a HEALTHY-but-quiet window burn the
`_STORM_THRESHOLD=10`/hour reconnect storm-cap → watchdog degrades to DEGRADED_REST_POLLING
and sticks DISCONNECTED (`/health overall=fail`) for the rest of the hour.

## Design (LOCKED — user-approved liveness-touch, 2026-05-31)

The `websockets` lib runs protocol-level ping/pong keepalive (`ping_interval=10, ping_timeout=10`
in `ws_market_client.py:74-75`). A healthy-but-quiet socket still exchanges pings; a genuinely
frozen socket (issue #292) fails `ping_timeout` → the lib CLOSES the connection → the
`async for ws in websockets.connect(...)` iterator reconnects on its own → fresh `touch()`.

**So the lib already handles true freezes.** The fix: the app-level watchdog must NOT fire a
data-silence reconnect when the underlying socket is provably alive (pongs flowing).

Wiring (minimal, decoupled):
- `WsWatchdog.__init__` gains optional `liveness_check: Callable[[], bool] | None = None`.
- In `watch()`, when `elapsed > stale_s`, BEFORE calling `_on_stale()`: if `liveness_check`
  is set AND returns True (socket alive), treat as benign quiet — reset the silence baseline
  (`last_event_time_s = monotonic()`, state stays/becomes WAITING_FOR_EVENT) and continue.
  Do NOT touch reconnect_attempt or storm timestamps. If `liveness_check` is None or returns
  False → existing `_on_stale()` path unchanged (preserves #292 detection if lib somehow misses).
- `stream_market_events` gains optional `on_connect: Callable[[Any], None] = None` hook called
  once per (re)connect with the live `ws` object, so the consumer can stash the current
  connection for liveness reads. (Iterator still yields ONLY event dicts — abstraction intact.)
- `WsConsumer`: holds `self._current_ws`; passes `on_connect=self._stash_ws` to
  `stream_market_events`; builds a `liveness_check` closure read by the watchdog that returns
  True when the current ws is open AND its keepalive latency is fresh (pong seen recently).
  Liveness = `ws is not None and ws.state is OPEN and ws.latency > 0` (latency is 0.0 only
  before the first pong; once keepalive runs it's a positive RTT, refreshed each ping cycle).

### Why this respects the locks
- `stale_s = 30.0` UNCHANGED (D-03 LOCKED — "DO NOT make user-configurable"). We don't raise it.
- #292 silent-freeze still caught: lib's `ping_timeout` closes a frozen socket → reconnect.
  If the lib's keepalive itself is the thing that froze, `ws.latency` goes stale and
  `liveness_check` returns False → watchdog falls through to `_on_stale()` (belt + suspenders).
- Storm-cap logic UNCHANGED — it just stops being consumed by false-trips.

## Tasks (TDD)

### Task 1 (RED→GREEN): WsWatchdog liveness gate
- RED: `test_ws_watchdog_liveness.py` — watchdog with `liveness_check=lambda: True` does NOT
  enter RECONNECTING after stale_s of silence (stays WAITING_FOR_EVENT, reconnect_attempt
  stays 0, no storm timestamp appended). With `liveness_check=lambda: False` (or None) it DOES
  reconnect (existing behavior preserved). A flip mid-watch (alive→dead) eventually reconnects.
- GREEN: implement the `liveness_check` gate in `WsWatchdog.__init__` + `watch()`.

### Task 2 (GREEN): wire liveness through consumer + client
- `ws_market_client.stream_market_events`: add `on_connect` hook (called with `ws` each connect).
- `ws_consumer`: stash current ws, build liveness closure, pass `liveness_check` to the watchdog
  (the watchdog is injected in `__init__` — wire the closure where the consumer owns the loop).
- Add a focused test that the consumer's liveness closure returns False when ws is None /
  not open, True when open + latency>0 (mock ws).

## Verify
- `uv run python -m pytest -q tests/m1-perception/test_ws_watchdog_liveness.py` green
- `uv run python -m pytest -q -k "watchdog or ws_consumer or ws_market or l2_health" tests/m1-perception/` green (no regression)
- `uv run pyright src/polyarb/daemon/ws_watchdog.py src/polyarb/daemon/ws_consumer.py src/polyarb/clients/ws_market_client.py` → 0 errors
- `stale_s = 30.0` literal still present + still annotated LOCKED in ws_watchdog.py

## Out of scope
- No prod deploy (ship with next L2 deploy alongside the un-deployed 04.1 code-review fixes).
- No change to storm-cap thresholds or backoff sequence.
