---
quick_id: 260531-gap-401-watchdog-false-trip
ws: m1-perception
subsystem: daemon
tags: [websockets, watchdog, liveness, keepalive, false-trip, reconnect-storm]

requires:
  - phase: 04.1-d01-restart-robustness-chaos-redesign
    provides: Prod chaos soak that surfaced GAP-401 (Finding 2 in 04.1-SOAK-LOG.md)

provides:
  - WsWatchdog liveness_check gate (liveness_check: Callable[[], bool] | None param)
  - WsConsumer _current_ws stash + _stash_ws() hook + _liveness_check() closure
  - stream_market_events on_connect side-channel hook

affects:
  - Any future plan that deploys or restarts ws_consumer / ws_watchdog
  - D-06 re-run verdict (false-trips no longer burn storm-cap during quiet windows)

tech-stack:
  added: []
  patterns:
    - "Liveness-probe injection: consumer passes closure to watchdog __init__; watchdog gate reads it in _on_stale() before escalating to RECONNECTING"
    - "Side-channel hook: stream_market_events on_connect is NOT a yielded value — only a callback for consumer bookkeeping"
    - "websockets State enum import: use websockets.protocol.State (canonical in 15+); websockets.connection is deprecated"

key-files:
  created:
    - tests/m1-perception/test_ws_watchdog_liveness.py
  modified:
    - src/polyarb/daemon/ws_watchdog.py
    - src/polyarb/daemon/ws_consumer.py
    - src/polyarb/clients/ws_market_client.py

key-decisions:
  - "liveness = OPEN AND latency > 0 (not just OPEN): latency==0 means no pong yet — conservative False to keep _on_stale as fallback"
  - "Watchdog liveness_check is injected via __init__ param (not hardcoded); backward-compat: default None = existing reconnect path"
  - "stale_s=30.0 UNCHANGED (D-03 LOCKED); storm-cap thresholds UNCHANGED — only the false-trip elimination is new"
  - "Use websockets.protocol.State (not deprecated websockets.connection.State) for State.OPEN comparison"

patterns-established:
  - "Liveness probe: consumer owns the ws reference; watchdog reads a closure — decoupled, testable, no circular import"

requirements-completed: []

duration: 30min
completed: 2026-05-31
---

# Quick Task GAP-401: Watchdog false-trip fix Summary

**Liveness gate in WsWatchdog prevents false reconnects when market is quiet but socket is healthy (OPEN + keepalive pong), fixing the reconnect-storm-cap burndown that stuck /health at DISCONNECTED for hours during low-activity windows.**

## Performance

- **Duration:** ~30 min
- **Started:** 2026-05-31T02:00Z (approx)
- **Completed:** 2026-05-31T02:34Z
- **Tasks:** 2 (TDD RED + GREEN)
- **Files modified:** 4

## Accomplishments

- Root cause (GAP-401 Finding 2 from 04.1 soak): WsWatchdog unconditionally reconnected on 30s silence even when websockets keepalive pings were flowing — burning the 10/hour storm-cap in quiet markets.
- WsWatchdog gains liveness_check: Callable[[], bool] | None = None param. In _on_stale(), if check returns True, baseline resets silently (no RECONNECTING, no storm timestamp, no backoff).
- WsConsumer stashes live ws object via on_connect hook; builds _liveness_check() closure returning ws.state is OPEN and ws.latency > 0.
- stream_market_events gains on_connect side-channel; iterator still yields ONLY event dicts (abstraction intact).
- 10 new tests covering all liveness branches. pyright 0 errors. 31 regression tests green.

## Task Commits

1. **Task 1 RED** - `a41ef23` (test: RED — 9/10 liveness gate tests failing as expected)
2. **Task 2 GREEN** - `fb5e271` (feat: GREEN — liveness gate implementation + test fixes)

## Files Created/Modified

- `tests/m1-perception/test_ws_watchdog_liveness.py` — 10 new liveness tests (RED->GREEN)
- `src/polyarb/daemon/ws_watchdog.py` — liveness_check param + _on_stale() gate
- `src/polyarb/daemon/ws_consumer.py` — _current_ws, _stash_ws(), _liveness_check(), wiring
- `src/polyarb/clients/ws_market_client.py` — on_connect hook param

## Decisions Made

1. Liveness = OPEN AND latency > 0: latency==0 before first pong; conservative False keeps _on_stale as fallback.
2. Closure injection: WsConsumer.__init__ sets self._watchdog._liveness_check = self._liveness_check (no circular dep).
3. stale_s=30.0 and storm-cap UNCHANGED — D-03 LOCKED fully respected.
4. websockets.protocol.State used (canonical in 15+; websockets.connection deprecated).

## Deviations from Plan

None - plan executed exactly as written.

## Known Stubs

None.

## Threat Flags

None.

## User Setup Required

None.

## Next Phase Readiness

- Ready to ship with next L2 deploy alongside un-deployed 04.1 code-review fixes.
- D-06 re-run should now show ws_state returning to WAITING_FOR_EVENT post-storm with no false-trip burndown.

---
*Quick task: 260531-gap-401-watchdog-false-trip*
*Completed: 2026-05-31*

## Self-Check: PASSED

- tests/m1-perception/test_ws_watchdog_liveness.py: 10/10 pass
- pyright: 0 errors on all 3 source files
- Commit a41ef23 (RED) and fb5e271 (GREEN): confirmed present
- stale_s = 30.0 literal + LOCKED annotation: confirmed present in ws_watchdog.py
