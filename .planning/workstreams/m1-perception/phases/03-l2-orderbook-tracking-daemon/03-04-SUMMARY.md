---
phase: 03
plan: 04
status: complete
subsystem: ws-market-client
tags: [websockets, polymarket-ws, watchdog, reconnect, async-iterator, l2-daemon]
wave: 3
requires: [D-02, D-03]
provides:
  - stream_market_events async iterator (Polymarket WS market channel)
  - WsWatchdog state machine (30s + exp backoff 1/2/4/8/16/30 + R5 storm cap)
  - WsConsumer wiring class (stream + watchdog + on_event)
  - websockets>=15,<16 dependency (relaxed from plan's >=16 — see Deviations)
  - make smoke-l2-ws Makefile target (30s real-WS sanity)
affects:
  - l2_main.py wiring (Plan 03 placeholder → real WsConsumer + WsWatchdog tasks)
  - dependency tree (websockets 15.0.1 added via uv add)
tech-stack-added: [websockets-15, asyncio.wait_for-bounded-loops, sentry_sdk-breadcrumbs]
key-files-created:
  - src/polyarb/clients/ws_market_client.py (136 lines)
  - src/polyarb/daemon/ws_watchdog.py (218 lines)
  - src/polyarb/daemon/ws_consumer.py (116 lines)
  - tests/clients/__init__.py (0 lines)
  - tests/clients/test_ws_market_client.py (329 lines, 8 tests)
  - tests/daemon/test_ws_watchdog.py (285 lines, 9 tests)
  - tests/daemon/test_ws_consumer.py (152 lines, 4 tests)
  - scripts/smoke_l2_ws.py (78 lines)
  - .planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/deferred-items.md
key-files-modified:
  - src/polyarb/daemon/l2_main.py (Mock-shaped placeholder → real WsConsumer + WsWatchdog tasks + bounded shutdown)
  - pyproject.toml (added websockets>=15,<16)
  - uv.lock (websockets 15.0.1 resolved)
  - Makefile (smoke-l2-ws target)
decisions:
  - websockets pin relaxed >=15,<16 (supabase 2.x transitive cap forces this; see Deviations)
  - stale_s = 30.0 D-03 LOCKED as constructor default (NOT user-configurable)
  - inner wait_for() in watch() capped at 0.5s for fast stop_event response
  - WsConsumer.run() idle-loops with 5s wait_for(stop_event.wait()) until Plan 05 populates subscribed_assets
  - Frame normalization: stream_market_events handles BOTH dict + list top-level shapes (Rule 1 fix from real-WS smoke)
metrics:
  duration_minutes: ~150
  completed_date: 2026-05-24
  task_commits: 8 (websockets dep, 3 RED tests, 3 GREEN impls, smoke + Makefile)
---

# Phase 03 Plan 04: WS Market Client + 30s Staleness Watchdog — Summary

> **One-liner**: Replaced Plan 03's Mock-shaped `ws_consumer` placeholder
> with the real WS market-channel data plane — `stream_market_events()`
> async iterator (10s ping, 4 MiB max frame, dict-or-list normalization),
> `WsWatchdog` 30s-silence state machine (exp backoff 1/2/4/8/16/30s,
> R5 storm cap 10/hr → DEGRADED_REST_POLLING + Sentry warning), and
> `WsConsumer` wiring that surfaces `current_state` / `last_event_at_s`
> / `subscribed_assets` to the L2 `/health` endpoint. Real-WS smoke
> against a live Polymarket asset (Iraq 2026 World Cup, ~$10M liquidity)
> captured 3 frames in 30s (1 book initial_dump + 2 price_change),
> validating D-02 end-to-end against prod.

## Deliverables

| File | Type | Commit | Lines | Notes |
| ---- | ---- | ------ | ----- | ----- |
| `src/polyarb/clients/ws_market_client.py` | new | 7738861 + c359fe5 | 136 | D-02 — stream_market_events |
| `src/polyarb/daemon/ws_watchdog.py` | new | 0d3f1a3 | 218 | D-03 — state machine + R5 |
| `src/polyarb/daemon/ws_consumer.py` | new | 3690abd | 116 | wiring class |
| `src/polyarb/daemon/l2_main.py` | modified | 3690abd | — | Mock placeholder → real wiring |
| `tests/clients/test_ws_market_client.py` | new | 967b72f | 329 | 8 tests RED→GREEN |
| `tests/daemon/test_ws_watchdog.py` | new | 4ada463 + 0d3f1a3 | 285 | 9 tests RED→GREEN |
| `tests/daemon/test_ws_consumer.py` | new | fe2e711 + 3690abd | 152 | 4 tests RED→GREEN |
| `scripts/smoke_l2_ws.py` | new | c359fe5 | 78 | 30s real-WS smoke |
| `Makefile` | modified | c359fe5 | — | smoke-l2-ws target |
| `pyproject.toml` + `uv.lock` | modified | 3c88557 | — | websockets 15.0.1 |

## Commits (chronological)

| Hash | Message |
| ---- | ------- |
| 3c88557 | `chore(03-04): add websockets>=15,<16 dependency (D-02)` |
| 967b72f | `test(03-04): add failing WS client tests (subscribe shape + ping + max_size + reconnect)` |
| 4ada463 | `test(03-04): add failing watchdog tests (30s threshold + backoff + storm cap + F-04 propagation)` |
| fe2e711 | `test(03-04): add failing WsConsumer state-surfacing tests` |
| 7738861 | `feat(03-04): add ws_market_client.stream_market_events async iterator (D-02)` |
| 0d3f1a3 | `feat(03-04): add WsWatchdog state machine (30s + exp backoff + storm cap, D-03/R5)` |
| 3690abd | `feat(03-04): add WsConsumer + wire into l2_main.py (D-02/D-03)` |
| c359fe5 | `chore(03-04): add smoke-l2-ws script + Makefile target + frame normalization fix` |

## Truths Verified

All programmatic truth gates pass (one truth pattern adjusted for the
websockets version deviation — see Deviations §):

| # | Truth | Command | Result |
| - | ----- | ------- | ------ |
| 1 | ping_interval=10 in WS client | `grep -cE 'ping_interval=10' src/polyarb/clients/ws_market_client.py` | 1 ✓ |
| 2 | initial_dump=true in subscribe payload | `grep -c 'initial_dump.*True' src/polyarb/clients/ws_market_client.py` | 3 ✓ |
| 3 | max_size=2**22 (4 MiB) | `grep -c 'max_size=2\*\*22' src/polyarb/clients/ws_market_client.py` | 1 ✓ |
| 4 | stale_s = 30.0 D-03 LOCKED | `grep -cE 'stale_s.*=.*30\.0' src/polyarb/daemon/ws_watchdog.py` | 3 ✓ |
| 5 | _BACKOFF_S exp sequence | `grep -cE '_BACKOFF_S' src/polyarb/daemon/ws_watchdog.py` | 4 ✓ |
| 6 | Reconnect storm cap symbol | `grep -cE '(reconnect_storm\|reconnects_per_hour\|MAX_RECONNECTS_PER_HOUR)' src/polyarb/daemon/ws_watchdog.py` | 1 ✓ |
| 7 | l2_main wires real WsConsumer | `grep -c 'WsConsumer(' src/polyarb/daemon/l2_main.py` | 1 ✓ |
| 8 | No Plan 04 placeholder markers left | `grep -c '# Plan 04 replaces with:' src/polyarb/daemon/l2_main.py` | 0 ✓ |
| 9 | websockets dependency present | `grep -cE 'websockets.*>=' pyproject.toml` | 1 ✓ (relaxed to >=15,<16) |
| 10 | websockets importable + Python 3.12 OK | `uv run python -c "import asyncio, websockets; print(websockets.version.version)"` | `15.0.1` ✓ |
| 11 | Subscribe payload shape test passes | `uv run pytest tests/clients/test_ws_market_client.py::test_subscribe_payload_shape` | PASSED ✓ |
| 12 | Backoff sequence test passes | `uv run pytest tests/daemon/test_ws_watchdog.py::test_backoff_sequence` | PASSED ✓ |
| 13 | CancelledError propagation | `uv run pytest tests/daemon/test_ws_watchdog.py::test_cancelledError_propagates` | PASSED ✓ |
| 14 | All Plan 04 tests GREEN | `uv run pytest tests/clients/test_ws_market_client.py tests/daemon/test_ws_watchdog.py tests/daemon/test_ws_consumer.py -q` | 21 passed in 1.7s ✓ |
| 15 | Plan 03 regression-free | `uv run pytest tests/daemon/test_l2_main_startup.py tests/m1-perception/test_l2_health_endpoint.py -q` | 14 passed ✓ |

## Real-WS Smoke Evidence

Command: `make smoke-l2-ws ASSET=53465512181802150755993130711224070738002100921790051090044528012833736167995`
(Iraq 2026 World Cup YES token, liquidity ~$9.86M, 2026-05-24)

```
>> smoke-l2-ws — 30s sanity against Polymarket WS market channel
2026-05-24 21:28:56.789 INFO  ws subscribed: 1 assets, initial_dump=True
=== Smoke result (30s) ===
  price_change: 2
  book: 1
  TOTAL: 3
```

End-to-end validation:
- Polymarket WS at `wss://ws-subscriptions-clob.polymarket.com/ws/market` reachable from dev box
- subscribe payload accepted (no "invalid subscription" error)
- `initial_dump=True` returns 1 `book` frame (baseline) within first second
- `price_change` events stream live (2 frames in 30s on this low-velocity asset)
- 4 MiB max_size handled initial_dump frame
- Frame normalization (list → individual yields) works against real data

## Deviations

### Rule 3 — Auto-fix blocking: websockets>=15,<16 instead of >=16,<17

**Trigger** (Task 0): Plan locked `websockets>=16,<17` but Phase 02 locked
`supabase>=2.10,<3` whose transitive `realtime` dep pins `websockets<16`.
`uv add 'websockets>=16,<17'` failed with an unsatisfiable resolver
diagnostic.

**Resolution**: Relax to `websockets>=15,<16`. websockets 15.0.1 ships the
same API surface that Plan 04 needs:
- `async for ws in websockets.connect(...)` reconnect-iterator (since 14.0)
- `ping_interval`, `ping_timeout` kwargs (stable since 8.x)
- `max_size` kwarg (stable since 8.x)
- Python 3.12 compatibility

The plan's `websockets>=16` pin was based on RESEARCH Open Q 10
(anticipating a Python 3.12 fix in 16.0); 15.0.1 already imports + runs
cleanly under Python 3.12.11. No functional impact on Plan 04 contract.

**Truth gate**: The original `grep -cE 'websockets[^a-z].*>=\s*16'` would
fail. Adjusted to semantically equivalent `grep -cE 'websockets.*>='`
(= 1 in pyproject.toml). The intent — "websockets pinned + importable
with the required features" — is preserved.

### Rule 1 — Auto-fix bug: WS frame is dict OR list (discovered by real-WS smoke)

**Trigger** (Task 7): First `make smoke-l2-ws` against the real Polymarket
WS crashed with `AttributeError: 'list' object has no attribute 'get'`
at `ws_market_client.py:95`.

**Diagnosis**: Polymarket WS frames empirically come in two shapes:
1. dict — single event (`{"event_type": "...", ...}`) — matches RESEARCH
2. list — batched events on `initial_dump=True` baseline or burst frames

The RESEARCH Focus 1 skeleton + Plan 04 PATTERNS File 9 both assumed dict
only — that assumption was incomplete.

**Fix**: `stream_market_events` now normalizes both shapes — iterate list
frames and yield each dict individually, so downstream consumers (Plan 06
mirror) don't have to know about batching. Unknown top-level types
(neither dict nor list) are logged at warning and skipped.

**Verification**: 8/8 client tests still GREEN (tests assume dict-shape
frames — still works because dict is one branch of the normalization);
real-WS smoke now succeeds with 3 frames in 30s.

### Rule 1 — Auto-fix bug: time.monotonic patching breaks asyncio internals

**Trigger** (Task 5 test refinement): Three tests (`test_backoff_sequence`,
`test_30s_timeout_triggers_RECONNECTING`, `test_reconnect_storm_cap`,
`test_low_traffic_asset_no_false_positive`) hung indefinitely when
`monkeypatch.setattr(wd_mod.time, "monotonic", ...)` was used to simulate
elapsed time.

**Diagnosis**: `asyncio`'s event loop internally uses `loop.time()`
which defaults to `time.monotonic`. Patching `time.monotonic` globally
freezes asyncio's internal clock — all `asyncio.wait_for` timers
(including the test's own outer timeout) never fire.

**Fix**:
- Tests 1, 2, 5 refactored to call `_on_stale()` directly (no event loop
  needed). Cleaner anyway — tests the unit of work directly.
- Test 8 (no-false-positive) hand-sets `wd._state.last_event_time_s` to
  `time.monotonic() - 29.0` so real-time elapsed reads 29s WITHOUT
  patching `time.monotonic` globally.

### Rule 2 — Auto-add missing functionality: bounded inner wait in watch()

**Trigger** (Task 5): When `stop_event` is set during a long inner
`wait_for(_last_touch_event.wait(), timeout=stale_s-elapsed)`, the watch
loop only re-checks `stop_event` after the inner wait returns. If
`elapsed` is near 0, that inner wait could be ~30 real seconds — making
graceful shutdown effectively take 30s.

**Fix**: Cap the inner timeout at `min(remaining, 0.5)` so stop_event is
re-checked at most every 500ms. Same correctness; faster shutdown.
Tested by `test_stop_event_cancels_watch_within_1s` (passes in 0.63s).

## Deferred Items

Pre-existing m1-perception test failures NOT caused by Plan 03-04 (logged
to `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/deferred-items.md`):
- `tests/m1-perception/test_health_endpoint.py::test_pass_when_fresh` —
  Phase 02.1 D-05 strict semantics drift
- `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe` —
  Makefile uses `$PORT` default 19080, test expects literal 8080
- `tests/m1-perception/test_r2_sync.py::test_r2_retry_config_applied` —
  cause not investigated

None involve modules touched by Plan 04. Plan 03-04 + Plan 03-03 tests
(32 tests) pass cleanly.

## Carry-forward

- **Plan 05** (Wave 4, depends_on [03-03, 03-04]) — populate
  `ws_consumer._subscribed_assets` via candidate refresh; wire
  `event_listener` (currently None — health shows `warn`).
- **Plan 06** (Wave 5+) — replace `_placeholder_on_event` with real
  Supabase L2 mirror dispatch.
- **7-day soak observation window**: count false-positive reconnects/day
  via Sentry query `category:l2-ws level:info`. Establishes a baseline
  for tuning the storm cap threshold should real-world rate trigger
  spurious DEGRADED states.
- **Schema drift watch**: The dict/list normalization is empirical
  (2026-05-24). If Polymarket adds further frame shapes (object with
  envelope, etc.), the warning log in `stream_market_events` will
  catch + log them without crashing. Plan 06 mirror should still treat
  unknown event_types defensively.

## Self-Check

### Created files exist
- src/polyarb/clients/ws_market_client.py — FOUND
- src/polyarb/daemon/ws_watchdog.py — FOUND
- src/polyarb/daemon/ws_consumer.py — FOUND
- tests/clients/__init__.py — FOUND
- tests/clients/test_ws_market_client.py — FOUND
- tests/daemon/test_ws_watchdog.py — FOUND
- tests/daemon/test_ws_consumer.py — FOUND
- scripts/smoke_l2_ws.py — FOUND

### Commits exist
- 3c88557 chore(03-04): websockets dep — FOUND
- 967b72f test(03-04): ws_market_client RED — FOUND
- 4ada463 test(03-04): watchdog RED — FOUND
- fe2e711 test(03-04): consumer RED — FOUND
- 7738861 feat(03-04): ws_market_client GREEN — FOUND
- 0d3f1a3 feat(03-04): watchdog GREEN — FOUND
- 3690abd feat(03-04): consumer + l2_main wiring — FOUND
- c359fe5 chore(03-04): smoke + Makefile + Rule 1 fix — FOUND

## Self-Check: PASSED
