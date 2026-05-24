---
phase: 03
plan: 05
status: complete
subsystem: event-bus-and-candidate-refresh
tags: [asyncpg, postgres-notify, listen, candidate-refresh, scanner-reuse, watchlist]
wave: 4
requires: [D-04, D-05]
provides:
  - polyarb.events.bus.publish_snapshot_complete (asyncpg fire-and-forget NOTIFY, fail-soft)
  - polyarb.events.listener.listen_snapshot_complete + catchup_from_cursor (LISTEN consumer + drop mitigation)
  - polyarb.observation.l2_candidate_refresh.compute_candidates + diff_candidate_sets + on_snapshot_complete
  - L1 orchestrator step 7.7 — fan-out NOTIFY (feature-flag POLYARB_EVENT_BUS_ENABLED default FALSE, fail-soft)
  - asyncpg>=0.31,<0.32 dependency
affects:
  - L1 orchestrator (NEW step 7.7 — gated by event_bus_enabled; default FALSE, opt-in via Fly secret)
  - L2 daemon l2_main.py wiring (event_listener None → real _EventListenerWrapper + listener task)
  - candidate set size cap enforced at 500 assets per R9 (watchlist always retained)
tech-stack-added: [asyncpg-0.31, postgres-listen-notify, scanner-reuse-from-phase-01.1]
key-files-created:
  - src/polyarb/events/__init__.py (19 lines) — module marker + re-exports
  - src/polyarb/events/bus.py (82 lines) — D-05 publisher (fail-soft envelope, info+warning breadcrumbs)
  - src/polyarb/events/listener.py (144 lines) — D-05 LISTEN consumer + catchup_from_cursor
  - src/polyarb/observation/l2_candidate_refresh.py (262 lines) — D-04 union + R9 cap + 60s debounce
  - tests/events/__init__.py (0 lines)
  - tests/events/test_bus_publish.py (157 lines, 5 tests)
  - tests/events/test_listener_catchup.py (192 lines, 8 tests)
  - tests/observation/__init__.py (0 lines)
  - tests/observation/test_l2_candidate_refresh.py (420 lines, 10 tests)
key-files-modified:
  - src/polyarb/config.py (+27 lines) — event_bus_enabled + candidate_scanner_yaml + candidate_watchlist_yaml
  - src/polyarb/snapshot/orchestrator.py (+24 lines) — step 7.7 NOTIFY fan-out (fail-soft, flagged)
  - src/polyarb/daemon/l2_main.py (~+60 lines) — _EventListenerWrapper, dispatch bridge, listener task, catchup at startup, shutdown propagation
  - tests/m1-perception/test_orchestrator.py (+131 lines) — 4 step 7.7 tests
  - pyproject.toml + uv.lock — asyncpg 0.31.0 resolved
  - Makefile — `smoke-event-bus` target
decisions:
  - event_bus_enabled DEFAULT FALSE (B1 spawn constraint overrides RESEARCH Open Q 6); opt-in via Fly secret only after Plan 07 chaos PASS for Inj L2-3
  - MAX_CANDIDATES=500 hard cap (R9) with watchlist always retained (never truncated)
  - REFRESH_DEBOUNCE_S=60.0 (SP8 cross-bug #1 + R1)
  - Phase 02.2 backlog applied preemptively (Open Q 9) — bus.py success path also emits info breadcrumb
  - asyncpg pinned >=0.31,<0.32 (matches plan; 0.31.0 resolved cleanly, no transitive conflicts)
  - catchup_from_cursor defensive vs Plan 06 (UndefinedTableError → []; cross-plan ordering preserved)
  - _EventListenerWrapper shim in l2_main.py — keeps listener.py pure of health-surface state
metrics:
  duration_minutes: ~95
  completed_date: 2026-05-24
  task_commits: 9 (1 chore setup, 4 RED tests, 4 GREEN impls; one fix amends a test stability issue)
---

# Phase 03 Plan 05: Event Bus + Candidate Refresh — Summary

> **One-liner**: Closed the L1↔L2 control-plane loop — L1 orchestrator
> step 7.7 publishes `snapshot_complete` events via Postgres NOTIFY
> (asyncpg, fail-soft, feature-flagged OFF by default per B1); L2 daemon
> subscribes through `listen_snapshot_complete` with a 5s-reconnect outer
> loop + cursor catch-up (defensive vs Plan 06 table absence); and
> `l2_candidate_refresh.on_snapshot_complete` recomputes the union of
> scanner-recipes ∪ watchlist (D-04 verbatim REUSE from Phase 01.1),
> applies the R9=500-asset hard cap with watchlist retained, runs
> through a 60s debounce floor, and mutates `ws_consumer._subscribed_assets`
> per the Plan 04 contract. All 9 plan tasks land, 27 new tests RED→GREEN
> + 4 orchestrator extensions GREEN, zero regressions across the relevant
> 75-test scope (events / observation / orchestrator / l2-health / daemon).

## Deliverables

| File | Type | Commit | Lines | Notes |
| ---- | ---- | ------ | ----- | ----- |
| `src/polyarb/events/__init__.py` | new | aef8c79 | 19 | re-exports publish + listen + catchup |
| `src/polyarb/events/bus.py` | new | aef8c79 | 82 | D-05 publisher (fail-soft envelope, dual breadcrumbs) |
| `src/polyarb/events/listener.py` | new | aef8c79 | 144 | D-05 LISTEN + catchup_from_cursor (defensive vs Plan 06) |
| `src/polyarb/observation/l2_candidate_refresh.py` | new | 15cc5ab | 262 | D-04 union + R9 cap + 60s debounce |
| `src/polyarb/snapshot/orchestrator.py` | modified | 2723b01 | +24 | step 7.7 NOTIFY (gated + fail-soft) |
| `src/polyarb/daemon/l2_main.py` | modified | 2723b01 | ~+60 | _EventListenerWrapper + dispatch + listener task + catchup |
| `src/polyarb/config.py` | modified | 66c6d5e | +27 | event_bus_enabled + candidate_scanner_yaml + candidate_watchlist_yaml |
| `pyproject.toml` + `uv.lock` | modified | 66c6d5e | — | asyncpg 0.31.0 |
| `Makefile` | modified | (final) | +9 | smoke-event-bus target |
| `tests/events/test_bus_publish.py` | new | 53fd949 + aef8c79 | 157 | 5 tests RED→GREEN |
| `tests/events/test_listener_catchup.py` | new | 19927ef + aef8c79 + 09870f0 | 192 | 8 tests RED→GREEN |
| `tests/observation/test_l2_candidate_refresh.py` | new | c8194c3 + 15cc5ab | 420 | 10 tests RED→GREEN |
| `tests/m1-perception/test_orchestrator.py` | modified | 6a16e41 + 2723b01 | +131 | 4 step 7.7 tests RED→GREEN |

## Commits (chronological)

| Hash | Message |
| ---- | ------- |
| 66c6d5e | `chore(03-05): add asyncpg dep + event_bus_enabled setting (D-05)` |
| 53fd949 | `test(03-05): add failing bus.py tests (fail-soft + breadcrumb + payload shape)` |
| 19927ef | `test(03-05): add failing listener tests (reconnect + catchup + cancellation)` |
| c8194c3 | `test(03-05): add failing candidate refresh tests (union + diff + cap + debounce)` |
| 6a16e41 | `test(03-05): extend test_orchestrator.py with 4 step 7.7 tests` |
| aef8c79 | `feat(03-05): add events.bus + events.listener (D-05 fail-soft NOTIFY)` |
| 09870f0 | `fix(03-05): make listener reconnect test robust to async scheduling` |
| 15cc5ab | `feat(03-05): add l2_candidate_refresh (D-04 union + R9 cap + 60s debounce)` |
| 2723b01 | `feat(03-05): wire orchestrator step 7.7 + l2_main EventListener task (D-05)` |

## Truths verified (programmatic)

| # | Truth | Command | Result |
|---|-------|---------|--------|
| 1 | L1 orchestrator step 7.7 publishes when enabled | `uv run pytest tests/m1-perception/test_orchestrator.py::test_step_7_7_emits_snapshot_complete_when_enabled` | exit 0 |
| 2 | L1 step 7.7 fail-soft when publish raises | `uv run pytest tests/m1-perception/test_orchestrator.py::test_step_7_7_failsoft_when_publish_raises` | exit 0 |
| 3 | listener.listen reconnects after connection loss | `uv run pytest tests/events/test_listener_catchup.py::test_listener_reconnects_after_connection_loss` | exit 0 |
| 4 | catchup_from_cursor replays missed snapshots | `uv run pytest tests/events/test_listener_catchup.py::test_catchup_replays_missed` | exit 0 |
| 5 | compute_candidates returns union(scanner ∪ watchlist) | `uv run pytest tests/observation/test_l2_candidate_refresh.py::test_compute_candidates_union` | exit 0 |
| 6 | diff_candidate_sets correct added/removed semantics | `uv run pytest tests/observation/test_l2_candidate_refresh.py::test_diff_candidate_sets_added_removed` | exit 0 |
| 7 | Candidate hard cap @ 500 (R9) | `grep -cE 'MAX_CANDIDATES.*=.*500' src/polyarb/observation/l2_candidate_refresh.py` | 2 (constant + plan reference) |
| 8 | Refresh debounce ≥60s | `grep -cE 'REFRESH_DEBOUNCE_S.*=.*60' src/polyarb/observation/l2_candidate_refresh.py` | 2 |
| 9 | asyncpg pinned >=0.31,<0.32 | `grep -cE 'asyncpg.*>=\s*0\.31' pyproject.toml` | 1 |
| 10 | L2 daemon wires real listener (no placeholder) | `grep -c 'event_listener = None' src/polyarb/daemon/l2_main.py` AND `grep -c 'listen_snapshot_complete\|on_snapshot_complete' src/polyarb/daemon/l2_main.py` | 0 / 3 |
| 11 | event_bus_enabled DEFAULT FALSE (B1 lock) | `python -c "re.search(r'event_bus_enabled.*=.*Field\(\s*default=False', ...)"` | True |

Broader regression GREEN: `uv run pytest tests/events tests/observation tests/m1-perception/test_orchestrator.py tests/m1-perception/test_l2_health_endpoint.py tests/daemon/ --tb=no` → **75 passed**.

## Cross-plan ordering (defensive design)

| Touch | Plan 05 behavior | Plan 06 expectation |
|-------|------------------|---------------------|
| `l2_event_cursor` table | catchup_from_cursor catches `UndefinedTableError` → returns `[]` and logs INFO | Alembic migration 003 (in Plan 06) creates the table; catchup begins replaying after that ships |
| `snapshots` table | Catchup reads `SELECT id, taken_at_ms FROM snapshots WHERE id > $1` — already exists from Phase 02 | n/a — already shipped |
| `_subscribed_assets` mutation | Plan 04 contract: `WsConsumer.subscribed_assets` property returns defensive copy; refresh handler writes to the private `_subscribed_assets` directly | n/a — locked in Plan 04 |
| `event_bus_enabled` feature flag | Default FALSE; opt-in `flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 -a polyarb-l1` | Plan 07 chaos Inj L2-3 must PASS before Plan 08 closes the flag-flip in prod |

## Feature-flag rollout discipline (B1 lock)

- Default state: `POLYARB_EVENT_BUS_ENABLED` unset → `settings.event_bus_enabled = False` → step 7.7 SKIPPED. Phase 02 behavior unchanged.
- Opt-in path: `flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 -a polyarb-l1` AFTER Plan 07 chaos Inj L2-3 PASS verdict.
- Emergency rollback: `flyctl secrets set POLYARB_EVENT_BUS_ENABLED=0 -a polyarb-l1 && flyctl machines restart -a polyarb-l1` — disables step 7.7 within ~30s without code change.

## Deviations from Plan

### Auto-fixed test stability issue

**1. [Rule 1 — Bug] listener reconnect test flake**
- **Found during**: Task 6 (GREEN listener.py)
- **Issue**: The originally-RED test used `monkeypatch.setattr(listener.asyncio, "sleep", _fake_sleep)` plus AsyncMock side_effect. After `asyncio.create_task(...)` scheduled the listener coroutine, the AsyncMock-side_effect-with-bare-yields combo never let the listener flow advance past the first `await asyncpg.connect(...)`, so the second connect attempt never registered.
- **Fix**: Use `patch.object` context manager (atomic install) plus a real-asyncio polling loop (`await real_sleep(0.01)` up to ~1s) until `connect_mock.await_count >= 2`. Implementation untouched; only the test scaffolding was reshaped.
- **Files modified**: `tests/events/test_listener_catchup.py` (test method body)
- **Commit**: `09870f0`

### No architectural deviation

- asyncpg 0.31.0 resolved with no transitive conflicts — the `>=0.31,<0.32` pin from the plan stuck verbatim. (No Plan 04-style websockets cap analog.)
- `asyncpg.exceptions.UndefinedTableError` exists with that exact name in 0.31 — no rename needed.

## Smoke evidence

`make smoke-event-bus` target added. NOT executed in this session because the local dev environment did not have `POLYARB_SUPABASE_DB_DSN` exported (production secret only). The target itself is:

```bash
make smoke-event-bus
# Requires POLYARB_SUPABASE_DB_DSN in .env or shell.
# Publishes one pg_notify('snapshot_complete', '{"snapshot_id":0,"taken_at_ms":0}').
# Prints 'OK' on success, 'FAIL' on fail-soft return.
```

The whole code path is exercised in the test suite via mocked asyncpg (`test_publish_payload_shape` asserts SQL + payload bytes exactly).

## Known Stubs / Carry-forward

- `catchup_from_cursor` returns `[]` until Plan 06 ships Alembic 003 (the `l2_event_cursor` table) — by design, see "Cross-plan ordering" above.
- `_dispatch_on_snapshot` schedules an asyncio task per NOTIFY but the on_snapshot_complete debounce ensures storm-collapse; sustained high-NOTIFY-rate behavior under chaos validated in Plan 07 Inj L2-3.
- L2 daemon's `event_bus:listener_state` health check now reports either `listening` (after first LISTEN succeeds) or `reconnecting` (during the 5s backoff) — verifiable via `curl /healthz | jq '.checks."event_bus:listener_state"[0].observedValue'` after L2 redeploy in Plan 08.

## Threat Flags

None — Plan 05 introduces no new trust boundaries beyond the ones already enumerated in `<threat_model>` (asyncpg NOTIFY/LISTEN via pgbouncer port 6543, payload size <100 bytes, defensive `UndefinedTableError` handling for cursor catch-up). All STRIDE entries T-03-05-01..07 mitigated as planned.

## Self-Check: PASSED

All deliverable paths verified:
- `[ -f src/polyarb/events/__init__.py ]` ✓
- `[ -f src/polyarb/events/bus.py ]` ✓
- `[ -f src/polyarb/events/listener.py ]` ✓
- `[ -f src/polyarb/observation/l2_candidate_refresh.py ]` ✓
- `[ -f tests/events/test_bus_publish.py ]` ✓
- `[ -f tests/events/test_listener_catchup.py ]` ✓
- `[ -f tests/observation/test_l2_candidate_refresh.py ]` ✓

All commits visible in `git log --oneline -10`:
- 66c6d5e, 53fd949, 19927ef, c8194c3, 6a16e41, aef8c79, 09870f0, 15cc5ab, 2723b01 ✓

All verification gates pass:
- `event_bus_enabled` default FALSE ✓
- `MAX_CANDIDATES = 500` ✓
- `REFRESH_DEBOUNCE_S = 60.0` ✓
- asyncpg `>=0.31,<0.32` ✓
- No `event_listener = None` placeholder remaining in l2_main.py ✓
- `make planning-status` → zero drift ✓
