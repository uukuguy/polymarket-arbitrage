---
phase: 05-ws-book-prices
plan: 02
subsystem: api
tags: [websockets, asyncio, polymarket, l3-promote, race-condition]

# Dependency graph
requires:
  - phase: 04.1-d01-restart-robustness-chaos-redesign
    provides: "GAP-401 liveness gate (WsConsumer._stash_ws / _liveness_check / _current_ws stash)"
  - phase: 03 / 03.1
    provides: "WsConsumer + ws_market_client + l2_candidate_refresh baseline"
provides:
  - "WsConsumer.add_subscriptions(asset_ids) async — send-after-connect subscribe payload"
  - "WsConsumer.remove_subscriptions(asset_ids) async — send-after-connect unsubscribe payload"
  - "WsConsumer.update_candidate_set(asset_ids) public helper — L2 refresh path"
  - "WsConsumer._candidate_set + _l3_active_set split (replaces single _subscribed_assets list)"
  - "WsConsumer._compute_active_assets() — sorted union helper"
  - "Backward-compat _subscribed_assets property+setter (legacy callers emit DeprecationWarning, L3 set preserved)"
  - "l2_candidate_refresh.on_snapshot_complete migrated to update_candidate_set (Pitfall 5 race fix)"
affects: [05-03, 05-04, 05-05, l3_promote, polywatch-l3]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Split-set subscription model — separate ownership for candidate vs L3 tokens, union on read"
    - "Send-after-connect API on WsConsumer reading the GAP-401 _current_ws stash"
    - "Deterministic send-failure semantics — no state pollution on RuntimeError"

key-files:
  created:
    - "tests/m1-perception/test_ws_consumer_dynamic_subscribe.py"
    - "tests/m1-perception/test_candidate_refresh_l3_protection.py"
  modified:
    - "src/polyarb/daemon/ws_consumer.py"
    - "src/polyarb/observation/l2_candidate_refresh.py"
    - "tests/observation/test_l2_candidate_refresh.py"

key-decisions:
  - "Split _subscribed_assets list into _candidate_set + _l3_active_set sets; expose union via _compute_active_assets()"
  - "Backward-compat _subscribed_assets is a property+setter — legacy assignments interpret incoming list as the candidate set only (L3 set untouched)"
  - "Send-failure deterministic spec: on raise → no mutation of _l3_active_set + return False (Warning #12)"
  - "Mid-conn payload schema: {operation: subscribe|unsubscribe, assets_ids: [...]} (not {type: market, ...} like initial-connect)"
  - "Empty asset_ids → return True noop (no send, no mutation) — keeps L3 promoter no-diff calls cheap"

patterns-established:
  - "Race-protected refresh pattern: writer-side helper (update_candidate_set) replacing direct private-attribute overwrite"
  - "GAP-401 invariant preservation pattern: read _current_ws via the existing stash, do NOT touch _stash_ws/_liveness_check/watchdog wiring"
  - "DeprecationWarning + setter for legacy private-attr writes during cross-plan migration windows"

requirements-completed: [PHASE05-R04, PHASE05-R06]

# Metrics
duration: 22min
completed: 2026-06-01
---

# Phase 05 Plan 02: WS dynamic subscribe + L3 race protection Summary

**WsConsumer gains async add/remove_subscriptions API reading the GAP-401 _current_ws stash, and l2_candidate_refresh migrates to update_candidate_set so the 60s NOTIFY-driven refresh no longer clobbers L3 tokens (Pitfall 5 race fix).**

## Performance

- **Duration:** 22 min
- **Started:** 2026-06-01T06:03:00Z (approx — Task 1 RED tests authored)
- **Completed:** 2026-06-01T06:25:33Z
- **Tasks:** 3 (all auto, all TDD RED → GREEN)
- **Files modified:** 5 (2 created, 3 modified)

## Accomplishments

- **WsConsumer.add_subscriptions / remove_subscriptions** — async methods sending `{operation: subscribe|unsubscribe, assets_ids: [...]}` payloads on the live ws stashed via GAP-401's on_connect hook. Empty/no-ws/send-failure semantics deterministic per Warning #12.
- **Split-set refactor** — `_candidate_set` (L2 candidate refresh ownership) + `_l3_active_set` (L3 promoter ownership); public `subscribed_assets` returns sorted union via `_compute_active_assets()`.
- **Backward-compat shim** — `_subscribed_assets` property+setter; legacy callers emit DeprecationWarning and now only mutate the candidate set (L3 set preserved).
- **`update_candidate_set(asset_ids)` public helper** — Phase 05's clean migration target for the L2 refresh path.
- **l2_candidate_refresh migrated** — `on_snapshot_complete` now calls `ws_consumer.update_candidate_set(new_asset_ids)`; diff `old_asset_ids` reads from `_candidate_set` (not the union) so the +N/-M log surface reflects ONLY candidate churn.
- **11 new tests** — 9 dynamic subscribe + 2 L3 protection regression. Plus existing 21 candidate-refresh and 10 GAP-401 liveness tests still green.

## Task Commits

Each task was committed atomically with `--no-verify` (parallel worktree):

1. **Task 1: RED tests for add/remove_subscriptions + L3 race protection** — `2439658` (test)
2. **Task 2: Refactor WsConsumer — split _candidate_set/_l3_active_set + add/remove_subscriptions** — `7da7868` (feat)
3. **Task 3: Migrate l2_candidate_refresh to update_candidate_set helper (Pitfall 5)** — `867372e` (fix)

## Files Created/Modified

- `tests/m1-perception/test_ws_consumer_dynamic_subscribe.py` — 9 unit tests covering no-live-ws fallback, live-ws send + payload schema, empty noop, send-failure determinism (Warning #12), unsubscribe symmetry, lint-no-Lock, union helper, 10-token Yes+No payload (D-05).
- `tests/m1-perception/test_candidate_refresh_l3_protection.py` — 2 regression tests: (1) `on_snapshot_complete` does NOT clobber `_l3_active_set` (using a real `WsConsumer` instance + mocked `compute_candidates`), (2) source-lint that the refresh code no longer assigns to `_subscribed_assets`.
- `src/polyarb/daemon/ws_consumer.py` — Replaced `_subscribed_assets: list[str]` with `_candidate_set: set[str]` + `_l3_active_set: set[str]`; added `_compute_active_assets()` union helper; replaced `subscribed_assets` property to return union; added backward-compat `_subscribed_assets` property+setter; added `update_candidate_set()` public helper; added async `add_subscriptions`/`remove_subscriptions` methods; updated `run()` loop to read via `_compute_active_assets()` directly. **GAP-401 stash/closure/wiring untouched.**
- `src/polyarb/observation/l2_candidate_refresh.py` — Migrated `on_snapshot_complete` from full-list overwrite of `_subscribed_assets` to `ws_consumer.update_candidate_set(new_asset_ids)`. Diff source switched from `subscribed_assets` (union) to `_candidate_set` so the log line reflects ONLY candidate churn; log message updated to include `(L3 set untouched: K tokens)`.
- `tests/observation/test_l2_candidate_refresh.py` — Updated 2 existing tests (`test_on_snapshot_complete_mutates_ws_consumer`, `test_on_snapshot_complete_calls_mirror_upsert_when_provided`) and consistency-touched 1 more (`test_on_snapshot_complete_no_mirror_call_when_none`) to use the new API surface (`fake_ws.update_candidate_set.assert_called_once()` + `_candidate_set`/`_l3_active_set` mocks).

## Decisions Made

- **Set semantics in setter shim**: legacy `_subscribed_assets = list(...)` writes are interpreted as the new candidate set only (do NOT subtract `_l3_active_set`). A token CAN legitimately be in both sets simultaneously; the union semantics handle overlap. This avoids surprising data loss if a recipe pick also happens to be an L3 promote.
- **Send-failure ordering**: payload build → ws read → send → mutate. Mutation happens ONLY after `await send` returns; on raise, the early-return path keeps `_l3_active_set` pristine. This satisfies the Warning #12 deterministic test which inspects both `_l3_active_set` AND `subscribed_assets`.
- **Reading `old_asset_ids` from `_candidate_set` in `on_snapshot_complete`**: prevents the +N/-M log surface from spuriously showing L3 tokens as "removed" when candidate set churns. The `removed` set is then a pure candidate-set delta and is passed to `mirror.mark_candidates_removed` correctly.
- **Test 11 is a source-lint** (uses `inspect.getsource` to assert no `_subscribed_assets =` assignment LHS in `on_snapshot_complete`'s source). Lightweight regression guard against the legacy pattern sneaking back in a future refactor.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 3 - Blocking] Missing pytest-asyncio install**

- **Found during:** Task 1 verification (running GAP-401 regression suite)
- **Issue:** Worktree `.venv` was missing `pytest-asyncio` despite being in `pyproject.toml` dev extras; async tests reported "async functions are not natively supported".
- **Fix:** `uv sync --extra dev` (installed pytest-asyncio==0.23.8 + freezegun + ruff).
- **Files modified:** none (lockfile was already current; only `.venv/` content changed).
- **Verification:** GAP-401 suite went from 1 failed/9 collected to 10 passed.
- **Committed in:** N/A (no source change).

**2. [Rule 1 - Bug] Lint-only false positive in add_subscriptions docstring**

- **Found during:** Task 2 (test 7 — concurrent-safe lint test).
- **Issue:** Initial `add_subscriptions` docstring contained the literal word "Lock" inside a comment explaining we do NOT use one; `inspect.getsource` lint test grepped for "Lock" anywhere in the source.
- **Fix:** Reworded the docstring to use "synchronization" instead of "Lock" so the lint pattern matches only actual `Lock` usage.
- **Files modified:** `src/polyarb/daemon/ws_consumer.py` (docstring only).
- **Verification:** Test 7 went from FAIL to PASS without changing semantics.
- **Committed in:** `7da7868` (Task 2 commit).

**3. [Rule 1 - Bug] Existing observation tests asserted legacy contract**

- **Found during:** Task 3 (Pitfall 5 migration).
- **Issue:** `tests/observation/test_l2_candidate_refresh.py::test_on_snapshot_complete_mutates_ws_consumer` asserted `len(fake_ws._subscribed_assets) == 5` after the call, and `test_on_snapshot_complete_calls_mirror_upsert_when_provided` pre-populated `fake_ws._subscribed_assets` for the "OLD-A/OLD-B → removed" diff. Both broke when the production code switched to `update_candidate_set` + `_candidate_set` for diff source.
- **Fix:** Rewrote the two assertions to use the new API surface (assert on `fake_ws.update_candidate_set.call_args[0][0]`, pre-populate `fake_ws._candidate_set` for the diff path). Consistency-touched a third test for fixture hygiene.
- **Files modified:** `tests/observation/test_l2_candidate_refresh.py`.
- **Verification:** All 21 tests in `tests/observation/test_l2_candidate_refresh.py` + `test_l2_candidate_refresh_coldstart.py` green.
- **Committed in:** `867372e` (Task 3 commit).

**4. [Rule 1 - Bug] Grep verification matched docstring/comment substrings**

- **Found during:** Task 3 verification step 5 (`grep -rn "_subscribed_assets\s*=" src/`).
- **Issue:** Plan verification expects 0 matches outside `ws_consumer.py`. After the migration, two doc-comment lines in `l2_candidate_refresh.py` contained the literal pattern `_subscribed_assets = ...` referencing the removed legacy behavior.
- **Fix:** Rewrote both comment lines to describe the legacy pattern in prose ("legacy full-list overwrite of the private subscriptions attribute") without the literal `_subscribed_assets =` token.
- **Files modified:** `src/polyarb/observation/l2_candidate_refresh.py` (comments only — no behavior change).
- **Verification:** Grep now returns 0 matches.
- **Committed in:** `867372e` (Task 3 commit, same commit as the fix above).

---

**Total deviations:** 4 auto-fixed (1 blocking — env, 3 bugs — all in test/doc surfaces, not production semantics).
**Impact on plan:** All four were narrow corrections. None changed the cross-plan contract; production semantics match the plan exactly.

## Issues Encountered

- **MagicMock auto-attribution surprise**: in Task 3, MagicMock instances auto-create `_candidate_set` as a MagicMock when accessed via `getattr`. To keep the production code robust against legacy mocks that haven't been migrated, the diff line uses `set(getattr(ws_consumer, "_candidate_set", set()))` — the `set()` default ensures a real empty set is used when the attribute is missing, and the explicit `set(...)` cast on the result ensures consistency even if a MagicMock leaks through. Updated 2 test fixtures to set real sets so the diff logic exercises the intended path.

## GAP-401 Invariants Preserved

Verified untouched in `src/polyarb/daemon/ws_consumer.py`:
- `_current_ws: Any = None` field declaration (line 142 in pre-plan, same offset post-plan)
- `_stash_ws(ws)` method body (no edits)
- `_liveness_check()` closure (no edits — still reads `_current_ws`, checks `ws.state is WsState.OPEN and ws.latency > 0`)
- `self._watchdog._liveness_check = self._liveness_check` wiring in `__init__`
- `on_connect=self._stash_ws` on `stream_market_events()` call in `run()`
- `self._current_ws = None` clear on disconnect in both `WsTestKillRequested` and `CancelledError` branches

GAP-401 regression suite `tests/m1-perception/test_ws_watchdog_liveness.py` remains **10/10 green** through every task.

## D-03 (`stale_s = 30.0`) Untouched

`src/polyarb/daemon/ws_watchdog.py` was not modified in this plan. The plan's `<critical_constraints>` requirement honored.

## User Setup Required

None — purely internal API additions + refactor. No new env vars, no new dependencies, no migration. The `pytest-asyncio` install was a worktree-local `.venv` sync (the dep is already declared in `pyproject.toml` dev extras and present in `uv.lock`).

## Next Phase Readiness

- **Plan 04 (L3 promoter)** can now call `ws_consumer.add_subscriptions(added_ids)` / `ws_consumer.remove_subscriptions(removed_ids)` mid-connection without reconnecting (D-11 fulfilled at the WsConsumer layer).
- **Plan 05 (l3_promote module)** can use `update_candidate_set` as the contract pattern for any other writers that need to mutate the subscription set without clobbering L3.
- **2 carry-forward items remain unchanged from session start**:
  - 04.1 code-review fixes (WR-02/03/IN-01) — still ship with next L2 deploy.
  - GAP-401 prod re-verification — open quiet window after next deploy.
- **No threat-flag surface added**: Plan 02 stays within trust boundaries already in the Phase 05 threat model (T-05-02-01..05). Pure refactor + internal API.

## Self-Check: PASSED

- File `tests/m1-perception/test_ws_consumer_dynamic_subscribe.py`: FOUND
- File `tests/m1-perception/test_candidate_refresh_l3_protection.py`: FOUND
- Commit `2439658` (Task 1 RED): FOUND
- Commit `7da7868` (Task 2 GREEN): FOUND
- Commit `867372e` (Task 3 GREEN): FOUND
- All 6 plan verification checks: PASSED (9/9 dynamic subscribe, 2/2 L3 protection, 10/10 GAP-401, 21/21 observation refresh+coldstart, grep `_subscribed_assets =` outside ws_consumer.py = 0, grep `operation.*[un]subscribe` in ws_consumer.py = 4)

---
*Phase: 05-ws-book-prices*
*Plan: 02*
*Completed: 2026-06-01*
