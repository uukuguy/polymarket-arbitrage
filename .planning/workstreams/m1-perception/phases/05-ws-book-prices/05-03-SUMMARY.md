---
phase: 05-ws-book-prices
plan: 03
subsystem: infra
tags: [supabase, l3-promote, depth, chain-truth, l2-mirror, projector, scaffold]

# Dependency graph
requires:
  - phase: 05-ws-book-prices/01
    provides: alembic-005 l2_book_levels DDL + 3 OHLC views + l2_candidates.l3_promoted_at_ts
  - phase: 05-ws-book-prices/02
    provides: WsConsumer.add_subscriptions/remove_subscriptions + update_candidate_set (L3 race fix) + ws_consumer._l3_active_set state
provides:
  - L3 depth write path (projector + mirror method + dispatcher branch)
  - l3_promote.py module scaffold (state + 4 getters + is_book_levels_write_overdue predicate + STUB promote_run/run_periodic)
  - chain-truth anchor (_last_book_levels_write_at_s) mutated on every successful push_book_levels
  - _isoformat_ts now accepts ISO 8601 strings (latent bug — would have dropped all book-event writes in prod)
affects:
  - 05-04 (promoter populates _l3_active_set; will REPLACE promote_run / run_periodic BODIES but PRESERVE state + getters)
  - 05-05 (Wave 3 — /health l3 sub-checks read get_last_book_levels_write_at_s + get_l3_active_count)
  - 05-06 (Wave 3 — dashboard /l3/[asset_id] reads l2_book_levels)

# Tech tracking
tech-stack:
  added: []  # zero new deps — pure intra-module wiring on supabase-py + loguru + sentry_sdk already in tree
  patterns:
    - "scaffold-then-augment (Warning #6 prevention): Plan 03 lands l3_promote.py with state + getters + STUB; Plan 04 replaces bodies but PRESERVES the durable shape"
    - "chain-truth anchor mutation: success-path-only assignment of l3_promote._last_book_levels_write_at_s (failure path leaves None sentinel intact)"
    - "cold-start trap (memory feedback_cold-start-debounce-trap-2026-05): None sentinel for both timestamp anchors — /health treats None as cold-start warn, never 0.0"
    - "narrow projection + 1000-row chunks + dual-anchor Sentry breadcrumb (verbatim from push_top_of_book)"
    - "defensive copy via public getter: get_l3_active_set() returns set(_l3_active_set) so callers can't accidentally mutate module state"
    - "local-import dispatcher gate: from polyarb.observation import l3_promote inside _on_event closure, NOT at module top — picks up live module state on every call (resilient to Plan 04 rebinding _l3_active_set)"

key-files:
  created:
    - "src/polyarb/observation/l3_promote.py — SCAFFOLD ONLY (Plan 04 augments BODIES, preserves state+getters)"
    - "tests/m1-perception/test_l2_main_book_levels.py — 9 tests (7 projector + 2 dispatcher gate)"
    - "tests/m1-perception/test_l2_supabase_mirror_book_levels.py — 7 tests (fail-soft envelope + chain-truth + narrow projection + Sentry breadcrumb)"
  modified:
    - "src/polyarb/daemon/l2_main.py — added _book_levels_rows_from_frame projector; augmented _isoformat_ts to handle ISO 8601 strings; added dispatcher gate inside _on_event"
    - "src/polyarb/storage/l2_supabase_mirror.py — added _NARROW_BOOK_LEVELS_COLUMNS + push_book_levels method with verbatim push_top_of_book envelope + chain-truth mutation"
    - ".planning/workstreams/m1-perception/phases/05-ws-book-prices/deferred-items.md — pre-existing pytest-asyncio harness gap logged"

key-decisions:
  - "Sentry breadcrumb category='l2-mirror' (NOT 'l3-book-levels') — matches push_top_of_book / push_trades convention; data.table='l2_book_levels' disambiguates the table downstream"
  - "Chain-truth anchor lives in l3_promote.py at module level (NOT in L2SupabaseMirror) — Plan 04 promoter will read the SAME field for /health, so co-locating with the rest of L3 state is the right home"
  - "Local import of l3_promote inside _on_event dispatcher closure (NOT at l2_main module top) — Plan 04 may reassign _l3_active_set; local import ensures dispatcher sees the live binding"
  - "Defensive copy via get_l3_active_set() in the gate check, NOT direct _l3_active_set read — promoter (Plan 04) is the sole writer; dispatcher is a read-only consumer"
  - "_book_levels_rows_from_frame returns [] (not None, not raise) on every error path — matches Polymarket WS frame defensive parsing convention from _trade_row_from_frame"

patterns-established:
  - "L3 promote scaffold-then-augment: Plan 03 ships durable state + getters; Plan 04 replaces STUB bodies; deletion of state declarations across plan boundaries is the regression-class (Warning #6)"
  - "Chain-truth surface for L3 freshness: l3_promote._last_book_levels_write_at_s is mutated by the mirror's success branch; /health reads via get_last_book_levels_write_at_s; predicate is_book_levels_write_overdue(now_s) wraps the threshold logic"
  - "_isoformat_ts handles both numeric (epoch seconds/ms) and ISO 8601 strings — needed because Polymarket book frames carry timestamp as ISO string while last_trade_price uses numeric"

requirements-completed: [PHASE05-R02]

# Metrics
duration: 26min
completed: 2026-06-01
---

# Phase 05 Plan 03: L3 Depth Write Path Summary

**L3 book-levels write path + l3_promote module scaffold: projector emits up to 20 rows per book event (top-10 per side, BUY/SELL), mirror pushes via verbatim push_top_of_book envelope with chain-truth anchor mutation, dispatcher gates on `asset_id ∈ l3_promote._l3_active_set`.**

## Performance

- **Duration:** 26 min
- **Started:** 2026-06-01T06:51:23Z
- **Completed:** 2026-06-01T07:17:43Z
- **Tasks:** 3 (TDD RED + GREEN + dispatcher)
- **Files modified:** 5 (3 src + 2 tests + 1 deferred-items log)
- **Commits:** 3 atomic task commits

## Accomplishments

- **`_book_levels_rows_from_frame` projector** lands in `l2_main.py`: bounded top-10 per side, bids→BUY/asks→SELL, defensive parsing for malformed/zero-size/missing-key entries, returns `[]` (not None, not raise).
- **`L2SupabaseMirror.push_book_levels`** lands with the canonical push_top_of_book envelope verbatim: narrow projection (drops extra keys via `_NARROW_BOOK_LEVELS_COLUMNS`), 1000-row chunks, dual-anchor Sentry breadcrumb (`category='l2-mirror'`, `data.table='l2_book_levels'`), loguru info/error logging, **chain-truth anchor mutation on success path only** (`l3_promote._last_book_levels_write_at_s = time.time()`).
- **`src/polyarb/observation/l3_promote.py` scaffold** ships with module-level state declarations (`_l3_active_set`, `_last_promote_at_s`, `_last_book_levels_write_at_s`), 4 public getters (`get_l3_active_set`, `get_l3_active_count`, `get_last_promote_at_s`, `get_last_book_levels_write_at_s`), `is_book_levels_write_overdue(now_s, warn_s=120.0)` predicate for /health, and STUB `promote_run` / `run_periodic` async functions. Plan 04 will REPLACE the stub BODIES but the state + getters are durable (Warning #6 prevention).
- **`_on_event` dispatcher** gains the L3 depth-write branch: when `event_type == "book"` AND `asset_id ∈ l3_promote.get_l3_active_set()`, the dispatcher projects via `_book_levels_rows_from_frame(frame, max_levels=10)` and calls `l2_mirror.push_book_levels(rows)`. TOB write path is **untouched** — every subscribed asset's book event still writes to `l2_top_of_book` unconditionally; the depth write is additive.
- **16 new unit tests** (7 projector + 7 mirror + 2 dispatcher gate) — all green; no regression in 5 existing `test_l2_supabase_mirror_persist.py` tests; pyright clean on all 3 modified src files.

## Task Commits

1. **Task 1 (RED): test scaffolding** — `9131020` (test)
2. **Task 2 (GREEN): l3_promote scaffold + projector + mirror method + isoformat_ts ISO-string fix** — `0af71b7` (feat)
3. **Task 3 (GREEN): dispatcher gate** — `5106c67` (feat)

_TDD: Task 1 lands RED first (16 fail with ImportError/AttributeError); Task 2 lands the implementation; Task 3 wires the dispatcher branch. Tests stayed at 16/16 green after Tasks 2 + 3._

## Files Created/Modified

- `src/polyarb/observation/l3_promote.py` **(NEW, SCAFFOLD ONLY)** — module-level `_l3_active_set: set[str]` + `_last_promote_at_s: float | None = None` + `_last_book_levels_write_at_s: float | None = None` (cold-start `None` sentinel, NEVER `0.0`); 4 getters + `is_book_levels_write_overdue` predicate; STUB `promote_run` / `run_periodic` that Plan 04 will replace WHILE preserving state + getters.
- `src/polyarb/storage/l2_supabase_mirror.py` **(MODIFIED)** — added `_NARROW_BOOK_LEVELS_COLUMNS = ("asset_id", "ts", "side", "level", "price", "size")` at module top; added `push_book_levels(self, rows) -> bool` method with verbatim push_top_of_book envelope + `l3_promote._last_book_levels_write_at_s = _time.time()` mutation on success (chain-truth across module boundary).
- `src/polyarb/daemon/l2_main.py` **(MODIFIED)** — extended `_isoformat_ts` to accept ISO 8601 strings (+ trailing `Z` normalization); added `_book_levels_rows_from_frame(frame: dict, max_levels: int = 10) -> list[dict]` projector; augmented `_on_event` book-event branch with `from polyarb.observation import l3_promote` local import + `asset_id in l3_promote.get_l3_active_set()` gate + `l2_mirror.push_book_levels(book_rows)`.
- `tests/m1-perception/test_l2_main_book_levels.py` **(NEW)** — 7 projector tests + 2 dispatcher gate tests (the gate tests use a local helper that reproduces the production dispatcher branch logic, with cleanup-safe `_l3_active_set` snapshot/restore in `try/finally`).
- `tests/m1-perception/test_l2_supabase_mirror_book_levels.py` **(NEW)** — 7 mirror tests covering happy path, fail-soft (no raise), 1000-row chunking, narrow projection (extras dropped), chain-truth anchor mutation on success, chain-truth UN-touch on failure, Sentry breadcrumb category+data.table.
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/deferred-items.md` **(MODIFIED)** — logged pre-existing `pytest-asyncio` harness gap (confirmed on base commit; out of scope per executor scope-boundary).

## Decisions Made

All taken without user prompting; rationale captured in code docstrings + commit messages:

- **Sentry breadcrumb category = `l2-mirror`** (not a new `l3-book-levels` category) — preserves single-namespace dashboard filter; `data.table` field already provides per-table disambiguation downstream.
- **Chain-truth anchor lives in `l3_promote.py` module-level state** (not in L2SupabaseMirror instance state) — Plan 04 promoter will read the SAME field to make /health decisions; co-locating with the rest of L3 state simplifies the surface and matches the pattern from `l2_candidate_refresh._last_fetch_success_at_s`.
- **Local import** (`from polyarb.observation import l3_promote` inside the dispatcher closure, NOT at module top) — Plan 04 will reassign `_l3_active_set` at runtime; local import picks up the live binding instead of caching a stale module reference from `main()`-init time.
- **Defensive copy via `get_l3_active_set()` in the gate check** — promoter is the sole writer; dispatcher should read through a defensive-copy boundary so any future concurrent-mutation bug surfaces immediately as "asset not in copy" rather than mid-iteration set mutation.
- **`_book_levels_rows_from_frame` returns `[]` (never None, never raise) on every error path** — matches the defensive parsing convention from `_trade_row_from_frame`. Mirror's `push_book_levels([])` is a no-op chunked loop (returns True without touching DB), so the empty case is safe.
- **Cold-start trap: `None` sentinel for both timestamp anchors** — memory `feedback_cold-start-debounce-trap-2026-05` discipline: `0.0` would mark the daemon as "fresh at process-start epoch 0" which is mathematically true but operationally misleading; `None` forces /health to treat cold-start as warn until the first successful write.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Extended `_isoformat_ts` to accept ISO 8601 strings**
- **Found during:** Task 2 (running Task 1 RED tests against the new projector)
- **Issue:** Plan 05-03 `<interfaces>` block documents the Polymarket `book` WS frame as carrying `"timestamp": "2026-06-01T12:00:00.000Z"` (an ISO 8601 string). The existing `_isoformat_ts` (used by both `_tob_row_from_frame` and now `_book_levels_rows_from_frame`) only handled numeric epoch values — it called `float(ts)` first, which raises `ValueError` on ISO strings, caught the exception, and returned `None`. The projector then bailed at `ts_iso is None` → returned `[]` for every Task 1 test frame. **Latent prod bug**: even the existing TOB path would silently drop all `book` events that arrive with a string timestamp.
- **Fix:** Extended `_isoformat_ts` to type-dispatch on `ts`: numeric (int/float) → epoch path (unchanged); string → first try numeric-via-string (some sources send `"1717243200"` as a string), then fall back to `datetime.fromisoformat` with `Z`→`+00:00` normalization and UTC-default for tz-naive results. Pre-existing numeric path is byte-for-byte unchanged; the test surface is purely additive.
- **Files modified:** `src/polyarb/daemon/l2_main.py` (lines 81-128)
- **Verification:** 16 new tests + 5 existing `test_l2_supabase_mirror_persist` tests all green; pyright clean.
- **Committed in:** `0af71b7` (Task 2 GREEN)

**2. [Rule 1 - Bug] Pyright `reportArgumentType` on `float(entry.get("price"))`**
- **Found during:** Task 2 (pyright sweep on modified files)
- **Issue:** `entry.get("price")` returns `Unknown | None`; `float(None)` is a runtime `TypeError` even though my surrounding `try/except` catches it. pyright correctly flags this as an unsafe argument type.
- **Fix:** Hoist `raw_price = entry.get("price")` then `if raw_price is None: continue` BEFORE the `float()` call; apply the same `is None` guard to `raw_size` with a `0.0` default. Behaviorally equivalent (the `try/except` still catches `ValueError` for non-numeric strings) but type-safe.
- **Files modified:** `src/polyarb/daemon/l2_main.py` (projector function body)
- **Verification:** `uv run pyright src/polyarb/daemon/l2_main.py src/polyarb/storage/l2_supabase_mirror.py src/polyarb/observation/l3_promote.py` → 0 errors. Base commit was also 0 errors on `l2_main.py` (so my changes preserve cleanliness).
- **Committed in:** `0af71b7` (Task 2 GREEN — folded into the same commit as fix #1 since pyright was the verification gate)

---

**Total deviations:** 2 auto-fixed (2× Rule 1 bug).
**Impact on plan:** Both fixes are required for correctness — fix #1 is a latent prod bug (would have silently dropped every book write); fix #2 is a type-safety hardening that prevents a runtime `TypeError` outside the `try/except` scope. No scope creep; the surface area of the plan is unchanged.

## Issues Encountered

- **Pre-existing `test_ws_watchdog_liveness.py` async-test harness gap** — 6 tests fail at collection because `pytest-asyncio` is not registered (the `asyncio_mode` config option in `pyproject.toml` shows "Unknown config option" warning, indicating the plugin isn't actually installed). Confirmed on base commit `db6638c` BEFORE my changes via `git stash + pytest`. GAP-401 watchdog code is fully intact in `src/polyarb/daemon/ws_watchdog.py` + `ws_consumer.py` (not touched by Plan 05-03 — `git diff db6638c..HEAD --stat` returns empty for those files). Logged to `deferred-items.md` for a separate housekeeping task; out of scope per the executor's SCOPE BOUNDARY rule.

## User Setup Required

None — no external service configuration required for this plan. The L3 write path is dormant until Plan 04 populates `_l3_active_set` and Plan 05 (Wave 3 deploy) rolls out to prod. No new env vars, no new Sentry routing rules, no new RLS policies.

## Threat Surface Scan

No threat flags raised. All 6 STRIDE threats in Plan 03's `<threat_model>` are mitigated as planned:
- T-05-03-01 (Tampering, projector input): mitigated by `isinstance(entry, dict)` + `try/except` around `float()` + `size <= 0` filter + `is None` guards (added in deviation #2).
- T-05-03-02 (DoS via 10k levels): mitigated by `max_levels=10` cap with `break` on threshold.
- T-05-03-03 (Information Disclosure via breadcrumb): accepted — breadcrumb data contains row counts + table name only (no asset_id, no price).
- T-05-03-04 (Tampering via wrong table): mitigated — `self._client.table("l2_book_levels")` is a literal string with no interpolation.
- T-05-03-05 (Repudiation via silent push failure): mitigated — `logger.error` + Sentry breadcrumb (level=`warning`) on failure; /health l3 sub-check (Plan 04) will surface staleness via `is_book_levels_write_overdue`.
- T-05-03-06 (Tampering via unexpected event_type): mitigated — depth write happens **only** when `event_type == "book"` (literal string match).

## Next Plan Readiness

**Plan 05-04 (Wave 3 — L3 promoter)** can build directly on this:

- `l3_promote._l3_active_set` exists and is mutable; Plan 04's promoter writes to it after running the `l3-promote.yaml` scanner recipe.
- `l3_promote._last_promote_at_s` exists as `None`; Plan 04's promoter mutates on every successful `promote_run()`.
- Getters + `is_book_levels_write_overdue` predicate are durable — Plan 04 must REPLACE only the STUB bodies of `promote_run` and `run_periodic`. **Do NOT delete the module-level state declarations or any of the 4 getters or the predicate** (Warning #6 anti-pattern).
- The dispatcher gate in `l2_main._on_event` reads `l3_promote.get_l3_active_set()` defensively; Plan 04 mutating `_l3_active_set` will be picked up by the very next book event without any wiring change in `l2_main`.
- `_last_book_levels_write_at_s` is the chain-truth anchor that Plan 04's /health l3 sub-checks will read; the mutation is already wired in `push_book_levels` (no additional Plan 04 work needed on the mirror side).

**Plan 05-05 (Wave 3 — /health l3 sub-checks)** can wire `l3:active_count = get_l3_active_count()`, `l3:last_promote_at_s = get_last_promote_at_s()`, `l3:last_book_levels_write_at_s = get_last_book_levels_write_at_s()` directly, with the threshold logic delegated to `is_book_levels_write_overdue(now_s, warn_s)`.

## Self-Check: PASSED

Verified before completion:

```
[FOUND] src/polyarb/observation/l3_promote.py (NEW, 105 lines)
[FOUND] tests/m1-perception/test_l2_main_book_levels.py (NEW, 264 lines)
[FOUND] tests/m1-perception/test_l2_supabase_mirror_book_levels.py (NEW, 235 lines)
[MODIFIED] src/polyarb/daemon/l2_main.py — projector + _isoformat_ts + dispatcher
[MODIFIED] src/polyarb/storage/l2_supabase_mirror.py — _NARROW_BOOK_LEVELS_COLUMNS + push_book_levels
[FOUND COMMIT] 9131020 (Task 1 — RED)
[FOUND COMMIT] 0af71b7 (Task 2 — GREEN, scaffold + projector + mirror + isoformat_ts fix)
[FOUND COMMIT] 5106c67 (Task 3 — GREEN, dispatcher gate)
[GREP] _book_levels_rows_from_frame in l2_main: 2 (def + dispatcher) — pass (≥2 expected)
[GREP] def push_book_levels in mirror: 1 — pass (=1 expected)
[GREP] l3_promote._last_book_levels_write_at_s assignment in mirror: 1 — pass (=1 expected)
[GREP] _l3_active_set + getters in l3_promote.py: 17 occurrences — pass (≥6 expected)
[GREP] 4 getters present in l3_promote.py — pass (=4 expected)
[GREP] is_book_levels_write_overdue defined — pass
[TESTS] tests/m1-perception/test_l2_main_book_levels.py: 9/9 green
[TESTS] tests/m1-perception/test_l2_supabase_mirror_book_levels.py: 7/7 green
[TESTS] tests/m1-perception/test_l2_supabase_mirror_persist.py: 5/5 green (no regression)
[PYRIGHT] 3 modified src files: 0 errors (was 0 on base; preserved)
```

---

*Phase: 05-ws-book-prices*
*Completed: 2026-06-01*
