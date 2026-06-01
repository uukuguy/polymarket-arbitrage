---
phase: 05-ws-book-prices
plan: 04
subsystem: market-perception
tags: [l3-promote, scanner-recipe, ws-subscriptions, chain-truth, supabase-mirror, fail-soft, asyncio-cron]

requires:
  - phase: 05-01
    provides: alembic 005 l2_book_levels + l2_candidates.l3_promoted_at_ts column
  - phase: 05-02
    provides: ws_consumer.add_subscriptions / remove_subscriptions dynamic mutation
  - phase: 05-03
    provides: l3_promote module-level state + getters scaffold (_l3_active_set, _last_promote_at_s, _last_book_levels_write_at_s) + L2SupabaseMirror.push_book_levels write-side
  - phase: 04-shipped
    provides: markets_latest.no_token_id column (D-07) + /health three-branch chain-truth pattern (D-08) + cold-start trap guidance
provides:
  - L3 promoter brain (promote_run + run_periodic) — selects top-5 markets per D-13 thresholds + Yes/No expansion to 10 token subscriptions
  - src/polyarb/scan_recipes/l3-promote.yaml — first source-controlled trusted-yaml recipe (3rd trust tier)
  - scanner.py module docstring — formalizes 3-tier recipe trust taxonomy
  - 3 /health L3 sub-checks (chain-truth, no config gate)
  - l2_candidates.l3_promoted_at_ts write-through (dashboard L3 badge surface)
  - l3-promoter task wired into l2_main F-04 bounded shutdown
affects: [05-05, 05-06, 04.2-future-chaos, m2-combinatorial]

tech-stack:
  added: []
  patterns:
    - "Trusted-yaml recipe tier: direct Recipe(_is_trusted=True) construction for source-controlled yaml on disk (yaml-only audit trail, no env-var threshold creep)"
    - "Adapter temp-DB pattern: scanner.run_recipe's hard-coded `FROM markets m LEFT JOIN question_translations qt` accommodated by materializing tob rows into a `markets` table with NULL `question` + empty `question_translations` (echoes Phase 04 Plan 02 l2_temp_db pattern)"
    - "INTEGER epoch ms ts comparison in scanner SQL — `strftime('%s','now','-1 hour') * 1000`, avoiding ISO-TEXT lex comparison failures"
    - "Last-known-good freeze policy on Supabase outage — _l3_active_set never cleared on transient outage; surfaces via /health l3:last_promote_at_s age"
    - "Prior-map snapshot before overwrite — _last_known_market_token_map captured BEFORE the new fetch to compute MARKET-level removed_markets correctly on the chain-truth mirror surface"

key-files:
  created:
    - "src/polyarb/scan_recipes/l3-promote.yaml — D-13 thresholds, INTEGER epoch ms ts predicate"
    - "tests/m1-perception/test_l3_promoter.py — 12 promoter tests (yaml schema + ts filter + freeze + Yes/No expansion + write-through + run_periodic loop)"
    - "tests/m1-perception/test_l2_health_l3_subchecks.py — 13 /health sub-check tests (cold-start + thresholds + chain-truth lint)"
  modified:
    - "src/polyarb/observation/l3_promote.py — REPLACED Plan 03 stub bodies with full implementation; PRESERVED module-level state + 4 getters verbatim"
    - "src/polyarb/observation/scanner.py — module docstring §Trusted-recipe tiers (3rd tier formalized)"
    - "src/polyarb/http/l2_health.py — 3 new L3 sub-checks (active_count + last_promote_at_s + last_book_levels_write_at_s)"
    - "src/polyarb/daemon/l2_main.py — l3_promoter_task wiring + F-04 shutdown list"

key-decisions:
  - "Adapter temp-DB schema: `markets` table holds tob columns (asset_id, ts, spread, depth_yes_usd, …) + NULL question; empty question_translations satisfies scanner's hard-coded LEFT JOIN. Avoids touching scanner.run_recipe SQL (reuses Layer 1/2/3/4 defense unchanged)."
  - "Recipe `_is_trusted=True` granted to l3-promote.yaml because it lives in the repo and is modified via PR + review — equivalent to BUILTIN_RECIPES trust. scanner.py docstring now formalizes this as the 3rd tier."
  - "Snapshot _last_known_market_token_map BEFORE step 5 overwrites it — without the snapshot, step 6's reverse map (token → market) would be blind to the prior tick's markets and produce empty removed_markets, silently breaking the l2_candidates write-through diff."
  - "L3_EXPECTED_TOKEN_COUNT = 10 strict (revision-1) — D-05 N=5 markets × 2 Yes+No tokens. Not the looser '≥5' relaxation."
  - "active_count is informational-only (does NOT bump overall); last_promote_at_s + last_book_levels_write_at_s DO bump overall on warn/fail — alignment with ws:subscribed_count's informational-only treatment + mirror:l2_tob_age_seconds's alarming treatment."
  - "No config-flag gating between getter and /health sub-check — chain truth IS the field. GAP-200 / Inj L2-2 RCA enforced via lint test (`test_health_l3_subchecks_chain_truth_no_config_gate`)."

patterns-established:
  - "Trusted-yaml tier (Recipe tier 3): yaml on disk loaded via `Recipe(..., _is_trusted=True)` direct construction; yaml-only audit trail per CLAUDE.md 'experiment values never touch baseline defaults'"
  - "Scanner adapter temp-DB: materialize alt-source rows (tob, future: price-history) into the `markets`-shaped schema so scanner.run_recipe's hard-coded JOIN stays untouched"
  - "Prior-map snapshot before fetch overwrite: when last-known-good state is also used for reverse computation, snapshot BEFORE the new fetch — not after"

requirements-completed: [PHASE05-R01, PHASE05-R07]

# Metrics
duration: ~75min
completed: 2026-06-01
---

# Phase 05 Plan 04: L3 Promoter Implementation Summary

**L3 promoter brain — selects top-5 markets via trusted yaml recipe over tob snapshot, expands to 10 Yes+No token subscriptions, write-through l2_candidates dashboard surface, 3 chain-truth /health anchors, all fail-soft.**

## Performance

- **Duration:** ~75 min (3 tasks, ~25 min each)
- **Started:** 2026-06-01T15:30:00Z (approx)
- **Completed:** 2026-06-01T16:45:00Z (approx)
- **Tasks:** 3 (Task 1 RED, Task 2 GREEN promoter, Task 3 GREEN wiring + sub-checks)
- **Files modified:** 7 (3 created + 4 modified; +deferred-items.md note)
- **Tests added:** 25 (12 promoter + 13 /health sub-checks)

## Accomplishments

- **`promote_run`** — 9-step pipeline: resolve creds → build client → fetch tob (last-1h, ≤1000 rows) → load trusted yaml recipe → run scanner against named-temp-file SQLite (INTEGER epoch ms ts) → fetch yes/no token map from markets_latest → expand N=5 markets to 10 tokens (D-05 / Warning #13) → diff vs `_l3_active_set` → ws_consumer.add_subscriptions / remove_subscriptions → write-through l2_candidates.l3_promoted_at_ts → mutate state + chain-truth anchor.
- **`run_periodic`** — raw `asyncio.wait_for(stop_event.wait(), timeout=300.0)` loop matching ws_consumer.run idle-wait pattern (no scheduler dep added; cross-pattern decision #4). Runs once immediately on startup so /health gets a fresh anchor without waiting 5 min.
- **`l3-promote.yaml`** — first source-controlled trusted-yaml recipe (3rd trust tier). D-13 thresholds (`spread < 0.02 AND depth_yes_usd > 500`), Blocker #2 INTEGER epoch ms ts predicate (`ts > (strftime('%s','now','-1 hour') * 1000)`), `ORDER BY depth_yes_usd DESC LIMIT 5`.
- **scanner.py module docstring** — now formally documents the 3-tier recipe trust taxonomy (BUILTIN_RECIPES / user yaml / source-controlled yaml).
- **3 /health L3 sub-checks** — `l3:active_count` (informational, expected 10), `l3:last_promote_at_s` (warn 600s, fail 1800s), `l3:last_book_levels_write_at_s` (warn 120s, fail 600s). All read chain-truth getters; lint test enforces no config-flag gating.
- **l2_main wiring** — `l3_promoter_task` created via `asyncio.create_task`; cancelled + 5s wait_for in F-04 bounded shutdown.
- **Plan 03 scaffold preservation** — verified by grep: 4 getters present (`get_l3_active_set`, `get_l3_active_count`, `get_last_promote_at_s`, `get_last_book_levels_write_at_s`); module-level state declarations intact (`_l3_active_set: set[str] = set()`, `_last_promote_at_s: float | None = None`, `_last_book_levels_write_at_s: float | None = None`).

## Task Commits

1. **Task 1: RED — yaml + scanner docstring + 12 RED tests** — `2af6c1e` (test)
2. **Task 2: GREEN — promote_run + run_periodic + Yes/No expansion + l2_candidates mirror (scaffold preserved)** — `482619e` (feat)
3. **Task 3: GREEN — l3_promoter_task wiring + 3 /health L3 sub-checks (chain-truth)** — `9b5017e` (feat)

_Plan was TDD-driven: Task 1 RED 10/12 fail (yaml + docstring pass on their own); Task 2 GREEN 12/12 (after 2 mid-impl fixes); Task 3 adds 13 fresh tests, all green._

## Files Created/Modified

- `src/polyarb/scan_recipes/l3-promote.yaml` (NEW) — D-13 thresholds + INTEGER epoch ms ts predicate; yaml-only audit trail for threshold tuning.
- `src/polyarb/observation/l3_promote.py` (MODIFIED) — replaced Plan 03 stub bodies; full promote_run + run_periodic + helpers (_fetch_latest_tob_rows_from_supabase, _fetch_market_token_map, _build_tob_temp_db, _load_recipe, _mirror_l3_promoted_at_ts, _iso_to_epoch_ms). Module-level state + 4 getters preserved verbatim from Plan 03.
- `src/polyarb/observation/scanner.py` (MODIFIED) — module docstring §Trusted-recipe tiers now lists 3 tiers (BUILTIN / user yaml / source-controlled yaml). No code change.
- `src/polyarb/http/l2_health.py` (MODIFIED) — 3 new L3 sub-checks appended to `_build_l2_health_checks` (after `process:rss_kb`); active_count informational, last_promote_at_s + last_book_levels_write_at_s bump overall on warn/fail.
- `src/polyarb/daemon/l2_main.py` (MODIFIED) — `l3_promoter_task = asyncio.create_task(...)`; appended `("l3-promoter", l3_promoter_task)` to F-04 shutdown tuple list.
- `tests/m1-perception/test_l3_promoter.py` (NEW) — 12 tests covering yaml schema, scanner docstring lint, ts predicate filter, freeze policies, Yes/No expansion, write-through Blocker #1, run_periodic loop.
- `tests/m1-perception/test_l2_health_l3_subchecks.py` (NEW) — 13 tests covering cold-start warn / threshold pass+warn+fail / chain-truth no-config-gate lint / L3_EXPECTED_TOKEN_COUNT=10 lint / getters-are-called lint.
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/deferred-items.md` (APPENDED) — flaky chaos_r2 test under broad collection (pre-existing).

## Decisions Made

See `key-decisions:` in frontmatter for the full list; highlights:

1. **Adapter temp-DB pattern** — scanner.run_recipe SQL is hard-coded `FROM markets m LEFT JOIN question_translations qt`. Rather than fork scanner.py (would break the 4-layer SQL injection defense + trust invariants), the L3 promoter materializes tob rows into a `markets`-shaped table (with NULL question + empty question_translations). The recipe references `spread`/`depth_yes_usd`/`ts`/`asset_id` as `markets` columns, queried via the existing scanner. This mirrors Phase 04 Plan 02 `l2_temp_db.build_temp_db` pattern.

2. **Trusted-yaml tier formalization** — `l3-promote.yaml` is the first recipe that's BOTH (a) yaml-on-disk for threshold audit trail AND (b) source-controlled so we trust it like BUILTIN_RECIPES. Used direct `Recipe(name=..., _is_trusted=True)` construction in `_load_recipe`. scanner.py docstring now documents this as the 3rd trust tier.

3. **Prior-map snapshot** — discovered during Task 2 GREEN: `_last_known_market_token_map` gets overwritten in step 5 by the new fetch, BEFORE step 6 needs it for the MARKET-level reverse lookup. Fix: `prior_market_token_map = dict(_last_known_market_token_map or {})` captured BEFORE step 5. Without this, the l2_candidates write-through `removed_markets` list is silently empty on every tick.

4. **Three-fail policy gradient** — active_count is **informational** (cold-start / under-fill is a normal initial state); last_promote_at_s and last_book_levels_write_at_s **alarm** (their absence indicates daemon dysfunction). Matches the ws:subscribed_count (informational) vs mirror:l2_tob_age_seconds (alarming) split.

5. **No scheduler dep** — pyproject lacks apscheduler; the raw `asyncio.wait_for(stop_event.wait(), timeout=interval_s)` loop is the same pattern already in ws_consumer.run. Idiomatic and adds zero dependency surface.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Prior-map snapshot before step-5 overwrite**

- **Found during:** Task 2 GREEN (test_promote_run_writes_l3_promoted_at_ts_on_add_and_clears_on_remove failure)
- **Issue:** Plan's example pseudocode for `promote_run` writes `_last_known_market_token_map = token_map` in step 4, then step 5's reverse-map computation iterates `_last_known_market_token_map` — but at that point the map only contains the NEW markets, not the prior tick's markets. The remove diff produces an empty set, silently breaking the l2_candidates write-through `NULL`-mirror for removed markets.
- **Fix:** Capture `prior_market_token_map = dict(_last_known_market_token_map or {})` at the start of step 5 (BEFORE the new fetch overwrites the module attribute). Step 6's reverse map iterates `prior_market_token_map` instead.
- **Files modified:** `src/polyarb/observation/l3_promote.py`
- **Verification:** test_promote_run_writes_l3_promoted_at_ts_on_add_and_clears_on_remove went RED → GREEN after the fix.
- **Committed in:** `482619e` (Task 2 commit)

**2. [Rule 1 - Bug] Adapter temp-DB schema mismatch with scanner.run_recipe**

- **Found during:** Task 1 (when designing the temp DB shape)
- **Issue:** Plan's example pseudocode for `_build_tob_temp_db` creates a `CREATE TABLE l2_top_of_book` and calls `scanner.run_recipe(db_path, recipe)`. But `scanner.run_recipe` is HARD-CODED to query `SELECT m.*, qt.question_zh FROM markets m LEFT JOIN question_translations qt ...` (scanner.py:147-154). Against a temp DB containing only `l2_top_of_book` — no `markets` table — this would fail with "no such table: markets".
- **Fix:** Build the temp DB with a `markets` table holding the tob columns (asset_id, ts, spread, depth_yes_usd, …) + NULL `question`, and an empty `question_translations` table so the LEFT JOIN doesn't error. The recipe's WHERE references `spread`/`depth_yes_usd`/`ts` as `markets` columns — query runs cleanly through the existing scanner.
- **Files modified:** `src/polyarb/observation/l3_promote.py`
- **Verification:** `test_recipe_ts_predicate_filters_synthetic_rows_correctly` exercises the full scanner round-trip and passes.
- **Committed in:** `482619e` (Task 2 commit)

**3. [Rule 1 - Bug] "apscheduler" string in run_periodic docstring trips lint test**

- **Found during:** Task 2 GREEN (test_run_periodic_uses_wait_for_pattern lint failure)
- **Issue:** Task 1 lint test asserts `"apscheduler" not in src.lower()` to prevent regression to a scheduler dep. My initial docstring mentioned "no APScheduler dep" — the case-insensitive check failed.
- **Fix:** Paraphrased to "no scheduler dep is added here" — same intent, no forbidden substring.
- **Files modified:** `src/polyarb/observation/l3_promote.py`
- **Verification:** lint test green.
- **Committed in:** `482619e` (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (all Rule 1 — bugs in plan example code rather than scope creep)
**Impact on plan:** All three are local correctness fixes to plan pseudocode; no scope creep, no architectural change. The fixes are required for the plan's stated outcomes (Blocker #1 mirror surface + Blocker #2 ts predicate) to actually work.

## Issues Encountered

- **Flaky test under broad collection:** `tests/m1-perception/test_chaos_r2.py::test_r2_retry_config_is_applied` fails when run as part of `uv run pytest tests/m1-perception/ -x` but passes in isolation. Pre-existing cross-test state leak in tenacity/boto retry config; unrelated to Plan 05-04 code paths. Logged to `deferred-items.md` for a separate housekeeping task.

## User Setup Required

None — no external service configuration required. The 3 L3 sub-checks will surface on `/health` as soon as the daemon ships with this code; cold-start warns are expected for the first 5 minutes (until the initial `promote_run` populates `_l3_active_set` and `_last_promote_at_s`).

## Next Phase Readiness

- **Plan 05-05** (next plan in Wave 4) can now consume `_l3_active_set` as a stable, populated set — the brain is wired and the dashboard surface (`l2_candidates.l3_promoted_at_ts`) is live.
- **Prod L2 deploy:** When the next L2 deploy ships (carries Plan 05 Wave 3 main + 04.1 code-review fixes + GAP-401 watchdog liveness from quick-task 260531), the 3 new /health sub-checks will appear immediately. Operators should expect cold-start warns for the first 5 min, then `l3:active_count` settles at 10 and `l3:last_promote_at_s` ages cyclically (0-300s).
- **Backstop verifications for prod:**
  1. `curl http://l2-host/health | jq '.checks["l3:active_count"]'` → status=pass, observedValue=10 within 6 min of boot.
  2. `psql ... -c 'SELECT COUNT(*) FROM l2_candidates WHERE l3_promoted_at_ts IS NOT NULL'` → returns 5 after first successful tick.
  3. Sentry breadcrumbs: `category="l3-promote" level="info"` per 5-min cron; `category="l2-mirror" level="info"` per tick.
- **Known stubs:** None (Plan 04 ships the brain; no placeholder behavior remains).

## Verification Snapshot (last-known good before this SUMMARY)

```
$ uv run pytest tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_l2_health_l3_subchecks.py tests/m1-perception/test_ws_watchdog_liveness.py
35 passed in ~1.1s

$ uv run pytest tests/m1-perception/test_ws_consumer_dynamic_subscribe.py tests/m1-perception/test_l2_supabase_mirror_book_levels.py tests/m1-perception/test_alembic_005_ohlc_views.py tests/m1-perception/test_candidate_refresh_l3_protection.py tests/m1-perception/test_l2_main_book_levels.py tests/m1-perception/test_observation_scanner.py
104 passed in ~1.5s

$ uv run pytest tests/m1-perception/test_l2_health_endpoint.py tests/m1-perception/test_l2_health_mirror_check.py tests/m1-perception/test_l2_health_rss.py
21 passed in ~1.1s

$ uv run pytest tests/m1-perception/test_daemon_shutdown.py tests/m1-perception/test_l2_main_book_levels.py tests/m1-perception/test_l2_startup_prime.py
19 passed in ~1.2s
```

Grep checks (Plan §verification 1-12 + §done):

```
$ grep -c "_is_trusted=True" src/polyarb/observation/l3_promote.py             # 3 (≥1)
$ grep -c "trusted-yaml\|Source-controlled yaml" src/polyarb/observation/scanner.py  # 2 (≥1)
$ grep -c "scan_recipes/l3-promote.yaml" src/polyarb/daemon/l2_main.py         # 1
$ grep -c "from polyarb.observation import l3_promote" src/polyarb/http/l2_health.py # 1 (≥1)
$ grep -c "asyncio.wait_for.*stop_event\.wait" src/polyarb/observation/l3_promote.py # 2 (≥1)
$ grep -c "_mirror_l3_promoted_at_ts" src/polyarb/observation/l3_promote.py    # 2 (≥2)
$ grep -c "strftime.*-1 hour.*1000" src/polyarb/scan_recipes/l3-promote.yaml   # 2 (≥1)
$ grep -c "_fetch_market_token_map" src/polyarb/observation/l3_promote.py      # 3 (≥2)
$ grep -c "_l3_active_set: set\[str\] = set()" src/polyarb/observation/l3_promote.py  # 1
$ grep -c "def get_l3_active_set\|def get_l3_active_count\|def get_last_promote_at_s\|def get_last_book_levels_write_at_s" src/polyarb/observation/l3_promote.py  # 4
$ grep -c "L3_EXPECTED_TOKEN_COUNT = 10" src/polyarb/http/l2_health.py         # 2 (≥1)
$ grep -c "l3:active_count\|l3:last_promote_at_s\|l3:last_book_levels_write_at_s" src/polyarb/http/l2_health.py  # 6 (≥3)
```

All Plan §success_criteria satisfied.

## Self-Check: PASSED

All claimed files present on disk; all 3 task commits reachable in git log.

- FOUND: src/polyarb/scan_recipes/l3-promote.yaml
- FOUND: src/polyarb/observation/l3_promote.py
- FOUND: src/polyarb/observation/scanner.py
- FOUND: src/polyarb/http/l2_health.py
- FOUND: src/polyarb/daemon/l2_main.py
- FOUND: tests/m1-perception/test_l3_promoter.py
- FOUND: tests/m1-perception/test_l2_health_l3_subchecks.py
- FOUND: .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-04-SUMMARY.md
- FOUND COMMIT: 2af6c1e (Task 1 RED)
- FOUND COMMIT: 482619e (Task 2 GREEN)
- FOUND COMMIT: 9b5017e (Task 3 GREEN)

---
*Phase: 05-ws-book-prices*
*Completed: 2026-06-01*
