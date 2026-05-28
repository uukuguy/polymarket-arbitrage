---
phase: 04-candidate-set-l2-throughput
plan: 02
subsystem: market-perception
tags: [l2-daemon, candidate-set, supabase, sqlite, pagination, fail-soft, chain-truth]

# Dependency graph
requires:
  - phase: 04-01
    provides: "markets_latest.yes_token_id column live (Alembic 004) + narrow projection widened to 11 cols including yes_token_id"
  - phase: 04-03
    provides: "l2_health.py three-branch mirror gate (we ADD candidates sub-check on top — must not regress mirror branches)"
  - phase: 03-05
    provides: "l2_candidate_refresh module skeleton (compute_candidates, on_snapshot_complete, debounce)"
  - phase: 01.1
    provides: "scanner.run_recipe SQL execution path + 4-layer defense + BUILTIN_RECIPES"

provides:
  - "compute_candidates reads REAL markets_latest data (no longer reads empty L2 SQLite)"
  - "_fetch_all_markets_latest pagination helper (1000-row PostgREST cap handled)"
  - "build_temp_db named-temp-file adapter (replaces broken :memory: pitfall)"
  - "warn_null_filled_recipe_columns fail-loud helper (no silent 0-row results)"
  - "_record_fetch_success + get_last_fetch_success_at_s write/read side for chain-truth"
  - "/health candidates:supabase_fetch_age_seconds sub-check (Inj L2-2-style failure mode prevented)"
  - "compute_candidates BUILTINS bug fix (Rule 1 deviation — pre-existing latent bug)"

affects: ["04-04 (prod chaos uses real candidate set)", "m2-combinatorial (real candidate set flows downstream)"]

# Tech tracking
tech-stack:
  added: ["tempfile.NamedTemporaryFile (named-temp-file SQLite adapter, not :memory:)"]
  patterns:
    - "fail-soft fetch with last-known-rows fallback (same envelope as recipe loop's per-recipe try/except)"
    - "chain-truth /health sub-check reading a field the write path mutates (NOT a dead-code config gate)"
    - "PostgREST pagination via .range(offset, offset+page_size-1) + len(batch) < page_size loop exit"
    - "PRAGMA foreign_keys=OFF on throwaway temp DB to avoid FK violation on sentinel snapshot_id"
    - "_SENTINEL_FILL dict for NOT-NULL columns absent from narrow projection (condition_id='', fetched_at_ms=0)"

key-files:
  created:
    - "src/polyarb/observation/l2_temp_db.py"
    - "tests/observation/test_l2_temp_db.py"
    - "tests/http/test_l2_candidates_fetch_health.py"
    - ".planning/workstreams/m1-perception/phases/04-candidate-set-l2-throughput/deferred-items.md"
  modified:
    - "src/polyarb/observation/l2_candidate_refresh.py (pagination, temp DB integration, fail-soft fetch, chain-truth write side, BUILTINS bug fix)"
    - "src/polyarb/http/l2_health.py (Check 4b: candidates:supabase_fetch_age_seconds sub-check)"
    - "tests/observation/test_l2_candidate_refresh.py (7 new tests + _reset_debounce_state fixture extended)"

key-decisions:
  - "D-02 Option A (PRAGMA foreign_keys=OFF on temp DB) chosen over Option B (seed snapshots row) — Option A is fewer LOC and FK integrity is meaningless on a throwaway DB the scanner opens read-only."
  - "_CANDIDATES_FETCH_WARN_S=120, _CANDIDATES_FETCH_FAIL_S=600 thresholds: warn at 2× REFRESH_DEBOUNCE_S, fail at 10× — leaves headroom for transient blips without firing alarms."
  - "Cold-start last_fetch=None → status='warn' (NOT fail) — boot must not trip /health alarm."
  - "Bug fix INSIDE the plan (Rule 1 deviation): pre-existing 'list_all_recipes(scanner_yaml) if scanner_yaml else {}' silently dropped BUILTIN_RECIPES when scanner_yaml=None. Phase 04 D-01 runs scanner_yaml=None most of the time so this latent bug had to be fixed for the new path to drive candidates."

patterns-established:
  - "Pattern: real-file SQLite adapter from a narrow REST projection — tempfile.NamedTemporaryFile + full DDL + sentinel-fill NOT-NULL absent cols + NULL-fill nullable cols + WARN when recipe references NULL-filled cols"
  - "Pattern: fail-soft fetch with chain-truth surface — write side calls _record_fetch_success on success; read side at /health reads the same field via public getter; sustained failure becomes a fail status not silence"
  - "Pattern: pre-fetch markets data BEFORE compute_candidates so the same temp DB feeds both NOTIFY-driven and ad-hoc compute paths"

requirements-completed: []

# Metrics
duration: ~50min
completed: 2026-05-28
---

# Phase 04 Plan 02: Candidate Set 通路打通 — Supabase fetch → temp DB → scanner (D-01/D-02/D-03/D-04)

**L2 compute_candidates now reads REAL markets_latest data (no longer empty local SQLite) via paginated Supabase fetch → named-temp-file SQLite → existing scanner recipes; fail-soft surfaces to /health.**

## Performance

- **Duration:** ~50min (Task 1 RED→GREEN, Task 2 RED→GREEN incl. one bug discovery + fix, Task 3 RED→GREEN)
- **Started:** 2026-05-28T15:25Z (approx, agent start)
- **Completed:** 2026-05-28T16:15Z (approx)
- **Tasks:** 3 (all TDD)
- **Files modified:** 6 (3 created, 3 modified)

## Accomplishments
- `compute_candidates` now reads from Supabase `markets_latest` (real data) instead of empty L2 SQLite. Phase 04 main objective met.
- `_fetch_all_markets_latest` paginates past PostgREST 1000-row cap (markets_latest ~6729 rows fully retrieved per call).
- `build_temp_db` is a **named-temp-file** SQLite (NOT `:memory:`) — proven readable by a separate connection (RESEARCH Pitfall 1 avoided).
- Fail-soft fetch surfaces to `/health` via new sub-check `candidates:supabase_fetch_age_seconds` (cold-start=warn, fresh=pass, 2× debounce=warn, 10× debounce=fail). Sustained Supabase outage now becomes a `fail` status rather than silent candidate-set freeze (Inj L2-2-style failure prevented).
- Pre-existing latent bug fixed (Rule 1 deviation): `list_all_recipes(scanner_yaml) if scanner_yaml else {}` was dropping BUILTINS when scanner_yaml=None. Phase 04 D-01 normally runs with scanner_yaml=None, so this bug had to be fixed for builtins (near-end / coin-flip / etc) to drive candidates at all.

## Task Commits

Each task was committed atomically:

1. **Task 1: l2_temp_db.py — named-temp-file Supabase adapter (D-02)** — `52858c1` (feat, TDD: RED→GREEN combined commit, 6 tests pass)
2. **Task 2: Supabase fetch + temp DB compute path + fail-soft (D-01/D-03/D-04)** — `de54785` (feat, TDD: RED→GREEN, 7 new tests + 1 Rule 1 bug fix)
3. **Task 3: candidates:supabase_fetch_age_seconds /health sub-check (D-01 chain-truth)** — bundled with SUMMARY commit below (feat, TDD: RED→GREEN, 5 tests pass)

**Plan metadata commit (bundles Task 3 + SUMMARY):** see final docs commit.

## Files Created/Modified

### Created
- `src/polyarb/observation/l2_temp_db.py` (202 lines) — adapter module: `build_temp_db(markets_rows) → Path`, `_insert_narrow_row`, `_maybe_insert_question_translation`, `warn_null_filled_recipe_columns`. Full schemas.DDL applied + `PRAGMA foreign_keys=OFF`.
- `tests/observation/test_l2_temp_db.py` (6 tests) — schema completeness, real-file-not-memory, NULL-fill warn, ghost-suspicious empty validation_issues, sentinel-fill NOT-NULL, FK-OFF allows orphan snapshot_id.
- `tests/http/test_l2_candidates_fetch_health.py` (5 tests) — cold-start warn, fresh pass, warn threshold, stale fail, not-registered when Supabase unconfigured.
- `.planning/workstreams/m1-perception/phases/04-candidate-set-l2-throughput/deferred-items.md` (1 entry — Makefile contract test pre-existing failure D-Defer-01).

### Modified
- `src/polyarb/observation/l2_candidate_refresh.py`:
  - Imports `os`, `tempfile` removed (only `os` kept for `os.unlink`), `from supabase import create_client`, `from polyarb.observation.l2_temp_db import build_temp_db, warn_null_filled_recipe_columns`
  - Module state: `_last_known_markets_rows`, `_last_fetch_success_at_s`
  - Helpers: `_fetch_all_markets_latest` (pagination), `_record_fetch_success`, `get_last_fetch_success_at_s`
  - `compute_candidates` signature: `markets_rows: list[dict] | None = None`. Wraps body in try/finally with `os.unlink`.
  - `_compute_candidates_against` extracted (verbatim recipe+watchlist loop, only db_path arg changes).
  - `on_snapshot_complete`: pre-fetch via `_fetch_all_markets_latest` inside try/except → fail-soft to `_last_known_markets_rows`.
  - Rule 1 bug fix: `list_all_recipes(scanner_yaml) if scanner_yaml else {}` → `list_all_recipes(scanner_yaml)` (helper handles None).
- `src/polyarb/http/l2_health.py`:
  - `_CANDIDATES_FETCH_WARN_S=120`, `_CANDIDATES_FETCH_FAIL_S=600` constants.
  - Check 4b: `candidates:supabase_fetch_age_seconds` sub-check between mirror gate (Check 4) and chaos check (Check 5). Gated on `_supabase_url` (case-a unchanged), maps cold-start → warn, age → pass/warn/fail.
- `tests/observation/test_l2_candidate_refresh.py`:
  - `_reset_debounce_state` autouse fixture now also resets `_last_known_markets_rows` and `_last_fetch_success_at_s` (test isolation for module state).
  - 7 new tests appended (pagination 1500 rows, single page <1000, temp DB usage, D-04 fallback, fail-soft, cap 500 with Supabase rows, fetch success records timestamp).

## must_haves.truths — verified

| # | Truth | Evidence |
|---|-------|----------|
| 1 | compute_candidates reads markets_latest from Supabase (via temp DB) instead of empty L2 local SQLite | `l2_candidate_refresh.py:191-203` build_temp_db invocation; `test_compute_candidates_uses_temp_db_when_markets_rows` |
| 2 | Supabase fetch paginates past 1000-row cap | `l2_candidate_refresh.py:79-95` `_fetch_all_markets_latest` `.range(offset, offset+page_size-1)` + `len(batch) < page_size` exit; `test_fetch_pagination` returns 1500 from page1=1000 + page2=500 |
| 3 | Temp DB is a named temp file (NOT :memory:) so run_recipe's separate connection sees data | `l2_temp_db.py:113-115` `tempfile.NamedTemporaryFile(suffix='.db')`; `test_build_temp_db_is_real_file_not_memory` opens via separate `sqlite3.connect(f"file:{path}?mode=ro", uri=True)` and reads rows |
| 4 | Adapter fail-loud: validation_issues + event_tags always created; NULL-filled cols WARN (not crash) | `l2_temp_db.py:119` `con.executescript(DDL)` creates all aux tables; `warn_null_filled_recipe_columns` at `l2_temp_db.py:189-208`; `test_ghost_suspicious_empty_validation_issues` runs subquery against empty validation_issues without error; `test_null_filled_column_warns` captures WARNING log |
| 5 | Supabase fetch failure → last-known rows used, daemon does not crash, /health surfaces sub-check | `l2_candidate_refresh.py:355-368` try/except envelope around create_client + fetch; `test_supabase_fetch_fail_uses_last_known` — RuntimeError side_effect, no exception raised; `l2_health.py` Check 4b — `test_fetch_health_stale_fails` returns status=fail, overall=fail |
| 6 | bootstrap_asset_ids still drive WS before first NOTIFY (D-04 cold-start) | `_compute_candidates_against` path with `markets_rows=None` falls back to `Path(settings.db_path)`; `test_compute_candidates_fallback_to_db_path_when_no_rows` confirms — l2_main.py bootstrap path untouched (not modified by this plan) |
| 7 | Temp file unlinked after compute_candidates returns (no /tmp leak) | `l2_candidate_refresh.py:181-189` try/finally with `os.unlink(db_path)`; `test_compute_candidates_uses_temp_db_when_markets_rows` asserts `not p.exists()` post-call via spy |

## chain-truth checklist (§1.6 — D-01 fail-soft surface)

| Step | Status | Evidence |
|------|--------|----------|
| Which /health sub-check observes the fail-soft fetch path? | `candidates:supabase_fetch_age_seconds` | `l2_health.py:265` |
| What data source does the sub-check read? | `l2_candidate_refresh.get_last_fetch_success_at_s()` → module-level `_last_fetch_success_at_s` | `l2_health.py:276`, `l2_candidate_refresh.py:64-66` |
| What config flag gates it? | NONE — gated only on `_supabase_url` non-empty (real condition, not a never-flipped flag) | `l2_health.py:269` `if _supabase_url:` |
| How does the write side update the source? | `_record_fetch_success()` called inside the success branch of `on_snapshot_complete`'s try/except (every successful fetch) | `l2_candidate_refresh.py:362` |
| Which test triggers end-to-end? | unit: `test_fetch_health_stale_fails` + `test_supabase_fetch_fail_uses_last_known`. Prod E2E: Plan 04 (chaos throughput) | tests cited above; Plan 04 is the live-Supabase chaos exercise |

## 5 RESEARCH findings — how each shaped implementation

1. **L2 `compute_candidates` reads empty local SQLite** (`l2_candidate_refresh.py:104` `db_path=Path(settings.db_path)`) → restructured: added `markets_rows` param + `build_temp_db` integration so the canonical L2 path now feeds from Supabase via a temp file. The line moved (Plan 02 code is `_compute_candidates_against(db_path, ...)`), and `db_path` is now the path returned by `build_temp_db`.
2. **PostgREST hard cap = 1000 rows** → `_fetch_all_markets_latest` uses `.range(offset, offset+999)` and loops until `len(batch) < page_size`. `test_fetch_pagination` exercises the 1000→500 sequence and asserts 1500 rows total.
3. **`:memory:` SQLite is connection-scoped** → `build_temp_db` uses `tempfile.NamedTemporaryFile(suffix='.db', delete=False)`. `test_build_temp_db_is_real_file_not_memory` opens a SEPARATE `sqlite3.connect(f"file:{path}?mode=ro", uri=True)` connection and successfully reads inserted rows — proving the pitfall is avoided.
4. **markets DDL has 4 NOT NULL columns** (`condition_id`, `fetched_at_ms`, `snapshot_id`, `incomplete`) → `_SENTINEL_FILL` dict in `l2_temp_db.py` fills with `("", 0, 0, 0)`. `PRAGMA foreign_keys=OFF` on temp DB so the sentinel snapshot_id=0 does not violate the FK. `test_not_null_columns_filled_with_sentinel` + `test_snapshot_fk_does_not_block_insert` confirm.
5. **D-08 handles mirror gate; do NOT modify config.py** → respected. Plan 02 only ADDS Check 4b (candidates sub-check) AFTER Check 4 (mirror three-branch gate). Plan 03's mirror logic is untouched. `l2_mirror_enabled` config field unchanged.

## Decisions Made

See `key-decisions` in frontmatter. Summary:
- **Option A for FK handling** (PRAGMA foreign_keys=OFF on temp DB) — Option B (seed snapshots row) would add code for zero practical benefit on a throwaway read-only-to-scanner artifact.
- **Thresholds 120s warn / 600s fail** for candidates sub-check — 2× / 10× REFRESH_DEBOUNCE_S leaves room for transient blips without false alarms.
- **Cold-start (last_fetch=None) → status='warn' NOT 'fail'** — boot must not trip an alarm before the first NOTIFY arrives.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] BUILTINS dropped when scanner_yaml=None in compute_candidates**
- **Found during:** Task 2 (test_compute_candidates_uses_temp_db_when_markets_rows initially failed with empty candidate list despite scanner recipes finding the row when run directly)
- **Issue:** `recipes = list_all_recipes(scanner_yaml) if scanner_yaml else {}` — when scanner_yaml is None, `recipes` was assigned `{}` and the loop ran over nothing. BUILTINS (near-end / coin-flip / ghost-suspicious / thick-but-slippery) were silently inert in this path. Pre-existing latent bug (commit `15cc5ab`, Plan 03-05) — invisible because all existing tests pass scanner_yaml. Phase 04 D-01 normally has `settings.candidate_scanner_yaml=None` in prod, so this bug would have made the new D-01 path return zero candidates even with the temp DB working perfectly.
- **Fix:** `recipes = list_all_recipes(scanner_yaml)` — `list_all_recipes` already accepts `Path | None` and returns just BUILTIN_RECIPES when arg is None.
- **Files modified:** `src/polyarb/observation/l2_candidate_refresh.py`
- **Verification:** `test_compute_candidates_uses_temp_db_when_markets_rows` now passes (YES-m1 present in result); all 25 observation tests pass; no regression in test_l2_candidate_refresh.py.
- **Committed in:** `de54785` (Task 2 commit, documented in commit message)

### Pre-existing failures (NOT my changes — deferred)

- `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe` — asserts hardcoded `127.0.0.1:8080/health` but Makefile recipe uses `${POLYARB_HTTP_PORT:-19080}`. Confirmed pre-existing via `git stash` regression check. Logged to `deferred-items.md` (D-Defer-01).

**Total deviations:** 1 auto-fixed (1 bug — Rule 1)
**Impact on plan:** Bug fix essential — without it, Phase 04 D-01 path would have returned zero candidates despite the data pipeline working perfectly. No scope creep; fix is 2-line, isolated, and the comment explains the latent nature.

## Issues Encountered

- `pytest-asyncio` was not installed in the active venv at agent start (despite being declared in pyproject.toml). Resolved by `uv sync --extra dev`. Async tests then ran correctly.
- First attempt at `_narrow_for_near_end` test fixture used `end_time_ms = now + 3 days` — but `near-end` builtin requires `end_time_ms BETWEEN now AND now+72h`. Shortened to 24h.

## User Setup Required

None — uses existing `POLYARB_SUPABASE_URL` + `POLYARB_SUPABASE_SERVICE_KEY` secrets (already prod). No new env vars.

## Next Phase Readiness

- ✅ Plan 04 (prod chaos `chaos-l2-inj4-throughput`) can now run against a REAL candidate set driven by Supabase markets_latest. Whether the candidate set is ≥3 assets in any given window depends on market activity at that wall-clock time (planner A2 assumption — 04-04 documents low-activity-window as a deferred outcome, not a failure).
- ✅ Mirror sub-check (Plan 03) and candidates sub-check (this plan) coexist correctly in `_build_l2_health_checks`.
- ⚠ Plan 04 should also be aware that `_last_known_markets_rows` starts None until the first successful fetch. If chaos induces a Supabase outage *during boot*, the candidate set remains the bootstrap_asset_ids (3 hardcoded) until first successful fetch. Acceptable per D-04.

## Self-Check: PASSED

All 7 expected files present; all 2 task commits (52858c1, de54785) reachable via `git log --all`. Task 3 + SUMMARY commit lands as final commit (hash recorded in JOURNAL post-commit).

---
*Phase: 04-candidate-set-l2-throughput*
*Completed: 2026-05-28*
