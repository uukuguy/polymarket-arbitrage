---
phase: 05-ws-book-prices
plan: 01
subsystem: database
tags: [alembic, postgres, supabase, ohlc, rls, brin, date_trunc, l2_book_levels, l2_candidates]

# Dependency graph
requires:
  - phase: 03-l2-orderbook-tracking-daemon
    provides: l2_top_of_book table (mid_price source for OHLC views) + RLS anon_read convention
  - phase: 04-candidate-set-expansion
    provides: Alembic 004 (markets_latest.yes_token_id) — establishes down_revision chain
provides:
  - "Alembic 005 migration: l2_book_levels (top-10 depth, append-only) + 3 OHLC views + l2_candidates.l3_promoted_at_ts"
  - "Wave 0 RED tests as schema-contract lint (6 substring-based file checks)"
  - "Makefile target make supabase-migrate-test for forward+reverse+forward roundtrip validation"
affects: [05-02-plans, 05-03-plans, 05-04-plans, 05-05-plans, 05-06-plans, dashboard/app/l3]

# Tech tracking
tech-stack:
  added: []  # No new deps — all Postgres core + existing alembic/sqlalchemy
  patterns:
    - "Lint-only RED tests for migrations (no live DB needed in CI)"
    - "date_trunc + epoch-floor() bucket idiom (Postgres core, no TimescaleDB)"
    - "Explicit GRANT SELECT ... TO anon on views (views don't inherit base-table RLS)"
    - "Surrogate-id + composite UNIQUE on append-only top-10 tables"

key-files:
  created:
    - "alembic/versions/005_l2_book_levels_and_ohlc.py"
    - "tests/m1-perception/test_alembic_005_ohlc_views.py"
  modified:
    - "Makefile (added supabase-migrate-test target)"

key-decisions:
  - "Use Postgres core date_trunc + epoch floor() — NEVER TimescaleDB time_bucket (Supabase PG17 deprecation, D-03 revised)"
  - "Surrogate id + UNIQUE(asset_id, ts, side, level) on l2_book_levels — matches l2_top_of_book style"
  - "Reuse l2_candidates with new l3_promoted_at_ts column (Pitfall 8 Option C) — no new view/table"
  - "Explicit GRANT SELECT TO anon on each OHLC view — Phase 02 D-19 pattern"
  - "Migration is pure add-only — no DROP / RENAME in upgrade() (Phase 02 LEARNINGS L15)"

patterns-established:
  - "Anti-regression lint via substring search: when a migration must NOT contain a specific identifier (e.g., a deprecated function), the Wave 0 RED test enforces this — and the migration's own docstring must avoid mentioning the forbidden substring even in explanatory text"
  - "Make target with exit-77 skip when required env var is unset (DSN-aware roundtrip validation)"

requirements-completed: [PHASE05-R02, PHASE05-R03]

# Metrics
duration: 19min
completed: 2026-06-01
---

# Phase 05 Plan 01: Alembic 005 Schema Foundation Summary

**Alembic 005 lands l2_book_levels (top-10 append-only depth) + 3 OHLC views (1m/5m/1h on l2_top_of_book.mid_price using Postgres core date_trunc) + l2_candidates.l3_promoted_at_ts nullable column — the schema bottleneck for every other Phase 05 plan.**

## Performance

- **Duration:** 19 min
- **Started:** 2026-06-01T05:51:21Z
- **Completed:** 2026-06-01T06:10:51Z
- **Tasks:** 3
- **Files modified:** 3 (2 created, 1 modified)

## Accomplishments

- Alembic 005 migration drafted, history-verified (`Rev: 005 (head), Parent: 004`), and committed — ready for Wave 4 push via `make supabase-migrate`
- All 6 Wave 0 schema-contract lint tests GREEN in 0.01s (no live DB needed in CI)
- `make supabase-migrate-test` target lands the forward+reverse+forward roundtrip helper; exits 77 when DSN unset (make-skip convention)
- Anti-regression guard for D-03 / Pitfall 1 is encoded as an executable test: any future PR that re-introduces TimescaleDB `time_bucket` will fail the test suite

## Task Commits

Each task was committed atomically (TDD: RED → GREEN → REFACTOR-as-feat):

1. **Task 1: RED — alembic 005 view + ddl + downgrade tests** — `96c5673` (test)
2. **Task 2: GREEN — alembic 005 l2_book_levels + 3 OHLC views (date_trunc)** — `6963207` (feat)
3. **Task 3: add make supabase-migrate-test target for 005 roundtrip validation** — `1178a09` (feat)

## Files Created/Modified

- `alembic/versions/005_l2_book_levels_and_ohlc.py` — Alembic 005 migration: `l2_book_levels` table (top-10 depth, surrogate id + composite UNIQUE + BRIN(ts) + RLS anon_read), three OHLC views over `l2_top_of_book.mid_price` (1m via `date_trunc('minute', ts)`, 5m via `to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300) AT TIME ZONE 'UTC'`, 1h via `date_trunc('hour', ts)`), explicit `GRANT SELECT ... TO anon` on each view, and `l2_candidates.l3_promoted_at_ts` nullable column with btree index for D-08 dashboard surface (Pitfall 8 Option C).
- `tests/m1-perception/test_alembic_005_ohlc_views.py` — 6 file-content lint tests encoding the schema contract: revision metadata, `date_trunc` ≥3 / `time_bucket` = 0, BRIN+RLS+UNIQUE shape on `l2_book_levels`, 3 explicit anon GRANTs, `l3_promoted_at_ts` nullable column, downgrade order (views before table).
- `Makefile` — added `supabase-migrate-test` PHONY target between `supabase-migrate` and `supabase-reconcile`; section header comment block updated.

## Decisions Made

- **`time_bucket` → `date_trunc` substitution (D-03 revised)** — RESEARCH Pitfall 1 established that TimescaleDB is deprecated on Supabase Postgres 17, so the originally-proposed `time_bucket` would fail at runtime. Migration uses `date_trunc('minute', ts)` for 1m, `date_trunc('hour', ts)` for 1h, and the epoch-floor() idiom for 5m (since `date_trunc` doesn't support arbitrary minute multiples). 12 `date_trunc` occurrences, 0 `time_bucket`.
- **Anti-regression test must include source-text lint** — the test `test_005_uses_date_trunc_not_time_bucket` does a case-insensitive substring search for `time_bucket`. The migration's own docstring therefore had to avoid mentioning the forbidden substring even in explanatory text (Rule 1 self-fix discovered during Task 2 GREEN — see Deviations below).
- **Surrogate id + composite UNIQUE (not composite PK)** — chose to match the `l2_top_of_book` / `l2_trades` style established in Alembic 003, per RESEARCH §Standard Stack / §Alternatives Considered. UNIQUE prevents duplicate rows from the same WS book frame; surrogate id keeps inserts cheap.
- **Reuse `l2_candidates` for L3 promote flag (Pitfall 8 Option C)** — adding a nullable `l3_promoted_at_ts` to the existing table inherits the existing RLS policy, mirror write path, and dashboard query envelope. Minimal-change approach over a new `l3_candidates` view.
- **Explicit GRANT on views** — Postgres views don't inherit base-table RLS policies the same way tables do. Phase 02 D-19 pattern requires explicit `GRANT SELECT ... TO anon` so the dashboard's anon key can query each OHLC view.

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] Migration docstring contained forbidden `time_bucket` substring**
- **Found during:** Task 2 (Alembic 005 GREEN)
- **Issue:** First draft of the migration's module docstring used the literal `time_bucket` identifier four times to explain the Pitfall-1 rationale. The Wave 0 anti-regression test `test_005_uses_date_trunc_not_time_bucket` does a case-insensitive substring search for `time_bucket` against the **entire migration file**, including its docstring. The test failed even though the actual SQL was correct — the lint-only test cannot distinguish "explanatory mention" from "SQL usage".
- **Fix:** Rewrote the docstring to describe the deprecated function without naming it (phrasing it as "the TimescaleDB bucket function"). Also replaced one inline comment `# date_trunc, NOT time_bucket — Pitfall 1` with `# date_trunc only — see docstring Pitfall 1 rationale`. Net effect: zero `time_bucket` occurrences in the file, rationale preserved.
- **Files modified:** `alembic/versions/005_l2_book_levels_and_ohlc.py` (2 string edits within Task 2 GREEN)
- **Verification:** `grep -c time_bucket alembic/versions/005_l2_book_levels_and_ohlc.py` → 0. All 6 tests pass.
- **Committed in:** `6963207` (the Task 2 GREEN commit — fix landed before commit, so single-commit history is clean)

---

**Total deviations:** 1 auto-fixed (Rule 1 — substring-lint discipline gap between test contract and developer-friendly comments)
**Impact on plan:** No scope creep. The auto-fix actually strengthened the documentation pattern — future authors will see that an anti-regression substring guard requires the guarded substring to be absent from comments too, not just from active SQL.

## Issues Encountered

None beyond the deviation documented above. The substring-lint test pattern is more strict than the natural-language description in the plan suggested, but the strictness is intentional: a lint test that whitelists "explanatory comments" would be trivially circumvented by a future author who adds a forbidden identifier "with a comment explaining it".

## Self-Check: PASSED

Files verified to exist:
- `alembic/versions/005_l2_book_levels_and_ohlc.py` — FOUND (214 lines)
- `tests/m1-perception/test_alembic_005_ohlc_views.py` — FOUND (218 lines)

Commits verified:
- `96c5673` (Task 1 RED) — FOUND in `git log`
- `6963207` (Task 2 GREEN) — FOUND in `git log`
- `1178a09` (Task 3 Makefile) — FOUND in `git log`

Frontmatter contract:
- `revision = "005"` present — verified
- `down_revision = "004"` present — verified
- `date_trunc` count: 12 (≥3 required)
- `time_bucket` count: 0 (forbidden)
- All 6 Wave 0 tests GREEN

## User Setup Required

None — this plan is local schema + tests + Makefile target. The Wave 4 plan (Plan 05) will push migration 005 to prod via `make supabase-migrate` after the other Phase 05 plans land their code paths. The new `make supabase-migrate-test` target is available for manual reversibility validation against a developer-pointed test database (set `POLYARB_SUPABASE_DB_DSN` to test DSN, then run the target).

## Next Phase Readiness

- **Plan 05-02 (l2_book_levels mirror writer) is unblocked** — `l2_book_levels` table DDL is locked, narrow column ordering established (`asset_id, ts, side, level, price, size`).
- **Plan 05-03 / 05-04 (L3 promoter + dashboard surface)** are unblocked on the `l3_promoted_at_ts` column.
- **Plan 05-05 (dashboard `/l3/[asset_id]` page)** is unblocked on the OHLC views — query shape `SELECT bucket_ts, open, high, low, close FROM l2_ohlc_1m WHERE asset_id = ? AND bucket_ts > now() - interval '24 hours' ORDER BY bucket_ts ASC` is now executable.
- **Wave 4 prod push (Plan 05-05 or 05-06)** must run `make supabase-migrate` and verify `uv run alembic current` shows 005. The new `make supabase-migrate-test` target should be used against a test DB first to catch any reversibility issues.
- **Concern:** The migration has not been executed against a live Postgres yet (per plan scope — that's Wave 4). The Wave 0 lint tests verify the migration text, but the actual SQL semantics (especially the 5m epoch-floor bucket and the `array_agg(... ORDER BY)` first/last pattern) are only validated when 005 runs on a real Supabase. Wave 4's verifier should run `make supabase-migrate-test` against a fresh test DB before deciding to push to prod.

---
*Phase: 05-ws-book-prices*
*Completed: 2026-06-01*
