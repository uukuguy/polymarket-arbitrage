---
phase: 04-candidate-set-l2-throughput
plan: 01
subsystem: database
tags: [alembic, supabase, postgres, narrow-projection, mirror, d-07]

# Dependency graph
requires:
  - phase: 03-l2-orderbook-prod-hardening
    provides: alembic 003 chain head + supabase_mirror.py narrow projection baseline (10-col)
  - phase: 02-supabase-mirror
    provides: SupabaseMirror.push_snapshot + narrow_market_row contract + L15 add-only discipline
provides:
  - markets_latest.yes_token_id nullable TEXT column (live Supabase Postgres, post-Alembic 004)
  - supabase_mirror.narrow_market_row 11-column projection including yes_token_id passthrough
  - alembic add-only discipline pattern reusable for future Phase 04+ column additions
affects: [04-02, 04-03, 04-04, m1-perception-watchlist, m2-combinatorial]

# Tech tracking
tech-stack:
  added: []  # No new dependencies — alembic + sqlalchemy already in project
  patterns:
    - "Plan-level [BLOCKING] step pattern: write file + tests in agent context; pause for human approval before live-DB mutation; document push evidence in SUMMARY"
    - "11-column narrow projection extension via _NARROW_MARKET_COLUMNS tuple — no special-case branch needed (default `.get(col)` handles nullable passthrough)"

key-files:
  created:
    - alembic/versions/004_add_yes_token_id.py
    - tests/alembic/test_004.py
    - tests/storage/test_supabase_mirror.py
  modified:
    - src/polyarb/storage/supabase_mirror.py

key-decisions:
  - "D-07 column type sa.Text nullable=True — matches source semantics (normalizer.py:107 returns str | None from clobTokenIds[0])"
  - "No special-case branch in narrow_market_row() — existing default `out[col] = full_row.get(col)` correctly passes through None for absent / explicitly-None source values"
  - "Tests/storage/test_supabase_mirror.py is the canonical D-07 regression file (separate from tests/m1-perception/test_supabase_mirror.py which covers push_snapshot/reconcile integration)"

patterns-established:
  - "Add-only Alembic migration with downgrade-for-testcontainer-replay: upgrade() uses only op.add_column; downgrade() uses op.drop_column for CI replay safety; production never executes downgrade"
  - "Per-narrow-projection-extension test pattern: 3 unit tests (present-value, missing-key None, explicit-None) + 1 contract test (set-equality on _NARROW_MARKET_COLUMNS) + regression test on existing special-case branch"

requirements-completed: []  # ROADMAP-scoped workstream, no REQ-IDs (covers D-07 from 04-CONTEXT.md)

# Metrics
duration: 25min
completed: 2026-05-28
---

# Phase 04 Plan 01: D-07 markets_latest.yes_token_id widening Summary

**Alembic 004 add-only migration adds nullable yes_token_id TEXT to Supabase markets_latest, paired with supabase_mirror 11-column narrow projection that passthrough-writes yes_token_id from normalizer.clobTokenIds[0].**

## Performance

- **Duration:** ~25 min (Task 1 + Task 2 code/test work, excluding [BLOCKING] gate wait)
- **Started:** 2026-05-28T05:23:00Z
- **Completed:** 2026-05-28T05:47:00Z (code + tests; live push evidence appended after operator approval)
- **Tasks:** 2 (Task 1 + Task 2 Steps A+B; Task 2 Step C [BLOCKING] push captured in dedicated section below)
- **Files modified:** 4 (3 created + 1 modified)

## Accomplishments

- Alembic 004 add-only migration ready: chains 003 → 004; upgrade() is op.add_column ONLY (no DROP/RENAME); downgrade() drops for replay safety
- supabase_mirror.py `_NARROW_MARKET_COLUMNS` widened 10 → 11 columns; `yes_token_id` added with inline comment citing normalizer source line
- 6 new mirror unit tests + 5 new alembic tests (3 static + 2 slow live-DB testcontainer) all green
- Regression: 22 existing mirror tests still pass after projection widening
- 11-column projection now enables Plan 02 temp-DB watchlist path (`SELECT yes_token_id FROM markets WHERE slug=?`) to receive real WS asset_ids from Supabase

## Task Commits

Each task was committed atomically:

1. **Task 1: Alembic 004 migration + static-check tests** — `083cf5f` (feat)
   - `alembic/versions/004_add_yes_token_id.py` (new): revision="004", down_revision="003", `op.add_column("markets_latest", sa.Column("yes_token_id", sa.Text, nullable=True))`
   - `tests/alembic/test_004.py` (new): 3 static tests (chain, no-drop, revision id) + 2 `@pytest.mark.slow` Docker-gated live-DB tests (column exists nullable TEXT, idempotent downgrade→upgrade replay)
2. **Task 2: Mirror narrow projection + [BLOCKING] live push** — `507be65` (feat) — see below for full body
   - Step A: `src/polyarb/storage/supabase_mirror.py`: added `"yes_token_id"` to `_NARROW_MARKET_COLUMNS` (10 → 11 cols)
   - Step B: `tests/storage/test_supabase_mirror.py` (new): 6 unit tests covering narrow projection contract
   - Step C: [BLOCKING] live `uv run alembic upgrade head` against Supabase Postgres — see "Live Push Evidence (D-07)" below; pushed at 2026-05-28T06:55:33Z (UTC) after operator approval
3. **Plan metadata: SUMMARY amendment with live D-07 evidence** — see latest `docs(04-01): record live D-07 alembic push evidence` commit (hash recorded in completion message)

## Files Created/Modified

- `alembic/versions/004_add_yes_token_id.py` — add-only migration adding `markets_latest.yes_token_id` nullable TEXT (Phase 04 D-07)
- `tests/alembic/test_004.py` — migration static-check tests + Docker-gated live testcontainer tests
- `tests/storage/test_supabase_mirror.py` — canonical D-07 regression test file for narrow_market_row 11-column contract
- `src/polyarb/storage/supabase_mirror.py` — `_NARROW_MARKET_COLUMNS` widened from 10 to 11 columns (added `yes_token_id` with inline source-line comment)

## must_haves.truths Verification

The plan promised 4 must_haves.truths. Evidence:

1. **"markets_latest table has a nullable yes_token_id column live in Supabase Postgres"** — see "Live Push Evidence (D-07)" section below; `information_schema.columns` query proves column existence + nullability.
2. **"narrow_market_row() output dict includes yes_token_id (None when source row lacks it)"** — VERIFIED by `tests/storage/test_supabase_mirror.py::test_narrow_includes_yes_token_id_when_present` (passes with value passthrough) + `::test_narrow_yes_token_id_none_when_absent` (None when key missing) + `::test_narrow_yes_token_id_none_when_explicit_none` (None passthrough when source explicitly None).
3. **"Existing markets_latest rows get NULL for yes_token_id (add-only, no data loss)"** — VERIFIED by add-only migration shape (`op.add_column` with `nullable=True`, no DEFAULT); PostgreSQL semantics fill new nullable column with NULL for pre-existing rows.
4. **"Migration 004 chains after 003 and contains no DROP in upgrade()"** — VERIFIED by `tests/alembic/test_004.py::test_down_revision_chain_to_003` + `::test_no_drop_in_upgrade` (both PASS): static text scan of file body asserts `down_revision = "003"` literal present + no `op.drop_` substring in `upgrade()` body between `def upgrade(` and `def downgrade(`.

## Alembic 004 Revision Chain Check

```
$ uv run alembic heads
004 (head)

$ grep -E '^(revision|down_revision) ' alembic/versions/004_add_yes_token_id.py
revision = "004"
down_revision = "003"
```

Chain: 001 → 002 → 003 → **004** (head).

## Live Push Evidence (D-07) — [BLOCKING] Step

**Status:** ✅ PUSHED + VERIFIED on Supabase Postgres at **2026-05-28T06:55:33Z** (UTC).

Operator approved the live ALTER TABLE; continuation agent sourced the main-repo `.env`, ran sanity checks (role: `postgres`, db: `postgres`), confirmed pre-state at revision `003` with column absent, then executed the migration.

### Alembic revision: before → after

```
$ uv run alembic current   # BEFORE
INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
INFO  [alembic.runtime.migration] Will assume transactional DDL.
003

$ uv run alembic upgrade head
INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
INFO  [alembic.runtime.migration] Will assume transactional DDL.
INFO  [alembic.runtime.migration] Running upgrade 003 -> 004, Add yes_token_id nullable column to markets_latest (Phase 04 D-07)

$ uv run alembic current   # AFTER
INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
INFO  [alembic.runtime.migration] Will assume transactional DDL.
004 (head)
```

### Live column proof (information_schema query)

```
$ psql "$POLYARB_SUPABASE_DB_DSN" -c "\
    SELECT column_name, is_nullable, data_type \
    FROM information_schema.columns \
    WHERE table_name='markets_latest' AND column_name='yes_token_id'"

 column_name  | is_nullable | data_type
--------------+-------------+-----------
 yes_token_id | YES         | text
(1 row)
```

### `\d+ markets_latest` confirmation (column row)

```
$ psql "$POLYARB_SUPABASE_DB_DSN" -c "\d+ markets_latest" | grep -i yes_token_id
 yes_token_id  | text             |           |          |         | extended |             |              |
```

The column is present, nullable (`is_nullable = YES`), of type `text`, with no DEFAULT (existing rows receive NULL per PostgreSQL semantics) and `extended` storage. This matches the migration's `op.add_column("markets_latest", sa.Column("yes_token_id", sa.Text, nullable=True))` exactly.

### Pre-push role / database sanity

```
$ psql "$POLYARB_SUPABASE_DB_DSN" -c "SELECT current_user, current_database()"
 current_user | current_database
--------------+------------------
 postgres     | postgres
(1 row)
```

**SECURITY NOTE:** The DSN was sourced via `set -a; . /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage/.env; set +a` (main-repo `.env`, not copied into worktree). `$POLYARB_SUPABASE_DB_DSN` was never echoed. Only `information_schema` query results, role/db identifiers, and alembic revision strings are recorded above. The yes_token_id values are public market metadata (Polymarket asset IDs); no PII.

**D-07 must_have #1 ("markets_latest table has a nullable yes_token_id column live in Supabase Postgres") is now backed by live production evidence.**

## Decisions Made

- **Test file location**: Created `tests/storage/test_supabase_mirror.py` (new) rather than appending to `tests/m1-perception/test_supabase_mirror.py`. Plan's acceptance criteria explicitly named `tests/storage/test_supabase_mirror.py` as the canonical D-07 test home; keeping narrow-projection contract tests in a small dedicated file simplifies future column-add extensions (D-XX).
- **No special-case branch added to `narrow_market_row()`**: Per RESEARCH Q3 + PATTERNS.md, the existing default branch `out[col] = full_row.get(col)` correctly handles nullable passthrough for `yes_token_id`. Adding a redundant special-case would have been noise.
- **Inline comment cites source-line**: Added `# D-07: nullable; source = normalizer.py:107 clobTokenIds[0]` next to `"yes_token_id"` tuple entry — anchors future readers to the upstream nullability contract without forcing a fresh code-graph hunt.

## Deviations from Plan

None — plan executed exactly as written for Task 1 + Task 2 Steps A+B. Task 2 Step C ([BLOCKING] live push) paused as the plan explicitly mandated (autonomous: false; operator must approve prod schema change).

## Issues Encountered

- **Worktree `.env` absence**: The git worktree at `.claude/worktrees/agent-aff6b8d0824ba573c/` does NOT have its own `.env`; the real `.env` lives in the main repo at `/Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage/.env` (verified: 1 line matching `^POLYARB_SUPABASE_DB_DSN=`). The push step must source `.env` from the main repo path. `make supabase-migrate` uses `set -a; [ -f .env ] && . ./.env; set +a` which reads CWD-local `.env` only — continuation agent will need to either (a) `set -a; . /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage/.env; set +a; uv run alembic upgrade head`, or (b) symlink/copy the parent `.env` into the worktree before invoking `make supabase-migrate`. Recommended (a) — keeps worktree clean.

## User Setup Required

None — `POLYARB_SUPABASE_DB_DSN` already in main-repo `.env` (verified). No new secrets required. The [BLOCKING] gate is operator approval of the prod schema ALTER, not a missing credential.

## Threat Flags

No new threat surface introduced beyond plan's threat_model coverage (T-04-01 / T-04-02 / T-04-03 all addressed in plan). DSN handling follows existing secrets-hygiene memory.

## Next Phase Readiness

- **04-02** (data-source swap + temp DB) now has the schema prerequisite: once Step C push completes, `markets_latest.yes_token_id` exists live, unblocking Plan 02's `_NARROW_TO_MARKETS` mapping for `yes_token_id` → `yes_token_id`.
- **04-03** (Makefile chaos-l2-inj4-throughput + frame counters) is independent of D-07 and ready to plan in parallel.
- **04-04** (D-08 GAP-200 + prod chaos human-verify) is also independent of D-07.

---

*Phase: 04-candidate-set-l2-throughput*
*Plan: 01*
*Completed: 2026-05-28 — code + tests + live D-07 push to Supabase Postgres (revision 004, evidence above)*
