---
phase: 05-ws-book-prices
plan: 05
subsystem: dashboard
tags: [lightweight-charts, nextjs, supabase, alembic, ohlc, depth-ladder, l3, makefile]

# Dependency graph
requires:
  - phase: 05-ws-book-prices/01
    provides: Alembic 005 migration (l2_book_levels + l2_ohlc_{1m,5m,1h} views + l2_candidates.l3_promoted_at_ts column + anon RLS GRANTs)
  - phase: 05-ws-book-prices/02
    provides: WsConsumer.add_subscriptions / remove_subscriptions (referenced by l3_promote_dry_run scripts)
  - phase: 05-ws-book-prices/03
    provides: l3_promote module state + getters (used by ohlc-spot-check /health anchors)
  - phase: 05-ws-book-prices/04
    provides: full promote_run + l2_candidates.l3_promoted_at_ts mirror (drives candidates L3 badge column)
provides:
  - Alembic 005 applied to prod Supabase (Task 1 — done by orchestrator)
  - lightweight-charts ^5.2.0 dashboard dep + getOhlcForAsset + getBookLevelsLatest query helpers
  - /l3/[asset_id] detail page (server component) with KlineChart (client) + DepthLadder
  - /candidates L3 badge column linking to /l3/<asset_id> for promoted assets
  - 3 Makefile targets — l3-promote-dry-run / ohlc-spot-check / smoke-l3-dashboard
affects: [phase-05-06-soak, m1-perception-state, m2-combinatorial-routing, dashboard-ops]

# Tech tracking
tech-stack:
  added:
    - lightweight-charts@^5.2.0 (TradingView candlestick lib, v5 addSeries API)
  patterns:
    - SSR-safe client component pattern — dynamic `await import("lightweight-charts")` INSIDE useEffect
    - Server-component → client-island data hand-off via plain Promise.all + try/catch + props
    - Makefile-routed Python helper scripts (l3_promote_dry_run.py, ohlc_spot_check.py) instead of inline heredocs

key-files:
  created:
    - dashboard/app/l3/[asset_id]/page.tsx
    - dashboard/app/l3/[asset_id]/KlineChart.tsx
    - dashboard/app/l3/[asset_id]/DepthLadder.tsx
    - scripts/l3_promote_dry_run.py
    - scripts/ohlc_spot_check.py
  modified:
    - dashboard/package.json (lightweight-charts ^5.2.0)
    - dashboard/pnpm-lock.yaml
    - dashboard/lib/supabase/l2-queries.ts (L2OhlcRow + L2BookLevel + getOhlcForAsset + getBookLevelsLatest + L2Candidate.l3_promoted_at_ts)
    - dashboard/app/candidates/page.tsx (L3 badge column)
    - Makefile (3 new targets in Phase 05 Plan 05-05 section)

key-decisions:
  - "lightweight-charts ^5.2.0 (v5) — used new addSeries(CandlestickSeries, opts) API, NOT v4 addCandlestickSeries"
  - "Dynamic `await import('lightweight-charts')` INSIDE useEffect — top-level import would crash Next.js 15 SSR with 'window is not defined'"
  - "DepthLadder rendered server-side (no 'use client') — pure presentation, no need to ship to browser"
  - "Makefile targets delegate to scripts/*.py helpers rather than inline Python heredocs — readability + AST-checkable + reusable"
  - "smoke-l3-dashboard exits 77 (skip) on unreachable URL rather than fail (2) — distinguishes 'no daemon' from 'daemon broken' for CI consumers"
  - "Default URL for ohlc-spot-check + smoke-l3-dashboard is localhost — pass URL=https://... for prod, no env var coupling"

patterns-established:
  - "lightweight-charts dynamic-import pattern — all chart libs now follow this convention in dashboard/"
  - "L2 query helpers naming — getXForAsset(assetId, ...) + getXLatest(assetId) — consistent with existing getTopOfBookForAsset / getTradesForAsset"
  - "Server-component fail-soft try/catch wrapping Supabase reads — matches Phase 02 LEARNINGS P5 pattern (already used in /candidates, /asset/[id]/tob)"
  - "Makefile section headers — `============ Phase XX Plan XX-YY — <topic>` for cross-plan grouping"

requirements-completed: [PHASE05-R02, PHASE05-R03, PHASE05-R05]

# Metrics
duration: 22min
completed: 2026-06-01
---

# Phase 05-ws-book-prices Plan 05: Alembic 005 prod push + /l3/[asset_id] dashboard + Makefile L3 ops Summary

**Alembic 005 schema live in prod Supabase, /l3/[asset_id] detail page with lightweight-charts v5 candlestick + DepthLadder, /candidates L3 badge column, and 3 new Makefile targets (l3-promote-dry-run / ohlc-spot-check / smoke-l3-dashboard) for daily L3 ops.**

## Performance

- **Duration:** ~22 min (executor wall-clock; Task 1 had been completed earlier by orchestrator)
- **Started:** 2026-06-01T08:45:00Z (executor agent spawn)
- **Completed:** 2026-06-01T09:09:14Z (last commit)
- **Tasks:** 3 (Task 1 resolved by orchestrator; this executor ran Tasks 2-4)
- **Files modified:** 5 modified + 5 created = 10 total

## Accomplishments

- **Alembic 005 in prod Supabase** (resolved by orchestrator) — l2_book_levels table + l2_ohlc_{1m,5m,1h} views + l2_candidates.l3_promoted_at_ts column all live behind anon RLS
- **lightweight-charts ^5.2.0** added to dashboard, locked in pnpm-lock.yaml (TradingView's canvas-based charting lib, ~50KB minified)
- **L2 query helpers** — getOhlcForAsset(assetId, granularity, hours) + getBookLevelsLatest(assetId) in dashboard/lib/supabase/l2-queries.ts, mirroring existing getTopOfBookForAsset / getTradesForAsset signature pattern
- **/l3/[asset_id] page** — server component, force-dynamic, parallel fetch via Promise.all, fail-soft banner on Supabase outage, two-column grid (KlineChart 1fr | DepthLadder 320px)
- **KlineChart client island** — v5 `addSeries(CandlestickSeries, opts)` API, dynamic `await import("lightweight-charts")` inside useEffect (SSR-safe), ResizeObserver for responsive width, full cleanup on unmount
- **DepthLadder server-rendered** — picks the latest ts batch from the top-20 desc fetch, groups by side, renders fixed 10-row top-10 ladder (blank cells when one side is partial)
- **/candidates L3 badge** — new "L3" column showing ★ L3 link to /l3/<asset_id> when l3_promoted_at_ts IS NOT NULL, em-dash placeholder otherwise
- **3 Makefile targets** — l3-promote-dry-run / ohlc-spot-check / smoke-l3-dashboard, all delegating to scripts/*.py helpers for readability

## Task Commits

Each task was committed atomically (--no-verify per parallel wave protocol):

1. **Task 1: [BLOCKING] Push Alembic 005 to prod Supabase** — **resolved by orchestrator** before this executor spawned. Evidence:
   - `alembic current` → `005 (head)` (was 004, now 005)
   - View smoke: l2_book_levels + l2_top_of_book + l2_trades + l2_candidates tables exist; l2_ohlc_1h / l2_ohlc_1m / l2_ohlc_5m views exist; l3_promoted_at_ts column exists. l2_ohlc_1m **returns 155 rows** from existing prod L2 top_of_book data (views work on live source rows)
   - Post-migrate Wave 0 tests: 6/6 green (`tests/m1-perception/test_alembic_005_ohlc_views.py`)
2. **Task 2: dashboard query helpers + lightweight-charts dep** — `03a2489` (feat)
3. **Task 3: /l3/[asset_id] page + KlineChart + DepthLadder + candidates L3 badge** — `a7be3f2` (feat)
4. **Task 4: Makefile l3-promote-dry-run + ohlc-spot-check + smoke-l3-dashboard targets** — `33b58ea` (feat)

_(SUMMARY.md final-commit pending after pre-commit hook bypass — committed by this executor with `--no-verify` per parallel wave protocol.)_

## Files Created/Modified

### Created
- `dashboard/app/l3/[asset_id]/page.tsx` — server component, force-dynamic, Promise.all + try/catch + fail-soft banner
- `dashboard/app/l3/[asset_id]/KlineChart.tsx` — client component, dynamic-import lightweight-charts v5, CandlestickSeries
- `dashboard/app/l3/[asset_id]/DepthLadder.tsx` — server-rendered top-10 bid/ask ladder
- `scripts/l3_promote_dry_run.py` — runs promote_run once with NoopConsumer (no real WS mutation); used by `make l3-promote-dry-run`
- `scripts/ohlc_spot_check.py` — parses /health JSON, prints 3 L3 anchors; used by `make ohlc-spot-check`
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-05-SUMMARY.md` — this file

### Modified
- `dashboard/package.json` — added `"lightweight-charts": "^5.2.0"` to dependencies
- `dashboard/pnpm-lock.yaml` — auto-updated by pnpm add
- `dashboard/lib/supabase/l2-queries.ts` — added L2OhlcRow + L2BookLevel types, getOhlcForAsset + getBookLevelsLatest helpers, l3_promoted_at_ts on L2Candidate + getActiveCandidates projection
- `dashboard/app/candidates/page.tsx` — added "L3" column header + ★ L3 link cell
- `Makefile` — appended "Phase 05 Plan 05-05" section with 3 targets and 2 supporting Python script invocations

## Decisions Made

- **lightweight-charts v5 (5.2.0) NOT v4** — v5 introduces unified `addSeries(SeriesDefinition, opts)` API. `addCandlestickSeries` (v4) is removed in v5. Picked v5 deliberately for forward-compatibility; the call site uses the v5 contract.
- **DepthLadder server-rendered** — no `'use client'` because it's pure presentation over already-fetched data. Saves ~5KB of JS shipped to browser. KlineChart MUST be client (canvas DOM API access); ladder doesn't need to be.
- **Makefile → Python script delegation** — initially tried inline Python heredoc in Makefile for ohlc-spot-check; rejected (Makefile `$` escaping + JSON dict access → unreadable chr() escapes). Promoted to dedicated `scripts/ohlc_spot_check.py` with `ast.parse` -checkable syntax + reusable independently. Same pattern for `scripts/l3_promote_dry_run.py`. This matches existing convention (`scripts/sentry_alert_audit.py`, `scripts/planning_status.py`, etc.).
- **smoke-l3-dashboard URL handling** — default `URL=http://localhost:3000`, pass `URL=https://...` for prod. No coupling to `POLYARB_DASHBOARD_URL` env var (would break parallelism with other dashboards). Exit 77 on unreachable URL signals "skip" to CI rather than "fail" (preserves Phase 02 dashboard convention).
- **`l3:active_count` extraction in ohlc-spot-check** — reads from /health's `checks` map keyed by `l3:active_count` (already exposed by Plan 05-04 promoter via 3 new sub-checks). Doesn't run a fresh query against Supabase — consistent with "/health is the canonical anchor source" pattern from Phase 04.
- **NO checkpoint pause this executor** — Task 1's `checkpoint:human-action` was resolved by the orchestrator BEFORE spawning this executor (see `<objective>`). Treated as a recorded-evidence task in this SUMMARY; no Tasks 2-4 paused.

## Deviations from Plan

None of significance. Two minor adjustments documented for traceability:

### Auto-fixed Issues

**1. [Rule 1 - Refactor] Promoted Makefile inline Python heredocs to scripts/*.py helpers**
- **Found during:** Task 4 (Makefile target authoring)
- **Issue:** Plan prescribed inline `python -c "..."` heredoc in the `ohlc-spot-check` target. First implementation produced an unreadable chr()-escaped one-liner due to Makefile's `$` quoting interacting with Python dict subscript via key strings. The l3-promote-dry-run inline heredoc was 20+ lines of cramped continuations.
- **Fix:** Created `scripts/ohlc_spot_check.py` (40 lines, parses stdin JSON, prints 3 anchors) and `scripts/l3_promote_dry_run.py` (60 lines, runs promote_run with NoopConsumer). Make targets now invoke them via `uv run python scripts/<name>.py`. Functionally identical, but `ast.parse` -checkable + readable + independently reusable.
- **Files modified:** Makefile (cleaner), scripts/ohlc_spot_check.py + scripts/l3_promote_dry_run.py (new)
- **Verification:** `python -c "import ast; ast.parse(open('scripts/l3_promote_dry_run.py').read()); ast.parse(open('scripts/ohlc_spot_check.py').read())"` → OK. `make -n l3-promote-dry-run` / `make -n ohlc-spot-check` / `make -n smoke-l3-dashboard asset_id=test123` all expand cleanly with no Makefile syntax errors.
- **Committed in:** `33b58ea` (Task 4 commit)

**2. [Rule 1 - Minor UX polish] L3 column placement + "—" placeholder for non-promoted rows**
- **Found during:** Task 3 (candidates page modification)
- **Issue:** Plan said "insert as appropriate position, e.g. after asset_id column" but didn't prescribe a placeholder for non-promoted rows. Leaving empty cells would look broken on the table.
- **Fix:** Placed L3 column immediately after asset_id (so the most-important info is visually first). Non-promoted rows render `<span style={{ color: "#444" }}>—</span>` (em-dash, very dim) so the column reads cleanly. Promoted rows show ★ L3 link with `title` attribute exposing the l3_promoted_at_ts ISO timestamp on hover.
- **Files modified:** dashboard/app/candidates/page.tsx
- **Verification:** `grep -c "l3_promoted_at_ts" dashboard/app/candidates/page.tsx` → 2. pnpm typecheck + pnpm build pass.
- **Committed in:** `a7be3f2` (Task 3 commit)

---

**Total deviations:** 2 auto-fixed (1 refactor for readability, 1 UX polish — both well within plan scope)
**Impact on plan:** Both improve maintainability / UX without changing the contract. No scope creep.

## Issues Encountered

None. Both `pnpm typecheck` and `pnpm build` succeeded first try after the file additions. The build correctly identified `/l3/[asset_id]` as a Dynamic (ƒ) route, confirming `force-dynamic` took effect and there are no SSR window-access bugs.

## Verification Evidence

### Task 1 (resolved by orchestrator pre-spawn)
- `alembic current` → `005 (head)`
- All Alembic 005 artifacts verified: l2_book_levels + 3 OHLC views + l3_promoted_at_ts column + anon GRANT SELECT
- `l2_ohlc_1m` already returns **155 rows** from existing prod l2_top_of_book data (proves views work on live data, even with l2_book_levels still empty pre-promoter)
- `tests/m1-perception/test_alembic_005_ohlc_views.py` 6/6 green post-migrate

### Task 2
- `grep -c "lightweight-charts" dashboard/package.json` → 1
- `grep -c "getOhlcForAsset\|getBookLevelsLatest" dashboard/lib/supabase/l2-queries.ts` → 2
- `cd dashboard && pnpm typecheck` → exit 0

### Task 3
- `ls dashboard/app/l3/[asset_id]/` → DepthLadder.tsx, KlineChart.tsx, page.tsx
- `grep -c "lightweight-charts" dashboard/app/l3/[asset_id]/KlineChart.tsx` → 5
- `grep -c "await import" dashboard/app/l3/[asset_id]/KlineChart.tsx` → 2
- `grep -c "l3_promoted_at_ts" dashboard/app/candidates/page.tsx` → 2
- `cd dashboard && pnpm typecheck` → exit 0
- `cd dashboard && pnpm build` → exit 0; `/l3/[asset_id]` listed as ƒ (Dynamic, server-rendered on demand). No "window is not defined" SSR errors. Bundle size: 764 B route-specific + 102 KB shared.

### Task 4
- `grep -c "^l3-promote-dry-run:\|^ohlc-spot-check:\|^smoke-l3-dashboard:" Makefile` → 3
- `make help | grep -E "l3-promote-dry-run|ohlc-spot-check|smoke-l3-dashboard" | wc -l` → 5 (each target has 2 `## ` doc lines)
- `make -n l3-promote-dry-run` → expands cleanly
- `make -n ohlc-spot-check` → expands cleanly
- `make -n smoke-l3-dashboard asset_id=test123` → expands cleanly
- `python -m ast scripts/l3_promote_dry_run.py` + `scripts/ohlc_spot_check.py` → both parse OK

## User Setup Required

None — no external service config touched. Alembic 005 was applied by the orchestrator. lightweight-charts is an npm dep with no API keys / accounts. The Makefile targets read existing /health endpoints (no new secrets).

## Next Phase Readiness

### Ready for Wave 5 (Plan 05-06 soak / verify)
- All 3 freshness anchors are surfaced via `/health` (l3:active_count, l3:last_promote_at_s, l3:last_book_levels_write_at_s) — soak verifier can probe via `make ohlc-spot-check URL=https://polyarb-l2.fly.dev`
- /l3/[asset_id] is reachable from `/candidates` for any promoted asset — visual verification of K-line + depth ladder can happen as soon as L3 promoter populates `l2_book_levels` in prod
- l2_ohlc_1m already returns 155 rows pre-promoter (sourced from existing l2_top_of_book) — this means the K-line will render meaningfully BEFORE the L3 promoter is even running in prod (acts as a baseline visual check)

### Known caveats for soak
- `l2_book_levels` table is currently empty in prod (Wave 1-3 code merged, but L3 promoter hasn't run a real `promote_run` against prod yet — Wave 5 soak triggers it). Until then, DepthLadder will render 10 blank rows + "0 rows" header. This is the documented fail-soft state, not a bug.
- KlineChart will only have data for assets that appear in `l2_top_of_book` (currently 3 bootstrap asset_ids — see MEMORY § cold-start). After Wave 5 promoter run, the L3 set should expand to 30+ assets.

### Carry-over for future plans
- None. All Plan 05-05 acceptance criteria met.

---
*Phase: 05-ws-book-prices*
*Plan: 05*
*Completed: 2026-06-01*

## Self-Check: PASSED

- 6/6 files declared in this SUMMARY exist on disk (`page.tsx`, `KlineChart.tsx`, `DepthLadder.tsx`, `l3_promote_dry_run.py`, `ohlc_spot_check.py`, `05-05-SUMMARY.md`)
- 3/3 task commits exist in git log (`03a2489`, `a7be3f2`, `33b58ea`)
- All must_haves.truths verified except the alembic prod truths (covered by orchestrator's Task 1 evidence)
- `dashboard/pnpm typecheck` + `dashboard/pnpm build` both exit 0
- `grep -c "^l3-promote-dry-run:\|^ohlc-spot-check:\|^smoke-l3-dashboard:" Makefile` → 3
- No deletions in any commit (verified via post-commit deletion check pattern)
- Worktree base reset to `4bc68280` at executor start (verified in worktree_branch_check)
