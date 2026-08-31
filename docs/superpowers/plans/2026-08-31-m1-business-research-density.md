# M1 Business Research Density Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the M1 Dashboard a dense, truthful research workbench where the published Structure and Quote universe can be scanned, filtered and drilled into without treating a legacy list or final opportunity count as the whole business.

**Architecture:** Extend the versioned control-plane business authority with bounded, generation-bound read-only pages for Structure and Quote records. The Dashboard uses those pages only through strict decoders and renders desktop-density tables with responsive overflow and compact mobile cards. The existing `BusinessOverviewV1` remains the trust/header authority; no browser joins or legacy Supabase tables are used.

**Tech Stack:** Python 3.12, psycopg/PostgreSQL, Starlette, Next.js 15, TypeScript, pytest, Playwright CLI.

## Global Constraints

- Every query is read-only, bounded and bound to an explicit current generation; a superseded requested generation returns a stable explanatory result, never mixed rows.
- Detail readers use a dedicated short deadline and never share the control-plane runtime reader pool.
- UI displays `available`, `lagging`, `not-published`, and `unavailable` distinctly; zero is valid only for available, counted products.
- Desktop tables keep a sticky header, readable column priority, monospace identity fields, sortable server order and a horizontal overflow wrapper; 375px uses row cards, not a squeezed table.
- No fake funnel, trend, coverage percentage, P&L, trade, recovery, or legacy Supabase authority.

### Task 1: Specify generation-bound research page contracts

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/api.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`
- Test: `tests/m1-perception/test_control_plane_api.py`

- [ ] Write failing fixtures for a certified Structure/Quote lineage and assert `business_structure_page(generation_key, limit, after)` and `business_quote_page(generation_key, limit, after)` return schema version, generation, parent generation, ordered rows, `next_after`, total count and one shared server time.
- [ ] Add invalid/missing cursor, limit overflow, absent current pointer, stale requested generation and successor-pointer tests; each must fail closed without returning a partial or mixed generation page.
- [ ] Implement bounded `REPEATABLE READ READ ONLY` Postgres readers with canonical keyset cursors. Structure rows expose entity identity/type and selected component metadata; Quote rows expose market/token identity, executable state, ask/size and quoted-at only where persisted by the immutable generation.
- [ ] Add `GET /perception/business/structure` and `GET /perception/business/quotes` transports with explicit generation/limit/after query parsing and a stable unavailable envelope.
- [ ] Run focused Postgres/API tests and commit `feat(m1): publish generation-bound research pages`.

### Task 2: Add strict Dashboard readers and dense table primitives

**Files:**
- Create: `dashboard/lib/business-pages.ts`
- Create: `dashboard/app/business/research-table.tsx`
- Modify: `dashboard/lib/business-overview.ts`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`

- [ ] Write failing source/decoder contract tests for malformed page schema, unsafe count, bad cursor, unknown product state, duplicate/non-ordered row identity and mismatched generation lineage.
- [ ] Implement strict `readBusinessStructurePage` and `readBusinessQuotePage`; neither may call `/perception`, Supabase, or compose separate BusinessOverview requests.
- [ ] Implement the table primitive: desktop sticky header, fixed cell padding, column width policy, overflow container, truncation plus full-value title, status pills, and narrow-screen card rows. The component receives prevalidated columns/rows and owns no domain fetching.
- [ ] Run dashboard contract tests plus `make dashboard-typecheck` and commit `feat(m1): add dense research table primitives`.

### Task 3: Rebuild Structure and Quote research pages around dense facts

**Files:**
- Modify: `dashboard/app/business/page.tsx`
- Modify: `dashboard/app/business/structure/page.tsx`
- Modify: `dashboard/app/business/quotes/page.tsx`
- Modify: `dashboard/app/business/business-ui.tsx`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`

- [ ] Write failing page contracts that reject the Structure “— records” placeholder, require component coverage in the overview, expose active generation/as-of/parent lineage above every detail table, and preserve unavailable/not-published copy.
- [ ] Make `/business` a dense command view: trust state, current Structure components, Quote coverage, final opportunity count, known analysis boundary and blockers in a compact metric grid; all secondary detail lives behind research links.
- [ ] Make `/business/structure` and `/business/quotes` render first bounded pages, table total, next-page URL, lineage context and product-specific empty/non-current states. Keep identifiers copyable and details scannable.
- [ ] Run contract tests, `make dashboard-build`, and commit `feat(m1): render dense M1 research pages`.

### Task 4: Playwright visual and production verification

**Files:**
- Create: `docs/superpowers/plans/2026-08-31-m1-business-research-density-TASK-4-SUMMARY.md`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `docs/learning/107-原子业务研究视图.md`

- [ ] Start the local Dashboard against deterministic business-page fixtures, then use `playwright-cli` to capture `/business`, `/business/structure` and `/business/quotes` at 1440px and 375px in `output/playwright/business-density/`.
- [ ] Verify screenshots: no squeezed table columns, no misleading zero/placeholder, visible status/as-of/generation, readable long identities, and no browser console error.
- [ ] Run focused Python tests, dashboard typecheck/build, docs contract and `git diff --check`; record artifact paths and production limitations in the task summary.
- [ ] Deploy only after all local gates pass, then verify Vercel deployment Ready and distinguish protected-route reachability from authenticated content verification.
- [ ] Commit `docs(m1): document dense business research operation`.

## Self-review

The plan gives M1 data density a durable fact source before adding visual density. It deliberately does not use the legacy `/perception` reader, does not infer an analysis funnel, and keeps Runtime operational evidence separate from business research.
