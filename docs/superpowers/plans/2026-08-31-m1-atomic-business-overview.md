# M1 Atomic Business Overview Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish a lineage-consistent, versioned M1 business snapshot that the CLI and Dashboard can read without composing independent authorities.

**Architecture:** `PostgresControlPlane.business_overview()` owns one bounded `REPEATABLE READ`, read-only transaction and returns `BusinessOverviewV1`. The HTTP API transports it unchanged; Python/TypeScript decoders fail closed. Stage 1 supplies only facts already available from durable tables and marks unsupported analysis fields as `not-published`.

**Tech Stack:** Python 3.12, psycopg/PostgreSQL, Starlette, pytest, Next.js/TypeScript.

## Global Constraints

- Observe-only: no schema migration, scheduler, recovery, deployment, wallet, order, trade, or write action.
- API output has `schema_version="m1.business-overview.v1"`; unavailable is never a zero count.
- Snapshot reads use a bounded read-only transaction and do not consume the existing operator/readiness pool.
- The existing `/perception/control-plane` and `/perception/opportunities` contracts remain unchanged in Stage 1.

### Task 1: Define and prove the atomic Python projection

**Files:** `src/polyarb/control_plane/postgres.py`, `tests/m1-perception/test_control_plane_postgres.py`.

- [ ] Write failing fixtures that publish a Structure, its child Quote and an Opportunity pointer; assert `business_overview()` returns exactly one schema version and shared `observed_at`, reports component counts (not `record_count` as markets), Quote parent lineage, real zero opportunities, and `analysis.status="not-published"`.
- [ ] Add a pointer-switch test which changes a pointer from a second connection during the overview read; assert the result contains only one generation lineage, never a mixed Structure/Quote/Opportunity triple.
- [ ] Implement `PostgresControlPlane.business_overview()` using one `REPEATABLE READ READ ONLY` connection, bounded queries, server timestamp, published pointers and qualification facts. Map absent pointers to `not-published`, freshness/lineage disagreement to `lagging`, and read failure to the caller exception boundary.
- [ ] Run `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k business_overview -q` and commit `feat(m1): add atomic business overview projection`.

### Task 2: Expose a fail-closed HTTP contract

**Files:** `src/polyarb/control_plane/api.py`, `tests/m1-perception/test_control_plane_api.py`, `tests/m1-perception/test_control_plane_http.py`.

- [ ] Write failing route tests for `/perception/business-overview`: available output passes through unchanged; malformed/missing authority returns only `{"status":"unavailable","reason":"business-overview-unavailable"}` with 503.
- [ ] Add the bounded blocking bridge call using a dedicated business-read deadline/pool and route it without changing other endpoints.
- [ ] Run focused API/HTTP tests and commit `feat(m1): expose business overview API`.

### Task 3: Make the CLI consume the same authority

**Files:** `src/polyarb/cli_control_plane.py`, `src/polyarb/control_plane/business_brief.py`, `tests/m1-perception/test_business_brief.py`, `Makefile`.

- [ ] Write failing tests asserting `business-brief` makes one business-overview read rather than `operational_snapshot` plus opportunity HTTP, preserves schema version in JSON, and prints `业务数据不可用` on authority failure.
- [ ] Replace the two-authority composition with the overview response; keep human output derived solely from it. Preserve `make control-plane-business-brief` as the entrypoint.
- [ ] Run focused CLI/Make tests and commit `refactor(m1): read business brief from atomic overview`.

### Task 4: Add strict Dashboard client contract and business landing state

**Files:** `dashboard/lib/business-overview.ts`, `dashboard/app/business/page.tsx`, `dashboard/app/page.tsx`, `dashboard/lib/control-plane.ts`, `tests/m1-perception/test_control_plane_dashboard_contract.py`.

- [ ] Write failing TypeScript fixture-contract tests for every status branch: available-zero, paused, stale/lagging, not-published and unavailable; reject missing generation/timestamps and invalid counts.
- [ ] Implement strict decoder and server-side fetch. Render the trust bar and Stage-1 product cards with status, generation, as-of, explicit unavailable/not-provided copy and links to Runtime; root redirects to `/business`.
- [ ] Run dashboard typecheck plus contract tests and commit `feat(m1): add business overview dashboard`.

### Task 5: Record evidence and prepare Stage 2

**Files:** `docs/learning/106-M1日常业务情报操作指南.md`, `docs/learning/00-INDEX.md`, `.planning/workstreams/m1-perception/STATE.md`, `.planning/JOURNAL.md`, `docs/superpowers/plans/2026-08-31-m1-atomic-business-overview-TASK-*-SUMMARY.md`.

- [ ] Document that the current CLI and Dashboard are the same snapshot and distinguish zero, paused, stale, not-published and unavailable. State that funnel/rejection metrics await durable projection support.
- [ ] Run `make planning-status`, focused Python tests, Dashboard typecheck, `make docs-m1-check`, and `git diff --check`; record actual outputs in per-task summaries.
- [ ] Commit the documentation/state closure after each code task has its summary.

## Self-review

Task 1 creates the authoritative fact boundary; Task 2 transports it; Task 3 removes the current mixed read; Task 4 renders only validated facts; Task 5 documents the business semantics. No task creates a write path or pretends unavailable data is zero.
