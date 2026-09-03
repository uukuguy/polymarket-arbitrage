# Opportunity-First Business Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven development task by task.

**Goal:** Make certified, current-lineage combination opportunities the primary M1 business view and add a bounded non-certified candidate funnel.

**Architecture:** Reuse the fenced opportunity projection as the only Certified authority. Add a bounded current Quote/parent-Structure candidate projection with group-level bundle math and rejection facts. Keep Quote Coverage as evidence/audit.

## Task 1: Candidate projection contract and storage

**Files:** `alembic/versions/047_m1_analysis_candidates.py`, `src/polyarb/control_plane/analysis_candidates.py`, `tests/m1-perception/test_analysis_candidates.py`.

- [ ] Write failing tests for group bundle cost, gross edge, minimum depth, expired/closed exclusion, incomplete expected-member coverage, and no-edge classification.
- [ ] Implement pure candidate builder. Inputs are exact parent Structure group/event payloads plus current Quote rows. It emits the five closed candidate states from the approved design and never emits an opportunity claim.
- [ ] Add bounded projection tables keyed by Quote generation/group ID: payload max 4KB, generation pointer/integrity metadata, read-only runtime grants.
- [ ] Run focused tests and migration contract tests; commit `feat(m1): add bounded analysis candidates`.

## Task 2: Fenced publication and read API

**Files:** `src/polyarb/control_plane/postgres.py`, `src/polyarb/control_plane/api.py`, `tests/m1-perception/test_control_plane_postgres.py`, `tests/m1-perception/test_control_plane_api.py`.

- [ ] Write failing tests proving candidate reader uses only current Quote and exact parent Structure, has deterministic positive-edge ordering, exposes rejection counts, and fails closed for incomplete projection.
- [ ] Add a certifier-owned candidate materialization/promotion step bound to current Quote generation; no coordinator restart, opportunity pointer mutation, or qualification change.
- [ ] Add `GET /perception/business/analysis/candidates` and summary reader with opaque pagination; reject stale cursor/generation input.
- [ ] Run Postgres/API tests; commit `feat(m1): publish analysis candidate funnel`.

## Task 3: Opportunity-first Dashboard

**Files:** `dashboard/lib/business-research.ts`, `dashboard/app/business/opportunities/page.tsx`, `dashboard/app/business/analysis/page.tsx`, `dashboard/app/business/quotes/page.tsx`, `tests/m1-perception/test_business_dashboard_contract.py`.

- [ ] Write failing contracts that require: Certified table only when projection is current; lagging projection cannot render as current; candidates show bundle cost/gross edge/min size/state; Quote page calls itself evidence/audit.
- [ ] Implement typed readers and three pages. Opportunities defaults to current certified rows sorted by executable gross-edge value. Analysis shows candidate/rejection funnel. Quotes removes discovery-lead priority and supports group evidence context.
- [ ] Run Dashboard contract tests, `make dashboard-typecheck dashboard-build`; commit `feat(m1): prioritize certified business opportunities`.

## Task 4: Rollout and evidence

**Files:** `docs/M1-市场感知平台使用手册.md`, `docs/superpowers/plans/2026-09-04-opportunity-first-business-discovery-SUMMARY.md`.

- [ ] Apply migration through the existing controlled migration path; deploy API and Dashboard without restarting the coordinator.
- [ ] Verify API lineage/current/lagging cases, then authenticated desktop and narrow-width Dashboard rendering in the existing browser session.
- [ ] Update operator guide with Certified vs candidate vs quote-evidence interpretation and record exact evidence; commit/push.

## Acceptance

- No historical projection can appear as a current certified result.
- Positive candidate requires all executable, valid, non-expired required group legs.
- Zero candidates/opportunities is explicit and never inferred from unavailable data.
- No deployment step restarts M1 collection or changes qualification.
