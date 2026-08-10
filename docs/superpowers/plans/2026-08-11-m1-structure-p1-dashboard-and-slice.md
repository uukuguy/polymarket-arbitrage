# M1 Structure P1 Dashboard and Slice Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline test-driven execution. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Structure producer P1 actionable on the Vercel Dashboard and ensure a contended publication writer exits before the bounded child ceiling.

**Architecture:** Persist typed Structure budget evidence, validate it in the public HTTP envelope, and render a dedicated Dashboard panel. Thread a deadline-derived SQLite writer timeout into publication chunks; retain the 75-second parent ceiling.

**Tech Stack:** Python 3.12, SQLite, Starlette read models, Next.js/TypeScript, pytest.

## Global Constraints

- Structure child hard limit stays 75 seconds and Quote priority is unchanged.
- Dashboard unavailable means unavailable, never zero incidents.
- Production evidence is redacted and bounded.

---

### Task 1: Typed Structure P1 Dashboard contract

**Files:** `tests/m1-perception/test_dashboard_perception_contract.py`, `dashboard/lib/types.ts`, `dashboard/lib/perception.ts`, `dashboard/app/perception/page.tsx`.

- [ ] Write a failing assertion for `p1StructureIncidents`, `P1 Structure publication incident`, `cooperative checkpoint target`, and `market-map-stale` reader validation.
- [ ] Run the single test and verify it fails.
- [ ] Add typed Structure diagnosis fields and a dedicated P1 panel.
- [ ] Run all Dashboard contract tests.

### Task 2: Bounded Structure evidence

**Files:** `tests/m1-perception/test_perception_http.py`, `src/polyarb/daemon/structure_incidents.py`, `src/polyarb/daemon/scheduler.py`, `src/polyarb/http/perception.py`.

- [ ] Write a failing API expectation for `cooperative_slice_budget_s=45` and `child_hard_limit_s=75`.
- [ ] Thread the constants from Scheduler through durable Structure evidence and strict HTTP validation.
- [ ] Run focused perception HTTP tests.

### Task 3: Bounded publication writer

**Files:** `tests/m1-perception/test_structure_generation_publication.py`, `src/polyarb/perception/structure_publication.py`, `src/polyarb/storage/sqlite_store.py`.

- [ ] Write a locked-writer red test with an explicit 0.01-second deadline.
- [ ] Add the optional validated writer timeout to the publication append operation and thread it from the remaining slice time.
- [ ] Run focused publication and scheduler tests.

### Task 4: Documentation and production proof

**Files:** `docs/M1-市场感知平台使用手册.md`, `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-39-SUMMARY.md`.

- [ ] Document the Structure P1 budget reading procedure.
- [ ] Deploy after focused checks; record fresh pointer, certified Quote, no P1/P2, Dashboard and Polywatch evidence.
