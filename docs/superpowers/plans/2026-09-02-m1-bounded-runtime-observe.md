# M1 Bounded Runtime Observe Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Restore observe-only monitoring without unbounded Supabase growth.

**Architecture:** Replace the raw per-turn immutable decision log with a lease-fenced bounded projection: one mutable status row, semantic current state, capped transitions, and hourly rollups. The database performs state update and retention atomically; dashboard and watchdog consume this projection.

**Tech Stack:** Python 3.12, psycopg, PostgreSQL, Alembic, pytest, Fly.

## Global Constraints

- Controller remains stopped until schema, capacity, one-turn, and watchdog gates pass.
- At most 100 evaluated targets/turn, 500 current targets/controller, 5,000 24-hour transitions/controller, and 720 hourly rows/controller.
- A truncated scan never infers recovery; it is visible as a degraded condition.
- Direct runtime-role DML is replaced by a lease-fenced database function.
- Every code commit receives a TASK summary and clean `make planning-status`.

### Task 1: Schema-level bound and migration

**Files:** `alembic/versions/042_m1_bounded_runtime_observe.py`, `src/polyarb/control_plane/schema_contract.py`, `tests/alembic/test_042.py`.

- [ ] Write a red integration test that an expired/mismatched controller lease cannot apply a turn, 21 changes create at most 20 detailed transitions plus one overflow, and unchanged semantic state creates no transition.
- [ ] Run `uv run pytest tests/alembic/test_042.py -q`; expect failure because revision 042 is absent.
- [ ] Create `m1_runtime_observe_status`, `m1_runtime_observe_current`, `m1_runtime_observe_transitions`, and `m1_runtime_observe_hourly`; add `m1_runtime_observe_apply_turn(jsonb)` as SECURITY DEFINER. It validates the active lease, applies semantic current changes, prunes before transition insertion, writes the hourly bucket and status last.
- [ ] Backfill only latest state and bounded recent semantic transitions; mark migrated status stale. Drop legacy `m1_runtime_observe_decisions`, mutation trigger/function and indexes only after the new projections are verified. Bump schema revision to 042.
- [ ] Re-run the test; expect pass; commit `feat(m1): bound runtime observe storage`.

### Task 2: Bounded writer and verifier

**Files:** `src/polyarb/control_plane/runtime_observe.py`, `src/polyarb/cli_control_plane.py`, `tests/m1-perception/test_control_plane_runtime_observe.py`, `tests/m1-perception/test_control_plane_cli.py`.

- [ ] Write red tests for repeated unchanged candidates (only liveness changes) and truncated scans (missing target stays current).
- [ ] Run `uv run pytest tests/m1-perception/test_control_plane_runtime_observe.py -q -k 'semantic or truncated'`; expect failure.
- [ ] Replace `insert_runtime_observe_decisions()` in `_runtime_reconcile_turn()` with one `apply_runtime_observe_turn()`. Semantic equality excludes timestamps, raw state digest, and absolute next check; CLI reads `limit + 1` and sends explicit coverage truncation.
- [ ] Replace the raw-500-row verifier with status freshness, continuous duration, max-gap, current-candidate parity, and zero observe-only recovery-action checks.
- [ ] Run focused suites; expect pass; commit `feat(m1): project bounded runtime observe turns`.

### Task 3: Operations contract and alert evidence

**Files:** `src/polyarb/control_plane/postgres.py`, `src/polyarb/control_plane/watchdog.py`, `dashboard/lib/control-plane.ts`, `dashboard/app/control-plane/RuntimeOverview.tsx`, corresponding tests.

- [ ] Write red snapshot tests for freshness, continuity, candidate counts, caps, coverage/storage flags, bounded current sample and transitions; write watchdog tests for stale, gap, truncation, storage cap and sustained suppression.
- [ ] Implement `runtime_observe` inside `operational_snapshot()` and typed dashboard decode/render. Missing rows are unavailable/stale, never healthy.
- [ ] Run focused backend/dashboard contracts; expect pass; commit `feat(m1): expose bounded runtime observe health`.

### Task 4: Controlled production rollout

**Files:** `Makefile`, `docs/learning/00-INDEX.md`, new learning doc, TASK summary.

- [ ] Add a read-only Makefile target for bounded runtime-observe status and contract test.
- [ ] Apply revision 042 while controller remains stopped; verify archive/backfill evidence, legacy relation removal, public health, and capacity decrease.
- [ ] Deploy controller image; run one controlled turn; verify bounded writes and no coverage/storage flags; then run for 30 minutes before restoring normal service.
- [ ] Update daily business-intelligence guide; commit docs and run `make planning-status`.

## Self-review

The plan addresses write amplification at its source, retains only operationally meaningful state and transitions, guarantees finite storage in the write transaction, and separately exposes the evidence needed for dashboard and alerting. Business snapshot/funnel work remains a distinct next plan.
