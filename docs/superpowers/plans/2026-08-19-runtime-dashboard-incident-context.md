# Runtime Dashboard Incident Context Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every runtime incident diagnosable from the production Dashboard.

**Architecture:** Extend the read-only Postgres operator projection with the
existing durable incident identity and bounded detail, then render that contract
in the server-rendered dashboard.  The watchdog and writer remain unchanged as
the independent detection and durable-writing boundary.

**Tech Stack:** Python 3.12, psycopg/Postgres, Next.js/TypeScript, pytest,
pnpm typecheck.

## Global Constraints

- Supabase/Postgres remains the sole durable authority.
- No dashboard write path, credential exposure, or new event store.
- Existing unavailable state remains fail-closed.
- Every new field is bounded and derived from durable incident facts.

---

### Task 1: Contract-test diagnostic incident projection

**Files:**
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_control_plane_api.py`

**Interfaces:**
- Produces `runtime_watchdog.current` with `incident_key`, `severity`,
  `summary`, `opened_at`, `source`, and `failures`.
- Produces each `runtime_watchdog.recent_events` entry with `incident_key`,
  `severity`, `summary`, `kind`, `occurred_at`, and `detail`.

- [ ] **Step 1: Write failing Postgres projection assertions** for an open
  external watchdog incident and a detected/recovered lifecycle.  Assert the
  source and failure codes come from the event detail and identity/severity
  come from the incident row.
- [ ] **Step 2: Run the targeted test**

  Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k runtime_watchdog -q`

  Expected: FAIL because the projection currently returns only summary and
  opened_at for an active incident and omits event identity/severity/summary.

- [ ] **Step 3: Extend the API fixture contract** with the new structured
  fields, preserving the exact read-only response shape.
- [ ] **Step 4: Commit after the implementation task passes.**

### Task 2: Project durable diagnostic fields

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Consumes `m1_incidents` and `m1_incident_events.detail`.
- Produces the Task 1 `runtime_watchdog` contract without new tables.

- [ ] **Step 1: Implement the minimal joined queries** selecting incident key,
  severity, summary, opened_at and the most recent event detail for active
  runtime incidents; select the same incident metadata for historical events.
- [ ] **Step 2: Run the Task 1 targeted tests**

  Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k runtime_watchdog -q`

  Expected: PASS.

- [ ] **Step 3: Run API contract tests**

  Run: `uv run pytest tests/m1-perception/test_control_plane_api.py -q`

  Expected: PASS.

### Task 3: Render current state, evidence age, and event diagnostics

**Files:**
- Modify: `dashboard/lib/control-plane.ts`
- Modify: `dashboard/app/control-plane/page.tsx`

**Interfaces:**
- Consumes the Task 1 `ControlPlaneRead` contract.
- Produces a fail-closed dashboard with visible active state and lifecycle
  fields.

- [ ] **Step 1: Extend TypeScript runtime validators** so malformed diagnostic
  payloads yield the existing unavailable state rather than partial UI.
- [ ] **Step 2: Render the active incident** with status, source, severity,
  detection time, incident key, and affected targets/failure codes.
- [ ] **Step 3: Render evidence timestamp with server-calculated age** and a
  ledger whose detected/recovered rows include incident key, source, severity,
  summary, time and failures.
- [ ] **Step 4: Run dashboard typecheck**

  Run: `pnpm --dir dashboard typecheck`

  Expected: PASS.

### Task 4: Publish and prove the production chain

**Files:**
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`

- [ ] **Step 1: Build and deploy the read API and dashboard** using the
  project’s existing Fly and Vercel release commands.
- [ ] **Step 2: Read the public API and deployed Dashboard** to prove the
  structured fields are present.
- [ ] **Step 3: Perform one bounded watchdog transition**, verify matching
  Telegram delivery and Dashboard detected/recovered facts, then return all
  exact Machines to started state.
- [ ] **Step 4: Record the production proof without claiming the ongoing
  24-hour acceptance complete.**
