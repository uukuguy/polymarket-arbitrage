# M1 Self-Healing Operator Surfaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make runtime tasks, incidents, recovery actions, and rolling qualification immediately understandable in the control API, Dashboard, and Telegram.

**Architecture:** Extend the existing bounded Postgres control-plane read model and reuse the incident/outbox authority. Next.js validates every new field fail-closed and renders four focused panels. Telegram renders the same transition facts; it does not invent state.

**Tech Stack:** Python 3.12, Starlette, psycopg 3, Next.js 15, React 19, TypeScript 5.7, pytest, pnpm, Make.

## Global Constraints

- Execute after Plans 01-04.
- Reuse `m1_incidents`, `m1_incident_events`, alert outbox, runtime state/events, recovery actions, and qualification stores.
- Any absent/malformed source renders `unavailable`, never empty or healthy.
- UI and Telegram contain bounded non-secret facts only.
- Meet the approved 5/10/35/40/60/65-second visibility SLOs in deterministic tests.
- Use TDD and atomic commits. End with `05.6-205-SUMMARY.md` and clean `make planning-status`.

---

### Task 1: Bounded control API read model

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/http/control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_control_plane_http.py`

**Interfaces:**
- Produces: `runtime_controller`, `active_tasks`, `runtime_incidents`, `recovery_actions`, and `qualification` fields under the existing available response.

- [ ] **Step 1: Write failing read-model tests**

Insert one progressing task, one recovering incident, one completed action, and
one accumulating epoch. Assert the API returns bounded arrays, deadline ages,
progress, qualification duration, last breaker, and no arbitrary error detail.
Corrupt or remove each source and assert HTTP 503 `status=unavailable`.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_http.py -k 'runtime_read_model or qualification_read_model' -q`

Expected: FAIL because the fields do not exist.

- [ ] **Step 3: Extend the bounded snapshot**

Use one read-only transaction with a five-second statement timeout. Limit
active tasks/incidents/actions to the requested `sample_limit`; return counts
when rows exceed the bound. Normalize ages against the API's single `now`.
Required top-level shape:

```python
{
    "runtime_controller": {"status": "healthy", "epoch": 4, "last_tick_at": "..."},
    "active_tasks": {"items": [...], "total": 2},
    "runtime_incidents": {"items": [...], "total": 1},
    "recovery_actions": {"items": [...], "total": 1},
    "qualification": {
        "state": "accumulating", "started_at": "...",
        "eligible_seconds": 1234, "required_seconds": 86400,
        "last_breaker": None, "policy_version": "runtime-v1",
    },
}
```

- [ ] **Step 4: Verify and commit**

Run both files without `-k`; expected PASS.

```bash
git add src/polyarb/control_plane/postgres.py src/polyarb/http/control_plane.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_http.py
git commit -m "feat(05.6-205): expose self-healing control state"
```

### Task 2: Fail-closed TypeScript decoder

**Files:**
- Modify: `dashboard/lib/control-plane.ts`
- Create: `tests/m1-perception/test_control_plane_dashboard_contract.py`

**Interfaces:**
- Produces TypeScript `ActiveTask`, `RuntimeIncident`, `RecoveryAction`, `QualificationView` and a validated `ControlPlaneRead`.

- [ ] **Step 1: Write source-contract and fixture tests**

Assert decoder rejects missing stage, non-numeric deadlines, unknown incident
transition, unbounded failures, malformed action, and qualification without
policy identity. Assert a complete fixture decodes available.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_dashboard_contract.py -q`

Expected: FAIL because the new decoder/types do not exist.

- [ ] **Step 3: Implement strict types and validators**

Use explicit unions:

```typescript
export type RuntimeState = "healthy" | "degraded" | "recovering" | "critical";
export type QualificationState = "accumulating" | "invalidated" | "recovering" | "qualified";
export type RecoveryActionState = "pending" | "running" | "succeeded" | "failed" | "stale-noop";
```

Validate every array item and cap failures/events/actions at the server bound.
Any invalid field returns `{status: "unavailable", reason:
"control-plane-read-unavailable"}`.

- [ ] **Step 4: Verify and commit**

Run the dashboard contract test and `make dashboard-typecheck`; expected PASS.

```bash
git add dashboard/lib/control-plane.ts tests/m1-perception/test_control_plane_dashboard_contract.py
git commit -m "feat(05.6-205): validate self-healing dashboard facts"
```

### Task 3: Four-panel Dashboard

**Files:**
- Create: `dashboard/app/control-plane/RuntimeOverview.tsx`
- Create: `dashboard/app/control-plane/ActiveTasks.tsx`
- Create: `dashboard/app/control-plane/IncidentTimeline.tsx`
- Create: `dashboard/app/control-plane/QualificationPanel.tsx`
- Modify: `dashboard/app/control-plane/page.tsx`
- Modify: `tests/m1-perception/test_control_plane_dashboard_contract.py`

- [ ] **Step 1: Write failing rendering-contract tests**

Assert the page renders runtime state, data-product freshness, each task's
stage/progress/heartbeat/progress/lease deadlines, incident transitions,
action result, qualification progress, last breaker, and certificate link.
Assert unavailable text explicitly says it is not healthy or empty.

- [ ] **Step 2: Prove red**

Run the dashboard contract test; expected FAIL because components do not exist.

- [ ] **Step 3: Implement focused server components**

Each component receives already validated props and performs no fetch. Use
semantic headings and stable incident/action keys. Qualification progress is
computed from server-provided `eligible_seconds/required_seconds`, never from a
client-side inferred start time. Preserve the existing cloud-usage and evidence
sections below the new panels.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_dashboard_contract.py -q`

Run: `make dashboard-typecheck && make dashboard-build`

Expected: PASS.

```bash
git add dashboard/app/control-plane dashboard/lib/control-plane.ts tests/m1-perception/test_control_plane_dashboard_contract.py
git commit -m "feat(05.6-205): render runtime recovery and qualification"
```

### Task 4: Telegram transition rendering and reminders

**Files:**
- Modify: `src/polyarb/control_plane/alert_delivery.py`
- Modify: `tests/m1-perception/test_transactional_alert_delivery.py`
- Modify: `src/polyarb/control_plane/watchdog.py`
- Modify: `tests/m1-perception/test_control_plane_watchdog.py`

- [ ] **Step 1: Write failing message and dedupe tests**

Cover `DETECTED`, `RECOVERY STARTED`, `RECOVERED`, and `ESCALATED`, with
incident ID, component, job/stage, reason, action, qualification impact, and
Dashboard URL. Prove one 15-minute reminder then hourly reminders, and no
duplicate transition delivery.

- [ ] **Step 2: Prove red**

Run both alert/watchdog test files with `-k runtime_transition`; expected FAIL.

- [ ] **Step 3: Implement one renderer**

Add `render_runtime_incident_message(payload: Mapping[str, object]) -> str` in
`alert_delivery.py`. The watchdog writes the same normalized transition payload
to the event writer; it does not build a second incident model. Bound the body
to Telegram's limit and omit arbitrary detail.

- [ ] **Step 4: Verify and commit**

Run full alert-delivery and watchdog test files; expected PASS.

```bash
git add src/polyarb/control_plane/alert_delivery.py src/polyarb/control_plane/watchdog.py tests/m1-perception/test_transactional_alert_delivery.py tests/m1-perception/test_control_plane_watchdog.py
git commit -m "feat(05.6-205): page runtime recovery transitions"
```

### Task 5: Operator smoke target, docs, and closure

**Files:**
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/84-任务自愈与滚动验收.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-205-SUMMARY.md`

- [ ] **Step 1: Add failing smoke/doc contract tests**

Assert `make smoke-control-plane-dashboard` checks authenticated page text for
runtime, incident, recovery, and qualification panels; docs list every new
read-only command and mutation boundary. The teaching file is exactly
`docs/learning/84-任务自愈与滚动验收.md` and the index places it after chapter 83.

- [ ] **Step 2: Implement target and documentation**

The smoke target takes `dashboard_url` and a pre-existing authenticated browser
session exactly as the current dashboard smoke conventions require. It never
turns anonymous HTTP 200 into acceptance.

- [ ] **Step 3: Run gates**

Run Python tests, `make dashboard-typecheck`, `make dashboard-build`,
`make docs-check`, and `make planning-status`. Expected PASS; production browser
smoke remains NOT RUN until an authenticated session is available.

- [ ] **Step 4: Write and commit SUMMARY**

Record commits, UI fields, Telegram transitions, SLA tests, and any external
browser gate. Do not claim production Dashboard acceptance from local build.
