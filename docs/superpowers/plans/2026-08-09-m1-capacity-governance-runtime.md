# M1 Capacity Governance Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline TDD task-by-task. Steps use checkbox syntax for tracking.

**Goal:** make M1 automatically work through SQLite-volume pressure while preserving fresh certified Quote production and durable operator diagnosis.

**Architecture:** extend the existing authenticated resource-decision ledger into a Quote-aware capacity runtime. A resident worker measures capacity, transitions through hysteretic watermarks, takes only bounded safe-reclaim actions, and records an incident/recovery chain visible through health and Dashboard. Manifest-backed cold archive and rolling compaction are separate plans after this runtime proves safe.

**Tech Stack:** Python 3.12, asyncio, SQLite WAL, Pydantic, Starlette, existing IncidentManager, pytest, Ruff, TypeScript/Next.

## Global Constraints

- Quote active/due wins before and after the shared writer lock.
- No online VACUUM, automatic Fly resize, destructive filesystem deletion, or M2 gate weakening.
- Reclaim only payload protected by existing proof skeleton and retention predicates.
- Every failure persists runtime and incident truth, then has health and Dashboard readers.
- New executable maintenance entry points appear in Makefile and make help.

## File structure

- Create src/polyarb/perception/capacity_controller.py for watermark policy, runtime, and one-step Quote-aware action.
- Modify src/polyarb/perception/store.py and src/polyarb/storage/schemas.py for singleton runtime plus append-only receipts.
- Modify src/polyarb/daemon/main.py and src/polyarb/config.py for resident lifecycle and settings.
- Modify src/polyarb/http/health.py, src/polyarb/http/perception.py, and src/polyarb/perception/resource_incidents.py for chain truth.
- Modify dashboard/lib/types.ts, dashboard/lib/perception.ts, and dashboard/app/perception/page.tsx for diagnostics.
- Create tests/perception/test_capacity_controller.py; modify daemon, health, HTTP, and Dashboard contract tests.

## Task 1: Durable state and watermarks

**Files:** create src/polyarb/perception/capacity_controller.py and tests/perception/test_capacity_controller.py; modify src/polyarb/perception/store.py and src/polyarb/storage/schemas.py.

**Interfaces:** CapacityState is normal, pressure, critical, or exhaustion-imminent. CapacityRuntime contains free bytes/percent, transition/measurement/success timestamps, failures, next retry, action, and error kind. CapacityPolicy.transition(previous, free_percent, now_ms) returns CapacityState.

- [ ] Write failing policy tests proving 20%, 12%, and 6% entry thresholds and that a degraded state does not exit until a 30-second high-watermark hold.
- [ ] Run: uv run pytest tests/perception/test_capacity_controller.py -q. Expected: FAIL because CapacityPolicy is absent.
- [ ] Add singleton capacity_controller_runtime and append-only capacity_reclaim_receipts tables; validate legal states and finite/non-negative measurements; implement hysteresis.
- [ ] Run: uv run pytest tests/perception/test_capacity_controller.py -q && uv run ruff check src/polyarb/perception/capacity_controller.py src/polyarb/perception/store.py src/polyarb/storage/schemas.py tests/perception/test_capacity_controller.py. Expected: PASS.
- [ ] Commit: git add src/polyarb/perception/capacity_controller.py src/polyarb/perception/store.py src/polyarb/storage/schemas.py tests/perception/test_capacity_controller.py && git commit -m "feat(m1): persist capacity watermarks".

## Task 2: Quote-aware bounded reclaim and retry

**Files:** modify src/polyarb/perception/capacity_controller.py, src/polyarb/perception/store.py, src/polyarb/storage/sqlite_store.py, tests/perception/test_capacity_controller.py.

**Interfaces:** CapacityController.run_once(quote_priority) returns CapacityRuntime. CapacityReclaimReceipt records action, deleted count, IDs, and completion timestamp.

- [ ] Write failing tests: a true quote_priority gives action quote-priority with no mutation; pressure calls bounded purge_old_snapshots; an OperationalError becomes durable backoff with a next retry.
- [ ] Run: uv run pytest tests/perception/test_capacity_controller.py -q. Expected: FAIL because run_once is absent.
- [ ] Implement one action only: recheck Quote priority immediately before the existing bounded purge_old_snapshots writer operation. Persist receipt after success; persist capped retry for SQLite/I/O failure. Never unlink files or call VACUUM.
- [ ] Run: uv run pytest tests/perception/test_capacity_controller.py tests/perception/test_resource_controller.py -q. Expected: PASS.
- [ ] Commit: git add src/polyarb/perception/capacity_controller.py src/polyarb/perception/store.py src/polyarb/storage/sqlite_store.py tests/perception/test_capacity_controller.py && git commit -m "feat(m1): reclaim bounded history under capacity pressure".

## Task 3: Resident lifecycle without a supervisor prerequisite

**Files:** modify src/polyarb/config.py, src/polyarb/daemon/main.py, tests/daemon/test_main_fault_wiring.py, Makefile.

**Interfaces:** add capacity_controller_enabled, capacity_pressure_free_percent, capacity_critical_free_percent, capacity_exhaustion_free_percent, capacity_recovery_hold_s, capacity_interval_s, and capacity_max_snapshots_per_run. Add make perception-capacity-status.

- [ ] Write a failing lifecycle test proving controller start and clean cancellation when capacity_controller_enabled is true but opportunity_producer_supervisor_enabled is false.
- [ ] Run: uv run pytest tests/daemon/test_main_fault_wiring.py -q. Expected: FAIL because no capacity worker is wired.
- [ ] Construct the worker with the shared Quote priority predicate/producer lock, run one bounded action then wait capacity_interval_s, and include it in daemon cancellation/five-second gather. Remove the legacy supervisor coupling.
- [ ] Run: uv run pytest tests/daemon/test_main_fault_wiring.py -q && make help | rg perception-capacity-status. Expected: PASS.
- [ ] Commit: git add src/polyarb/config.py src/polyarb/daemon/main.py tests/daemon/test_main_fault_wiring.py Makefile && git commit -m "feat(m1): run capacity governance as a resident worker".

## Task 4: Health, incident, Dashboard, and learning chain

**Files:** modify src/polyarb/http/health.py, src/polyarb/http/perception.py, src/polyarb/perception/resource_incidents.py, dashboard/lib/types.ts, dashboard/lib/perception.ts, dashboard/app/perception/page.tsx; modify health, HTTP, Dashboard tests; update docs/M1-市场感知平台使用手册.md and docs/learning/00-INDEX.md; create docs/learning/07-m1-capacity-governance.md.

- [ ] Write failing tests proving critical state yields strict-health fail and a capacity diagnosis with next action; Dashboard source renders Capacity controller and Next action.
- [ ] Run: uv run pytest tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_perception_http.py tests/m1-perception/test_dashboard_perception_contract.py -q. Expected: FAIL because runtime is not exposed.
- [ ] Implement pressure as deduplicated warning; critical/exhaustion as strict-health fail/escalation; verified recovery only after a reclaim receipt and hysteresis. Render watermark, action, error, next retry, and recovery receipt. Document that extension is emergency headroom, not a normal prerequisite.
- [ ] Run: uv run pytest tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_perception_http.py tests/m1-perception/test_dashboard_perception_contract.py tests/perception/test_resource_incidents.py -q && uv run ruff check src/polyarb dashboard tests/perception/test_capacity_controller.py. Expected: PASS.
- [ ] Commit the code/tests/docs with message feat(m1): expose capacity recovery chain.

## Task 5: Verification and rollout

- [ ] Run focused capacity/daemon/health/HTTP/Dashboard pytest suites, Ruff for src and tests, git diff --check, and make planning-status. Expected: all pass and no drift.
- [ ] Deploy only after exact HEAD SHA verification using flyctl deploy with POLYARB_RELEASE_ID set to that SHA. Verify release ID and active capacity controller from health; do not request a copied approval token.
- [ ] Record live evidence in JOURNAL and the architecture thread. Create a separate cold-archive plan only if receipts show SQLite reuse cannot return above the high watermark; add rolling compaction only after that evidence.

## Self-review

- The plan covers durable watermarks, safe bounded recovery, Quote priority, restart/retry, health, alerts, Dashboard, Makefile, teaching, and production evidence.
- Cold archive and compaction are intentionally independent future plans so they cannot block immediate recovery.
