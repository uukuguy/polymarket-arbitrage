---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: in_progress
stopped_at: final cloud acceptance is collecting immutable evidence; do not change the topology or declare completion before its 24-hour verifier passes
last_updated: "2026-08-19T01:41:00+08:00"
progress:
  total_phases: 14
  completed_phases: 13
  total_plans: 72
  completed_plans: 72
  percent: 99
---

# M1 Perception — Current State

## Current Position

- **Sole authority:** Supabase project `polyarb` (`lgykffpcsebewvobkbdm`),
  Alembic migrations through `017`, and R2 bucket `polyarb-control-plane`.
  The runtime's durable job, receipt, lease, pointer, evidence, and incident
  facts live there.

- **Transactional collection:** `polyarb-control-worker-m1` has three fixed,
  started 1GB roles: coordinator `e82d1220b2d138`, Structure range worker
  `683e46ea500dd8`, and Quote batch worker `4d895231f66748`. Fenced Postgres
  leases and idempotent receipts make process replacement safe; Structure and
  Quote are not competing for a local SQLite writer.

- **Read and observability plane:** `polyarb-control-api` is a read-only
  control-plane API. `polyarb-control-alert` is a 256MB database-independent
  watchdog, now on image `m1-watchdog-writer-8458893d`; every 30 seconds it
  checks the API, the three worker identities, evidence sampler
  `830152f7274378`, and isolated dashboard-ledger writer `28654e35a73d08`.
  Failure/recovery transitions page Telegram and are recorded in the dashboard
  ledger. The production dashboard exposes current evidence and that ledger at
  `/control-plane`.

- **Credential/runtime invariant:** Fly detached Machines retain duplicate
  same-name environment values on update. Runtime commands therefore map only
  dedicated versioned variables; all database runtime roles use the Supabase
  IPv4 Session Pooler, not the unreachable direct IPv6 database endpoint.

## Formal Acceptance

- **Only qualifying run:** `m1-formal-20260818T1733Z`.
- **Baseline:** `2026-08-18T17:33:25.500033Z`; first independent sample:
  `2026-08-18T17:33:51.952568Z`.
- **Current early evidence:** three immutable samples cover 327 seconds,
  successful jobs advanced `1916 → 1957`, all three worker identities stayed
  `started`, and expired leases/open circuits stayed at zero.
- **Hard completion gate:** `make control-plane-cloud-soak-verify
  run_id=m1-formal-20260818T1733Z` must pass its default 86,400-second,
  900-second-gap fail-closed policy. Earlier runs are immutable audit history
  only and are not qualifying evidence.

## Completion Audit

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Transactional authority and fenced jobs | migrations, API, workers, lease/receipt contracts | complete |
| Structure/Quote cloud worker migration | three independent fixed-role workers and advancing durable successes | complete |
| Process-loss recovery | fenced R2-before-receipt takeover evidence for both job classes | complete |
| Immediate fault visibility | independent watchdog, Telegram, Dashboard incident/recovery ledger | complete |
| Continuous final-topology acceptance | cloud evidence verifier | in progress: full 24 hours required |

## Resume

1. Use the read-only cloud verifier and exact watchdog gate; do not restart or
   reconfigure any acceptance target while the run is active.
2. If an alert opens, diagnose from API, Machine state, and dashboard ledger;
   repair the smallest failing boundary, then start a fresh uniquely named
   formal run rather than relabeling prior evidence.
3. After the 24-hour verifier passes, perform the final topology/document audit
   and only then mark Phase 05.6 and the M1 goal complete.
