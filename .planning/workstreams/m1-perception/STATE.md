---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: in_progress
stopped_at: Plans 201-203 are locally complete; next is Plan 204 rolling qualification, with no manual 24-hour restart before automatic epochs exist
last_updated: "2026-08-25T10:15:00+08:00"
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
  Alembic migrations through `021`, and R2 bucket `polyarb-control-plane`.
  The runtime's durable job, receipt, lease, pointer, evidence, and incident
  facts live there.

- **Transactional collection:** `polyarb-control-worker-m1` has three fixed,
  started 1GB roles on the current layered runtime image: coordinator `e82d1220b2d138`, Structure range worker
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

- **Independent monitor-of-monitor:** Cloudflare Worker
  `polyarb-control-watchdog-supervisor` version `e4c02c17-46cd-46e5-bee0-f6a3f985d557`
  runs UTC `* * * * *` with an isolated KV transition cache and only Fly-read,
  Telegram and writer credentials. Its public route is disabled. A real
  scheduled healthy observation was stored at `2026-08-18T18:07:14Z`; the
  controlled alert-Machine stop/start proof recorded source-specific Dashboard
  events `detected` at `18:16:14Z` and `recovered` at `18:18:14Z`. Production
  Dashboard entries visibly show `Observed by: <source>`, timestamp and bounded
  failure list (Vercel deployment `dpl_28qrQKP5NHPG9qzqh9UdECBL7iJS`).

- **Credential/runtime invariant:** Fly detached Machines retain duplicate
  same-name environment values on update. Runtime commands therefore map only
  dedicated versioned variables; all database runtime roles use the Supabase
  IPv4 Session Pooler, not the unreachable direct IPv6 database endpoint.
  The evidence sampler and Cloudflare supervisor each have separate short-lived
  Fly read-only credentials; neither shares a worker credential.

## Local Event-Driven Recovery Closure (2026-08-25)

- Plans `05.6-201` and `05.6-202` now make all eight transactional task
  lifecycles explicit: start, closed stage/progress chain, heartbeat and one
  atomic terminal/retry fact. Task-local results are the primary detector;
  watchdogs remain the missing-fact and infrastructure backstop.
- Plan `05.6-203` is locally complete through additive migrations 022/023,
  pure deadline decisions, controller/action/budget fencing, a five-action
  job-level executor, read-only status, guarded one-shot control and a
  fail-loud sequential service. Process and Machine actions remain disabled.
- Climb H-014 cycle 15 and H-015 cycle 16 both scored 100/100 on dedicated
  local gates. H-015 run `20260825-021111-h-015` covers real Testcontainers
  migration/fencing/race/DB-clock rollback plus CLI fail-loud behavior.
- These changes have **not** been deployed and migrations 022/023 have not
  been applied to production. The production topology still reflects the
  earlier runtime and must not be described as self-healing until Plans
  204-206, deployment verification and a new automatic qualification epoch
  complete.

## Formal Acceptance

- **Active qualifying run:** none. `m1-formal-20260823T1335Z` is immutable
  historical evidence; replay found the first breaking `lease.expired` fact at
  `2026-08-23T16:22:21Z`, so its earlier 338-second liveness pass cannot become
  final acceptance.
- Repeatedly starting another manual 24-hour run is no longer an accepted
  detection/recovery mechanism. Plan `05.6-204` must first provide automatic
  rolling epochs and immutable certificates; Plans 205-206 then add operator
  surfaces and least-privilege production enablement.
- A future production epoch may qualify only after the new runtime facts,
  reconciler, recovery service, alerts and Dashboard are deployed and the
  certificate independently verifies exact 86,400-second coverage.

## Completion Audit

| Requirement | Current evidence | Status |
| --- | --- | --- |
| Transactional authority and fenced jobs | migrations, API, workers, lease/receipt contracts | complete |
| Structure/Quote cloud worker migration | three independent fixed-role workers and advancing durable successes | complete |
| Process-loss recovery | fenced R2-before-receipt takeover evidence for both job classes | complete |
| Immediate fault visibility | Fly watchdog plus independent Cloudflare supervisor, Telegram and source-aware Dashboard incident/recovery ledger | complete |
| Continuous final-topology acceptance | rolling epoch + immutable certificate | blocked on Plans 204-206 and fresh production enablement |

## Resume

1. Execute Plan `05.6-204` Task 1: pure rolling qualification policy and
   virtual-time state machine. Keep old soak rows and failed runs immutable.
2. Complete Plans 204-205 locally, then use Plan 206 for migration/deployment
   authority. Do not run local implementation commands against production.
3. After production enablement, let recovery confirmation open a fresh epoch
   automatically; independently verify its immutable certificate before
   marking Phase 05.6 and M1 complete.
