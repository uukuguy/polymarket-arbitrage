---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: in_progress
stopped_at: Plans 201-205 are locally complete and H-017 is confirmed; next is Plan 206 deterministic fault qualification and least-privilege production enablement
last_updated: "2026-08-25T15:44:29+08:00"
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
- Plan `05.6-204` is locally complete through additive migration 024, a
  monotonic source-ingress ledger, automatic rolling epochs, recovery-period
  observations, immutable reproducible certificates, read-only status/history
  and a guarded sequential qualification service.
- Plan `05.6-205` is locally complete through the bounded fail-closed API read
  model, strict Dashboard decoder, four operator panels, normalized durable
  Telegram/outbox transitions, restart-safe reminders, authenticated-body
  smoke and teaching chapter 87. Stale/equal-time observations cannot move the
  incident ledger backward.
- Climb H-014 cycle 15 through H-017 cycle 18 scored 100/100 on dedicated local
  gates. H-016 run `20260825-053507-h-016` covers real
  migration/trust/late-ingress/freshness/recovery-observation behavior plus
  exact 26-hour restart/replay certificate evidence; H-017 run
  `20260825-074318-h-017` covers strict reads, real-PostgreSQL event/outbox
  chains, API/smoke contracts and restart/ordering behavior.
- These changes have **not** been deployed and migrations 022/023/024 have not
  been applied to production. The production topology still reflects the
  earlier runtime and must not be described as self-healing until Plans
  206, deployment verification and a new automatic qualification epoch
  complete.

## Formal Acceptance

- **Active qualifying run:** none. `m1-formal-20260823T1335Z` is immutable
  historical evidence; replay found the first breaking `lease.expired` fact at
  `2026-08-23T16:22:21Z`, so its earlier 338-second liveness pass cannot become
  final acceptance.
- Repeatedly starting another manual 24-hour run is no longer an accepted
  detection/recovery mechanism. Plan `05.6-204` now provides the local automatic
  epoch/certificate mechanism and Plan `05.6-205` now provides shared operator
  truth. Plan 206 must prove least-privilege production enablement before a
  production epoch can start.
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
| Continuous final-topology acceptance | rolling epoch + immutable certificate | local mechanism and operator surfaces complete; blocked on Plan 206 and fresh production enablement |

## Resume

1. Execute Plan `05.6-206` Task 1: the deterministic local runtime fault matrix.
2. Continue with Plan 206 least-privilege topology and exact recovery adapter.
   Do not deploy or inject production faults without the plan's exact separate
   authorization gate.
3. After production enablement, let recovery confirmation open a fresh epoch
   automatically; independently verify its immutable certificate before
   marking Phase 05.6 and M1 complete.
