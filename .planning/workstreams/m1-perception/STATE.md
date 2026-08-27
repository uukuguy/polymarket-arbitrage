---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: market-perception
current_phase: 05.6
status: in_progress
stopped_at: ff4c8093 minimal-rotation rollout explicitly authorized; immediate preflight is next
last_updated: "2026-08-27T19:54:43+08:00"
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
  production Alembic revision `026`, and R2 bucket `polyarb-control-plane`.
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

- **Credential/runtime invariant:** The runtime-event-writer's exposed
  versioned DSN was rotated and removed from ordinary Machine env; both
  canonical writer secrets are hidden and the same Machine/image remains
  healthy. Do not rotate it again during this rollout. All database runtime
  roles use hidden secrets and the Supabase IPv4 Session Pooler, not the
  unreachable direct IPv6 database endpoint.

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
- Plan `05.6-206` is locally complete through migration 025, a deterministic
  12-class real-PostgreSQL fault matrix, isolated runtime/qualification apps,
  an exact capability-limited Fly recovery adapter, durable observe-only
  decision/idle facts, and a read-only zero-mutation verifier. The production
  observe, job-recovery, and process-recovery gates are explicitly NOT RUN.
- Plan `05.6-207` is locally complete after production-boundary remediation. Corrected
  executable release `ff4c8093d55a6363f7dcb26f6ffe2113e260a9ba`
  schema-qualifies both daemon paths and catalog-enumerates a closed effective
  authority envelope across every non-system namespace, relation, sequence,
  ownership, SECURITY DEFINER routine, database CREATE, search-path setting and
  exact PostgreSQL 16 membership option. `TEMPORARY` remains an explicit
  compatibility allowance under the controlled namespace contract.
- The first authorized production `026` attempt under `d050c829` failed closed
  and rolled back completely at revision `025`. Production Supabase exposes two
  PUBLIC-readable views in the inaccessible `extensions` schema; PostgreSQL's
  object ACL helper reports them true without schema USAGE. Commit `fe36a330`
  corrects migration and daemon verification to enumerate effective object
  authority only in reachable schemas while retaining the all-schema namespace,
  direct-grant, ownership, membership and search-path gates.
- The second authorized production `026` attempt under `db51b21d` also failed
  closed and rolled back at revision `025`. PostgreSQL 16 automatically records
  Supabase's delegated `CREATEROLE` creator `postgres` as an incoming member of
  each created capability, granted by `supabase_admin` with exact options
  `(admin=true, inherit=false, set=false)`. Commit `03a2deee` admits only that
  ambient non-effective creator edge while still requiring the matching scoped
  login to be the sole effective member. Climb H-021 cycle 24 run
  `20260827-100730-h-021` scored 100/100 across all nine nodes.
- The authorized `03a2deee` attempt successfully applied revision `026` and
  passed the independent capability preflight. Scoped login provisioning then
  failed closed and rolled back both passwords/logins before any Fly app was
  created: delegated `CREATEROLE` also adds the exact non-effective creator
  edge to each new LOGIN. Commit `ad8a123f` admits that exact incoming login
  tuple while preserving its single outgoing application capability tuple.
  Climb H-022 cycle 25 run `20260827-104319-h-022` scored 100/100 on all nine
  nodes.
- The authorized `ad8a123f` provision then created and committed both exact
  scoped LOGIN envelopes. Active daemon verification failed closed before any
  Fly app was created, so their first passwords were never shipped and were
  discarded. A retry exposed PostgreSQL `42501`: the delegated creator can
  rotate a password or toggle LOGIN, but cannot replay the complete verified
  role attribute clause. Release `ff4c8093` validates the full existing
  envelope and writes only the necessary LOGIN/password delta. H-023 cycle 26
  run `20260827-113937-h-023` scored 100/100 across all nine nodes.
- Climb H-020 cycle 23 run `20260827-085048-h-020` scored 100/100 and introduced
  `make control-plane-fly-topology-audit`. Its read-only production proof shows
  all original Machines started, no credential-bearing ordinary env key on the
  repaired writer, and both canonical writer secret names present without
  exposing values or raw provider bodies.
- Operator mutations use only `POLYARB_CONTROL_PLANE_DB_ADMIN_DSN`; runtime and
  qualification verification retain their distinct app-scoped DSNs. The
  disable path reconnects as admin after scoped verification, and neither app
  receives the admin or the other app's secret.
- Plan `05.6-207` is registered into `make planning-status` through the
  explicit `plan-source` anchor in `05.6-207-SUMMARY.md`; that gate also
  recomputes reviewed template hashes. `.githooks/pre-commit` protects staged
  SUMMARY content and `.githooks/commit-msg` reliably enforces plan-scoped
  subjects from the real message file.
- Climb H-014 cycle 15 through H-018 cycle 21 scored 100/100 on dedicated local
  gates. H-016 run `20260825-053507-h-016` covers real
  migration/trust/late-ingress/freshness/recovery-observation behavior plus
  exact 26-hour restart/replay certificate evidence; H-017 run
  `20260825-074318-h-017` covers strict reads, real-PostgreSQL event/outbox
  chains, API/smoke contracts and restart/ordering behavior. Fresh H-018 run
  `20260826-114829-h-018` covers isolated topology and adapter authority,
  both scoped daemon nodes, qualification identity digest, zero recovery
  actions, and restart behavior. Append-only cycle 21 run
  `20260826-135855-h-018` binds to exact executable commit
  `d050c8290c52e07acb72c8db7fe3fb02072d126c`.
- H-019 cycle 22 run `20260827-075919-h-019` scored 100/100 across the same
  nine-node production-enablement profile and confirms the immutable exact
  observe-only authorization envelope. It performs no external submission or
  production mutation.
- Production truth boundary as of the 2026-08-27 19:49 +08:00 audit: production database is
  `postgres`; revision 026 is applied with exactly two NOLOGIN capability roles
  and two safe scoped LOGIN roles. The full admin authority preflight is ready,
  successful attempts reached 92111, the control API health endpoint is 200,
  and all six original Machines are started. The new runtime-controller and
  qualification-worker apps do not exist. No new-app secret, recovery
  enablement, fault mutation, observe-only window, job recovery gate, or process
  recovery gate has run.

## Formal Acceptance

- **Active qualifying run:** none. `m1-formal-20260823T1335Z` is immutable
  historical evidence; replay found the first breaking `lease.expired` fact at
  `2026-08-23T16:22:21Z`, so its earlier 338-second liveness pass cannot become
  final acceptance.
- Repeatedly starting another manual 24-hour run is no longer an accepted
  detection/recovery mechanism. Plan `05.6-204` now provides the local automatic
  epoch/certificate mechanism, Plan `05.6-205` provides shared operator truth,
  and Plan `05.6-206` provides deterministic local enablement proof. An exact
  authorized production observe-only release must still pass before a
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
| Continuous final-topology acceptance | rolling epoch + immutable certificate | local mechanism plus deterministic enablement complete; blocked on exact authorized production enablement |

## Resume

1. Obtain explicit approval for the fresh exact remaining observe-only rollout package:
   corrected application release `ff4c8093d55a6363f7dcb26f6ffe2113e260a9ba`,
   production database `postgres`, revision 026,
   minimal password rotation for the two existing scoped LOGIN roles, active
   identity verification, the two new private apps, observe-only mode,
   empty recovery allowlist, rollback, and evidence directory. Do not infer
   this authority from generic approval.
2. Treat `d050c829`, `db51b21d`, `03a2deee`, and `ad8a123f` authorizations as
   consumed evidence. Revision 026 and both LOGIN envelopes stay applied; their
   unshipped credentials are not recoverable and must be rotated.
   The separate runtime-event-writer remediation is complete and verified; do
   not rotate it again during the corrected rollout.
3. After authorized production enablement, let recovery confirmation open a fresh epoch
   automatically; independently verify its immutable certificate before
   marking Phase 05.6 and M1 complete.
