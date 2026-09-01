# M1 Runtime Recovery Closure Design

## Problem and evidence

M1 is not healthy when the control API returns HTTP 200 but production work is
stale. On 2026-09-01 the durable control-plane records showed active Quote and
Structure attempts with heartbeats and progress older than 33 hours. The
external watchdog correctly classified those failures, but the durable
notification outbox had 13,766 rows in `pending` state and zero delivery
attempts. The deployed `polyarb-control-alert` app runs the database-free
watchdog only; the separately defined `fly-control-alert-delivery` consumer is
not deployed.

The Supabase database is 1.58GB and has entered provider read-only mode. Its
largest relations are append-only operational and qualification evidence, not
the current business research products. Clearing the database without a
retention policy has therefore only repeated the failure.

## Goal

Restore M1 to a state in which an operator can trust one explicit verdict:

`healthy` requires fresh business products, a deliverable alert route, and
database capacity headroom. A passing HTTP liveness check or started Fly
Machine alone never establishes that verdict.

## Options considered

1. Add a dashboard warning while retaining the current topology. This improves
   presentation only; the database can still become read-only and suppress the
   data path needed to create or deliver an alert. Rejected.
2. Make the watchdog write its failures into the existing outbox and have the
   dashboard poll it. This retains the same database dependency at the precise
   point that database capacity fails. Rejected.
3. Keep the database-free watchdog as a direct Telegram pager, deploy the
   existing transactional outbox consumer as a second service, and introduce a
   bounded evidence lifecycle with an independent capacity probe. Chosen.

## Architecture

### Independent pager and durable delivery

The external watchdog remains able to read the control API and exact Fly
Machine state without a Postgres credential. It must emit a direct Telegram
page on a transition to unhealthy whenever the runtime-event writer rejects or
cannot acknowledge the transition. The pager outcome is logged with a stable,
redacted delivery code.

`polyarb-control-alert-delivery` is a distinct Fly application that runs the
existing `alert-serve` process every five seconds. It owns only the scoped
Postgres outbox DSN and Telegram credentials. Its process health, oldest
pending outbox age, and pending count are explicit control-plane facts. A
pending Telegram record older than two minutes, a pending Dashboard record
older than five minutes, or a missing delivery-worker heartbeat is a critical
runtime failure.

The watchdog is not considered proven merely because it logs a failed check.
The production verification requires a Telegram delivery receipt for a
synthetic, bounded transition and a recovered receipt after the fault clears.

### Capacity and data lifecycle

Introduce a single database-capacity observation model. It records only
bounded aggregate facts: observation time, database size bytes, configured
warning/critical budgets, top-relation summaries capped at ten, and the
oldest-pending alert age. It does not retain SQL text, DSNs, or row payloads.

The capacity controller classifies these thresholds:

| State | Database budget used | Required action |
|---|---:|---|
| healthy | below 60% | continue |
| warning | 60% to below 75% | page Dashboard; schedule retention review |
| critical | 75% to below 85% | Telegram page; stop admitting nonessential evidence |
| exhausted | at least 85% or provider read-only | Telegram break-glass page; pause writers; require operator recovery |

The initial budget is an explicit configured number below the Supabase plan
hard limit, never inferred from a free-plan quota. The capacity controller
uses the same database connection only to measure capacity; the independent
watchdog treats its stale or unavailable observation as failure.

Evidence retention is a separate, explicit administrative action. It deletes
only expired operational rows from a finite allowlist and never deletes current
or previous certified business generations, current incidents, unresolved
alerts, or qualification certificates. Each retention run records a compact
receipt containing cutoff, protected generations, table counts, and reclaimed
row counts. Raw long-horizon evidence belongs in immutable R2/Parquet archive;
Postgres retains short operational windows and business projections.

The first implementation uses retention classes rather than arbitrary table
purges:

- 14 days: runtime event stages, job attempts, checkpoint/range/source/quote
  receipts and their input ledgers.
- 30 days: resolved incidents, delivered alert rows, qualification ingress and
  recovery observations.
- forever in Postgres until superseded by a bounded generation policy: current
  and immediately previous Structure/Quote/Opportunity business projections,
  unresolved incidents, pending alerts, qualification certificates, and
  publication pointers.

No deletion runs automatically until an administrative dry-run emits its exact
protected set and the operator supplies the existing explicit destructive
approval token. The production recovery run uses this audited path, not manual
SQL or another database reset.

### Business and operational truth

The business overview and Dashboard receive a `runtime_verdict` with separate
fields for `business_freshness`, `alert_delivery`, `capacity`, and
`qualification`. A degraded or unavailable value never becomes zero
opportunities. The control-plane page shows the oldest task heartbeat, oldest
pending delivery age, current capacity state, and the newest capacity receipt.

The user-facing business page continues to prioritize Structure and Quote
research products. Operational evidence stays in the control-plane page; it
does not crowd out market facts.

## Recovery sequence

1. Restore Supabase writes by resolving the provider quota. This is an external
   billing/storage action and cannot be automated by the repository.
2. Run an exact read-only preflight: database identity, revision, capacity,
   stale task detection, outbox age, and Fly topology.
3. Deploy the isolated alert-delivery app and prove one Telegram receipt.
4. Migrate through revision 040, then run role and schema preflights.
5. Execute the approved retention dry-run, review its immutable receipt, and
   only then run the narrowly approved cleanup if headroom is still inadequate.
6. Deploy the new M1 image and require fresh Structure and Quote generation
   lineage, alert delivery healthy, capacity below warning, and a 24-hour
   watchdog/collection soak before declaring the system normal.

## Non-goals

- No automated billing upgrade, password reset, or database reset.
- No deletion of business generations outside the defined protected window.
- No claim of business health from HTTP liveness, Fly machine state, or an
  empty opportunity list.
- No dashboard scan of raw R2 data or unbounded operational history.

## Verification

Tests must cover: failed writer response invokes direct pager; missing delivery
service and stale pending rows produce critical verdicts; threshold boundaries;
protected records survive retention while expired allowed rows are deleted; a
read-only database yields `exhausted` rather than a false healthy result; and
the dashboard decoder renders each state without showing fabricated zeros.

Production proof requires: a capacity observation after write restoration, the
alert-delivery Machine in `started` state, a Telegram receipt, a delivered
outbox receipt, revision 040, fresh business lineage, and one uninterrupted
24-hour window in which no business freshness, delivery, or capacity gate is
degraded.
