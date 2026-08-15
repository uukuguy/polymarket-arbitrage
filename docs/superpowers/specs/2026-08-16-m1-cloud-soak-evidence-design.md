# M1 Cloud-Resident Transactional Soak Evidence Design

## Goal

Prove a continuous 24-hour formal M1 run without requiring the operator's
computer to be powered on.  The production collector, its observer, and the
authoritative evidence must all survive independently of local macOS state.

## Decision

Add one independently scheduled `soak_sampler` process group to the existing
`polyarb-control-worker` Fly application.  The sampler is deliberately not a
Structure, Quote, coordinator, or alert worker.  Every five minutes it reads
the independent formal control API, reads the exact five formal Worker machine
states from Fly's Machines API, builds the existing canonical V2 evidence
record, and appends it transactionally to Postgres.

The sampler persists only immutable observations.  It never creates M1 jobs,
claims leases, reads SQLite, alters R2 pointers, or sends alerts.  Missing
observations remain missing: a later verifier rejects the time gap rather than
manufacturing continuity.

## Alternatives considered

1. Keep the macOS LaunchAgent. Rejected: it makes final acceptance dependent
   on an operator computer even though production is cloud-resident.
2. Fold sampling into the coordinator. Rejected: a Worker could make its own
   health record and scheduling failures become harder to distinguish.
3. Use an external cron provider. Rejected: it adds another credential and
   operational trust boundary while Fly already hosts the formal topology.
4. **Chosen: separate Fly sampler + Postgres ledger.** It shares the formal
   identity and database but has an independent failure boundary, no volume,
   and a durable queryable ledger.

## Data model

Migration 017 adds `m1_soak_runs` and `m1_soak_observations`.

- A run has an operator-supplied immutable `run_id`, the locked control API
  URL, the sorted exact five-worker identity set, a `started_at` timestamp,
  and the canonical baseline record.
- An observation is keyed by `(run_id, observed_at)`, stores the full V2
  canonical JSON record and its SHA-256 digest, and references its run.
- The database accepts no update or delete grants for the sampler role; code
  likewise exposes append/read only methods.  Duplicate observation writes
  with the same digest are idempotent, while a conflicting digest fails.

The retained JSON record keeps the local verifier format intact.  The new
remote verifier reads ordered rows from Postgres and delegates to the same
pure `verify_soak` function, preserving all existing fail-closed semantics.

## Runtime flow

```text
Fly soak_sampler (5 min)
  -> GET independent control API
  -> GET Fly Machines API for the locked five IDs
  -> canonical V2 record
  -> Postgres append-only observation ledger

operator, at any later time
  -> Postgres ordered observations
  -> existing strict verifier
  -> PASS only after >= 24h, <= 900s gaps, and healthy progress
```

`POLYARB_FLY_API_TOKEN` is a Fly Machine API read token injected only into the
sampler Machine; it is never exposed through the public API.  The sampler
uses the HTTPS API directly rather than requiring `flyctl` in the production
image.  Its process command contains the locked formal app, API URL and the
five exact IDs.  The default interval is 300 seconds and the acceptance gap is
900 seconds, allowing one transient scheduling delay but never an invisible
outage.

## Failure handling and acceptance

- API/Machines/Postgres errors append no row.  The resulting missing interval
  fails verification.
- A non-started, missing, or unexpected locked Machine produces no healthy
  record and therefore fails the run.
- A changed identity set, digest conflict, count regression, unavailable API,
  newly higher expired lease/circuit count, or no successful-job progress is
  rejected by the pure verifier.
- The existing local `formal-transactional-soak-v4.jsonl` is retained as
  failed/superseded historical evidence; it cannot satisfy cloud-only final
  acceptance.  The local LaunchAgent is unloaded after a cloud baseline is
  successfully recorded.

## Verification

Unit and real-Postgres tests prove atomic run creation, idempotent append,
digest-conflict rejection, ordered reads, and remote verifier parity with the
existing JSONL verifier.  CLI tests prove the sampler uses only the API,
Machines API and ledger boundary.  Deployment-template tests require exactly
one `soak_sampler` process, no mount and no public HTTP service.  Production
acceptance is a fresh cloud ledger run whose remote verifier reports PASS after
an uninterrupted 86,400-second window.
