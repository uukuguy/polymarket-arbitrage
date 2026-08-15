# Lease-fenced cloud worker topology — design

## Problem and evidence

The transactional authority is Postgres and safely permits multiple claimers,
but the deployed service uses one `TransactionalControlPlaneScheduler` that
executes every source, Structure and Quote turn serially. On 2026-08-15 the
independent control-plane read service reported 9,327 runnable jobs, 67
retryable jobs, 74 open circuits and an oldest runnable age of roughly thirteen
hours. Quote had 514 runnable batches and Structure had 4,049 runnable ranges.
That is evidence of a service-rate deficit, so another larger serial turn
budget cannot establish continuous-production readiness.

## Options considered

1. Increase the current scheduler's serial turn budgets. This retains simple
   deployment but only extends a slow tick and lets source/CLOB latency block
   range and Quote work. Rejected.
2. Start many copies of the current all-purpose scheduler. Postgres leases
   prevent duplicate receipt commits, but every replica also runs admission and
   certification turns, producing needless contention and non-obvious source
   rate amplification. Rejected.
3. Deploy bounded role-specific worker pools over the existing lease-fenced
   jobs. One coordinator owns admission/certification cadence; independent
   range and Quote-batch pools drain their own queues. Recommended.

## Target topology

```
Postgres transactional job authority
   ├─ coordinator (1 replica): source admit/materialize + Structure/Quote certify
   ├─ structure-range pool (N bounded replicas): only range receipts
   ├─ quote-batch pool (N bounded replicas): only frozen Quote batches
   └─ alert pool (separate, no data-plane credentials): outbox delivery
```

The coordinator remains a single Fly process group. Pool replicas have stable,
unique worker IDs supplied by deployment identity; they do not create source
windows, write publication pointers, or run certifiers. Every worker continues
to claim with `FOR UPDATE SKIP LOCKED`, commit a lease-epoch-fenced outcome,
and treat a lost process as reclaimable work. No SQLite state, pointer
authority, or in-memory ownership is introduced.

## Backpressure and safety

- A pool has explicit `max_workers` and `turns_per_worker` bounds; no unbounded
  fan-out or retry loop is allowed.
- The coordinator declines new source admission when outstanding Structure or
  Quote work exceeds named high-water marks. It records/retains the decision
  in the existing operational projection rather than silently dropping a
  window.
- Old certified truth remains readable while new work drains. Certification
  remains fenced and rejects incomplete receipts.
- The production topology is activated only after staging proves a measurable
  drain rate greater than admission rate, zero duplicate receipts, and no
  lease-reclaim SLA violation.

## Operator read model

`/perception/control-plane` gains a bounded per-kind projection:

- runnable/retryable/leased counts and oldest age for Structure range and Quote
  batch queues;
- the next runnable `job_key` per pool (identity only, no payload);
- active worker/lease count and configured high-water status.

It remains read-only, Postgres-only and bounded by the existing timeout. This
removes batch-number guessing from fault acceptance and exposes saturation
before an extended backlog becomes an outage.

## Staged acceptance

1. Unit and real-Postgres tests prove role isolation, mutually exclusive
   claiming across workers, high-water admission behavior, and bounded status
   projection.
2. Deploy coordinator plus one Structure-range and one Quote-batch worker to
   staging; confirm no pointer mutation and unique receipts.
3. Increase each pool only to the bounded staging target, then prove queue age
   and depth fall over a fixed evidence window while live admission continues.
4. Use the API-projected next Quote key for exact retry and
   R2-upload-before-receipt fault tests; prove reclaim, recovery and scoped
   alert intents.
5. Start the 24-hour soak only after the above passes. Production cutover is a
   separate reversible, explicitly audited decision.

## Non-goals

This design does not migrate legacy L1/L2, alter public pointers, submit
orders, make a production cutover, or bulk-deliver historical alerts.
