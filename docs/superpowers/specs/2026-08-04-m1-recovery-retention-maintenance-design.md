# M1 Recovery Streak and Resident Retention Maintenance Design

**Date:** 2026-08-04  
**Status:** approved for implementation
**Scope:** M1 L1 scheduler recovery truth, Structure staging retention, legacy
snapshot retention, and resident Structure-generation evidence cleanup

## Incident facts

Fly release 233 runs exact source
`06d3f92a8947eb74300391b964b8225c64ace28d`. Its first post-sidecar natural
Structure window authenticated and sealed 153,525 event-member rows, then began
building the successor generation. Four isolated-child timeouts were separated
by many successful durable sidecar and generation checkpoints, but
`snapshot:failure_counter` remained `4/5`. The implementation resets the
counter only after a complete `OK` or `DEGRADED` snapshot, so a fifth unrelated
timeout can incorrectly satisfy a contract described everywhere as
*consecutive* failures.

The same production cycle exposed three retention failures. Current-schema
minimal reproductions establish two exact causes:

1. Structure-window purge omits
   `structure_sync_event_group_truth_staging` and attempts to delete parent
   windows retained by immutable publication/drift evidence. SQLite correctly
   rejects the transaction with `FOREIGN KEY constraint failed`.
2. `purge_old_snapshots` can select a snapshot still referenced by
   `snapshot_attempts`, then attempts to delete the parent without retiring the
   same-retention operational attempt.

Generation evidence pressure is independently stuck at nine retained
generations and seven reclaimable. The authenticated bounded cleanup primitive
exists, but only a manual CLI/Makefile command calls it. A primitive without a
resident owner is not production self-healing.

## Decision

Implement one coherent maintenance contract:

1. A successful, non-deferred durable scheduler checkpoint breaks the current
   failure streak immediately and persists `failure_counter=0`.
2. A scheduler already in `RECOVERING` remains there until a complete certified
   snapshot succeeds; partial progress resets the streak but does not claim
   full recovery.
3. Structure retention reclaims heavy staging payloads while preserving the
   small window/publication/receipt identity skeleton required by immutable
   audit and drift authorization.
4. Snapshot retention transactionally retires old snapshot-attempt evidence,
   but never implicitly deletes an independently retained Quote run.
5. A dedicated resident generation-cleanup worker owns the existing bounded,
   authenticated cleanup API. It is lower priority than Quote and cooperative
   with Structure, restart-safe, health-visible, alerted, and continuously
   drains pressure without an operator command.

This design does not relax any source-truth, comparison, drift, read-cutover,
Quote-age, or opportunity-serving gate.

## 1. Consecutive-failure semantics

### Successful scheduler work

The following results break the streak when they are non-deferred and their
durable child contract validates:

- an event-member checkpoint, including a terminal seal;
- a classifier-v2 drift checkpoint or seal;
- a Gamma page/bootstrap checkpoint;
- a generation normalization/write/certification checkpoint.

The scheduler sets `_failure_counter=0` and persists it before returning from
that tick. The state transition is deliberately asymmetric:

| Prior state | Durable checkpoint result | Counter | Resulting state |
|---|---|---:|---|
| `RUNNING` | forward checkpoint | 0 | `RUNNING` |
| `RECOVERING` | forward checkpoint | 0 | `RECOVERING` |
| `RECOVERING` | complete certified snapshot | 0 | `RUNNING` |

Writer-busy, Quote-priority defer, identity-stale defer, contract supersession,
and a killed/failed child neither prove success nor reset the streak.
Supersession retains the existing explicit test contract. A defer also does not
increment the counter.

The implementation will use one scheduler helper so every checkpoint family
cannot silently diverge. Health continues to expose the durable scheduler
state and counter; Polywatch alerts only when a real consecutive streak reaches
the threshold.

## 2. Structure staging retention

Deleting a published `structure_sync_windows` row is no longer the retention
operation. Publication and drift receipts intentionally retain that identity.
Add nullable `staging_reclaimed_at_ms` to the window metadata and perform one
bounded `BEGIN IMMEDIATE` transaction that:

1. selects one terminal `published` or `failed` window whose staging has not
   been reclaimed and which is outside the configured keep set;
2. deletes only heavy replayable staging payloads;
3. sets `staging_reclaimed_at_ms` on the preserved window row;
4. commits all deletions and the marker atomically.

Heavy payload ownership includes:

- `structure_sync_event_staging`;
- `structure_sync_market_staging`;
- `structure_sync_event_market_staging`;
- `structure_sync_event_metadata_staging`;
- `structure_sync_event_member_staging`;
- `structure_sync_event_group_truth_staging`;
- event-conflict proof and Merkle-node payloads.

Small authority records remain: source/member receipts, conflict summary,
completed cursor/checkpoint rows, publication identity, drift receipts, and the
window row. They are the proof skeleton and are not the source of volume
pressure.

The existing purge entry points keep their public signatures for callers but
return windows whose *staging payload* was reclaimed. A schema contract test
enumerates direct Structure-window foreign keys and forces every current or
future child table to be classified as heavy-reclaimed, proof-retained, or
independently protected. Adding a new child table without a classification
fails CI.

## 3. Legacy snapshot retention

`purge_old_snapshots` keeps selection and deletion under its existing
`BEGIN IMMEDIATE` boundary. Before deleting an eligible snapshot it deletes
the associated `snapshot_attempts`; these are bounded operational attempt
records with the same lifetime as the snapshot they describe, not immutable
market-authority receipts.

The candidate query adds an explicit `NOT EXISTS neg_risk_quote_runs` guard.
Quote runs and their legs/quotes/source receipts remain owned by
`NegRiskQuoteStore.purge_old_runs`. Once that independent cleanup releases the
snapshot reference, a later snapshot retention tick may reclaim the snapshot.
This preserves the last known-good Quote restoration floor and prevents either
retention subsystem from deleting another subsystem's authority.

A second schema contract test classifies every direct FK to `snapshots` as one
of:

- transactionally deleted legacy payload/attempt;
- an explicit candidate-selection protector;
- immutable generation/drift evidence protected through publication identity.

## 4. Resident generation-cleanup worker

Add a small `StructureGenerationCleanupWorker` sibling to the existing
Snapshot scheduler and Quote worker. It uses the same `producer_lock` and the
same Quote runtime priority predicate.

### Admission and fairness

1. If Quote is active or due, record `quote-priority` and wait without taking
   the producer lock.
2. Acquire the shared lock, then recheck Quote priority.
3. Run exactly one existing
   `cleanup_structure_generation_evidence(max_rows=500)` transaction.
4. Release the lock before sleeping or emitting network alerts.
5. When pressure remains, yield for at least 50 ms before the next chunk. This
   gives the scheduler's 100 ms continuation and any Quote request a chance to
   queue; `asyncio.Lock` waiter ordering then prevents the maintenance loop
   from monopolizing the producer. The interval is a CPU/lock fairness floor,
   not a throughput throttle.
6. When no candidate remains, use a 30-second idle interval. SQLite writer-busy
   defers for five seconds and does not count as a failure.

The worker performs no Gamma/CLOB/network reads and never modifies current
generation pointers, publications, comparisons, drift receipts, or the two-
generation rollback floor.

### Durable runtime truth

Add a singleton `structure_generation_cleanup_runtime` row containing:

- `state`: `idle`, `running`, `backoff`, or `blocked`;
- `consecutive_failures`;
- `last_attempt_at_ms`, `last_success_at_ms`, and `next_attempt_at_ms`;
- last generation ID, phase, rows deleted, and safe error kind;
- checkpoint timestamp.

The cleanup progress/receipt tables remain the destructive-operation authority;
the runtime singleton is restart-persistent operational truth. Startup converts
an orphaned `running` state to bounded retry rather than leaving it wedged.
Authentication-blocked cleanup is `blocked` and fail-closed. Unexpected errors
increment the maintenance-specific counter and retry with capped exponential
backoff; they never stop Structure or Quote.

Health adds `snapshot:structure_generation_cleanup_runtime`. Polywatch opens an
incident for blocked state, repeated failures, or stale progress while
reclaimable pressure exists; it sends one recovery only after pressure and the
runtime state recover. Existing `snapshot:structure_generation_evidence`
remains the source of retained/reclaimable truth.

## 5. Configuration and lifecycle

Configuration adds production-safe bounded fields with Makefile-documented
defaults:

- cleanup enabled when generation publication is enabled;
- `max_rows=500`;
- active/idle/writer-busy intervals `0.05/30/5` seconds;
- capped retry delay and a health failure threshold.

`daemon.main` constructs and starts the worker only in the in-process L1
producer topology, exposes its durable runtime to the HTTP app through the
shared SQLite store, and includes it in the
same cancellation and five-second graceful-shutdown gather as the scheduler
and Quote worker. Isolated-supervisor mode must either own an equivalent
explicit worker process or report cleanup disabled; it cannot accidentally run
two cleanup owners.

The existing manual `make structure-generation-cleanup` entry remains an
operator diagnostic/recovery primitive, not the normal production mechanism.

## 6. Error and alert chain truth

For every failure path the implementation must identify:

1. the writer mutation or rejected transaction;
2. the durable cleanup runtime/progress field it updates;
3. the exact `/healthz` subcheck reading that field;
4. the Polywatch decision and deduplicated incident state;
5. the successful retry/receipt that closes the incident.

No cleanup exception is reduced to a log-only fail-soft warning. Conversely,
maintenance failure cannot change certified market truth, disable Quote, or
make a stale generation appear fresh.

## 7. Test and production acceptance

TDD begins with observed RED tests for:

- failure → successful durable checkpoint → failure produces counter `1`, not
  `2`, after the second failure;
- partial progress in `RECOVERING` resets the streak but does not set
  `RUNNING`;
- defer and supersession preserve their existing semantics;
- failed and published windows containing group-truth staging reclaim payloads
  without deleting proof skeletons or raising FK errors;
- snapshot attempts are retired atomically, while Quote-referenced snapshots
  are skipped and later become eligible after Quote retention;
- schema FK classification fails when an unowned child table is introduced;
- worker restart recovery, single ownership, Quote pre/post-lock priority,
  fairness, writer-busy defer, bounded backoff, blocked authentication, health,
  Polywatch alert/recovery, and graceful cancellation;
- a production-shaped 300,000-row reclaimable generation drains within 240
  seconds, including transaction and fairness waits, while a simulated Quote
  request that becomes due acquires the producer after at most the current
  500-row cleanup transaction and Quote age never reaches 300 seconds.

Before deployment, the exact committed SHA must pass focused tests, the full
repository suite, Ruff, documentation checks, `git diff --check`, planning
status, and independent review.

Production rollout keeps Quote disabled and legacy reads while proving:

1. release/image/machine/volume identity matches the approved SHA;
2. the current natural generation and classifier-v2 authorization complete
   without manual advance, restart, or pointer writes;
3. a successful durable checkpoint resets the false 4/5 streak;
4. generation pressure automatically converges from nine retained generations
   toward the exact floor of two, with current and rollback identities intact;
5. staging and snapshot retention produce no FK errors;
6. injected scoped cleanup failure alerts, retries, and closes without stopping
   Structure or Quote.

Only after those gates pass may Task 8 switch to generation reads, enable Quote,
and run the natural-generation/two-minute availability UAT. Candidate lifecycle
queue implementation and its final three-cycle UAT remain required before M1
completion.

## Rejected alternatives

- **Manual cleanup as normal operation:** cannot satisfy unattended production
  recovery and leaves health failed indefinitely.
- **Cleanup only after complete snapshots:** couples housekeeping back into the
  publication critical path and cannot drain at the generation production
  rate.
- **Raise the failure threshold:** hides incorrect streak semantics and merely
  delays false RECOVERING.
- **Delete publication/window proof skeletons:** saves little space and destroys
  the immutable identities required to authenticate drift and rollback.
- **Let cleanup bypass the shared lock:** creates unbounded SQLite contention
  against Quote and Structure and makes the 300-second Quote SLA unverifiable.
