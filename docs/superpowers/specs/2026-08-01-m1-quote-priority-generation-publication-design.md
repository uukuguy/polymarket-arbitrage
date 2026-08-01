# M1 Quote-Priority Generation Publication Design

**Date:** 2026-08-01  
**Status:** approved for implementation
**Scope:** Phase 05.6 Plan 02 production closure  
**Amends:** `2026-07-31-m1-self-healing-structure-design.md` and
`2026-08-01-m1-opportunity-feed-double-buffer-design.md`

## Production evidence

The page-checkpointed Structure collector fixed unbounded Gamma traversal, but
production qualification exposed a second monolith at the publication boundary.
The completed window contained 164 event pages and 1,214 market pages. Three
publication attempts reached `persist` and then timed out: one child admitted
with a 75-second slice budget and two children with the 180-second finalizer
budget. The latter attempts occupied 316–320 seconds wall clock including the
wait for the shared producer lock.

During the same interval, complete Quote runs took roughly 194 seconds from
process start through certification and feed publication. Quote age crossed
304 seconds and later 403 seconds; strict `/health` and
`/arbitrage/opportunities` returned HTTP 503. The runtime feed recovered, but
strict health remained failed because a Structure attempt was timestamped
before it acquired the producer slot.

The finalizer's critical write is a single transaction that deletes the entire
`markets` table and reinserts about 116,000 rows. Raising timeouts cannot make
that operation cooperative or restart-safe. The opportunity ledger
reconciliation and notification work also runs synchronously after Quote feed
publication, delaying the next Quote attempt even though it is not part of the
feed's certification boundary.

## Decision

M1 will use three explicit execution lanes with durable handoffs:

1. **Quote core is the high-priority serving lane.** Its boundary includes
   collection, durable run completion, projection certification, opportunity
   scan, and atomic runtime feed publication. No Structure step may start while
   that boundary is active.
2. **Structure collection and publication are cooperative background lanes.**
   Gamma traversal keeps the existing page checkpoints. Normalization and
   publication write a new invisible generation in bounded chunks, then switch
   one durable current-generation pointer atomically.
3. **Candidate lifecycle processing is an asynchronous durable consumer.** Quote
   publication appends or coalesces one work item and returns. Ledger
   reconciliation, focused tracking, notification delivery, and retries consume
   that work independently and cannot delay the next Quote attempt.

The existing one-version Quote double buffer remains valid. It protects the
brief Structure-to-Quote handoff, while this design prevents producer contention
from exhausting the 300-second hard freshness limit.

## Structure generation model

### Durable state

Add a resumable publication record keyed by the completed Structure window:

| Field | Meaning |
|---|---|
| `window_id` | Completed raw Structure window being published |
| `generation_id` | Stable identity of the invisible normalized generation |
| `status` | `normalizing`, `writing`, `ready`, `published`, or `failed` |
| `normalize_cursor` | Exact durable continuation through raw staged facts |
| `write_cursor` | Exact durable continuation through normalized rows |
| `event_count`, `market_count` | Committed generation counts |
| `validation_hash` | Hash binding coverage, membership truth, issues, and counts |
| `checkpoint_at_ms`, `failure_reason` | Recovery and operator evidence |
| `published_snapshot_id` | Snapshot created by the atomic pointer switch |

Normalized events, memberships, group truth, issues, and markets are stored by
`generation_id`. Chunk upserts are idempotent. A generation is invisible to
current readers until it is complete and its validation hash is certified.

### Atomic publication

Publication no longer executes `DELETE FROM markets` followed by a full
reinsertion. The final transaction:

1. verifies the generation is `ready`, its counts match its durable receipts,
   and its validation hash still matches;
2. creates the new snapshot metadata row;
3. switches a singleton `current_structure_generation` pointer to the new
   generation and snapshot;
4. marks the generation and source window `published`; and
5. commits.

Readers resolve the current generation once per read transaction and select
only rows bound to that generation. They never join rows from two generations.
Old generations are reclaimed later in bounded batches after no current pointer
references them.

### Cooperative budgets

- Gamma collection: checkpoint after 45 seconds or 40 pages; hard stop at 75
  seconds.
- Generation normalization/write: checkpoint after at most 45 seconds and a
  bounded row count; hard stop at 75 seconds.
- Atomic pointer switch: a small transaction with a 15-second hard deadline.
- A step that reaches its terminal chunk returns a checkpoint. The following
  scheduler attempt performs the pointer switch; a child admitted under one
  budget never silently enters another stage.

A crash may repeat the last committed chunk but cannot skip one. A failed or
abandoned generation never changes current truth.

## Quote priority and attempt truth

`QuoteWorkerRuntime` exposes a distinct `pipeline_active` signal covering the
entire Quote core boundary, not merely the CLOB subprocess. Structure admission
checks it after obtaining the coordination lock and yields without increasing
the failure counter when Quote is active or due.

Structure `snapshot_attempts.started_at_ms` is written only after the execution
slot is acquired. Queue wait is separately observable as a deferred/admission
receipt and is not compared with a child runtime budget. Consequently:

- a genuinely over-budget child fails and is recovered;
- waiting behind a healthy Quote pipeline cannot create a false
  `snapshot-subprocess-timeout-exceeded`; and
- prolonged deferral is visible and alerts as Structure starvation rather than
  being mislabeled a child timeout.

Quote cadence remains start-to-start. Post-publication cleanup is limited to
bounded maintenance work and must not execute inside the Quote core boundary.
If maintenance lacks safe resource capacity, it remains queued; it cannot delay
the next Quote collection.

## Candidate lifecycle queue

The durable Quote-run certification transaction writes one idempotent
reconciliation item identified by `(quote_run_id, universe_hash)` before the
in-memory feed is published. Publishing an existing item is a no-op. Items are
claimed FIFO and no certified run may be superseded or discarded before its
terminal receipt; this preserves candidates that exist for only one Quote run.

The consumer:

1. loads the certified opportunity scan for that exact Quote run;
2. reconciles candidate masters and transitions in bounded transactions;
3. assigns stable non-null `opportunity_id` values;
4. records first-seen, edge/size change, unavailable, no-edge, closed, and
   reappeared transitions;
5. appends notification outbox facts atomically with lifecycle changes; and
6. retries delivery independently with durable incident evidence.

The queue records claim owner, lease expiry, attempt count, last error,
checkpoint cursor, and terminal receipt. A daemon restart resumes from the last
committed candidate. Queue corruption, missing certified input, or an expired
lease is health-visible and fail-closed for lifecycle claims, but cannot erase
or invalidate an independently certified Quote feed.

The legacy watcher status endpoint must stop deriving availability from the old
market-map cache. Current candidate authority, exact-ID history, and alert state
must all be readable from the durable lifecycle store.

## Health and alert contract

Strict health exposes separate checks for:

- Quote core freshness and active pipeline;
- Structure collection/publication stage, checkpoint age, and defer age;
- current generation identity and generation/snapshot count agreement;
- candidate reconciliation queue lag, lease state, last terminal receipt, and
  lifecycle authority count; and
- notification outbox backlog and delivery incident state.

An ordinary Quote-priority Structure defer is `warn`, never a fabricated
failure. It becomes a failure if its persisted defer age breaches the Structure
publication SLA. Quote age greater than 300 seconds remains an unconditional
failure. A valid empty candidate set is distinct from an unavailable or lagging
lifecycle consumer.

Polywatch tracks Quote, Structure publication, candidate reconciliation, and
notification delivery as separate incidents. Recovery requires a new
component-specific terminal receipt; another component's success cannot close
the incident.

## Migration and rollout

1. Create generation, pointer, queue, and receipt schemas without changing the
   current reader.
2. Backfill one generation from the current certified snapshot and verify
   counts and hashes against existing current tables.
3. Dual-write and compare one complete Structure publication without switching
   readers.
4. Atomically enable generation readers only after the comparison passes.
5. Enable the asynchronous lifecycle consumer and prove non-null IDs/history
   before retiring the synchronous Quote callback.
6. Retain the old tables and last certified generation as a rollback source
   until production acceptance completes.

No migration step may expose a partial generation or require an empty current
market view. Rollback switches the current reader mode/pointer to the last
certified source; it never reconstructs truth from incomplete staging.

## Verification

Local tests must prove:

1. chunk transaction atomicity, exact cursor resume, idempotent replay, and no
   visibility before pointer switch;
2. pointer-switch rollback on count/hash mismatch and concurrent-reader
   consistency before, during, and after publication;
3. Quote pipeline priority across collection, certification, feed publication,
   maintenance, and candidate queue enqueue;
4. attempt runtime starts at slot acquisition and defer age is independently
   reported;
5. lifecycle queue lease recovery, lossless FIFO ordering, restart continuation, stable
   opportunity IDs, complete transition history, and notification retry;
6. existing fail-closed market truth, Quote identity, double-buffer, and
   opportunity response contracts remain intact; and
7. schema migration, rollback, full tests, lint, planning-status, manual, and
   image gates all pass on one committed revision.

Production acceptance requires all of the following:

- three consecutive complete Structure generations publish naturally;
- Quote age remains below 300 seconds at every sample and the opportunity
  endpoint never returns 503 during those cycles;
- the current generation, snapshot, Quote feed, and opportunity response expose
  mutually consistent identities;
- Structure failure counter returns to zero with no stale running attempt;
- detected gross candidates receive non-null opportunity IDs and replayable
  first-seen/change/closed history;
- a controlled lifecycle/notification failure creates an incident, retries,
  and closes only with component-specific recovery evidence; and
- resident Polywatch has no unresolved incident after recovery.

## Rejected alternatives

- **Longer finalizer timeout:** preserves the non-cooperative full-table rewrite
  and already failed at 180 seconds in production.
- **Vertical scaling plus unrestricted concurrency:** may reduce wall time but
  does not prove SQLite publication atomicity, Quote priority, or bounded
  recovery; it also makes correctness depend on one machine size.
- **Pause Structure whenever Quote is slow:** protects the feed by allowing
  Structure to become indefinitely stale, violating continuous production.
- **Serve stale Quote indefinitely:** hides producer failure and defeats the
  hard freshness and alert contract.
- **Synchronous candidate reconciliation after feed publication:** continues to
  delay the next Quote attempt and couples observation history availability to
  the serving path.
