# Task 6 Implementer Report

Status: AUTHORITY INTEGRITY CLOSURE COMPLETE — verification green

## Scope

Task 6 only: bounded read-only perception APIs, replay-resistant HMAC operator
wake-ups, durable coalescing queue evidence, real serial producer consumption,
cloud Make entries, and manual synchronization. No Dashboard, deployment,
wallet, signing, balances, orders, or real-money execution.

## Current-authority schema and invariants

1. Discovery and Candidate current authority are materialized per group. Each
   row stores canonical current identity/state fields plus a canonical row
   hash; no status path deserializes or iterates a singleton all-groups blob.
2. One O(1) aggregate row stores only counters, queue depths, attempt/breach
   totals, current Candidate opportunity count, and a commutative aggregate
   digest of the per-group row hashes. Oldest schedule age is read through an
   indexed `ORDER BY ... LIMIT 1`, never a lifecycle aggregate.
3. Canonical triggers append every INSERT/UPDATE/DELETE on Group revisions,
   schedules, Candidate facts, admissions, attempts, and their materialized
   dependencies to an owner mutation journal with table/op/key and canonical
   old/new fields. A legal writer declares and consumes only its expected
   deltas while advancing a guard cursor/hash in the same transaction.
4. Any direct SQL mutation leaves an unconsumed journal event. Status,
   initialization, and every later legal writer fail closed before mutation;
   no projection refresh may consume, overwrite, or “wash clean” that event.
   Initialization may full-validate/bootstrap only when journal, guard, and
   projection are all absent as one legacy state.
5. Initialization verifies canonical trigger SQL. Missing or drifted triggers
   fail closed, or are recreated only after safe full validation; `IF NOT
   EXISTS` is not accepted as proof that trigger semantics are current.
6. Candidate historical Quote/fact/receipt rows may compact into a bounded
   suffix even when active group cardinality exceeds the suffix limit. Current
   per-group authority remains complete; the aggregate count/digest stays
   exact. Reconciliation close/change revokes or synchronizes schedule,
   admission, per-group projection, and aggregate authority in one transaction.
7. Candidate current authority/aggregate and Discovery status/group projection
   are journaled owners, not trusted caches. Their canonical INSERT/UPDATE/DELETE
   triggers use the same transaction token as the raw facts that caused them.
   The guard authenticates both aggregate roots, so hot reads compare them in
   O(1) before returning current authority.
8. The owner guard retains an authenticated prefix-base id/hash. Every read,
   initialization, and next writer replays at most 128 retained events, checks
   the first link against that base, recomputes every canonical event hash, and
   requires the final id/hash to equal the consumed guard tail. Pruning advances
   the base atomically; a changed event, missing tail, or broken link fails
   closed.

## Truth chain

1. All six `/perception/*` handlers run behind one absolute request deadline.
   Their SQLite connections use URI `mode=ro`, `query_only=ON`,
   `busy_timeout=250`, and an 0.8-second progress abort; nested validators and
   Python replay loops share that deadline. A timed-out SQL worker is
   interrupted, closes its connection, and converges before the response.
   Every endpoint reads one SQLite snapshot; page/history JSON and response
   bytes are bounded before serialization, and group/history endpoints expose
   stable cursors.
2. Status validates every retained Group revision, complete/failed/superseded
   Quote, Candidate fact, and success receipt before counting an edge.
   Group event identity is immutable; revisions and timestamps are contiguous
   and monotonic; Quote/fact state-specific fields and as-of authority are
   exact. Once the live Quote/fact/receipt suffix crosses 8,000 rows, a fully
   validated prefix is atomically replaced by a versioned rolling checkpoint
   that binds its cumulative digest and the retained per-group seed rows.
   Validation then checks the checkpoint and replays a bounded suffix. Revoked
   historical groups are committed by the prefix digest and evicted from the
   live seed; current watching authority and its exact legs remain present. A
   10,010-success continuity test, checkpoint/suffix tampering, membership
   supersede across the boundary, and delete-failure rollback all pass.
   Valid count zero is `available/no-certified-edge`; corrupt Candidate or
   incident evidence is HTTP 503 `unavailable`.
3. Group list/history validate the same event/revision/time/status transition
   chain, not only membership digests or ascending revision numbers.
   Discovery and Reconciliation preserve the Task 3/4 validators without
   unbounded history reads. Discovery validates and atomically compacts a
   completed receipt prefix after 8,000 rows, leaving a 1,000-row suffix. Its
   versioned checkpoint binds the completed batch/sample/schedule-evidence
   anchor, cumulative compacted counts, coverage seed and chained prefix
   digest. Reconciliation checkpoints every successfully published page and
   binds the exact window owner, staging digest/count, latest page receipt,
   seen cursors, cumulative page metrics and chained prefix digest before
   pruning that page's receipt/sample rows in the same transaction. Discovery
   status reads a hash-bound current-identity projection and trigger-maintained
   attempt/breach counters instead of scanning Group revision, admission,
   attempt, or Candidate fact lifecycles. Legal writers refresh it in their
   owner transaction; raw/projection tampering and projection-write failure
   fail closed. Status validates each checkpoint and replays only the bounded
   suffix; checkpoint
   tampering, retained-prefix injection or prune failure fails closed.
   Validated legacy histories migrate atomically and idempotently, while
   histories that cannot prove the new checkpoint are not guessed or pruned.
   Incidents validate lifecycle ordering plus Task 5 component-specific
   recovery proof. Returned incident evidence is size/depth bounded and
   recursively redacted.
4. New controls reject bodies above 64 KiB before auth persistence, apply the
   same absolute deadline while streaming a slow-drip body, and bind
   timestamp, nonce, method, exact path and exact body under
   HMAC-SHA256 with constant-time comparison. Authentication acceptance is an
   append-only, hash-chained durable receipt; missing, stale, tampered,
   replayed, or historically corrupt authentication returns 401/409 without a
   late write after the shared 0.9-second auth+queue deadline. Active nonce rows
   retain only the replay window; queue receipts embed the accepted auth proof,
   so safe expiry pruning does not destroy the append-only queue chain.
5. A valid control validates the component enable flag, complete incident
   lifecycle and recovery proofs, resource decision, producer pause/heartbeat,
   and the complete append-only queue receipt chain. Active, paused, shed,
   expired-resource, unavailable, or corrupt state is refused with 409.
   Queue materialization and its hashed queued/coalesced receipt commit in one
   bounded `BEGIN IMMEDIATE` transaction. They cannot mutate market facts or
   call a producer. Expired auth rows are pruned before validation, and each
   component's fully validated queue prefix rolls into a versioned checkpoint
   at 8,000 rows while retaining a bounded 1,000-receipt suffix.
   Legacy Task 6 schemas are validated and upgraded to the same proof chain in
   one idempotent transaction; an invalid legacy chain rolls back the ALTERs.
6. Discovery and Reconciliation serial loops first peek the exact queued
   nonce, then consume it only after a real producer attempt reaches a durable
   terminal state. Cancellation or crash before terminalization preserves the
   wake-up for retry; exact-nonce consumption is append-only and exactly once.
   A wake-up shortens the wait but never creates a second producer or bypasses
   incident/resource/concurrency policy.
7. Read Make targets use curl with curlrc disabled, 3-second connect and
   10-second total deadlines. Control targets fail before network without
   `POLYARB_SCAN_SHARED_SECRET`; the secret is never echoed.

## Verification

```text
Initial RED: 8 expected failures (404/auth/Make contracts)
Seven review-remediation rounds: all Important findings covered by adversarial tests
Canonical owner mutation matrix: 7 tables x INSERT/UPDATE/DELETE; 21 direct-tamper cases fail closed
Derived authority matrix: 10 feasible direct I/U/D mutations fail closed; singleton INSERT is schema-impossible
Canonical trigger matrix: 33 missing-trigger recreations plus 33 drift cases
Candidate continuity: 10,010 legal writes in 60.62s; 128-row authenticated journal window and bounded raw suffix
Discovery hot path: incremental per-group projection; no full schedule/fact scan or all-groups JSON parse
Reconciliation close/change: schedule, admission authority, projection and aggregate synchronize in one transaction
Perception package: 349 pass
Full repository: 2765 collected; 2763 pass, 1 expected xfail, 1 skip
Collection audit: 0eb4031 2586 -> 6717e48 2596 -> 2618 -> 2642 -> 2654 -> 2723 -> current 2765
Ruff changed scope: pass
compileall: pass
make docs-m1-check: pass
make planning-status: 82 plans, no drift
git diff --check: pass
```

## Remaining boundary

Task 7 owns Dashboard rendering and Task 8 owns production qualification and
cutover. All opportunity-first flags remain default-off and nothing was
deployed.
