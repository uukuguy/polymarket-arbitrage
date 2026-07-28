# Task 6 Implementer Report

Status: CONTINUITY REMEDIATION COMPLETE — awaiting independent re-review

## Scope

Task 6 only: bounded read-only perception APIs, replay-resistant HMAC operator
wake-ups, durable coalescing queue evidence, real serial producer consumption,
cloud Make entries, and manual synchronization. No Dashboard, deployment,
wallet, signing, balances, orders, or real-money execution.

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
   Validation then checks the checkpoint and replays a bounded suffix. A
   10,010-success continuity test, checkpoint/suffix tampering, membership
   supersede across the boundary, and delete-failure rollback all pass.
   Valid count zero is `available/no-certified-edge`; corrupt Candidate or
   incident evidence is HTTP 503 `unavailable`.
3. Group list/history validate the same event/revision/time/status transition
   chain, not only membership digests or ascending revision numbers.
   Discovery and Reconciliation reuse the Task 3/4 full-history validators.
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
Four review-remediation rounds: all Important findings covered by adversarial tests
Focused API/control: 41 pass
Candidate/control/HTTP regression: pass
Full repository: 2618 collected; 2616 pass, 1 expected xfail, 1 skip
Collection audit: 0eb4031 2586 -> 6717e48 2596 -> current 2618
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
