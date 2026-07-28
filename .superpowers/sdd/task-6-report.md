# Task 6 Implementer Report

Status: REVIEW REMEDIATION COMPLETE — awaiting independent re-review

## Scope

Task 6 only: bounded read-only perception APIs, replay-resistant HMAC operator
wake-ups, durable coalescing queue evidence, real serial producer consumption,
cloud Make entries, and manual synchronization. No Dashboard, deployment,
wallet, signing, balances, orders, or real-money execution.

## Truth chain

1. All six `/perception/*` handlers run behind a one-second thread deadline.
   Their SQLite connections use URI `mode=ro`, `query_only=ON`,
   `busy_timeout=250`, and an 0.8-second progress abort. Limits accept only
   canonical integers from 1 through 500. Every endpoint reads one SQLite
   snapshot; page/history JSON and response bytes are bounded before
   serialization, and group/history endpoints expose stable cursors.
2. Status validates current Candidate fact → current certified revision →
   complete Quote batch → atomic success receipt/hash before counting an edge.
   Valid count zero is `available/no-certified-edge`; corrupt Candidate or
   incident evidence is HTTP 503 `unavailable`.
3. Group list/history validate membership digests and revision ordering.
   Discovery and Reconciliation reuse the Task 3/4 full-history validators.
   Incidents validate lifecycle ordering plus Task 5 component-specific
   recovery proof. Returned incident evidence is size/depth bounded and
   recursively redacted.
4. New controls bind timestamp, nonce, method, exact path and exact body under
   HMAC-SHA256 with constant-time comparison. Authentication acceptance is an
   append-only, hash-chained durable receipt; missing, stale, tampered,
   replayed, or historically corrupt authentication returns 401/409 without a
   late write after the handler deadline.
5. A valid control validates the component enable flag, complete incident
   lifecycle and recovery proofs, resource decision, producer pause/heartbeat,
   and the complete append-only queue receipt chain. Active, paused, shed,
   expired-resource, unavailable, or corrupt state is refused with 409.
   Queue materialization and its hashed queued/coalesced receipt commit in one
   bounded `BEGIN IMMEDIATE` transaction. They cannot mutate market facts or
   call a producer.
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
Review remediation: all 6 Important findings covered by adversarial tests
Focused API/control: 19 pass
Proportional perception/health/wiring/Make suite: 321 pass
Full repository: 2596 collected; 2594 pass, 1 expected xfail, 1 skip
Collection audit against 0eb4031: 2586 -> 2596 (+10 tests, no test deletion)
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
