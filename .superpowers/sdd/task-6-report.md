# Task 6 Implementer Report

Status: DONE — ready for independent review

## Scope

Task 6 only: bounded read-only perception APIs, replay-resistant HMAC operator
wake-ups, durable coalescing queue evidence, real serial producer consumption,
cloud Make entries, and manual synchronization. No Dashboard, deployment,
wallet, signing, balances, orders, or real-money execution.

## Truth chain

1. All six `/perception/*` handlers run behind a one-second thread deadline.
   Their SQLite connections use URI `mode=ro`, `query_only=ON`,
   `busy_timeout=250`, and an 0.8-second progress abort. Limits accept only
   canonical integers from 1 through 500.
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
   HMAC-SHA256 with constant-time comparison. Nonces are durable, expire
   incrementally, and survive restart; missing, stale, tampered or replayed
   authentication returns 401.
5. A valid control checks the component's existing enable flag and full
   incident history, refusing disabled/escalated/corrupt state with 409. One
   `BEGIN IMMEDIATE` transaction writes only a coalescing queue flag and
   append-only receipt. It cannot mutate market facts or call a producer.
6. Discovery and Reconciliation serial loops atomically consume the wake-up
   during their normal cadence. A wake-up shortens the wait but never creates
   a second producer or bypasses resource/concurrency policy.
7. Read Make targets use curl with curlrc disabled, 3-second connect and
   10-second total deadlines. Control targets fail before network without
   `POLYARB_SCAN_SHARED_SECRET`; the secret is never echoed.

## Verification

```text
Initial RED: 8 expected failures (404/auth/Make contracts)
Focused API/control/Make: 10 pass
Task 3/4/5 chain-focused suite: 87 pass
Proportional perception/health/wiring/manual suite: 293 pass
Full repository: 2629 collected; 2627 pass, 1 expected xfail, 1 skip
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
