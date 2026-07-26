# L3 Continuity Boundary Repair Design

**Date:** 2026-07-26
**Workstream:** `m1-perception`
**Scope:** Repair the two production L3 continuity failures observed during M1
operational closure without weakening the locked acceptance thresholds.

## Outcome

L3 market rotation and quiet-market refresh must preserve a truthful, complete
five-market/two-token evidence surface at every strict health sample:

1. A promoter run may not report success while fewer than 10 target tokens have
   current-generation durable book evidence.
2. Replacing one market must not expose a partially transitioned mapping to the
   sampler.
3. A quiet committed token must receive a durable book refresh before its age
   reaches the locked 120-second failure boundary.
4. A refresh that cannot collect all required evidence must be persisted as a
   runtime failure and force recovery of the captured WebSocket generation.
5. `/health`, durable evidence, Polywatch, and the promoter ledger must describe
   the same identities and failure reason.

The existing L1 opportunity-quote 24-hour interval continues while this repair
is implemented and deployed. An L2-only deployment must not restart the L1 app
machine or reset its quote evidence anchor. Because L3 behavior changes, a new
L3 continuity evidence window begins after the repaired L2 release is live.

## Production Evidence

The design is based on persisted production rows, not only the HTTP response:

- At `2026-07-26T06:03:31Z`, promoter run 450 selected five markets and changed
  two token identities (`add_count=2`, `remove_count=2`). It reported
  `status=success` with only `evidenced_count=8`.
- At `2026-07-26T06:03:36Z`, sample 4500 saw desired 10, committed 10, but
  evidenced 9 and failed with `membership_convergence_failed`.
- At `2026-07-26T06:04:02Z`, the next sample had 10/10 evidence and passed.
- At `2026-07-26T06:12:00Z`, strict health reported only 2/5 current market
  rows and a worst persisted market age above 120 seconds.
- At `2026-07-26T06:13:02Z`, all five rows passed again. There was no WebSocket
  reconnect or subscription-control failure recorded during this interval.
- The resident two-minute Polywatch detected both failures, delivered Telegram
  alerts, detected both recoveries, and delivered recovery messages.

These facts prove that monitoring works, while the transition and refresh
contracts remain too weak for continuous strict health.

## Approaches Considered

### A. Transactional transition plus deadline recovery

Stage the next mapping, obtain durable evidence before publication, then expose
the new mapping and membership at one commit boundary. Treat incomplete quiet
refresh as a failed WebSocket generation and reconnect before the 120-second
deadline.

- Advantage: preserves the existing strict definition and removes both known
  gaps at their source.
- Cost: requires an explicit staged-evidence boundary instead of reusing
  partially committed membership as transition state.

### B. Add a health grace period

Ignore membership/freshness failures for a bounded interval after promotion or
refresh.

- Advantage: small code change.
- Disadvantage: changes a real coverage gap into a false pass and makes the
  durable evidence untrustworthy.

### C. Raise the freshness threshold or debounce alerts

Increase 120 seconds to 180 seconds, or require several failed monitor ticks.

- Advantage: fewer visible 503 responses.
- Disadvantage: delays detection without repairing missing data and violates
  the locked Phase 05.4 acceptance configuration.

**Decision:** Approach A. Thresholds and first-failure alerting remain unchanged.

## Design

### 1. Staged market rotation

The promoter separates a proposed target from the currently published mapping.
The current mapping remains the sampler's truth until the target is ready.

For every changed token pair:

1. Build and validate the five-market/10-token target without mutating published
   desired, committed, or mirrored state.
2. Register a generation-scoped evidence barrier for the target tokens. This
   barrier makes depth writes eligible for newly selected tokens even while the
   old mapping remains active.
3. Request initial book dumps and require successful durable book-level writes
   for all 10 target tokens.
4. If the barrier completes on the same WebSocket generation, commit the target
   mapping, desired set, committed set, and evidenced identities together.
5. Remove identities that are no longer needed only after the new target is
   committed. Candidate subscriptions may keep an old token in the union, but
   it is no longer part of L3 truth.

Pre-staged evidence is bounded by generation and target identity. A generation
change, target change, timeout, or failed durable write discards it. Evidence
from an old socket can never seed a new mapping.

A promoter run is `success` only when its terminal row has:

- selected markets = 5;
- desired = committed = evidenced = target tokens;
- all three counts = 10;
- mapping hash equal to the committed mapping;
- mirror reconciliation complete.

Otherwise it records a specific failed reason and leaves the last fully
converged mapping published.

### 2. Quiet-market deadline recovery

The quiet refresher continues to start from successful durable book evidence,
not transport frames or outbound control messages.

- Normal refresh begins when the oldest committed L3 evidence reaches 60
  seconds.
- The existing bounded first attempt and missing-token retry remain.
- If the evidence barrier is still incomplete, the failure is no longer
  returned as a harmless `False` on the same socket. The captured generation is
  marked failed, its socket is closed, and the normal consumer reconnect path
  requests a complete initial dump.
- Only the captured socket may be closed; a replacement generation must never
  be affected by an old timeout.
- Recovery must either restore all 10 identities before 120 seconds or leave
  strict health failed with a persisted reason. It must never forge freshness.

The reconnect policy is a recovery action on an already ambiguous market-data
generation. It does not restart the Fly machine or L2 process.

### 3. Chain-truth observability

Every failed staging or quiet-refresh barrier emits a bounded
`subscription_control_failed` runtime event containing:

- operation (`promotion_stage` or `book_refresh`);
- reason code;
- WebSocket generation;
- required count and missing count;
- no token IDs, credentials, or raw exception text.

The same failure updates the process-local evidence state consumed by strict
health. The durable sampler then records the exact failed count/reason, and
Polywatch alerts from that strict check. This preserves the chain:

`receive/write result → generation evidence → durable sample → /health → Telegram`.

No separate log-only success or failure source is introduced.

## Failure Handling

- Selection underfill, staging timeout, durable-write failure, generation
  change, mirror failure, or atomic-commit mismatch leaves the previous
  converged mapping active.
- A quiet-refresh evidence timeout closes only its captured socket and lets the
  existing supervisor reconnect.
- A failed reconnect remains visible through existing WebSocket and L3 strict
  checks; no loop marks data fresh merely because it sent a subscription.
- Repeated failures remain subject to Polywatch's existing duplicate
  suppression and 30-minute reminder policy.
- No automatic Fly restart, threshold adjustment, alert-recipient change, or
  trading action is added.

## Testing

Implementation follows test-first red/green/refactor cycles.

1. A promoter rotation test reproduces `10 desired / 10 committed / 8 or 9
   evidenced` and proves the new mapping is not published or marked successful.
2. A successful staged rotation proves the old mapping remains readable until
   all target book writes succeed, then changes atomically with 10/10 evidence.
3. A generation change during staging proves old evidence is discarded.
4. A quiet-refresh timeout test proves the captured socket is closed, a runtime
   failure event is recorded, and a replacement socket is never closed.
5. A recovery test proves a complete reconnect initial dump restores 10/10
   without modifying timestamps in the send path.
6. Health-chain tests prove incomplete staging and stale markets return 503,
   while the recovered state passes with the locked 120-second threshold.
7. Existing WebSocket transaction, evidence, sampler, health, promoter,
   Polywatch, Ruff, and full pytest suites remain green.

## Production Verification

1. Build and verify the exact candidate image.
2. Deploy only `polyarb-l2`; preserve the L1 machine identity and quote anchor.
3. Confirm release ID, machine/image identity, 10/10 membership, 5/5 market
   freshness, durable sampler cadence, and resident Polywatch state.
4. Observe at least one real promoter boundary and one quiet-refresh boundary
   with no strict-health failure.
5. Start a new exact 24-hour immutable L3 evidence window on the repaired
   release, using the existing Phase 05.4 acceptance contract, and retain the
   current L1 quote interval in parallel. The earlier A7 verdict remains valid
   for its old release but cannot validate newly changed continuity behavior.
6. Close M1 only when the required L1 interval and repaired L3 evidence verdict
   are both passing and the remaining operational checks are green.

## Out of Scope

- Relaxing the 120-second freshness requirement.
- Debouncing first failure alerts.
- Restarting L1 or resetting its quote interval.
- Changing market ranking or strategy selection.
- Trading, wallet access, or order placement.
