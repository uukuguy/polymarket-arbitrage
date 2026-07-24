# L3 Quiet Refresh Non-Destructive Retry Design

**Date:** 2026-07-25  
**Scope:** M1 Phase 05.4 Plan 05 production-soak repair  
**Excludes:** trading, H-009, retention cleanup, threshold relaxation

## Problem

Release 70 proved the application heartbeat and promoter startup gate, but A6
failed at health sample 35 with membership `10/10/8`. Durable runtime events
showed that the daemon itself closed generations every 30 seconds after a quiet
refresh evidence timeout.

The current refresh conflates two failures:

1. a control failure or generation change, where final wire membership is
   genuinely ambiguous and compensation is required;
2. missing post-refresh business evidence, where unsubscribe and the final
   subscribe were both sent on the same live connection, but Polymarket did not
   emit every requested initial `book` before the 25-second barrier.

Polymarket documents `book` on subscribe and `initial_dump=true`, but its CLOB
WebSocket can intermittently omit initial book data. An isolated live probe
received all ten selected books on the initial subscription and five repeated
dynamic resubscriptions in under 0.46 seconds. Production also completes many
refreshes in roughly one second, so the intermittent missing frame—not the
payload format or normal Supabase write latency—is the differentiator.

Closing the whole socket for case 2 clears truthful current-generation
membership and creates the exact sample failure the refresh was intended to
avoid.

## Selected Design

Keep one connection generation stable when only business evidence is missing.
Refresh remains evidence-strict: no timestamp, membership, or freshness value
advances unless a real `book` frame completes its required depth write.

### Initial connection grace

`WsConsumer` records the monotonic/epoch time when a successfully initialized
socket is published. If committed L3 membership exists but not all ten tokens
have current-generation evidence, `refresh_if_quiet` gives the normal initial
subscription up to `quiet_after_s` to converge. It does not immediately race
the initializer with another unsubscribe/subscribe transaction.

Connection release, compensation, or generation replacement clears all
retry-specific missing-token state. Grace is scoped to the current published
generation and cannot leak across reconnects.

### Two-stage evidence barrier

For a due stable-generation refresh:

1. install one waiter for the required token set;
2. send ordered unsubscribe then subscribe with `initial_dump=true`;
3. wait for a bounded first evidence interval;
4. if tokens remain, repeat unsubscribe then subscribe for only those missing
   identities;
5. wait for the remainder of the existing 25-second total barrier.

All control sends and identity checks remain serialized under the existing
subscription-control lock. The total barrier stays below one 30-second sample
slot.

### Failure classification

- Send failure, missing socket, or generation/connection identity change:
  preserve the existing compensation path and durable control-failure events.
- Cancellation: preserve cancellation propagation and compensation.
- Final evidence timeout after successful same-generation control sends:
  return `False`, retain the current socket/generation/committed membership,
  retain the prior evidence timestamps, and expose the exact missing identities
  only through bounded internal state. Do not emit a control-failure event and
  do not claim freshness.

The quiet loop retries the exact missing subset on its next eligible cadence.
When that subset produces real depth-write evidence, the waiter clears and
normal freshness suppression resumes.

## Invariants

- `desired`, `committed`, and `evidenced` remain distinct truths.
- A sent control message never advances book freshness.
- A failed depth write never satisfies the barrier.
- Evidence from another generation never satisfies the barrier.
- Final wire intent after every non-destructive timeout is subscribed because
  the last successful control operation is `subscribe`.
- No threshold in `AcceptanceConfig` changes.
- Existing control-send ambiguity still closes the connection.
- No A6 artifact is overwritten or extended after sample 35 failed.

## Tests

RED tests must prove:

1. a final evidence timeout after successful unsubscribe/subscribe leaves the
   live connection published and does not reserve reconnect;
2. the second stage targets only identities still missing after the first wait;
3. incomplete evidence immediately after a successful initializer is granted
   convergence grace rather than triggering refresh;
4. missing-token retry state is generation-scoped and cleared by release;
5. a real control-send failure still compensates/closes;
6. successful depth writes for every required identity still complete the
   barrier and clear retry state;
7. cancellation semantics remain unchanged.

Focused quiet-refresh, subscription, evidence, daemon, and heartbeat suites run
before full pytest, changed-file Ruff, compile, docs, planning, and image gates.

## Production Acceptance

The repair requires a new exact-SHA Fly deployment and a new DB boot. Before a
new immutable attempt:

- every promoter row is successful 5/10/10;
- at least 12 consecutive complete health samples span at least 330 seconds
  with max gap at most 75 seconds;
- the monitor includes at least two successful quiet-refresh cycles after the
  initial 60-second boundary;
- generation remains stable through those cycles;
- disallowed runtime events remain zero;
- GitHub/Fly/DB/config/mapping identities match.

Only then may a uniquely named A7 future-grid manifest be created and bound.

