# L3 promoter startup-connection gate

**Date:** 2026-07-24  
**Scope:** M1 Phase 05.4 Plan 05 recovery after rejected release-68 boot

## Problem

Release 68 boot `e542fd4c-8d74-4d79-9e64-33dfc719a8f2` started the promoter
and WebSocket consumer as sibling tasks. Promoter run 0 began at
`15:22:06Z`; WebSocket generation 1 was not initialized until `15:22:07Z`.
The promoter truthfully persisted `failed/generation_changed`, after which
membership stayed 0/10 for the observed samples. This boot can never satisfy
the strict all-ticks-successful contract.

Task creation order does not establish asyncio execution order, so another
restart could reproduce the same race.

## Considered approaches

1. **Gate only the first promoter run on an active socket (selected).** Expose
   a read-only `WsConsumer.has_active_connection` property. Before scheduler
   run 0, wait cancellably until it is true. Keep run 0's `scheduled_at` on the
   immutable boot grid so real startup lag remains observable.
2. **Retry or reinterpret `generation_changed`.** Rejected. A generation change
   after a tick begins is ambiguous control truth and must remain a durable
   failure.
3. **Restart until task scheduling happens to work.** Rejected. It does not
   repair the race and produces non-deterministic boot eligibility.

## Boundaries

The gate applies once, before the boot's first promoter transaction. After the
socket becomes active, all later disconnects and generation changes retain
their current strict behavior. If startup never becomes ready, cancellation is
prompt and no success row is fabricated; the boot remains ineligible through
missing readiness/evidence.

No sampler threshold, acceptance config, event allowlist, trading path,
retention path, or H-009 behavior changes.

## Tests

A failing scheduler test must prove that run 0 is not invoked while the active
socket property is false, then is invoked exactly once after it becomes true
with the original boot-grid `scheduled_at`. A second test must prove shutdown
while waiting is prompt and emits no promoter row. Existing
`generation_changed` terminal tests must remain green.
