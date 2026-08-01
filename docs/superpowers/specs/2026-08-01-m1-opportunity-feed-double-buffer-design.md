# M1 Opportunity Feed Double-Buffer Design

**Date:** 2026-08-01  
**Status:** approved design; awaiting written-spec review  
**Scope:** Phase 05.6 Plan 02 production closure

## Problem

Structure and Quote are independently certified products. When Structure
publishes revision `N+1`, the opportunity endpoint currently rejects the still
fresh, fully certified feed for revision `N` until Quote finishes `N+1`.
Production measured a 93-second HTTP 503 window during an otherwise healthy
handoff. This turns normal refresh work into a recurring user-visible outage.

The repair must preserve fail-closed truth. It may serve one prior complete
version for a bounded handoff, but it must never combine Structure `N+1` with
quotes or opportunities from `N` as though they were one version.

## Decision

Use a one-version atomic double buffer. `QuoteWorkerRuntime` continues to hold
the last complete `CertifiedQuoteFeed` while Quote builds the next revision.
Publication of a new complete feed replaces that immutable pointer atomically.
There is no partially visible feed and no endpoint-side rebuild.

During a Structure-to-Quote handoff, the endpoint may serve the previous feed
only when all of these conditions hold:

1. the feed's Structure revision is lower than the latest complete Structure
   revision;
2. the previous quote age is at most the existing 300-second hard Quote SLA;
3. the handoff age, measured from the latest Structure completion, is at most
   300 seconds;
4. the feed's own universe age is within the existing universe SLA; and
5. the feed remains an internally consistent certified projection/opportunity
   pair.

If any condition fails, the opportunity endpoint returns HTTP 503. A feed whose
revision is greater than the latest durable Structure revision is an integrity
error and also fails closed.

## Response contract

Every successful `/arbitrage/opportunities` response adds two top-level fields:

- `refreshing`: `false` when the feed matches the latest complete Structure;
  `true` when the endpoint is serving the bounded previous revision.
- `latest_structure_snapshot_id`: the latest complete durable Structure
  revision observed for this request.

`source_snapshot_id`, `quote_run_id`, `universe_hash`, opportunity contents,
ages, and durable opportunity IDs always describe the served feed, never the
newer revision. Therefore a refreshing response makes the version difference
explicit:

```json
{
  "refreshing": true,
  "latest_structure_snapshot_id": 782,
  "source_snapshot_id": 781,
  "quote_run_id": 1450
}
```

Existing query validation and the 1-second bounded source-truth/database reads
remain unchanged. A source-truth read failure or timeout returns HTTP 503; the
endpoint does not guess whether it is refreshing.

## Health and alert semantics

The strict `/health` quote check uses the same handoff predicate as the
opportunity endpoint.

- Matching current feed: existing quote-age thresholds remain unchanged
  (`pass` below 240 seconds, `warn` through 300 seconds, then `fail`).
- Valid previous feed during handoff: `warn`, with output
  `source-snapshot-refreshing-serving-previous`; `observedValue` reports the
  age of the feed actually being served.
- Handoff older than 300 seconds, quote older than 300 seconds, stale universe,
  revision regression, or unavailable certified feed: `fail`.

Thus `/health` stays HTTP 200/warn during a normal atomic handoff and becomes
HTTP 503/fail at the existing hard boundary. Polywatch keeps its current
health-driven incident, reminder, recovery, and durable tracking behavior; the
new warning text exposes bounded refreshing without creating a false outage.

## Data flow

1. Structure atomically publishes revision `N+1` and wakes Quote.
2. Runtime continues exposing the immutable certified feed for `N`.
3. Requests read the latest complete Structure metadata and the runtime feed.
4. If the bounded handoff predicate passes, requests return feed `N` with
   `refreshing=true`; health reports `warn`.
5. Quote collects, persists, scans, and certifies one complete feed for `N+1`.
6. Runtime atomically replaces the feed pointer.
7. Subsequent requests return `N+1` with `refreshing=false`; health returns to
   its normal quote-age status.

No new database table, queue, partial projection, or multi-version history is
introduced. Durable history already exists in SQLite; the runtime pointer is
only the bounded serving cache.

## Failure behavior

- Quote collection failure does not discard a still-valid previous feed. The
  collector state remains health-visible, and the handoff hard deadline still
  forces failure after 300 seconds.
- Process restart restores only an already validated durable feed. If it is the
  previous revision and satisfies the same bounds, it may serve during refresh.
- Another Structure publication before Quote catches up changes the latest
  revision and restarts the handoff-age clock only for that actual publication;
  it does not refresh the old quote's own 300-second age. The old feed therefore
  cannot be kept alive indefinitely by repeated Structure publications.
- A partial or mismatched projection/opportunity pair is never published and
  never served.

## Verification

Test-driven implementation must prove:

1. a matching feed returns HTTP 200 with `refreshing=false`;
2. a fresh previous feed returns HTTP 200 with `refreshing=true`, keeps all
   served IDs bound to the previous revision, and performs no rebuild;
3. the runtime feed switches as one projection/opportunity pair after the new
   certification;
4. quote age and handoff age are accepted at exactly 300 seconds and rejected
   above 300 seconds;
5. stale universe, missing truth, source regression, read timeout, and invalid
   feed all return HTTP 503;
6. health reports the serving quote age and the explicit refreshing warning,
   then fails at the same hard boundary; and
7. the full repository test, lint, planning-status, and documentation gates
   remain clean.

Production acceptance requires observing a natural Structure publication from
before the revision changes until its matching Quote completes. During that
window the opportunity endpoint must never return 503, must first expose the
old/new revision pair with `refreshing=true`, then atomically expose the new
revision with `refreshing=false`. Strict health must remain HTTP 200 throughout
the successful handoff, and Polywatch must retain no open incident afterward.

## Rejected alternatives

- **Immediate invalidation:** preserves the current recurring 503 window and
  fails the continuous-production requirement.
- **Partial new feed:** reduces latency by mixing certification states and can
  produce opportunities against incomplete membership truth.
- **Unbounded stale serving:** hides a stuck Quote worker and defeats both the
  300-second SLA and automatic alert/recovery tracking.
