# M1 Self-Healing Structure Synchronization Design

**Date:** 2026-07-31  
**Status:** approved continuation of the 2026-07-27 data-layer design  
**Hypothesis:** H-011

## Incident fact

The deployed Structure process still materializes the entire active Gamma event
universe in one child process.  Production recorded `snapshot_id=764` roughly
3.7 days ago; later children time out in `gamma-events` at about 247 seconds.
After five failures `SnapshotScheduler` enters `PAUSED`, so the alerting path
continues to report the incident while the producer has no route back to a
successful run.  Raising the timeout only lengthens the outage and does not
prove recovery.

## Decision

Structure is one *atomic publication* assembled from many bounded, durable
steps.  A step fetches no more than one Gamma page (and its bounded validation
work), commits its opaque continuation plus staged facts, then exits.  A restart
continues the same window.  Only a completed window passes the existing source
coverage and membership checks and replaces `markets`; readers never consume a
partial window.

The scheduler may enter `RECOVERING` after repeated failures, but it keeps
attempting bounded steps with a persisted backoff.  `RECOVERING` is observable,
alerted, and self-clears only after an atomic certified publication.  It is not
a synonym for accepting stale data: consumers remain fail-closed once the
published revision exceeds 30 minutes.

## Data contract

`structure_sync_windows` is the durable authority for a single target universe:

| Field | Meaning |
|---|---|
| `id`, `status`, `started_at_ms`, `updated_at_ms` | identity and lifecycle (`running`, `ready_to_publish`, `failed`) |
| `event_cursor`, `market_cursor`, `stage` | opaque upstream continuation and current bounded step |
| `event_pages`, `market_pages`, `last_error` | progress and operator evidence |
| `base_snapshot_id` | last certified truth observed when the window began |

Staged events and markets are keyed by `(window_id, upstream_id)`.  Their
upserts are idempotent.  A unique active-window constraint prevents two local
writers from mixing pages.  A terminal failure retains facts for diagnosis;
the next bounded attempt resumes it unless its cursor proof is invalid, in which
case a new window begins and the old one is retained as failed evidence.

## Safety and recovery invariants

1. A page commit stores either all its validated items and the exact successor
   cursor, or neither.
2. The upstream cursor is opaque; it is never parsed, generated, or advanced
   before the page transaction commits.
3. `markets` changes only in the final transaction, after every event and
   market page completed and existing reconciliation validators pass.
4. A crash at any point can cause a page to be fetched again, never skipped;
   idempotent staged upserts make that safe.
5. Repeated errors move the scheduler to `RECOVERING`, not terminal `PAUSED`.
   Backoff is bounded and every next-attempt time is persisted and health-visible.
6. Alert incidents are per component: failure, periodic reminder, recovery, and
   published-revision confirmation are separate facts.  One component's ongoing
   failure cannot suppress another's recovery notice.

## Rejected alternatives

- **Increase child timeout / memory:** masks an unbounded critical path and
  leaves restart recovery undefined.
- **Publish each fetched page:** produces a partial market universe, breaking
  M2 neg-risk membership truth.
- **Keep `PAUSED` with auto-unpause:** replays the same universe-sized job and
  creates a silent gap between an alert and an actual recovery attempt.

## Acceptance evidence

Local tests must prove page transaction atomicity, restart continuation, retry
after the former pause threshold, no partial publication, and recovery only on
certified publication.  Production qualification begins only after deployment:
a timestamped release ID, one complete new Structure revision, independent
Quote completion bound to it, and a continuous 24-hour observation with
Structure age <= 30 minutes and Quote age <= 300 seconds.
