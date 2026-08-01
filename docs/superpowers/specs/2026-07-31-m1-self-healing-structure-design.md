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

## Task 5 refinement: bounded comparison authentication

An authenticated generation cannot become `ready` until it owns a durable,
sealed comparison receipt. Normal publication and legacy backfill use the same
bounded comparison state machine; neither pointer publication nor a hot reader
may scan Structure rows.

The comparison state machine pins the exact current legacy snapshot identity
before reading either side. Every later invocation revalidates that identity
and rejects drift. It advances one of four keyset phases—legacy universe,
generation universe, legacy rejections, generation rejections—and reads no more
than `max_rows` in one invocation. Cursor, row count, digest state, and phase
advance with compare-and-swap semantics so a crash or competing worker can
repeat a chunk but cannot skip or combine it.

Comparison hashes retain the existing canonical byte framing exactly: JSON list
open/close bytes, comma placement, tuple serialization, universe-hash prefix,
and rejection list framing do not change. Incremental progress persists a small
pure SHA-256 state containing the eight 32-bit state words, total byte count, and
at most 63 uncompressed tail bytes. It does not use pickle, OpenSSL internal
state, a new hash algorithm, or unbounded prefix storage. Tests must prove
NIST/hashlib vectors, malformed-state rejection, resume at every tail boundary
and store reopen, deterministic framing, and equality with the existing one-shot
`hashlib.sha256` result across empty, boundary, and randomized chunk partitions.
If exact equality cannot be established, publication remains blocked rather than
changing the canonical digest.

After all four phases, one transaction verifies the pinned identities and
counts, seals `structure_generation_comparison_receipts`, and only then marks
the publication `ready`. `receipt_digest` is the SHA-256 of a canonical encoding
of every receipt identity, count, universe/source-truth hash, generation
validation hash, and creation timestamp. Readers recompute it and cross-check
generation count against the resolved generation snapshot/pointer and legacy
count against the exact legacy snapshot metadata. Sealed receipts reject
arbitrary update or delete. The pointer records the sealed receipt digest;
pointer switching verifies metadata and the digest only and performs no full
count or hash scan.

Schema initialization repairs a literal pre-Task-5 pointer only when all four new
authentication fields—validation hash, component counts, certification marker,
and comparison receipt digest—are NULL and its publication and snapshot prove the
frozen identity. A sealed receipt copies all four facts atomically. If no receipt
exists, initialization copies the first three facts and atomically creates durable
bounded-comparison progress; generation is usable while compare reports
`comparison-receipt-missing`, and generic backfill may only continue that exact
provenance until it atomically binds the digest. Every fabricated mixed
NULL/non-NULL state, conflicting value, or unverifiable reference remains unchanged
and fail-closed. Both repair forms are idempotent.

Generic snapshot retention never reclaims immutable generation evidence. Its
bounded candidate query excludes snapshots referenced by the current pointer,
publication metadata, comparison progress or receipts, sync-window publication,
or any generation component row. Keep-set selection, the complete evidence
exclusion query, and deletion share one `BEGIN IMMEDIATE` transaction, preventing
a new evidence writer from acquiring a selected snapshot before deletion. Purge
can continue deleting unrelated snapshots without touching a sealed receipt or
depending on a foreign-key rollback. Old
generation reclamation requires a separate bounded, evidence-aware cleanup API
with explicit chain ownership; that API must be implemented and exposed before
production closure and is outside Task 5.
