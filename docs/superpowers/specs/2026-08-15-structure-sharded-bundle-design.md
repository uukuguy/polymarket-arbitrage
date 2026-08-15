# Sharded Transactional Structure Bundle Design

## Problem

The event-only Structure source sealed 208 immutable R2 pages correctly, but
the materializer concurrently decoded the complete window and built one
in-memory bundle. In staging its RSS reached 1.7GB and outlived its lease.
Increasing VM memory would preserve a single-process failure domain and would
move the same memory fault to range workers, which currently read the full
bundle again.

## Decision

Replace the monolithic source bundle with an authenticated manifest and
component shards. A shard is a content-addressed R2 NDJSON artifact containing
one bounded canonical slice of a component. The manifest names every shard,
its digest, its component, and its stable ordinal. The manifest is the existing
Structure bundle identity for admission, range planning, certification, and
Quote admission.

The materializer claims its existing `structure-materialize` job and processes
at most a fixed number of sealed source pages per turn. It uploads only the
resulting shard(s), then atomically records `checkpoint_cursor` and a digest of
the ordered shard receipt set using the existing fenced checkpoint API. A new
lease resumes at the next source-page ordinal. No process-local state is
required for recovery.

When every page shard is checkpointed, a final bounded turn writes the
manifest. It atomically admits the manifest and component/range jobs under the
existing source-window fence. Range workers load only the shard(s) named by
their range, not a whole market universe.

## Invariants

- A checkpoint receipt is fenced by `(job_key, lease_epoch)` and names one
  immutable R2 shard digest; a stale worker cannot advance or replace it.
- The manifest digest commits the ordered `(component, ordinal, shard digest)`
  receipt set plus the existing source-page receipt digest.
- Every input page is processed exactly once logically. Retried PUTs with the
  same digest are idempotent; a different digest for the same cursor is a
  conflict.
- No Structure or Quote pointer can move until all expected source-page shards
  exist, are R2 HEAD-verified, and the manifest is committed.
- Memory is bounded by `materializer_page_batch_size` plus one shard/range;
  workers never deserialize the full source window.
- The old v1/v2 monolithic parser remains readable for already-created
  artifacts; new artifacts use explicit source kind
  `gamma-source-window-events-v3-sharded`.

## Data Flow

```text
sealed R2 event pages
  -> fenced materializer lease (N pages)
  -> normalized component shards in R2
  -> Postgres checkpoint receipt
  -> repeated takeover-safe batches
  -> authenticated shard manifest
  -> component/range jobs reading only referenced shards
  -> certification -> transactional Quote batches
```

## Failure Handling

- R2 read/PUT/HEAD failure marks only the claimed turn retryable; no partial
  checkpoint is trusted.
- A process loss after upload but before checkpoint reuploads/HEAD-verifies the
  same content-addressed shard and commits it under the next lease.
- A malformed source page, normalization mismatch, duplicate shard ordinal, or
  missing expected shard quarantines the source window and leaves publication
  pointers unchanged.
- A range worker validates the manifest and every referenced shard digest before
  normalizing; it fails closed on a missing or altered shard.

## Acceptance Evidence

1. Unit and real-Postgres contracts prove checkpoint/takeover/idempotency and
   no monolithic bundle read by a range worker.
2. A staging window larger than 200 pages completes with bounded RSS below the
   2GB VM limit, advances through at least one intentional restart, and creates
   an authenticated manifest.
3. Structure ranges certify and the Quote worker consumes the resulting
   transactionally admitted universe; publication pointers remain isolated until
   the existing promotion gate is explicitly satisfied.
