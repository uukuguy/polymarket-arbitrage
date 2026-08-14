# M1 Scoped Structure Source Throughput Design

## Problem

An event-rooted Structure window makes its independent exact-ID market batches
durable in PostgreSQL, but the scheduler currently invokes one source worker
turn per two-second tick. A live scope that exceeds 1,000 batches therefore
cannot converge within its five-minute cadence and is not a continuously
operable control plane.

## Decision

Use a bounded in-process market-source lane pool of eight independently named
workers on the existing staging machine. The pool is invoked as one scheduler
stage and performs up to eight concurrent `run_once()` calls only when the
claimed input is a scoped `markets` batch.

Event cursor work remains serialized: it has one opaque continuation and only
one runnable event job exists. Materialization, range normalization,
certification, and Quote stages retain one turn per scheduler cycle.

## Data Flow and Fencing

1. Each lane uses a distinct `worker_id` suffix and claims through the existing
   PostgreSQL lease transaction.
2. A lane that finds no runnable job returns `idle`; no shared in-memory cursor
   or SQLite state is introduced.
3. Every successful exact-ID fetch still uploads and HEAD-verifies R2 evidence
   before its receipt transaction. Receipt conflicts, lease loss, and Gamma
   failures retain existing retry/quarantine behavior.
4. The pool is bounded at eight concurrent remote calls. It does not change
   the 25-ID exact-request cap, 5,000-batch hard scope cap, materializer gate,
   or publication-pointer policy.
5. A terminal materializer remains runnable only after every admitted market
   batch has a receipt; parallel fetch completion order is deliberately
   non-semantic.

## Scheduler Contract

The source pool returns an aggregate turn result that names all claimed job
keys and outcomes. The scheduler emits that one aggregate stage once per tick,
so the existing ordered control-plane cycle and local overlap lock remain
intact. An aggregate failure is isolated to its lane and is recorded through
the existing incident path; it cannot cancel sibling lanes or downstream
workers.

## Validation

- Unit tests prove event work runs once while scoped market batches fill up to
  eight lanes concurrently.
- Postgres integration proves distinct leases and durable receipts survive a
  mid-pool restart, with no duplicate page receipt and no early materializer.
- Staging evidence proves a new source window transitions from events to
  concurrent exact-ID market receipts, then materializes and produces a
  shadow-only Structure generation with zero live pointer mutations.

## Non-goals

- No additional Fly machines, production deployment, Telegram change, Quote
  migration, or pointer promotion.
- No unbounded fan-out: both the lane count and source scope remain explicit
  hard limits.
