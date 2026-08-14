# M1 Event-Rooted Structure Source Design

## Problem

The transactional Structure source worker authenticated individual Gamma pages,
but treated the global active-market keyset as the market half of one atomic
window. In staging this request stream exceeded 1,000 pages. The page ceiling
correctly quarantined the window, but a later cadence could begin the same
unbounded traversal again. A page limit is a safety brake, not a source scope.

## Decision

An M1 Structure source window is rooted in a terminally collected active-event
stream. Its market authority is the deterministic set of open member market
IDs contained in those frozen event artifacts, not the global `/markets/keyset`
feed.

The event stream remains cursor-backed. When it records its terminal receipt,
the control plane re-authenticates its already stored page artifacts, derives
the unique open member IDs using the existing event projection semantics, sorts
them lexicographically, and partitions them into fixed-size batches. Each
batch becomes exactly one fenced `structure-fetch` job with an immutable ID
list and SHA-256 identity. The number of batches is known before the first
market request starts.

Each market batch calls Gamma's existing exact-ID list endpoint:

```
GET /markets?id=<id>&id=<id>...&limit=<batch-size>
```

The returned IDs must be an exact set match to the admitted batch. Duplicate,
unknown, missing, malformed, inactive, closed, or archived members cause the
fenced job and source window to be quarantined. A complete batch is stored as
the same authenticated R2 page artifact already used by the materializer.

## Data Contract

The next additive migration extends `m1_structure_source_page_inputs` with
`market_ids_json TEXT NULL` and `market_ids_digest TEXT NULL`.

- Events inputs retain both fields as `NULL` and keep their opaque
  `requested_cursor` semantics.
- Markets inputs require a canonical JSON array of non-empty, strictly sorted,
  duplicate-free IDs and a 64-character SHA-256 digest of those bytes.
- A market input has no cursor. Its durable job identity remains
  `window_key:fetch:markets:<ordinal>`; the digest authenticates the exact
  upstream identity assigned to that ordinal.
- The repository authenticates an existing input on conflict. It must never
  replace a batch after admission.

The page-artifact header gains an optional `market_ids_digest` field. This
binds the R2 receipt to the batch, allowing the materializer to reject a page
whose database input and artifact header disagree.

## State Flow

```text
events cursor pages ──terminal──> derive + persist market batches
                                      │
                                      ▼
                            exact-ID market batch jobs
                                      │ all receipts
                                      ▼
                              source window complete
                                      │
                                      ▼
                        existing materialize → ranges → certify → shadow
```

The first terminal event checkpoint and all market job inserts occur in one
fenced PostgreSQL transaction. A crash before commit leaves the event lease
reclaimable; a crash after commit leaves the same immutable batch set for any
replacement worker. A terminal market receipt moves the window to `complete`
and admits its existing materializer exactly once. No partial batch set is
materializable.

## Limits and Failure Handling

- `market_batch_size` defaults to 25, matching Gamma's established exact-ID
  request bound.
- `max_market_batches` defaults to 1,000. Exceeding it quarantines before any
  market job is admitted; it cannot become another long cursor traversal.
- The existing `max_pages` guard remains for the event cursor stream and for
  defence in depth. It remains a quarantine path, not a normal completion path.
- A source quarantine records `StructureSourcePageLimitError`,
  `StructureSourceBatchLimitError`, or the concrete exact-ID validation error.
  It creates no source bundle, ranges, certifier work, or publication pointer.
- A completed source window blocks later windows until materialization starts
  under the existing transactional job chain; quarantined windows do not block
  later cadence admission.

## Compatibility and Boundaries

This is additive to the staging control plane and deliberately does not alter
legacy SQLite Structure/Quote collection, existing publication pointers,
Telegram, or production L1/L2 workers. Old cursor-based market jobs stay
quarantined and are never reinterpreted as batches. New scoped windows use the
new input form exclusively.

## Verification Requirements

1. Unit tests prove deterministic member extraction, canonical batch identity,
   exact-ID response validation, and artifact digest binding.
2. Real Postgres tests prove terminal event receipt atomically creates stable
   batches; replay and replacement leases cannot change them; final batch
   atomically releases exactly one materializer job.
3. Worker tests prove no global market-keyset call occurs for a scoped market
   input, and a mismatched response quarantines before receipt/materialization.
4. Staging proves one source window reaches terminal `complete`, emits an R2
   bundle, ranges, certification, and a zero-live-pointer shadow result.
5. A controlled worker restart during market batches resumes only unfinished
   ordinal jobs and preserves the final bundle digest.

## Spec Self-Review

No placeholder implementation steps remain. The only upstream dependency is
the existing exact-ID Gamma endpoint already used by `fetch_market_states` and
parent reconciliation. The design has one source authority, one durable input
form per stream, and explicit terminal/failure states. It is deliberately
limited to Structure source scoping; Quote migration and final soak remain
separate objective steps.
