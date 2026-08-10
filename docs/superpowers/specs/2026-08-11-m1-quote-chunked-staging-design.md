# M1 Quote Chunked Staging Design

**Status:** authorized by the standing M1 continuous-production objective

## Problem

The authoritative SQLite file is 44.24GB.  A full Quote collection contains
41,302 terminal rows.  The existing collector appends all terminal rows in one
writer transaction; runs 2669–2677 repeatedly reached that boundary and
terminalized as `child-persist-timeout`, including after the bounded writer
budget increased from 15 to 30 seconds.

## Decision

Keep `neg_risk_quote_runs.status='collecting'` as the only staging state and
append terminal quote rows in bounded chunks.  A chunk commit is never feed
visible.  `complete_run()` remains the sole certification authority: it checks
cardinality, binds the leg/quote receipt digest, marks the run complete, and
switches `neg_risk_quote_current_generation` atomically.

## Invariants

1. A collecting or failed run is invisible to `latest_complete_projection`,
   compact-feed reads, opportunity serving, and M2.
2. Every committed chunk validates each token's identity against the immutable
   `neg_risk_quote_run_legs` inventory before inserting rows.
3. A failed chunk rolls back only that chunk.  Earlier chunks remain staging
   evidence only and existing failed-payload reclamation releases them.
4. A run cannot certify unless its staged quote count exactly equals its
   requested leg count and all existing receipt checks pass.
5. Chunk progress is durably checkpointed on the existing collection attempt
   after each successful chunk, so the Fly console distinguishes active
   persistence from a stalled writer.

## Bounds

- Default chunk size is 1,000 terminal rows.
- Each chunk uses the existing independently bounded writer connection.
- The 180-second subprocess envelope remains unchanged.
- No wallet, order, signing, execution, or external write capability is added.

## Acceptance evidence

- A multi-chunk run stays invisible until `complete_run`.
- A later invalid/failed chunk never displaces the prior certified generation.
- Collection attempt progress reports committed chunks/rows.
- Existing atomic one-shot API remains available for callers that require it.
- Production demonstrates a new certified Quote, live opportunity feed, and
  recovery of the Quote incident without a child hard timeout.
