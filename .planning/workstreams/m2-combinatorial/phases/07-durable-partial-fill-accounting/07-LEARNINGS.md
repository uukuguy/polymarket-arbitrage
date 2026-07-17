# Phase 7 Learnings — Durable Partial-Fill Accounting

## What changed our understanding

1. Partial-fill safety is primarily an identity problem. Exact arithmetic still double
   books if the same venue fact can enter under two caller operation IDs.
2. Remaining quantity and remaining cost basis are authority, not presentation fields.
   Persisting them directly makes overfill validation and restart semantics local.
3. Proportional rounding is safe only when terminal allocation consumes the residual.
   Repeating the same rounded fraction to the end can leak micro-cash dust.
4. Response-loss tests must discard a real subprocess response after commit. Calling a
   repository twice in one process does not prove independent recovery.
5. Engine memory is not the deduplication boundary. A transactionally stored receipt is
   the only fact that survives crash, retry, and a new process.

## Patterns to reuse

- Map immutable external event identity to one canonical internal operation key.
- Store post-event remaining authority in the same transaction as the receipt.
- Test intermediate residual state and terminal conservation, not only final PnL.
- Preserve paper-model boundaries explicitly so venue-truth work can replace cash facts
  without rewriting quantity/idempotency contracts.

## Adversarial decision questions

1. The first 30-share fill committed but the response timed out. Which exact repository
   key determines whether retry mutates again?
2. Why must a final 1-micro-share fill consume all remaining cost basis?
3. When may an anonymous full close remain compatible while an anonymous partial fill
   must fail?
4. If one fill ID appears for a different market, why is returning zero more dangerous
   than an operation target conflict?
5. Which H-005 fields remain authoritative when H-006 supplies actual cash and fees?

## Next hypothesis boundary

H-006 introduces exact venue-confirmed shares/cash/fees and reconciliation. It must not
weaken canonical fill identity, remaining Quantity, atomic receipt replay, or fail-closed
overfill behavior.
