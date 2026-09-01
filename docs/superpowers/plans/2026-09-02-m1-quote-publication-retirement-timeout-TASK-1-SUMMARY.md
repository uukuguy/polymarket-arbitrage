# M1 Quote publication retirement timeout — Task 1 Summary

## Outcome

Large Quote publication can now atomically advance its pointer and retire the
superseded dashboard generation without the generic ten-second read/write
statement limit cancelling the transaction.

## Implementation

- Quote certification recomputes its remaining fenced lease budget immediately
  before retiring the old research index.
- Only that retirement statement receives an extended timeout, capped at 110
  seconds and always below the actual lease expiry.
- If no safe lease budget remains, certification fails closed before deleting
  any old index rows.

## Verification

- Production diagnosis reproduced the exact server error:
  `canceling statement due to statement timeout` at the superseded Quote index
  delete.
- Focused publication/retention integration coverage and Quote factory tests
  pass after the bounded override.

## Production follow-up

Deploy the coordinator, re-run the already complete candidate through the
ordinary `certify_quote_generation` method, and verify pointer advancement,
one remaining Quote index generation, and capacity headroom.
