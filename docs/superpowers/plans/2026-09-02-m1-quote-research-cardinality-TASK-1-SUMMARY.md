# M1 Quote research cardinality — Task 1 Summary

## Outcome

Quote research-index completeness is measured against the admitted candidate
universe, not only successful CLOB book responses.

## Why

The Quote manifest record count intentionally counts successful responses.
The business view additionally retains terminal records such as `missing-book`,
which are valuable evidence for explaining why a market did not become an
opportunity. Comparing those different units falsely marked a complete view as
incomplete.

## Implementation and verification

- The Quote page derives its expected research-row count from the sum of
  `m1_quote_batch_inputs.leg_count` for the current generation, falling back
  to the legacy manifest count only when no admitted-leg metadata exists.
- RED: a one-success-response manifest with two admitted legs and two research
  rows was rejected.
- GREEN: it is available, while the separate incomplete-index regression still
  rejects a genuine row shortfall. Structure uses its one-to-one normalized
  record count unchanged.
