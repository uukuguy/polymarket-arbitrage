# M1 business research integrity — Task 1 Summary

## Outcome

The Structure and Quote business-research endpoints no longer present a
partial current index as a complete business view.

## Implementation

- Each page reads the certified manifest record count and the number of rows
  materialized for that same current generation before returning any items.
- A mismatch returns `research-index-incomplete`, together with
  `expected_record_count` and `materialized_record_count`.
- Normal current pages remain unchanged when the counts match.

## Verification

- RED: a manifest declaring two records with one staged row was returned as an
  available page for both products.
- GREEN: focused real-Postgres tests prove both products reject the incomplete
  index and preserve normal current-generation pagination when counts match.
- API routing regression and undefined-name lint pass.

## Operational meaning

An incomplete index is now an explicit repair/commissioning condition, never a
silent claim that the displayed subset represents the full current market.
