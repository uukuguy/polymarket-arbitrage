# M1 business overview index readiness — Task 1 Summary

## Outcome

The production business overview now fails closed while the current Structure
research index is incomplete.  It exposes `unavailable` with
`research-index-incomplete`, rather than presenting a partially backfilled
research corpus as usable business intelligence.

## Implementation

- Derive the expected browsable Structure record count from published `events`
  and `group_truth` range receipts.
- Compare that durable expected count with the staged research-index count in
  the same repeatable-read overview snapshot.
- Omit `reason_code` when the product is available; include the explicit
  incomplete-index reason only when it is unavailable.

## Verification

- `uv run pytest tests/m1-perception/test_control_plane_postgres.py::test_business_overview_reports_the_published_quote_and_real_zero_opportunities -q`
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py -q -k 'business_structure_page'`
- `uv run python -m py_compile src/polyarb/control_plane/postgres.py`

