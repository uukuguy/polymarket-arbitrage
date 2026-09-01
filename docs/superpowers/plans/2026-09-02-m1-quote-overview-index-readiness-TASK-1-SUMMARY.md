# M1 quote overview index readiness — Task 1 Summary

## Outcome

The business overview now reports a current Quote generation as unavailable
when its browseable research index is incomplete.  This makes temporary
capacity-protection release of retired Quote pages truthful to Dashboard users.

## Implementation

- Read expected Quote research cardinality from durable quote-batch inputs,
  falling back to the published manifest count.
- Compare it with the current-generation staged index count in the same
  repeatable-read overview snapshot.
- Gate both Quote and its dependent analysis status with the explicit
  `research-index-incomplete` reason.

## Verification

- Added a regression that reproduces a current Quote pointer with missing
  research rows and proves the overview fails closed.
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py -q -k 'business_quote_page or business_overview'`
- `uv run python -m py_compile src/polyarb/control_plane/postgres.py`
