# M1 business research retention — Task 1 Summary

## Outcome

Dashboard research indexes in PostgreSQL are now bounded to the current
published Structure and Quote generations. Historical source artifacts remain
immutable in R2; staged rows without a published manifest are retained until
their generation can be certified.

## Implementation

- `PostgresControlPlane.certify_structure_generation` removes only superseded,
  manifest-backed Structure research rows in its existing certification
  transaction.
- `PostgresControlPlane.certify_quote_generation` does the same only after the
  Quote current-pointer mutation has succeeded, in the same transaction.
- The shared helper joins rows against `m1_generation_manifests`, so it cannot
  delete an unpublished candidate merely because it has been staged.

## Verification

- RED: each product retained the old published row before the change.
- GREEN: focused Postgres integration tests prove current rows remain, old
  manifest-backed rows are removed, and unpublished staged rows remain.
- Regression: Quote certification and Structure certification rollback tests
  pass; changed-file undefined-name lint is clean.

## Production follow-up

Deploy the worker image, let the next Structure and Quote certifications apply
the bounded retention transaction, then verify relation-level capacity via
`make control-plane-status limit=10`.
