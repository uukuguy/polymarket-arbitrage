# M1 Structure index batch staging — Task 1 Summary

## Outcome

Structure research-index staging now validates every row before issuing one
bounded psycopg `executemany` batch, instead of paying a remote Postgres round
trip for each row during idempotent recovery.

## Safety boundary

- Input validation is unchanged and happens before any database write.
- Inserts retain `ON CONFLICT DO NOTHING` semantics.
- One artifact remains one transaction, preserving bounded rollback and restart
  scope.

## Verification

- `uv run pytest tests/m1-perception/test_control_plane_postgres.py::test_structure_range_receipt_atomically_stages_business_research_rows -q`
- `uv run pytest tests/m1-perception/test_transactional_structure_worker.py -q -k 'research or business'`
- `uv run python -m py_compile src/polyarb/control_plane/postgres.py`

The broader prune test was also run but is order-sensitive because it queries
the shared test database without a generation filter; its extra rows are from
prior focused tests rather than this staging path.
