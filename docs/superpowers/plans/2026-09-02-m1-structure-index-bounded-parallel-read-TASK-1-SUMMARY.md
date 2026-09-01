# M1 Structure index bounded parallel read — Task 1 Summary

## Outcome

The operator backfill for the published Structure research index now performs
up to four independent R2 artifact reads concurrently while retaining serial,
idempotent PostgreSQL staging.

## Safety boundary

- Only authenticated immutable R2 reads are parallelized.
- Artifact order remains deterministic at the staging boundary.
- Database writes remain single-threaded and retain the existing row-level
  idempotency and recovery behavior.

## Verification

- Added a regression proving the reader reaches, but never exceeds, four
  simultaneous artifact reads.
- `uv run pytest tests/m1-perception/test_control_plane_cli.py::test_structure_index_backfill_reads_published_artifacts_with_bounded_parallelism -q`
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py -q -k 'business_structure_page or business_overview'`
- `uv run python -m py_compile src/polyarb/cli_control_plane.py`
