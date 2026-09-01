# M1 Structure index resume skip — Task 1 Summary

## Outcome

An interrupted Structure business-index rebuild now reads the current staged
entity IDs once and skips already committed rows before staging.  Restarting a
large index no longer replays thousands of remote `ON CONFLICT` writes.

## Safety boundary

- The authoritative source remains the authenticated published R2 artifacts.
- Existing rows are skipped only when their generation-scoped entity ID already
  exists.
- Missing rows still use the bounded, idempotent batch staging path.

## Verification

- Added a regression that seeds one known ID, proves it is excluded from
  staging, and still proves four-way bounded reads.
- `uv run pytest tests/m1-perception/test_control_plane_cli.py::test_structure_index_backfill_reads_published_artifacts_with_bounded_parallelism -q`
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py::test_structure_range_receipt_atomically_stages_business_research_rows -q`
- `uv run python -m py_compile src/polyarb/cli_control_plane.py src/polyarb/control_plane/postgres.py`
