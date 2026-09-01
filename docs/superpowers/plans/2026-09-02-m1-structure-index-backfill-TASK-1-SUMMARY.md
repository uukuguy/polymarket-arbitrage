# M1 Structure Index Backfill — Task 1 Summary

## Outcome

- Added an explicit, enabled-only backfill path which rebuilds the compact current Structure index from authenticated published R2 range artifacts.
- Retires only rows belonging to generations without a published manifest.
- Stores a compact business projection for events and group truth rather than raw Gamma payloads, preserving free-tier capacity.

## Verification

- `uv run pytest tests/m1-perception/test_transactional_structure_worker.py -q -k 'research or business'`
- `uv run python -m py_compile src/polyarb/cli_control_plane.py src/polyarb/control_plane/postgres.py src/polyarb/control_plane/structure_worker.py`
