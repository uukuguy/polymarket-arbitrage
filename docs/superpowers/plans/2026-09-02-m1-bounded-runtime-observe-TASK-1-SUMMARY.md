# M1 Bounded Runtime Observe — Task 1 Summary

## Delivered

- Added Alembic revision 042, replacing the append-only runtime-observe ledger with bounded status, current-state, transition, and hourly-rollup projections.
- All writes now go through a lease-fenced `SECURITY DEFINER` turn function. Direct controller DML is revoked; only status reads and the function grant remain.
- Enforced finite retention: 500 evaluated/current targets per turn, 5,000 transitions, and 30 days of hourly rollups. The legacy raw table is dropped by the forward-only migration.
- Changed the observe-only controller to probe `limit + 1`; it publishes a visible `coverage_truncated` degradation instead of incorrectly inferring recovery from a partial scan. Revision 043 raised the original 100-target scan bound to the 500-target current-state cap after production proved M1 has 216 active targets.
- Reworked read-only verification to use the bounded status row: controller identity, continuity, freshness, gap, current-candidate parity, coverage/storage flags, and zero recovery actions.

## Verification

- `uv run pytest tests/m1-perception/test_control_plane_runtime_observe.py tests/alembic/test_042.py tests/alembic/test_control_plane_schema_contract.py tests/m1-perception/test_control_plane_db_role_contract.py -q`
- `uv run pytest tests/m1-perception/test_control_plane_cli.py -q`
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py -q -k claim_reclaim_and_epoch_fencing`
- Focused Ruff checks passed for changed bounded-observe modules and tests.

## Production Follow-up

Controller remains stopped. Apply revision 042, inspect reclaimed capacity and exact role grants, deploy the controller image, then run a controlled turn and a 30-minute bounded soak before enabling continuous service.
