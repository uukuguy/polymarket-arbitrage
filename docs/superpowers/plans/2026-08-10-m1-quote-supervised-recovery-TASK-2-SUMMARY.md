# M1 Quote Supervised Recovery — Task 2 Summary

## Delivered

- Extended the producer authority schema to accept the `quote` component.
- Added an in-transaction SQLite rebuild migration for legacy producer tables
  whose `CHECK(component ...)` pre-dates Quote supervision.
- The migration preserves receipt, child-start and heartbeat evidence, recreates
  the heartbeat index, and removes all temporary legacy tables before commit.
- Quote can now reserve an outer-supervisor attempt and publish an authenticated
  producer heartbeat under the same authority contract as existing workers.

## Verification

`uv run pytest tests/perception/test_supervisor.py::test_quote_producer_schema_migration_preserves_legacy_evidence tests/perception/test_supervisor.py::test_quote_producer_can_publish_supervised_heartbeat tests/perception/test_supervisor.py::test_output_hash_migration_backfills_legacy_receipt_and_is_idempotent tests/perception/test_supervisor.py::test_output_hash_migration_rejects_invalid_legacy_tail_without_partial_write -q`

Result: `4 passed`.

`uv run ruff check src/polyarb/perception/store.py src/polyarb/storage/schemas.py tests/perception/test_supervisor.py`

Result: all checks passed.

## Next

Wire the long-lived Quote worker into the isolated producer CLI and supervisor;
the migration alone does not alter the live production topology.
