# V3 Task 3 Report — versioned exclusion receipt evidence

## Result

- Code commit: `62b8105` (`feat(m1): version classifier v3 exclusion receipts`)
- Added `_migrate_structure_drift_classifier_v3_exclusions()` after the
  Structure authority DDL boundary so both fresh and pre-v3 databases converge.
- Authorization and terminal receipts gain four nullable v3 exclusion fields.
  Progress gains those four plus non-null digest-state JSON with v2-safe defaults.
- The migration never updates receipt rows or recomputes historical digests.
- Immutable authorization and terminal update/delete/duplicate-insert triggers
  are restored inside the migration savepoint before release.
- Digest selectors retain the exact v1/v2 tuple order and insert the four v3
  commitments immediately before the existing terminal timestamp field(s).
- Unknown classifier contracts raise
  `invalid-structure-drift-classifier-contract`; status readers catch this and
  preserve the existing fail-closed tamper result.

## TDD evidence

RED command:

```text
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  -k 'v3_migration or v3_receipt_digest or v2_receipt_digest_field'
```

Observed target failures: missing receipt/progress columns, missing migration
entry point, and rejection of v3 authorization/terminal digest payloads. Both
independent v2 digest oracles already passed.

Final GREEN verification:

```text
uv run ruff check src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_structure_generation_readers.py
# All checks passed

uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  -k 'v3_migration or v3_receipt_digest or v2_receipt_digest_field'
# 8 passed

uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py
# 107 passed

uv run pytest -q tests/m1-perception/test_structure_generation_readers.py
# 80 passed
```

`git diff --check` also passed for all three changed code/test files.

## Evidence integrity and rollback review

- A real sealed v2 authorization plus stale v2 terminal receipt are captured
  before migration and compared over their exact historical field tuples and
  stored digest bytes afterward.
- New receipt columns are verified `NULL`; migrated progress values are exactly
  `(0, 0, "{}", "{}", "{}")`.
- Fault injection after authorization ALTERs, terminal ALTERs, and progress
  ALTERs proves the complete authority schema/index/trigger signature rolls
  back to its pre-v3 value.
- Independent test oracles construct canonical JSON tuples and call
  `hashlib.sha256` directly; they never call a production digest helper.
- Every one of the four new exclusion commitments is independently mutated for
  authorization and terminal receipt digests.

## Self-review

- No source/serving table, historical receipt value, or old digest is mutated.
- v1/v2 field order is unchanged byte-for-byte.
- Fresh initialization originally revealed that authority tables are created by
  `STRUCTURE_GENERATIONS_DDL`; the migration now runs immediately after it.
- One adjacent reader test used an integer as a fake classifier version. It was
  corrected to the real v2 contract and now asserts explicit rejection when
  the contract itself is tampered.
- The old v2 tuple aliases remain temporarily for the v2-only write/read sites;
  Task 4 can switch those sites to the new contract selectors without breaking
  the existing interface in the interim.

## Concerns

None for Task 3. Task 4 must populate all four receipt fields for v3 rows and
use `_structure_drift_receipt_fields()` /
`_structure_drift_terminal_receipt_fields()` at write and read boundaries.

## Review-fix evidence — lightweight schema convergence and v1 oracles

- RED added `test_v3_migration_lightweight_structure_sync_schema_converges`
  against a database containing only `snapshots(id)`. The exact lightweight
  `init_structure_sync_schema()` entry point failed because all four v3 receipt
  columns were absent.
- GREEN calls `_migrate_structure_drift_classifier_v3_exclusions(con)`
  immediately after `STRUCTURE_GENERATIONS_DDL` in the existing-schema branch.
  The migration's savepoint installs the columns before views/readers and
  restores the five immutable receipt triggers before release.
- The regression asserts every v3 progress/authorization/terminal field, the
  exact five trigger names and seal messages, then compares the complete
  authority column/index/trigger signature with a fresh full `init_schema()`
  database.
- Added independent v1 authorization and terminal SHA-256 oracles. Test-owned
  fixed tuples prove both v1 and v2 select the same historical field order;
  expected digests are computed directly with `hashlib.sha256`, never with a
  production digest helper.

Exact RED/GREEN and final verification:

```text
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  -k 'v3_migration or v3_receipt_digest or v2_receipt_digest_field or v1_receipt_digest_field'
# RED: 1 failed, 10 passed (lightweight entry point missing v3 columns)
# GREEN: 11 passed

uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py
# 110 tests collected; exit 0

uv run pytest -q tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_structure_sync_window.py
# 350 tests collected; exit 0

uv run ruff check src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_structure_generation_readers.py \
  tests/m1-perception/test_structure_sync_window.py
# All checks passed

git diff --check -- src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  .superpowers/sdd/v3-task-3-report.md
# exit 0
```
