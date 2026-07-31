# Task 1 Summary — Group Revision and Quote-Batch Authority

## Delivered

- Added immutable `GroupRevision`/`GroupLeg` contracts with deterministic SHA-256
  identity over the complete ordered membership.
- Added immutable `GroupQuoteBatch`/`GroupQuoteLeg` contracts that reject mixed
  memberships, duplicate tokens, non-executable legs, invalid ask values, and
  reversed timestamps.
- Added append-only group-revision and quote-batch SQLite tables without changing
  existing tables or read paths.
- Added `OpportunityPerceptionStore` with WAL-compatible busy-timeout connections.
- Group publication and membership-change quote supersession share one
  `BEGIN IMMEDIATE` transaction.
- Quote publication re-reads the latest certified group inside its write transaction
  and requires the exact ordered all-leg token identity.
- Current-batch reads fail closed for stale, future, superseded, non-certified, or
  mismatched membership state.
- Certified groups and complete quote batches are revalidated after
  deserialization, so corrupt ordered legs cannot become authoritative.
- Current group status and quote authority are selected in one joined SQLite
  statement, eliminating the split-read revocation window.
- A SQL-trace regression counts only authoritative perception-table reads and
  mutation-proves that a two-statement group/quote implementation is rejected.

## Safety Boundaries

- This slice does not implement Candidate Watcher, Discovery, Reconciliation,
  incidents, APIs, Dashboard, deployment, or execution.
- The existing snapshot and opportunity read paths remain unchanged.
- A metadata-only revision with the same membership hash preserves an otherwise
  fresh complete batch; a membership hash change supersedes prior complete batches.

## TDD and Verification

```text
uv run pytest tests/perception/test_models.py -q
RED: ModuleNotFoundError: No module named 'polyarb.perception.models'

uv run pytest tests/perception/test_store.py -q
RED: ModuleNotFoundError: No module named 'polyarb.perception.store'

uv run pytest tests/perception/test_models.py tests/perception/test_store.py \
  tests/routing/test_opportunity_ledger.py \
  tests/routing/test_neg_risk_quote_store.py -q
109 passed

uv run ruff check src/polyarb/perception tests/perception
All checks passed!

make planning-status
no drift detected
```

## Slice A → B Production-Clone Gate

The volume-owning `polyarb-l1` machine was opened read-only and its live
`/data/state.db` schema plus up to 200 recent rows per legacy table were copied
into a compact SQLite clone. The source database was not migrated or stopped.
The clone retained all 20 production legacy table definitions and 2,530
representative rows.

Running `OpportunityPerceptionStore.init_schema()` against a fresh copy proved:

```text
legacy_tables_unchanged=20
legacy_rows_unchanged=2530
new_tables=neg_risk_group_quote_batches,neg_risk_group_revisions
quick_check=ok
```

The pre/post fingerprint also covered `SQLiteStore.get_latest_snapshot()` and
`NegRiskQuoteStore.latest_universe()`. Their results were byte-for-byte stable.
This closes the copied-production-database compatibility gate before Slice B.
Later rollout and deployment gates remain open.
