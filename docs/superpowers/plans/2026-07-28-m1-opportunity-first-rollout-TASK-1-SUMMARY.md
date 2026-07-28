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
105 passed

uv run ruff check src/polyarb/perception tests/perception
All checks passed!

make planning-status
no drift detected
```

Production copied-database migration and later rollout gates are not claimed by
this Task 1 summary.
