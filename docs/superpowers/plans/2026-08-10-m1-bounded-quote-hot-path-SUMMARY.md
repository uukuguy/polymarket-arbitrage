# M1 bounded Quote hot path — implementation summary

## Task 2: current authenticated generation

- Added `neg_risk_quote_current_generation`, a singleton pointer to the only
  scanner-visible certified Quote run.
- Collection still writes a `collecting` staging run. `complete_run(...,
  publish_current_generation=True)` validates its cardinality/receipt, marks
  it complete, switches the pointer, and removes the prior pointed generation
  in one SQLite transaction.
- `latest_complete_projection()` and `latest_complete_run()` read the pointer
  whenever it exists; legacy historical lookup remains only for databases that
  have not yet published a pointer generation.
- Retention purge excludes the pointed run, so generic history cleanup cannot
  remove the feed selected by the current-generation authority.

## Verification

`uv run pytest tests/routing/test_neg_risk_quote_store.py tests/routing/test_neg_risk_quote_collector.py tests/routing/test_opportunity_scanner.py tests/daemon/test_quote_worker.py -q`

Result: 175 passed.

## Remaining production proof

The running historical database still needs a physical online-copy migration
to reclaim pages from prior unbounded history. After deployment, verify that a
fresh collection writes and switches the pointer, old current payload is
reclaimed, and incident/health chains remain consistent.
