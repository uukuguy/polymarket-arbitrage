# M1 Quote Supervised Recovery — Task 4 Summary

## Delivered

- Split Quote supervision from the legacy all-producer isolation switch.
- Added `neg_risk_quote_supervisor_enabled` and enabled only that production
  capability in `fly.toml`.
- When Quote supervision is enabled, the parent daemon does not collect Quote;
  it hydrates certified feed state while the supervised child is the sole
  collector. Structure scheduling remains in the parent and is not disabled.
- The existing global producer-supervisor mode still behaves as before for
  candidate, discovery, and reconciliation workers.

## Why the split is required

The old global supervisor flag also disables the parent Structure scheduler.
Using it solely to repair Quote timeouts would create a new market-data outage.
The new Quote-only switch preserves the running Structure/Snapshot path while
giving Quote bounded process recovery.

## Verification

`uv run pytest tests/perception/test_supervisor.py tests/daemon/test_quote_worker.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_opportunity_watcher_http.py -q`

Result: `142 passed`.

Changed-file Ruff and `git diff --check` both passed.
