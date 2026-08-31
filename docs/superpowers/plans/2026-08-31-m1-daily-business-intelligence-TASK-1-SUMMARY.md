# M1 Daily Business Intelligence — Task 1 Summary

Task 1 added the official bounded public reader for the current certified M1
opportunity projection.

## Changed files

- `Makefile` — added the read-only `control-plane-opportunities` target, with
  bounded curl transport and JSON formatting.
- `tests/m1-perception/test_makefile_contract.py` — added the target contract
  and then corrected its source-token lint coverage.

## Evidence

- Focused Makefile contract test:
  `uv run pytest tests/m1-perception/test_makefile_contract.py -k
  control_plane_opportunities_is_current_read_only_business_entrypoint -q`.
- In the delivery verification session, `make control-plane-opportunities
  limit=5` exited 0 and returned `status=available`,
  `current_opportunity_count=0`, and an empty bounded item list.

## Non-goals

The target neither deploys nor writes production state. It does not create an
order, execute a trade, access a wallet, or turn a candidate projection into a
profit or P&L claim.

## Commits

- `307d3dd2d8bbf375dc5bd63390618b63c04cf786` — implementation.
- `f3589981` — Task 1 contract-lint completion fix.
