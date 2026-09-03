# Quote Coverage Discovery — Summary

## Delivered

- Server-side, generation-current ordering by executable notional and YES-price extremity.
- Per-row explainable discovery evidence, opaque continuation cursor, and strict parent-Structure context joins.
- Dashboard research-lead table with readable event and neg-risk context plus an explicit non-opportunity disclaimer.
- Control-plane API deployed to Fly release v18; live endpoint returned score, context, and first ranked record.

## Verification

- `uv run pytest tests/m1-perception/test_quote_discovery.py -q` — pass.
- `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k business_quote_page tests/m1-perception/test_control_plane_api.py -q` — pass.
- `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q` — pass.
- `make dashboard-typecheck dashboard-build` — pass.
- Authenticated Playwright inspected `/business/quotes`; Research leads, current generation, score, executable notional, event title, group context, and non-opportunity copy were all present.

## Operational Boundary

The coordinator remains intentionally stopped for capacity protection. This delivery neither restarts Quote admission nor changes qualification; it only improves the truthfulness and discoverability of already published Quote research evidence.
