# M1 Business Dashboard Integrity — Task 1 Summary

## Delivered

- The business overview now reports both authoritative product records and the bounded, generation-bound research index count.
- The Structure research route rejects a nonempty published generation with an empty index as `research-index-incomplete`; it no longer renders an empty list as usable business detail.
- The business Dashboard now shows dense product cards for Structure, Quotes, Analysis, and Opportunities, including browseable-index size, lineage links, qualification state, and direct runtime navigation.

## Verification

- `pnpm run typecheck` and `pnpm run build` in `dashboard/`.
- Focused business overview, structure research, and dashboard-contract pytest suite.

## Production Follow-up

- Deploy the control-plane API image so index-integrity metadata is authoritative in production.
- Deploy Dashboard through the authenticated Vercel project, then inspect `/business`, `/business/structure`, `/business/quotes`, and `/control-plane` with Playwright.
