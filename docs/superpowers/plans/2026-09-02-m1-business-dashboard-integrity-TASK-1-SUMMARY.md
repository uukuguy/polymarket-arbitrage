# M1 Business Dashboard Integrity — Task 1 Summary

## Delivered

- The business overview now reports both authoritative product records and the bounded, generation-bound research index count.
- The Structure research route rejects a nonempty published generation with an empty index as `research-index-incomplete`; it no longer renders an empty list as usable business detail.
- The business Dashboard now shows dense product cards for Structure, Quotes, Analysis, and Opportunities, including browseable-index size, lineage links, qualification state, and direct runtime navigation.
- The Runtime page now accepts the watchdog's `escalated` transition as a valid operator fact and labels it as an automatic-remediation escalation, instead of rejecting the complete runtime snapshot as unavailable.
- The analysis route now publishes a lineage-bound research funnel (Structure records → Quote records → same-generation certified opportunities) directly from the atomic overview; absent candidate/reject detail remains explicit rather than inferred.

## Verification

- `pnpm run typecheck` and `pnpm run build` in `dashboard/`.
- Focused business overview, structure research, and dashboard-contract pytest suite; the decoder fixture covers a watchdog escalation.
- Focused Postgres overview tests cover the research-funnel lineage; Dashboard typecheck and production build pass.

## Production Follow-up

- Deploy the control-plane API image so index-integrity metadata is authoritative in production.
- Deploy Dashboard through the authenticated Vercel project, then inspect `/business`, `/business/structure`, `/business/quotes`, and `/control-plane` with Playwright.
