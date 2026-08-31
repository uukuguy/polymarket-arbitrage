# Task 2 Summary — Operator Guidance Alignment

The M1 manual and current-state entry now require the two complementary,
read-only production observations: `make smoke-control-plane-prod` for public
strict readiness and `make control-plane-status` for durable business truth.
They explicitly distinguish API reachability from Opportunity freshness and
rolling qualification.

The manual marks every retired L1/L2 health and Fly-status target as fail-loud
compatibility only. Historical descriptions remain available for traceability
but are explicitly non-executable.

Live evidence: Fly reports `polyarb-control-api` Machine
`d8d0e27a734158` started with `/healthz` passing and release `3a70cd9f…`;
the new public strict probe returned HTTP 200 with
`{"status":"ok","control_plane":"available"}`.
