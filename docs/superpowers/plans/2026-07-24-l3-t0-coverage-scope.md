# L3 T0 Coverage Scope Implementation Plan

**Goal:** Make the 30-second T0 artifact enforce the scheduled sample contract
without weakening cumulative or final exact-window coverage.

1. Add RED verdict tests for zero raw churn with complete T0 identity sets and
   for missing T0 identity keys.
2. Detect only the exact non-final manifest T0 sample interval in
   `build_soak_report`.
3. For that interval, require exact coverage key sets but retain zero counts as
   diagnostic/hash-bound evidence; keep positive-count gates for every longer
   checkpoint and final verify.
4. Update the Phase 05.4 design and Plan 05 Task 4 wording so source freshness
   at T0 and cumulative raw coverage cannot be conflated.
5. Run focused, phase-wide, full, Ruff, compile, docs, image, and planning
   gates; deploy an exact new SHA and restart readiness on a new boot.
6. Preserve both production-rejected manifests/reports and record their exact
   reasons in the soak log. Never reuse either T0.
