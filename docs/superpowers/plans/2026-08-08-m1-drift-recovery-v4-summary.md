# M1 Drift Recovery and Nullable-Event v4 Summary

**Status:** Locally qualified; protected production deployment awaits exact-SHA authorization.

- A timeout gets 100 ms continuation only after the same comparison ID proves a durable checkpoint advance; failed attempts and defers remain immutable.
- Unchanged checkpoints, replacement identities, terminal state, and unavailable status retain normal cadence.
- Immutable classifier v4 preserves v3 receipt fields/digests while adding only the observed nullable ordinary-event predicate: null `negRisk`, false `enableNegRisk`, null `negRiskMarketID`, null member group, and false `negRiskOther`.
- That exact shape is a `non-neg-risk-event-member` exclusion; v1-v3 rows and receipts are not changed.

Verification: RED/GREEN focused tests, full drift end-to-end plus scheduler suites, changed-file Ruff, `git diff --check`, and `make planning-status` (84 plans, no drift).

Deployment must use the committed exact SHA with classifier enabled, legacy reads, Quote disabled, and cleanup protections unchanged. A new v4 comparison must achieve authenticated sealed health before any read-mode or Quote change.
