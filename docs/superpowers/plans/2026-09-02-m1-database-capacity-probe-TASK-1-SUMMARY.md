# M1 Database Capacity Probe — Task 1 Summary

**Delivered:** An independent, bounded Postgres capacity probe now reports the
database byte total, the explicit 450 MB M1 operating budget, the policy
verdict, and no more than ten largest public relations. The control-plane HTTP
projection merges this optional diagnostic only after the primary operational
snapshot succeeds; a timeout or provider failure remains a typed
`database-size-observation-unavailable` value rather than cancelling operator
truth.

**Operator surface:** `/perception/control-plane` and `/control-plane` now
show the verdict and the largest relations when available. The business pages
remain independent of this runtime diagnostic.

**Verification:** focused Python capacity/API/Dashboard-contract tests,
Dashboard TypeScript typecheck, and a read-only live probe against the new M1
database. The live probe measured 21,384,339 bytes (4% of the 450,000,000-byte
budget); its largest relations were the qualification ingress ledger and
runtime-event ledger.

**Still required:** persistent observation history, retention planning and
execution receipts, production API deployment, and restoration of the valid
Telegram credential before declaring M1 healthy.
