# M1 Quote Pool Pressure — Task 1 Summary

**Delivered:** The deployed Quote worker now caps its concurrent lease lanes at
two, matching its deliberately bounded two-session Postgres pool.

**Why:** Production logs showed `PoolTimeout` while twelve simultaneous Quote
lanes staged receipt and research-index rows through a two-session pool. The
jobs are durable and recoverable, but that mismatch turns normal publication
load into self-inflicted connection contention.

**Verification:** The Fly deployment-template contract now proves the cap is
present, and the complete local template and rollout test set passes.
