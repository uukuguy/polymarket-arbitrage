# M1 Schema Contract 040 — Task 1 Summary

**Delivered:** The control-plane schema contract now names Alembic revision
`040`, the actual sole migration head.

**Why:** Production was correctly migrated to `040`, but role provisioning and
rollout checks still expected `039`. That false mismatch blocked the safe,
scoped runtime-controller login provisioner.

**Verification:** The migration-head contract was first observed failing
(`040` versus `039`), then the schema-contract, 040 migration, rollout, and
database-role-admin suites all passed.
