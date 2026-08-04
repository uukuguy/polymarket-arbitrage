# M1 Recovery, Retention, and Resident Maintenance — Summary

Date: 2026-08-04  
Workstream: `m1-perception`  
Status: local implementation and M1 verification complete; protected production deployment/UAT pending

## Outcome

M1 now treats authenticated forward progress, retention ownership, and resident maintenance as
separate production contracts:

- a non-deferred durable Structure/member/drift checkpoint breaks the consecutive failure streak
  without changing `RECOVERING` back to `RUNNING`;
- terminal Structure windows retain their authority/proof skeleton while heavy staging payload is
  reclaimed and marked with `staging_reclaimed_at_ms`;
- snapshot retention deletes same-retention `snapshot_attempts` transactionally and excludes the
  actual Quote owner, `neg_risk_quote_runs.universe_snapshot_id`;
- generation cleanup has restart-persistent `idle/running/backoff/blocked` runtime truth and one
  daemon-owned, Quote-aware loop with at most 500 deleted rows per transaction;
- `/health` reads the exact runtime row mutated by the worker, and Polywatch alerts/recoveries use
  the resulting `snapshot:structure_generation_cleanup_runtime` check.

## Commits

| Commit | Result |
|---|---|
| `3d2bed5` | reset failure streak on authenticated durable progress only |
| `e715ca7` | reclaim Structure staging without deleting window/proof authority |
| `da09bf7` | make snapshot retention follow real Quote ownership |
| `484886b` | persist singleton generation-cleanup runtime and restart recovery |
| `40ee2f1` | run bounded cleanup continuously below Quote priority |
| `88a8a8a` | wire lifecycle, configuration, health, Makefile help, and Polywatch |
| `00fdfee` | prove 300k-row throughput, fairness, restart, and operator model |

Design and plan anchors: `836ca4b` and `9d30d63`.

## TDD and verification evidence

RED evidence included missing settings/lifecycle helpers, missing runtime health chain, missing
Polywatch action, inverted retry bounds, and the production-shaped acceptance harness. The 300k
harness initially appeared to exceed 240 seconds; systematic tracing showed the fixture had resealed
`committed_counts` but not `expected_counts`. Cleanup correctly returned
`generation-count-contract-mismatch` with zero mutation. The harness now reseals both facts and
fails immediately on any authenticated `blocked` result.

Fresh GREEN evidence:

- production-shaped cleanup: approximately 300,000 rows, `15.21s < 240s`, every transaction
  `<=500` rows, final retained `<=2`, reclaimable `=0`;
- Quote waiter acquires the shared lock after the current transaction and before the next cleanup
  transaction;
- recreating `SQLiteStore` and `StructureGenerationCleanupWorker` after partial progress resumes the
  authenticated phase and preserves current/rollback rows;
- `uv run pytest -q tests/m1-perception --junitxml=/tmp/m1-recovery-retention-full.xml`:
  `2778` tests, `0` failures, `0` errors, `2` skipped, `1278.093s`;
- `uv run ruff check src tests scripts`: clean;
- `make docs-m1-check`: `M1 manual contract: OK`;
- `make planning-status`: no drift across 84 plans;
- product diff excluding five pre-existing user-owned `.superpowers/sdd/*` files:
  `git diff --check` clean;
- `make help` exposes `structure-generation-status` and describes
  `structure-generation-cleanup` as a resident-worker diagnostic.

`uv run ruff check .` is not a clean repository-wide baseline: it reports 20 pre-existing findings
under unchanged `alembic/` and `tools/climb/`. Canonical CI scopes Ruff to `src/ tests/`; the broader
Task 8 command and this deviation are recorded rather than modifying unrelated capability lines.
Plain `git diff --check` also reports a pre-existing EOF blank line in the user-owned dirty
`.superpowers/sdd/task-7-brief.md`; that file was neither modified nor staged by this work.

## Inline fresh-context review

Scope: all 25 files changed from root-cause record `23cbe82` through `00fdfee`.

- CRITICAL: 0
- HIGH: 0
- MEDIUM: 0
- LOW: 0
- Recommendation: APPROVE for protected maintenance deployment after exact-SHA authorization.

The review explicitly verified that durable progress does not change scheduler state; supersession,
writer-busy, and defer paths do not reset the counter; Structure windows/receipts remain; Quote-owned
snapshots are excluded before purge; cleanup admission authenticates existing comparison receipts and
retention floor; Quote is checked before and after lock acquisition; runtime errors persist beyond
logs; health reads the worker-mutated singleton; and lifecycle creates/cancels/gathers at most one
in-daemon owner.

## Design deviations

- The requested learning filename `47-resident-retention-maintenance.md` conflicted with existing
  learning documents 47 and 48, so the new document is `49-常驻保留维护.md` and the index remains
  monotonic.
- Repository-wide Ruff is recorded as an unrelated baseline deviation; the canonical product/CI
  scope is clean and no unrelated lint-only changes were bundled.

## Production risk and rollback

The protected first deployment keeps Structure enabled, Quote disabled, generation read mode
`legacy`, and cleanup defaults `500 / 0.05s / 30s / 5s`. The principal risks are unexpected receipt
authentication blocks, writer contention, or a stale runtime while pressure remains. All are visible
through the dedicated health check and Polywatch component lifecycle.

Rollback is release rollback to the previous exact Fly image while preserving the SQLite volume.
Do not manually move the generation pointer, delete evidence, edit receipts/runtime rows, enable
Quote, or switch generation reads to hide a fault. Any in-flight `running` cleanup owner is converted
to bounded backoff by the next process startup; already committed chunks remain authenticated.

## Remaining production acceptance

This summary does not complete M1. The next authorized deployment must naturally demonstrate runtime
advance, retained `9 → <=2`, reclaimable `7 → 0`, staging/snapshot retention without FK errors,
failure→checkpoint→failure counter `1`, one maintenance alert and one recovery, Quote fairness, and
health/direct-authority agreement. Only then does the existing protected classifier/read/Quote
cutover and candidate-lifecycle opportunity UAT resume.
