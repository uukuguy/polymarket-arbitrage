---
phase: 05.6-self-healing-structure-production
plan: task-8-natural-window-recovery-hotfix
subsystem: production-recovery
tags: [sqlite, scheduler, structure-generation, drift, fail-closed, flyio]
requires:
  - phase: 05.6-self-healing-structure-production
    provides: classifier-v2 drift authority and protected production rollout
provides:
  - exact historical-memberless wait state yields to a natural snapshot
  - atomic supersession of an active drift identity when a successor generation publishes
  - rollback and multiple-active fail-closed regression coverage
affects: [task-8-production-rollout, structure-generation-read-cutover, quote-worker-cutover]
tech-stack:
  added: []
  patterns: [authenticated-exact-state-yield, pointer-coupled-atomic-supersession]
key-files:
  created:
    - docs/superpowers/plans/2026-08-02-m1-structure-drift-classifier-recovery-TASK-8-HOTFIX-SUMMARY.md
  modified:
    - src/polyarb/daemon/scheduler.py
    - src/polyarb/storage/sqlite_store.py
    - tests/m1-perception/test_scheduler.py
    - tests/m1-perception/test_structure_generation_publication.py
    - docs/dev/structure-drift-operations.md
    - docs/learning/46-Structure漂移安全切换.md
    - .planning/threads/market-observation-architecture.md
key-decisions:
  - "Only the exact authenticated waiting-natural-window/member-receipt-unavailable state may yield drift scheduling to snapshot work."
  - "Publishing a successor generation atomically stales one mismatched active drift identity; multiple active identities fail closed."
patterns-established:
  - "A recovery wait must yield the scheduler resource needed to produce its own missing evidence."
  - "Authority supersession and pointer publication share one SQLite transaction and rollback boundary."
requirements-completed: []
completed: 2026-08-04
---

# Task 8 Hotfix: Natural Window Recovery Summary

**The scheduler can create the natural source window required by historical memberless recovery, while generation publication atomically retires the obsolete active drift identity without forging terminal authority.**

## Accomplishments

- Reproduced the production deadlock after classifier-v2 naturally advanced through source projection and then repeatedly deferred on stale generation identity.
- Added an exact, fail-closed scheduler yield for authenticated `waiting-natural-window` plus `structure-event-source-receipt-unavailable` state.
- Coupled active drift supersession to successor pointer publication in the same `BEGIN IMMEDIATE` transaction.
- Added rollback injection and corrupt multiple-active identity tests proving no partial pointer, publication, progress, or receipt mutation.
- Documented the recovery chain and operator expectations in the operations guide, learning guide, and architecture thread.

## Verification

- Focused scheduler and generation-publication tests: PASS.
- Full suite: 4,722 tests, 0 failures, 0 errors, 2 skipped, 1,610.994 seconds.
- `uv run ruff check src tests scripts`: PASS.
- `make docs-m1-check`: PASS.
- `make planning-status`: PASS, no drift.
- Scoped `git diff --check`: PASS.
- Independent review: APPROVE; no Critical or Important findings.

## Production Boundary

This summary closes the code hotfix only. Task 8 remains incomplete until the exact reviewed commit is deployed in protected mode and production proves, without manual pointer mutation or forced advancement:

1. a new natural window and successor generation are published;
2. the obsolete drift identity becomes stale atomically;
3. a new classifier-v2 comparison seals with authenticated evidence;
4. generation read mode, then Quote, are enabled on the same image under strict health checks;
5. one full natural generation and the final two-minute UAT remain healthy.

## User Setup Required

None.

## Next Phase Readiness

The hotfix is ready to be committed, independently bound to an exact SHA, and deployed under the existing protected production configuration. Candidate lifecycle work remains sequenced after Task 8 production acceptance.

---
*Phase: 05.6-self-healing-structure-production*
*Completed: 2026-08-04 (code hotfix only; production Task 8 remains open)*
