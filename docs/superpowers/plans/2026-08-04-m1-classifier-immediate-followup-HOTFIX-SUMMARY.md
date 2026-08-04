---
phase: 05.6-self-healing-structure-production
plan: classifier-immediate-followup-hotfix
subsystem: production-recovery
tags: [scheduler, classifier-v2, drift, quote-priority, flyio]
requires:
  - phase: 05.6-self-healing-structure-production
    provides: bounded classifier-v2 drift slices and protected production rollout
provides:
  - immediate resident re-admission after durable non-terminal drift checkpoints
  - preserved Quote-priority checks at every classifier slice boundary
affects: [task-8-production-rollout, generation-read-cutover, quote-worker-cutover]
tech-stack:
  added: []
  patterns: [durable-checkpoint-immediate-followup]
key-files:
  created:
    - docs/superpowers/plans/2026-08-04-m1-classifier-immediate-followup-HOTFIX-SUMMARY.md
  modified:
    - src/polyarb/daemon/scheduler.py
    - tests/m1-perception/test_scheduler.py
    - .planning/JOURNAL.md
    - .planning/threads/market-observation-architecture.md
key-decisions:
  - "A durable non-terminal classifier checkpoint sets the same resident follow-up signal as event-member and Structure checkpoints."
  - "Sealed, deferred, and failed classifier paths keep their existing cadence and recovery semantics."
requirements-completed: []
completed: 2026-08-04
---

# Classifier-v2 Immediate Follow-up Hotfix Summary

**Production proved that classifier-v2 made safe progress but slept the full
300-second Structure cadence after every non-terminal slice. The hotfix restores
the already-approved approximately 100 ms resident continuation contract.**

## Production reproduction and root cause

- Fly release 237 enabled only classifier-v2 on exact release SHA
  `c45dd166b07c6386a07c218bd3d63e578980c621`, keeping generation reads on
  `legacy` and Quote disabled.
- Attempts 259-262 checkpointed real `source-events` and `source-markets`
  progress, but their starts were approximately five minutes apart. For
  example, attempt slices ended at `15:42:10Z`, `15:47:24Z`, `15:52:38Z`, and
  `15:57:54Z`.
- `_maybe_advance_structure_event_members` and the normal Structure checkpoint
  path set `_checkpoint_pending=True`; the equivalent successful, non-terminal
  drift path recorded progress and returned without setting it. The outer loop
  therefore chose `_effective_cadence_s` instead of its 100 ms continuation.

## TDD repair

- RED: the existing scheduler integration test asserted that a real
  non-terminal drift checkpoint schedules immediate follow-up. It failed with
  `_checkpoint_pending == False`.
- GREEN: after durable non-terminal drift progress, set
  `_checkpoint_pending=True`. The next admission still reacquires the shared
  producer lock and rechecks Quote active/due state; terminal/deferred/failure
  behavior is unchanged.

## Verification

- RED command:
  `uv run pytest -q tests/m1-perception/test_scheduler.py::test_pending_structure_drift_slice_precedes_snapshot_child`
- Drift/event-member scheduler tests: 40 passed.
- Complete scheduler, Quote wiring, and health files: passed.
- Full M1 suite: 2,785 tests, zero failures/errors, two skips,
  `1245.124s`; JUnit `/tmp/m1-drift-followup-hotfix-junit.xml`.
- Scoped Ruff and scoped `git diff --check`: passed. Repository-wide
  `git diff --check` still reports only the user's pre-existing
  `.superpowers/sdd/task-7-brief.md` trailing blank line, which remains untouched.

## Production boundary

Release 237 remains safe but slow: it is still `legacy`, Quote is off, and the
classifier continues naturally at the old cadence. Deploying this hotfix
requires a new exact-SHA approval. No read cutover or Quote enablement is allowed
until the hotfixed classifier produces an authenticated `drift-safe-sealed`
receipt and the read-only comparator passes.

---
*Phase: 05.6-self-healing-structure-production*
*Completed: 2026-08-04 (local hotfix; production Task 8 remains open)*
