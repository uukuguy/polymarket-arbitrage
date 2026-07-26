---
phase: 05-ws-book-prices
plan: 07
subsystem: l3-continuity
tags: [websocket, l3, evidence, promoter, sampler, fly-io]
status: in_progress
requires:
  - phase: 05.4-continuous-l3-soak-evidence
    provides: strict continuous evidence and 120-second per-market freshness
provides:
  - bounded quiet-refresh timeout recovery
  - generation-scoped prepared L3 targets
  - atomic make-before-break membership publication
  - sampler transition serialization
affects: [05-06-operational-closure, polywatch, l3-soak]
tech-stack:
  added: []
  patterns:
    - generation-scoped evidence before membership publication
key-files:
  created:
    - .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-07-PLAN.md
    - docs/learning/24-L3-连续性事务.md
  modified:
    - src/polyarb/daemon/ws_consumer.py
    - src/polyarb/observation/l3_evidence.py
    - src/polyarb/observation/l3_sampler.py
key-decisions:
  - "Do not relax the 120-second threshold or debounce first-failure alerts."
patterns-established:
  - "A business-evidence timeout records bounded failure but preserves a control-consistent WebSocket generation."
  - "A prepared target contains exact durable evidence for one generation and publishes no membership."
  - "Sampling waits across both promoter publication and reconnect membership convergence."
requirements-completed: []
updated: 2026-07-26
---

# Phase 05 Plan 07: L3 Continuity Boundary Repair — In Progress

**Release 73 was rejected by its first real promoter boundary; the corrected
non-destructive timeout and reconnect-sampling repair is green locally and
awaits L2-only redeployment.**

## Completed

- Reproduced the production timeout defect with a failing test.
- Changed evidence timeout handling to emit bounded counts without token IDs.
- Release 73 proved that compensating a control-consistent socket was
  destructive: the `08:04:13Z` promoter timeout closed generation 1 and
  durable samples 11/12 persisted 8/10; `08:08:01Z` quiet refresh closed
  generation 2 after missing two identities.
- Corrected the contract: a confirmed final subscribe plus missing business
  evidence preserves the socket and exact missing set. Control-send failure,
  cancellation, and generation drift still compensate only the captured
  generation.
- An unchanged exact 10/10/10 promoter target reuses current-generation
  durable evidence with zero subscription controls.
- Focused verification: 77 tests passed; Ruff passed.
- Added immutable `PreparedL3Target` carrying exact target evidence for one
  WebSocket generation.
- Preparation accepts only successful durable depth writes and leaves the old
  desired/committed/evidenced snapshot untouched.
- Commit sends subscribe-before-unsubscribe and publishes the exact new
  desired/committed/evidenced set once; ambiguous controls compensate the
  captured generation.
- Sampling waits on the shared runtime transition lock and on reconnect
  membership convergence; strict live health still exposes partial membership
  immediately, while no durable 8/10 or 9/10 sample is written.
- Task 2 verification: 71 focused tests passed; Ruff passed.
- Replaced promoter `set desired → remove → add` publication with exact target
  preparation followed by one atomic commit.
- Promoter success now requires five mapped markets and exact
  desired/committed/evidenced counts of 10/10/10.
- Mapping mirror, membership commit, terminal ledger append, and cached mapping
  publication are serialized against the sampler; failed terminal persistence
  restores prior reconnect intent and compensates the current generation.
- Added a chaos-chain regression proving a sample cannot persist a new mapping
  while only 9/10 target identities are evidenced.
- Task 3 verification: 150 promoter/chaos/health/consumer tests passed; Ruff passed.
- Full pytest exited 0 with the established one xfail and one skip. Changed
  Python files pass Ruff, the M1 manual contract passes, and planning status
  reports no drift.
- Repository-wide Ruff still reports 36 inherited findings confined to
  unrelated Alembic/legacy scripts/climb files; this is recorded as baseline
  debt rather than falsely reported as green.
- Merged the already-qualified resident two-minute Fly monitor and documented
  the exact operator checks and failure meanings.
- Corrected-repair verification: 232 focused L2/L3 tests passed, changed-file
  Ruff passed, and the full repository pytest suite exited zero with the
  established one xfail and one skip.

## RED Evidence

`test_evidence_timeout_records_failure_without_closing_healthy_generation`
failed because release-73 source closed the healthy socket.

`test_prepare_unchanged_exact_target_reuses_current_evidence_without_controls`
timed out and returned no prepared target because release-73 source cycled all
ten identities even when the target was unchanged.

`test_reconnect_requires_current_generation_sample_before_health_recovers`
persisted immediately at 8/10 because the sampler gated promoter commits but
not reconnect convergence.

`test_sample_once_waits_for_membership_transition_lock` failed because the
runtime exposed no transition lock; prepared-target tests initially had no
consumer APIs to call.

`test_rotation_never_reports_success_or_publishes_partial_evidence` initially
returned the legacy `desired_update_failed` path, and the prepared-target
success regression showed the promoter never called either new transaction API.

## GREEN Evidence

```text
77 focused tests passed
71 target transaction and sampler tests passed
150 promoter, chaos-chain, health, and consumer tests passed
232 corrected-repair focused tests passed
Full pytest: exit 0 (one xfail, one skip)
Changed-file Ruff: All checks passed
M1 manual contract: OK
Planning status: no drift
Ruff: All checks passed
```

## Remaining

- Corrected L2-only deployment, real promoter/quiet-refresh/reconnect boundary
  verification, and a new exact 24-hour L3 evidence window. Release 73 and its
  boot are permanently rejected.

---
*Phase: 05-ws-book-prices*
*Plan: 07*
*Status: in progress*
