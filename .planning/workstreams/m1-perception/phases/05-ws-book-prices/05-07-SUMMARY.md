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

**Release 73 was rejected by its first real promoter boundary; corrected
release 75 is live, its real promoter/quiet-refresh boundaries and T0 passed,
and the immutable 24-hour continuity interval is actively monitored.**

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
- Deployed the same corrected image by digest as Fly release 75. Exact runtime
  identity is machine `85e647c4eed598`, instance
  `01KYES89KD9WA8VV9V2B3PJV7R`, boot
  `d029c2ea-e357-4ce2-8f7c-6c4e11867254`, release
  `9f385cacc104fa54dd444151a8c4ecb423e94dde`, digest
  `sha256:f0d39892207577bb024995d76e91f5c0b8c0a88fd8e2839e182d25125da16ad5`.
- Real promoter runs at `08:39:43Z` and `08:44:43Z` both completed
  5/10/10/10 on generation 1. A later real quiet refresh also completed
  without compensation; all persisted samples remained 10/10/10.
- Bound corrected manifest `3ad69a90…` before its exact T0
  `2026-07-26T08:51:13.206077Z`. Its T0 report is PASS with one health row,
  five market rows, zero events, maximum gap 24.989517 seconds, and maximum
  freshness 36.893 seconds.
- Fly Polywatch continues two-minute live fault detection. A separate local
  `launchd` operator job handles the immutable T6/T12/T18/T24 artifact
  boundaries without a foreground wait.

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

- Collect immutable PASS reports at T+6/T+12/T+18/T+24 and run the final
  mechanical verifier. No final continuity claim is permitted before
  `2026-07-27T08:51:13.206077Z`.
- Reconcile this repaired-window result with the independent L1 quote interval
  and finish Plan 05-06/Phase 05 closure. Release 73 and both incorrectly
  located/corrected-manifest preflights remain immutable rejected evidence.

---
*Phase: 05-ws-book-prices*
*Plan: 07*
*Status: in progress*
