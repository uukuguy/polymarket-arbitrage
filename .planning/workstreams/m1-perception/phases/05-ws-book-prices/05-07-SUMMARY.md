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
  modified:
    - src/polyarb/daemon/ws_consumer.py
    - src/polyarb/observation/l3_evidence.py
    - src/polyarb/observation/l3_sampler.py
key-decisions:
  - "Do not relax the 120-second threshold or debounce first-failure alerts."
patterns-established:
  - "A timed-out evidence barrier invalidates its captured WebSocket generation."
  - "A prepared target contains exact durable evidence for one generation and publishes no membership."
  - "Sampling and promoter publication share one transition lock."
requirements-completed: []
updated: 2026-07-26
---

# Phase 05 Plan 07: L3 Continuity Boundary Repair — In Progress

**Quiet-refresh recovery and the atomic target transaction are green locally;
promoter integration, deployment, and repaired-release evidence remain in progress.**

## Completed

- Reproduced the production timeout defect with a failing test.
- Changed evidence timeout handling to emit bounded counts without token IDs.
- Compensates only the captured WebSocket generation.
- Focused verification: 77 tests passed; Ruff passed.
- Added immutable `PreparedL3Target` carrying exact target evidence for one
  WebSocket generation.
- Preparation accepts only successful durable depth writes and leaves the old
  desired/committed/evidenced snapshot untouched.
- Commit sends subscribe-before-unsubscribe and publishes the exact new
  desired/committed/evidenced set once; ambiguous controls compensate the
  captured generation.
- Sampling waits on the shared runtime transition lock.
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

## RED Evidence

`test_evidence_timeout_records_failure_and_compensates_captured_generation`
failed because the old code awaited zero socket closes.

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
Ruff: All checks passed
```

## Remaining

- Full verification, L2-only deployment, and new exact 24-hour L3 evidence.

---
*Phase: 05-ws-book-prices*
*Plan: 07*
*Status: in progress*
