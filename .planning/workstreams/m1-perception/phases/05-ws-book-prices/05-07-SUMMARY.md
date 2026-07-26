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
key-decisions:
  - "Do not relax the 120-second threshold or debounce first-failure alerts."
patterns-established:
  - "A timed-out evidence barrier invalidates its captured WebSocket generation."
requirements-completed: []
updated: 2026-07-26
---

# Phase 05 Plan 07: L3 Continuity Boundary Repair — In Progress

**Quiet-refresh timeout recovery is green locally; atomic market rotation,
deployment, and repaired-release evidence remain in progress.**

## Completed

- Reproduced the production timeout defect with a failing test.
- Changed evidence timeout handling to emit bounded counts without token IDs.
- Compensates only the captured WebSocket generation.
- Focused verification: 77 tests passed; Ruff passed.

## RED Evidence

`test_evidence_timeout_records_failure_and_compensates_captured_generation`
failed because the old code awaited zero socket closes.

## GREEN Evidence

```text
77 focused tests passed
Ruff: All checks passed
```

## Remaining

- Prepared 10-token target evidence.
- Atomic mapping/membership publication.
- Sampler transition serialization.
- Full verification, L2-only deployment, and new exact 24-hour L3 evidence.

---
*Phase: 05-ws-book-prices*
*Plan: 07*
*Status: in progress*
