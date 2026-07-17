---
phase: 08-venue-truth-fill-reconciliation
plan: 01
subsystem: venue-truth-reconciliation
tags: [settlement, money, sqlite, fingerprint, idempotency, tdd]
requires:
  - phase: 07-durable-partial-fill-accounting
    provides: Canonical fill identity and exact remaining quantity/cost basis
provides:
  - Exact structured settlement receipt codec
  - Atomic operation request fingerprint migration and replay conflict
affects: [position-tracker, execution-engine, cli, live-venue-adapter]
tech-stack:
  added: []
  patterns: [tagged-settlement-receipt, fingerprinted-operation-replay, overlapping-writer-proof]
key-files:
  created: []
  modified:
    - src/polyarb/routing/position_repository.py
    - tests/routing/test_position_repository.py
key-decisions:
  - "Fingerprint equality is strict: equal replays; any mismatch, including empty/non-empty, conflicts."
  - "Venue settlement receipts store gross, fee, net, and PnL as tagged integer micros."
requirements-completed: []
status: in-progress
completed: null
---

# Phase 8 Plan 01: Venue-Truth Fill Reconciliation Summary

> In-progress SUMMARY anchor created immediately after the first Phase 8 production
> commit. It will be completed only after tracker, Engine/CLI, full gates, teaching,
> and H-006 climb adjudication finish.

## Completed Slice: Repository Receipt and Fingerprint

- RED `2791f90`: 13 failures specified structured exact receipts, migration,
  fingerprint replay matrix, corrupt JSON, and overlapping SQLite writers.
- GREEN `811640a`: `SettlementReceipt` exact codec, additive
  `request_fingerprint TEXT NOT NULL DEFAULT ''`, in-memory/SQLite parity, and
  comparison inside `BEGIN IMMEDIATE`.
- Focused evidence: **59 repository tests passed**; targeted Ruff passed.
- Concurrency evidence: writer A pauses inside its transaction, writer B begins a
  conflicting apply, A commits, then B observes the committed fingerprint and fails;
  one operation row and one balance mutation remain.

## Remaining

- VenueSettlement tracker transition and formula-supersession proof.
- Engine/CLI/Makefile subprocess response-loss proof.
- Teaching chapter 17, full regression, learnings/state closure, and H-006 climb cycle.

---
*Phase: 08-venue-truth-fill-reconciliation — in progress*
