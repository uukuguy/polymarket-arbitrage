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
  - CONFIRMED venue cash transition with exact net cash and realized PnL
  - Reconstructible CLI/Makefile delivery and subprocess response-loss recovery
affects: [position-tracker, execution-engine, cli, live-venue-adapter]
tech-stack:
  added: []
  patterns: [tagged-settlement-receipt, fingerprinted-operation-replay, overlapping-writer-proof]
key-files:
  created: []
  modified:
    - src/polyarb/routing/position_repository.py
    - src/polyarb/routing/position_tracker.py
    - src/polyarb/cli_arbitrage.py
    - Makefile
    - tests/routing/test_position_repository.py
key-decisions:
  - "Fingerprint equality is strict: equal replays; any mismatch, including empty/non-empty, conflicts."
  - "Venue settlement receipts store gross, fee, net, and PnL as tagged integer micros."
requirements-completed: [H-006]
status: complete
completed: 2026-07-17
---

# Phase 8 Plan 01: Venue-Truth Fill Reconciliation Summary

**M2 now replaces modeled fill cash with complete terminal venue truth, persists an
exact structured receipt, and rejects every changed retry atomically across processes.**

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

## Completed Slice: Tracker Venue Truth

- RED `a6cde69`: 13 expected failures specified terminal settlement validation,
  wrong-price supersession, restart replay, and changed-payload conflicts; the
  modeled compatibility case already passed.
- GREEN `b581e03`: frozen `VenueSettlement`, deterministic versioned request
  fingerprint, and exact `net = gross - fee`, `PnL = net - allocated cost`
  transition returning the durable `SettlementReceipt`.
- Focused evidence: **14 venue reconciliation tests passed**, then **102 tracker +
  repository tests passed**; targeted Ruff passed.

## Completed Slice: Engine and Operator Delivery

- RED `84509de`: delivery-boundary tests specified Engine fact forwarding,
  all-or-none CLI inputs, true subprocess response loss, changed-fee conflict,
  and Makefile flag forwarding.
- GREEN `bfecdf6`: CLI reconstructs complete venue input on every retry and
  always invokes repository fingerprint adjudication; Makefile exposes the
  exact venue fields through the existing `close-arb` target.
- Focused evidence: **43 Engine/CLI/process/Makefile tests passed**; targeted
  Ruff and `git diff --check` passed. The changed-fee subprocess returned 2
  while the SQLite file remained byte-identical.

## Verification Evidence

- Full M2 regression: **260 passed**.
- Makefile + climb adapter gate: **21 passed** after clearing the completed in-flight
  baton.
- Repository/tracker focused: **102 passed**; Engine/CLI/process/Makefile: **43 passed**.
- Targeted Ruff over every changed Python file: clean; `git diff --check`: clean.
- `make planning-status`: zero drift.
- H-006 climb cycle 6: planning/unit/integration/CLI/restart =
  **100/100/100/100/100**, total **100**, verdict **confirmed**.

## Deviations from Plan

- Engine required no production edit: it already forwarded the complete `Fill` object
  without doing fee math. New tests lock that fact-forwarding boundary.
- The SUMMARY anchor was created after Task 1 rather than Task 3, strengthening the
  project invariant without changing the delivered contract.

## Pre-Merge Fail-Closed Corrections

- Durable position booking is now part of execution success: a venue-successful leg
  whose `open_position` is rejected becomes failed, cannot enter the close path, and
  cannot report expected profit.
- Modeled immutable fill IDs now bind exact quantity and canonical exit price through
  the repository fingerprint, so changed retries conflict instead of replaying stale
  results.
- CLI fill-ID recovery requires explicit size and reconstructs the original request on
  every retry; subprocess coverage locks changed-payload rejection.
- Removed the climb post-commit auto-amend hook. `tools/climb/cycle.py` already performs
  research-tree regeneration in the command that owns the state mutation.
- Regression evidence: 192 focused execution/accounting/CLI/Makefile tests passed;
  targeted Ruff, `git diff --check`, and `make planning-status` passed.
- Final review correction binds canonical exit price into venue-settlement fingerprints
  because partial fills persist it as remaining-position mark price. Tracker and true
  subprocess retries now reject changed exit price; the final M2 gate is **272 passed**.

## User Setup Required

None. This phase performs no live venue authentication, signing, allowance mutation,
or network write.

## Next Phase Readiness

H-006 is ready for deterministic climb adjudication. The next live-facing phase may
implement an adapter that supplies auditable terminal gross/fee facts, but must not
weaken finality, fingerprint, or remaining-authority contracts.

---
*Phase: 08-venue-truth-fill-reconciliation — completed 2026-07-17*
