---
phase: 07-durable-partial-fill-accounting
plan: 01
subsystem: durable-partial-fill-ledger
tags: [partial-fill, quantity, money, sqlite, idempotency, cli, restart]

requires:
  - phase: 06-unit-safe-execution-accounting
    provides: Exact Quantity, Money cost basis, and v3 SQLite authority
provides:
  - Exact remaining quantity and cost-basis transitions for every partial fill
  - Canonical venue fill identity with transactionally durable receipt replay
  - Engine and subprocess response-loss recovery proofs
  - Operator CLI and Makefile partial-fill surface
affects: [venue-adapter, fee-reconciliation, order-monitoring, execution-risk]

tech-stack:
  added: []
  patterns: [remaining-authority, final-residual-allocation, venue-fill-idempotency, response-loss-replay]

key-files:
  created:
    - docs/learning/16-部分成交如何不重不漏.md
  modified:
    - src/polyarb/routing/money.py
    - src/polyarb/routing/position_tracker.py
    - src/polyarb/execution/engine.py
    - src/polyarb/cli_arbitrage.py
    - Makefile

key-decisions:
  - "Position quantity and cost basis represent remaining authority after every fill."
  - "A venue fill always owns canonical operation identity venue-fill:{fill_id}; caller IDs cannot override it."
  - "Partial allocation uses HALF_EVEN, while the final fill consumes all residual cost basis."
  - "Anonymous partial, zero fill, overfill, and identity conflict fail without mutation."

requirements-completed: [H-005]
completed: 2026-07-17
---

# Phase 7 Plan 01: Durable Partial-Fill Accounting Summary

**M2 now applies every immutable venue fill exactly once, preserves exact remaining shares and cash cost basis across restart, and recovers committed partial-fill results after process response loss.**

## Accomplishments

- Converted position quantity and cost basis into remaining authority after each fill.
- Added exact proportional cost allocation, with the final fill consuming the complete
  residual so repeated rounding cannot leave cash dust.
- Made `venue-fill:{fill_id}` the non-overridable repository operation identity and
  proved duplicate delivery through a restarted Engine cannot mutate twice.
- Added `close --fill-id` and `make close-arb ... size=... fill_id=...`, preserving the
  existing full-close and caller `operation_id` compatibility paths.
- Added a true subprocess proof: first partial close commits while stdout is discarded;
  retry recovers its receipt, leaves q=70/cost=28, and a second fill closes at exact
  balance 1008.5 / realized PnL 8.5.

## TDD Commits

1. Partial transition and failure vectors RED — `ccf8778`
2. Exact residual/canonical fill implementation GREEN — `eda5cf9`
3. Climb implementation checkpoint — `92786f2`
4. Engine/CLI/Makefile response-loss slice and phase closure — phase closure commit

## Delivered Contracts

- A fill quantity must be positive and no larger than the current remaining Quantity.
- A partial fill must have a non-empty immutable venue fill ID.
- Duplicate fill delivery returns the stored Money receipt without re-running mutation.
- Cost basis is allocated against current remaining authority; full consumption returns
  the entire residual rather than re-quantizing a ratio.
- Mutation, remaining position projection, account cash, realized PnL, and receipt are
  committed atomically by the repository.
- Full anonymous operator/paper closes remain compatible; H-006 owns actual venue
  proceeds, fees, and reconciliation.

## Verification Evidence

- Corrected full M2 gate: **227 passed**.
- Engine + subprocess focused gate: **25 passed** after RED→GREEN.
- Makefile contract: **4 passed**.
- `git diff --check`: clean.
- Targeted Ruff over all changed Python files: clean.
- Raw response-loss vector: BUY 100 @ .40 → fill 30 @ .45 → retry unchanged at
  q=70/cost=28/balance=973.5 → fill 70 @ .50 → q=0/balance=1008.5/PnL=8.5,
  with exactly two close receipts.

## Deviations from Plan

None. The existing operations table was sufficient; no new fill ledger, executable
command, dependency, live SDK, or fee model was introduced.

## User Setup Required

None — the complete proof is local and requires no credentials or external venue.

## Next Phase Readiness

- H-005 is ready for deterministic climb adjudication.
- H-006 can replace modeled proceeds/PnL with exact venue-confirmed shares, cash, and
  fees while preserving fill identity and remaining-quantity semantics.

---
*Phase: 07-durable-partial-fill-accounting*
*Completed: 2026-07-17*
