---
phase: 06-unit-safe-execution-accounting
plan: 01
subsystem: dimension-safe-execution-accounting
tags: [quantity, money, collateral, sqlite-migration, cli, tdd]

requires:
  - phase: 05-exact-cash-ledger
    provides: Exact Money authority, additive SQLite migration, and tagged receipts
provides:
  - Frozen exact micro-share Quantity distinct from micro-pUSD Money
  - Explicit execution, position, and full-fill quantity contracts
  - Side-aware BUY/SELL cash collateral and quantity-based PnL
  - Transactional Phase 5 quantity/cost-basis migration with one-time balance repair
  - Explicit operator quantity/cost-basis views with legacy aliases
affects: [partial-fills, venue-adapter, fees, reconciliation, execution-risk]

tech-stack:
  added: []
  patterns: [dimension-types, fully-collateralized-paper-short, transactional-equity-repair, compatibility-projection]

key-files:
  created:
    - src/polyarb/routing/quantity.py
    - tests/routing/test_quantity.py
    - tests/routing/test_unit_safe_accounting.py
    - docs/learning/15-成交数量与现金不是一回事.md
  modified:
    - src/polyarb/routing/money.py
    - src/polyarb/models/signal.py
    - src/polyarb/routing/engine.py
    - src/polyarb/routing/orchestrator.py
    - src/polyarb/routing/position_tracker.py
    - src/polyarb/routing/position_repository.py
    - src/polyarb/execution/engine.py
    - src/polyarb/cli_arbitrage.py

key-decisions:
  - "Quantity is exact micro-shares; Money is exact micro-pUSD, and matching scales never make them interchangeable."
  - "BUY collateral is q*p; paper SELL collateral is q*(1-p); modeled PnL is q times the side-aware price delta."
  - "Legacy Phase 5 stake_micros is quantity evidence, not cash cost basis; migration repairs old over-reserved balance exactly once."
  - "size/stake/filled_size remain compatibility projections, while all internal accounting reads explicit quantity/cost basis."

requirements-completed: [H-004]
completed: 2026-07-17
---

# Phase 6 Plan 01: Unit-Safe Execution Accounting Summary

**M2 now represents outcome-token shares and pUSD cash as different exact dimensions, so open-position balance, exposure, full-fill validation, and PnL can all be correct at the same time.**

## Accomplishments

- Added non-negative exact `Quantity(micros)` with six-decimal HALF_EVEN conversion,
  bool/non-finite/overflow rejection, and Quantity-only arithmetic.
- Made `ExecutionLeg.quantity_value`, `Position.quantity_value`,
  `Position.cost_basis_money`, and `Fill.filled_quantity_value` authoritative; old
  `size/stake/filled_size` values are read-only compatibility projections.
- Corrected paper cash flow: BUY 100 @ .50 moves balance 1000 → 950; full close @ .60
  returns collateral + PnL and ends at 1010. SELL 100 @ .60 reserves 40 and closes @
  .50 at the same 1010.
- Added SQLite v3 `quantity_micros/cost_basis_micros` authority. Phase 5 databases
  derive both under `BEGIN IMMEDIATE` and refund legacy over-reservation in the same
  transaction; restart validates and never refunds twice.
- Added explicit CLI/decision JSON `quantity` and `cost_basis` while preserving legacy
  `size/stake`, and taught the side-dependent official market-order `amount` hazard.

## TDD Commits

1. Quantity/cash conversion RED — `98122ec`
2. Exact Quantity GREEN — `57ae184`
3. Execution lifecycle RED — `c4806b1`
4. Explicit quantity/collateral GREEN — `d2b0e00`
5. SQLite v3 migration RED — `6b99e7d`
6. Transactional migration/equity repair GREEN — `1f11208`
7. Operator contract RED — `6faa75c`
8. Operator quantity/cost-basis GREEN — `f8a5ec0`
9. Teaching chapter — `f6848bd`

**Knowledge/design/planning:** `ee08dbc`, `a31192b`.

## Delivered Contracts

- `Quantity.from_value` produces a non-negative integer micro-share count. It cannot be
  added to Money or used where Money is required.
- `Money.collateral_for(quantity, price, side)` and
  `Money.pnl_for(quantity, entry, exit, side)` are the only modeled shares→cash
  boundaries in the position lifecycle.
- `ExecutionLeg.size`, `Position.stake`, and `Fill.filled_size` still expose shares for
  legacy callers; new internal code reads `quantity_value` and `cost_basis_money`.
- Tracker balance/exposure compare Money cost basis. Full-fill equality compares
  Quantity. A rejected partial fill creates no receipt and leaves state unchanged.
- Fresh/migrated SQLite rows require integer `quantity_micros` and
  `cost_basis_micros`; every load validates dynamic types and cost-basis consistency.
- CLI status and routing output add explicit `quantity/cost_basis` without removing
  existing keys or adding a new executable command.

## Verification Evidence

### RED → GREEN

- Quantity RED: both test modules failed import because `quantity.py` did not exist;
  GREEN focused gate: **41 passed**.
- Domain RED: **7 failures** for missing explicit fields/constructors and wrong cash
  flow; GREEN unit-safe lifecycle: **7 passed** and routing/execution suite green.
- Migration RED: **5 failures** for missing columns, absent balance repair, missing
  idempotence, partial authority acceptance, and invalid-side acceptance; GREEN
  repository gate: **46 passed**.
- Operator RED: **2 failures** for absent explicit JSON fields; GREEN CLI gate:
  **14 passed**.

### Corrected full M2 gate

```bash
uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q
```

Result: **219 passed**, with existing `datetime.utcnow` deprecation warnings only.

Additional evidence:

- `uv run pytest tests/test_makefile.py -q` — **3 passed**
- targeted Ruff across changed production/operator modules — all checks passed
- `git diff --check` — clean
- four-process run/status/close/status test — exact open balance/exposure and raw v3
  integer authority, followed by correct close PnL
- literal Phase 4 and Phase 5 fixtures — additive migration, receipt preservation,
  equity repair, restart idempotence, and rollback on invalid/partial authority

### Raw lifecycle vector

For CLI `run --mid .40 --stake 100`:

```json
{
  "balance": 960.0,
  "max_exposure": 40.0,
  "quantity": 100.0,
  "cost_basis": 40.0,
  "quantity_micros": 100000000,
  "cost_basis_micros": 40000000
}
```

SQLite reports both new columns with dynamic type `integer`. Closing at .50 realizes
10 pUSD and returns final balance 1010.

## Migration and Failure Semantics

- No v3 columns means a legitimate Phase 5 database: add both, derive every position,
  repair balance, validate, and commit together.
- Exactly one v3 column means corrupt partial authority: fail closed without adding the
  other column or changing balance.
- Both columns mean migrated authority: validate only; never reinterpret legacy stake
  or repeat the refund.
- Invalid side/price/quantity, negative shares, non-finite input, overflow, wrong
  SQLite dynamic type, and inconsistent cost basis abort without partial state.
- A Phase 4 open BUY 100 @ .40 with stored balance 920 first becomes exact Phase 5
  money, then v3 refunds 60 to balance 980 while preserving realized PnL and receipts.

## Deviations from Plan

1. Safe Ruff auto-fixes normalized pre-existing import/type style in touched production
   modules so the new targeted gate could cover the whole changed file.
2. Operator output retains both old and explicit fields rather than deleting ambiguity
   in one release; this is the approved compatibility seam, not a second authority.
3. No new Makefile target was added because all behavior is exercised through existing
   `eval-arb/run-arb/status-arb/close-arb` commands.

## User Setup Required

None — no dependency, credential, wallet, live venue, or network mutation is required.

## Next Phase Readiness

- H-005 can now aggregate immutable fills against exact remaining Quantity without
  comparing shares to cash.
- H-006 can later replace modeled cost/proceeds/fee with venue-confirmed Money while
  preserving the Quantity boundary.
- Phase 6 still requires metadata/learnings closure, zero-drift confirmation, and the
  H-004 climb cycle before it is marked complete.

---
*Phase: 06-unit-safe-execution-accounting*
*Completed: 2026-07-17*
