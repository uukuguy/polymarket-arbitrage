---
quick_id: 260720-book-order-normalization
ws: m1-perception
subsystem: daemon
tags: [polymarket, websocket, orderbook, top-of-book, l3, production]

requires:
  - phase: 05.3-l3-prerequisite-repair
    provides: Representative L3 seed and complete Yes/No token projection

provides:
  - Price-ranked Polymarket book projection independent of upstream array order
  - Correct best bid/ask, spread, top-10 notional depth, and persisted level rank
  - Regression coverage for the production-observed worst-first book shape
  - Manual contract registry coverage for make scan-l3-seed

affects:
  - Phase 05 Plan 06 production L3 promotion and book-level proof
  - Any strategy or dashboard consuming l2_top_of_book or l2_book_levels

tech-stack:
  added: []
  patterns:
    - "Normalize at the ingestion boundary: BUY descending and SELL ascending"
    - "TOB, depth, and persisted levels share one ranked-level helper"

key-files:
  created:
    - .planning/quick/260720-book-order-normalization/PLAN.md
  modified:
    - src/polyarb/daemon/l2_main.py
    - tests/m1-perception/test_l2_main_book_levels.py
    - scripts/check_m1_manual.py
    - docs/learning/11-L3-K线.md

key-decisions:
  - "Do not change the locked L3 spread or depth thresholds; repair their upstream truth"
  - "Do not rely on Polymarket array position even though the observed production shape is deterministic"
  - "Keep invalid/non-positive level filtering before ranking and top-N truncation"

requirements-completed: []
completed: 2026-07-20
---

# Quick 260720: Polymarket Book Order Normalization Summary

The L2 projector now records executable nearest prices instead of the first
array entries, removing the production cause of false ~0.98 spreads and zero L3
promotions.

## Evidence and Accomplishments

- Compared one production `l2_top_of_book` row with the same token's live CLOB
  `/book`: stored `0.001 / 0.999` versus actual best `0.369 / 0.377`.
- Added RED tests that reproduced bids ascending and asks descending; both TOB
  and book-level tests failed on the old implementation.
- Added `_ranked_book_levels`: filters malformed/non-positive entries, ranks
  BUY high-to-low and SELL low-to-high, then feeds all three projections.
- Preserved locked L3 recipe thresholds; the bug was upstream observation
  truth, not market eligibility policy.
- Closed the full-suite manual-contract drift by registering the existing
  `scan-l3-seed` Make target.
- Added the observed array-order trap and the correct mental model to the Phase
  05 L3 teaching document.

## Verification

- Focused projector and mirror/health regressions: 40 passed.
- Manual contract plus projector suite: 63 passed.
- Full repository: 1431 passed, 1 skipped, 1 xfailed.
- Ruff: passed on all touched Python files.
- `git diff --check`: passed.

An optional direct Pyright invocation still reports two pre-existing findings
outside the changed lines: an Optional key in `scripts/check_m1_manual.py` and
the established bool-returning L2 dispatcher callback against a None callback
annotation. Pyright is not a repository gate for this task.

## Production Boundary

The repair shipped in L2 releases 35 and 36. Release 36 production TOB showed
all five promoted Yes markets at spread 0.01, and real level-one rows included
adjacent BUY 0.75 / SELL 0.76. No trading path, funds, exchange order, or
24-hour soak was started here.

---
*Quick task: 260720-book-order-normalization*
*Completed: 2026-07-20*
