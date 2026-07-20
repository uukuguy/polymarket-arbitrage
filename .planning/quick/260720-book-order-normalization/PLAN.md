# Quick 260720 — Polymarket Book Order Normalization

## Goal

Repair the production L2/L3 book projector so top-of-book, top-10 depth, and
`l2_book_levels` reflect executable nearest prices regardless of upstream array
order.

## Root Cause

The 2026-07-20 production rollout compared one stored row with the same live
CLOB `/book` response. Polymarket supplied bids ascending and asks descending:
the farthest price was first and the best price was last. The projector trusted
index zero and therefore stored spreads around 0.98 instead of the live 0.008,
making the unchanged L3 promotion recipe select zero markets.

## Scope

- RED tests reproducing the observed worst-first array order;
- shared valid-level ranking: BUY descending, SELL ascending;
- use the ranked order for best prices, top-10 depth, and persisted book levels;
- register the Phase 05.3 `scan-l3-seed` manual command in the existing manual
  contract checker, closing the repository-level regression found by full pytest;
- update the L3 teaching document with the production lesson;
- redeploy L2 only and prove the production chain without trading or starting
  the 24-hour soak implicitly.

## Verification

- focused RED then GREEN projector tests;
- `make test`;
- Ruff, manual contract, planning status, and diff checks;
- production `/health`: `l3:active_count == 10` and fresh book-level write;
- production database: real `l2_book_levels` rows for active L3 assets.
