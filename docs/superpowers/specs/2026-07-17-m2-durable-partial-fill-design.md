# M2 Durable Partial-Fill Accounting Design

**Date:** 2026-07-17  
**Hypothesis:** H-005  
**Status:** approved for autonomous execution

## Goal

Allow one open position to close through multiple immutable venue fills without
double-decrementing quantity, leaking cost basis, or double-booking PnL across retries,
response loss, and process restarts.

## Domain transition

`Position.quantity_value` and `cost_basis_money` become remaining authority. For fill
quantity `f` against remaining `q`:

1. reject `f <= 0` or `f > q` before mutation;
2. allocate remaining cost basis proportionally with HALF_EVEN for a partial fill;
3. if `f == q`, allocate the entire residual cost basis (no rounding dust);
4. calculate modeled fill PnL from `f × side-aware price delta`;
5. release allocated collateral + PnL to balance and add PnL to realized total;
6. subtract exact Quantity and Money residuals; delete only when quantity reaches zero.

This phase records per-fill modeled cash. H-006 may replace it with venue-confirmed
proceeds/fees, but not change fill identity or residual quantity semantics.

## Identity

A venue fill must carry non-empty immutable `fill_id`. Its canonical repository
operation identity is `venue-fill:{fill_id}`; caller-provided IDs cannot override it.
Therefore the same venue fill cannot book twice through another signal/leg/process.
Legacy operator/paper full closes without `fill_id` retain their existing operation-ID
path. A partial fill without `fill_id` fails closed because it cannot be retried safely.

The existing operation receipt stores per-fill realized Money. Retry returns that exact
receipt without re-running the transition. Reusing a fill ID for another market is an
operation target conflict.

## Persistence

No new position table is required: v3 already stores exact remaining quantity and cost
basis. The applied-operation ledger is the fill deduplication ledger via canonical ID.
Every partial mutation and receipt remain in the same `BEGIN IMMEDIATE` transaction.

## Compatibility and non-goals

- Full fills, operator close, paper-close, CLI keys, and Makefile entry points remain.
- `Fill.filled_size` remains a quantity compatibility projection.
- No weighted average entry rewrite, live SDK, order polling, fee truth, or cash
  reconciliation. H-006 owns venue Money truth.

## Acceptance vectors

- BUY 100 @ .40: fill 30 @ .45 leaves q=70/cost=28, balance 973.5, realized 1.5;
  fill 70 @ .50 closes, final balance 1008.5, realized 8.5.
- A lost response for the first fill retries after restart with unchanged q/cost/balance
  and the original receipt.
- Duplicate fill ID under another caller ID cannot double book.
- Overfill, zero fill, anonymous partial fill, and identity conflict roll back fully.
- A micro-share split whose proportional allocation rounds must leave no final cost
  basis dust because the final fill consumes the residual.

