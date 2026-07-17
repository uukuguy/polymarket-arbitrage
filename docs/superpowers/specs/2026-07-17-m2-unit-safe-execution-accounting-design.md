# M2 Unit-Safe Execution Accounting Design

**Date:** 2026-07-17  
**Workstream:** `m2-combinatorial`  
**Hypothesis:** H-004  
**Status:** approved for autonomous climb execution

## Problem

M2 currently uses one `size/stake/filled_size` value as both outcome-token shares and
pUSD. Routing multiplies it by price, the account deducts it directly as cash, and PnL
multiplies it by a price delta. For 100 shares bought at 0.50, correct collateral is
50 pUSD and PnL at 0.60 is 10 pUSD; the current account deducts 100 while obtaining the
10 only by treating that same alleged cash value as shares.

Official V2 order semantics make the distinction mandatory: limit `size` is shares;
market BUY `amount` is pUSD; market SELL `amount` is shares. FAK and cumulative
`size_matched` make share quantity the primary partial-fill dimension.

## Selected architecture

### Exact `Quantity`

Add a frozen `Quantity(micros: int)` with `1 share = 1_000_000 micro-shares`.
Conversion uses `Decimal(str(value))`, HALF_EVEN, exact-int validation, and the signed
SQLite range. Addition/subtraction compare only Quantity. It is distinct from Money
even though both currently use six decimal places.

### Explicit execution/domain fields

- `ExecutionLeg.quantity` is shares. Deprecated `size` is a read-only compatibility
  alias; routing/execution internals do not read it.
- `Position.quantity_value` is Quantity and `cost_basis_money` is reserved pUSD.
  For BUY, collateral is `quantity * entry_price`; for synthetic fully-collateralized
  SELL, collateral is `quantity * (1 - entry_price)`.
- `Position.pnl_money` is `quantity * price_delta`, quantized once to Money.
- `Fill.filled_quantity_value` is Quantity. Deprecated constructor/property
  `filled_size` remains a compatibility seam, never an accounting authority.
- `stake` remains a deprecated float alias for quantity so existing CLI input/output
  does not silently change dimensions; new output also names `quantity` and
  `cost_basis` explicitly. `stake_money` becomes a compatibility alias for cash cost
  basis, matching its type/name.

### Cash flow

Opening checks and deducts `cost_basis_money`, not quantity. Closing a full fill returns
`cost_basis_money + pnl_money`. Exposure is reserved cash collateral. Fill equality is
Quantity-to-Quantity. Prices stay float-facing but convert through decimal text at the
quantity-to-cash boundary.

### SQLite v3 migration

Add `quantity_micros` and `cost_basis_micros` to open positions. A Phase 5 row's
`stake_micros` is interpreted as legacy quantity, because all producers passed
`ExecutionLeg.size` and routing computed `price * size`.

Under the existing `BEGIN IMMEDIATE` initialization transaction:

1. add missing v3 columns;
2. validate side/price/legacy quantity;
3. derive exact cost basis;
4. repair account balance by `legacy_quantity_as_money - derived_cost_basis` for each
   migrated open position, preserving total account equity despite the old over-reserve;
5. populate exact integer columns and validate SQLite dynamic types;
6. commit atomically and make later startups validation-only.

Normal writes use v3 integer authority. Legacy `stake/stake_micros` remain derived
quantity projections for old inspection code; no risk/accounting read uses them.

### Compatibility and scope

No live SDK, keys, signing, orders, fee truth, or network calls. H-004 supports only
full fills; H-005 owns multiple fill IDs, residual quantity, weighted proceeds, and
out-of-order/retry aggregation. Existing Makefile commands remain the executable entry
points, so no new target is needed.

## Failure semantics

- bool/non-finite/negative quantity, invalid price, invalid side, overflow, and corrupt
  SQLite authority fail before/inside the transaction and never partially commit.
- Quantity and Money are not comparable or arithmetically interchangeable.
- A fill quantity mismatch leaves the position and balance unchanged.
- A legacy database with some v3 authority present but invalid/missing fails closed;
  it is not silently re-derived on every restart.

## Verification

1. Unit vectors prove exact quantity conversion and BUY/SELL collateral/PnL.
2. Routing proves 100 shares @ 0.50 yields 50 pUSD modeled notional/collateral.
3. Tracker full lifecycle proves balance 1000 → 950 → 1010 for BUY 100 @ .50→.60.
4. Literal Phase 5 SQLite fixtures prove transactional balance repair, exact integer
   authority, idempotent restart, and response replay preservation.
5. Engine/CLI subprocess tests prove explicit fields and compatibility aliases.
6. Full M2, Makefile, Ruff, diff, planning, teaching, SUMMARY, learnings, JOURNAL, and
   climb H-004 gates must pass.

## Primary references

- https://docs.polymarket.com/trading/orders/create
- https://docs.polymarket.com/trading/orders/overview
- https://docs.polymarket.com/trading/clients/l1
- https://docs.polymarket.com/concepts/positions-tokens
- https://docs.polymarket.com/concepts/pusd

