---
title: M2 cash-ledger exactness and migration boundary
date: 2026-07-17
context: H-003 exploration after Phase 4 durable-close smoke exposed cumulative REAL drift
---

# M2 cash-ledger exactness and migration boundary

## Observation

Phase 4's two-close subprocess smoke produced the correct operator-facing value `20`,
but the persisted SQLite `REAL` accumulator held `19.999999999999996`. This does not
break the present CLI contract, but it is the wrong representation for an account
ledger: repeated closes, fee deductions, risk gates, and receipt replay must compare
canonical cash values rather than binary-float approximations.

## External constraints checked

- Polymarket CLOB V2 uses pUSD collateral, and the official unwrap interface defines
  `_amount` with 6 decimals: <https://docs.polymarket.com/concepts/pusd>.
- CLOB V2 became the active API on 2026-04-28 and is not backward-compatible with V1:
  <https://docs.polymarket.com/v2-migration>.
- Current order-book and tick-size APIs expose decimal price values; price precision is
  a venue-wire concern distinct from cash-ledger storage:
  <https://docs.polymarket.com/api-reference/market-data/get-tick-size> and
  <https://docs.polymarket.com/api-reference/market-data/get-order-books-request-body>.
- The legacy Python client is archived, while the V2 client repository recommends the
  unified `py-sdk` for new projects. Adapter selection must therefore be revalidated
  when live execution begins, not baked into this accounting phase:
  <https://github.com/Polymarket/py-clob-client> and
  <https://github.com/Polymarket/py-clob-client-v2>.

## Locked boundary

1. **Cash is exact:** balance, snapshot balance, stake, fees, realized PnL, and durable
   money receipts use integer micro-pUSD (`1 pUSD = 1_000_000 micros`).
2. **Prices stay float in perception/ranking:** H-003 does not rewrite signal,
   slippage, or market-price models wholesale.
3. **Quantize once at the accounting boundary:** public float-compatible inputs are
   converted through `Decimal(str(value))`, then rounded explicitly to micro-pUSD.
4. **Modeled PnL is centralized:** paper PnL converts price deltas through decimal
   strings, multiplies by exact stake, and rounds `ROUND_HALF_EVEN` to one micro-pUSD.
   A future venue-confirmed cash amount supersedes modeled PnL.
5. **Receipts carry tagged money:** new close receipts serialize money as a tagged
   micro-unit value, avoiding ambiguity with JSON booleans/integers. Legacy float
   receipts remain readable and replayable during migration.
6. **Migration is additive and transactional:** existing `REAL` databases gain integer
   columns, backfill under `BEGIN IMMEDIATE`, verify non-null/invariants, and then use
   integers as authority. Legacy `REAL` columns remain as derived compatibility
   projections for one phase; corrupt or unconvertible state fails closed.
7. **No silent reset and no in-place reinterpretation:** durable state still wins over
   startup config, and old numeric columns are never treated as already-scaled units.

## Bounded H-003

**Hypothesis:** micro-pUSD account state plus tagged money receipts eliminates
cumulative binary-float drift across memory, SQLite, restart, and response-replay paths
without converting market prices to fixed point.

### Acceptance surface

- Unit: exact conversion/rounding and tagged receipt round-trip, including negative and
  half-micro cases.
- Migration: a Phase 4 schema backfills deterministically, remains idempotent on
  restart, preserves open positions and legacy receipts, and fails closed on invalid
  source values.
- Integration: repeated decimal closes produce exact integer account/PnL state in both
  repositories while public snapshots/CLI remain backward-compatible.
- Restart/replay: close response loss returns the same exact receipt after restart and
  never books cash twice.
- Planning/operations: Makefile contract, SUMMARY, teaching, learnings, JOURNAL, zero
  drift, and climb gates all pass.

### Non-goals

- Live order signing, tick-size normalization, fees fetched from the venue, partial-fill
  accounting, reconciliation/outbox, or selecting the production Polymarket SDK.
- Replacing every price, percentage, signal score, or slippage estimate with a fixed
  point type.

## Routing decision

The exploration has a clear, testable completion definition, so it graduates to M2
Phase 5 rather than remaining an open research thread. Venue-wire numerical exactness is
kept as a later hypothesis triggered by live-adapter work.
