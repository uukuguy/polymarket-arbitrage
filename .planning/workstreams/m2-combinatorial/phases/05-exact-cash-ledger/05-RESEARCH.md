# Phase 5: Exact Cash Ledger - Research

**Researched:** 2026-07-17
**Status:** Complete

## Findings

1. Polymarket pUSD collateral uses six-decimal amounts, so micro-pUSD is a venue-aligned
   ledger unit rather than an arbitrary internal precision.
2. SQLite `INTEGER` gives canonical equality and atomic arithmetic storage within the
   signed 64-bit range; `REAL` cannot represent repeated decimal cash increments exactly.
3. `Decimal(str(float_value))` preserves the caller-visible decimal form and avoids
   importing the binary expansion that `Decimal(float_value)` would capture.
4. `ROUND_HALF_EVEN` is deterministic and unbiased for the paper model. Real execution
   must eventually prefer venue-confirmed cash/fee amounts over modeled multiplication.
5. SQLite additive columns are the lowest-risk Phase 4 migration: one immediate
   transaction can add, backfill, validate, and commit without rebuilding operation IDs
   or open-position identity.
6. Tagged receipt JSON distinguishes money from JSON `bool`/number values and supports
   backward decoding of the Phase 4 receipt ledger.

## Validation architecture

| Gate | Evidence |
|---|---|
| unit | conversion, rounding, range, Money PnL, exact in-memory mutations |
| migration | raw Phase 4 schema fixture → v2 integer columns, exact values, idempotent restart |
| compatibility | legacy receipt types and float-facing tracker/snapshot/CLI contracts |
| integration | repeated decimal closes exact in memory and SQLite |
| restart | lost-response tagged receipt replay, one operation, one cash booking |
| operational | Makefile tests, full M2, Ruff, diff, planning-status, teaching/SUMMARY/learnings/JOURNAL |

## Primary references

- <https://docs.polymarket.com/concepts/pusd>
- <https://docs.polymarket.com/v2-migration>
- <https://docs.python.org/3/library/decimal.html>
- <https://sqlite.org/datatype3.html>

## Planning conclusion

The phase is bounded and needs one ordered plan. Money/domain types must land before the
repository migration; migration must land before the subprocess compatibility proof.
