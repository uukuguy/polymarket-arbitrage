# Phase 6: Unit-Safe Execution Accounting - Context

**Gathered:** 2026-07-17  
**Status:** Ready for planning  
**Source:** autonomous H-004 Knowledge Layer exploration

<domain>
Separate exact outcome-token quantity from pUSD collateral across routing, positions,
full fills, SQLite migration, restart, and CLI compatibility. Correct the existing
paper balance/PnL dimensional inconsistency without implementing partial aggregation or
live venue orders.
</domain>

<decisions>

- Quantity is exact integer micro-shares; Money remains exact integer micro-pUSD.
- Execution and fills name quantity explicitly. Deprecated size/stake aliases are
  compatibility projections only.
- BUY collateral is `q * p`; fully-collateralized synthetic SELL collateral is
  `q * (1-p)`; modeled PnL is side-aware `q * price_delta`.
- Phase 5 DB migration derives quantity/cost basis and repairs over-reserved balance in
  one immediate transaction. New integer fields are authority after migration.
- Price remains decimal-facing float; no live SDK/signing/network/credential behavior.
- H-005 owns partial fills; H-006 owns venue cash/fee truth.

</decisions>

<canonical_refs>

- `docs/superpowers/specs/2026-07-17-m2-unit-safe-execution-accounting-design.md`
- `.planning/notes/m2-fill-quantity-accounting-boundary.md`
- `.planning/threads/execution-accounting.md`
- Phase 5 SUMMARY and exact Money implementation

</canonical_refs>

<specifics>

- Regression vector: initial 1000, BUY 100 @ .50 reserves 50; close @ .60 returns 60,
  final 1010, realized +10.
- Migration vector: legacy open BUY quantity 100 @ .50 with stored balance 900 becomes
  quantity 100, cost basis 50, corrected balance 950.
- Raw SQLite checks assert `typeof(quantity_micros/cost_basis_micros)='integer'`.

</specifics>

<deferred>

- Residual positions and multi-fill aggregation (H-005).
- Live adapter amount translation, fees, signing, and reconciliation (H-006).
- Removal of compatibility columns and aliases.

</deferred>

