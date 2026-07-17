# Phase 8: Venue-Truth Fill Reconciliation - Context

**Gathered:** 2026-07-17  
**Status:** Ready for planning  
**Source:** approved autonomous M2 climb design

<domain>

Replace paper fill cash formulas with complete, terminal, exact venue-confirmed share,
gross cash, and fee facts. Preserve H-005 canonical fill identity, remaining Quantity,
atomic SQLite mutation, and response-loss replay. This phase creates the local domain,
persistence, Engine, CLI, and Makefile proof; it does not place network orders.

</domain>

<decisions>

- A venue settlement is complete only with exact gross Money, exact fee Money,
  `status=CONFIRMED`, non-empty source reference, immutable fill ID, and exact Fill
  quantity.
- Missing actual fee/cash may not be derived from price or `fee_rate_bps` and labeled
  venue truth.
- Venue net cash is `gross_cash - fee`; realized PnL is net cash minus exact allocated
  cost basis. Paper fills retain their modeled formula.
- Durable venue receipts preserve gross, fee, net, realized PnL, and source tag in
  integer micros; legacy Money/float receipts remain readable.
- The applied-operation ledger gains an atomic request fingerprint so a reused fill ID
  with different quantity/cash/fee/status/source fails even under concurrent processes.
- Engine forwards complete Fill facts unchanged. CLI/Makefile expose an all-or-none
  venue settlement acceptance harness, not live trading.
- Non-terminal/incomplete/negative/fee-over-gross settlement, anonymous venue truth,
  zero/overfill, or identity conflict fails without mutation.

</decisions>

<canonical_refs>

- `docs/superpowers/specs/2026-07-17-m2-venue-truth-reconciliation-design.md`
- `.planning/workstreams/m2-combinatorial/phases/07-durable-partial-fill-accounting/07-01-SUMMARY.md`
- `.planning/threads/execution-accounting.md`
- <https://docs.polymarket.com/trading/orders/overview>
- <https://docs.polymarket.com/api-reference/trade/get-trades>
- <https://docs.polymarket.com/api-reference/trade/get-builder-trades>

</canonical_refs>

<deferred>

- Wallet keys/signing, allowance mutation, live order placement, polling daemon, and
  external network calls.
- Local reconstruction of Polymarket fee formulas.
- General double-entry ledger and resolved-market redemption.

</deferred>

---
*Phase: 08-venue-truth-fill-reconciliation*
*Context gathered: 2026-07-17*
