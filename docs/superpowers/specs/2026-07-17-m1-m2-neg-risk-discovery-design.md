# M1→M2 Neg-Risk Discovery Design

## Product Slice

Deliver one truthful, executable discovery path: buy one YES share in every active
market of a Polymarket neg-risk group when the sum of best asks is below one dollar.
The guaranteed bundle payout is one dollar, so gross edge is `1 - sum(asks)`.

## Fail-Closed Contract

- M1 subset snapshots retain every active neg-risk market regardless of liquidity;
  dropping a low-liquidity sibling would create a false incomplete bundle.
- M2 groups only rows with the same non-empty `neg_risk_market_id`.
- Every leg must be active, open, complete, and have positive ask price and size.
- Executable bundle quantity is the minimum ask size across all legs.
- Output reports gross edge explicitly; it does not claim fee-adjusted net profit.
- Only buy-all is supported. Sell-all requires inventory/conversion controls and is
  outside this paper-safe slice.

## Delivery

- Pure SQLite scanner returning deterministic opportunity records.
- JSON CLI and Makefile entry for any M1 snapshot database.
- Public read-only L1 HTTP endpoint so the production mounted snapshot is directly
  usable without SSH or copying credentials.
- Tests lock group completeness, missing-book rejection, edge threshold, and HTTP/CLI
  projections.
