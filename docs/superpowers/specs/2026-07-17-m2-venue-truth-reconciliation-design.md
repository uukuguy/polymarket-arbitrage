# M2 Venue-Truth Fill Reconciliation Design

**Date:** 2026-07-17  
**Hypothesis:** H-006  
**Status:** approved for autonomous execution under the M2 climb mandate

## Goal

Let a terminal venue fill replace paper price-delta cash formulas with exact confirmed
share, gross cash, and fee facts. Preserve H-005 canonical fill identity, remaining
quantity, transactionality, and response-loss replay.

## Protocol evidence

- Authenticated CLOB trades expose immutable trade `id`, `size`, `price`,
  `fee_rate_bps`, and lifecycle `status`; `CONFIRMED` is the successful terminal state.
  `MATCHED`, `MINED`, and `RETRYING` are not final cash authority.
- The ordinary trade object exposes a fee *rate*, not actual fee cash. A local formula
  must therefore remain an estimate, never venue truth.
- Builder trades expose exact `size`, `sizeUsdc`, `fee`, and `feeUsdc`. This is the
  first documented payload that can populate complete settlement truth directly.

Canonical references:

- <https://docs.polymarket.com/trading/orders/overview>
- <https://docs.polymarket.com/api-reference/trade/get-trades>
- <https://docs.polymarket.com/api-reference/trade/get-builder-trades>

## Domain contract

Add an immutable `VenueSettlement` attached to `Fill`:

- `gross_cash: Money` — exact venue-confirmed cash before fee;
- `fee: Money` — exact venue-confirmed cash fee;
- `status` — must be exactly `CONFIRMED`;
- `source_ref` — non-empty immutable venue evidence reference.

The existing `Fill.filled_quantity_value` remains exact confirmed shares and
`Fill.fill_id` remains the immutable trade/fill identity. A fill carrying settlement
truth requires both a non-empty fill ID and complete settlement fields. Missing fee,
cash, source, or terminal status fails closed; the tracker never derives a missing
component from price or `fee_rate_bps`.

For the filled slice:

```text
net_cash = gross_cash - fee
venue_pnl = net_cash - allocated_cost_basis
balance += net_cash
realized_pnl += venue_pnl
```

Modeled paper fills retain the H-005 formula. Venue truth and modeled truth are
different explicit receipt sources, never silently mixed.

## Durable reconciliation receipt

Add a tagged `SettlementReceipt` result containing exact `gross_cash`, `fee`, `net_cash`,
and `realized_pnl`, plus source=`venue-confirmed`. SQLite JSON uses integer micros only.
Legacy Money/float receipts remain readable.

The operation ledger gains a backward-compatible request fingerprint. For a venue
settlement it covers market, filled quantity, gross cash, fee, terminal status, and
source reference. The same `venue-fill:{fill_id}` with a different quantity/cash/fee
must fail atomically even under concurrent processes; it may not return an unrelated
old receipt merely because the ID matches.

## Operator and engine boundary

- `ExecutionEngine.fill_provider` forwards the complete Fill unchanged; tracker owns
  the exact reconciliation decision.
- CLI `close` adds an all-or-none venue truth surface for local proof:
  `--fill-id`, explicit original fill `--size`, `--venue-cash`, `--venue-fee`, `--venue-status CONFIRMED`,
  `--venue-ref`.
- `make close-arb` forwards the same parameters. This is a deterministic acceptance
  harness, not live order submission.
- CLI output/replay exposes receipt source, gross cash, fee, net cash, and PnL.

## Failure semantics

- non-terminal venue status: reject without mutation or receipt;
- incomplete venue fields: reject at construction/CLI boundary;
- negative cash/fee or fee greater than gross cash: reject;
- venue truth without fill ID/source reference: reject;
- duplicate identical settlement: replay exact structured receipt;
- duplicate fill ID with different quantity/cash/fee/source: identity conflict;
- stored empty legacy fingerprint plus supplied venue fingerprint (or the reverse):
  identity conflict; only empty+empty retains legacy replay;
- zero/overfill/market conflict: retain H-005 fail-closed behavior.

## Acceptance vectors

1. BUY 100 @ .40, confirmed partial 30 shares with gross cash 13.80 and fee .30:
   allocated cost=12, net cash=13.50, PnL=1.50, remaining q=70/cost=28,
   balance=973.50.
2. Paper price may say .90 for the same fill; venue cash still determines the exact
   1.50 PnL, proving the formula was superseded.
3. Lost response and restart with identical settlement returns the same structured
   receipt and leaves state unchanged.
4. Same fill ID retried with fee .31 instead of .30 fails identity conflict without
   mutation.
5. `MATCHED` payload, fee-rate-only payload, or missing `venue_ref` cannot book cash.

## Non-goals

- Wallet keys, signing, allowance changes, order submission, polling infrastructure,
  or network calls.
- Locally reproducing Polymarket fee formulas.
- Replacing H-005 fill identity or remaining-quantity semantics.
- General ledger double-entry, tax lots, or settlement after market resolution.
