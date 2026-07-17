# Exploration — M2 Fill Quantity / Cash Accounting Boundary

Date: 2026-07-17  
Workstream: `m2-combinatorial`  
Trigger: climb Knowledge Layer after H-003 exhausted the pending pool

## Problem found

The current execution path gives one floating `size` three incompatible meanings:

1. `ArbitrageLeg.pm_size` is a share quantity because `effective_cost` computes
   `price * pm_size`.
2. `ExecutionLeg.size` is documented as dollars, while routing computes
   `size_usd = price * size`, which treats it as shares.
3. `Position.stake_money` is deducted directly from cash, but modeled PnL computes
   `stake * (exit_price - entry_price)`, which treats that same value as shares.
4. `Fill.filled_size` is quantized as `Money` and compared with cash stake even
   though venue order/trade sizes represent filled outcome-token quantity.

At price 0.50, opening 100 shares should reserve 50 pUSD and a move to 0.60 should
realize 10 pUSD. The current model reserves 100 pUSD and happens to realize the
right 10 pUSD only because it treats the cash field as shares in the PnL formula.
The balance and the PnL cannot both be correct under the current contract.

## External protocol facts (official sources, checked 2026-07-17)

- A limit order's `size` is outcome-token shares; the official example buys 10
  shares at price 0.50.
- A market BUY `amount` is pUSD to spend, while a market SELL `amount` is shares
  to sell. The wire request is therefore side-dependent and cannot be represented
  safely by an untagged `size` field.
- FAK explicitly permits partial fills; order state exposes `original_size` and
  cumulative `size_matched`, and trade events expose `size`.
- Outcome positions are token balances; prices are pUSD per share.
- Current pUSD/base amounts use six decimals. The SDK/order-builder ecosystem also
  converts token amounts at 10^6 precision, so paper quantity needs an explicit,
  exact six-decimal boundary before a live adapter is allowed to sign orders.

Sources:

- https://docs.polymarket.com/trading/orders/create
- https://docs.polymarket.com/trading/orders/overview
- https://docs.polymarket.com/trading/clients/l1
- https://docs.polymarket.com/concepts/positions-tokens
- https://docs.polymarket.com/concepts/pusd

## Decision

Do not implement partial-fill aggregation on top of the ambiguous contract.
Create a prerequisite phase that makes the domain dimensions explicit:

- `Quantity` = exact outcome-token shares in integer micro-shares.
- `Money` = exact pUSD in integer micros.
- `ExecutionLeg.quantity` = shares; its modeled cash notional is
  `quantity * estimated_price`, quantized once to `Money`.
- `Position.quantity` and `Position.cost_basis` are separate authorities.
- `Fill.filled_quantity` is shares, never cash.
- Paper BUY/SELL accounting uses explicit cash flows; the later live adapter may
  translate BUY cash amount vs SELL share amount only at the SDK boundary.

Compatibility aliases may remain read-only for one phase, but internal risk,
balance, PnL, persistence, and fill checks must not read ambiguous `size`/`stake`.

## Ranked hypotheses

1. **H-004 — Unit-safe execution accounting**: explicit exact share quantity and
   pUSD cost basis make route/open/full-fill-close mathematically consistent.
2. **H-005 — Durable partial-fill accumulation**: once H-004 exists, immutable
   fill IDs plus cumulative quantity/proceeds can make partial closes restart-safe.
3. **H-006 — Venue-truth reconciliation**: signed/confirmed venue cash, fee, and
   share amounts can supersede paper formulas without corrupting exact ledgers.

## H-004 acceptance boundary

- A 100-share BUY at 0.50 reserves exactly 50 pUSD; closing at 0.60 returns 60
  pUSD and realizes 10 pUSD.
- A 100-share SELL model has explicit cash-flow semantics and no field whose unit
  changes based on which caller reads it.
- Full-fill validation compares `Quantity` to `Quantity`, not `Money` to shares.
- SQLite restart preserves quantity and cost basis using INTEGER authorities and
  migrates Phase 5 databases transactionally.
- Execution engine, CLI paper lifecycle, legacy compatibility, and Makefile gates
  remain green without live credentials.
- Teaching material explains why market BUY amount and SELL amount are not the
  same dimension even though the official SDK names both `amount`.

## Explicit deferrals

- Multiple fills / residual open quantity: H-005.
- Live SDK signing, credentials, actual order placement: outside H-004.
- Venue fee truth and reconciliation: H-006.
- Converting perception prices to fixed point: unnecessary; prices remain decimal-
  facing estimates and quantization occurs at cash/quantity boundaries.
