# Phase 6 Learnings — Unit-Safe Execution Accounting

## What changed our understanding

1. A numerically correct PnL does not prove correct accounting. The old model got
   `100 × .10 = 10` right while over-reserving cash by 50; intermediate balance and
   exposure are first-class correctness gates.
2. Field names copied from a venue are not domain types. Official market-order
   `amount` changes dimension by side, so adapter translation must occur after the
   domain has explicit Quantity and Money.
3. Equal decimal scales do not make values interchangeable. Separate types prevent
   micro-shares from entering micro-pUSD arithmetic even though both use 10^6.
4. Schema migration may need semantic equity repair, not only structural backfill.
   Adding correct columns without refunding legacy over-reservation would preserve a
   corrupted balance indefinitely.
5. Compatibility is safest as a one-way projection. Legacy `stake/size/filled_size`
   remain shares for old consumers, while all internal decisions use explicit fields.

## Patterns to reuse

- Add a dimension type before implementing aggregation or reconciliation over that
  dimension.
- Test intermediate cash state, raw storage units/types, and terminal PnL together.
- Classify migration state as none/all/partial authority; partial is corruption and
  should fail closed.
- When correcting historical accounting, calculate the repair from authoritative
  source facts and apply it in the same transaction as the new authority marker.

## Adversarial decision questions

1. A market BUY requests 50 pUSD and fills 98 shares. Which value determines residual
   position quantity, and which determines cash reconciliation?
2. A Phase 5 BUY 100 @ .40 has balance 900. What exact refund proves the migration
   repaired equity rather than only adding columns?
3. Why is `Quantity(50_000_000) == Money(50_000_000)` conceptually false even though
   their SQLite integers match?
4. If only `quantity_micros` exists after an external manual ALTER, should startup
   derive cost basis or fail? What duplicate-refund risk decides this?
5. A live SELL requires token inventory rather than paper short collateral. Where
   should that venue truth enter without weakening the paper risk model?

## Next hypothesis boundary

H-005 may now add immutable per-fill identities, remaining Quantity, allocated cost
basis/proceeds, and restart-safe aggregation. It must not introduce live order signing
or venue fee truth; those remain H-006.

