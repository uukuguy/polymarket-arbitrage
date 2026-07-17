# Phase 6: Unit-Safe Execution Accounting - Research

**Researched:** 2026-07-17  
**Status:** Complete

## Findings

1. Official V2 limit-order size is shares, while market-order amount changes dimension
   by side: BUY cash versus SELL shares.
2. Order/trade `size_matched` and FAK semantics require quantity to be distinct before
   durable partial fills can be correct.
3. Existing M2 routing already treats size as shares (`price * size`), while the ledger
   deducts it as cash; this is a local contract bug, not an SDK integration issue.
4. Outcome tokens and pUSD use six-decimal base-unit workflows; separate types prevent
   accidental interchange even when scales match.
5. Phase 5 migrations already provide the correct `BEGIN IMMEDIATE`, additive-column,
   dynamic-type validation, and dual-projection pattern for v3.

## Planning conclusion

One ordered TDD plan is sufficient. Quantity/domain must land before migration; migration
must land before subprocess compatibility proof. Partial fill aggregation remains a
separate hypothesis.

