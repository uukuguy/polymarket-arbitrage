# H-009 response-count correctness fix

## Scope

`successful_response_count` now means accepted, integrity-verified CLOB book
responses. It no longer means executable best-ask observations.

## Change

- The collector passes `len(indexed_books)` to `complete_run()` after strict
  response integrity indexing succeeds.
- `complete_run()` persists that supplied count in its existing atomic
  completion transaction and rejects non-integer or out-of-range values. The
  allowed range is `0..requested_token_count`.
- Lease checks, terminal-row completeness, and malformed/unknown/duplicate
  response rejection remain unchanged.

## TDD evidence

The collector test was first changed to require a returned `missing-ask` book
to count as a successful response. Before the implementation it failed with
`1 == 2`; invalid-ask variants failed for the same reason. Store-bound tests
were then added before the API implementation.

## Verification

```text
uv run pytest tests/routing/test_neg_risk_quote_store.py tests/routing/test_neg_risk_quote_collector.py tests/routing/test_opportunity_scanner.py -q
65 passed

uv run ruff check src/polyarb/routing/neg_risk_quote_collector.py src/polyarb/routing/neg_risk_quote_store.py tests/routing/test_neg_risk_quote_collector.py tests/routing/test_neg_risk_quote_store.py tests/routing/test_opportunity_scanner.py
All checks passed!

git diff --check
passed
```
