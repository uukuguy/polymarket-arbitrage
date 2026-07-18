# H-009 Task 3 Report — quote-run scanner and fail-closed HTTP boundary

## Scope delivered

- Kept `scan_neg_risk_buy_all()` as the snapshot-compatible offline entry point.
- Added `scan_neg_risk_quote_run()` which reads only
  `NegRiskQuoteStore.latest_complete_projection()` and never reads snapshot
  best-asks.
- Enforced a single complete run, all-sibling executable terminal states,
  Decimal arithmetic, deterministic edge/group ordering, quote-first then
  universe SLA checks, and bounded precondition errors.
- Switched the HTTP endpoint to the quote-run scanner with the fixed 300s / 50,400s
  SLAs and its fixed known-universe coverage metadata.
- Extended feed diagnosis for exact bounded quote/universe stale messages while
  retaining stale-snapshot compatibility fields and avoiding server-detail leaks.

## TDD evidence

### RED

After adding scanner, HTTP, and diagnosis tests, before production code changed:

```text
$ uv run pytest -q tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/routing/test_opportunity_diagnosis.py
ImportError: cannot import name 'QuoteRunUnavailableError' from 'polyarb.routing.opportunity_scanner'
ImportError: cannot import name 'QuoteRunUnavailableError' from 'polyarb.routing.opportunity_scanner'
!!!!!!!!!!!!!!!!!!! Interrupted: 2 errors during collection !!!!!!!!!!!!!!!!!!!!
```

The imports were intentionally new API contracts used by scanner and HTTP tests;
the test suite could not collect until Task 3 defined the fail-closed quote-run
exceptions and route source.

### GREEN

```text
$ uv run pytest -q tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/routing/test_opportunity_diagnosis.py
........................................                                 [100%]
```

```text
$ uv run pytest -q tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/routing/test_opportunity_diagnosis.py tests/routing/test_neg_risk_quote_store.py tests/routing/test_neg_risk_quote_collector.py
........................................................................ [ 97%]
..                                                                       [100%]
```

```text
$ uv run ruff check src/polyarb/routing/opportunity_scanner.py src/polyarb/http/arbitrage.py src/polyarb/routing/opportunity_diagnosis.py tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/routing/test_opportunity_diagnosis.py
All checks passed!

$ uv run ruff format --check src/polyarb/routing/opportunity_scanner.py src/polyarb/http/arbitrage.py src/polyarb/routing/opportunity_diagnosis.py tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/routing/test_opportunity_diagnosis.py
6 files already formatted

$ git diff --check
<no output; exit 0>
```

## Self-review

- [x] Selected source is only one complete quote run; newer failed/collecting
  runs cannot displace it.
- [x] A non-executable terminal sibling invalidates its entire group; snapshot
  asks are not used as a fallback.
- [x] Quote age is checked before universe age, using the required exact public
  error strings and inclusive boundaries.
- [x] Quote-run results expose run and universe provenance; snapshot-compatible
  results omit the new `None` fields to avoid changing existing serializations.
- [x] HTTP has distinct bounded handling for unavailable/stale-quote/stale-universe;
  validation and database failures remain generic.
- [x] H-008 accepts 200 only with fixed `known-universe`, integer `300`, and
  integer `50400` metadata. Exact stale regexes retain parsed generic and
  backward-compatible specific fields; unknown 503s stay unavailable.
- [x] No Makefile/manual/evaluator/deployment/scheduler/wallet/credential work
  was changed.
