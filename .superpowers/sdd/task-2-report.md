# H-009 Task 2 — Read-only atomic CLOB quote collection report

## Scope

Implemented only the local quote collector and its Typer command in the
isolated `climb-h009-quote-producer` worktree. The collector consumes the
Task 1 `NegRiskQuoteStore` authoritative latest-universe API and the existing
read-only `ClobReaderClient.get_books()` path. No Makefile targets, scanner or
route changes, orders, credentials, Fly, cron, deploy, or scheduler code was
added.

## RED evidence

Before production code, the new fixture-only collector and process-command
tests were added. Running their focused selection failed at collection because
the requested implementation module did not exist:

```text
uv run pytest -q tests/routing/test_neg_risk_quote_collector.py \
  tests/cli/test_arbitrage_cli_process.py -k 'collect_neg_risk_quotes or collects_latest_universe or partial_responses or transport_failure or unusable_or_mismatched or busy_or_unavailable'

ModuleNotFoundError: No module named 'polyarb.routing.neg_risk_quote_collector'
```

The non-zero result is the expected missing-feature RED state.

## GREEN evidence

After implementation, the equivalent targeted test selection passed with 11
tests, then the persistence-failure and zero-eligible-universe regressions
were added. The final task-mandated command completed successfully:

```text
uv run pytest -q tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/cli/test_arbitrage_cli_process.py

...............................................
exit 0
```

`pytest --collect-only` confirmed this command covers 47 tests. Fresh static
and patch checks also passed:

```text
uv run ruff check src/polyarb/routing/neg_risk_quote_collector.py \
  src/polyarb/cli_arbitrage.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/cli/test_arbitrage_cli_process.py
All checks passed!

git diff --check
exit 0
```

## Implementation

- Added `QuoteCollectionResult` (frozen), bounded unavailable/integrity
  exceptions, injected store/reader/clock collection, defensive dict-or-SDK
  field access, numeric finite ask validation, and best-valid-ask selection.
- The collector reads the one latest universe and acquires the Task 1 run lock
  before exactly one `get_books()` call. It persists a terminal row for every
  requested leg and only then promotes the run complete.
- Reader transport, response-integrity, parsing, and persistence failures after
  begin best-effort fail the new run with bounded reasons while re-raising the
  original exception. Existing complete projections remain selectable.
- Added `collect-neg-risk-quotes --db-path ... [--verbose/-v]`. It initializes
  exactly the selected local database, uses `asyncio.run`, prints a stable,
  bounded success JSON summary, and exits 2 without a success payload on any
  error. Its help text states that it is neither a scheduler nor an order
  command.

## Regression coverage

- One fake async reader observes a single de-duplicated batch request; both
  dict fixtures and attribute fixtures are accepted.
- Lowest valid ask retains its matching size; missing book/ask and malformed
  price/size become terminal sibling states within an otherwise complete run.
- Unknown, duplicate, and unusable CLOB responses fail the new run. Busy,
  absent-universe, and zero-eligible-universe paths make no reader request.
- Transport and injected storage failures preserve an earlier complete run.
- The subprocess CLI test confirms a no-universe collection initializes only
  the supplied sidecar, exits 2, emits no successful JSON, and never starts a
  quote run.

## Self-review

- `QuoteRunBusyError` propagates from `begin_run()` before reader access.
- The store remains the only authority for durable requested membership and
  complete-run selection; collector state is not cached.
- Response assets are strict: unrequested/missing/duplicate IDs or non-list
  ask payloads cannot be silently converted into a partial complete run.
- The command creates `ClobReaderClient` only for its existing read-only API;
  it neither supplies credentials nor reaches any order/execution surface.

## Concern

The success JSON path is covered by the collector fixture tests and explicit
CLI serialization code; the process test intentionally exercises the safe
no-universe failure path to guarantee it makes no live CLOB request. A full
success-process fixture would require an SDK transport injection mechanism,
which this task deliberately does not introduce.
