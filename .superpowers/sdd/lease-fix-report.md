# H-009 quote-producer lease recovery fix

## Scope

Only the atomic neg-risk quote store/collector, their focused tests, and this
report changed. No production CLOB request, deployment, scanner logic, or Task
4 documentation was touched.

## Root cause

`NegRiskQuoteStore.begin_run()` treated every `collecting` row as permanently
busy. If a process died after `begin_run()` and the collector's best-effort
`fail_run()` could not persist, no durable state transition remained that could
release the one-run gate. All later collections failed with `QuoteRunBusyError`.

## RED then GREEN

The first regression test created that crashed durable shape by expiring a
collecting run, then attempted a new run. Before the fix it failed exactly at
the old busy check:

```
QuoteRunBusyError: collecting quote run already exists: 1
```

After the fix, the same test verifies that the expired row becomes
`failed / collector-lease-expired`, a new run acquires the lease, and a live
unexpired collecting row still raises busy. Additional deterministic async tests
use an injected clock and gate-controlled sleeper (no wall-clock sleeps):

- a CLOB request still awaiting its response renews just before its original
  expiry; after that original expiry, another store instance remains busy;
- a renewal write failure cancels the outstanding reader, marks the original
  run `failed / collector-lease-lost` when SQLite is available, raises
  `QuoteRunLeaseLostError`, and permits a later owner only after the old run is
  terminal.

## Lease policy and concurrency contract

- `neg_risk_quote_runs.lease_expires_at_ms` is add-only. Fresh databases create
  it as `INTEGER NOT NULL DEFAULT 0`; `SQLiteStore.init_schema()` adds the same
  column to existing databases. The zero default deliberately makes legacy
  collecting rows immediately recoverable.
- A new collecting run owns a 30,000 ms lease. While the read-only CLOB request
  is pending, its collector renews every 10 seconds (one third of TTL).
  Thirty seconds accommodates normal request latency and scheduling jitter;
  ten seconds leaves two renewal opportunities before expiry, while a crash
  becomes recoverable within one bounded TTL.
- `begin_run()` holds `BEGIN IMMEDIATE`, atomically marks only
  `collecting AND COALESCE(lease_expires_at_ms, 0) <= quoted_at_ms` rows failed
  with `collector-lease-expired`, then checks for a remaining live collector and
  inserts the new owner. A live row is never reclaimed.
- `renew_run_lease()` also holds `BEGIN IMMEDIATE` and updates only when the run
  is still collecting **and** `lease_expires_at_ms > now_ms`; it cannot revive a
  lease that is already expired or reclaimed. SQLite's write transaction
  serializes a competing recovery and renewal: the first committer wins; the
  losing collector detects lease loss and stops.
- The collector runs reader and heartbeat tasks concurrently. It performs one
  final ownership renewal when the reader returns, before persisting terminal
  quotes. A heartbeat/final-renewal error cancels the reader, best-effort fails
  its own run, and re-raises `QuoteRunLeaseLostError`; it never records or
  completes a run after losing ownership. If the best-effort failure also cannot
  persist, the unrenewed TTL still provides bounded crash recovery.
- The scanner still selects only `complete` runs ordered by
  `quoted_at_ms DESC, id DESC`; no selection query or older-complete behavior
  changed.

## Verification

Passed after the implementation:

```
uv run python -m pytest \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/routing/test_opportunity_scanner.py -q
# 52 passed

uv run ruff check \
  src/polyarb/routing/neg_risk_quote_store.py \
  src/polyarb/routing/neg_risk_quote_collector.py \
  src/polyarb/storage/schemas.py \
  src/polyarb/storage/sqlite_store.py \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py
# All checks passed

git diff --check
# clean
```

`uv run ruff check .` was also run. It reports 335 pre-existing violations in
unrelated historical files (for example `alembic/`, `scripts/`, and older test
modules); none are in this change's scoped ruff set.
