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
- `begin_run()` obtains `BEGIN IMMEDIATE` first, then samples the store-owned
  current clock and atomically marks only
  `collecting AND COALESCE(lease_expires_at_ms, 0) <= current_time` rows failed
  with `collector-lease-expired`. It then checks for a remaining live collector
  and inserts the new owner. `quoted_at_ms` is quote metadata, never lease
  authority. A live row is never reclaimed.
- `renew_run_lease()` also obtains `BEGIN IMMEDIATE` before sampling its clock,
  then updates only when the run is still collecting **and**
  `lease_expires_at_ms > current_time`; it cannot revive a lease that is already
  expired or reclaimed. SQLite's write transaction serializes a competing
  recovery and renewal: the first committer wins; the losing collector detects
  lease loss and stops.
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

## Review follow-up: terminal mutation ownership proof

Review found a remaining ownership gap in the first fix: `record_terminal_quotes`
and `complete_run` used a status-only collecting check. A collector could renew,
be paused past its TTL, then insert and complete before a replacement ran
`begin_run()`.

### RED then GREEN

New deterministic tests first demonstrated RED:

- after the collector's final successful renewal, the injected clock advances
  exactly one TTL before terminal persistence; the old implementation returned
  a complete run instead of raising lease loss;
- store tests showed terminal recording could use status alone after a lease
  had expired.

The completed fix makes every terminal mutation prove a live lease inside its
own existing `BEGIN IMMEDIATE` transaction. The `NegRiskQuoteStore` owns an
injectable authoritative clock (wall clock in production); begin, renew, record,
and complete read that clock inside their own operation. Terminal APIs no longer
accept a caller-supplied timestamp, so a stale caller cannot bypass a lease by
passing old time. `_require_live_collecting` requires `status='collecting'` and
a non-null `lease_expires_at_ms > store_current_time`; otherwise it raises the
bounded `QuoteRunLeaseLostError` without inserting or completing. The collector
uses the same store clock by default, while deterministic tests construct both
with one controlled clock. `completed_at_ms` remains completion metadata, not
lease authority.

The follow-up tests have no wall-clock sleeps and cover:

- final-renewal then TTL expiry: no quote rows, no completion,
  `collector-lease-lost`, followed only then by a new owner;
- direct expired terminal record and direct expired completion both reject;
- collecting legacy lease rows with default, explicit zero, and nullable NULL
  expiry all recover through `begin_run`;
- an expired original owner cannot renew, including after a replacement has
  reclaimed its run.

Focused follow-up verification:

```
uv run python -m pytest \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/routing/test_opportunity_scanner.py -q
# 59 passed
```

## Review follow-up: transaction-time linearization

Review found a final timing boundary: the store-owned clock was sampled before
calling `BEGIN IMMEDIATE`. A process could wait for the SQLite write lock (or be
paused) until its lease expired, then evaluate the predicate with its earlier
timestamp. That could resurrect an expired lease or accept terminal work after
expiry.

### RED then GREEN

New no-sleep deterministic tests use a narrow test-only `_begin_immediate`
seam that pauses a worker immediately before it serializes the transaction.
While paused, the fake clock advances one full TTL. Against the old ordering,
the gate was never part of the lease path and the tests failed RED; in the
pre-seam equivalent, each operation used the stale pre-wait sample.

The production repair is a single linearization change: `begin_run`,
`renew_run_lease`, `record_terminal_quotes`, and `complete_run` all acquire
`BEGIN IMMEDIATE` first and only then call the store-owned authoritative clock.
Thus the lease comparison and mutation are evaluated in one serialized temporal
boundary. The helper is a direct `BEGIN IMMEDIATE` wrapper in production; the
gate exists only in the test subclass.

The contention regressions prove that after a pre-BEGIN wait crosses TTL:

- renewal rejects rather than resurrecting the original owner;
- begin recovers the expired collecting owner rather than returning stale busy;
- terminal recording inserts no rows; and
- completion leaves the run collecting.

Focused final verification:

```
uv run python -m pytest \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/routing/test_opportunity_scanner.py -q
# 63 passed

uv run ruff check \
  src/polyarb/routing/neg_risk_quote_store.py \
  src/polyarb/routing/neg_risk_quote_collector.py \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py \
  tests/routing/test_opportunity_scanner.py
# All checks passed
```
