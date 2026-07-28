# Task 3 final repair report

## Outcome

The opportunity-first Discovery → Candidate handoff now has one durable truth
chain from current authority through actual watcher entry:

- Candidate source and freshness use the same current-certified authority
  predicate, including independent pre-Discovery bootstrap authorities. Legacy
  seeds preserve ordering only and cannot revive invalid,
  factless, or otherwise authority-free groups. A current certified group with
  an existing Candidate fact remains actual even when it is not promoted.
- Admission capacity charges every admitted attempt-start write:
  `poll + selection + high_burst*(timeout+terminal_write) +
  capacity*attempt_start_write + (capacity-1)*(timeout+terminal_write)`.
  All configured durations are rounded up to integer milliseconds. The
  1/1/10/5/5 second counterexample admits capacity 2 (42s) and rejects
  capacity 3 (62s > 60s).
- Scheduler selection carries an immutable admission context into
  `CandidateWatcher.run_once`. The watcher's first durable operation validates
  group/event/membership/promotion/deadline against current certified authority
  and records actual `started_at`. No scheduler control thread can create an
  orphan start receipt.
- Cancellation during the start transaction waits for SQLite convergence
  before cancellation propagates. Cancellation before coroutine entry creates
  no receipt. A late restart writes the unavailable breach fact and admits the
  next group in the same transaction, then skips Structure and book I/O.
- Attempt receipts retain group/event/membership/promotion/max-wait/deadline.
  Status validates every receipt and requires matching durable breach evidence.
- Every historical batch sample retains event/membership/quality/reason,
  validates its historical revision existed by batch completion, enforces a
  finite nonnegative liquidity value, and checks quality/reason/promotion
  semantics. Count-preserving forged historical rows fail closed.

## Verification

Focused behavior and regression:

```text
uv run pytest tests/perception/test_discovery.py \
  tests/perception/test_discovery_status.py \
  tests/perception/test_candidate_watcher.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py -q
110 passed
```

Full repository (2477 collected):

```text
uv run pytest -q
100% passed (one expected xfail, one skip)
```

Static checks:

```text
uv run ruff check <all changed Python files>
All checks passed!

git diff --check
exit 0
```

## Scope and remaining boundary

This repair does not deploy, trade, or claim Task 4 production qualification.
Process-level kill isolation remains outside Task 3. The implementation does
not mutate admission policy during generic schema initialization.
