# Task 4 Implementer Report

Status: DONE

## Scope

Task 4 only: default-off checkpointed Full Reconciliation, exact restart,
terminal completion proof, atomic concurrency-safe diff, scoped health,
operator Make entries, and legacy Structure demotion. No Task 5 incidents or
process isolation, no Task 6 API/Dashboard/cutover, no deployment, and no
trading capability.

## Final truth chain

1. One batch fetches exactly one bounded Gamma event page. Staging samples,
   page receipt, window aggregates and opaque next cursor commit together.
   Cancellation waits for that writer to reach COMMIT or ROLLBACK.
2. Restart reads the latest validated window and resumes its exact durable
   cursor. Receipt sequence, requested→next cursor chain, terminal empty page,
   checkpoint timestamps and batch/window/staging aggregates are validated in
   one read snapshot; corrupted state fails closed.
3. An `open` window cannot call diff application. A terminal empty page first
   leaves a recoverable `complete` window; restart applies it idempotently.
4. Diff application is one `BEGIN IMMEDIATE` transaction using Task 1 revision
   and quote-supersession semantics. It persists added, changed, closed,
   unchanged and rejected counts plus the full observation interval.
5. Concurrent online Discovery/Candidate authority wins: a current revision
   newer than its staging observation is unchanged. Closure applies only to a
   certified group that existed no later than window start and was absent from
   the entire complete window.
6. Incomplete/unsupported group identity is durable rejected evidence. Its
   group ID counts as observed, so uncertainty neither changes nor closes the
   prior online group.
7. Health reads the exact window and last batch receipt. Reconciliation
   progress/checkpoint age are scoped checks and do not alter overall or
   Candidate availability. Missing/corrupt schema is unavailable, not idle.
8. New and legacy producers are independently default-off. Legacy Structure
   adaptive timing and history remain readable, but main does not start its
   universe-sized loop unless explicitly enabled.

## Verification

```text
Initial RED: ModuleNotFoundError for polyarb.perception.reconciliation
Focused reconciliation + health: 28 passed
Proportional perception/scheduler/config/wiring/watcher suite: passed
Full repository: 2490 collected, 100% passed (1 expected xfail, 1 skip)
Changed-file Ruff and Ruff format: pass
python compileall: pass
make reconciliation-status fixture: pass
make docs-m1-check: pass
git diff --check: pass
make planning-status: 82 plans, no drift
```

## Remaining boundary

Task 5 owns incidents, load shedding and producer process isolation. Task 6
owns public API/Dashboard/cutover and production qualification. Flags remain
off; nothing was deployed and no wallet, signing, balance, order or real-money
path was added.
