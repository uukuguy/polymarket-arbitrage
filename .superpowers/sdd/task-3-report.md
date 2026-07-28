# Task 3 Implementer Report

Status: DONE

## Scope

Task 3 only: bounded Discovery, durable identity and receipts, capacity-proven
Candidate admission, actual watcher-entry evidence, and full historical status
validation. No Task 4, deployment, process isolation, Dashboard/API, or trading.

## Final truth chain

1. Candidate source and freshness share one current-certified authority
   predicate. Independent bootstrap authorities remain valid; legacy strings
   cannot revive invalid or authority-free groups; fact-backed current groups
   remain actual even when unpromoted.
2. Admission capacity charges each attempt-start write:
   `poll + selection + high_burst*(timeout+terminal_write) +
   capacity*attempt_start_write + (capacity-1)*(timeout+terminal_write)`.
   The 60-second counterexample admits capacity 2 at 42 seconds and rejects
   capacity 3 at 62 seconds.
3. Scheduler passes persisted admission context into `CandidateWatcher`.
   `run_once` begins with a cancellation-safe transaction that validates exact
   admission identity and writes actual start evidence. A late start writes the
   unavailable breach fact and admits the next group atomically, without
   Structure or book I/O.
4. Every real admission appends `neg_risk_candidate_admissions` in the same
   schedule transaction. It retains group/event/membership/promotion/deadline
   and the complete capacity/timing proof snapshot. Attempt-start writes require
   that audit to exist; status exact-joins it, validates its proof, and proves
   the certified revision existed by promotion. A self-consistent forged
   attempt/audit dated before its revision fails closed.
5. Every historical discovery sample validates count, identity fields,
   finite nonnegative liquidity, quality/reason, and promotion semantics.
   `complete-supported` requires its exact revision by batch completion.
   Legitimate first `incomplete-source` and `complete-unsupported` samples may
   have no revision and remain valid non-promoted evidence.
6. Generic schema initialization is additive and policy-free. Explicit active
   configuration records audits for retained legacy promotions; admit-next and
   late-start transitions append new audits transactionally.

## Verification

```text
Focused Discovery/Candidate/config/status suite: 112 passed
Full repository: 2480 collected, 100% passed (1 expected xfail, 1 skip)
Changed-file Ruff: pass
git diff --check: pass
make planning-status: 82 plans, no drift
git hooksPath: .githooks
```

The full suite was rerun on the final implementation state before handoff.

## Remaining boundary

Process-level kill isolation and production qualification remain later tasks.
Feature flags remain off; nothing was deployed and no trade path was enabled.
