---
phase: 04-durable-close-receipts
plan: 01
subsystem: execution-recovery
tags: [sqlite, idempotency, receipts, cli, fill-identity, tdd]

requires:
  - phase: 03-position-persistence
    provides: Transactional account/position projection and applied-operation replay ledger
provides:
  - Public immutable OperationReceipt lookup in memory and SQLite
  - Caller-owned retry-safe operator close identity and replay response
  - Stable venue fill_id close identity with explicit legacy warning
  - True subprocess response-loss recovery proof
affects: [venue-adapter, reconciliation, execution-audit, operator-cli]

tech-stack:
  added: []
  patterns: [projection-plus-receipt, caller-owned-operation-identity, replay-first-response-recovery]

key-files:
  created: []
  modified:
    - src/polyarb/routing/position_repository.py
    - src/polyarb/routing/position_tracker.py
    - src/polyarb/execution/engine.py
    - src/polyarb/cli_arbitrage.py
    - Makefile
    - tests/routing/test_position_repository.py
    - tests/routing/test_position_tracker.py
    - tests/execution/test_engine.py
    - tests/cli/test_arbitrage_cli_process.py
    - tests/test_makefile.py
    - docs/learning/13-仓位持久化.md

key-decisions:
  - "Only caller-retained close identities report retry_safe=true; generated compatibility IDs do not overclaim response-loss recovery."
  - "Receipt lookup recovers responses, while repository apply under BEGIN IMMEDIATE remains the final concurrency/idempotency authority."
  - "Venue fill_id is preferred as immutable truth; missing fill_id retains the timestamp fallback with an explicit durability warning."

patterns-established:
  - "Projection vs receipt: open-position state answers what exists now; the operation ledger answers whether identity X committed and what it returned."
  - "Replay-first CLI: validate existing receipt type/target/result before requiring the now-absent position projection."

requirements-completed: []

duration: 8min
completed: 2026-07-17
---

# Phase 4 Plan 01: Durable Close Receipts Summary

**Caller-owned close identities now recover the exact committed PnL across process/response loss without changing money, positions, or ledger cardinality.**

## Performance

- **Duration:** 8 min implementation and verification after planning
- **Started:** 2026-07-17T10:51:05+08:00
- **Completed:** 2026-07-17T10:59:00+08:00
- **Tasks:** 4
- **Files modified:** 11 implementation/test/teaching files

## Accomplishments

- Promoted applied operations to frozen public `OperationReceipt` values and exposed observational lookup through repository and tracker boundaries.
- Added stable venue `Fill.fill_id` close identities while preserving legacy timestamp compatibility with a warning.
- Made operator close response-loss recovery work across independent CLI processes, including conflict and legitimate reopen/new-ID paths.
- Expanded the corrected M2 gate from 130 to 145 passing tests and updated the persistence teaching chapter.

## Task Commits

Each TDD behavior was committed RED before GREEN:

1. **Receipt repository RED** — `188a313` (`test(04-01)`)
2. **Receipt repository GREEN** — `39b7471` (`feat(04-01)`)
3. **Tracker/fill identity RED** — `7dbdb08` (`test(04-01)`)
4. **Tracker/fill identity GREEN** — `0c8cd15` (`feat(04-01)`)
5. **Operator recovery RED** — `68c4d7c` (`test(04-01)`)
6. **Operator recovery GREEN** — `3b24f5c` (`feat(04-01)`)

**Plan metadata:** `094a980`; approved spec/implementation superplan: `6e0b0f3`, `7c08a28`.

## Delivered Contracts

- `OperationReceipt(operation_id, operation_type, target_id, result)` is frozen and detached.
- `PositionRepository.get_receipt(operation_id)` returns `None` only for unknown IDs; SQLite/JSON errors propagate.
- `PositionTracker.operation_receipt(operation_id)` keeps callers out of repository internals.
- `Fill.fill_id` produces `close:{signal_id}:{leg_id}:fill:{fill_id}`; paper close remains deterministic.
- CLI `close --operation-id ID` and `make close-arb ... operation_id=ID` return `operation_id`, `replayed`, `retry_safe`, per-close PnL, and cumulative PnL.
- Existing CLI close without an ID generates one before mutation and reports `retry_safe: false`.

## Verification Evidence

### RED → GREEN

- Repository RED: 10 new failures, all due to missing `get_receipt`; GREEN: **24 passed**.
- Tracker/engine RED: 3 expected failures for missing tracker method, fill field/stable identity, and warning; GREEN plus E2E: **60 passed**.
- CLI/Make RED: 4 expected failures for missing option/response/forwarding; GREEN: CLI **14 passed**, Makefile **3 passed**.

### Corrected full M2 gate

```bash
uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q
```

Result: **145 passed**, 46 pre-existing `datetime.utcnow` deprecation warnings.

Additional gates:

- `uv run pytest tests/test_makefile.py -q` — **3 passed**
- Ruff on all four modified production modules — **all checks passed**
- `git diff --check` — clean
- `make planning-status` — correctly reported Phase 4 DRIFT until this SUMMARY was created; no older shipped plan drift

### True response-loss operator smoke

One validated temporary SQLite database was used across independent commands:

1. Open `cond-0` at 0.40 with stake 100.
2. Close with `h002-smoke-close-1`; discard stdout.
3. Retry from another process: `replayed=true`, `retry_safe=true`, PnL 10.
4. Status: balance 1010, cumulative PnL 10, zero positions, one close receipt.
5. Reopen with a new signal and close using `h002-smoke-close-2`.
6. Final ledger/account: two close receipts, balance 1020, cumulative PnL approximately 20 (binary float stored as `19.999999999999996`).

The temporary database and SQLite sidecars were removed after validation.

## Failure and Concurrency Semantics

- Existing receipt with another operation type or target exits non-zero as an identity conflict.
- Existing close receipt with a non-float/bool result exits non-zero as corrupt rather than inventing success.
- Unknown ID plus missing open position remains non-zero; it does not create a zero-PnL receipt.
- Two processes may both miss the observational lookup, but `BEGIN IMMEDIATE` and the ledger primary key in `apply()` still serialize the transition and return one stored result.
- Parameterized SQL keeps caller/venue identities opaque; no live venue, wallet, credential, signing, or network scope was added.

## Decisions Made

1. Receipt lookup is the response-recovery seam, not a replacement for transactional replay authority.
2. Replayed stored PnL is authoritative; the retry's exit price is request context only.
3. Generated IDs remain convenient but cannot be recovered if their own response is lost, so the response says `retry_safe: false`.
4. A future real adapter must populate immutable `fill_id`; local timestamps are explicitly not venue truth.

## Deviations from Plan

### Auto-fixed Issues

**1. GSD planned-phase metadata did not advance the current phase**
- **Found during:** Phase 4 planning read-back.
- **Issue:** the tool updated Last Activity but left frontmatter/body on Phase 3.
- **Fix:** set `current_phase: 04`, Ready-to-execute state, Phase 4 progress, resume file, and first RED command explicitly.
- **Files modified:** `.planning/workstreams/m2-combinatorial/STATE.md`
- **Verification:** read-back plus `make planning-status` zero drift before implementation.
- **Committed in:** `094a980`.

**2. Climb state path in the superplan was incorrect**
- **Found during:** pre-execution artifact audit.
- **Issue:** plan assumed `.planning/climb`; tracked state is under `docs/status/climb/`.
- **Fix:** corrected canonical and closure paths before implementation.
- **Files modified:** Phase 4 context and implementation plan.
- **Verification:** repository file search and clean planning commit.
- **Committed in:** `094a980`.

---

**Total deviations:** 2 planning/state auto-fixes.
**Impact on plan:** Both prevented stale/missing durable context; implementation scope and approved behavior were unchanged.

## Issues Encountered

- GSD planner and checker collaboration tasks did not return within repeated bounded waits. They were interrupted; the main session applied the loaded GSD format/checklist, verified 15/15 decisions and all required task fields, then continued. No code was delegated.
- Ruff over the complete legacy test files exposes pre-existing unused imports, ambiguous `l` variables, and long lines outside this phase's diff. Modified production modules pass Ruff; this phase did not expand into unrelated test cleanup.

## User Setup Required

None — no external services, secrets, or venue credentials were added.

## Next Phase Readiness

- H-002 implementation is ready for learnings extraction, phase metadata closure, and `make climb-cycle hypothesis=H-002`.
- A real venue adapter can now map exchange-confirmed fill/trade identity into `Fill.fill_id` without changing tracker/repository contracts.
- Live reconciliation/outbox behavior remains deliberately deferred until venue truth is available.

---
*Phase: 04-durable-close-receipts*
*Completed: 2026-07-17*
