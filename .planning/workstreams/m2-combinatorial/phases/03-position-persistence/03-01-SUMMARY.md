# Phase 3 Plan 01 — SUMMARY

> **Plan**: `03-01-PLAN.md`
> **Status**: ✅ CLOSED — all 6 tasks completed
> **Date**: 2026-07-17

## Outcome

M2 paper-account state now survives process boundaries. Independent `run`,
`status`, and `close` commands share one crash-consistent SQLite projection,
while existing in-memory construction remains compatible for tests and local
callers. Account/PnL/position writes are atomic, operation replay is idempotent,
and corrupt or busy durable state fails closed.

## Commit Matrix

| Commit | Deliverable |
|---|---|
| `89b091f` | Repository contract, complete `PositionState`, deep-copy transaction semantics, operation replay |
| `7560d5a` | Three-table SQLite repository, `BEGIN IMMEDIATE`, schema/cardinality validation, rollback/restart tests |
| `7936eb5` | Repository-backed `PositionTracker`, shared reads, transition closures, UTC position timestamps |
| `ca9a21d` | Stable execution IDs from `signal_id + leg_id`, deterministic paper close replay |
| `f425d25` | Durable CLI composition, config/env precedence, `db=` Makefile surfaces, real subprocess tests |
| `ff93d1b` | Teaching chapter 13 plus correction/link from chapter 12 and index |
| `4e04237` | Planning-status lifecycle upper bound preventing same-number commits from leaking across workstreams |

Planning setup and infrastructure commits preceding implementation:
`7201f95` (phase context), `5030a9b` (workstream-scoped planning-status fix),
and `3f01802` (Phase 3 plan registration).

## Delivered API and Operator Surface

- `PositionRepository.load()` / `apply(operation_id, operation_type, target_id, transition)`
- `InMemoryPositionRepository(initial_balance)`
- `SQLitePositionRepository(db_path, initial_balance, busy_timeout_ms=5000)`
- `PositionTracker(config=None, repository=None)` with optional operation IDs on state mutations
- `PositionConfig.db_path` / `busy_timeout_ms`
- `POLYARB_POSITION_DB_PATH` / `POLYARB_POSITION_BUSY_TIMEOUT_MS`
- CLI `run/status/close --db-path`; synthetic retry surface `run --signal-id`
- `make run-arb/status-arb/close-arb db=<path>`

## Verification Evidence

### Corrected full M2 gate

```bash
uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli
```

Result: **130 passed**, 42 pre-existing `datetime.utcnow` warnings, 2.93s.

The plan's original command listed both `tests/routing/test_signal.py` and its
parent `tests/routing`. Pytest path de-duplication skipped the rest of the
routing directory and reported only 69 tests. Removing the overlapping child
path restored the intended 130-test collection.

### Additional gates

- `uv run pytest tests/test_makefile.py -q` — **2 passed**
- Ruff on all five modified production modules — **all checks passed**
- `git diff --check` — clean
- `tests/test_planning_status.py` — **3 passed**; closed M1 `03-01` no longer absorbs later M2 `03-01` commits
- Focused repository/tracker regression — 31 passed
- Execution replay/E2E regression — 39 passed
- Config/CLI/subprocess/Makefile task gate — 31 passed

### True-process lifecycle proof

Using one isolated `/tmp/.../positions.db` across four commands:

1. `make run-arb ... mid=0.40 stake=100 legs=1 signal_id=phase3-smoke`
   → balance 900, one `cond-0` BUY position.
2. Fresh `make status-arb` → observed the committed position.
3. Fresh `make close-arb ... exit_price=0.50` → realized PnL +10.
4. Fresh `make status-arb` → balance 1010, zero positions, cumulative PnL +10.

The dedicated smoke database and directory were removed after verification.

## Atomicity and Failure Proofs

- Mutating a candidate state then raising leaves account, PnL, and positions unchanged.
- Two repository/tracker instances see each other's commits without private-state access.
- Duplicate operation IDs return the original bool/float/None result without re-running the transition.
- Reusing one operation ID for another type/target raises an identity conflict.
- Closing then replaying the old open ID does not reopen; a new ID can legitimately reopen.
- A changed configured initial balance never resets durable state.
- Invalid account cardinality, corrupt SQLite, and bounded lock timeout exit non-zero.
- Stable synthetic signal replay keeps one open operation in the ledger; paper close keeps exactly open+close.

## Design Decisions

1. Domain math remains in `PositionTracker`; repositories own transaction and durability mechanics.
2. SQLite uses normalized account/open-position/operation tables rather than one JSON blob.
3. `BEGIN IMMEDIATE` is acquired before reading state so risk checks and writes serialize.
4. Rejected operations are memoized because an identical retry must retain its first outcome.
5. Existing durable state wins over changed startup `initial_balance`; reset needs a future explicit command.
6. CLI dependency construction is per-command, not module-global, while unit tests inject the in-memory factory.

## Deviations from Plan

- Added `run --signal-id` and `make run-arb signal_id=...` so subprocess tests and operators can explicitly distinguish retrying one synthetic opportunity from creating a new one.
- Created `tests/test_makefile.py`; the planned path did not yet exist in the repository.
- Corrected the overlapping pytest verification paths as described above.
- Updated chapter 12's now-stale per-process warning in addition to creating chapter 13.
- GSD `phase complete 03` reported `roadmap_updated: true` but left the Phase 3 goal/plan as `TBD`; plan-progress plus an exact metadata repair were required before the closure audit.
- Planning-status initially used only the exact PLAN creation commit as a lower bound. Phase-number reuse later caused M2 `03-01` to inflate closed M1 `03-01`; committed SUMMARY creation is now the lifecycle upper bound.

## Remaining Risks / Deferred Work

- Real venue close identity currently includes `Fill.filled_at`; the venue adapter must supply/use an immutable venue fill/trade ID for retry-proof production closes.
- SQLite intentionally supports one writer at a time; the bounded timeout exposes contention rather than hiding it.
- Operator-driven `close` retries do not yet expose a caller-supplied stable close operation ID.
- Existing `Fill` and `PositionSnapshot` default factories still emit Python 3.12 `datetime.utcnow` deprecation warnings; persisted `Position.opened_at` is already UTC-aware.
- Full closed-position history/event sourcing and explicit account reset are outside this phase.

## Teaching Artifact

`docs/learning/13-仓位持久化.md` covers the mental model, transaction sequence,
operation identity, three-table choice, fail-closed behavior, trade-offs, five
adversarial questions, and FAQ increment.
