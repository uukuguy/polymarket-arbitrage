# M2 Position Persistence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist the M2 paper account and open-position lifecycle so independent `run`, `status`, and `close` processes share one crash-consistent state.

**Architecture:** Keep PnL and risk rules in `PositionTracker`, and add a repository transaction boundary that supplies a complete `PositionState` to domain transitions. The SQLite repository uses WAL plus `BEGIN IMMEDIATE`, records applied operation IDs for idempotency, and commits account and position changes together. CLI processes explicitly construct the repository-backed tracker; unit tests may continue using the in-memory repository.

**Tech Stack:** Python 3.12, stdlib `sqlite3`, dataclasses/Protocol, Typer, pytest, uv, Makefile, GSD planning artifacts.

## Global Constraints

- No real Polymarket orders, credentials, wallet signing, or remote deployment.
- Use `uv`; do not install with `pip`.
- Use SQLite WAL, parameterized SQL, explicit `BEGIN IMMEDIATE`, rollback-and-reraise behavior.
- Preserve current full-fill-only, one-open-position-per-market, PnL, exposure, and stop-loss semantics.
- Every executable command remains exposed through the Makefile and `make help`.
- Every code plan receives a matching SUMMARY before another plan begins.
- Do not modify or stage the user's `CLAUDE.md` type change or untracked `AGENTS.md`.

## File Structure

- Create `src/polyarb/routing/position_repository.py`: repository protocol, durable state types, in-memory repository, SQLite schema and transaction implementation.
- Modify `src/polyarb/routing/position_tracker.py`: express mutations as repository transactions without duplicating arithmetic in SQLite code.
- Modify `src/polyarb/execution/engine.py`: pass stable operation IDs derived from signal and leg identities.
- Modify `src/polyarb/routing/config.py`: add configurable local position database path.
- Modify `src/polyarb/cli_arbitrage.py`: construct a fresh durable tracker per command and remove module-global state dependence.
- Modify `Makefile`: pass an optional `db=` override and update command help.
- Create `tests/routing/test_position_repository.py`: repository contract, crash consistency, and idempotency.
- Modify `tests/routing/test_position_tracker.py`: repository-backed domain behavior.
- Modify `tests/execution/test_engine.py`: stable operation-ID propagation and replay.
- Modify `tests/cli/test_arbitrage_cli.py`: isolated in-process compatibility tests.
- Create `tests/cli/test_arbitrage_cli_process.py`: true multi-process lifecycle tests.
- Create `docs/learning/13-仓位持久化.md` and modify `docs/learning/00-INDEX.md`.

---

### Task 1: Register Phase 3 and bootstrap its GSD context

**Files:**
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-CONTEXT.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-DISCUSSION-LOG.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-01-PLAN.md`

**Interfaces:**
- Consumes: `docs/superpowers/specs/2026-07-17-m2-position-persistence-design.md`.
- Produces: a GSD-recognized Phase 3 whose single initial plan owns Tasks 2–7 below.

- [ ] **Step 1: Add Phase 3 through GSD tooling**

Run:

```bash
node "$HOME/.codex/get-shit-done/bin/gsd-tools.cjs" phase add "Position Persistence"
```

Expected: Phase 3 is appended to the active `m2-combinatorial` ROADMAP and a `03-position-persistence` directory is created.

- [ ] **Step 2: Generate context from the approved spec**

Use the approved spec as the source of locked decisions. The Phase Boundary must say:

```markdown
Persist the local M2 paper account and open-position lifecycle so independent
run/status/close processes share crash-consistent state. Real venue access,
partial fills, remote replication, and multi-host locking remain out of scope.
```

Canonical references must include:

```markdown
- `docs/superpowers/specs/2026-07-17-m2-position-persistence-design.md`
- `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/02-CONTEXT.md`
- `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/02-1-SUMMARY.md`
- `.planning/threads/market-microstructure.md`
```

- [ ] **Step 3: Write the GSD plan with the repository, tracker, engine, CLI, verification, and teaching tasks below**

The plan must list these Makefile surfaces as deliverables:

```markdown
- `make run-arb db=<path>`
- `make status-arb db=<path>`
- `make close-arb db=<path> market_id=<id> exit_price=<price>`
```

- [ ] **Step 4: Verify planning recognizes the new phase without drift**

Run: `make planning-status`

Expected: Phase 3 is `NOT-STARTED` or the current plan is recognized; no shipped plan lacks a SUMMARY.

- [ ] **Step 5: Commit planning artifacts**

```bash
git add .planning/workstreams/m2-combinatorial/ROADMAP.md .planning/workstreams/m2-combinatorial/STATE.md .planning/workstreams/m2-combinatorial/phases/03-position-persistence
git commit -m "docs(m2): plan position persistence phase"
```

### Task 2: Define the repository contract and in-memory implementation

**Files:**
- Create: `src/polyarb/routing/position_repository.py`
- Create: `tests/routing/test_position_repository.py`

**Interfaces:**
- Produces: `PositionState`, `AppliedOperation`, `PositionRepository.apply(...)`, `InMemoryPositionRepository`, and `RepositoryStateError`.
- Consumes: `Position` from `polyarb.routing.position_tracker`; use `TYPE_CHECKING` to avoid a runtime import cycle.

- [ ] **Step 1: Write failing contract tests**

Add tests with this behavior:

```python
def test_in_memory_apply_commits_state_once():
    repo = InMemoryPositionRepository(initial_balance=1000.0)
    calls = 0

    def transition(state: PositionState) -> float:
        nonlocal calls
        calls += 1
        state.balance -= 100.0
        return state.balance

    first = repo.apply("open:s1:l1", "open", "m1", transition)
    second = repo.apply("open:s1:l1", "open", "m1", transition)
    assert first == 900.0
    assert second == 900.0
    assert calls == 1
    assert repo.load().balance == 900.0


def test_in_memory_apply_rolls_back_on_exception():
    repo = InMemoryPositionRepository(initial_balance=1000.0)

    def transition(state: PositionState) -> None:
        state.balance = 0.0
        raise ValueError("reject")

    with pytest.raises(ValueError, match="reject"):
        repo.apply("bad", "open", "m1", transition)
    assert repo.load().balance == 1000.0
```

- [ ] **Step 2: Run tests and confirm RED**

Run: `uv run pytest tests/routing/test_position_repository.py -q`

Expected: collection fails because `position_repository` does not exist.

- [ ] **Step 3: Implement state cloning and idempotent in-memory apply**

Use these public signatures:

```python
@dataclass
class PositionState:
    balance: float
    snapshot_balance: float
    realized_pnl: float = 0.0
    open_positions: dict[str, Position] = field(default_factory=dict)


TransitionResult = bool | float | None
Transition = Callable[[PositionState], TransitionResult]


class PositionRepository(Protocol):
    def load(self) -> PositionState: ...
    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult: ...
```

`InMemoryPositionRepository.apply` must deep-copy the state before invoking the transition, publish the copy only after success, and memoize the JSON-safe result by `operation_id`.

- [ ] **Step 4: Run repository tests**

Run: `uv run pytest tests/routing/test_position_repository.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/routing/position_repository.py tests/routing/test_position_repository.py
git commit -m "feat(m2): define position repository contract"
```

### Task 3: Implement the transactional SQLite repository

**Files:**
- Modify: `src/polyarb/routing/position_repository.py`
- Modify: `tests/routing/test_position_repository.py`

**Interfaces:**
- Produces: `SQLitePositionRepository(db_path: Path, initial_balance: float, busy_timeout_ms: int = 5000)`.
- Guarantees: atomic account/position persistence, operation replay, additive idempotent schema initialization, and fail-closed corrupt-state handling.

- [ ] **Step 1: Add failing SQLite contract tests**

Cover exact behaviors:

```python
def test_sqlite_instances_share_committed_state(tmp_path):
    path = tmp_path / "positions.db"
    left = SQLitePositionRepository(path, initial_balance=1000.0)
    right = SQLitePositionRepository(path, initial_balance=1000.0)

    left.apply("debit-1", "open", "m1", lambda state: _debit(state, 125.0))
    assert right.load().balance == 875.0


def test_sqlite_duplicate_operation_returns_original_result(tmp_path):
    path = tmp_path / "positions.db"
    repo = SQLitePositionRepository(path, initial_balance=1000.0)
    calls = 0

    def transition(state):
        nonlocal calls
        calls += 1
        state.realized_pnl += 5.0
        return 5.0

    assert repo.apply("close:f1", "close", "m1", transition) == 5.0
    assert repo.apply("close:f1", "close", "m1", transition) == 5.0
    assert calls == 1
    assert repo.load().realized_pnl == 5.0
```

Also test rollback, one-position-per-market uniqueness, reopen with a new operation ID, incompatible account cardinality, and a changed configured initial balance preserving durable state.

- [ ] **Step 2: Run SQLite tests and confirm RED**

Run: `uv run pytest tests/routing/test_position_repository.py -q`

Expected: failures because `SQLitePositionRepository` is missing.

- [ ] **Step 3: Add the schema**

Use three tables:

```sql
CREATE TABLE IF NOT EXISTS m2_account_state (
    account_id TEXT PRIMARY KEY,
    snapshot_balance REAL NOT NULL,
    balance REAL NOT NULL,
    realized_pnl REAL NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m2_open_positions (
    market_id TEXT PRIMARY KEY,
    condition_id TEXT NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    stake REAL NOT NULL,
    entry_price REAL NOT NULL,
    current_price REAL NOT NULL,
    leg_id TEXT NOT NULL,
    opened_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS m2_applied_operations (
    operation_id TEXT PRIMARY KEY,
    operation_type TEXT NOT NULL,
    target_id TEXT NOT NULL,
    result_json TEXT NOT NULL,
    applied_at TEXT NOT NULL
);
```

- [ ] **Step 4: Implement `load` and `apply`**

Each connection must run:

```python
con.execute("PRAGMA journal_mode=WAL")
con.execute("PRAGMA foreign_keys=ON")
con.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
```

`apply` must `BEGIN IMMEDIATE`, return `json.loads(result_json)` on replay, load exactly one account row plus all positions, call the transition, replace the persisted account/position projection, insert the applied operation, and commit. Every exception must call `ROLLBACK` and re-raise. If account cardinality is not exactly one after initialization, raise `RepositoryStateError`.

- [ ] **Step 5: Run focused tests**

Run: `uv run pytest tests/routing/test_position_repository.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/routing/position_repository.py tests/routing/test_position_repository.py
git commit -m "feat(m2): add transactional sqlite position repository"
```

### Task 4: Make PositionTracker repository-backed

**Files:**
- Modify: `src/polyarb/routing/position_tracker.py`
- Modify: `tests/routing/test_position_tracker.py`

**Interfaces:**
- Consumes: `PositionRepository.apply` and `PositionRepository.load`.
- Produces: `PositionTracker(config=None, repository=None)` plus optional stable `operation_id` on state-changing methods.

- [ ] **Step 1: Add failing repository-backed tracker tests**

Test two trackers against the same SQLite file:

```python
def test_two_trackers_observe_the_same_open_and_close(tmp_path):
    path = tmp_path / "positions.db"
    repo1 = SQLitePositionRepository(path, initial_balance=1000.0)
    repo2 = SQLitePositionRepository(path, initial_balance=1000.0)
    first = PositionTracker(repository=repo1)
    second = PositionTracker(repository=repo2)

    assert first.open_position(
        "m1", "c1", "BUY", "YES", 100.0, 0.4,
        leg_id="l1", operation_id="open:s1:l1",
    )
    assert second.open_count == 1
    pnl = second.close_position_with_fill(
        Fill("m1", 0.5, 100.0), operation_id="close:f1"
    )
    assert pnl == pytest.approx(10.0)
    assert first.open_count == 0
    assert first.total_realized_pnl == pytest.approx(10.0)
```

Add tests proving duplicate open and close operation IDs do not double-change balance/PnL and failed exposure/full-fill checks leave the repository unchanged.

- [ ] **Step 2: Run focused tracker tests and confirm RED**

Run: `uv run pytest tests/routing/test_position_tracker.py -q`

Expected: signature or state-sharing failures.

- [ ] **Step 3: Refactor tracker mutations into transitions**

Use the repository as the only mutable source of truth. Read properties call `repository.load()`. `open_position`, `close_position`, and `close_position_with_fill` pass pure mutation closures into `repository.apply`. Keep backwards compatibility by constructing `InMemoryPositionRepository(config.initial_balance)` when no repository is injected.

State-changing signatures become:

```python
def open_position(..., leg_id: str = "", operation_id: str | None = None) -> bool: ...
def close_position(
    self, market_id: str, exit_price: float | None = None,
    operation_id: str | None = None,
) -> float: ...
def close_position_with_fill(
    self, fill: Fill, operation_id: str | None = None,
) -> float: ...
```

When an operation ID is omitted for legacy in-memory callers, derive a process-local unique ID. Durable CLI/engine paths must always pass a stable ID.

- [ ] **Step 4: Run tracker and repository tests**

Run: `uv run pytest tests/routing/test_position_repository.py tests/routing/test_position_tracker.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/routing/position_tracker.py tests/routing/test_position_tracker.py
git commit -m "refactor(m2): persist tracker transitions through repository"
```

### Task 5: Propagate stable execution operation IDs

**Files:**
- Modify: `src/polyarb/execution/engine.py`
- Modify: `tests/execution/test_engine.py`
- Modify: `tests/execution/test_arbitrage_e2e.py`

**Interfaces:**
- Produces stable IDs `open:{signal_id}:{leg_id}` and `close:{signal_id}:{leg_id}:{filled_at}`.
- Consumes existing `RoutingDecision.signal_id`, `ExecutionLeg.leg_id`, and `Fill.filled_at`.

- [ ] **Step 1: Add failing replay tests**

Execute the same decision twice against a SQLite-backed tracker and assert the second execution does not debit balance or duplicate positions. For paper-close, assert replay does not double-book realized PnL.

- [ ] **Step 2: Run the execution tests and confirm RED**

Run: `uv run pytest tests/execution/test_engine.py tests/execution/test_arbitrage_e2e.py -q`

Expected: the replay changes state twice or fails on the duplicate position.

- [ ] **Step 3: Pass operation identity through the engine**

Change `_update_tracker_for_leg` to accept `signal_id` and call:

```python
operation_id = f"open:{signal_id}:{leg.leg_id}"
self.tracker.open_position(..., operation_id=operation_id)
```

Pass a close operation identity to `close_position_with_fill`. For deterministic paper fills, use the open operation identity plus `:paper-close`; for real fills, use an immutable venue fill identity when the adapter later supplies one. Phase 3 must not use a newly generated timestamp as the sole retry identity.

- [ ] **Step 4: Run M2 execution tests**

Run: `uv run pytest tests/execution/test_engine.py tests/execution/test_arbitrage_e2e.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/execution/engine.py tests/execution/test_engine.py tests/execution/test_arbitrage_e2e.py
git commit -m "feat(m2): make execution state transitions idempotent"
```

### Task 6: Wire durable state through CLI and Makefile

**Files:**
- Modify: `src/polyarb/routing/config.py`
- Modify: `src/polyarb/cli_arbitrage.py`
- Modify: `Makefile`
- Modify: `tests/routing/test_config.py`
- Modify: `tests/cli/test_arbitrage_cli.py`
- Create: `tests/cli/test_arbitrage_cli_process.py`

**Interfaces:**
- Produces: `PositionConfig.db_path: Path`, default `data/m2-positions.db`, env override `POLYARB_POSITION_DB_PATH`.
- CLI option: `--db-path PATH` on `run`, `status`, and `close`, overriding settings.
- Make variables: `db=<path>` for `run-arb`, `status-arb`, and `close-arb`.

- [ ] **Step 1: Add failing config and subprocess tests**

The subprocess test must invoke four independent processes with the same temp database:

```python
def _cli(*args: str, cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "python", "-m", "polyarb.cli_arbitrage", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=False,
    )
```

Flow: `run --legs 1 --db-path <tmp>` leaves one position; fresh `status` sees it; fresh `close` books a positive PnL; final fresh `status` sees zero positions and the expected cumulative realized PnL. Add a replay test using a stable signal/operation identity exposed for test use, plus a locked/corrupt database non-zero exit test.

- [ ] **Step 2: Run CLI/config tests and confirm RED**

Run: `uv run pytest tests/routing/test_config.py tests/cli/test_arbitrage_cli.py tests/cli/test_arbitrage_cli_process.py -q`

Expected: missing `db_path`/`--db-path` failures.

- [ ] **Step 3: Add configuration and dependency construction**

Add to `PositionConfig`:

```python
db_path: Path = Path("data/m2-positions.db")
busy_timeout_ms: int = 5000
```

Replace `_TRACKER` use in real command handlers with:

```python
def _build_tracker(db_path: Path | None = None) -> PositionTracker:
    config = PositionConfig()
    resolved = db_path or config.db_path
    repository = SQLitePositionRepository(
        resolved,
        initial_balance=config.initial_balance,
        busy_timeout_ms=config.busy_timeout_ms,
    )
    return PositionTracker(config=config, repository=repository)
```

Tests that require pure in-memory state should inject or monkeypatch the tracker factory, not depend on module globals.

- [ ] **Step 4: Update Makefile recipes and help**

Each recipe must append `--db-path "$${db:-data/m2-positions.db}"`. Remove the warning that separate processes cannot share state. Keep paper-mode language explicit.

- [ ] **Step 5: Run CLI lifecycle tests**

Run: `uv run pytest tests/routing/test_config.py tests/cli/test_arbitrage_cli.py tests/cli/test_arbitrage_cli_process.py -q`

Expected: PASS, including real subprocess boundaries.

- [ ] **Step 6: Run Makefile contract tests**

Run: `uv run pytest tests/test_makefile.py -q`

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add src/polyarb/routing/config.py src/polyarb/cli_arbitrage.py Makefile tests/routing/test_config.py tests/cli/test_arbitrage_cli.py tests/cli/test_arbitrage_cli_process.py
git commit -m "feat(m2): share position state across cli processes"
```

### Task 7: Verify, teach, summarize, and close the phase

**Files:**
- Create: `docs/learning/13-仓位持久化.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-01-SUMMARY.md`
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify when learning applies: `.planning/threads/learnings-meta.md`

**Interfaces:**
- Produces: complete Phase 3 evidence, zero planning drift, learning artifact, and exact resume command.

- [ ] **Step 1: Run the focused M2 suite**

Run:

```bash
uv run pytest tests/models/test_signal.py tests/models/test_slippage.py tests/routing tests/execution tests/cli -q
```

Expected: all M2 tests PASS.

- [ ] **Step 2: Run the broader contract gates**

Run:

```bash
uv run pytest tests/test_makefile.py -q
make planning-status
git diff --check
```

Expected: tests PASS, no planning drift, no whitespace errors.

- [ ] **Step 3: Perform an operator smoke test with an isolated database**

Run:

```bash
tmp_db="data/m2-phase3-smoke.db"
make run-arb db="$tmp_db" mid=0.40 stake=100 legs=1
make status-arb db="$tmp_db"
make close-arb db="$tmp_db" market_id=cond-0 exit_price=0.50
make status-arb db="$tmp_db"
```

Expected: the second command sees one open position; close reports positive PnL; final status sees zero positions and retained realized PnL. Remove the smoke database only after confirming it is exactly the dedicated test path.

- [ ] **Step 4: Write the teaching document**

Include:

```markdown
## 30 秒心智模型
CLI 是短命进程；SQLite repository 才是长期账户记忆。PositionTracker 负责
“这笔交易是否合法、PnL 怎么算”，repository 负责“这一组变化要么全落、要么全不落”。

## 自检题
1. 为什么 JSON snapshot 不能自然防止同一 close 重放两次？
2. 为什么 `BEGIN IMMEDIATE` 要在读取账户状态之前执行？
3. 为什么 market_id 不能单独充当永久 operation_id？
```

Add real `file:line` references after implementation line numbers stabilize, plus design trade-offs and an empty FAQ increment section.

- [ ] **Step 5: Write the plan SUMMARY before closure**

The SUMMARY must list every implementation commit, test counts, subprocess evidence, deviations, API surface, and any remaining risks. Then run `make planning-status` and require `SUMMARY ✓`.

- [ ] **Step 6: Extract learnings and ask adversarial questions**

Record 3–5 operational questions, including duplicate close replay, concurrent `run` processes, initial-balance config mismatch, database corruption, and reopen-after-close identity.

- [ ] **Step 7: Update ROADMAP, STATE, and JOURNAL**

Mark Phase 3 complete only after all gates pass. The JOURNAL entry must state the exact next-session command and whether the next M2 candidate is partial-fill aggregation or real venue adapter.

- [ ] **Step 8: Commit closure artifacts**

```bash
git add docs/learning/00-INDEX.md docs/learning/13-仓位持久化.md .planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-01-SUMMARY.md .planning/workstreams/m2-combinatorial/ROADMAP.md .planning/workstreams/m2-combinatorial/STATE.md .planning/JOURNAL.md .planning/threads/learnings-meta.md
git commit -m "docs(m2): close position persistence phase"
```

---

## Execution Choice

Execute inline in the current climb session. The repository's multi-agent policy does not authorize subagent dispatch, and climb requires advancing without conversational checkpoints. Apply TDD task-by-task, review each diff before its commit, and never begin the next task before the current plan SUMMARY discipline is satisfied.
