# M2 Durable Close Receipts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make a committed operator or venue close recoverable across process/response loss when the caller reuses one immutable operation identity.

**Architecture:** Promote the existing applied-operation ledger row to a public immutable `OperationReceipt`, expose lookup through the repository and tracker boundaries, and keep `apply()` as the final concurrency/idempotency authority. The CLI accepts a caller-owned operation ID, returns stored close results on replay, and labels locally generated identities as not retry-safe. Venue fills gain an optional immutable `fill_id`; the execution engine prefers it and warns when it must retain the legacy timestamp fallback.

**Tech Stack:** Python 3.12, stdlib `sqlite3`/`uuid`/`logging`, dataclasses and Protocol, Typer, pytest, uv, Makefile, GSD workstream artifacts, climb local evaluation.

## Global Constraints

- No live venue calls, credentials, wallet signing, reconciliation daemon, or outbox worker.
- Use `uv`; do not install dependencies with `pip`.
- Follow strict RED → GREEN: add the specified failing test, observe the expected failure, then implement only the behavior under test.
- Keep `PositionRepository.apply()` as the race-safe mutation authority; receipt lookup is observational and must not create an account or swallow SQLite errors.
- Preserve paper-close identity and existing commands without explicit operation IDs.
- Only a caller-supplied operation ID earns `retry_safe: true`; a generated ID must report `retry_safe: false`.
- Reusing an operation ID for another operation type or market fails closed.
- Every executable surface remains in `Makefile` and visible through `make help`.
- Create `04-01-SUMMARY.md` immediately after code commits and before any later plan.
- Do not modify or stage the main worktree's `CLAUDE.md` type change or untracked `AGENTS.md`.

## File Map

- Modify `src/polyarb/routing/position_repository.py`: public immutable receipt contract and lookup in both repositories.
- Modify `src/polyarb/routing/position_tracker.py`: receipt delegation and optional venue `Fill.fill_id`.
- Modify `src/polyarb/execution/engine.py`: stable venue-fill close identity with explicit legacy warning.
- Modify `src/polyarb/cli_arbitrage.py`: `--operation-id`, replay/conflict handling, and response metadata.
- Modify `Makefile`: forward `operation_id=` through `make close-arb`.
- Modify `tests/routing/test_position_repository.py`: receipt lookup/type/error contract.
- Modify `tests/routing/test_position_tracker.py`: public tracker receipt delegation.
- Modify `tests/execution/test_engine.py`: immutable venue fill identity and legacy warning.
- Modify `tests/cli/test_arbitrage_cli_process.py`: true response-loss recovery, conflict, and reopen lifecycle.
- Modify `tests/test_makefile.py`: operation ID forwarding contract.
- Modify `docs/learning/13-仓位持久化.md`: operation-receipt mental model and FAQ.
- Create `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/*`: approved GSD Phase 4 context, plan, summary, and learnings.
- Modify `.planning/workstreams/m2-combinatorial/{ROADMAP.md,STATE.md}` and `.planning/JOURNAL.md`: durable project state.

---

### Task 1: Register the approved work as M2 Phase 4

**Files:**
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-CONTEXT.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-DISCUSSION-LOG.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-01-PLAN.md`

**Inputs:**
- `docs/superpowers/specs/2026-07-17-m2-durable-close-receipts-design.md`
- This implementation plan.

- [ ] **Step 1: Add the phase using the active workstream tooling**

Run:

```bash
node "$HOME/.codex/get-shit-done/bin/gsd-tools.cjs" phase add "Durable Close Receipts"
```

Expected: Phase 4 and `04-durable-close-receipts/` are added under `m2-combinatorial`.

- [ ] **Step 2: Write context and discussion artifacts from the approved spec**

The phase boundary must be:

```markdown
Expose already-committed close results through durable receipts and make
caller-owned operator/venue identities replayable across response and process
loss. Live venue reconciliation, outbox workers, partial fills, and mandatory
adapter fill IDs remain out of scope.
```

Record the selected explicit-receipt approach and the rejected derived-ID and full-reconciliation approaches. Do not reopen approved design questions.

- [ ] **Step 3: Write one GSD plan that owns Tasks 2–5 below**

The plan must name these deliverables:

```markdown
- `PositionRepository.get_receipt(operation_id)`
- `PositionTracker.operation_receipt(operation_id)`
- `make close-arb ... operation_id=<immutable-id>`
- a true multi-process response-loss recovery test
- `Fill.fill_id` as the future venue identity seam
```

- [ ] **Step 4: Update phase progress and verify the generated metadata**

Run the GSD roadmap/state progress commands appropriate to Phase 4, then read `ROADMAP.md` and `STATE.md` back. Repair any unchanged placeholder fields before continuing.

Run:

```bash
make planning-status
```

Expected: Phase 4 is recognized as `NOT-STARTED` with zero drift; all completed Phase 3 plan commits remain bounded by its SUMMARY.

- [ ] **Step 5: Commit Phase 4 planning metadata**

```bash
git add .planning/workstreams/m2-combinatorial/ROADMAP.md \
  .planning/workstreams/m2-combinatorial/STATE.md \
  .planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts
git commit -m "docs(m2): plan durable close receipts phase"
```

---

### Task 2: Expose immutable operation receipts from both repositories

**Files:**
- Modify: `src/polyarb/routing/position_repository.py`
- Modify: `tests/routing/test_position_repository.py`

**Public contract:**

```python
@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    operation_type: str
    target_id: str
    result: TransitionResult


class PositionRepository(Protocol):
    def load(self) -> PositionState: ...
    def get_receipt(self, operation_id: str) -> OperationReceipt | None: ...
    def apply(
        self,
        operation_id: str,
        operation_type: str,
        target_id: str,
        transition: Transition,
    ) -> TransitionResult: ...
```

- [ ] **Step 1: Add RED tests for in-memory lookup**

Add parametrized coverage for `True`, `False`, `3.25`, and `None`:

```python
@pytest.mark.parametrize("result", [True, False, 3.25, None])
def test_in_memory_receipt_round_trips_identity_and_result(result) -> None:
    repository = InMemoryPositionRepository(initial_balance=1000.0)

    assert repository.get_receipt("unknown") is None
    repository.apply("op-1", "close", "m1", lambda state: result)

    receipt = repository.get_receipt("op-1")
    assert receipt == OperationReceipt("op-1", "close", "m1", result)
    with pytest.raises(FrozenInstanceError):
        receipt.target_id = "m2"
```

Also assert a returned receipt is detached: replacing/mutating the local reference must not change the repository's next lookup.

- [ ] **Step 2: Observe RED**

Run:

```bash
uv run pytest tests/routing/test_position_repository.py -q
```

Expected: import/attribute failures for `OperationReceipt` or `get_receipt`.

- [ ] **Step 3: Implement the in-memory public receipt contract**

Replace the private `AppliedOperation` record with `OperationReceipt`, including `operation_id`. Store receipts in `_operations`; return a deep-copied receipt from `get_receipt`. Keep existing replay conflict validation unchanged.

- [ ] **Step 4: Add RED SQLite receipt tests**

Add tests that:

1. return `None` for an unknown ID;
2. round-trip bool/float/None without type loss from a second repository instance;
3. return all three identity fields;
4. prove lookup does not insert ledger rows;
5. prove storage errors propagate by monkeypatching that repository instance's `_connect` to raise `sqlite3.DatabaseError("storage unavailable")`, then asserting the same exception, not `None`.

Use a fresh path per result so the ledger primary key never masks the case.

- [ ] **Step 5: Observe SQLite RED**

Run:

```bash
uv run pytest tests/routing/test_position_repository.py -q
```

Expected: SQLite repository has no `get_receipt`.

- [ ] **Step 6: Implement SQLite lookup without a schema migration**

Use one parameterized query:

```sql
SELECT operation_type, target_id, result_json
FROM m2_applied_operations
WHERE operation_id = ?
```

Return `OperationReceipt(operation_id, row[0], row[1], json.loads(row[2]))`; allow connection/query/JSON exceptions to propagate. Reuse `get_receipt` inside neither `_initialize()` nor `load()` so observational lookup cannot initialize or mutate state beyond the repository constructor's existing additive schema initialization.

- [ ] **Step 7: Run contract and static gates**

```bash
uv run pytest tests/routing/test_position_repository.py -q
uv run ruff check src/polyarb/routing/position_repository.py tests/routing/test_position_repository.py
git diff --check
```

Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
git add src/polyarb/routing/position_repository.py tests/routing/test_position_repository.py
git commit -m "feat(m2): expose durable operation receipts"
```

---

### Task 3: Carry receipts through the tracker and immutable venue fill IDs through the engine

**Files:**
- Modify: `src/polyarb/routing/position_tracker.py`
- Modify: `src/polyarb/execution/engine.py`
- Modify: `tests/routing/test_position_tracker.py`
- Modify: `tests/execution/test_engine.py`

**Interfaces:**

```python
@dataclass
class Fill:
    market_id: str
    exit_price: float
    filled_size: float
    filled_at: datetime = field(default_factory=datetime.utcnow)
    fill_id: str = ""


class PositionTracker:
    def operation_receipt(self, operation_id: str) -> OperationReceipt | None:
        return self.repository.get_receipt(operation_id)
```

- [ ] **Step 1: Add RED tracker delegation test**

Open and close a position with explicit IDs, then assert:

```python
receipt = tracker.operation_receipt("close:f1")
assert receipt is not None
assert receipt.operation_id == "close:f1"
assert receipt.operation_type == "close"
assert receipt.target_id == "m1"
assert receipt.result == pytest.approx(10.0)
assert tracker.operation_receipt("unknown") is None
```

Ensure the test uses only the tracker's public method, never `tracker.repository`.

- [ ] **Step 2: Observe RED, implement delegation, and verify GREEN**

```bash
uv run pytest tests/routing/test_position_tracker.py -q
```

Expected RED: missing `operation_receipt`. Import `OperationReceipt`, add the one-line delegation, rerun for PASS.

- [ ] **Step 3: Add RED engine tests for stable venue fill identity**

Create a durable tracker and a `fill_provider` returning:

```python
Fill(
    market_id=leg.asset,
    exit_price=0.525,
    filled_size=leg.size,
    fill_id="venue-fill-001",
)
```

Execute the same decision twice and assert:

- exactly two ledger rows exist: one open and one close;
- the close row ID is `close:{signal_id}:{leg_id}:fill:venue-fill-001`;
- realized PnL and balance are booked once;
- no durability warning is logged.

Add a compatibility test where `fill_id` is empty and `caplog` contains `durable retry guarantees unavailable`; assert the timestamp-shaped legacy identity remains in use. Keep the existing paper-close replay test unchanged.

- [ ] **Step 4: Observe engine RED**

```bash
uv run pytest tests/execution/test_engine.py -q
```

Expected: `Fill` rejects `fill_id` and/or the stored operation ID uses `filled_at`.

- [ ] **Step 5: Implement `fill_id` selection and explicit fallback warning**

In `_maybe_close_for_leg`, choose:

```python
if fill.fill_id:
    operation_id = f"close:{signal_id}:{leg.leg_id}:fill:{fill.fill_id}"
else:
    logger.warning(
        "venue fill for leg %s has no fill_id; durable retry guarantees unavailable",
        leg.leg_id,
    )
    operation_id = f"close:{signal_id}:{leg.leg_id}:{fill.filled_at.isoformat()}"
```

Pass that ID to `close_position_with_fill`. Do not warn for synthesized paper close.

- [ ] **Step 6: Run focused and regression gates**

```bash
uv run pytest tests/routing/test_position_tracker.py tests/execution/test_engine.py -q
uv run pytest tests/execution/test_arbitrage_e2e.py -q
uv run ruff check src/polyarb/routing/position_tracker.py src/polyarb/execution/engine.py \
  tests/routing/test_position_tracker.py tests/execution/test_engine.py
git diff --check
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/polyarb/routing/position_tracker.py src/polyarb/execution/engine.py \
  tests/routing/test_position_tracker.py tests/execution/test_engine.py
git commit -m "feat(m2): bind venue closes to fill identities"
```

---

### Task 4: Recover operator closes across response loss

**Files:**
- Modify: `src/polyarb/cli_arbitrage.py`
- Modify: `Makefile`
- Modify: `tests/cli/test_arbitrage_cli_process.py`
- Modify: `tests/test_makefile.py`

**CLI option:**

```python
operation_id: str | None = typer.Option(
    None,
    "--operation-id",
    help="Caller-owned immutable close identity for cross-process retry",
)
```

**Success JSON:**

```json
{
  "closed": "cond-0",
  "operation_id": "operator-close-001",
  "replayed": false,
  "retry_safe": true,
  "exit_price": 0.5,
  "realized_pnl": 10.0,
  "total_realized_pnl": 10.0
}
```

- [ ] **Step 1: Add the RED true-subprocess recovery test**

Add `test_close_receipt_recovers_lost_response_across_processes` with one temporary database:

1. run one-leg signal at entry `0.40`, stake `100`, explicit signal ID;
2. call close with `--operation-id close-001`, require exit 0, but deliberately do not parse/save stdout;
3. call the identical close from a new process;
4. assert exit 0, `operation_id == "close-001"`, `replayed is True`, `retry_safe is True`, and `realized_pnl == 10.0`;
5. call status and assert balance `1010.0`, total PnL `10.0`, zero positions;
6. inspect SQLite and assert exactly one `operation_id='close-001'` row;
7. reuse `close-001` with another market ID and assert non-zero plus `operation identity conflict`;
8. rerun/reopen `cond-0` with a different signal ID, close with `close-002`, and assert cumulative PnL becomes `20.0` with two close rows.

Add a separate compatibility assertion that an omitted operation ID returns a nonempty generated ID with `replayed is False` and `retry_safe is False`.

- [ ] **Step 2: Observe CLI RED**

```bash
uv run pytest tests/cli/test_arbitrage_cli_process.py -q
```

Expected: `--operation-id` is rejected or replay exits “no open position.”

- [ ] **Step 3: Implement the replay-first close flow**

Before looking for an open position:

1. record `caller_supplied = operation_id is not None`;
2. choose `effective_operation_id = operation_id or f"local:operator-close:{market_id}:{uuid4()}"`;
3. when caller supplied the ID, call `tracker.operation_receipt(effective_operation_id)`;
4. if found, validate `operation_type == "close"` and `target_id == market_id`; otherwise print `operation identity conflict` to stderr and exit 2;
5. require `receipt.result` to be a float (reject bool despite bool being a float subclass in Python); otherwise fail closed as a corrupt close receipt;
6. return the stored result without requiring/opening/closing a position, with `replayed: true`;
7. if not found, retain the current open-position and full-fill validation, pass `effective_operation_id` explicitly to `close_position_with_fill`, and return `replayed: false`.

For both paths include `operation_id`, `retry_safe=caller_supplied`, close PnL, and current cumulative PnL. On replay, keep the requested `exit_price` in output only as request context; the stored receipt is authoritative for PnL.

- [ ] **Step 4: Add RED Makefile forwarding test**

Extend the dry-run contract:

```python
result = _make(
    "-n",
    "close-arb",
    "db=build/test.db",
    "market_id=cond-0",
    "exit_price=0.5",
    "operation_id=close-001",
)
assert '--operation-id "close-001"' in result.stdout
```

Also assert the `close-arb` help/usage text mentions `operation_id=`.

- [ ] **Step 5: Observe Makefile RED, implement forwarding, and verify GREEN**

Use a shell variable that is empty unless `operation_id` is set:

```make
OPERATION_FLAG=""; \
if [ -n "$${operation_id}" ]; then OPERATION_FLAG="--operation-id \"$${operation_id}\""; fi; \
```

Prefer direct conditional invocation if embedded quotes are not preserved by the current shell. The verified output must pass the ID as one argument and remain compatible with the default macOS `/bin/sh` behavior used by this Makefile.

Run:

```bash
uv run pytest tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py -q
make -n close-arb db=build/test.db market_id=cond-0 exit_price=0.5 operation_id=close-001
```

Expected: all PASS and dry-run contains `--operation-id`.

- [ ] **Step 6: Run focused CLI regressions and Ruff**

```bash
uv run pytest tests/cli -q
uv run ruff check src/polyarb/cli_arbitrage.py tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py
git diff --check
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/polyarb/cli_arbitrage.py Makefile \
  tests/cli/test_arbitrage_cli_process.py tests/test_makefile.py
git commit -m "feat(m2): recover durable operator closes"
```

---

### Task 5: Teach, verify, close Phase 4, and evaluate H-002

**Files:**
- Modify: `docs/learning/13-仓位持久化.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-01-SUMMARY.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-LEARNINGS.md`
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `docs/status/climb/*` state/evidence files generated by `make climb-cycle hypothesis=H-002`

- [ ] **Step 1: Update the existing teaching chapter**

Add a “durable receipt” section and FAQ increment that explain:

- an open-position projection answers “what is open now,” while the receipt ledger answers “did operation X already commit?”;
- why market/price/timestamp-derived IDs are insufficient across reopen/retry;
- why repository `apply()` still owns race correctness after a read-before-write receipt check;
- why generated CLI IDs are convenient but not response-loss safe;
- how a future adapter should populate `Fill.fill_id`.

Include current `file:line` references after implementation and these adversarial self-checks:

1. A close response is lost; which value must the caller retain to recover the original result?
2. Why is “position missing” not proof that a close never committed?
3. What happens if two processes race with the same ID?
4. Why must reopening the same market use a new close ID?
5. Why does a timestamp fallback not represent venue truth?

- [ ] **Step 2: Run the corrected complete M2 gate**

Use non-overlapping paths:

```bash
uv run pytest \
  tests/models/test_slippage.py \
  tests/routing \
  tests/execution \
  tests/cli -q
uv run pytest tests/test_makefile.py -q
uv run ruff check \
  src/polyarb/routing/position_repository.py \
  src/polyarb/routing/position_tracker.py \
  src/polyarb/execution/engine.py \
  src/polyarb/cli_arbitrage.py
git diff --check
make planning-status
```

Expected: every test passes, Ruff passes, no whitespace errors, zero planning drift.

- [ ] **Step 3: Perform an operator smoke with independent processes**

With a temporary database, execute `run`, discard the first explicit-ID close response, retry it, inspect status and ledger count, then reopen and close with a new ID. Remove only the validated temporary database and its SQLite sidecars afterward.

Expected invariant:

```text
first close: committed + response discarded
retry close-001: replayed=true, PnL=10
after retry: balance=1010, realized=10, close-001 rows=1
reopen + close-002: balance=1020, realized=20, close rows=2
```

- [ ] **Step 4: Write and commit the plan SUMMARY immediately**

Create `04-01-SUMMARY.md` from the GSD summary template. Include exact commits, files, test counts, receipt failure semantics, and deviations from this plan.

```bash
git add docs/learning/13-仓位持久化.md \
  .planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-01-SUMMARY.md
git commit -m "docs(m2): summarize durable close receipts"
make planning-status
```

Expected: Phase 4 plan is `OK`, not DRIFT.

- [ ] **Step 5: Extract learnings and close Phase 4 metadata**

Create `04-LEARNINGS.md` with decisions, patterns, surprises, and the five adversarial questions. Mark Phase 4 `1/1 complete` in ROADMAP, update STATE to completed, and append a JOURNAL session entry with a concrete `[NEXT]` command.

Run `make planning-status` after metadata edits and repair any stale GSD-tool placeholders before commit.

- [ ] **Step 6: Run the H-002 climb cycle**

```bash
make climb-cycle hypothesis=H-002
```

Expected: local evaluator exercises repository/tracker, execution, CLI restart recovery, and planning gates; H-002 moves from pending to confirmed only if every subscore meets the configured threshold. If it does not, keep H-002 active and use the score breakdown to choose the next bounded fix rather than manually overriding status.

- [ ] **Step 7: Final verification and closure commit**

```bash
make planning-status
git status --short
git log --oneline -8
```

Expected: zero planning drift; only intentional planning/climb closure files remain staged or modified.

Commit the closure artifacts:

```bash
git add .planning/workstreams/m2-combinatorial/ROADMAP.md \
  .planning/workstreams/m2-combinatorial/STATE.md \
  .planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-LEARNINGS.md \
  .planning/JOURNAL.md docs/status/climb
git commit -m "docs(m2): close durable close receipts phase"
```

Stage only the exact tracked `docs/status/climb/*` paths shown by `git status`; never use a broad add that captures unrelated user files or generated `runs/climb/` evidence unless the repository already tracks that exact run artifact.
