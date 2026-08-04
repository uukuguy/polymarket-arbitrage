# M1 Structure Pointer-Switch Dual-Deadline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a production `ready` Structure publication survive Python child startup while preserving a separately enforced 15-second atomic pointer-switch transaction deadline.

**Architecture:** Keep the existing schema-ready Structure child and shared producer lane, but give the complete child the established 75-second hard envelope. Move the 15-second authority deadline into `SQLiteStore.publish_structure_generation`, cap writer-lock acquisition at five seconds, interrupt overdue SQLite work, and expose distinct budgets and bounded failure evidence through the existing CLI, scheduler, and health contracts.

**Tech Stack:** Python 3.12, asyncio subprocess isolation, sqlite3 WAL transactions/progress handlers, Typer JSON child protocol, pytest, Ruff, Fly.io Machines.

## Global Constraints

- Structure remains enabled, generation reads remain `legacy`, Quote remains disabled, and resident cleanup remains enabled.
- The complete Structure child hard limit is exactly 75 seconds.
- The atomic pointer-switch transaction deadline is exactly 15 seconds.
- SQLite writer-lock acquisition is capped at exactly five seconds.
- No parent-process pointer write, manual pointer advance, restart, or manual cleanup is allowed for production acceptance.
- All production mutations remain atomic under `BEGIN IMMEDIATE`; deadline failure rolls back every authority field.
- No new dependency is permitted; `uv.lock` and `pyproject.toml` remain unchanged.
- Preserve the five user-owned `.superpowers/sdd/*.md` modifications without staging or editing them.

---

### Task 1: Separate Scheduler Child and Transaction Budgets

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py:1-35`
- Modify: `src/polyarb/daemon/scheduler.py:108-140,1070-1105`
- Modify: `src/polyarb/http/health.py:1290-1335`
- Modify: `tests/m1-perception/test_scheduler.py:2020-2070`
- Modify: `tests/m1-perception/test_health_endpoint.py:1370-1450`

**Interfaces:**
- Consumes: publication status returned by `SQLiteStore.get_latest_structure_publication()`.
- Produces shared constants in `polyarb.perception.structure_contract`: `STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S: float`, `STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S: float`, and `STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S: float`.
- Produces a uniform 75-second result from `structure_attempt_slot_budget_s(publication_status: object) -> float` without introducing a daemon-to-perception import cycle.

- [ ] **Step 1: Write failing scheduler and health tests**

Replace the ready-budget expectation and assert distinct health labels:

```python
@pytest.mark.asyncio
async def test_ready_structure_publication_keeps_child_hard_envelope(
    daemon_settings_for_test, monkeypatch
) -> None:
    observed_timeout_s = None

    async def run_snapshot(*, timeout_s: float):
        nonlocal observed_timeout_s
        observed_timeout_s = timeout_s
        return SimpleNamespace(status=SnapshotStatus.OK)

    monkeypatch.setattr(scheduler_module, "run_snapshot_in_subprocess", run_snapshot)
    store = MagicMock()
    store.get_latest_structure_publication.return_value = SimpleNamespace(status="ready")
    scheduler = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    scheduler._effective_timeout_s = 240

    await scheduler._run_snapshot()

    assert observed_timeout_s == 75
```

Health expectations must contain:

```python
assert "generation_child_hard_limit_s=75" in output
assert "pointer_switch_transaction_deadline_s=15" in output
assert "pointer_switch_writer_lock_timeout_s=5" in output
assert "pointer_switch_hard_deadline_s" not in output
```

- [ ] **Step 2: Run the focused tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_scheduler.py::test_ready_structure_publication_keeps_child_hard_envelope \
  tests/m1-perception/test_health_endpoint.py::test_health_surfaces_short_incomplete_structure_slice_budget \
  tests/m1-perception/test_health_endpoint.py::test_health_surfaces_bounded_generation_publication_budgets
```

Expected: the scheduler test reports `15 != 75`; health tests cannot find the two new labels and still see the ambiguous legacy label.

- [ ] **Step 3: Implement the minimal budget split**

Put explicit shared constants in `structure_contract.py`, import them into the
scheduler and health builder, and keep every child in the same envelope:

```python
STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S = 75.0
STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S = 15.0
STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S = 5.0


def structure_attempt_slot_budget_s(publication_status: object) -> float:
    """Bound the complete isolated child; transaction timing is store-owned."""
    return STRUCTURE_GENERATION_CHILD_HARD_LIMIT_S
```

Update health output to emit all three unambiguous fields and remove
`pointer_switch_hard_deadline_s`.

- [ ] **Step 4: Run focused scheduler and health tests and verify GREEN**

Run the Step 2 command.

Expected: all three tests pass.

- [ ] **Step 5: Run the complete scheduler/health contract slice**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py
```

Expected: all tests pass with no unexpected warnings.

- [ ] **Step 6: Commit Task 1**

```bash
git add src/polyarb/perception/structure_contract.py \
  src/polyarb/daemon/scheduler.py src/polyarb/http/health.py \
  tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py
git commit -m "fix(m1): separate structure child and pointer budgets"
```

---

### Task 2: Enforce the Atomic Pointer Transaction Deadline

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:10977-11120`
- Modify: `src/polyarb/perception/structure_publication.py:430-480`
- Modify: `src/polyarb/snapshot/cli.py:40-70`
- Modify: `src/polyarb/daemon/scheduler.py:650-735`
- Modify: `tests/m1-perception/test_structure_generation_publication.py:3880-3995`
- Modify: `tests/m1-perception/test_snapshot_cli_json.py:300-350`
- Modify: `tests/m1-perception/test_scheduler.py:1740-1810`

**Interfaces:**
- Consumes: Task 1 constants from `polyarb.perception.structure_contract`: `STRUCTURE_POINTER_SWITCH_TRANSACTION_DEADLINE_S` and `STRUCTURE_POINTER_SWITCH_WRITER_LOCK_TIMEOUT_S`.
- Produces: `StructurePointerSwitchDeadlineError(ValueError)`, and `SQLiteStore.publish_structure_generation(publication_id: str, now_ms: int, *, transaction_deadline_s: float = 15.0, writer_lock_timeout_s: float = 5.0, trace_callback: Callable[[str], None] | None = None, monotonic: Callable[[], float] = time.monotonic) -> int`.
- Produces child failure kind: `pointer-switch-deadline`.

- [ ] **Step 1: Write failing atomic rollback and success tests**

Add a fake monotonic clock advanced by the existing trace callback:

```python
def test_pointer_switch_deadline_rolls_back_all_authority(tmp_path: Path) -> None:
    store, publication = _ready_successor_store(tmp_path)
    before = _pointer_authority_rows(store.db_path)
    clock = FakeMonotonic()

    def trace(_statement: str) -> None:
        clock.advance(16.0)

    with pytest.raises(
        StructurePointerSwitchDeadlineError,
        match="pointer-switch-deadline",
    ):
        store.publish_structure_generation(
            publication.publication_id,
            now_ms=11_009,
            transaction_deadline_s=15.0,
            writer_lock_timeout_s=5.0,
            trace_callback=trace,
            monotonic=clock,
        )

    assert _pointer_authority_rows(store.db_path) == before
```

Add a successful switch test using the same explicit budgets and assert the
pointer, publication, snapshot, and window all commit together.

- [ ] **Step 2: Write failing CLI and parent-protocol tests**

Assert `_structure_failure_kind(StructurePointerSwitchDeadlineError(...))`
returns `pointer-switch-deadline`, the CLI JSON contains only the bounded
failure object, and `run_snapshot_in_subprocess` accepts that allowlisted
failure without converting it to `invalid-json`. A five-second writer-lock
exhaustion remains the existing `sqlite-busy` failure kind; it is distinct from
crossing the 15-second transaction deadline.

- [ ] **Step 3: Run Task 2 tests and verify RED**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_structure_generation_publication.py -k 'pointer_switch_deadline or pointer_switch_exception' \
  tests/m1-perception/test_snapshot_cli_json.py -k 'pointer_switch_deadline' \
  tests/m1-perception/test_scheduler.py -k 'pointer_switch_deadline'
```

Expected: import/signature/failure-kind assertions fail because the deadline
exception and protocol value do not yet exist.

- [ ] **Step 4: Implement deadline ownership in the store**

Add the bounded exception and validate budgets:

```python
class StructurePointerSwitchDeadlineError(ValueError):
    """Atomic generation-pointer transaction exceeded its authority budget."""


def publish_structure_generation(..., transaction_deadline_s=15.0,
                                 writer_lock_timeout_s=5.0,
                                 monotonic=time.monotonic) -> int:
    if transaction_deadline_s <= 0 or not 0 < writer_lock_timeout_s <= transaction_deadline_s:
        raise ValueError("invalid-pointer-switch-deadline")
    deadline = monotonic() + transaction_deadline_s
    con = self._connect_writer(timeout_s=writer_lock_timeout_s)

    def ensure_deadline() -> None:
        if monotonic() >= deadline:
            raise StructurePointerSwitchDeadlineError("pointer-switch-deadline")

    con.set_progress_handler(lambda: int(monotonic() >= deadline), 1_000)
```

Call `ensure_deadline()` before `BEGIN IMMEDIATE`, before every
authority-changing statement, and immediately before `COMMIT`. Convert SQLite
`interrupted` raised after the deadline into
`StructurePointerSwitchDeadlineError`, roll back under the existing
`BaseException` handler, clear the progress handler, and close the connection.

- [ ] **Step 5: Thread budgets through publication and bound the protocol**

Pass the Task 1 transaction and lock constants from
`run_structure_publication_step` to `publish_structure_generation`. Add
`pointer-switch-deadline` to `_STRUCTURE_FAILURE_KINDS`, the scheduler child
payload allowlist, and `_STRUCTURE_FAILURE_MARKER_RE`.

- [ ] **Step 6: Run Task 2 tests and verify GREEN**

Run the Step 3 command.

Expected: all selected tests pass; rollback assertions prove the old pointer
and every authority row are unchanged.

- [ ] **Step 7: Run full publication/CLI/scheduler regression slices**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_snapshot_cli_json.py \
  tests/m1-perception/test_scheduler.py
```

Expected: all tests pass.

- [ ] **Step 8: Commit Task 2**

```bash
git add src/polyarb/storage/sqlite_store.py \
  src/polyarb/perception/structure_publication.py src/polyarb/snapshot/cli.py \
  src/polyarb/daemon/scheduler.py \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_snapshot_cli_json.py \
  tests/m1-perception/test_scheduler.py
git commit -m "fix(m1): bound atomic structure pointer switch"
```

---

### Task 3: Recovery Chain, Documentation, and Release Gate

**Files:**
- Modify: `tests/m1-perception/test_scheduler.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `docs/learning/18-m1-production-operations-handbook.md`
- Modify: `.planning/JOURNAL.md`

**Interfaces:**
- Consumes: Task 1 budget constants and Task 2 bounded deadline failure.
- Produces: exact regression evidence, protected Fly config gate, operator documentation, and a deployable exact SHA.

- [ ] **Step 1: Add the recovery-chain regression test**

Construct a scheduler in `RECOVERING` with a persisted failure streak, return
one `pointer-switch-deadline` failure followed by a certified
`SnapshotStatus.OK` result, and assert:

```python
assert scheduler.state is SchedulerState.RUNNING
assert scheduler._failure_counter == 0
assert store.get_scheduler_state() == {
    "state": "RUNNING",
    "failure_counter": 0,
}
```

The same test asserts the failed attempt contains the bounded failure kind and
the successful attempt contains the published snapshot ID.

- [ ] **Step 2: Verify the recovery test is RED, then GREEN without new behavior**

Run:

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py -k 'pointer_switch_recovery'
```

Expected before fixture completion: FAIL on missing exact durable evidence.
Complete only the test fixture and existing-path assertions; production logic
from Tasks 1-2 must already make it pass.

- [ ] **Step 3: Update protected deployment and operator documentation**

Update the operations handbook with the three deadline meanings, the
`pointer-switch-deadline` health/attempt evidence, and the rule that operators
must not manually switch the pointer. Update the protected Fly test to assert:

```python
assert env["POLYARB_STRUCTURE_SYNC_ENABLED"] == "true"
assert env["POLYARB_STRUCTURE_GENERATION_READ_MODE"] == "legacy"
assert env["POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED"] == "false"
assert env["POLYARB_STRUCTURE_GENERATION_CLEANUP_ENABLED"] == "true"
```

- [ ] **Step 4: Run all focused M1 verification**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_snapshot_cli_json.py \
  tests/m1-perception/test_structure_generation_publication.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py
uv run ruff check src tests
git diff --check
make docs-check-m1
make planning-status
```

Expected: all tests pass, Ruff and documentation checks are clean, and
planning status reports no drift.

- [ ] **Step 5: Run the complete M1 suite**

Run:

```bash
uv run pytest -q tests/m1-perception --junitxml=/tmp/m1-pointer-switch-junit.xml
```

Expected: zero failures and zero errors; only previously documented skips are
allowed.

- [ ] **Step 6: Record session and verification evidence**

Append one `.planning/JOURNAL.md` session containing the release-235 incident,
root cause, test counts, exact commits, protected rollout settings, and next
command. Preserve the existing journal chronology and `[NEXT]` block.

- [ ] **Step 7: Commit Task 3 and expose the deployable SHA**

```bash
git add tests/m1-perception/test_scheduler.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py \
  docs/learning/18-m1-production-operations-handbook.md .planning/JOURNAL.md
git commit -m "docs(m1): record pointer switch recovery gate"
git rev-parse HEAD
git status --short
```

Expected: HEAD is the exact candidate SHA; status contains only the five
pre-existing user-owned `.superpowers/sdd/*.md` modifications.

- [ ] **Step 8: Production rollout after exact-SHA approval**

After the user sends `DEPLOY_SHA_APPROVE <exact HEAD>`, run the protected Fly
test, `make deploy`, verify both machines share the new image and release ID,
then observe the natural generation-868 pointer switch, certified scheduler
success, failure counter reset, cleanup fairness, and post-load localhost/public
health latency. Do not enable Quote or generation reads during this rollout.
