# Structure Range Turn Budget Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drain bounded Structure ranges faster without increasing source, materializer, certification, or Quote call volume.

**Architecture:** Preserve the existing ordered base worker selection governed by `max_turns`, then append an explicit sequential number of turns for the existing Structure range worker. Expose the budget as a default-zero CLI option. Each turn retains its timeout wrapper and claims an independent Postgres lease.

**Tech Stack:** Python 3.12, asyncio, pytest, Ruff, Fly staging Worker.

## Global Constraints

- `structure_range_turns=0` preserves existing scheduler output and ordering.
- Extra turns call only `structure-range`; all turns remain serial and timeout-bounded.
- Postgres leases remain the only cross-process ownership authority.
- Deploy only `polyarb-control-worker-staging`; do not touch Telegram or production L1/L2.

---

### Task 1: Add the scheduler range-budget contract

**Files:**

- Modify: `src/polyarb/control_plane/scheduler.py:18-83`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py:1-146`

**Interfaces:**

- Produces: `TransactionalControlPlaneScheduler(..., structure_range_turns: int = 0)`.
- Produces: a tick result with trailing additional `structure-range` turns.

- [ ] **Step 1: Write failing scheduler contracts**

```python
def test_zero_range_budget_preserves_existing_bounded_tick() -> None:
    scheduler = _scheduler(max_turns=2, structure_range_turns=0)
    assert [turn["worker"] for turn in asyncio.run(scheduler.run_tick())["turns"]] == [
        "structure-source-admit", "structure-source"
    ]


def test_range_budget_appends_only_serial_structure_range_turns() -> None:
    scheduler, structure = _scheduler_with_structure(max_turns=8, structure_range_turns=3)
    turns = asyncio.run(scheduler.run_tick())["turns"]
    assert [turn["worker"] for turn in turns][-4:] == [
        "structure-range", "structure-range", "structure-range", "structure-range"
    ]
    assert structure.calls == 4
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py -q`

Expected: FAIL with `unexpected keyword argument 'structure_range_turns'`.

- [ ] **Step 3: Add the minimal serial budget**

```python
def __init__(..., max_turns: int, structure_range_turns: int = 0, ...) -> None:
    if max_turns <= 0 or structure_range_turns < 0 or turn_timeout_seconds <= 0:
        raise ValueError("scheduler bounds must be positive")
    self._structure_range_worker = ("structure-range", structure_worker)
    self._structure_range_turns = structure_range_turns

# In run_tick(), after building existing base workers:
workers = (*base_workers, *((self._structure_range_worker,) * self._structure_range_turns))
```

Keep the existing turn loop and timeout handling unchanged.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py -q && uv run ruff check src/polyarb/control_plane/scheduler.py tests/m1-perception/test_transactional_control_plane_scheduler.py`

Expected: all pass, no Ruff findings.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/scheduler.py tests/m1-perception/test_transactional_control_plane_scheduler.py
git commit -m "feat(05.6): budget transactional Structure range turns"
```

### Task 2: Expose the default-safe CLI budget and stage it

**Files:**

- Modify: `src/polyarb/cli_control_plane.py:115-139,304-342,621-642`
- Modify: `tests/m1-perception/test_control_plane_cli.py:201-238,418-441`

**Interfaces:**

- Consumes: `--structure-range-turns` as a non-negative integer.
- Produces: `_transactional_scheduler(..., max_turns: int, structure_range_turns: int)`.

- [ ] **Step 1: Write the failing CLI forwarding contract**

```python
def test_control_plane_serve_forwards_explicit_range_turn_budget(monkeypatch, capsys) -> None:
    captured: dict[str, int] = {}
    monkeypatch.setattr(
        cli_control_plane, "_transactional_scheduler",
        lambda _control_plane, *, worker_id, max_turns, structure_range_turns: (
            captured.update(max_turns=max_turns, structure_range_turns=structure_range_turns)
            or Scheduler()
        ),
    )
    assert cli_control_plane.main([
        "serve", "--enable", "--max-turns", "8", "--structure-range-turns", "8", "--json"
    ]) == 0
    assert captured == {"max_turns": 8, "structure_range_turns": 8}
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py -q`

Expected: FAIL with `unrecognized arguments: --structure-range-turns`.

- [ ] **Step 3: Add the CLI option and validation**

```python
for command in (tick_once, serve):
    command.add_argument("--structure-range-turns", type=int, default=0)

def _transactional_scheduler(..., max_turns: int, structure_range_turns: int):
    return TransactionalControlPlaneScheduler(
        ..., max_turns=max_turns, structure_range_turns=structure_range_turns
    )

if args.max_turns <= 0 or args.structure_range_turns < 0:
    print("--max-turns must be positive and --structure-range-turns non-negative", file=sys.stderr)
    return 2
```

- [ ] **Step 4: Verify focused regression**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_transactional_structure_worker.py -q && uv run ruff check src/polyarb/control_plane/scheduler.py src/polyarb/cli_control_plane.py tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py`

Expected: all pass, no Ruff findings.

- [ ] **Step 5: Commit with summary**

```bash
git add src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_cli.py
git add .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-163-SUMMARY.md
git commit -m "feat(05.6): expose Structure range turn budget"
```

- [ ] **Step 6: Stage and verify bounded drain**

Deploy `m1-range-budget-<commit>` only to staging machine `48e3104c979578` with:

```text
python -m polyarb.cli_control_plane serve --enable --worker-id fly-control-plane --max-turns 8 --structure-range-turns 8 --interval-seconds 2 --json
```

Compare range receipt counts across 60 seconds. Accept only if growth materially exceeds the prior one-per-tick rate, no new `IncompleteStructureGenerationError` incident occurs, RSS remains below 2048MB, and `m1_publication_pointers` remains zero.
