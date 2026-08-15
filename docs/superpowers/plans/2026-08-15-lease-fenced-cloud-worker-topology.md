# Lease-fenced cloud worker topology Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make transactional Structure and Quote collection drain through bounded role-specific cloud workers while retaining Postgres as the only ownership and receipt authority.

**Architecture:** Split the all-purpose scheduler into a coordinator and independently runnable Structure-range/Quote-batch roles. Roles keep the same fenced `claim_job` and receipt paths. Add atomic source-admission high-water checks and a bounded Postgres-only per-kind queue projection.

**Tech Stack:** Python 3.12, asyncio, psycopg 3/Postgres, Starlette, Fly Machines, pytest, Ruff.

## Global Constraints

- Use `uv`; do not use `pip install`.
- Lease epoch and idempotency receipts remain the only cross-process ownership authority.
- No role reads legacy SQLite, mutates a public pointer, or contains wallet/order code.
- Worker loops have positive bounded turn and interval values; no unbounded fan-out.
- Operator projection stays read-only, Postgres-only, statement-timeout bounded, and fail-closed.
- All new runnable commands have Makefile targets.
- Staging changes do not alter production L1/L2 or bulk-send historical alerts.

---

### Task 1: Add a role-local bounded worker loop

**Files:**
- Create: `src/polyarb/control_plane/worker_loop.py`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py`

**Interfaces:** Produces `TransactionalWorkerLoop(worker_name: str, worker: _Worker, turns_per_tick: int, turn_timeout_seconds: float = 105)` with `run_tick`, `run_until_stopped`, and `aclose`.

- [ ] **Step 1: Write the failing tests**

```python
from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

def test_role_loop_runs_only_its_named_worker_for_its_bound() -> None:
    worker = _AsyncWorker("quote-batch")
    loop = TransactionalWorkerLoop(worker_name="quote-batch", worker=worker, turns_per_tick=2)
    assert asyncio.run(loop.run_tick())["turns"] == [
        {"worker": "quote-batch", "job_key": "quote-batch", "outcome": "succeeded"},
        {"worker": "quote-batch", "job_key": "quote-batch", "outcome": "succeeded"},
    ]
    assert worker.calls == 2

def test_role_loop_timeout_records_one_turn_and_continues() -> None:
    loop = TransactionalWorkerLoop(worker_name="structure-range", worker=_HangingWorker(), turns_per_tick=2, turn_timeout_seconds=0.001)
    assert [turn["outcome"] for turn in asyncio.run(loop.run_tick())["turns"]] == ["timed-out", "timed-out"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py -k role_loop -q`

Expected: FAIL with missing `polyarb.control_plane.worker_loop`.

- [ ] **Step 3: Write minimal implementation**

```python
class TransactionalWorkerLoop:
    def __init__(self, *, worker_name: str, worker: _Worker, turns_per_tick: int, turn_timeout_seconds: float = 105) -> None:
        if not worker_name or turns_per_tick <= 0 or turn_timeout_seconds <= 0:
            raise ValueError("worker loop bounds are invalid")
        self._worker_name, self._worker = worker_name, worker
        self._turns_per_tick, self._turn_timeout_seconds = turns_per_tick, turn_timeout_seconds
        self._running = asyncio.Lock()
```

Implement `run_tick` with the same await/timeout result shape as `TransactionalControlPlaneScheduler.run_tick`; retain scheduler callback/close semantics in `run_until_stopped` and `aclose`.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py -q`

Expected: PASS.

`git add src/polyarb/control_plane/worker_loop.py tests/m1-perception/test_transactional_control_plane_scheduler.py && git commit -m "feat(m1): add bounded role-local worker loop"`

### Task 2: Expose coordinator, Structure-range, and Quote-batch service roles

**Files:**
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_transactional_control_plane_scheduler.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `Makefile`
- Modify: `deploy/control-plane/fly-control-worker.toml.template`
- Modify: `tests/m1-perception/test_control_plane_deployment_templates.py`

**Interfaces:** Produces `serve --worker-role {all,coordinator,structure-range,quote-batch}` with default `all` compatibility. Pool roles wrap only their named worker in `TransactionalWorkerLoop`; coordinator excludes both pool worker types.

- [ ] **Step 1: Write failing tests**

```python
def test_control_plane_serve_builds_quote_batch_only_loop(monkeypatch, capsys) -> None:
    # Patch _control_plane_from_env, _transactional_quote_workers and the service runner.
    # Assert worker ID is "test:quote-batch", worker name is "quote-batch", and pool turns is 3.
    assert cli_control_plane.main([
        "serve", "--enable", "--worker-role", "quote-batch",
        "--worker-id", "test", "--pool-turns", "3", "--json",
    ]) == 0

def test_worker_template_declares_role_processes() -> None:
    payload = tomllib.loads((ROOT / "deploy/control-plane/fly-control-worker.toml.template").read_text())
    assert set(payload["processes"]) == {"coordinator", "structure_range", "quote_batch"}
    assert "--worker-role coordinator" in payload["processes"]["coordinator"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_deployment_templates.py -k 'quote_batch_only or role_processes' -q`

Expected: FAIL because the role argument and process groups do not exist.

- [ ] **Step 3: Write minimal implementation**

```python
serve.add_argument("--worker-role", choices=("all", "coordinator", "structure-range", "quote-batch"), default="all")
serve.add_argument("--pool-turns", type=int, default=1)
```

Extend `TransactionalControlPlaneScheduler` with `include_structure_range: bool = True` and `include_quote_batch: bool = True`. Build its worker tuple from enabled entries and reject an empty tuple. Add a coordinator builder using both values false. Pool branches build only current `TransactionalStructureWorker` or `TransactionalQuoteBatchWorker`, use IDs `f"{worker_id}:structure-range"` / `f"{worker_id}:quote-batch"`, and run `TransactionalWorkerLoop`.

Replace the one template process with `coordinator`, `structure_range`, and `quote_batch`, all carrying explicit role arguments. Add `control-plane-coordinator-serve`, `control-plane-range-serve`, and `control-plane-quote-serve` Make targets with the existing `.env`/DSN guard and only their matching role.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_deployment_templates.py -q && uv run ruff check src/polyarb/cli_control_plane.py src/polyarb/control_plane/scheduler.py src/polyarb/control_plane/worker_loop.py`

Expected: PASS.

`git add src/polyarb/control_plane/worker_loop.py src/polyarb/control_plane/scheduler.py src/polyarb/cli_control_plane.py Makefile deploy/control-plane/fly-control-worker.toml.template tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_deployment_templates.py && git commit -m "feat(m1): split transactional worker service roles"`

### Task 3: Make source admission atomically backpressure on pool backlog

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_structure_schedule.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:** Produces `SourceAdmissionDecision(state: Literal["admitted", "busy", "backpressured:structure", "backpressured:quote"], job_key: str | None)`. `admit_due_structure_source_window(..., structure_high_water: int, quote_high_water: int)` returns it inside its existing advisory-lock transaction.

- [ ] **Step 1: Write failing tests**

```python
def test_due_source_admission_is_backpressured_before_insert_when_structure_queue_is_full(control_plane, now):
    decision = control_plane.admit_due_structure_source_window(
        cadence_seconds=300, now=now, structure_high_water=2, quote_high_water=10,
    )
    assert decision.state == "backpressured:structure"
    assert decision.job_key is None

def test_source_admitter_returns_quote_backpressure_without_claiming_gamma_work() -> None:
    worker = TransactionalStructureSourceAdmitter(
        control_plane=_QuoteBackpressureControlPlane(), cadence_seconds=300,
        structure_high_water=10, quote_high_water=2, now=lambda: NOW,
    )
    assert asyncio.run(worker.run_once()).outcome == "backpressured:quote"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_structure_schedule.py -k backpressured -q`

Expected: FAIL because admission lacks high-water arguments and decisions.

- [ ] **Step 3: Write minimal implementation**

Within the existing source-admission advisory lock, count unfinished `structure-normalize` and `quote-batch` jobs using `runnable`, `retryable`, `leased`, `checkpointed`. Validate positive limits. Return `backpressured:structure` or `backpressured:quote` before the current window query/insert if count is at least its limit. Preserve `busy` for an active window and `admitted` only after the existing enqueue.

Add `--structure-high-water` default `2000` and `--quote-high-water` default `512` to `serve`; only coordinator/all forwards them to the source admitter. Pool roles reject non-default values.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_structure_schedule.py tests/m1-perception/test_control_plane_cli.py -q && uv run ruff check src/polyarb/control_plane/postgres.py src/polyarb/control_plane/structure_source.py src/polyarb/cli_control_plane.py`

Expected: PASS.

`git add src/polyarb/control_plane/postgres.py src/polyarb/control_plane/structure_source.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_structure_schedule.py tests/m1-perception/test_control_plane_cli.py && git commit -m "feat(m1): backpressure transactional source admission"`

### Task 4: Project bounded per-kind lag and next runnable identity

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_control_plane_api.py`
- Modify: `docs/M1-市场感知平台使用手册.md`

**Interfaces:** Produces `queue_health` in `operational_snapshot` with keys `structure-range` and `quote-batch`. Each value contains `unfinished_count`, `oldest_age_seconds`, and `next_job_key`; no payload, lease owner, R2 key, or error detail is returned.

- [ ] **Step 1: Write failing tests**

```python
def test_operational_snapshot_projects_next_claimable_per_pool(control_plane, now):
    snapshot = control_plane.operational_snapshot(now=now)
    assert snapshot["queue_health"]["structure-range"]["next_job_key"] == "structure:one:normalize:events:0"
    assert snapshot["queue_health"]["quote-batch"]["next_job_key"] == "quote:one:batch:0"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_api.py -k 'queue_health or next_claimable' -q`

Expected: FAIL with missing `queue_health`.

- [ ] **Step 3: Write minimal implementation**

For each job type issue a read-only `SELECT job_key ... LIMIT 1` with the same claimability predicate and order as `claim_job`, but without lock/update. Count unfinished states and compute oldest age from `created_at`. Add the two compact records under `queue_health`. Document that `next_job_key` is a hint: operators/workers must still call fenced `claim_job`, so another replica may acquire it first.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_api.py -q && uv run ruff check src/polyarb/control_plane/postgres.py`

Expected: PASS.

`git add src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_api.py docs/M1-市场感知平台使用手册.md && git commit -m "feat(m1): expose transactional queue health"`

### Task 5: Staging topology proof and learning artifacts

**Files:**
- Create: `docs/learning/75-事务型云端采集工作池.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-169-SUMMARY.md`

- [ ] **Step 1: Verify local gates before deployment**

Run: `make planning-status && uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_deployment_templates.py -q`

Expected: `no drift detected` and all selected tests pass.

- [ ] **Step 2: Deploy and prove staging only**

Update only `polyarb-control-worker-staging`, then run one coordinator, one Structure-range worker and one Quote-batch worker. Record start/mid/end API samples. Acceptance requires 200 `/healthz`; distinct attempts for same-role worker IDs; falling `queue_health` depth and age over fifteen minutes; no pointer mutation; and coordinator `backpressured:*` above either high-water mark. Restore the known normal coordinator command if any worker exits or queue age rises across two consecutive samples.

- [ ] **Step 3: Write evidence and commit**

The learning document explains lease fencing versus concurrency, why an API identity is never a claim, and why high-water admission preserves freshness. The phase summary records exact image, machine IDs, samples, pointer evidence, and explicitly leaves Quote fault/24-hour soak open.

`git add docs/learning/75-事务型云端采集工作池.md docs/learning/00-INDEX.md .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-169-SUMMARY.md && git commit -m "docs(m1): record bounded worker pool staging proof"`

## Plan self-review

- Tasks 1–2 cover role isolation, Task 3 atomic admission backpressure, Task 4 read-only queue visibility, and Task 5 staging evidence.
- Every implementation step names its files, interface, command, and acceptance result.
- All pool behavior consumes Task 1 and retains existing `claim_job` lease fencing.
