# M1 Deadline Reconciler and Recovery Executor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Detect silent/stalled attempts and execute typed, fenced, budgeted job recovery without unsafe deployment or topology authority.

**Architecture:** Migration 023 stores controller leases and recovery actions. A pure reconciler creates decisions; `PostgresRecoveryStore` schedules them under a controller epoch; a separate executor applies job-level actions. Process/Machine adapters remain disabled until Plan 06.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL, asyncio, pytest, uv, Make.

## Global Constraints

- Execute after Plans 01-02.
- Multiple reconcilers may observe; only the current database-fenced controller epoch may schedule actions.
- Every stale decision returns `stale-noop`; it is never retried as if current.
- Recovery cannot deploy, migrate, change configuration/credentials/capacity, or touch R2/publication evidence.
- Mutation commands require `enable=1`; status remains read-only.
- Use TDD and atomic commits. End with `05.6-203-SUMMARY.md` and clean `make planning-status`.

---

### Task 1: Recovery types and pure reconciler

**Files:**
- Create: `src/polyarb/control_plane/recovery_models.py`
- Create: `src/polyarb/control_plane/reconciler.py`
- Create: `tests/m1-perception/test_control_plane_reconciler.py`

**Interfaces:**
- Produces: `RecoveryActionType`, `RecoveryDecision`, `RuntimeReconciler.evaluate()`.

- [ ] **Step 1: Write the decision-table tests**

```python
def test_alive_without_progress_requests_cancel_and_retry() -> None:
    decision = reconciler.evaluate(state(heartbeat_age=5, progress_age=91), now=NOW)
    assert decision.action is RecoveryActionType.CANCEL_JOB
    assert decision.reason_code == "job.progress-stalled"

def test_missing_heartbeat_waits_for_fence_before_reclaim() -> None:
    assert reconciler.evaluate(state(heartbeat_age=91, lease_expired=False), now=NOW).action is None
    assert reconciler.evaluate(state(heartbeat_age=121, lease_expired=True), now=NOW).action is RecoveryActionType.RECLAIM_JOB
```

Cover healthy progress, lease-at-risk heartbeat, progress stall, missing
heartbeat before/after expiry, attempt deadline, open-circuit probe, recovery
budget exhaustion, and integrity/auth/schema human-only escalation.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_reconciler.py -q`

Expected: FAIL because the modules do not exist.

- [ ] **Step 3: Implement the pure decision function**

```python
class RecoveryActionType(StrEnum):
    HEARTBEAT_JOB = "heartbeat-job"
    CANCEL_JOB = "cancel-job"
    RETRY_JOB = "retry-job"
    RECLAIM_JOB = "reclaim-job"
    PROBE_CIRCUIT = "probe-circuit"
    RESTART_WORKER_PROCESS = "restart-worker-process"
    RESTART_MACHINE = "restart-machine"

@dataclass(frozen=True, slots=True)
class RecoveryDecision:
    action: RecoveryActionType | None
    reason_code: str
    incident_severity: Literal["warning", "critical"]
    qualification_breaking: bool
    next_check_at: datetime
```

`evaluate()` is deterministic and performs no I/O. Human-only failures return
no action, critical severity, and a breaking decision.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_reconciler.py -q`

Expected: PASS.

```bash
git add src/polyarb/control_plane/recovery_models.py src/polyarb/control_plane/reconciler.py tests/m1-perception/test_control_plane_reconciler.py
git commit -m "feat(05.6-203): classify bounded runtime recovery"
```

### Task 2: Migration 023, controller fencing, and action ledger

**Files:**
- Create: `alembic/versions/023_m1_runtime_recovery.py`
- Create: `tests/alembic/test_023.py`
- Create: `src/polyarb/control_plane/recovery_store.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces: `claim_controller()`, `schedule_action()`, `claim_action()`, `finish_action()`.

- [ ] **Step 1: Write failing schema and concurrency tests**

Prove revision `023` revises `022`, creates
`m1_runtime_controller_leases` and `m1_recovery_actions`, permits only one
active conflicting action per target, and rejects stale controller/attempt/
lease preconditions. Two simultaneous controller claims must yield increasing
epochs and only the latest owner may schedule.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/alembic/test_023.py tests/m1-perception/test_control_plane_postgres.py -k 'controller or recovery_action' -q`

Expected: FAIL because revision 023 and store do not exist.

- [ ] **Step 3: Implement fenced store methods**

`schedule_action()` locks controller and runtime rows, compares exact epochs,
checks cooldown/budget, inserts one pending action with a canonical
idempotency key, and appends `job.recovery-started` plus incident/outbox facts
in the same transaction. On any mismatch it inserts a completed
`stale-noop` action without changing the job.

The unique active-action index is partial:

```sql
CREATE UNIQUE INDEX uq_m1_recovery_action_active_target
ON m1_recovery_actions(target_type, target_id)
WHERE state IN ('pending', 'running');
```

- [ ] **Step 4: Verify and commit**

Run the tests from Step 2; expected PASS.

```bash
git add alembic/versions/023_m1_runtime_recovery.py tests/alembic/test_023.py src/polyarb/control_plane/recovery_store.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(05.6-203): fence runtime recovery actions"
```

### Task 3: Job-level recovery executor

**Files:**
- Create: `src/polyarb/control_plane/recovery_executor.py`
- Create: `tests/m1-perception/test_control_plane_recovery_executor.py`
- Modify: `src/polyarb/control_plane/postgres.py`

**Interfaces:**
- Produces: `RecoveryExecutor.run_once()` and exact job action postconditions.

- [ ] **Step 1: Write failing action tests**

Test heartbeat, cooperative cancel-to-retry, expired-lease reclaim, one circuit
probe, duplicate command, stale attempt, exhausted budget, and executor crash
between claim and finish. Assert no action can publish a receipt or pointer.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_recovery_executor.py -q`

Expected: FAIL because the executor does not exist.

- [ ] **Step 3: Implement typed dispatch only**

```python
_JOB_ACTIONS = {
    RecoveryActionType.HEARTBEAT_JOB: "heartbeat_recovering_job",
    RecoveryActionType.CANCEL_JOB: "cancel_stalled_job",
    RecoveryActionType.RETRY_JOB: "release_retryable_job",
    RecoveryActionType.RECLAIM_JOB: "reclaim_expired_job",
    RecoveryActionType.PROBE_CIRCUIT: "release_one_circuit_probe",
}

def run_once(self, *, now: datetime) -> RecoveryActionResult | None:
    action = self._store.claim_action(worker_id=self._worker_id, now=now)
    if action is None:
        return None
    method_name = _JOB_ACTIONS.get(action.action_type)
    if method_name is None:
        return self._store.finish_action(action, result_code="disabled-action", now=now)
    result = getattr(self._control_plane, method_name)(action, now=now)
    return self._store.finish_action(action, result_code=result, now=now)
```

Process and Machine actions return `disabled-action` in this plan.

- [ ] **Step 4: Verify and commit**

Run the executor tests and relevant Postgres tests; expected PASS.

```bash
git add src/polyarb/control_plane/recovery_executor.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_recovery_executor.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(05.6-203): execute fenced job recovery"
```

### Task 4: Reconcile CLI, service loop, Makefile, and closure

**Files:**
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-203-SUMMARY.md`

- [ ] **Step 1: Write CLI and Make contract tests**

Prove `runtime-controller-status` is read-only;
`runtime-reconcile-once` rejects missing `--enable`; and duplicate runs under
the same state produce one action plus one `stale-noop`, never two mutations.

- [ ] **Step 2: Add commands and targets**

```make
## runtime-controller-status: Read the current reconciler lease, active runtime incidents, and recovery budgets.
runtime-controller-status:
	@uv run python -m polyarb.cli_control_plane runtime-controller-status --json

## runtime-reconcile-once: Evaluate and execute at most one fenced job recovery action; requires enable=1.
runtime-reconcile-once:
	@test "$(enable)" = "1" || (echo "usage: make runtime-reconcile-once enable=1" >&2; exit 2)
	@uv run python -m polyarb.cli_control_plane runtime-reconcile-once --enable --json
```

Add `runtime-reconcile-serve --enable --interval-seconds 30` for Plan 06
deployment. It runs decision and executor turns sequentially under the current
controller epoch and exits on an unhandled fencing/store failure.

- [ ] **Step 3: Run complete plan gates**

Run: `uv run pytest tests/alembic/test_023.py tests/m1-perception/test_control_plane_reconciler.py tests/m1-perception/test_control_plane_recovery_executor.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -q`

Run: `uv run ruff check src/polyarb/control_plane src/polyarb/cli_control_plane.py tests/m1-perception`

Expected: PASS.

- [ ] **Step 4: Write SUMMARY and planning gate**

Record all action classes, disabled process/Machine boundary, concurrency
evidence, and commits. Run `make planning-status`; expected no drift. Commit
the SUMMARY.
