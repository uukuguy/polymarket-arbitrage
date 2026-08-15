# Transactional Retry Fault and Scoped Alert Delivery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Safely produce real staging circuit/recovery evidence for exact Structure and Quote jobs without delivering historical alert backlog.

**Architecture:** An exact-key finite retry callback raises an ordinary injected exception inside existing Structure/Quote worker `try` blocks, preserving the production fenced retry/circuit transaction. Recovery can optionally stamp new outbox payloads with an acceptance-run ID; an alert worker with the same selector claims only those rows through Postgres, leaving historical rows untouched.

**Tech Stack:** Python 3.12, psycopg 3, existing transactional workers, pytest, Fly staging.

## Global Constraints

- No production pointer, L1/L2, SQLite authority, or historical outbox mutation.
- Retry faults require an exact job key, positive finite attempt count, and literal staging acknowledgement.
- Process-loss R2 hook remains separate and unchanged.
- Scope filtering must happen in the SQL claim predicate, not after a broad claim.
- Every behavior change starts RED and ends with a focused green test command.

---

### Task 1: Exact bounded retry-fault callback and worker seam

**Files:**
- Modify: `src/polyarb/cli_control_plane.py:54-213, 337-380, 650-715`
- Modify: `src/polyarb/control_plane/structure_worker.py:55-131`
- Modify: `src/polyarb/control_plane/quote_worker.py:45-129`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`

**Interfaces:**
- Produces `RetryFaultInjectedError(RuntimeError)` and
  `_retry_fault_callback(target_job_key, attempts, acknowledgement)`.
- Produces optional `retry_fault_before_receipt: Callable[[JobLease], None]`
  constructor parameter on Structure and Quote workers.
- Consumes the existing `finish_retryable_with_incident` path; no new repository
  circuit method is introduced.

- [ ] **Step 1: Write failing callback/CLI tests**

```python
def test_retry_fault_requires_exact_acknowledgement_and_positive_attempts() -> None:
    from polyarb.cli_control_plane import _retry_fault_callback
    with pytest.raises(ValueError, match="exact staging acknowledgement"):
        _retry_fault_callback(target_job_key="structure:a", attempts=3, acknowledgement=None)
    with pytest.raises(ValueError, match="attempts"):
        _retry_fault_callback(target_job_key="structure:a", attempts=0,
                              acknowledgement="staging-retry-before-receipt")

def test_serve_passes_retry_fault_callback_to_scheduler(monkeypatch, capsys) -> None:
    # invoke `serve --fault-retry-job-key structure:a --fault-retry-attempts 3`
    # and assert the scheduler receives a non-null retry callback.
    ...
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py -k retry_fault -q`

Expected: FAIL because the callback and CLI arguments do not exist.

- [ ] **Step 3: Write failing worker tests**

```python
def test_structure_retry_fault_uses_existing_retry_incident_path() -> None:
    worker = TransactionalStructureWorker(...,
        retry_fault_before_receipt=lambda _lease: (_ for _ in ()).throw(
            RetryFaultInjectedError("staging retry fault")))
    with pytest.raises(RetryFaultInjectedError):
        asyncio.run(worker.run_once())
    assert control_plane.recorded is None
    assert control_plane.retry_incidents[0]["component"] == "structure-normalize"
```

Create the equivalent Quote test and add `retry_incidents` to Structure's fake
control plane.

- [ ] **Step 4: Verify RED**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_worker.py tests/m1-perception/test_transactional_quote_worker.py -k retry_fault -q`

Expected: FAIL because neither worker accepts/calls the retry callback.

- [ ] **Step 5: Implement minimal callback plumbing**

Add parser arguments to both `tick-once` and `serve`. The callback retains a
per-process counter keyed by the exact target and raises only while its count is
less than `attempts`; it rejects target/attempt/ack combinations before worker
construction. Pass it through `_transactional_scheduler` into Structure and
Quote constructors. Invoke it after artifact creation and before receipt, inside
each existing `try`, so `except Exception` records the ordinary retry incident.

- [ ] **Step 6: Verify GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_transactional_structure_worker.py tests/m1-perception/test_transactional_quote_worker.py -q`

Run: `uv run ruff check src/polyarb/cli_control_plane.py src/polyarb/control_plane/structure_worker.py src/polyarb/control_plane/quote_worker.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_transactional_structure_worker.py tests/m1-perception/test_transactional_quote_worker.py`

Commit: `feat(05.6): add bounded staging retry fault`

### Task 2: Scoped acceptance-run recovery outbox and alert SQL claim

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py:3001-3055, 3090-3185`
- Modify: `src/polyarb/control_plane/alert_delivery.py:42-92`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_alert_delivery.py`

**Interfaces:**
- Extends `record_job_recovery(..., acceptance_run_id: str | None = None)`;
  when present it adds `acceptance_run_id` to only the recovery event's new
  outbox payload.
- Extends `claim_alert_delivery(..., acceptance_run_id: str | None = None)`;
  when present its SQL requires `payload->>'acceptance_run_id' = %s`.
- Extends `TransactionalAlertDeliveryWorker(..., acceptance_run_id: str | None = None)`.

- [ ] **Step 1: Write failing repository test**

```python
def test_scoped_alert_claim_never_claims_historical_outbox(control_plane) -> None:
    # insert an old pending dashboard row without scope and a new scoped row;
    # claim with acceptance_run_id="run-a" and assert only the new row is leased.
    ...
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k scoped_alert_claim -q`

Expected: FAIL because `claim_alert_delivery` has no scope argument/predicate.

- [ ] **Step 3: Write failing delivery-worker test**

```python
def test_alert_worker_passes_acceptance_scope_to_claim() -> None:
    control_plane = _ControlPlane()
    worker = TransactionalAlertDeliveryWorker(..., acceptance_run_id="run-a")
    asyncio.run(worker.run_once())
    assert control_plane.claim_kwargs["acceptance_run_id"] == "run-a"
```

- [ ] **Step 4: Verify RED**

Run: `uv run pytest tests/m1-perception/test_transactional_alert_delivery.py -k acceptance_scope -q`

Expected: FAIL because the worker does not accept/pass the scope.

- [ ] **Step 5: Implement scoped payload and SQL predicate**

Validate non-empty IDs. Preserve no-scope behavior exactly. Build the SQL as
two explicit query variants so the no-scope predicate never reads JSON fields;
the scoped variant filters before `FOR UPDATE SKIP LOCKED`. Only
`record_job_recovery` stamps scoped new recovery outbox payloads.

- [ ] **Step 6: Verify GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_alert_delivery.py -q`

Run: `uv run ruff check src/polyarb/control_plane/postgres.py src/polyarb/control_plane/alert_delivery.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_alert_delivery.py`

Commit: `feat(05.6): scope acceptance alert delivery`

### Task 3: CLI alert scope, scheduler recovery scope, and operator evidence

**Files:**
- Modify: `src/polyarb/cli_control_plane.py:151-160, 337-380, 721-733`
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `Makefile:615-667`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `docs/learning/74-受控重试熔断与范围告警.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-168-SUMMARY.md`

**Interfaces:**
- `serve`/`tick-once` take optional `--acceptance-run-id`, forwarding it to
  recovery writers.
- `alert-serve` takes optional `--acceptance-run-id`, forwarding it only to
  the alert worker claim selector.
- Makefile exposes `control-plane-alert-serve` with explicit `enable=1` and
  optional `acceptance_run_id=`.

- [ ] **Step 1: Write failing CLI and Makefile contract tests**

```python
def test_alert_serve_scopes_only_the_named_run(monkeypatch, capsys) -> None:
    # fake alert worker captures `acceptance_run_id="run-a"` from CLI.
    ...

def test_make_control_plane_alert_serve_has_explicit_enable_gate() -> None:
    assert "control-plane-alert-serve" in MAKEFILE.read_text()
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -k 'alert_serve or control_plane_alert_serve' -q`

Expected: FAIL because scoped operator entrypoints are absent.

- [ ] **Step 3: Implement minimal forwarding and command target**

Keep `alert-serve` separate from scheduler process groups. Require `--enable`
for its Make target and source DSN only from normal environment resolution.
Document that omitting the acceptance ID intentionally reverts to all-outbox
behavior and must not be used on this staging database.

- [ ] **Step 4: Verify GREEN, documentation, and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -q`

Run: `make planning-status`

Run: `uv run ruff check src/polyarb/cli_control_plane.py src/polyarb/control_plane/scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py`

Commit: `docs(05.6): expose scoped fault-soak operations`

## Plan self-review

- Coverage: Task 1 implements exact bounded real retry behavior; Task 2 ensures
  historical rows cannot be claimed; Task 3 makes both controls operable and
  teaches the acceptance boundary.
- Placeholder scan: no TODO/TBD steps; every behavior-changing task includes a
  RED command, a GREEN command, and concrete interfaces.
- Type consistency: callback type is `Callable[[JobLease], None]`; scopes are
  optional non-empty strings across repository, worker, scheduler, and CLI.
