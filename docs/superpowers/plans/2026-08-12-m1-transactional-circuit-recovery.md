# M1 Transactional Circuit and Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bound repeated M1 transactional-worker failures with a durable scoped circuit and resolve the exact incident only after a fenced successful probe.

**Architecture:** Add an additive Postgres circuit row keyed by `job_key`. The existing fenced retry transaction derives deterministic exponential delay and emits threshold-aware incident events. Every terminal successful worker effect clears only its own circuit and appends recovery evidence; the read API projects bounded circuit facts.

**Tech Stack:** Python 3.12, psycopg 3, Alembic, pytest/testcontainers, Starlette.

## Global Constraints

- Scope every circuit by one immutable `job_key`; never globally pause M1.
- Use 15/30/60-second doubling with a five-minute maximum, without random jitter.
- A stale lease cannot open or recover a circuit.
- Recovery resolves only the matching job-key incident and emits durable outbox intent.
- No SQLite authority, pointer mutation, external request, or new dependency.

---

### Task 1: Add additive circuit schema and repository transition

**Files:**
- Create: `alembic/versions/014_m1_transactional_circuits.py`
- Create: `tests/alembic/test_014.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces `m1_job_circuits(job_key, consecutive_failures, state, opened_at, next_probe_at, updated_at)`.
- Extends `finish_retryable_with_incident(...)` to derive the next delay and return it.
- Produces `record_job_recovery(lease, component, channels, now)`.

- [ ] **Step 1: Write failing migration/repository tests**

```python
def test_retry_circuit_opens_on_third_failure_and_caps_probe_delay(control_plane):
    # three fenced retries of one job return 15, 30, 60 seconds;
    # repeated probes eventually return 300 seconds and one open circuit row.
    ...

def test_fenced_success_resolves_matching_open_incident(control_plane):
    # a current lease recovery writes recovered event and resolved_at;
    # a stale lease raises StaleLeaseError and writes nothing.
    ...
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/alembic/test_014.py tests/m1-perception/test_control_plane_postgres.py -q`

Expected: fail because revision 014 and circuit methods do not exist.

- [ ] **Step 3: Implement revision and fenced transitions**

```python
delay_seconds = min(15 * 2 ** max(0, failures - 1), 300)
state = "open" if failures >= 3 else "closed"
```

Use `SELECT ... FOR UPDATE` on the job-key circuit row inside the same
transaction as the job retry mutation. Use the lease epoch in every event
idempotency key. Recovery updates `m1_incidents.state='resolved'`, sets
`resolved_at`, clears the circuit, and appends `recovered` only under the
current lease fence.

- [ ] **Step 4: Run GREEN and commit**

Run: `uv run pytest tests/alembic/test_014.py tests/m1-perception/test_control_plane_postgres.py -q`

Run: `uv run ruff check alembic/versions/014_m1_transactional_circuits.py src/polyarb/control_plane/postgres.py tests/alembic/test_014.py tests/m1-perception/test_control_plane_postgres.py`

Commit: `feat(05.6-125): add fenced transactional retry circuits`

### Task 2: Connect successful worker effects and operator snapshot

**Files:**
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/control_plane/quote_admission.py`
- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_materializer.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`
- Modify: `tests/m1-perception/test_transactional_quote_admission.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Every successful terminal worker path calls `record_job_recovery` after its
  durable receipt/finish transaction succeeds.
- `operational_snapshot()` returns bounded `open_circuits` and their job keys,
  state, failure count and next probe timestamp.

- [ ] **Step 1: Write failing worker and snapshot tests**

```python
def test_source_success_records_recovery_for_its_current_lease():
    # Fake control plane captures one recovery call after source receipt commit.
    ...

def test_snapshot_projects_open_circuit_without_sqlite_authority():
    # Real Postgres snapshot shows one circuit state and next_probe_at.
    ...
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_control_plane_postgres.py -q`

Expected: fail because workers do not call recovery and snapshot has no circuit projection.

- [ ] **Step 3: Implement minimal recovery calls and projection**

Call recovery only after the worker's durable output has committed. Do not call
it from idle, recovered-receipt shortcut, quarantine, incomplete certification,
or exception paths. Keep samples bounded to the existing `sample_limit`.

- [ ] **Step 4: Run GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_transactional_structure_source_materializer.py tests/m1-perception/test_transactional_structure_worker.py tests/m1-perception/test_transactional_quote_admission.py tests/m1-perception/test_transactional_quote_worker.py -q`

Commit: `feat(05.6-126): recover scoped transactional circuits`

### Task 3: Align preflight, rollout, docs and acceptance evidence

**Files:**
- Modify: `Makefile`
- Modify: `src/polyarb/control_plane/rollout.py`
- Modify: `tests/m1-perception/test_control_plane_rollout.py`
- Modify: `docs/learning/00-INDEX.md`
- Create: `docs/learning/69-事务型熔断与恢复.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-125-SUMMARY.md`

- [ ] **Step 1: Write failing rollout contract**

Assert revision 014 migration and an explicit circuit-open/worker-loss/probe/recovery evidence step precede the 24-hour soak.

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_rollout.py -q`

Expected: failure because rollout still names revision 013.

- [ ] **Step 3: Implement revision-014 preflight/rollout and learning evidence**

Require circuit evidence to record opened job key, bounded retry schedule,
worker replacement, recovery event, resolved incident, delivery receipts and
control-API readability.

- [ ] **Step 4: Verify final local gate and commit**

Run: `make docs-m1-check`

Run: `make planning-status`

Run the complete focused revision-014 control-plane suite plus Ruff and
targeted `git diff --check`.

Commit: `docs(05.6-127): align circuit recovery rollout evidence`
