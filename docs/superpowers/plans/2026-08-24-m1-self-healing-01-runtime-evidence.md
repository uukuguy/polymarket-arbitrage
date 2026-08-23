# M1 Runtime Evidence Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add typed runtime lifecycle/deadline evidence and replay the failed formal run so qualification-breaking facts are detected immediately.

**Architecture:** Migration 022 adds current runtime state plus append-only events. Focused domain and policy modules remain pure; `PostgresControlPlane` owns the SQL transaction boundary. The first operator surface is read-only historical replay.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL/Supabase, Alembic, pytest, uv, Make.

## Global Constraints

- Follow `docs/superpowers/specs/2026-08-24-m1-event-driven-self-healing-qualification-design.md`.
- Preserve `m1-formal-20260823T1335Z` unchanged and use it as a regression fixture.
- Python dependencies remain locked by `uv.lock`; do not use `pip install`.
- Every command is exposed through a documented Makefile target and `make help`.
- Runtime facts contain bounded non-secret detail; never persist DSNs, tokens, headers, or response bodies.
- Use TDD and atomic commits. End this plan with `05.6-201-SUMMARY.md` and `make planning-status` clean.
- Do not mutate production or touch the pre-existing dirty files listed in `.planning/HANDOFF.json`.

---

### Task 1: Runtime domain and deadline policy

**Files:**
- Create: `src/polyarb/control_plane/runtime_models.py`
- Create: `tests/m1-perception/test_control_plane_runtime_models.py`

**Interfaces:**
- Produces: `RuntimeEventKind`, `RuntimeDeadlineProfile`, `RuntimeProgress`, `RuntimeEvent`.
- Consumed by: Tasks 2-4 and Plans 02-06.

- [ ] **Step 1: Write failing validation tests**

```python
def test_deadline_profile_separates_liveness_progress_and_attempt() -> None:
    profile = RuntimeDeadlineProfile(
        policy_version="runtime-v1", lease_seconds=120, heartbeat_seconds=30,
        progress_seconds=90, attempt_seconds=300,
    )
    assert profile.missed_heartbeat_incident_seconds == 90

def test_runtime_progress_is_monotonic_and_bounded() -> None:
    assert RuntimeProgress(sequence=2, current=10, total=20, stage="upload").current == 10
    with pytest.raises(ValueError, match="current cannot exceed total"):
        RuntimeProgress(sequence=2, current=21, total=20, stage="upload")
```

- [ ] **Step 2: Prove tests fail**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_models.py -q`

Expected: FAIL because `runtime_models` does not exist.

- [ ] **Step 3: Implement the exact public types**

```python
class RuntimeEventKind(StrEnum):
    STARTED = "job.started"
    STAGE_CHANGED = "job.stage-changed"
    LEASE_AT_RISK = "job.lease-at-risk"
    PROGRESS_STALLED = "job.progress-stalled"
    RETRYABLE_FAILED = "job.retryable-failed"
    RETRY_SCHEDULED = "job.retry-scheduled"
    RECOVERY_STARTED = "job.recovery-started"
    RECOVERED = "job.recovered"
    TERMINAL_FAILED = "job.terminal-failed"
    SUCCEEDED = "job.succeeded"

@dataclass(frozen=True, slots=True)
class RuntimeDeadlineProfile:
    policy_version: str
    lease_seconds: int
    heartbeat_seconds: int
    progress_seconds: int
    attempt_seconds: int

    def __post_init__(self) -> None:
        values = (self.lease_seconds, self.heartbeat_seconds, self.progress_seconds, self.attempt_seconds)
        if not self.policy_version or any(value <= 0 for value in values):
            raise ValueError("runtime deadline profile values must be positive")
        if self.heartbeat_seconds * 3 > self.lease_seconds:
            raise ValueError("heartbeat must run at least three times per lease")
        if self.progress_seconds > self.attempt_seconds:
            raise ValueError("progress deadline cannot exceed attempt deadline")

    @property
    def missed_heartbeat_incident_seconds(self) -> int:
        return self.heartbeat_seconds * 3

@dataclass(frozen=True, slots=True)
class RuntimeProgress:
    sequence: int
    current: int
    total: int | None
    stage: str

    def __post_init__(self) -> None:
        if self.sequence < 0 or self.current < 0 or not self.stage:
            raise ValueError("runtime progress values are invalid")
        if self.total is not None and (self.total < 0 or self.current > self.total):
            raise ValueError("current cannot exceed total")
```

Add the frozen event contract:

```python
@dataclass(frozen=True, slots=True)
class RuntimeEvent:
    job_key: str
    attempt_id: str
    lease_epoch: int
    worker_id: str
    event_sequence: int
    kind: RuntimeEventKind
    stage: str
    progress: RuntimeProgress | None
    detail: dict[str, object]
    occurred_at: datetime
    idempotency_key: str

    def __post_init__(self) -> None:
        identities = (self.job_key, self.attempt_id, self.worker_id, self.stage, self.idempotency_key)
        if any(not value for value in identities):
            raise ValueError("runtime event identities must be non-empty")
        if self.lease_epoch < 1 or self.event_sequence < 1:
            raise ValueError("runtime event sequences must be positive")
        if self.occurred_at.tzinfo is None or self.occurred_at.utcoffset() is None:
            raise ValueError("runtime event time must be timezone-aware")
        if len(self.detail) > 20:
            raise ValueError("runtime event detail is not bounded")
```

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_models.py -q`

Expected: PASS.

```bash
git add src/polyarb/control_plane/runtime_models.py tests/m1-perception/test_control_plane_runtime_models.py
git commit -m "feat(05.6-201): define runtime evidence contracts"
```

### Task 2: Migration 022 and transactional persistence

**Files:**
- Create: `alembic/versions/022_m1_job_runtime_evidence.py`
- Create: `tests/alembic/test_022.py`
- Create: `src/polyarb/control_plane/runtime_store.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Consumes: Task 1 runtime types.
- Produces: cursor helpers `append_runtime_event_cursor`, `start_runtime_attempt_cursor`, `update_runtime_heartbeat_cursor`, `update_runtime_progress_cursor`; public `PostgresControlPlane.record_runtime_progress()` and `heartbeat_runtime_attempt()`; bounded runtime snapshot rows.

- [ ] **Step 1: Write migration and real-Postgres failing tests**

Assert migration `022` revises `021`, creates `m1_job_runtime_state` and `m1_job_runtime_events`, rejects event UPDATE/DELETE, and enforces unique `(attempt_id,event_sequence)` plus `idempotency_key`. In the Postgres test, claim a job and assert one `job.started` event and one matching runtime-state row commit together.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/alembic/test_022.py tests/m1-perception/test_control_plane_postgres.py -k runtime -q`

Expected: FAIL because revision 022 and runtime persistence do not exist.

- [ ] **Step 3: Implement schema and cursor helpers**

Migration 022 must create the columns specified by the design, with foreign keys to `m1_jobs(job_key)` and `m1_job_attempts(attempt_id)`, UTC timestamps, non-negative progress checks, and an append-only trigger:

```sql
CREATE FUNCTION m1_reject_runtime_event_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'runtime events are append-only'; END $$;
CREATE TRIGGER m1_runtime_events_immutable
BEFORE UPDATE OR DELETE ON m1_job_runtime_events
FOR EACH ROW EXECUTE FUNCTION m1_reject_runtime_event_mutation();
```

`claim_job()` must retain the generated `attempt_id`, insert runtime state with profile-derived deadlines, and append `job.started` before returning. `heartbeat()` updates `last_heartbeat_at`, heartbeat/lease deadlines, and the job lease in one transaction. Runtime event helpers accept the current cursor so specialized job methods can reuse the same transaction later.

- [ ] **Step 4: Verify migration and persistence**

Run: `uv run pytest tests/alembic/test_022.py tests/m1-perception/test_control_plane_postgres.py -k 'runtime or claim_job or heartbeat' -q`

Expected: PASS, including stale lease rejection and append-only mutation rejection.

- [ ] **Step 5: Commit**

```bash
git add alembic/versions/022_m1_job_runtime_evidence.py tests/alembic/test_022.py src/polyarb/control_plane/runtime_store.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(05.6-201): persist fenced runtime evidence"
```

### Task 3: Pure runtime policy and historical replay

**Files:**
- Create: `src/polyarb/control_plane/runtime_policy.py`
- Create: `src/polyarb/control_plane/runtime_replay.py`
- Create: `tests/m1-perception/test_control_plane_runtime_policy.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Produces: `RuntimeRuleResult`, `evaluate_soak_observation()`, `replay_soak_observations()`.

- [ ] **Step 1: Write the regression test**

```python
def test_replay_rejects_the_first_expired_lease_without_waiting_for_final_verify() -> None:
    records = (
        soak_record("2026-08-23T13:41:00Z", expired=0),
        soak_record("2026-08-23T16:22:21Z", expired=1),
        soak_record("2026-08-23T16:27:21Z", expired=0),
    )
    result = replay_soak_observations(records)
    assert result.first_breaking_at == datetime(2026, 8, 23, 16, 22, 21, tzinfo=UTC)
    assert result.reason_codes == ("lease.expired",)
```

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_policy.py -q`

Expected: FAIL because the policy module does not exist.

- [ ] **Step 3: Implement pure classification and CLI**

`evaluate_soak_observation()` returns a frozen result with observed time,
severity, breaking flag, and normalized reason codes. It classifies increased
expired leases, circuits, missing Machines, unavailable API, count regression,
and evidence gaps without mutating state. Add CLI:

```text
runtime-policy-replay --run-id run-a --json
```

It reads ordered immutable observations using the scoped DSN and prints the
first breaking sample, all normalized failures, sample count, and maximum gap.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_policy.py tests/m1-perception/test_control_plane_cli.py -k runtime_policy -q`

Expected: PASS.

```bash
git add src/polyarb/control_plane/runtime_policy.py src/polyarb/control_plane/runtime_replay.py tests/m1-perception/test_control_plane_runtime_policy.py src/polyarb/cli_control_plane.py tests/m1-perception/test_control_plane_cli.py
git commit -m "feat(05.6-201): replay runtime policy immediately"
```

### Task 4: Makefile entry, regression proof, and plan closure

**Files:**
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-201-SUMMARY.md`

- [ ] **Step 1: Add the Make contract test**

Assert `make -n runtime-policy-replay run_id=run-a` invokes
`python -m polyarb.cli_control_plane runtime-policy-replay --run-id run-a
--json` and never invokes Fly mutation or a deploy target.

- [ ] **Step 2: Implement the documented target**

```make
## runtime-policy-replay: Read immutable cloud observations and report the first live-policy break; never mutates jobs or Machines.
runtime-policy-replay:
	@test -n "$(run_id)" || (echo "usage: make runtime-policy-replay run_id=<run-id>" >&2; exit 2)
	@test -n "$$POLYARB_SUPABASE_DB_DSN" || (echo "ERROR: explicitly export scoped DSN" >&2; exit 2)
	@uv run python -m polyarb.cli_control_plane runtime-policy-replay --run-id "$(run_id)" --json
```

- [ ] **Step 3: Run plan gates**

Run: `uv run pytest tests/alembic/test_022.py tests/m1-perception/test_control_plane_runtime_models.py tests/m1-perception/test_control_plane_runtime_policy.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -q`

Run: `uv run ruff check src/polyarb/control_plane/runtime_models.py src/polyarb/control_plane/runtime_store.py src/polyarb/control_plane/runtime_policy.py src/polyarb/control_plane/runtime_replay.py src/polyarb/cli_control_plane.py tests/m1-perception`

Expected: all selected tests and Ruff PASS.

- [ ] **Step 4: Record the real failed-run replay and close the plan**

Run read-only: `set -a; source .env; set +a; make runtime-policy-replay run_id=m1-formal-20260823T1335Z`

Expected: first breaker `2026-08-23T16:22:21.027503+00:00`, reason `lease.expired`; no production mutation.

Write the SUMMARY with task commits, exact output, deviations, and next-plan interfaces. Then run `make planning-status` and commit only plan-owned files.
