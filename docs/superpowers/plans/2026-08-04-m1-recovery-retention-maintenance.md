# M1 Recovery Streak and Resident Retention Maintenance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make M1 production self-healing by restoring true consecutive-failure semantics, making snapshot and Structure staging retention FK-safe, and continuously draining authenticated Structure-generation evidence without operator intervention.

**Architecture:** Preserve the scheduler's existing certified-snapshot recovery gate while treating authenticated durable checkpoints as successful work that breaks a failure streak. Convert destructive Structure-window retention into payload reclamation that preserves authority skeletons. Give the existing bounded generation-evidence cleanup primitive a low-priority, durable, health-visible resident worker sharing the producer lock with Structure and Quote.

**Tech Stack:** Python 3.12, asyncio, Pydantic Settings, SQLite WAL/foreign keys, FastAPI health checks, pytest/pytest-asyncio, Ruff, Fly.io.

## Global Constraints

- Work only in `.worktrees/m1-self-healing-structure`; preserve the five user-owned dirty `.superpowers/sdd/*` files.
- Use `uv run` and existing Makefile targets. Do not add a raw Python-only operator entry point.
- Follow RED → GREEN → focused regression for every task; do not weaken existing assertions to make a new behavior pass.
- Every deletion transaction remains bounded and uses `BEGIN IMMEDIATE`; no network operation may occur while holding `producer_lock` or a SQLite transaction.
- Durable progress resets the scheduler failure streak but never moves `RECOVERING` to `RUNNING`; only a certified `OK`/`DEGRADED` snapshot does that.
- Quote owns priority. Generation cleanup must check Quote before and after acquiring the shared producer lock.
- Cleanup may never mutate current-generation pointers, publications, comparison/drift receipts, or the two-generation rollback floor.
- Use the existing `cleanup_structure_generation_evidence(max_rows=500)` authority. Do not create a second evidence-deletion implementation.
- Health must read fields that the worker/store really mutates, and Polywatch must alert and recover from that same health check.
- After each implementation commit, run `make planning-status`; create the plan SUMMARY before claiming the plan complete.

---

## Task 1: Correct consecutive-failure semantics

**Files:**

- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `tests/m1-perception/test_scheduler.py`

### Step 1: Write failing scheduler contract tests

Add focused async tests that construct a scheduler with a persisted counter and drive `_tick_once` through controlled child results:

```python
@pytest.mark.asyncio
async def test_durable_checkpoint_breaks_failure_streak_without_claiming_recovery(
    daemon_settings_for_test, store
):
    scheduler._failure_counter = 4
    scheduler.state = SchedulerState.RECOVERING
    scheduler._run_snapshot = AsyncMock(
        return_value=IsolatedStructureCheckpoint(
            stage="events", pages_processed=1, elapsed_ms=10
        )
    )

    assert await scheduler._tick_once(queued_at_ms=1) is True
    assert scheduler.failure_counter == 0
    assert scheduler.state == SchedulerState.RECOVERING
    store.set_scheduler_state.assert_called_with(
        failure_counter=0, state=SchedulerState.RECOVERING
    )
```

Cover all forward checkpoint families: event-member, classifier-v2 drift, Gamma/bootstrap, generation publication. Add a sequence test `failure → forward checkpoint → failure` proving the final counter is `1`, not `5`. Preserve counter values for:

- writer-busy / Quote-priority / identity-stale defers;
- `IsolatedStructurePublicationCheckpoint(stage="superseded")`;
- child failure or timeout.

Run:

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py -k 'failure_streak or durable_checkpoint'
```

Expected RED: forward checkpoints leave the counter unchanged.

### Step 2: Add one scheduler helper and call it only for authenticated progress

Add a helper near `_persist_counter`:

```python
def _record_durable_progress(self) -> None:
    """Break a failure streak without certifying full scheduler recovery."""
    if self._failure_counter == 0:
        return
    self._failure_counter = 0
    self._persist_counter()
```

Call it after each non-deferred child contract has been parsed and its attempt/checkpoint has been durably recorded. For publication checkpoints, call it only when `stage != "superseded"`. Do not assign `self.state` in this helper. Keep the complete snapshot branch responsible for `RECOVERING → RUNNING` and the recovery heartbeat.

### Step 3: Run focused and scheduler regressions

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py -k 'failure_counter or checkpoint or recovering or defer or superseded'
uv run pytest -q tests/m1-perception/test_scheduler.py
uv run ruff check src/polyarb/daemon/scheduler.py tests/m1-perception/test_scheduler.py
```

Expected GREEN: sequence ends at one failure; all recovery/defer/supersession contracts pass.

### Step 4: Commit

```bash
git add src/polyarb/daemon/scheduler.py tests/m1-perception/test_scheduler.py
git commit -m "fix(m1): reset failure streak on durable progress"
make planning-status
```

---

## Task 2: Reclaim Structure staging without deleting authority identities

**Files:**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_sync_window.py`
- Modify: `tests/m1-perception/test_schema_lockstep.py`

### Step 1: Write failing retention and schema-classification tests

Build published and failed windows containing rows in every Structure child table. Bind the published window to generation publication and drift evidence so its parent cannot legally disappear. Assert:

```python
reclaimed, ids = store.purge_published_structure_sync_windows(
    keep_last=1, max_windows_per_run=1
)
assert (reclaimed, ids) == (1, [old_window_id])
assert window_row["staging_reclaimed_at_ms"] is not None
assert heavy_payload_count(old_window_id) == 0
assert proof_skeleton_count(old_window_id) > 0
```

The heavy set is exactly:

```python
HEAVY_STRUCTURE_WINDOW_CHILDREN = {
    "structure_sync_event_staging",
    "structure_sync_market_staging",
    "structure_sync_event_market_staging",
    "structure_sync_event_metadata_staging",
    "structure_sync_event_member_staging",
    "structure_sync_event_group_truth_staging",
    "structure_sync_event_conflict_proofs",
    "structure_sync_event_conflict_merkle_nodes",
}
```

The retained set includes window row, source/member receipts, conflict summary, progress/checkpoint records, publication and drift evidence. Add an FK-introspection test using `PRAGMA foreign_key_list(table)` that enumerates every direct `structure_sync_windows(id)` child and asserts it belongs to exactly one classification: heavy-reclaimed, proof-retained, or independently protected. This test must fail when an unclassified child table is added.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py -k 'purge or reclaim'
uv run pytest -q tests/m1-perception/test_schema_lockstep.py -k structure_sync_window
```

Expected RED: current purge raises `sqlite3.IntegrityError` or removes the parent; no reclamation marker exists.

### Step 2: Add the nullable reclamation marker with migration

In `STRUCTURE_SYNC_WINDOWS_DDL`, add:

```sql
staging_reclaimed_at_ms INTEGER,
CHECK (staging_reclaimed_at_ms IS NULL OR staging_reclaimed_at_ms >= 0)
```

In each schema initialization/migration path that executes `STRUCTURE_SYNC_WINDOWS_DDL`, call the existing `_ensure_column` helper for old databases. Verify both a fresh database and an old-schema fixture gain the column without changing existing rows.

### Step 3: Replace parent deletion with atomic payload reclamation

Keep both public method signatures and return semantics, but interpret their result as reclaimed window payloads. Candidate queries must require `staging_reclaimed_at_ms IS NULL`. For published windows, retain the newest `keep_last` published windows; for failed windows, select the oldest bounded batch.

Within one `BEGIN IMMEDIATE` transaction:

1. select at most `max_windows_per_run` candidates;
2. delete only tables in `HEAVY_STRUCTURE_WINDOW_CHILDREN`, child-before-parent where needed;
3. update selected terminal windows with one `staging_reclaimed_at_ms` timestamp;
4. commit.

Do not delete receipts, conflict summaries, window rows, publication identities, drift progress, or drift receipts. A retry after commit returns zero; a rollback leaves both payload and marker unchanged.

### Step 4: Run migration, FK, and retention regressions

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py
uv run pytest -q tests/m1-perception/test_schema_lockstep.py
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py -k 'purge or status or evidence'
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_sync_window.py
```

Expected GREEN: no FK error, all heavy payload is gone, the proof skeleton and authenticated status remain readable.

### Step 5: Commit

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_schema_lockstep.py
git commit -m "fix(m1): reclaim structure staging safely"
make planning-status
```

---

## Task 3: Make legacy snapshot retention respect all direct owners

**Files:**

- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_sqlite_store.py`
- Modify: `tests/m1-perception/test_schema_lockstep.py`

### Step 1: Write failing snapshot ownership tests

Add three fixtures:

1. an expired snapshot referenced only by `snapshot_attempts` is selected, its attempts are retired in the same transaction, and the snapshot is deleted;
2. an expired snapshot referenced by `neg_risk_quote_runs` is skipped even if outside `keep_last`;
3. after `NegRiskQuoteStore.purge_old_runs` releases that reference, a later snapshot purge deletes it.

Add a schema contract test that classifies every direct FK to `snapshots(id)` as:

- transactionally deleted legacy payload/attempt;
- explicit candidate-selection protector;
- immutable generation/drift evidence already excluded by publication identity.

Run:

```bash
uv run pytest -q tests/m1-perception/test_sqlite_store.py -k 'purge_old_snapshots and (attempt or quote or foreign)'
```

Expected RED: attempt case raises an FK error; Quote-referenced candidate is not explicitly guarded.

### Step 2: Implement candidate guard and owned-attempt retirement

In the candidate `SELECT`, add:

```sql
AND NOT EXISTS (
  SELECT 1 FROM neg_risk_quote_runs q WHERE q.snapshot_id = snapshots.id
)
```

Before deleting selected snapshot parents, delete `snapshot_attempts` for only those selected IDs. Keep all selection and deletion inside the existing `BEGIN IMMEDIATE`. Do not delete Quote runs or their children from `SQLiteStore.purge_old_snapshots`.

### Step 3: Run focused and store regressions

```bash
uv run pytest -q tests/m1-perception/test_sqlite_store.py -k purge_old_snapshots
uv run pytest -q tests/m1-perception/test_schema_lockstep.py -k snapshots
uv run pytest -q tests/m1-perception/test_sqlite_store.py -k 'quote and purge'
uv run ruff check src/polyarb/storage/sqlite_store.py tests/m1-perception/test_sqlite_store.py
```

Expected GREEN: ownership ordering is explicit and both retention subsystems preserve one another's authority.

### Step 4: Commit

```bash
git add src/polyarb/storage/sqlite_store.py tests/m1-perception/test_sqlite_store.py tests/m1-perception/test_schema_lockstep.py
git commit -m "fix(m1): make snapshot retention owner aware"
make planning-status
```

---

## Task 4: Persist generation-cleanup runtime truth

**Files:**

- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`

### Step 1: Write failing runtime state-machine tests

Test fresh initialization, atomic begin, single-owner rejection, success, writer-busy defer, authentication block, failure backoff, and restart recovery. Use a singleton row (`id=1`) and assert exact public fields:

```python
{
    "state": "idle" | "running" | "backoff" | "blocked",
    "consecutive_failures": int,
    "last_attempt_at_ms": int | None,
    "last_success_at_ms": int | None,
    "next_attempt_at_ms": int,
    "generation_snapshot_id": int | None,
    "phase": str | None,
    "rows_deleted": int,
    "error_kind": str | None,
    "checkpoint_at_ms": int,
}
```

An orphaned `running` row recovered on startup must become `backoff`, carry safe `error_kind="worker-restarted"`, and receive a bounded `next_attempt_at_ms`. Two stores racing `begin_structure_generation_cleanup_attempt` must yield exactly one owner.

### Step 2: Add schema and store API

Add `STRUCTURE_GENERATION_CLEANUP_RUNTIME_DDL` with checks for singleton ID, allowed states, nonnegative counters/timestamps/row counts, and required checkpoint. Initialize it in all normal schema paths with `INSERT OR IGNORE`.

Implement:

```python
def structure_generation_cleanup_runtime_status(self) -> dict[str, object]:
    raise NotImplementedError

def recover_structure_generation_cleanup_runtime(
    self, *, now_ms: int, retry_delay_ms: int
) -> dict[str, object]:
    raise NotImplementedError

def begin_structure_generation_cleanup_attempt(self, *, now_ms: int) -> bool:
    raise NotImplementedError
def finish_structure_generation_cleanup_attempt(
    self,
    *,
    state: Literal["idle", "backoff", "blocked"],
    now_ms: int,
    next_attempt_at_ms: int,
    generation_snapshot_id: int | None,
    phase: str | None,
    rows_deleted: int,
    error_kind: str | None,
    increment_failure: bool,
) -> dict[str, object]:
    raise NotImplementedError
```

Use a conditional singleton update for admission: set `state='running'`, update attempt/checkpoint timestamps, and require `id=1 AND state!='running' AND next_attempt_at_ms<=?`. Finish updates require `id=1 AND state='running'`. A writer-busy defer sets `backoff` without incrementing failures. Blocked authentication increments/retains evidence independently from unexpected-error backoff.

Extend every return branch of `cleanup_structure_generation_evidence` with
`generation_snapshot_id: int | None`. Return the authenticated active/candidate
generation ID, never infer it from retained ordering. The worker will read
remaining reclaimable pressure from `structure_generation_status()` after the
chunk; the cleanup primitive remains the only deletion authority.

### Step 3: Run store tests

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py -k cleanup_runtime
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py -k cleanup
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_generation_readers.py
```

Expected GREEN: lifecycle survives store recreation and has one admitted owner.

### Step 4: Commit

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): persist cleanup worker runtime"
make planning-status
```

---

## Task 5: Add the low-priority resident cleanup worker

**Files:**

- Create: `src/polyarb/daemon/generation_cleanup_worker.py`
- Create: `tests/m1-perception/test_generation_cleanup_worker.py`
- Modify: `src/polyarb/daemon/scheduler.py` only if extracting a shared Quote-priority predicate
- Modify: `tests/m1-perception/test_scheduler.py` if the predicate is extracted

### Step 1: Write worker behavior tests with a fake clock/store

Cover:

- startup recovery of orphaned `running` state;
- Quote active/due before lock: no lock acquisition and `quote-priority` defer;
- Quote becomes due while waiting: acquire, recheck, release without cleanup;
- one admitted tick calls `cleanup_structure_generation_evidence(max_rows=500)` exactly once;
- remaining pressure schedules `now + 50ms`, no candidate schedules `now + 30s`;
- `sqlite3.OperationalError` writer-busy schedules five seconds and does not increment failures;
- receipt/authentication failure becomes durable `blocked`;
- unexpected errors use capped exponential backoff;
- cancellation propagates and releases the lock;
- two worker instances sharing one DB cannot both run a chunk.

Because cancelling an `asyncio.to_thread` await does not stop its SQLite thread,
add a test that cancels during a real bounded chunk and proves the worker waits
for that thread to finish before releasing `producer_lock`. There must never be
two writers merely because the coroutine was cancelled.

### Step 2: Implement the worker

Create `StructureGenerationCleanupWorker` with injected settings, store, shared `asyncio.Lock`, Quote runtime, clock, and sleep/wait seams. Its `run(stop_event)` loop calls one bounded `_tick()` and then waits interruptibly until the durable `next_attempt_at_ms`.

Admission pseudocode:

```python
if quote_priority_reason(self._quote_runtime) is not None:
    await self._record_defer("quote-priority", busy_delay)
    return

async with self._producer_lock:
    if quote_priority_reason(self._quote_runtime) is not None:
        await self._record_defer("quote-priority", busy_delay)
        return
    if not await asyncio.to_thread(
        store.begin_structure_generation_cleanup_attempt,
        now_ms=now_ms,
    ):
        return
    cleanup_task = asyncio.create_task(asyncio.to_thread(
            store.cleanup_structure_generation_evidence,
            retain_generations=settings.structure_generation_retention_floor,
            max_rows=settings.structure_generation_cleanup_max_rows,
            now_ms=now_ms,
        ))
    try:
        result = await asyncio.shield(cleanup_task)
    except asyncio.CancelledError:
        await cleanup_task
        raise
```

Classify the typed cleanup result rather than guessing from text: authenticated
`blocked`/`blocked_reason` → blocked; after any nonblocked chunk, read durable
evidence pressure and schedule active when reclaimable remains or idle when it
is zero. Persist `generation_snapshot_id` from the explicit result field. Keep
log/error kinds bounded and secret-free.

### Step 3: Run worker and shared-lock regressions

```bash
uv run pytest -q tests/m1-perception/test_generation_cleanup_worker.py
uv run pytest -q tests/m1-perception/test_scheduler.py -k 'quote_priority or producer_lock'
uv run ruff check src/polyarb/daemon/generation_cleanup_worker.py tests/m1-perception/test_generation_cleanup_worker.py
```

Expected GREEN: cleanup is resident, bounded, cancellable, single-owner, and cannot outrank Quote.

### Step 4: Commit

```bash
git add src/polyarb/daemon/generation_cleanup_worker.py tests/m1-perception/test_generation_cleanup_worker.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_scheduler.py
git commit -m "feat(m1): run generation cleanup continuously"
make planning-status
```

---

## Task 6: Wire configuration, lifecycle, health, and alert recovery

**Files:**

- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `tests/m1-perception/test_settings_yaml.py`
- Modify: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `tests/m1-perception/test_daemon_shutdown.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Modify: `Makefile`

### Step 1: Add failing config and lifecycle tests

Add validated settings with defaults:

```python
structure_generation_cleanup_enabled: bool = True
structure_generation_cleanup_max_rows: int = Field(default=500, ge=1)
structure_generation_cleanup_active_interval_s: float = Field(default=0.05, gt=0)
structure_generation_cleanup_idle_interval_s: float = Field(default=30.0, gt=0)
structure_generation_cleanup_writer_busy_interval_s: float = Field(default=5.0, gt=0)
structure_generation_cleanup_retry_initial_s: float = Field(default=1.0, gt=0)
structure_generation_cleanup_retry_max_s: float = Field(default=30.0, gt=0)
structure_generation_cleanup_failure_threshold: int = Field(default=3, ge=1)
```

Assert the worker starts only when Structure generation publication and cleanup are enabled in the in-process L1 topology. Assert isolated/supervisor topology cannot create a duplicate owner and reports disabled explicitly. Assert graceful shutdown sets the stop event, cancels/gathers the worker with the same five-second boundary as sibling tasks, and leaves no pending task.

### Step 2: Add failing chain-truth health tests

Extend `SQLiteStore.structure_generation_status()` with the durable runtime object, then add `_structure_generation_cleanup_health_check`. Required output:

```text
state=<state> failures=<n> rows_deleted=<n> generation_snapshot_id=<id>
phase=<phase> next_attempt_at_ms=<ts> checkpoint_age_seconds=<age>
reclaimable=<count> error_kind=<safe-kind>
```

Severity:

- pass: cleanup enabled, runtime idle/running/backoff below threshold, and no stale unresolved pressure;
- warn: active bounded drain or transient backoff below threshold;
- fail: blocked, failures at/above threshold, or stale runtime checkpoint while reclaimable pressure remains;
- pass with `enabled=false`: intentionally disabled topology, with explicit reason.

The check key is `snapshot:structure_generation_cleanup_runtime`. Test the full chain: store mutation → `structure_generation_status` → `/health` sub-check → top-level severity.

### Step 3: Add failing Polywatch alert/recovery tests

Teach `decide_l1` to push on failed cleanup runtime with a diagnostic that includes bounded state/error/pressure fields. Extend resident incident identity and recovery matching so one alert opens for the same cleanup incident and one recovery occurs only after:

- cleanup health is pass; and
- `snapshot:structure_generation_evidence` is below pressure failure (ultimately production acceptance requires retained `<=2`).

Test blocked, repeated failures, stale progress, deduplicated repeated polling, and recovery.

### Step 4: Wire lifecycle and document Makefile configuration

Construct the worker beside scheduler/Quote with the same `producer_lock` and Quote runtime. Start it only for the single-owner in-process topology, add it to shutdown gather, and keep manual `make structure-generation-cleanup` as an operator primitive. Update `make help` text/comments to say resident cleanup is normal and the target is diagnostic/recovery; do not add a duplicate executable command.

### Step 5: Run chain-truth regressions

```bash
uv run pytest -q tests/m1-perception/test_settings_yaml.py -k generation_cleanup
uv run pytest -q tests/m1-perception/test_l1_quote_worker_wiring.py -k generation_cleanup
uv run pytest -q tests/m1-perception/test_daemon_shutdown.py -k generation_cleanup
uv run pytest -q tests/m1-perception/test_health_endpoint.py -k generation_cleanup
uv run pytest -q tests/m1-perception/test_polywatch_healthz_watcher.py -k generation_cleanup
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py -k 'status or cleanup_runtime'
make help
uv run ruff check src/polyarb/config.py src/polyarb/daemon/main.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py
```

Expected GREEN: an injected runtime failure is durable, health-visible, alerting, retrying, and later produces a deduplicated recovery.

### Step 6: Commit

```bash
git add src/polyarb/config.py src/polyarb/daemon/main.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_settings_yaml.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_daemon_shutdown.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py Makefile
git commit -m "feat(m1): expose resident cleanup health"
make planning-status
```

---

## Task 7: Prove production-shaped throughput, fairness, and operator understanding

**Files:**

- Modify: `tests/m1-perception/test_generation_cleanup_worker.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`
- Create: `docs/learning/47-resident-retention-maintenance.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `docs/dev/perception-fault-runbook.md`
- Modify: `.planning/threads/market-observation-architecture.md`

### Step 1: Add the production-shaped acceptance test

Seed a real temporary SQLite database with an authenticated reclaimable generation containing approximately 300,000 rows across the same evidence tables observed in production. Drive the real worker/store cleanup loop with `max_rows=500` and a monotonic timer.

Assert:

```python
assert elapsed_s < 240
assert final_status["evidence_pressure"]["retained"] <= 2
assert final_status["evidence_pressure"]["reclaimable"] == 0
assert max_rows_deleted_in_one_transaction <= 500
```

During the drain, enqueue a Quote waiter after a cleanup transaction starts. Assert Quote acquires the producer lock before the next cleanup transaction and its wait is bounded by one current 500-row transaction plus scheduling jitter. Mark this as an M1 production acceptance test, not a unit-only extrapolation.

### Step 2: Add restart and fault recovery acceptance

Interrupt after a partially deleted cleanup phase, recreate store/worker, and verify cleanup resumes from authenticated progress without deleting rollback-floor/current evidence. Inject one bounded unexpected error and verify:

1. runtime moves to backoff;
2. health fails at configured threshold;
3. watcher produces one alert;
4. later successful cleanup clears pressure and produces one recovery.

### Step 3: Write learning and operations docs

Create the learning document with:

- 30-second model: authority evidence vs replayable payload vs operational runtime;
- code references with current `file:line` locations after implementation;
- why checkpoints break a streak but do not certify recovery;
- why Quote owns snapshot retention and producer-lock priority;
- self-check questions and FAQ increment section.

Update the learning index and production runbook with health interpretation, normal resident behavior, safe manual diagnostic command, backoff/blocked response, and the exact acceptance thresholds. Record the new chain-truth lesson in the architecture thread.

### Step 4: Run focused acceptance and commit

```bash
uv run pytest -q tests/m1-perception/test_generation_cleanup_worker.py -m 'not live'
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py -k 'cleanup and (restart or pressure or rollback)'
uv run ruff check tests/m1-perception/test_generation_cleanup_worker.py tests/m1-perception/test_structure_generation_readers.py
make planning-status
```

Expected GREEN: real 300k-row evidence drains below the floor within 240 seconds and Quote preempts after at most the current bounded transaction.

```bash
git add tests/m1-perception/test_generation_cleanup_worker.py tests/m1-perception/test_structure_generation_readers.py docs/learning/47-resident-retention-maintenance.md docs/learning/00-INDEX.md docs/dev/perception-fault-runbook.md .planning/threads/market-observation-architecture.md
git commit -m "test(m1): prove resident maintenance recovery"
make planning-status
```

---

## Task 8: Full verification, plan summary, review, protected deployment, and UAT

**Files:**

- Create: `docs/superpowers/plans/2026-08-04-m1-recovery-retention-maintenance-SUMMARY.md`
- Modify: `.planning/JOURNAL.md`
- Modify only if verification finds a real defect: files owned by Tasks 1–7

### Step 1: Run the complete local quality gate

```bash
make planning-status
uv run ruff check .
uv run pytest -q tests/m1-perception
make help
git diff --check
git status --short
```

Expected: planning status OK, Ruff clean, all M1 tests pass, Makefile help exposes the existing cleanup diagnostic, no whitespace errors, and only the five pre-existing user-owned dirty files remain outside committed work.

### Step 2: Review the complete diff against the approved specification

Run an independent code review only if explicitly authorized; otherwise perform an inline fresh-context review. Reject completion for any of:

- counter reset changes `RECOVERING` to `RUNNING`;
- defer/supersession resets the counter;
- Structure window/proof skeleton is deleted;
- snapshot purge deletes Quote-owned rows;
- cleanup bypasses existing authenticated receipts/progress;
- Quote priority is checked only once;
- runtime errors exist only in logs;
- health reads a field the writer never mutates;
- lifecycle can start two owners or leak a shutdown task.

Fix findings with a new RED test and atomic commit, then rerun the full gate.

### Step 3: Create the required plan SUMMARY

Document commits, behavior changes, exact verification outputs, design deviations, production risks, and deployment rollback. Then:

```bash
git add docs/superpowers/plans/2026-08-04-m1-recovery-retention-maintenance-SUMMARY.md
git commit -m "docs(m1): summarize resident maintenance"
make planning-status
```

### Step 4: Obtain exact-SHA deployment approval and deploy protected defaults

Present the exact commit SHA and local gate evidence. Deployment is a separate external mutation and requires explicit approval for that SHA. Build and deploy with:

- Structure enabled;
- Quote still disabled;
- generation read mode still `legacy`;
- cleanup enabled with `500 / 0.05s / 30s / 5s` defaults.

Verify Fly release image/digest matches the approved SHA. Do not manually advance pointers, delete evidence, restart to hide a fault, or enable Quote/read-generation during this maintenance deployment.

### Step 5: Observe natural production recovery

Require all of the following from natural runtime behavior:

1. active classifier-v2 work seals or resumes safely;
2. generation cleanup runtime advances without operator command;
3. retained generations converge from `9` to `<=2`, reclaimable becomes `0`;
4. Structure staging reclamation reports no FK errors and marks terminal windows;
5. old snapshot retention reports no FK errors and preserves Quote-owned snapshots;
6. a durable checkpoint between two scoped failures yields failure counter `1`;
7. injected maintenance failure opens one alert, auto-retries, and closes with one recovery;
8. Quote-priority test shows cleanup yields after the current bounded transaction;
9. `/health` chain fields match direct SQLite authority.

Observe long enough to cover at least one natural scheduler continuation, one idle cleanup interval, and the alert/recovery cycle. Preserve timestamps, generation/window IDs, release/image identity, row counts, and query latency in the SUMMARY/JOURNAL.

### Step 6: Return to Task 8 cutover and actual opportunity UAT

Only after maintenance acceptance passes, continue the pre-existing protected cutover sequence: classifier-v2 seal → compare/read-generation gate → Quote enablement → verified opportunity endpoint → candidate lifecycle queue. Final M1 acceptance still requires live candidate tracking and UAT; successful maintenance alone does not complete the persistent goal.

### Step 7: Close session state

Append the JOURNAL session with modifications, learning, decisions, production evidence, remaining cutover gate, and exact next command. Update roadmap/state only to the level proven by UAT. Commit planning state without touching user-owned dirty files.
