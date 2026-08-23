# M1 Task-Local Runtime Instrumentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make all eight transactional production job types report stages, progress, heartbeat, success, and atomic failure without relying on the periodic sampler.

**Architecture:** A shared runtime reporter wraps the existing fenced `JobLease`; async and sync workers use the same persistent contract. Specialized completion methods append success facts inside their existing receipt/pointer transactions.

**Tech Stack:** Python 3.12, asyncio, psycopg 3, pytest, uv.

## Global Constraints

- Execute after Plan 01 and consume its runtime types/store interfaces exactly.
- Instrument `structure-fetch`, `structure-materialize`, `structure-normalize`, `structure-certify`, `quote-admit`, `quote-batch`, `quote-certify`, and `opportunity-certify`.
- Heartbeat proves liveness only; only explicit `progress()` advances `last_progress_at`.
- Every external call retains a timeout shorter than the attempt deadline.
- Preserve existing R2-before-receipt fencing and idempotency.
- Use TDD and atomic commits. End with `05.6-202-SUMMARY.md` and clean `make planning-status`.
- No production deployment or mutation belongs to this plan.

---

### Task 1: Shared sync/async attempt reporter

**Files:**
- Create: `src/polyarb/control_plane/runtime_contract.py`
- Create: `tests/m1-perception/test_control_plane_runtime_contract.py`
- Modify: `src/polyarb/control_plane/postgres.py`

**Interfaces:**
- Produces: `AttemptRuntime.progress()`, `AttemptRuntime.heartbeat_if_due()`, `AsyncAttemptRuntime.start()`, `AsyncAttemptRuntime.stop()`.

- [ ] **Step 1: Write failing reporter tests**

```python
def test_progress_is_monotonic_and_heartbeat_does_not_advance_progress() -> None:
    runtime = AttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock)
    runtime.progress(stage="read-shards", current=1, total=4)
    clock.advance(seconds=30)
    runtime.heartbeat_if_due()
    assert store.progress_sequences == [1]
    assert store.heartbeat_count == 1

@pytest.mark.asyncio
async def test_async_runtime_heartbeats_while_blocking_io_runs() -> None:
    async with AsyncAttemptRuntime(store=store, lease=LEASE, profile=PROFILE, clock=clock):
        await clock.advance_async(seconds=61)
    assert store.heartbeat_count >= 2
```

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_contract.py -q`

Expected: FAIL because the reporter does not exist.

- [ ] **Step 3: Implement reporters**

`AttemptRuntime` owns a monotonically increasing progress sequence and calls
the Plan 01 store only when the heartbeat interval has elapsed.
`AsyncAttemptRuntime` starts one cancellable heartbeat task; its `__aexit__`
always awaits task termination before the worker finishes the lease. A
heartbeat error cancels the worker path and surfaces `StaleLeaseError`; it may
not be swallowed.

Core contract:

```python
def progress(self, *, stage: str, current: int, total: int | None) -> None:
    self._sequence += 1
    self._store.record_runtime_progress(
        self._lease,
        RuntimeProgress(sequence=self._sequence, current=current, total=total, stage=stage),
        now=self._clock(),
    )

async def _heartbeat_loop(self) -> None:
    while not self._stopped.is_set():
        try:
            await asyncio.wait_for(
                self._stopped.wait(), timeout=self._profile.heartbeat_seconds
            )
        except TimeoutError:
            pass
        if not self._stopped.is_set():
            self._lease = await asyncio.to_thread(
                self._store.heartbeat_runtime_attempt,
                self._lease,
                now=self._clock(),
                lease_seconds=self._profile.lease_seconds,
            )
```

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_runtime_contract.py tests/m1-perception/test_control_plane_postgres.py -k runtime -q`

Expected: PASS.

```bash
git add src/polyarb/control_plane/runtime_contract.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_runtime_contract.py
git commit -m "feat(05.6-202): add fenced task runtime reporters"
```

### Task 2: Fix and instrument Quote admission regression

**Files:**
- Modify: `src/polyarb/control_plane/quote_admission.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_transactional_quote_admission.py`

**Interfaces:**
- Consumes: `AsyncAttemptRuntime`.
- Produces stages: `read-manifest`, `read-shards`, `build-batches`, `upload-batches`, `commit-admission`.

- [ ] **Step 1: Write the 207-second regression test**

Use a virtual clock and blocking object-client fixture. Run Quote admission for
207 simulated seconds, assert at least six fenced heartbeats, strictly
increasing shard/batch progress, no expired lease observation, and the same
terminal Quote input rows as before.

- [ ] **Step 2: Prove the existing implementation fails**

Run: `uv run pytest tests/m1-perception/test_transactional_quote_admission.py -k long_runtime -q`

Expected: FAIL because Quote admission holds one 120-second lease without heartbeat.

- [ ] **Step 3: Instrument the exact stages**

Wrap `run_once()` in `AsyncAttemptRuntime`. Move synchronous R2 reads to
`asyncio.to_thread`; call `progress()` after every authenticated shard and
every uploaded batch. Keep `admit_quote_generation()` as the fenced terminal
commit, and append `job.succeeded` in that same transaction. The progress shape
is:

```python
runtime.progress(stage="read-manifest", current=1, total=1)
for index, shard in enumerate(shards, start=1):
    rows.extend(await asyncio.to_thread(self._read_market_shard, shard))
    runtime.progress(stage="read-shards", current=index, total=len(shards))
for index, artifact in enumerate(input_artifacts, start=1):
    await asyncio.to_thread(upload_quote_batch_artifact, self._object_client, bucket=self._bucket, artifact=artifact)
    runtime.progress(stage="upload-batches", current=index, total=len(input_artifacts))
```

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_transactional_quote_admission.py tests/m1-perception/test_control_plane_postgres.py -k 'quote_admission or runtime' -q`

Expected: PASS, including 207-second regression, stale-heartbeat rejection, and unchanged input digests.

```bash
git add src/polyarb/control_plane/quote_admission.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_transactional_quote_admission.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "fix(05.6-202): keep Quote admission lease live and visible"
```

### Task 3: Instrument Structure source and normalization jobs

**Files:**
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_materializer.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`

**Interfaces:**
- Produces bounded stages for four Structure job types.

- [ ] **Step 1: Add failing lifecycle tests**

For each job type, assert claim creates `job.started`, every bounded page/range
or parity chunk advances progress, success is appended in the same receipt or
certification transaction, and exception paths append retry/incident facts in
the same transaction.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_transactional_structure_source_materializer.py tests/m1-perception/test_transactional_structure_worker.py -k runtime -q`

Expected: FAIL because lifecycle facts are absent.

- [ ] **Step 3: Add stage contracts**

Use these exact stage names:

```python
STRUCTURE_STAGES = {
    "structure-fetch": ("fetch-page", "validate-page", "upload-page", "commit-page"),
    "structure-materialize": ("read-page-receipts", "build-bundle", "upload-bundle", "commit-bundle"),
    "structure-normalize": ("read-range", "normalize-range", "upload-range", "commit-range"),
    "structure-certify": ("verify-parity", "build-manifest", "upload-manifest", "commit-certification"),
}
```

Reuse the existing Structure certifier heartbeat but route it through the
shared reporter. Preserve its parity heartbeat frequency and stale lease
behavior.

- [ ] **Step 4: Verify and commit**

Run the three test files above without `-k`, then:
`uv run ruff check src/polyarb/control_plane/structure_source.py src/polyarb/control_plane/structure_worker.py tests/m1-perception/test_transactional_structure*`

Expected: PASS.

```bash
git add src/polyarb/control_plane/structure_source.py src/polyarb/control_plane/structure_worker.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_transactional_structure_source_materializer.py tests/m1-perception/test_transactional_structure_worker.py
git commit -m "feat(05.6-202): expose Structure task lifecycle"
```

### Task 4: Instrument Quote batch/certifier and opportunity certifier

**Files:**
- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/opportunity_worker.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`
- Modify: `tests/m1-perception/test_transactional_opportunity_projection.py`

- [ ] **Step 1: Add failing lifecycle and failure-atomicity tests**

Assert Quote batch exposes token/batch progress, both certifiers expose
verification/commit stages, and a raised CLOB/R2/Postgres error produces one
retryable event, one incident transition, and one alert intent atomically.

- [ ] **Step 2: Prove red**

Run: `uv run pytest tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_transactional_opportunity_projection.py -k runtime -q`

Expected: FAIL because stages and atomic runtime facts are absent.

- [ ] **Step 3: Instrument all terminal paths**

Use exact stages:

```python
QUOTE_BATCH_STAGES = ("read-input", "fetch-books", "build-artifact", "upload-artifact", "commit-receipt")
QUOTE_CERTIFY_STAGES = ("verify-batches", "publish-pointer")
OPPORTUNITY_CERTIFY_STAGES = ("read-current-quote", "compute-opportunities", "upload-projection", "publish-opportunity")
```

Specialized Postgres receipt/pointer methods append `job.succeeded` before
commit. Every generic exception routes through one
`finish_retryable_with_runtime_incident()` method; do not duplicate SQL in a
worker.

- [ ] **Step 4: Verify and commit**

Run both test files without `-k` plus `tests/m1-perception/test_control_plane_postgres.py -q`.

Expected: PASS.

```bash
git add src/polyarb/control_plane/quote_worker.py src/polyarb/control_plane/opportunity_worker.py src/polyarb/control_plane/postgres.py tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_transactional_opportunity_projection.py
git commit -m "feat(05.6-202): expose Quote and opportunity lifecycle"
```

### Task 5: Cross-job contract gate and plan closure

**Files:**
- Create: `tests/m1-perception/test_transactional_runtime_coverage.py`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-202-SUMMARY.md`

- [ ] **Step 1: Add the eight-job coverage test**

Claim and complete one fixture for each required job type. Assert every attempt
has exactly one start, at least one meaningful stage/progress event, and one
terminal event, with no secret-like detail keys.

- [ ] **Step 2: Run the complete task gate**

Run: `uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_transactional_* -q`

Run: `uv run ruff check src/polyarb/control_plane tests/m1-perception`

Expected: PASS.

- [ ] **Step 3: Write SUMMARY and planning gate**

Record all task commits, the 207-second regression result, all eight job types,
deadline profiles, and any deviation. Run `make planning-status`; expected no
drift. Commit the SUMMARY only after all evidence is present.
