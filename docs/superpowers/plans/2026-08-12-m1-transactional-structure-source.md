# M1 Transactional Structure Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `test-driven-development` task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move authoritative bounded Gamma Structure collection from legacy SQLite into fenced Postgres/R2 source-window jobs, so replacement workers resume without local disk.

**Architecture:** A source window owns two ordered Gamma keysets: events then markets. A worker claims one durable page job, fetches one opaque-cursor page, uploads an immutable page artifact to R2, and records the artifact plus successor cursor in the same fenced Postgres transaction. A sealed source window later materializes the existing Structure bundle contract and feeds the already-proven normalize/certify/quote graph.

**Tech Stack:** Python 3.12, psycopg 3, Alembic, PostgreSQL, R2, Gamma client, pytest/Testcontainers.

## Global Constraints

- Additive Alembic only; do not mutate SQLite or a production pointer.
- Each Gamma request is one validated page; cursors are opaque `None | str`.
- R2 receipt means PUT and HEAD authentication before the DB transaction.
- All writes are fenced by `JobLease.lease_epoch`.
- At-least-once execution; exactly-once durable page receipt.
- Operator commands require `--enable` and a Makefile entry.
- Existing range normalizer/certifier remains unchanged until source-window bundle parity passes.

---

### Task 1: Add source-window schema and typed contracts

**Files:**
- Create: `alembic/versions/010_m1_transactional_structure_source.py`
- Modify: `src/polyarb/control_plane/models.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/alembic/test_010.py`
- Test: `tests/m1-perception/test_transactional_structure_source.py`

**Interfaces:**
- Produces `StructureSourcePageSpec(window_key, stream, ordinal, requested_cursor)`.
- Produces `admit_structure_source_window(...)` and fenced `record_structure_source_page(...)`.
- The receipt transaction creates the successor page only when the response is non-terminal.

- [ ] **Step 1: Write the failing repository test**

```python
def test_source_page_receipt_advances_only_after_fenced_commit(pg_control_plane, now):
    page = pg_control_plane.admit_structure_source_window(window_key="window:1", now=now)[0]
    lease = pg_control_plane.claim_job(
        worker_id="w1", job_types=("structure-fetch",), lease_seconds=30, now=now
    )
    assert lease and lease.job_key == page.job_key
    pg_control_plane.record_structure_source_page(
        lease, artifact_key="m1/structure/source/window:1/events/0.json",
        artifact_digest="a" * 64, next_cursor="opaque-next", completed=False, now=now,
    )
    assert pg_control_plane.structure_source_page_receipt(page.job_key)["next_cursor"] == "opaque-next"
```

- [ ] **Step 2: Confirm RED**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source.py -q`

Expected: FAIL because the types and repository methods do not exist.

- [ ] **Step 3: Implement minimum schema and repository**

```python
@dataclass(frozen=True, slots=True)
class StructureSourcePageSpec:
    window_key: str
    stream: str
    ordinal: int
    requested_cursor: str | None

    @property
    def job_key(self) -> str:
        return f"{self.window_key}:fetch:{self.stream}:{self.ordinal}"
```

Create `m1_structure_source_windows`, `m1_structure_source_page_inputs`, and `m1_structure_source_page_receipts`; each source page input has an `m1_jobs` foreign key and immutable identity.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/alembic/test_010.py tests/m1-perception/test_transactional_structure_source.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add alembic/versions/010_m1_transactional_structure_source.py src/polyarb/control_plane/models.py src/polyarb/control_plane/postgres.py tests/alembic/test_010.py tests/m1-perception/test_transactional_structure_source.py
git commit -m "feat(05.6-111): add transactional Structure source windows"
```

### Task 2: Persist authenticated Gamma page artifacts and recover from worker loss

**Files:**
- Create: `src/polyarb/control_plane/structure_source.py`
- Test: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**
- Consumes `StructureSourcePageSpec`, a Gamma-compatible single-page reader, and an R2 object client.
- Produces `TransactionalStructureSourceWorker.run_once() -> StructureWorkerResult`.

- [ ] **Step 1: Write failing recovery tests**

```python
async def test_replacement_reuses_uploaded_source_page_after_crash_before_receipt(...):
    # worker one PUTs + HEADs page artifact, then dies before DB receipt
    # worker two reclaims expired lease, refetches and records the same cursor/digest
    assert receipt["artifact_digest"] == expected_digest
    assert gamma.calls == 2
```

Also require that a failed Gamma request leaves its durable requested cursor unchanged and enqueues no successor job.

- [ ] **Step 2: Confirm RED**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py -q`

Expected: FAIL because `TransactionalStructureSourceWorker` does not exist.

- [ ] **Step 3: Implement canonical page artifact and worker**

```python
async def run_once(self) -> StructureWorkerResult:
    lease = self._control_plane.claim_job(..., job_types=("structure-fetch",), ...)
    spec = self._control_plane.structure_source_page_spec(lease.job_key)
    page = await self._fetch(spec)
    artifact = StructureSourcePageArtifact.from_page(spec, page)
    upload_structure_source_page_artifact(..., artifact=artifact)  # PUT + HEAD
    self._control_plane.record_structure_source_page(lease, ..., now=self._now())
```

Malformed page/cursor contracts are quarantined. Transport errors become retryable and commit no successor cursor.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_worker.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/structure_source.py tests/m1-perception/test_transactional_structure_source_worker.py
git commit -m "feat(05.6-112): fetch Structure source pages transactionally"
```

### Task 3: Seal source windows and materialize a parity-preserving bundle

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/structure_artifact.py`
- Test: `tests/m1-perception/test_transactional_structure_source_bundle.py`

**Interfaces:**
- Consumes terminal event and market page receipts for one window.
- Produces the existing `StructureBundleArtifact` and `enqueue_structure_generation(...)` input without SQLite reads.

- [ ] **Step 1: Write failing parity test**

```python
def test_terminal_source_window_materializes_existing_bundle_contract(...):
    bundle = materialize_structure_source_window(...)
    identity, components = parse_structure_bundle_bytes(bundle.payload, expected_sha256=bundle.sha256)
    assert identity.header()["source_kind"] == "gamma-source-window-v1"
    assert components["events"] == expected_events
```

- [ ] **Step 2: Confirm RED**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_bundle.py -q`

Expected: FAIL because terminal source materialization does not exist.

- [ ] **Step 3: Implement sealed-window materialization**

Read all page artifacts in ordinal order, re-parse each with its recorded digest, reject gaps and duplicate upstream IDs, derive the six current bundle components using the existing normalizer helpers, PUT+HEAD the bundle, and admit existing ranges. One fenced terminal transaction records the source-window bundle receipt and rejects a second different digest.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/m1-perception/test_transactional_structure_source_bundle.py tests/m1-perception/test_transactional_structure_worker.py tests/m1-perception/test_transactional_quote_worker.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/postgres.py src/polyarb/control_plane/structure_source.py src/polyarb/control_plane/structure_artifact.py tests/m1-perception/test_transactional_structure_source_bundle.py
git commit -m "feat(05.6-113): materialize Structure bundles from source windows"
```

### Task 4: Schedule source jobs and document the real cloud cutover gate

**Files:**
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Modify: `docs/learning/64-事务型云端控制面.md`
- Test: `tests/m1-perception/test_transactional_control_plane_scheduler.py`
- Test: `tests/m1-perception/test_control_plane_cli.py`

**Interfaces:**
- Adds `structure-source` and `structure-source-materialize` turns ahead of range normalization.
- Adds guarded `make control-plane-source-once enable=1`.

- [ ] **Step 1: Write failing scheduler and CLI tests**

```python
async def test_scheduler_rotates_source_before_structure_normalization():
    outcome = await scheduler.run_tick()
    assert outcome["turns"][0]["worker"] == "structure-source"
```

- [ ] **Step 2: Confirm RED**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py -q`

Expected: FAIL because source workers are not scheduled or exposed.

- [ ] **Step 3: Implement guarded construction and docs**

Only the separated worker receives Gamma/R2 writer secrets; the API remains Postgres-only. The learning note must distinguish legacy SQLite shadow export from actual source windows and fix the acceptance order:

`named environment → migration 010 → isolated deploy → three fresh source-window shadows → worker-loss injection → 24-hour evidence → reversible pointer switch`.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py -q && make docs-m1-check && make planning-status`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/scheduler.py src/polyarb/cli_control_plane.py Makefile docs/learning/64-事务型云端控制面.md tests/m1-perception/test_transactional_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py
git commit -m "feat(05.6-114): schedule transactional Structure source collection"
```

## Self-review

- Tasks cover the missing authoritative source boundary, worker-loss recovery, handoff to the current transactional graph, and operation in the isolated worker service.
- Legacy SQLite export remains a shadow-only comparison tool after this work; it is not a production source.
- Pointer mutation remains explicitly deferred until fresh source-window shadow, chaos, and 24-hour cloud evidence pass.
