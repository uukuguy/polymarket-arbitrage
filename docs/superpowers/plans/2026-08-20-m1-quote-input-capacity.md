# M1 Quote Input Capacity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove large Quote-batch JSONB from Supabase while preserving M1's transactional retry and recovery guarantees.

**Architecture:** Create an immutable R2 Quote-input artifact before admission commits its fenced SQL row. Store only artifact identity and scalar scheduling metadata in Postgres; workers and certifiers re-authenticate the artifact before use. Backfill existing rows transactionally, then compact the table and expose capacity evidence.

**Tech Stack:** Python 3.12, psycopg, Alembic, boto3-compatible R2, pytest/Testcontainers.

## Global Constraints

- R2 PUT + HEAD + SHA-256 authentication precedes every SQL receipt/reference.
- No M1 code accepts historical JSONB as a fallback once the migration is live.
- Legacy L1/L2 is not a source, fallback, or deployment dependency.
- All behavior changes are test-first; no destructive production SQL runs until the preflight manifest is approved by the command itself.

---

### Task 1: Canonical Quote-input artifact

**Files:**
- Modify: `src/polyarb/control_plane/quote_artifact.py`
- Modify: `tests/m1-perception/test_quote_batch_artifact.py`

**Interfaces:**
- Produces: `QuoteBatchInputArtifact.from_spec(spec) -> QuoteBatchInputArtifact`
- Produces: `parse_quote_batch_input_bytes(payload) -> QuoteBatchSpec`

- [ ] **Step 1: Write the failing tests**

```python
def test_quote_input_artifact_round_trips_the_fenced_batch_spec() -> None:
    spec = QuoteBatchSpec.from_tokens(...)
    artifact = QuoteBatchInputArtifact.from_spec(spec)
    assert parse_quote_batch_input_bytes(artifact.payload) == spec
    assert artifact.sha256 == hashlib.sha256(artifact.payload).hexdigest()
```

- [ ] **Step 2: Run the focused test and observe RED**

Run: `uv run pytest tests/m1-perception/test_quote_batch_artifact.py -q`

Expected: FAIL because the input artifact API does not exist.

- [ ] **Step 3: Implement canonical serializer/parser and R2 upload helper**

The artifact key must be content-addressed below `m1/quote-inputs/`; use the
existing Quote output PUT/HEAD digest verification pattern.

- [ ] **Step 4: Run focused tests and commit**

Run: `uv run pytest tests/m1-perception/test_quote_batch_artifact.py -q`

Expected: PASS.

### Task 2: Slim SQL input contract

**Files:**
- Create: `alembic/versions/019_m1_quote_input_artifacts.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Create: `tests/alembic/test_019.py`

**Interfaces:**
- `m1_quote_batch_inputs`: `input_artifact_key TEXT`, `input_artifact_digest TEXT`, `leg_count BIGINT`; no `token_ids` or `legs` columns after upgrade.
- Produces: `quote_batch_input(job_key) -> QuoteBatchInputReference`.

- [ ] **Step 1: Write failing migration and control-plane tests**

```python
assert row["input_artifact_digest"] == artifact.sha256
assert "legs" not in table_columns
assert "token_ids" not in table_columns
```

- [ ] **Step 2: Run the focused tests and observe RED**

Run: `uv run pytest tests/alembic/test_019.py tests/m1-perception/test_control_plane_postgres.py -q`

Expected: FAIL because revision 019 and `QuoteBatchInputReference` are absent.

- [ ] **Step 3: Implement additive reference columns, verified cutover, then JSONB drop**

The migration must reject a row that lacks an R2-authenticated backfill marker;
the production operator performs the backfill before applying the destructive
column drop revision.

- [ ] **Step 4: Run focused tests and commit**

Run: `uv run pytest tests/alembic/test_019.py tests/m1-perception/test_control_plane_postgres.py -q`

Expected: PASS.

### Task 3: Worker and projection recovery path

**Files:**
- Modify: `src/polyarb/control_plane/quote_admission.py`
- Modify: `src/polyarb/control_plane/quote_worker.py`
- Modify: `src/polyarb/control_plane/opportunity_worker.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_opportunity_projection.py`

- [ ] **Step 1: Write failing tests**

```python
def test_quote_worker_rejects_input_artifact_digest_mismatch() -> None:
    with pytest.raises(QuoteBatchWorkerError, match="input-artifact-digest"):
        asyncio.run(worker.run_once())
```

Also add a crash-after-R2-input-upload test proving retry reuses the same
content-addressed input object and commits exactly one SQL reference.

- [ ] **Step 2: Run focused tests and observe RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_opportunity_projection.py -q`

Expected: FAIL because workers still deserialize Postgres JSONB.

- [ ] **Step 3: Implement R2-only reads after admission**

Admission uploads/authenticates the canonical input artifact; worker and
certifier parse it and compare its identity to the leased SQL row before any
external quote request or publication.

- [ ] **Step 4: Run focused tests and commit**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_opportunity_projection.py -q`

Expected: PASS.

### Task 4: Production backfill, compaction and capacity observability

**Files:**
- Create: `scripts/m1_quote_input_compaction.py`
- Modify: `Makefile`
- Modify: `src/polyarb/control_plane/postgres.py`
- Create: `tests/m1-perception/test_quote_input_compaction.py`

**Interfaces:**
- `make control-plane-quote-input-preflight` prints a read-only manifest with row count, relation size and expected R2 keys.
- `make control-plane-quote-input-backfill enable=1` uploads/authenticates only missing content-addressed inputs and records a report.
- `make control-plane-capacity-status` returns relation sizes plus 80% warning/fail state.

- [ ] **Step 1: Write failing command and capacity-status tests**

```python
assert report["would_compact_rows"] == 1848
assert report["threshold_status"] == "warn"
```

- [ ] **Step 2: Run tests and observe RED**

Run: `uv run pytest tests/m1-perception/test_quote_input_compaction.py -q`

Expected: FAIL because no preflight/backfill command exists.

- [ ] **Step 3: Implement guarded command sequence**

Preflight is read-only. Backfill requires explicit `enable=1`; it must not
delete rows. A separately guarded compaction command requires a verified
backfill manifest and runs `VACUUM FULL` only during a worker maintenance
window after all processes are stopped.

- [ ] **Step 4: Run full verification and commit**

Run: `uv run pytest tests/m1-perception/test_quote_batch_artifact.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_opportunity_projection.py tests/m1-perception/test_quote_input_compaction.py tests/alembic/test_019.py -q && make planning-status`

Expected: PASS with no planning drift.
