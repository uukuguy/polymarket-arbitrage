# M1 Transactional Structure Generations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move Structure publication from a SQLite-owned multi-stage operation to recoverable, fenced Postgres jobs whose immutable source and output artifacts live in R2.

**Architecture:** The legacy Structure publication is initially only a shadow source. A bounded exporter seals one complete legacy publication into a canonical R2 bundle, then admits a `structure:<bundle-digest>` generation. Postgres owns page-range normalization/certification receipts and the terminal Structure pointer; workers recover only from immutable bundle/range inputs, never from SQLite's current pointer. Legacy and transactional manifests are compared before a separately authorized pointer switch.

**Tech Stack:** Python 3.12, psycopg 3, Alembic, Supabase Postgres, Cloudflare R2/S3, existing SQLite Structure publication reader, pytest/testcontainers.

## Global Constraints

- Preserve all six existing Structure components: events, event tags, memberships, group truth, markets, and validation issues.
- A frozen Structure bundle binds `publication_id`, `window_id`, `snapshot_id`, legacy comparison receipt digest, normalization contract version, component counts, and canonical row bytes.
- R2 PUT must be HEAD-verified by exact length and SHA-256 metadata before a Postgres receipt may reference it.
- A replacement worker reads only admitted R2/Postgres input; it does not rebuild a generation from SQLite or a newer legacy pointer.
- Normalization/certification jobs are fenced by `(job_key, lease_owner, lease_epoch)`; retries retain the same component/range identity.
- No transactional Structure pointer changes the legacy current generation until manifest parity and a separately authorized reversible switch pass.
- SQLite stays comparison/staging-only and no wallet, signing, or order capability is introduced.

### Task 1: Canonical frozen Structure bundle contract

**Files:**
- Create: `src/polyarb/control_plane/structure_artifact.py`
- Create: `tests/m1-perception/test_structure_generation_artifact.py`

**Interfaces:**
- `StructureBundleIdentity(publication_id, window_id, snapshot_id, comparison_receipt_digest, normalization_contract_version, component_counts)` validates one immutable legacy source identity.
- `canonical_structure_bundle_bytes(identity, components)` produces deterministic NDJSON with a metadata header and ordered component records.
- `StructureBundleArtifact.from_bytes(payload)` and `upload_structure_bundle_artifact(client, bucket, artifact)` use `structure-bundles/<sha256>/generation.ndjson` and PUT/HEAD authenticate it.

- [ ] **Step 1: Write failing canonicalization tests**

```python
def test_structure_bundle_is_ordered_and_content_addressed() -> None:
    identity = _identity()
    payload = canonical_structure_bundle_bytes(
        identity=identity,
        components={"events": ({"id": "b"}, {"id": "a"}), "markets": ()},
    )
    artifact = StructureBundleArtifact.from_bytes(payload)
    assert artifact.key == f"structure-bundles/{artifact.sha256}/generation.ndjson"
    assert b'"component":"events"' in payload
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/m1-perception/test_structure_generation_artifact.py -v`

Expected: FAIL because no Structure artifact module exists.

- [ ] **Step 3: Implement canonical bundle and R2 verification**

Reject missing/duplicate component rows and component-count mismatch. Serialize a first metadata line followed by component-tagged rows in declared component order; JSON uses sorted keys, compact separators, UTF-8 and `allow_nan=False`. Upload with SHA-256 metadata, then require HEAD content length and metadata to exactly equal local bytes.

- [ ] **Step 4: Run GREEN and commit**

Run: `uv run pytest tests/m1-perception/test_structure_generation_artifact.py -v && uv run ruff check src/polyarb/control_plane/structure_artifact.py tests/m1-perception/test_structure_generation_artifact.py`

Commit: `feat(05.6-84): add immutable Structure generation bundles` with `05.6-84-SUMMARY.md`.

### Task 2: Admit and recover fenced Structure generation work

**Files:**
- Modify: `alembic/versions/009_m1_transactional_control_plane.py`
- Modify: `src/polyarb/control_plane/models.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/alembic/test_009.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- `enqueue_structure_generation(identity, bundle_artifact, ranges, now)` creates deterministic `structure-normalize` jobs, `structure-certify` jobs and `structure-publish` job.
- `structure_range_spec(job_key)` returns the frozen bundle identity/key/digest and one component/key range.
- `record_structure_range(lease, artifact_key, artifact_digest, record_count, now)` is fenced and replay-authenticating.

- [ ] **Step 1: Write failing Postgres contracts**

```python
generation = control_plane.enqueue_structure_generation(
    identity=_identity(), bundle=_bundle(), ranges=(StructureRange("events", "", "m"),), now=now
)
lease = control_plane.claim_job(worker_id="a", job_types=("structure-normalize",), lease_seconds=30, now=now)
assert control_plane.structure_range_spec(lease.job_key).bundle_digest == _bundle().sha256
with pytest.raises(StaleLeaseError):
    control_plane.record_structure_range(expired_lease, ...)
```

- [ ] **Step 2: Run RED**

Run: `uv run pytest tests/alembic/test_009.py tests/m1-perception/test_control_plane_postgres.py -k structure_generation -v`

Expected: FAIL because Structure generation inputs/receipts are absent.

- [ ] **Step 3: Add additive schema and fenced repository methods**

Create `m1_structure_generation_inputs` (generation key, immutable bundle key/digest and identity JSON), `m1_structure_range_inputs` (job key, component, start/end cursor, input digest), and `m1_structure_range_receipts` (job key, output artifact key/digest, record count). Authenticate conflict replays field-for-field; stale leases make no durable change.

- [ ] **Step 4: Run GREEN and commit**

Run the focused migration/Postgres suite and changed-file Ruff. Commit `feat(05.6-85): admit fenced Structure generations` with `05.6-85-SUMMARY.md`.

### Task 3: Build shadow exporter and worker execution boundaries

**Files:**
- Create: `src/polyarb/control_plane/structure_shadow.py`
- Create: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `Makefile`
- Create: `tests/m1-perception/test_transactional_structure_worker.py`

**Interfaces:**
- `read_legacy_structure_bundle(db_path, publication_id)` reads one already-published legacy generation read-only and returns a canonical bundle source; it refuses partial/non-current/unauthenticated publications.
- `TransactionalStructureWorker.run_once()` claims one range, reads its admitted bundle from R2, writes a bounded output artifact, then records a fenced receipt.
- `make structure-control-plane-shadow-once enable=1 db_path=/data/state.db` can export/admit at most one legacy generation, never switching either pointer.

- [ ] **Step 1: Test source refusal, worker takeover and no pointer switch**

```python
with pytest.raises(StructureShadowRefusal):
    read_legacy_structure_bundle(db_path, partial_publication_id)
assert await worker.run_once() == "succeeded"
assert await replacement.run_once() == "recovered"
assert current_legacy_pointer(db_path) == before
assert current_control_pointer() is None
```

- [ ] **Step 2: Implement exporter and bounded worker**

Read SQLite only in the explicit shadow exporter. The worker parses the immutable R2 bundle, processes the admitted component/range, uploads authenticated output, records its receipt, and finishes. Use retryable completion for dependency failures and quarantine only malformed immutable bundle/contract mismatches.

- [ ] **Step 3: Add default-off Make/CLI operator contract**

Require `enable=1`, DSN and R2 configuration; without enable reject before DB/R2 connection. Return JSON including source identity, bundle digest, admitted job count, and `pointer_mutations=0`.

- [ ] **Step 4: Verify and commit**

Run worker/CLI/Postgres suites, Ruff, docs check, planning status and diff check. Commit `feat(05.6-86): shadow transactional Structure workers` with a summary.

### Task 4: Certify manifest parity and prepare reversible cutover

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/http/control_plane.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_control_plane_http.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/64-事务型Structure世代.md`

**Interfaces:**
- A `structure-certify` receipt covers every expected range, bundle input and ordered output artifact identities.
- `structure-publish` writes a control-plane-only `structure:current:shadow` pointer after parity with the legacy digest/counts; it cannot mutate the legacy pointer.
- `/perception/control-plane` reports Structure range states, shadow generation identity/parity state and the published legacy/current control pointer identities separately.

- [ ] **Step 1: Write parity and stale-writer tests**

Prove missing receipt, count/hash mismatch, bundle substitution and stale lease leave the shadow pointer absent. Prove complete matching receipts publish one shadow pointer; legacy pointer stays unchanged.

- [ ] **Step 2: Implement certification/publish gate**

Derive a manifest digest from the frozen input plus ordered receipts. Compare it to the legacy bundle digest and per-component counts before allowing only `structure:current:shadow`; record contract mismatch as quarantined and preserve the last pointer.

- [ ] **Step 3: Verify, teach and commit**

Run all focused Structure/Quote/control-plane tests, R2 fake contracts, HTTP tests, docs gate and planning status. Commit `feat(05.6-87): certify transactional Structure shadow generations` with summary and teaching docs.

## Separately Authorized Production Acceptance

1. Apply revision 009 to the designated control-plane authority and run the forward/reverse/forward test against a non-production DSN.
2. Perform two read-only shadow exports from the same certified legacy Structure identity; require identical bundle digests and `pointer_mutations=0`.
3. Run three complete transactional Structure shadow generations; for each compare all component counts, ordered manifest digest, source receipt identity and Quote admitted universe against the legacy generation.
4. Kill a Structure range worker after R2 upload and before receipt/finish; prove lease takeover, no duplicate durable receipt, old legacy/current truth continues, and durable control-plane evidence remains readable.
5. Authorize a reversible Structure pointer-only switch, prove rollback to the prior generation, then run Quote shadow on the new Structure identity.
6. Soak Structure, Quote, opportunity feed, Dashboard, control-plane reads and alerts across deploy/restart/cache-loss faults before declaring M1 production acceptance.
