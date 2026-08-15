# Sharded Transactional Structure Bundle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Structure source materialization and range execution bounded-memory, checkpointed, and restart-safe for large event-only windows.

**Architecture:** Materialization creates authenticated per-page component shards and stores progress with existing fenced checkpoint receipts. A final manifest commits the ordered shard set and becomes the sole source for Structure ranges. Range workers read only the shards named by their range.

**Tech Stack:** Python 3.12, psycopg/Postgres transactions, Cloudflare R2/S3 client, pytest, existing M1 control-plane models.

## Global Constraints

- Staging-only deployment; do not mutate production pointers or credentials.
- Use `uv`, `apply_patch`, TDD, Makefile command entrances, and a plan SUMMARY for every committed plan.
- Every R2 artifact must be content-addressed and HEAD-verified before a Postgres receipt refers to it.
- Preserve v1 and v2 artifact readers for existing sealed evidence.

---

### Task 1: Define authenticated shard and manifest artifacts

**Files:**
- Modify: `src/polyarb/control_plane/structure_artifact.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_bundle.py`

**Interfaces:**
- Produces `StructureShardArtifact`, `StructureShardManifestArtifact`, canonical
  encode/parse helpers, and R2 upload verification helpers.
- Consumes existing `StructureBundleIdentity` and component names.

- [ ] Write failing tests that a shard has one component/ordinal/digest, rejects
  noncanonical rows, and a manifest rejects missing/duplicate ordinal receipts.
- [ ] Run the focused tests and observe missing symbols.
- [ ] Implement immutable canonical shard/manifest encoding and digest-keyed R2
  upload/HEAD verification.
- [ ] Re-run focused artifact tests and Ruff.
- [ ] Commit `feat(05.6): add authenticated Structure shards`.

### Task 2: Fence materializer page-batch checkpoint progress

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_materializer.py`

**Interfaces:**
- Produces `structure_source_materializer_shards(window_key)` ordered only from
  authenticated checkpoint receipts and `checkpoint_structure_materializer_batch`.
- Consumes a `JobLease`, page ordinal interval, shard receipts, and current time.

- [ ] Write a real-Postgres RED test proving a batch checkpoint releases the
  lease, takeover resumes at the successor page, and a stale lease cannot append
  another shard.
- [ ] Write a materializer RED test using more pages than one batch and asserting
  one bounded batch per `run_once`.
- [ ] Implement the fenced receipt query/checkpoint methods and the worker's
  page-batch selection/upload/checkpoint path.
- [ ] Re-run both focused suites, Ruff, and diff check.
- [ ] Commit `feat(05.6): checkpoint Structure source shards`.

### Task 3: Commit a final manifest and plan shard-backed ranges

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/structure_shadow.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_bundle.py`

**Interfaces:**
- Produces `gamma-source-window-events-v3-sharded` manifest identity and range
  specs carrying manifest/shard references.
- Consumes the complete ordered checkpoint receipt set.

- [ ] Write failing tests for finalization only after every expected page shard
  exists, deterministic manifest digest, and no pointer mutation on a missing
  shard.
- [ ] Implement manifest admission under the existing source-window and lease
  fence, with range boundaries over shard references.
- [ ] Re-run source/postgres focused suites and Ruff.
- [ ] Commit `feat(05.6): admit sharded Structure manifests`.

### Task 4: Read only named shards in Structure range workers

**Files:**
- Modify: `src/polyarb/control_plane/structure_worker.py`
- Modify: `src/polyarb/control_plane/structure_artifact.py`
- Modify: `tests/m1-perception/test_transactional_structure_worker.py`

**Interfaces:**
- Consumes authenticated manifest/range shard references.
- Produces the unchanged normalized range artifact and certification receipt.

- [ ] Write a failing test whose manifest has multiple shards and assert the
  worker reads only the shard(s) in its claimed range.
- [ ] Implement manifest/shard digest validation and bounded range loading while
  retaining legacy monolithic bundle support.
- [ ] Run focused worker/certifier tests and Ruff.
- [ ] Commit `feat(05.6): normalize Structure from bounded shards`.

### Task 5: Staging restart and end-to-end acceptance

**Files:**
- Modify: `Makefile`
- Modify: `docs/learning/71-事件内嵌Structure源.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-159-SUMMARY.md`

- [ ] Add a Makefile target to inspect shard manifest/checkpoint progress without
  printing secrets.
- [ ] Run all source/materializer/Postgres/Structure/Quote focused suites.
- [ ] Deploy only to `polyarb-control-worker-staging`, force one restart after a
  shard checkpoint, and verify takeover, manifest, ranges, certifier, and Quote
  admission with zero publication pointers.
- [ ] Record concrete evidence in the learning document and SUMMARY; run
  `make planning-status` and commit.

## Self-Review

- Every stage preserves one durable source of truth: R2 artifacts plus fenced
  Postgres receipts.
- The plan explicitly removes both materializer and range-worker whole-window
  reads; no task depends on a VM-memory increase.
- Legacy source artifacts stay readable and new identity is explicit.
