# M1 Transactional Quote Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven-development task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn every certified transactional Structure generation into bounded, restart-safe Quote work without SQLite authority.

**Architecture:** Structure certification atomically releases one fenced `quote-admit` intent. An independent worker reads its immutable R2 bundle, derives canonical active neg-risk YES-token legs, then fences its lease while creating existing Quote batch and certifier jobs. The scheduler runs this worker before Quote collection.

**Tech Stack:** Python 3.12, psycopg, Alembic, R2, pytest/Testcontainers.

## Global Constraints

- Additive migrations only; no SQLite reads, pointer changes, cloud deployment, or legacy shutdown.
- Durable effects are fenced by `JobLease.lease_epoch`; process execution remains at-least-once.
- Quote legs are derived only from an authenticated immutable Structure bundle.
- Existing Quote batch/certifier and current-pointer semantics stay unchanged.

---

### Task 1: Release Quote admission with Structure certification

**Files:**
- Create: `alembic/versions/012_m1_transactional_quote_admission.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/alembic/test_012.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:** `quote_admission_input(job_key) -> (generation_key, bundle_key, bundle_digest)`; successful `certify_structure_generation` atomically creates `{generation_key}:quote-admit`.

- [ ] Write a failing real-Postgres test that certifies a complete Structure generation, claims `quote-admit`, and asserts its immutable bundle identity.
- [ ] Run `uv run pytest tests/m1-perception/test_control_plane_postgres.py -q -k quote_admit`; expect missing input/job behavior.
- [ ] Add migration 012 and immutable `m1_quote_admission_inputs`; in the certifier fence transaction enqueue the matching job and reject conflicting replay identity.
- [ ] Run `uv run pytest tests/alembic/test_012.py tests/m1-perception/test_control_plane_postgres.py -q -k 'quote_admit or structure_cert'`; expect PASS.
- [ ] Commit with `feat(05.6-118): release Quote admission from Structure certification`.

### Task 2: Derive immutable Quote batches from the authenticated bundle

**Files:**
- Create: `src/polyarb/control_plane/quote_admission.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/m1-perception/test_transactional_quote_admission.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:** `TransactionalQuoteAdmitter.run_once()` claims `quote-admit`, parses the R2 bundle at its recorded digest, derives sorted `QuoteBatchLeg` values and a canonical universe digest, then atomically admits existing Quote batches/certifier under the lease fence.

- [ ] Write failing tests for active/open neg-risk leg derivation and tampered R2 bundle retryability.
- [ ] Run `uv run pytest tests/m1-perception/test_transactional_quote_admission.py -q`; expect import failure.
- [ ] Implement only authenticated bundle reads; require market/event/condition/YES-token identities and filter inactive, closed, or non-neg-risk markets. A retryable failure writes no Quote batch input.
- [ ] Run `uv run pytest tests/m1-perception/test_transactional_quote_admission.py tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_control_plane_postgres.py -q`; expect PASS.
- [ ] Commit with `feat(05.6-119): derive Quote jobs from certified Structure`.

### Task 3: Schedule and document the Quote bridge

**Files:**
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Modify: `docs/learning/64-事务型云端控制面.md`
- Test: `tests/m1-perception/test_transactional_control_plane_scheduler.py`

- [ ] Write failing scheduler rotation test that exposes the `quote-admit` bounded turn before Quote batches.
- [ ] Run `uv run pytest tests/m1-perception/test_transactional_control_plane_scheduler.py -q`; expect constructor failure.
- [ ] Construct the bridge only in the worker scheduler, retain API Postgres-only, and document the Structure-to-Quote durable chain.
- [ ] Run scheduler/CLI tests, `make docs-m1-check`, and `make planning-status`; expect PASS.
- [ ] Commit with `feat(05.6-120): schedule transactional Quote admission`.

## Self-review

The design introduces no direct certifier-to-CLOB calls: certification releases durable intent, the bridge owns authenticated R2 input, and existing Quote workers own CLOB calls. No change authorizes a cloud deployment or pointer switch.
