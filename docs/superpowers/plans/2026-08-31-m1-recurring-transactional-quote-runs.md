# M1 Recurring Transactional Quote Runs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let one certified Structure generation produce recurring immutable Quote and Opportunity generations without weakening lineage, freshness, or recovery guarantees.

**Architecture:** Migration 038 adds an authoritative Quote-to-Structure lineage relation. A serialized cadence admitter creates ordinary run-scoped `quote-admit` jobs, while existing workers and pointer CAS publish complete Quote and Opportunity successors. Qualification and projection readers join lineage instead of parsing a Quote digest.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL 16, Alembic, asyncio, pytest, Ruff, Pyright, uv

## Global Constraints

- Keep `quote:<64 lowercase hex>` as the external Quote generation shape.
- Use `cadence_seconds=300`; keep the hard Quote/Opportunity qualification bound at 900 seconds.
- Never overlap unfinished quote-admit, quote-batch, quote-certify, or opportunity-certify work.
- Keep source windows non-overlapping and do not change Gamma pagination or the 900-second executable-data bound.
- Preserve all existing lease fences, task-local facts, retry circuits, alerts, and commissioning node names.
- Build only through command-scoped `DOCKER_CONTEXT=orbstack`; never mutate the global Docker context.
- Every task follows RED, observed failure, minimal GREEN, focused verification, Summary, then commit.

---

### Task 1: Authoritative Quote generation lineage

**Files:**
- Create: `alembic/versions/038_m1_recurring_quote_generations.py`
- Modify: `src/polyarb/control_plane/models.py`
- Modify: `tests/alembic/test_control_plane_schema_contract.py`
- Create: `tests/alembic/test_038.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`

**Interfaces:**
- Produces: `QuoteRunIdentity.create(structure_generation_key, universe_hash, cadence_seconds, cadence_bucket)` and `QuoteBatchSpec.quote_generation_digest`.
- Produces table: `m1_quote_generation_inputs(generation_key, structure_generation_key, universe_hash, cadence_seconds, cadence_bucket, admitted_at)`.

- [ ] **Step 1: Write failing model tests**

```python
identity = QuoteRunIdentity.create(
    structure_generation_key=f"structure:{'a' * 64}",
    universe_hash="b" * 64,
    cadence_seconds=300,
    cadence_bucket=5960404,
)
assert identity.generation_key == f"quote:{identity.digest}"
assert identity == QuoteRunIdentity.create(
    structure_generation_key=f"structure:{'a' * 64}",
    universe_hash="b" * 64,
    cadence_seconds=300,
    cadence_bucket=5960404,
)
assert identity != replace(identity, cadence_bucket=5960405)
```

- [ ] **Step 2: Run the focused model test and observe `ImportError: QuoteRunIdentity`**

Run: `uv run pytest -q tests/m1-perception/test_transactional_quote_worker.py -k quote_run_identity`

- [ ] **Step 3: Implement canonical identity and run-scoped batch identity**

```python
@dataclass(frozen=True, slots=True)
class QuoteRunIdentity:
    structure_generation_key: str
    universe_hash: str
    cadence_seconds: int
    cadence_bucket: int

    @property
    def digest(self) -> str:
        payload = {"cadence_bucket": self.cadence_bucket, "cadence_seconds": self.cadence_seconds,
                   "policy_version": "transactional-quote-run-v1",
                   "structure_generation_key": self.structure_generation_key,
                   "universe_hash": self.universe_hash}
        return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
```

Add `quote_generation_digest` to `QuoteBatchSpec`; default it to
`structure_receipt_digest` only in the legacy constructor. Make `generation_key`, `job_key`,
and `input_identity` use the quote generation digest while retaining the exact Structure digest.

- [ ] **Step 4: Write and apply migration 038 tests**

Assert exact PK/FKs/checks, legacy backfill from matching Quote/Structure manifests, no write
grant to the qualification capability, and exact SELECT grant on the new relation.

Run: `uv run pytest -q tests/alembic/test_038.py tests/alembic/test_control_plane_schema_contract.py`

- [ ] **Step 5: Run focused static gates and commit**

Run: `uv run ruff check alembic/versions/038_m1_recurring_quote_generations.py src/polyarb/control_plane/models.py tests/alembic/test_038.py tests/m1-perception/test_transactional_quote_worker.py`

Commit: `feat(m1): add authoritative quote generation lineage`

### Task 2: Run-scoped Quote admission and publication

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/quote_admission.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_quote_admission.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`

**Interfaces:**
- Consumes: `QuoteRunIdentity`, `QuoteBatchSpec.quote_generation_digest`.
- Produces: `PostgresControlPlane.admit_due_quote_refresh(cadence_seconds: int, now: datetime) -> SourceAdmissionDecision`.
- Changes: `quote_admission_input(job_key)` returns `(structure_generation_key, bundle_key, bundle_digest, quote_generation_key)`.

- [ ] **Step 1: Write failing PostgreSQL tests for two runs on one Structure**

Create legacy run `Q0`, then call admission in two distinct 300-second buckets. Assert two
different `quote:<digest>` generations map to the same Structure and have disjoint job keys.
Assert a repeat in the same bucket returns `busy` without new rows.

- [ ] **Step 2: Observe the missing admission method**

Run: `uv run pytest -q tests/m1-perception/test_control_plane_postgres.py -k 'due_quote_refresh or recurring_quote'`

- [ ] **Step 3: Implement one serialized admission transaction**

Use advisory key `m1:quote-generation-admission`. Under the lock, reject unfinished
`quote-admit`, `quote-batch`, `quote-certify`, and `opportunity-certify`; load current lineage
and Structure bundle input; compute the bucket identity; insert the relation, quote-admit job,
and admission input atomically.

- [ ] **Step 4: Thread the intended Quote generation through the existing worker**

Pass `quote_generation_digest=quote_generation_key.removeprefix("quote:")` into
`quote_batches_from_legs`, persist the relation before batch jobs, and make certification verify
every batch's Quote-run and Structure identities against that row before pointer CAS.

- [ ] **Step 5: Prove pointer conflict, retry, and committed replay behavior**

Run: `uv run pytest -q tests/m1-perception/test_transactional_quote_admission.py tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_control_plane_postgres.py -k 'quote or publication_pointer'`

- [ ] **Step 6: Commit**

Commit: `feat(m1): admit recurring immutable quote runs`

### Task 3: Serialize Structure handoff with recurring admission

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_runtime_coverage.py`

**Interfaces:**
- Consumes: advisory key `m1:quote-generation-admission` and Task 2 admission API.
- Produces: one total order for initial-Structure and recurring-Quote admission.

- [ ] **Step 1: Write a real-PostgreSQL concurrency regression**

Block Structure certification immediately before successor admission, race a due recurring
admission, release both, and assert there is only one runnable Quote lineage and no doomed old
Structure refresh generation.

- [ ] **Step 2: Observe both transactions currently admit work**

Run: `uv run pytest -q tests/m1-perception/test_control_plane_postgres.py -k quote_admission_race`

- [ ] **Step 3: Acquire the shared advisory lock in Structure certification**

Take `pg_advisory_xact_lock(hashtext('m1:quote-generation-admission'))` before freezing the
pointer predecessor and creating the immediate `quote-admit` successor. After lock acquisition,
recheck current Structure/Quote lineage before either transaction admits work.

- [ ] **Step 4: Verify race and stale-owner coverage**

Run: `uv run pytest -q tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_runtime_coverage.py -k 'quote_admission_race or stale_owner or pointer_conflict'`

- [ ] **Step 5: Commit**

Commit: `fix(m1): serialize structure and quote admission`

### Task 4: Lineage-based Opportunity and qualification reads

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/qualification_service.py`
- Modify: `tests/m1-perception/test_transactional_opportunity_projection.py`
- Modify: `tests/m1-perception/test_control_plane_qualification_service.py`
- Modify: `tests/m1-perception/test_control_plane_db_role_contract.py`

**Interfaces:**
- Consumes: `m1_quote_generation_inputs`.
- Produces: exact Structure generation from relation joins; no Quote-digest substring inference.

- [ ] **Step 1: Write failing same-Structure multi-Quote projection tests**

Publish `Q0`, project it, publish `Q1` with the same Structure, then assert Q1 is independently
projectable and `opportunity:current` moves from Q0 to Q1 exactly once.

- [ ] **Step 2: Write failing qualification lineage tests**

Use a Quote run digest different from its Structure digest. Assert Structure freshness resolves
the related Structure manifest and malformed/missing lineage produces `evidence.gap`.

- [ ] **Step 3: Replace inference joins with authoritative relation joins**

Change `current_quote_projection_inputs` and the Structure freshness query to join
`pointer.generation_key = m1_quote_generation_inputs.generation_key`, then join the exact
`structure_generation_key`. Remove `substr(pointer.generation_key, 7)` from these paths.

- [ ] **Step 4: Verify chain-truth and least privilege**

Run: `uv run pytest -q tests/m1-perception/test_transactional_opportunity_projection.py tests/m1-perception/test_control_plane_qualification_service.py tests/m1-perception/test_control_plane_db_role_contract.py`

- [ ] **Step 5: Commit**

Commit: `fix(m1): read quote lineage from durable relation`

### Task 5: Coordinator cadence and operator visibility

**Files:**
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `src/polyarb/control_plane/scheduler.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`
- Modify: `tests/m1-perception/test_control_plane_scheduler.py`
- Modify: `tests/m1-perception/test_control_plane_cli.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Modify: `Makefile`

**Interfaces:**
- Produces: `TransactionalQuoteRefreshAdmitter.run_once()`.
- Produces Make target: `make quote-refresh-admit-once enable=1` for bounded operator diagnosis.

- [ ] **Step 1: Write failing scheduler cadence tests**

Assert the quote refresh admitter is a coordinator lane, uses 300 seconds, can admit while a
source traversal is active, and cannot admit while any Quote/Opportunity successor is unfinished.

- [ ] **Step 2: Implement the lightweight admitter and scheduler lane**

The worker calls only `admit_due_quote_refresh`; it performs no provider/R2 work. Insert it before
`quote-admit` in the coordinator worker list and preserve non-overlapping local lane execution.

- [ ] **Step 3: Add the Make operator entry**

Expose one explicit read/transaction diagnostic command with `enable=1`; do not add deploy,
Machine, Docker, wallet, or recovery authority.

- [ ] **Step 4: Expose truthful status**

Add current Quote lineage, parent Structure generation, cadence bucket, and next eligibility hint
to the existing control-plane snapshot. Keep unavailable/conflicting lineage fail-closed.

- [ ] **Step 5: Verify scheduler, CLI and Make contracts**

Run: `uv run pytest -q tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_control_plane_scheduler.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -k 'quote_refresh or recurring_quote'`

- [ ] **Step 6: Commit**

Commit: `feat(m1): schedule recurring quote refresh admission`

### Task 6: Exact-image commissioning and production closure

**Files:**
- Modify: `src/polyarb/control_plane/production_commissioning_disposable.py`
- Modify: `tests/m1-perception/test_production_commissioning_harness.py`
- Modify: `docs/dev/chaos-toolkit.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-264-SUMMARY.md`
- Create: `docs/learning/105-市场全集与可执行报价必须使用两个时钟.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/threads/market-observation-architecture.md`

**Interfaces:**
- Consumes: all Tasks 1–5.
- Produces: exact same-Structure `Q0 -> Q1` production proof and qualification-resume evidence.

- [ ] **Step 1: Add deterministic interruption and duplicate-admission attacks**

Commission a stopped coordinator after durable refresh admission, a repeated same-bucket admit,
and a successor retry. Verify one generation relation, one terminal Quote manifest, one pointer
move, one Opportunity, and no stale-owner write.

- [ ] **Step 2: Run focused and complete gates without an arbitrary outer timeout**

Run: `uv run pytest -q tests/alembic/test_038.py tests/m1-perception/test_production_commissioning_harness.py`

Run: `make planning-status`

Run: `make climb-cycle hypothesis=H-061`

- [ ] **Step 3: Build one exact OrbStack release image**

Run: `make runtime-image-build image_tag=m1-runtime-v35-$(git rev-parse --short HEAD)`

Verify the emitted invocation carries `DOCKER_CONTEXT=orbstack` and exact OCI revision. Do not
run `docker context use` and do not install/start Colima.

- [ ] **Step 4: Roll all authorized Machines on the same image digest**

Use existing exact rollout/preflight commands and preserve Machine IDs. Verify migrations,
release/config identity, current watchdog health, zero expired leases and zero open circuits.

- [ ] **Step 5: Capture the natural same-Structure successor proof**

Record Q0 and Q1 with equal `structure_generation_key`, different Quote generation keys,
publication gap within 900 seconds, corresponding Opportunity pointers, matching qualification
freshness facts, same epoch ID, and increasing `eligible_seconds`.

- [ ] **Step 6: Update teaching, Summary, JOURNAL and commit evidence**

Commit: `docs(m1): record recurring quote production proof`

## Self-review

- Spec coverage: Tasks 1–6 cover identity, migration/backfill, durable admission, race ordering,
  publication, lineage reads, scheduling, operator surface, recovery, commissioning and rollout.
- Placeholder scan: no deferred implementation marker is present; every task names a failing test,
  implementation boundary, verification command and commit.
- Type consistency: `QuoteRunIdentity`, `QuoteBatchSpec.quote_generation_digest`,
  `admit_due_quote_refresh`, and the four-field `quote_admission_input` contract are used
  consistently across their producer and consumer tasks.
