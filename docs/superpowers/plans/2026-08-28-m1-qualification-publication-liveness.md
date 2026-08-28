# M1 Qualification Publication Liveness Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:test-driven-development task-by-task. This plan is executed
> inline because the active worktree contains user-owned dirty files.

**Goal:** Let the transactional Structure chain finish large certification
jobs and feed qualification from the Structure generation actually backing the
current Quote publication.

**Architecture:** Preserve the common runtime deadlines for seven job types,
but derive a one-hour absolute ceiling for `structure-certify`. Replace the
qualification service's legacy Structure pointer query with a fail-closed join
from the canonical `quote:current` identity to its certified Structure
manifest.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL 16, pytest, uv

## Global Constraints

- Heartbeat and progress-stall deadlines remain unchanged.
- The Structure certifier retains an absolute attempt ceiling.
- Qualification remains fail-closed if any publication link is missing.
- No schema, permission, recovery-mode, fault, wallet, signing, order, balance,
  or trade mutation is introduced.
- All commands continue through `uv` or existing Makefile targets.

---

### Task 1: Structure certifier absolute deadline

**Files:**

- Modify: `src/polyarb/control_plane/runtime_store.py`
- Test: `tests/m1-perception/test_transactional_runtime_coverage.py`

**Interfaces:**

- Produces: `runtime_deadline_profile(job_type: str, lease_seconds: int) -> RuntimeDeadlineProfile`
- Consumes: `start_runtime_attempt_cursor(..., job_type, lease_seconds)`

- [x] **Step 1: Write the failing profile test**

```python
def test_structure_certifier_gets_bounded_long_attempt_without_weakening_liveness() -> None:
    certifier = runtime_deadline_profile("structure-certify", 30)
    normalizer = runtime_deadline_profile("structure-normalize", 30)
    assert (certifier.heartbeat_seconds, certifier.progress_seconds) == (10, 30)
    assert certifier.attempt_seconds == 3_600
    assert normalizer.attempt_seconds == 300
```

- [x] **Step 2: Verify RED**

Run:

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py::test_structure_certifier_gets_bounded_long_attempt_without_weakening_liveness -q
```

Expected: import failure because `runtime_deadline_profile` does not exist.

- [x] **Step 3: Implement the job-specific profile**

Replace `_derived_profile(lease_seconds)` with a job-aware helper. Keep the
existing heartbeat/progress derivation and set `attempt_multiplier = 120` only
for `structure-certify`, otherwise `10`. Pass `job_type` from
`start_runtime_attempt_cursor`.

- [x] **Step 4: Verify GREEN and focused regressions**

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py::test_structure_certifier_gets_bounded_long_attempt_without_weakening_liveness tests/m1-perception/test_control_plane_runtime_models.py tests/m1-perception/test_control_plane_reconciler.py -q
```

Expected: all selected tests pass.

### Task 2: Qualification Structure freshness truth chain

**Files:**

- Modify: `src/polyarb/control_plane/qualification_service.py`
- Test: `tests/m1-perception/test_control_plane_qualification_service.py`

**Interfaces:**

- Consumes: `quote:current` and `m1_generation_manifests`
- Produces: the unchanged bounded Structure freshness row contract

- [x] **Step 1: Write the failing SQL contract assertions**

Capture the first freshness SELECT and require all of:

```python
assert "pointer.pointer_key = 'quote:current'" in structure_query
assert "pointer.generation_key ~ '^quote:[0-9a-f]{64}$'" in structure_query
assert "'structure:' || substr(pointer.generation_key, 7)" in structure_query
assert "m1_quote_admission_inputs" not in structure_query
assert "pointer.pointer_key = 'structure:current'" not in structure_query
```

- [x] **Step 2: Verify RED**

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_service.py::test_postgres_freshness_observations_use_bounded_wrapper -q
```

Expected: failure because the current query still names
`structure:current` and does not derive Structure identity from Quote.

- [x] **Step 3: Implement the fail-closed transactional join**

Query `quote:current`, derive
`'structure:' || substr(pointer.generation_key, 7)`, and join the certified
Structure manifest without touching an ungranted relation. Require the exact
lowercase 64-hex Quote generation grammar for both Structure and Quote
freshness. Keep the returned columns and bounded wrapper call unchanged.

- [x] **Step 4: Verify GREEN and focused regressions**

```bash
uv run pytest tests/m1-perception/test_control_plane_qualification_service.py tests/m1-perception/test_control_plane_db_role_contract.py -q
```

Expected: all selected tests pass and the scoped qualification role still has
every required read.

### Task 3: Closure, teaching, and production proof

**Files:**

- Create: `docs/learning/92-资格计时为何会被健康任务打断.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-208-SUMMARY.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `docs/learning/00-INDEX.md`

- [x] **Step 1: Run local completion gates**

```bash
uv run pytest tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_control_plane_qualification_service.py tests/m1-perception/test_control_plane_reconciler.py tests/m1-perception/test_control_plane_db_role_contract.py -q
uv run ruff check src/polyarb/control_plane/runtime_store.py src/polyarb/control_plane/qualification_service.py tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_control_plane_qualification_service.py
uv run ruff format --check src/polyarb/control_plane/runtime_store.py src/polyarb/control_plane/qualification_service.py tests/m1-perception/test_transactional_runtime_coverage.py tests/m1-perception/test_control_plane_qualification_service.py
make planning-status
make climb-check
```

Expected: every command exits zero.

- [x] **Step 2: Record the mental model and durable planning state**

Explain the distinction between lease, heartbeat, progress, and absolute
attempt deadlines; document why qualification follows consumed publication
truth rather than a compatibility pointer; record exact test evidence and the
pending production rollout boundary.

- [x] **Step 3: Commit the locally verified repair**

```bash
git add src/polyarb/control_plane/runtime_store.py \
  src/polyarb/control_plane/qualification_service.py \
  tests/m1-perception/test_transactional_runtime_coverage.py \
  tests/m1-perception/test_control_plane_qualification_service.py \
  docs/superpowers/plans/2026-08-28-m1-qualification-publication-liveness.md \
  docs/learning/92-资格计时为何会被健康任务打断.md \
  docs/learning/00-INDEX.md \
  .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-208-SUMMARY.md \
  .planning/workstreams/m1-perception/STATE.md \
  .planning/JOURNAL.md \
  .planning/threads/market-observation-architecture.md
git commit -m "fix(05.6-208): restore qualification publication liveness"
```

- [ ] **Step 4: Deploy the verified image and observe production**

Use the existing image-only rollout path. Preserve all Machine IDs and
non-image configuration. Prove the certifier passes progress 300 in one lease,
reaches `job.succeeded`, publishes a fresh Quote and opportunity projection,
and opens an accumulating qualification epoch. No certificate can be claimed
until the full 86,400 seconds are observed.
