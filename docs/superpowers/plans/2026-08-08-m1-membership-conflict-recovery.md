# M1 Membership Conflict Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline TDD task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Ensure an authenticated, internally contradictory frozen Structure publication fails once with immutable evidence and automatically yields to a fresh natural window.

**Architecture:** Keep event/market source disagreement fail-closed. Reuse the existing atomic publication-contract supersession transaction to retire the unpublished publication, its building snapshot, and its source window under a distinct membership-conflict reason. The scheduler then resumes its existing natural collection path; no stored source payload or serving generation is altered.

**Tech Stack:** Python 3.12, SQLite, asyncio, pytest.

## Global Constraints

- Do not relax membership validation or mutate frozen source/generation rows.
- Preserve the old serving pointer and every failed publication/window row.
- Retire only an unpublished `writing` publication for an authenticated `StructureMembershipInvalidError`.
- Reuse normal next-window collection; do not force a snapshot or pointer switch.

---

### Task 1: Atomic membership-conflict retirement

**Files:**

- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**

- Consumes: `window_id`, `now_ms`, and authenticated failure reason `publication-membership-invalid`.
- Produces: idempotent failed publication/window, failed invalid snapshot, preserved generation rows, and a successor-eligible source state.

- [ ] **Step 1: Write a failing test** that creates a writing publication with an event-member/market status mismatch, drives certification to `StructureMembershipInvalidError`, invokes the recovery primitive, and asserts:
  - publication/window are `failed` with `publication-membership-invalid`;
  - building snapshot becomes failed/non-serving;
  - current generation and generated rows remain unchanged;
  - a new `begin_or_resume_structure_sync` call creates a different open window.
- [ ] **Step 2: Run the node**

  Run: `uv run pytest tests/m1-perception/test_structure_generation_publication.py::test_membership_invalid_publication_is_retired_for_fresh_window -q`

  Expected: FAIL because no membership-conflict retirement primitive exists.

- [ ] **Step 3: Add one transactional store method** beside `reconcile_structure_publication_contract` with an exact `writing` precondition. It updates only the publication, source window, and building snapshot in the same writer transaction and is idempotent for an already failed record.
- [ ] **Step 4: Re-run the node**

  Expected: PASS, including immutable serving-generation assertions.

### Task 2: Producer recovery boundary

**Files:**

- Modify: `src/polyarb/perception/structure_sync.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**

- Consumes: `StructureMembershipInvalidError` raised from `run_structure_publication_slice`.
- Produces: a bounded superseded checkpoint after durable retirement, so the scheduler releases its producer slot and the next tick begins a natural successor window.

- [ ] **Step 1: Write a failing test** for `run_structure_sync_until_published` with a completed conflicting window. Assert it returns `StructurePublicationCheckpoint(stage="superseded")`, writes the exact failure reason, and the next worker batch begins a different window.
- [ ] **Step 2: Run the node**

  Run: `uv run pytest tests/m1-perception/test_structure_generation_publication.py::test_membership_invalid_publication_yields_natural_successor -q`

  Expected: FAIL by propagating `membership-invalid` and leaving the old writing publication active.

- [ ] **Step 3: Catch only `StructureMembershipInvalidError` around the publication slice, call the transactional retirement method, and return a superseded checkpoint. Do not catch other `ValueError` types.
- [ ] **Step 4: Re-run the node**

  Expected: PASS; an unchanged source mismatch remains fail-closed but no longer loops.

### Task 3: Verification and operational evidence

**Files:**

- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`

- [ ] **Step 1: Run focused publication and scheduler suites, changed-file Ruff, and `make planning-status`.
- [ ] **Step 2: Record that source conflict is terminal for its immutable window but recoverable at the scheduler level only through a fresh window.
- [ ] **Step 3: Create the plan SUMMARY, commit only task files, then deploy
  directly in R&D mode while recording the exact SHA and local/Fly verification
  evidence. Do not create a `DEPLOY_SHA_APPROVE` blocking step. Escalate only
  if a change crosses a risk boundary (funds, secrets, read mode, Quote,
  orders, pointer override, cleanup disablement, or irreversible production
  data mutation).
