# Structure Publication Contract Supersession Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Automatically retire an active Structure publication created under an incompatible normalization contract and let normal scheduling build a fresh source window and snapshot.

**Architecture:** Persist an explicit semantic contract version on every publication. Reconcile it at the publication boundary with one SQLite transaction that terminal-fails the publication, its unpublished snapshot, and its source window while leaving pointer and generation rows immutable. Represent expected supersession as a successful machine checkpoint so the scheduler records progress without changing the existing failure counter.

**Tech Stack:** Python 3.12, SQLite, Typer, asyncio scheduler, pytest, Ruff.

## Global Constraints

- Any mismatched or `NULL` active contract is incompatible; do not adopt or relabel legacy rows.
- Supersession uses exact reason `publication-contract-superseded` on publication and window.
- Publication, snapshot, and window terminal transitions occur in one transaction.
- Generation rows and `current_structure_generation` are immutable.
- Supersession neither increments nor resets the existing scheduler failure counter.
- Only a later certified fresh publication resets the counter through the existing success path.
- Do not deploy or mutate production data.

---

### Task 1: Persist and atomically reconcile the normalization contract

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`
- Test: `tests/m1-perception/test_schema_lockstep.py`

**Interfaces:**
- Produces: `STRUCTURE_NORMALIZATION_CONTRACT_VERSION: str`
- Produces: `SQLiteStore.reconcile_structure_publication_contract(window_id: str, current_version: str, now_ms: int) -> StructurePublicationContractReconciliation`
- Produces: `StructurePublicationContractReconciliation(publication_id: str, compatible: bool, superseded: bool)`

- [ ] **Step 1: Write failing persistence, migration, compatibility, supersession, and idempotence tests**

Create a production-shaped publication with pointer 845, unpublished snapshot 846, generation rows, `issues|done`, certification started, and legacy `NULL`/old version. Assert:

```python
result = store.reconcile_structure_publication_contract(
    window_id, STRUCTURE_NORMALIZATION_CONTRACT_VERSION, now_ms=900
)
assert result.superseded is True
assert publication_row == ("failed", "publication-contract-superseded")
assert snapshot_row == ("failed", 0, 0)
assert window_row == ("failed", "publication-contract-superseded")
assert current_pointer == 845
assert generation_rows_after == generation_rows_before
assert store.reconcile_structure_publication_contract(
    window_id, STRUCTURE_NORMALIZATION_CONTRACT_VERSION, now_ms=901
) == result
```

Add separate tests proving an exact-version publication resumes unchanged, a fresh publication stores the current version, and schema initialization adds a nullable version column to the pre-R220 table.

- [ ] **Step 2: Run the new tests and verify RED**

Run: `uv run pytest -q tests/m1-perception/test_structure_generation_publication.py -k 'contract_version or contract_supersession' tests/m1-perception/test_schema_lockstep.py`

Expected: FAIL because the column, constant, reconciliation result, and method do not exist.

- [ ] **Step 3: Add the minimal schema and transaction implementation**

Add the current version constant, the nullable DDL/migration column, and persist it from `begin_structure_publication`. Implement one `BEGIN IMMEDIATE` reconciliation transaction with exact-version compatibility and mismatch/`NULL` compare-and-set updates. Require active publication status, an unpublished Structure snapshot, and a complete bound window; update all three or roll back. On an already failed publication carrying the exact reason, return the same superseded result without further writes. Never update or delete generation/pointer rows.

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/perception/structure_contract.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_schema_lockstep.py
git commit -m "fix(m1): supersede incompatible structure publications"
```

### Task 2: Carry controlled supersession across worker, CLI, and scheduler

**Files:**
- Modify: `src/polyarb/perception/structure_sync.py`
- Modify: `src/polyarb/perception/structure_publication.py`
- Modify: `src/polyarb/snapshot/cli.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`
- Test: `tests/m1-perception/test_structure_sync_window.py`
- Test: `tests/m1-perception/test_snapshot_cli_json.py`
- Test: `tests/m1-perception/test_scheduler.py`

**Interfaces:**
- Consumes: Task 1 reconciliation API and current contract constant.
- Produces: `StructureSyncCheckpoint(stage="contract-superseded", pages_processed=0)`.
- Produces: stderr audit marker `structure-publication-superseded publication_id=<32 lowercase hex>`.
- Produces: `IsolatedStructureCheckpoint` support for the controlled stage.

- [ ] **Step 1: Write failing worker/protocol/scheduler tests**

Assert one publication call returns the controlled checkpoint after the atomic store transition; CLI exit is zero, stdout is only terminal JSON, and stderr is only the allowlisted audit marker. Assert subprocess parsing accepts only `stage=contract-superseded` with `pages_processed=0`. Seed scheduler failure counter 193, return the isolated supersession checkpoint, tick once, and assert the persisted and in-memory counters remain 193 while the attempt is checkpointed rather than failed.

- [ ] **Step 2: Run the protocol tests and verify RED**

Run: `uv run pytest -q tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py tests/m1-perception/test_scheduler.py -k 'supersed or contract'`

Expected: FAIL because the worker does not reconcile and the protocol rejects the new checkpoint.

- [ ] **Step 3: Implement the minimal controlled path**

At the start of publication work, call reconciliation before normalization/certification/publish. If superseded, return the controlled zero-work checkpoint immediately. Extend CLI JSON and subprocess validation with the exact stage/zero-row pairing, emit only the fixed audit marker to stderr, add it to safe-tail parsing, and log at warning level. Reuse the scheduler's checkpoint branch so it calls `_finish_attempt(... outcome="cancelled", failure_kind="structure-contract-superseded")`, persists but does not modify the failure counter, and leaves reset exclusively in the certified snapshot branch.

- [ ] **Step 4: Run protocol tests and verify GREEN**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/perception/structure_sync.py src/polyarb/perception/structure_publication.py src/polyarb/snapshot/cli.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py tests/m1-perception/test_scheduler.py
git commit -m "fix(m1): checkpoint publication contract supersession"
```

### Task 3: Prove fresh natural recovery and close verification

**Files:**
- Modify: `tests/m1-perception/test_structure_sync_window.py`
- Modify: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Consumes: controlled supersession checkpoint and normal sync admission.
- Verifies: failed 846 is never resumed; next admission creates a distinct source window and next complete publication reserves snapshot 847.

- [ ] **Step 1: Write the failing end-to-end recovery test**

Start with pointer 845 and incompatible 846. Run one bounded sync slice and assert supersession. Run the next natural admission against a deterministic Gamma source and assert a new window id. Complete its pages/bootstrap/publication and assert its snapshot id is 847, pointer stays 845 before certification, and moves to 847 only after the existing publish gate.

- [ ] **Step 2: Run the recovery test and verify RED if any integration is missing**

Run: `uv run pytest -q tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_publication.py -k 'fresh_after_contract_supersession'`

Expected before integration completion: FAIL at the first missing natural-admission assertion.

- [ ] **Step 3: Make only the minimal integration correction**

Ensure failed windows are not resumable and that snapshot id reservation occurs only after the new source window is complete. Do not create a successor in the supersession transaction and do not special-case id 847.

- [ ] **Step 4: Run focused quality gates**

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_schema_lockstep.py
uv run ruff check src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_publication.py src/polyarb/perception/structure_sync.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/snapshot/cli.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_schema_lockstep.py
git diff --check 3254e77..HEAD
```

Expected: all pass, Ruff exit 0, diff check empty.

- [ ] **Step 5: Commit**

```bash
git add tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_publication.py
git commit -m "test(m1): prove fresh recovery after contract supersession"
```

- [ ] **Step 6: Request independent review and run the full repository suite**

Review git range `3254e77..HEAD` against the approved design. Resolve every Critical/Important finding with a new RED/GREEN cycle. Then run `uv run pytest -q`, collect the exact total, rerun Ruff on all changed Python files, and run `git diff --check 3254e77..HEAD`. Do not deploy.

### Task 4: Repair a pre-existing active-publication split state

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Consumes: `reconcile_structure_publication_contract(...)` from Task 1.
- Preserves: the historical `finished_at_ms` of an already-failed snapshot.
- Produces: the same controlled supersession result and natural 847 recovery as the ordinary path.

- [ ] **Step 1: Write migration and split-repair RED tests**

Reproduce an active building/invalid/unpublished 846, rerun full schema initialization, and assert it remains building. Separately seed the production-observed split (`snapshot=failed`, `publication=writing`, contract `NULL`, window `complete`, pointer 845) and assert reconciliation terminal-fails publication/window without changing snapshot `finished_at_ms`, pointer, or generation rows. Continue through a fresh window to 847.

- [ ] **Step 2: Write the atomic rollback RED test**

Install a temporary trigger that aborts the window terminal update. Assert reconciliation raises and publication, building snapshot, and window all retain their pre-call states.

- [ ] **Step 3: Run RED**

Run: `uv run pytest -q tests/m1-perception/test_structure_generation_publication.py -k 'active_snapshot_status_backfill or existing_split or supersession_rollback'`

Expected: migration test sees `failed`; split repair raises `structure-publication-supersession-unsafe`.

- [ ] **Step 4: Implement the minimal repair**

Exclude snapshots joined to active `writing`/`ready` publications from `_backfill_structure_snapshot_statuses`. In reconciliation, accept only `building` or the exact already-failed invalid/unpublished split. CAS-authenticate an already-failed snapshot without updating it; update publication/window in the same `BEGIN IMMEDIATE` transaction. Retain fail-closed behavior for every other partial combination.

- [ ] **Step 5: Run GREEN, focused, review, and full gates**

Run the Step 3 tests, the five-file focused suite, Ruff on changed files, independent review, and `uv run pytest -q`. Collect exact totals and do not deploy.
