# M1 Drift Recovery and Nullable-Event v4 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use inline TDD task-by-task; each change must show a failing test before production code.

**Goal:** Resume only evidenced drift timeouts promptly and create a new immutable v4 contract for the nullable ordinary-event production shape.

**Architecture:** Scheduler admission records the prior durable checkpoint and, after a timeout, revalidates status before selecting the existing 100ms continuation path. Storage and classification add a v4 contract that extends only the exact event-only exclusion predicate; v3 rows and receipts remain untouched.

**Tech Stack:** Python 3.12, asyncio, SQLite, pytest, Pydantic settings.

## Global Constraints

- Preserve append-only terminal attempt and receipt evidence.
- Quote priority must be rechecked on every continuation admission.
- No pointer, read-mode, Quote, cleanup, or production-data mutation.
- All new classifier semantics require a new contract identifier.

---

### Task 1: Timeout continuation evidence gate

**Files:**
- Modify: `src/polyarb/daemon/scheduler.py`
- Test: `tests/m1-perception/test_scheduler.py`

**Interfaces:**
- Consumes: `structure_generation_drift_status()` fields `progress_id`, `phase`, and `checkpoint_at_ms`.
- Produces: `_checkpoint_pending=True` only after a timeout with proven same-comparison advance.

- [ ] **Step 1: Write failing async tests** for (a) timeout plus advanced same comparison sets `_checkpoint_pending`, and (b) unchanged, terminal, changed-ID, and unavailable status do not.
- [ ] **Step 2: Run the focused test node** and confirm the positive case fails because current timeout handling leaves `_checkpoint_pending` false.
- [ ] **Step 3: Add the minimal post-timeout status revalidation** after recording the failed attempt; preserve defer evidence and error handling.
- [ ] **Step 4: Run the focused scheduler tests** and confirm all pass.
- [ ] **Step 5: Commit** the scheduler recovery slice with its tests.

### Task 2: v4 nullable ordinary-event exclusion

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_drift_end_to_end.py`

**Interfaces:**
- Consumes: exact event-only raw event/member fields from the sealed staging window.
- Produces: `structure-drift-classifier-v4` comparison identity and `non-neg-risk-event-member` exclusion for the exact nullable ordinary-event predicate.

- [ ] **Step 1: Write a failing fixture test** with null `negRisk`, `enableNegRisk=false`, absent group IDs, `negRiskOther=false`, no market row, and a closed member; assert v4 has no diagnostic while a deliberately ambiguous variant remains diagnostic.
- [ ] **Step 2: Run the focused v4 test** and confirm it fails because v4 is absent or the item remains `evidence-missing`.
- [ ] **Step 3: Add the v4 contract and the exact exclusion predicate**, preserving v1-v3 behavior and terminal-receipt digest selection.
- [ ] **Step 4: Run focused storage/end-to-end tests** and confirm conservation and immutable-v3 assertions pass.
- [ ] **Step 5: Commit** the v4 classification slice with its tests.

### Task 3: Evidence and release readiness

**Files:**
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Modify: `docs/learning/` only if a new operator concept is introduced.

- [ ] **Step 1: Run targeted scheduler and classifier tests, Ruff, and the relevant M1 gate.**
- [ ] **Step 2: Record exact production defect/recovery semantics and the new deployment acceptance criteria.**
- [ ] **Step 3: Create the required plan SUMMARY and verify `make planning-status`.**
- [ ] **Step 4: Request exact-SHA deployment approval; deploy only with drift=true, legacy reads, Quote=false, and cleanup protections.**
