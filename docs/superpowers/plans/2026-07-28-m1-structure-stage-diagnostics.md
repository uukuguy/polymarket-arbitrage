# M1 Structure Snapshot Stage Diagnostics Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `subagent-driven-development` to execute this plan task-by-task, with an independent review after each implementation task.

**Goal:** Make every terminal Structure snapshot attempt explainable in production by persistently recording its bounded semantic last stage and wall-clock elapsed time, before changing cadence, timeout, VM sizing, or the Archive path.

**Architecture:** The isolated snapshot child emits fixed-vocabulary stage markers to stderr. The scheduler parent already owns/reaps that child, so it extracts only the final valid marker after normal completion or timeout, measures its own wall-clock duration, and writes both facts to the terminal `snapshot_attempts` row. Health and the existing attempt-status read model consume those exact persisted fields; no raw child stderr is stored or exposed.

**Tech Stack:** Python 3.12, asyncio subprocesses, SQLite/WAL additive migrations, Starlette health, pytest, Make.

## Guardrails

- This is observer-only diagnostic work: no wallet, signing, orders, balance, or execution code.
- Do **not** increase the 240-second timeout, change the Structure cadence, resize Fly resources, or unpause the production scheduler automatically in this plan.
- `failure_kind` remains stable (`snapshot-subprocess-timeout`); stage context is a separate nullable field, never encoded by copying raw stderr.
- Stage values are an allowlist chosen in code. Parsing ignores arbitrary child output and retains only the final valid marker.
- Migrations are add-only in `SQLiteStore.init_schema`; existing historical attempts remain readable with `NULL` diagnostic fields.
- Chain truth is mandatory: child marker → parent parser/result → scheduler terminal write → SQLite row → `/health` and `make snapshot-attempt-status`.

## Task 1: Child-to-parent bounded diagnostic contract

**Files:** Modify `src/polyarb/snapshot/orchestrator.py`, `src/polyarb/daemon/scheduler.py`; modify `tests/m1-perception/test_scheduler.py`.

**Interfaces:** Child stderr emits `snapshot-stage stage=<allowed-stage> state=start|complete elapsed_ms=<nonnegative-int>`. `IsolatedSnapshotResult` and `SnapshotSubprocessError` expose nullable `last_stage` plus nonnegative `elapsed_ms`; no callers receive child stderr.

- [ ] **Step 1: Write failing tests**

Add tests that:

1. accept the final allowlisted marker while ignoring malformed and arbitrary error lines;
2. on a normal child result, return parent wall-clock elapsed time and final stage;
3. on timeout, reap the child then raise `SnapshotSubprocessError("timeout")` with final parsed stage and elapsed time; and
4. preserve the existing terminate/kill semantics and the stable timeout error string.

Use fake child stderr with a final `gamma-markets` start marker to prove timeout diagnostics are obtained only after communication/reaping. Ensure the test cannot pass by storing a raw stderr string.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/m1-perception/test_scheduler.py -q`

Expected: FAIL because result/error diagnostic fields and bounded parser do not yet exist.

- [ ] **Step 3: Implement the minimal contract**

Give `_phase()` an explicit fixed `stage` argument and emit the semantic marker at phase start and completion for Structure-relevant work (`gamma-events`, `gamma-markets`, `membership-recheck`, `validate`, `persist`). In the parent, use a compiled allowlisted parser over the captured stderr, calculate elapsed from the parent monotonic clock on every terminal path, and attach only the final stage token plus elapsed milliseconds to the normal result or timeout error.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_scheduler.py -q && uv run ruff check src/polyarb/snapshot/orchestrator.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_scheduler.py`

Expected: PASS. Commit with:

`git add src/polyarb/snapshot/orchestrator.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_scheduler.py && git commit -m "feat(m1): diagnose structure snapshot stages"`

## Task 2: Durable terminal-attempt evidence and read-chain

**Files:** Modify `src/polyarb/storage/schemas.py`, `src/polyarb/storage/sqlite_store.py`, `src/polyarb/daemon/scheduler.py`, `src/polyarb/http/health.py`, `scripts/snapshot_attempt_status.py`; modify `tests/m1-perception/test_scheduler.py`, `tests/m1-perception/test_health_endpoint.py`, `tests/m1-perception/test_snapshot_attempt_status.py`.

**Interfaces:** Terminal `snapshot_attempts` rows gain nullable `elapsed_ms` and `last_stage`. `finish_snapshot_attempt()` accepts these facts independently of `failure_kind`; `get_latest_snapshot_attempt()`, `/health`, and `make snapshot-attempt-status` display the persisted values exactly.

- [ ] **Step 1: Write failing chain-truth tests**

Add tests proving that:

1. a fresh database creates the two columns and an existing database receives them through idempotent migration;
2. a timeout result writes `failure_kind=snapshot-subprocess-timeout`, `last_stage=gamma-markets`, and parent elapsed time to the same terminal row;
3. successful results persist `elapsed_ms` and their last stage;
4. health reports the stored diagnostic fields, including nullable historical values without claiming a stage; and
5. the attempt-status script emits only fields read from the persisted row.

The scheduler test must pass a diagnostic error object through `_finish_attempt`; it must not reconstruct stage data from a log line at the storage/read layer.

- [ ] **Step 2: Run the red tests**

Run: `uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_snapshot_attempt_status.py -q`

Expected: FAIL because the schema, terminal write, and read models lack diagnostic fields.

- [ ] **Step 3: Implement additive persistence and readers**

Add `elapsed_ms INTEGER` and `last_stage TEXT` to the create DDL and add-only migration list. Extend store finish/read methods with optional values so unrelated callers and legacy rows remain compatible. Thread values from normal and error scheduler outcomes to the terminal write. Render the fields as bounded labelled diagnostics in health and the status script (for example `stage=gamma-markets`, `elapsed_ms=245012`), preserving existing health gate semantics rather than silently making a failed attempt healthy.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_snapshot_attempt_status.py -q && uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py src/polyarb/http/health.py scripts/snapshot_attempt_status.py`

Expected: PASS. Commit with:

`git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py src/polyarb/http/health.py scripts/snapshot_attempt_status.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_snapshot_attempt_status.py && git commit -m "feat(m1): persist snapshot stage diagnostics"`

## Task 3: Operator learning material and deployment evidence

**Files:** Create `docs/learning/29-structure-snapshot-stage-diagnostics.md`; modify `docs/learning/00-INDEX.md`, `.planning/JOURNAL.md`; create `docs/superpowers/plans/2026-07-28-m1-structure-stage-diagnostics-SUMMARY.md` after implementation commits.

**Interfaces:** Operators can distinguish a child timeout near `gamma-markets` from a healthy completion and know that diagnostics guide the next experiment rather than justify automatic tuning.

- [ ] **Step 1: Write teaching material**

Create the learning note with the required 30-second mental model, code map with `file:line` references, chain-truth explanation, explicit non-decisions (no cadence/timeout/VM change), diagnostic interpretation examples, self-check questions, and an empty FAQ increment section. Add it to the index in reading order.

- [ ] **Step 2: Verify local gates**

Run: `make planning-status && uv run pytest tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_snapshot_attempt_status.py -q && make docs-m1-check`

Expected: clean planning status, passing targeted suite, and offline verification
that the M1 manual's command/route/health references match the repository.
`smoke-health-local` deliberately requires a separately running daemon and is
therefore an operator smoke, not a deployment-blocking local gate.

- [ ] **Step 3: Deploy only the verified commit and collect an explicit diagnostic sample**

Push the repair branch for source traceability, then run `make deploy`. After deployment, use the existing signed `make unpause-prod` once **only if** the shared secret is locally configured; otherwise record the missing authority and stop rather than fabricating a run. Observe the next scheduler attempt with `make snapshot-attempt-status` and `make smoke-health-prod`. Record the deployed Git SHA/release, terminal outcome, stage, elapsed time, and health result in the summary and JOURNAL.

The acceptance evidence must establish one of these two bounded outcomes:

- a success with persisted duration/stage, which supplies a baseline for a later cadence experiment; or
- a failure with persisted last stage/duration, which identifies the next diagnostic hypothesis.

Do not apply a performance fix in response within this task.

- [ ] **Step 4: Commit documentation and write the required plan summary**

Commit the learning note/JOURNAL after the verified deployment evidence, then create the required SUMMARY with commits, verification commands, release evidence, decisions, and remaining follow-up. Run `make planning-status` again and commit the SUMMARY if the hook/plan workflow requires it.
