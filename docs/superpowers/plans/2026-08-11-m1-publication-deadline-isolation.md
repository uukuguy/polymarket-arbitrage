# M1 Structure Publication Deadline Isolation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Structure publication yield a durable checkpoint before its 45-second cooperative budget rather than hit the 75-second watchdog, and ensure timed-out health reads release their lane.

**Architecture:** A monotonic absolute deadline originates in `run_structure_publication_slice` and is passed into every SQLite connection used by a publication chunk. SQLite progress handlers interrupt statements at that deadline; the caller converts an interrupted current chunk into a normal checkpoint without advancing its cursor. Health authority reads register connections with `_HealthReadExecution` so request timeout frees the lane worker.

**Tech Stack:** Python 3.12, SQLite progress handlers, asyncio subprocess supervision, pytest, Fly.io.

## Global Constraints

- Preserve the 45-second cooperative slice and 75-second child hard limit.
- An interrupted chunk advances neither cursor nor committed count; earlier chunks remain valid.
- Do not relax Quote's 300-second SLA or hide P1 incidents.
- No dependency additions.
- Acceptance needs a fresh Structure pointer, a certified later Quote receipt, cleared incidents, and multi-cycle dashboard/health soak.

---

### Task 1: Deadline-aware publication chunk

**Files:**
- Modify: `src/polyarb/perception/structure_publication.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:** `deadline_monotonic: float | None` is calculated once per slice and supplied to every source read, anti-join, metadata read, certification read, and writer transaction. The result is a `StructurePublicationCheckpoint` at the last committed cursor, never a child timeout.

- [ ] Write failing test `test_publication_deadline_interrupts_current_source_read_without_cursor_advance`. Create an unfinished markets publication, save `before = store.get_structure_publication_progress(...)`, install a SQLite progress handler that interrupts the current source query, invoke `run_structure_publication_slice(..., max_rows=500, max_elapsed_s=45.0)`, and assert `result.rows_processed == 0` and `after.cursor == before.cursor`.
- [ ] Add a second RED test with one successful fake chunk before interruption; assert exactly 500 rows, cursor `markets|market-500`, and committed market count 500.
- [ ] Run `uv run pytest tests/m1-perception/test_structure_generation_publication.py -k publication_deadline -v`; expected RED is uncaught `sqlite3.OperationalError` or no checkpoint.
- [ ] Add optional keyword-only `deadline_monotonic` to `fetch_structure_staging_chunk`, `structure_event_ids_for_markets`, `structure_events_with_duplicate_markets`, `structure_event_only_market_ids`, publication metadata/certification reads, and `append_structure_publication_chunk`. Immediately after each connection opens, install `con.set_progress_handler(lambda: int(time.monotonic() >= deadline_monotonic), 1_000)` when set.
- [ ] Pass the one deadline through `normalize_structure_component_chunk` and `run_structure_publication_step`. Catch only deadline-caused SQLite `interrupted` and raise module-local `StructurePublicationDeadlineReached`; do not mask lock, validation, or contract errors.
- [ ] In `run_structure_publication_slice`, calculate `deadline_monotonic = started_at + max_elapsed_s`; retain commit-reserve admission. On `StructurePublicationDeadlineReached`, return the prior durable checkpoint. With none, return a zero-row checkpoint from current durable progress; emit no fake progress marker.
- [ ] Run `uv run pytest tests/m1-perception/test_structure_generation_publication.py -k 'publication_deadline or publication_slice' -v`; expected PASS.
- [ ] Commit `fix(05.6-67): bound Structure publication chunks` with source and test files, plus the matching phase summary required by the hook.

### Task 2: Complete health read-lane cancellation

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/storage/sqlite_store.py` only for health authority methods opening unregistered connections
- Test: `tests/m1-perception/test_health_endpoint.py`

**Interfaces:** `_HealthReadExecution.register(connection)` and `.interrupt()` encompass all authority connections. A timed-out request's worker exits after SQLite interruption, allowing the next request to acquire the single lane.

- [ ] Write `test_timed_out_health_authority_read_releases_single_read_lane`: block the first actual SQLite authority query until its progress handler receives interrupt; first `/healthz` returns 503, second returns 200/503 but never `read-model-saturated`.
- [ ] Run `uv run pytest tests/m1-perception/test_health_endpoint.py -k timed_out_health_authority_read_releases -v`; expected RED because a serial authority query remains unregistered.
- [ ] Pass a context-aware registration callback to every health storage method; immediately register on open, install deadline handler, and clear handler before close. `_HealthReadExecution.interrupt()` snapshots and interrupts every registered connection.
- [ ] Run `uv run pytest tests/m1-perception/test_health_endpoint.py -k 'health_read or timed_out_health_authority_read_releases' -v`; expected PASS, preserving existing true-saturation P1 behavior.
- [ ] Commit `fix(05.6-68): release interrupted health reads` with source, test, and matching phase summary.

### Task 3: Regression, documentation, and live acceptance

**Files:**
- Create: `docs/learning/63-Structure发布块截止.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-67-SUMMARY.md`

- [ ] Run `uv run pytest tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py -q` and `uv run ruff check src/polyarb/perception/structure_publication.py src/polyarb/storage/sqlite_store.py src/polyarb/http/health.py`. Poll any outliving process; never call a partial run green.
- [ ] Deploy exactly once with `flyctl deploy --app polyarb-l1 --detach`; poll the existing release if the local client is slow and verify both app and cron release identity.
- [ ] Require live evidence: latest Structure attempt checkpointed/succeeded rather than timeout; `current_structure_generation.snapshot_id > 891`; Structure P1 verified/closed after publish; a Quote receipt uses that snapshot; health lane remains unsaturated across multiple probes. Query opportunity history too and preserve zero candidates as truth.
- [ ] Write learning note with mental model, `file:line` map, 75-second containment rule, atomic checkpoint invariant, self-check, FAQ. Record exact test/deploy/live evidence in summary, commit `docs(05.6-67): record publication deadline recovery`, then run `make planning-status`.
