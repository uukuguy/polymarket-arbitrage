# M1 Structure Generation Publication Implementation Plan

> **For implementation:** Use `subagent-driven-development` when explicitly selected, or execute inline task-by-task with `test-driven-development` and `verification-before-completion`. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the monolithic Structure full-table rewrite with Quote-priority, resumable generation publication whose final visible switch is bounded and atomic.

**Architecture:** A completed raw Structure window is normalized into an unpublished snapshot generation. Events, memberships, group truth, issues, and markets are written in durable bounded chunks; readers continue using the prior current generation until a small pointer-switch transaction certifies and publishes the new snapshot. Quote pipeline activity has admission priority, and Structure attempt runtime begins only after its execution slot is acquired.

**Tech Stack:** Python 3.12, asyncio, stdlib SQLite/WAL, Typer, Starlette, pytest, uv, Fly.io.

## Global Constraints

- Preserve complete-source and membership validation; no partial Structure generation may become current.
- Gamma collection checkpoints after 45 seconds or 40 pages and has a 75-second hard child limit.
- Generation work checkpoints after at most 45 seconds and has a 75-second hard child limit.
- The final pointer switch has a 15-second hard deadline.
- Quote age greater than 300 seconds remains an unconditional production failure.
- Every executable operator surface must have a documented `make <verb>-<noun>` target.
- No wallet, signing, order placement, balance mutation, or trading authority is introduced.
- Preserve the existing uncommitted final-page checkpoint regression in `tests/m1-perception/test_structure_sync_window.py`; incorporate it in Task 3 rather than discarding it.

---

## File map

- `src/polyarb/storage/schemas.py`: generation, pointer, publication-progress, and migration DDL.
- `src/polyarb/storage/sqlite_store.py`: bounded generation writes, validation receipts, pointer switch, backfill, cleanup, and current-generation reads.
- `src/polyarb/snapshot/orchestrator.py`: split phases 1–6 into a pure `SnapshotProjection` builder while preserving `run_snapshot` compatibility.
- `src/polyarb/perception/structure_publication.py`: resumable publication state machine and 45-second cooperative boundary.
- `src/polyarb/perception/structure_sync.py`: route completed raw windows into the publication worker; never enter a new stage under the prior budget.
- `src/polyarb/snapshot/cli.py`: hidden scheduler options plus explicit local status/backfill commands.
- `src/polyarb/daemon/quote_worker.py`: expose the complete Quote-core activity boundary.
- `src/polyarb/daemon/scheduler.py`: Quote-priority admission, post-lock attempt creation, defer receipts, and 75/15-second stage budgets.
- `src/polyarb/daemon/main.py`: wire Quote runtime into the Structure scheduler.
- `src/polyarb/routing/neg_risk_quote_store.py`, `src/polyarb/routing/focused_quote_collector.py`, `src/polyarb/routing/opportunity_scanner.py`, `src/polyarb/http/market_map.py`, `src/polyarb/storage/supabase_mirror.py`: read a single current generation.
- `src/polyarb/http/health.py`: generation identity, progress, defer age, and truthful attempt timing.
- `Makefile`, `docs/M1-市场感知平台使用手册.md`: operator entry points and production interpretation.

---

### Task 1: Freeze the production failure as executable contracts

**Files:**
- Modify: `tests/m1-perception/test_structure_sync_window.py`
- Modify: `tests/m1-perception/test_scheduler.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Create: `tests/m1-perception/test_structure_generation_publication.py`

**Interfaces:**
- Consumes: current `StructureSyncCheckpoint`, `SnapshotScheduler`, and snapshot-attempt tables.
- Produces: failing contracts for stage separation, post-slot timing, invisible partial generations, and bounded final switch.

- [ ] **Step 1: Add the final-page stage-separation regression**

Preserve and finish the existing test so a child invoked with `max_elapsed_s=45.0` returns:

```python
StructureSyncCheckpoint(
    window_id=window_id,
    stage="markets",
    pages_processed=2,
)
```

after the last market page commits, leaves the window `complete`, and proves `finalize_structure_window` was not awaited.

- [ ] **Step 2: Add post-lock attempt timing tests**

Use an acquired `asyncio.Lock`, start one scheduler tick, advance the wall clock beyond 180 seconds while the lock remains held, and assert no `snapshot_attempts` row exists. Release the lock, assert one `running` row is created, then complete the fake child and assert its recorded wall duration excludes the lock wait.

- [ ] **Step 3: Add generation invisibility and switch tests**

Create baseline snapshot `10`, begin generation `11`, commit one market chunk, and assert:

```python
assert store.current_structure_generation()["snapshot_id"] == 10
assert store.current_generation_market_ids() == ("old-market",)
```

After a valid terminal receipt and pointer switch, assert all reads return only generation `11`; inject an exception before commit and assert generation `10` remains current.

- [ ] **Step 4: Run the RED tests**

Run:

```bash
uv run pytest -q \
  tests/m1-perception/test_structure_sync_window.py::test_bounded_slice_yields_before_entering_structure_finalizer \
  tests/m1-perception/test_scheduler.py -k 'slot or attempt' \
  tests/m1-perception/test_structure_generation_publication.py
```

Expected: the final-page regression may pass from the preserved local edit; generation and post-lock attempt tests fail because their APIs and schema do not exist.

- [ ] **Step 5: Commit the contracts**

```bash
git add tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_structure_generation_publication.py
git commit -m "test(m1): lock generation publication boundaries"
```

---

### Task 2: Add generation schema, migration, and bounded store APIs

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_generation_publication.py`
- Modify: `tests/m1-perception/test_schema_lockstep.py`

**Interfaces:**
- Consumes: existing snapshot/event/membership/truth/issue column converters and `STRUCTURE_SYNC_WINDOWS_DDL`.
- Produces:
  - `SQLiteStore.begin_structure_publication(window_id, snapshot_metadata, now_ms) -> StructurePublicationState`
  - `SQLiteStore.append_structure_publication_chunk(publication_id, component, rows, next_cursor, now_ms) -> None`
  - `SQLiteStore.certify_structure_generation(publication_id, receipt) -> None`
  - `SQLiteStore.publish_structure_generation(publication_id, now_ms) -> int`
  - `SQLiteStore.current_structure_generation() -> dict[str, object] | None`
  - `SQLiteStore.current_generation_market_ids() -> tuple[str, ...]`

- [ ] **Step 1: Add exact DDL**

Add `structure_publications`, `structure_generation_markets`, and
`current_structure_generation` tables. Use `snapshot_id` as the generation ID,
`PRIMARY KEY(snapshot_id, market_id)` for markets, and a singleton pointer:

```sql
CREATE TABLE IF NOT EXISTS current_structure_generation (
  id INTEGER PRIMARY KEY CHECK(id=1),
  snapshot_id INTEGER NOT NULL UNIQUE REFERENCES snapshots(id),
  publication_id TEXT NOT NULL UNIQUE,
  switched_at_ms INTEGER NOT NULL CHECK(switched_at_ms >= 0)
);
```

`structure_publications.status` must be constrained to
`normalizing`, `writing`, `ready`, `published`, or `failed`; store separate
normalization component/source cursor and write component/row cursor columns,
plus expected/committed counts and a 64-character validation hash. Add
generation-keyed tables for every visible component (`events`, `event_tags`,
`memberships`, `group_truth`, `markets`, and `issues`); unpublished rows are
addressable only through publication APIs, never through the current view.

- [ ] **Step 2: Implement idempotent migration**

Schema initialization creates empty generation tables only. Add
`backfill_current_structure_generation(max_rows: int) -> BackfillCheckpoint`
that copies the latest complete published legacy snapshot in ascending
`market_id` chunks and switches the pointer only after source/destination counts
and hashes agree. Do not copy 116,000 rows in `init_schema()`.

- [ ] **Step 3: Implement bounded component writes**

`append_structure_publication_chunk` must execute one `BEGIN IMMEDIATE`, verify
the expected prior cursor and status, insert/upsert only the requested bounded
rows, update committed count/cursor, and commit. A cursor mismatch raises
`StructurePublicationCursorError` without changing either rows or progress.
`current_generation_market_ids` resolves the singleton pointer and reads that
generation in one read transaction; it exists as the minimal contract used by
the invisibility tests and reader adapters.

- [ ] **Step 4: Implement the atomic pointer switch**

Inside one transaction, re-read every committed count and validation hash,
verify the publication is `ready`, update the unpublished snapshot's
`market_count`, `market_view_published`, `is_valid`, and `snapshot_status`,
upsert the singleton pointer, mark both publication and raw window published,
and commit. No row-copy or table-wide delete is allowed in this transaction.

- [ ] **Step 5: Prove rollback, replay, and migration**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_schema_lockstep.py
```

Expected: PASS, including interrupted backfill resume and pointer-switch rollback.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_schema_lockstep.py
git commit -m "feat(m1): add resumable structure generations"
```

---

### Task 3: Split Structure projection from persistence and add the publication worker

**Files:**
- Modify: `src/polyarb/snapshot/orchestrator.py`
- Create: `src/polyarb/perception/structure_publication.py`
- Modify: `src/polyarb/perception/structure_sync.py`
- Modify: `src/polyarb/snapshot/cli.py`
- Modify: `tests/m1-perception/test_orchestrator.py`
- Modify: `tests/m1-perception/test_structure_generation_publication.py`
- Modify: `tests/m1-perception/test_snapshot_cli_json.py`

**Interfaces:**
- Consumes: Task 2 store APIs and completed `SQLiteStagedGammaSource`.
- Produces:
  - `SnapshotProjection` containing immutable snapshot metadata and tuples for each persisted component.
  - `build_snapshot_projection(settings: Settings, *, mode: str, product: str, now_ms: int, gamma_client: object, schema_ready: bool = False) -> SnapshotProjection`
  - `persist_snapshot_projection(settings: Settings, projection: SnapshotProjection) -> SnapshotResult` for legacy/archive compatibility.
  - `normalize_structure_component_chunk(store, publication, component, after_source_key, max_source_rows) -> NormalizationChunk`
  - `run_structure_publication_step(settings, window_id, max_rows, max_elapsed_s) -> StructurePublicationCheckpoint | SnapshotResult`

- [ ] **Step 1: Extract the projection result without semantic changes**

Move orchestrator phases 1–6 behind `build_snapshot_projection`. The returned
frozen dataclass must include `taken_at_ms`, status, validity, source coverage,
events, event tags, event members, group truths, markets, issues, and notes.
Keep `run_snapshot` as composition:

```python
projection = await build_snapshot_projection(
    settings,
    mode=mode,
    product=product,
    now_ms=now_ms,
    gamma_client=gamma_client,
    schema_ready=schema_ready,
)
return await asyncio.to_thread(persist_snapshot_projection, settings, projection)
```

Existing archive and local snapshot tests must remain byte-for-byte compatible.

- [ ] **Step 2: Make normalization itself resumable**

Do not call `build_snapshot_projection` from the production generation path.
Extract the same phase 1–6 rules into pure per-component reducers used by both
the legacy builder and `normalize_structure_component_chunk`. The chunk
normalizer reads at most `max_source_rows` staged raw rows after the persisted
source cursor, emits deterministically sorted canonical rows for exactly one
component, and commits those rows plus the next source cursor in one bounded
transaction. Components advance in this fixed order:

```python
("events", "event_tags", "memberships", "group_truth", "markets", "issues")
```

Sort every component by its stable primary key. Check elapsed wall clock after
every committed normalization chunk. Cross-row reducers such as memberships and
group truth must operate one event/group key at a time from staged SQLite rows;
they may not materialize the complete market universe in Python. Tests instrument
the raw-source fetch size and prove no call exceeds `max_source_rows`.

- [ ] **Step 3: Add terminal certification and the publication state machine**

After the last normalization component, compute expected counts and hashes by
streaming generation rows in primary-key chunks. Store a terminal validation
receipt only if complete-source, membership, source-truth, and component-hash
checks match the raw window. `run_structure_publication_step` performs at most
one bounded normalization/certification chunk per loop iteration, checkpoints
at `max_elapsed_s`, and returns `ready` without switching. A later invocation
with status `ready` performs only `publish_structure_generation`. Restarting at
any cursor must reproduce the same count/hash and must never expose unpublished
rows.

- [ ] **Step 4: Route completed windows through the new worker**

Replace the direct `finalize_structure_window` call in production slice mode
with `run_structure_publication_step`. Preserve the compatibility finalizer for
unbounded local/test callers until Task 6 removes the rollout flag.

- [ ] **Step 5: Add hidden scheduler CLI budgets**

Wire `--max-publication-rows` and the existing `--max-elapsed-seconds` into the
publication worker. JSON checkpoints must include `stage`, `component`,
`rows_processed`, `cursor`, and `publication_id`.

- [ ] **Step 6: Run projection and restart tests**

```bash
uv run pytest -q tests/m1-perception/test_orchestrator.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py
```

Expected: PASS; killing after any committed normalization or certification
chunk resumes without changing current generation, and the maximum staged-source
fetch size never exceeds the configured chunk size.

- [ ] **Step 7: Commit**

```bash
git add src/polyarb/snapshot/orchestrator.py src/polyarb/perception/structure_publication.py src/polyarb/perception/structure_sync.py src/polyarb/snapshot/cli.py tests/m1-perception/test_orchestrator.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_snapshot_cli_json.py
git commit -m "feat(m1): checkpoint structure generation publication"
```

---

### Task 4: Enforce Quote-core priority and truthful Structure attempt timing

**Files:**
- Modify: `src/polyarb/daemon/quote_worker.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `tests/m1-perception/test_scheduler.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`

**Interfaces:**
- Consumes: publication stages from Task 3.
- Produces:
  - `QuoteWorkerRuntime.pipeline_active() -> bool`
  - `SQLiteStore.record_structure_defer(reason, queued_at_ms, observed_at_ms) -> int`
  - snapshot attempts whose `started_at_ms` is post-slot-acquisition.

- [ ] **Step 1: Extend the Quote runtime contract**

Set a private pipeline-active flag immediately before `mark_started()` and clear
it only after feed publication, cleanup scheduling, reconciliation enqueue, and
projection-memory release finish. Clear it on cancellation and failure. Expose
it through `pipeline_active()` and `QuoteWorkerSnapshot.pipeline_active`.

- [ ] **Step 2: Move Structure attempt creation after lock acquisition**

The scheduler acquires the shared slot, rechecks Quote activity, then creates
the attempt row and starts the child. If Quote is active, persist a bounded
defer receipt, release the slot, wait five seconds, and retry without incrementing
the failure counter or creating a running attempt.

- [ ] **Step 3: Apply stage-specific hard budgets**

Use 75 seconds for Gamma/generation chunk work and 15 seconds when publication
status is `ready`. Remove the 180-second finalizer budget. A timeout must record
the actual stage and child elapsed time; lock wait is never included.

- [ ] **Step 4: Wire one runtime instance**

Pass `quote_worker.runtime` to `SnapshotScheduler` in `main.py`. Tests must prove
that a runtime transition to active between initial scheduling and lock
acquisition still defers Structure after the lock is acquired.

- [ ] **Step 5: Run priority and cancellation tests**

```bash
uv run pytest -q tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_daemon_shutdown.py
```

Expected: PASS with no orphaned child, no running attempt while queued, and no
failure-counter increment for Quote-priority defers.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/daemon/quote_worker.py src/polyarb/daemon/scheduler.py src/polyarb/daemon/main.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_l1_quote_worker_wiring.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_daemon_shutdown.py
git commit -m "fix(m1): give quote core producer priority"
```

---

### Task 5: Cut production readers over to one current generation

**Files:**
- Modify: `src/polyarb/config.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `src/polyarb/routing/focused_quote_collector.py`
- Modify: `src/polyarb/routing/opportunity_scanner.py`
- Modify: `src/polyarb/http/market_map.py`
- Modify: `src/polyarb/storage/supabase_mirror.py`
- Modify: `tests/routing/test_neg_risk_quote_store.py`
- Modify: `tests/routing/test_focused_quote_collector.py`
- Modify: `tests/m1-perception/test_opportunity_watcher_http.py`
- Create: `tests/m1-perception/test_structure_generation_readers.py`

**Interfaces:**
- Consumes: Task 2 current pointer and generation rows.
- Produces: `current_structure_markets` SQL view/helper and dual-read comparison.

- [ ] **Step 1: Add the current-generation view**

Create a view selecting generation rows joined to the singleton pointer. Every
read opens one transaction, resolves pointer/snapshot identity once, and uses
that identity for markets, coverage, memberships, and truth.

- [ ] **Step 2: Add explicit rollout modes**

Add `structure_generation_read_mode` constrained to `legacy`, `compare`, or
`generation`, defaulting to `legacy`. In `compare`, return legacy results but
compute generation count/hash in the same command and fail health on mismatch.

- [ ] **Step 3: Update all production consumers**

Replace direct current `markets` reads in the files listed above with the
shared generation-aware query builder. Exact historical snapshot reads remain
bound to their snapshot/generation and may not silently resolve the current
pointer.

- [ ] **Step 4: Prove concurrent reader consistency**

Hold a read transaction across a pointer switch and assert it sees the old
generation throughout; a new transaction sees only the new generation. Add
identity mismatch, missing pointer, count mismatch, and compare-mode tests.

- [ ] **Step 5: Run consumer suites**

```bash
uv run pytest -q tests/m1-perception/test_structure_generation_readers.py tests/routing/test_neg_risk_quote_store.py tests/routing/test_focused_quote_collector.py tests/m1-perception/test_opportunity_watcher_http.py tests/m1-perception/test_arbitrage_opportunities_http.py
```

Expected: PASS with no mixed-generation response.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/config.py src/polyarb/storage/sqlite_store.py src/polyarb/routing/neg_risk_quote_store.py src/polyarb/routing/focused_quote_collector.py src/polyarb/routing/opportunity_scanner.py src/polyarb/http/market_map.py src/polyarb/storage/supabase_mirror.py tests/routing/test_neg_risk_quote_store.py tests/routing/test_focused_quote_collector.py tests/m1-perception/test_opportunity_watcher_http.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): read one atomic structure generation"
```

---

### Task 6: Add health, Make surfaces, migration gates, and rollback

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/snapshot/cli.py`
- Modify: `Makefile`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Consumes: generation, pointer, defer, and attempt APIs from Tasks 2–5.
- Produces: `make structure-generation-status`, `make structure-generation-backfill`, and `make structure-generation-compare`.

- [ ] **Step 1: Expose generation health**

Add checks for publication stage/cursor/checkpoint age, pointer snapshot,
generation count/hash agreement, and producer defer age. A healthy Quote-priority
defer is `warn`; exceeding the configured Structure publication SLA is `fail`.

- [ ] **Step 2: Add bounded operator commands**

`structure-generation-backfill` writes one bounded chunk and prints JSON;
`structure-generation-status` and `structure-generation-compare` are read-only.
No command switches production read mode automatically.

- [ ] **Step 3: Document rollout and rollback**

Document exact sequence: schema deploy → bounded backfill → compare PASS → set
generation mode → natural publication → rollback by restoring `legacy` mode.
State that a pointer switch, not row-copy completion, is the publication event.

- [ ] **Step 4: Run gates**

```bash
uv run pytest -q tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py
make docs-m1-check
make planning-status
uv run ruff check src tests/m1-perception
```

Expected: every command exits zero and planning status reports no drift.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/http/health.py src/polyarb/snapshot/cli.py Makefile docs/M1-市场感知平台使用手册.md tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_makefile_contract.py tests/m1-perception/test_m1_manual_contract.py
git commit -m "feat(m1): expose generation rollout health"
```

---

### Task 7: Qualify and deploy Structure generation publication

**Files:**
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/workstreams/m1-perception/ROADMAP.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`
- Create: `docs/learning/45-structure-generation-publication.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- Consumes: committed Tasks 1–6 and production Fly access.
- Produces: exact-SHA deployment and evidence; does not complete M1 until the linked lifecycle plan also passes.

- [ ] **Step 1: Run the complete local gate**

```bash
make test-m1-perception
make docs-m1-check
make planning-status
git diff --check
```

Expected: all pass on a clean committed revision.

- [ ] **Step 2: Deploy schema in legacy mode**

Deploy the exact SHA, verify `/health.releaseId`, run bounded backfill repeatedly,
and require `make structure-generation-compare` to report identical snapshot ID,
market count, universe hash, and source-truth hash.

- [ ] **Step 3: Switch readers and observe one natural generation**

Enable generation read mode, deploy the exact config/source revision, and sample
every ten seconds through one complete generation. Any Quote age `>=300`, any
opportunity HTTP 503, any mixed identity, or any unbounded attempt fails the gate
and triggers rollback to legacy mode.

- [ ] **Step 4: Record evidence without claiming full closure**

Update SUMMARY, STATE, JOURNAL, architecture thread, manual, and the learning
document with exact release, image digest, generation/snapshot IDs, observed
max Quote age, defer receipts, and rollback status.

- [ ] **Step 5: Commit evidence**

```bash
git add .planning docs/learning docs/M1-市场感知平台使用手册.md
git commit -m "docs(m1): record generation publication qualification"
```
