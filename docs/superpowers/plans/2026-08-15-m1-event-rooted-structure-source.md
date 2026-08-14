# M1 Event-Rooted Structure Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make each transactional Structure source window terminally enumerable by replacing global market-keyset traversal with exact-ID batches derived from its sealed active-event artifacts.

**Architecture:** Events retain their fenced opaque-cursor pages. When the terminal event page is about to be committed, the source worker reconstructs the authenticated event truth from R2, deterministically derives open member IDs, and passes immutable batches into a single repository transaction that commits the terminal receipt and admits all market jobs. Each market job owns an ordered exact-ID list; its receipt is terminal, and the final receipt releases the pre-existing materializer.

**Tech Stack:** Python 3.12, psycopg 3, Alembic, httpx, R2/S3-compatible storage, pytest/testcontainers, Ruff.

## Global Constraints

- The migration is additive; no SQLite table, legacy pointer, R2 object, Telegram credential, or L1/L2 deployment changes.
- `m1_structure_source_page_inputs` is the durable input authority; no worker-local batch state.
- Event pages use opaque cursors; scoped market pages use canonical immutable ID lists and no cursor.
- Each exact-ID response must exactly match its admitted batch before R2 receipt/DB checkpoint.
- A malformed, missing, duplicate, inactive, closed, archived, or over-limit batch quarantines its fenced window before materialization.
- All executable behaviour continues through existing control-plane commands; no new user command is required.
- Every code commit has a matching `05.6-xxx-SUMMARY.md`, and `make planning-status` must be green before proceeding.

---

### Task 1: Persist and authenticate scoped market inputs

**Files:**
- Create: `alembic/versions/015_m1_event_rooted_structure_source.py`
- Modify: `src/polyarb/control_plane/models.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/alembic/test_015.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces `StructureSourcePageSpec(..., market_ids: tuple[str, ...] = ())`.
- Produces `StructureSourcePageSpec.market_ids_digest: str | None` and canonical `input_identity` containing that digest for market batches.
- Adds nullable `market_ids_json` and `market_ids_digest` fields to source inputs; old cursor inputs remain readable and are not reinterpreted.
- Produces `PostgresControlPlane.record_structure_source_page(..., market_batches: tuple[tuple[str, ...], ...] | None = None)`.

- [ ] **Step 1: Write RED migration and model tests**

```python
def test_015_adds_authenticated_market_batch_inputs() -> None:
    text = Path("alembic/versions/015_m1_event_rooted_structure_source.py").read_text()
    assert 'revision = "015"' in text
    assert 'down_revision = "014"' in text
    assert "market_ids_json" in text
    assert "market_ids_digest" in text

def test_market_batch_spec_canonicalizes_and_hashes_ids() -> None:
    spec = StructureSourcePageSpec(
        window_key="source-window", stream="markets", ordinal=3,
        requested_cursor=None, market_ids=("market-a", "market-b"),
    )
    assert spec.market_ids_digest is not None
    assert spec.input_identity.endswith(spec.market_ids_digest)
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/alembic/test_015.py tests/m1-perception/test_control_plane_postgres.py -q`.

Expected: fail because revision `015` and `market_ids` do not exist.

- [ ] **Step 3: Implement migration and immutable input form**

Create revision `015` from `014`; add nullable text fields and an index on
`(window_key, stream, ordinal)`. Do not add a database check that would reject
the historic cursor inputs already present in staging. Implement canonical
JSON (`json.dumps(ids, separators=(",", ":"))`) and SHA-256 in the model.
Reject a market spec with an empty list, unsorted IDs, duplicates, a cursor, or
invalid identity; reject an event spec with market IDs. Extend all source input
reads/inserts to authenticate both new fields on conflict.

- [ ] **Step 4: Add transactional batch-admission contracts**

Add a real-Postgres test that records a terminal event receipt with batches
`(("market-a", "market-b"), ("market-c",))`. Assert all three facts after
one commit: event receipt exists, window is `events-complete`, and jobs
`markets:0`/`markets:1` have the exact persisted lists/digests. Re-run the
same fenced call and assert identity equality; submit a changed list for an
existing ordinal and assert `JobIdentityConflict`.

- [ ] **Step 5: Verify GREEN and commit**

Run: `uv run pytest tests/alembic/test_015.py tests/m1-perception/test_control_plane_postgres.py -q`.

Run: `uv run ruff check alembic/versions/015_m1_event_rooted_structure_source.py src/polyarb/control_plane/models.py src/polyarb/control_plane/postgres.py tests/alembic/test_015.py tests/m1-perception/test_control_plane_postgres.py`.

Commit: `feat(m1): persist event-rooted market batch inputs`, with
`05.6-131-SUMMARY.md` recording RED/GREEN evidence.

### Task 2: Add exact-ID market retrieval and authenticated batch artifacts

**Files:**
- Modify: `src/polyarb/clients/gamma_client.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `tests/m1-perception/test_gamma_client.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**
- Produces `GammaClient.fetch_markets_by_ids(market_ids: tuple[str, ...]) -> tuple[dict, ...]`.
- Produces `market_batch_ids_from_event_pages(pages) -> tuple[tuple[str, ...], ...]` in `structure_source.py`.
- `canonical_structure_source_page_bytes` binds `market_ids_digest` when the spec is a market batch.

- [ ] **Step 1: Write RED exact-ID contracts**

```python
async def test_fetch_markets_by_ids_rejects_missing_or_unknown_response_ids() -> None:
    # mocked /markets returns market-a plus market-other for requested a,b
    with pytest.raises(PaginationIntegrityError, match="exact-id.*identity set mismatch"):
        await client.fetch_markets_by_ids(("market-a", "market-b"))

def test_market_batch_ids_are_sorted_deduplicated_and_bounded() -> None:
    batches = market_batch_ids_from_event_pages(event_pages, batch_size=2, max_batches=2)
    assert batches == (("market-a", "market-b"), ("market-c",))
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_gamma_client.py tests/m1-perception/test_transactional_structure_source_worker.py -q`.

Expected: fail because the exact-ID API and batch derivation do not exist.

- [ ] **Step 3: Implement exact-ID retrieval**

Validate a strictly sorted, unique tuple of 1–25 IDs. Call `/markets` with one
`id` parameter per member and exact `limit`; require a list of dicts whose IDs
are exactly the requested set once each. Require each returned market to be
open (`active is True`, `closed is False`, `archived is not True`). Return rows
in admitted ID order, not upstream response order.

- [ ] **Step 4: Implement artifact and event-member derivation**

Extend the Gamma protocol and source artifact header/parser with batch digest
authentication. Reconstruct all receipt-authenticated event artifacts plus the
current terminal artifact, use existing `normalize_events` semantics, sort its
open `market_to_event` keys, and split into 25-ID tuples. Raise a named source
error before the DB checkpoint when there are zero members or more than 1,000
batches. Keep this operation in the worker; it has no mutable state after the
fenced repository commit.

- [ ] **Step 5: Verify GREEN and commit**

Run the two focused suites and Ruff on the changed client/source/tests.

Commit: `feat(m1): fetch structure markets by sealed event members`, with
`05.6-132-SUMMARY.md`.

### Task 3: Release all scoped batches atomically and terminally materialize

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/structure_source.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_structure_source_worker.py`

**Interfaces:**
- Terminal events call `record_structure_source_page(..., market_batches=...)`.
- Scoped market batches always record `completed=True, next_cursor=None`.
- The final outstanding batch atomically transitions the window to `complete`
  and enqueues one `<window>:materialize` job.

- [ ] **Step 1: Write RED completion/fencing tests**

```python
def test_only_last_scoped_market_batch_releases_materializer(control_plane) -> None:
    # admit terminal events with two batches; finish ordinal 0
    assert claim_materializer() is None
    # finish ordinal 1
    assert claim_materializer().job_key == "source-window:materialize"

def test_market_batch_cannot_supply_cursor_or_nonterminal_receipt(control_plane) -> None:
    with pytest.raises(ValueError, match="scoped market batch"):
        control_plane.record_structure_source_page(lease, ..., next_cursor="x", completed=False)
```

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_structure_source_worker.py -q`.

Expected: fail because terminal events still enqueue a global cursor page and
any single terminal market page releases materialization.

- [ ] **Step 3: Implement state transitions**

For a terminal event, require non-empty `market_batches`, insert every market
input/job in the same transaction, and return the first admitted spec only as
an operator convenience. Remove the global-market successor path for new
scoped windows. For a scoped market receipt, require terminal/no cursor; after
its receipt insert, query receipt completeness across all market inputs in the
window under the enclosing transaction. Only the transition that observes zero
missing receipts changes `events-complete → complete` and inserts the
idempotent materializer job. A legacy cursor market input encountered by the
new worker is quarantined, never traversed.

- [ ] **Step 4: Verify GREEN and commit**

Run focused Postgres/worker suites, then `uv run ruff check` on changed files.

Commit: `feat(m1): terminally materialize scoped structure batches`, with
`05.6-133-SUMMARY.md`.

### Task 4: Migrate, deploy, and prove the staging Structure shadow chain

**Files:**
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Create: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-134-SUMMARY.md`
- Create: `docs/learning/11-事务型结构采集窗口.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- Staging worker image contains revision `015` code and reads the existing
  isolated DSN/R2 credentials only.
- Evidence records source-window key, immutable event/member digest, batch
  count, final bundle digest, shadow result, pointer count, restart result,
  and no Telegram/L1/L2 change.

- [ ] **Step 1: Run full local gate**

Run: `uv run pytest tests/m1-perception/test_gamma_client.py tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_control_plane_postgres.py -q`.

Run: `uv run ruff check src/polyarb/clients/gamma_client.py src/polyarb/control_plane alembic/versions/015_m1_event_rooted_structure_source.py tests/m1-perception/test_gamma_client.py tests/m1-perception/test_transactional_structure_source_worker.py tests/m1-perception/test_control_plane_postgres.py`.

Run: `make planning-status`.

- [ ] **Step 2: Apply only additive migration to isolated staging database**

Use the pre-existing staging DSN credential retrieval path. Run `alembic upgrade
015`; verify schema and source tables without printing credentials. Do not run
against production.

- [ ] **Step 3: Build/push without configuration deployment, then update one machine image**

Build `m1-event-rooted-source-<sha>` with `flyctl deploy --build-only --push`.
Update only `48e3104c979578` with `flyctl machine update --image ...`; preserve
its current 1024MB and eight-turn/two-second command. Start it explicitly.

- [ ] **Step 4: Capture source and recovery acceptance**

Prove one source window has terminal events, a fixed batch count, all batch
receipts, `complete` state, one materializer, authenticated R2 bundle,
Structure ranges/certifier, and zero `m1_publication_pointers`. Restart the
worker during unfinished market batches and prove it resumes only outstanding
batch jobs and produces the same batch/input identities.

- [ ] **Step 5: Document and commit evidence**

Add the teaching document: 30-second model, source/event/batch flow with
file:line anchors, why cursor and batch authority differ, failure matrix,
self-check questions, and FAQ increment. Update index, STATE, JOURNAL, and
summary with exact staging evidence. Commit docs only after data shows no live
pointer mutation.

## Plan Self-Review

Spec coverage: Task 1 establishes immutable inputs; Task 2 establishes the
only legal upstream fetch and batch derivation; Task 3 establishes the fenced
state chain; Task 4 proves the cloud boundary, restart path, materialization,
shadow result, and teaching handoff. The plan intentionally does not claim
Quote migration or final soak completion. All named methods and transitions
are introduced before consumers rely on them; no TBD or deferred error path is
used.
