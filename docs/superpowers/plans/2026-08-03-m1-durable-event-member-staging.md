# M1 Durable Event-Member Staging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an authenticated per-ordinal event-member sidecar so classifier-v2 can enumerate the complete frozen source with real database- and Python-side work bounded to 500 members per call.

**Architecture:** Preserve the existing frozen event and event-market relation tables. Derive a new append-only member sidecar from immutable event payloads through a restart-safe cursor, seal it with an immutable source-bound receipt, and make fresh projection consume only its covering keysets. The sidecar retains duplicates and malformed raw members instead of silently normalizing them away.

**Tech Stack:** Python 3.12, SQLite, stdlib `json/hashlib/dataclasses`, existing row-chain SHA-256 v2, pytest, Ruff, uv.

## Global Constraints

- Metadata contract is exactly `structure-event-member-staging-v1`.
- Event source contract is exactly `structure-event-source-v1`.
- Never rebuild or mutate `structure_sync_event_staging` or `structure_sync_event_market_staging` business rows.
- No derivation/reader call may parse, normalize, hash, query as candidates, or write more than 500 raw members.
- Resume authority is `(window_id,event_id,member_ordinal,member_character_offset,member_byte_offset,checkpoint_digest)` and every checkpoint is committed in the same transaction as its sidecar rows.
- Historical derivation uses stdlib `json.JSONDecoder.raw_decode` from the durable byte offset; it never materializes a complete event/member array with `json.loads()`.
- Duplicate market IDs at different ordinals remain distinct sidecar rows.
- Invalid/null/blank/padded fields remain nullable diagnostic evidence; they are never coerced into a valid identity.
- Only naturally collected post-contract windows receive event metadata/source receipts and member sidecars; historical windows are never backfilled or rescanned.
- The seal receipt is append-only and replacement-safe; missing, mixed, or tampered evidence fails closed.
- Reuse existing `source-event`, `projection-member`, and `diagnostic/unclassified` row-chain domains; add no domain.
- Sidecar reads/writes use PK/covering keysets, no nullable-OR, no `json_each`, no per-member SELECT, and no temporary order B-tree.
- Existing 500-row, 100-chunk, 45-second cooperative and 75-second parent contracts remain unchanged.
- Quote priority, producer lock, pointer, serving, publication, generation, exact receipts, read mode, and Quote state remain unchanged.
- No new dependency; use `uv`, never `pip`.
- Shared `.superpowers/sdd/*` changes are user-owned and never included in task commits.

---

## File Responsibility Map

- `src/polyarb/perception/structure_contract.py`: exact metadata contract name.
- `src/polyarb/perception/structure_event_members.py`: canonical sidecar row/progress/receipt types, strict raw-member extraction, tagged commitment rows.
- `src/polyarb/storage/schemas.py`: sidecar, progress, receipt, indexes, guards, append-only triggers.
- `src/polyarb/storage/sqlite_store.py`: restart-safe migration, bounded derivation CAS, receipt validation, indexed fresh projection reads.
- `src/polyarb/daemon/scheduler.py`: sidecar derivation before publication/drift work under existing producer/Quote priority.
- `src/polyarb/http/health.py`: receipt-validated recovering/fail/pass sidecar status.
- `scripts/polywatch/healthz_watcher.py`: precise sidecar failure priority and existing incident lifecycle.
- `tests/m1-perception/*`: schema, migration, derivation, restart, scheduler, health, Polywatch, projection and performance contracts.
- `docs/dev/structure-drift-operations.md`, `docs/learning/46-Structure漂移安全切换.md`: operator/learning amendment.

### Task 1: Add Canonical Sidecar Schema and Restart-Safe Migration

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py`
- Create: `src/polyarb/perception/structure_event_members.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_sync_window.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`

**Interfaces:**
- Produces: `STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT = "structure-event-member-staging-v1"`.
- Produces: `StructureEventMemberRow`, `StructureEventMemberProgress`, `StructureEventMemberReceipt` immutable dataclasses.
- Produces: canonical DDL/migration for `structure_sync_event_member_staging`, `structure_sync_event_member_progress` (including `member_byte_offset`), and `structure_sync_event_member_receipts`.

- [ ] **Step 1: Write RED fresh-schema and registry tests**

Assert the exact metadata contract and canonical columns:

```python
assert STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT == (
    "structure-event-member-staging-v1"
)
assert sidecar_columns == (
    "window_id", "event_id", "event_ordinal", "member_ordinal",
    "market_id", "market_sort_key", "group_id", "member_kind",
    "active", "closed", "payload_json", "payload_hash",
)
assert ROW_CHAIN_DOMAINS == domains_before_sidecar
```

Assert primary/covering indexes exactly match the design and no unique index
contains `market_id` without `member_ordinal`.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_readers.py -k 'event_member_schema or event_member_contract'
```

Expected: RED because the contract, module, tables, and indexes do not exist.

- [ ] **Step 2: Write RED immutability and replacement tests**

Insert two rows with the same market ID at ordinals 4 and 9 and assert both
survive. Freeze/seal the window and assert INSERT/UPDATE/DELETE fail with stable
sidecar errors. For the receipt, assert duplicate INSERT and
`INSERT OR REPLACE` both raise:

```text
structure-event-member-receipt-sealed
```

and the original row remains byte-identical.

- [ ] **Step 3: Write RED fresh-vs-migrated lockstep and rollback tests**

Downgrade a fixture to the `9b117d4` schema, preserve event/relation/market/
publication rows, migrate twice, and compare `PRAGMA table_info`, index lists,
and normalized trigger SQL against a fresh database. Inject failures after each
new table/trigger creation and assert the savepoint restores the exact old
schema and all business-table rows.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py -k 'event_member_migration or event_member_rollback or event_member_immutable'
```

Expected: RED before migration exists.

- [ ] **Step 4: Implement strict types and canonical DDL**

Use immutable dataclasses:

```python
@dataclass(frozen=True)
class StructureEventMemberRow:
    window_id: str
    event_id: str
    event_ordinal: int
    member_ordinal: int
    market_id: str | None
    market_sort_key: str
    group_id: str | None
    member_kind: str | None
    active: bool | None
    closed: bool | None
    payload_json: str
    payload_hash: str
```

Create the three tables and exact indexes from the approved design. Receipt
triggers include a `BEFORE INSERT WHEN EXISTS(...)` guard in addition to UPDATE
and DELETE guards so SQLite replacement semantics cannot overwrite evidence.

- [ ] **Step 5: Implement restart-safe schema bootstrap**

Create only empty sidecar/progress/receipt tables inside the existing schema
savepoint. Do not read `payload_json` or populate sidecar rows in
`init_schema()`. Recreate identical triggers and indexes on every migration
path. Preserve every pre-existing business row byte-for-byte.

- [ ] **Step 6: Run GREEN and static gates**

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_readers.py -k 'event_member_schema or event_member_contract or event_member_migration or event_member_rollback or event_member_immutable'
uv run ruff check src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_event_members.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_readers.py
git diff --check
```

- [ ] **Step 7: Commit**

```bash
git add src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_event_members.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): add durable event member sidecar"
```

Review for schema lockstep, duplicate preservation, trigger replacement bypass,
rollback, forbidden business-row writes, and domain-registry drift.

### Task 2: Implement Bounded Derivation, Seal Receipt, and Chain-Truth

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py`
- Modify: `src/polyarb/perception/structure_event_members.py`
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `tests/m1-perception/test_structure_sync_window.py`
- Modify: `tests/m1-perception/test_structure_generation_publication.py`
- Modify: `tests/m1-perception/test_scheduler.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`

**Interfaces:**
- Produces: `STRUCTURE_EVENT_SOURCE_CONTRACT = "structure-event-source-v1"`.
- Produces: same-transaction event metadata staging and immutable source receipt for new windows.
- Produces: `extract_structure_event_member_row(...) -> StructureEventMemberRow`.
- Produces: `advance_structure_event_member_staging_chunk(self, *, window_id: str, limit: int = 500, inspection_callback: Callable[[int], None] | None = None) -> dict[str, object]`.
- Produces: `structure_event_member_status(self, *, window_id: str) -> dict[str, object]` with receipt validation.
- Produces: scheduler/health/Polywatch recovery chain before publication/drift.

- [ ] **Step 1: Write RED strict extraction tests**

Parameterize null, blank, padded, wrong-type, and valid-but-mismatched market/
group/kind/active/closed fields. Strict extraction preserves canonical raw JSON,
uses a lowercase SHA-256 payload hash, and returns nullable fields rather than
inventing `""`, `"None"`, or `False`.

Two equal market IDs at different ordinals must yield two distinct rows.

Add production-shaped events where only the top-level event has
`negRiskMarketID`. Nested members do not contain group IDs. Assert sidecar rows
receive the exact event group. Unknown/mismatched top-level group evidence
remains nullable/fail-closed.

- [ ] **Step 2: Write RED new-window source-authority tests**

Commit event pages through the production writer. Assert event rows and
metadata rows commit together, retries must match exact group/hash/length, and
`events_complete` atomically seals one replacement-safe source receipt. Inject
metadata/rolling-state/receipt failures and prove event page/cursor/window
transition rollback.

Create a historical pre-contract window and assert no metadata/source/member
backfill occurs and status returns `structure-event-source-receipt-unavailable`.
Create the next natural window and assert it becomes eligible without changing
the old pointer/publication/serving rows.

- [ ] **Step 3: Write RED 1,200-member bounded/restart tests**

Seed one immutable event with 1,200 raw members in adversarial order. Advance
with limit 500 and assert exact progress:

```python
assert [(r["rows_written"], r["member_ordinal"], r["complete"]) for r in runs] == [
    (500, 499, False),
    (500, 999, False),
    (200, 1199, True),
]
assert max(database_member_rows_inspected_per_call) <= 500
assert max(raw_decode_calls_per_call) <= 500
assert max(python_member_rows_inspected_per_call) <= 500
assert whole_event_json_load_calls == 0
```

Kill/reopen before and after each CAS and prove ordinals 0..1199 exist exactly
once. Assert both `member_ordinal` and `member_byte_offset` resume at the first
undecoded object. Inject write/progress/receipt failures and prove neither
cursor advances without its rows and terminal progress never commits without
its receipt.

- [ ] **Step 4: Write RED receipt oracle and tamper matrix**

Use an independent test-side fixed tuple and SHA-256 oracle. Change every
identity/count/root/cursor/contract/time/digest field independently. Valid
status exposes bounded counts; missing, mixed, or tampered evidence returns:

```text
structure-event-member-receipt-invalid
```

with no untrusted counts/samples.

- [ ] **Step 5: Implement new-window event source authority**

During event-page ingestion, use the already-decoded top-level event and exact
canonical payload string to write metadata and advance the tagged source-event
commitment. Do not reopen or iterate the nested markets array. Seal the source
receipt in the same transaction as `events_complete`; require it before
publication/member derivation. Advance/status/health validate with constant-size
PK reads and never recompute the source root from payload rows.

- [ ] **Step 6: Implement the bounded stdlib member-array scanner**

Create a focused scanner in `structure_event_members.py`:

```python
@dataclass(frozen=True)
class DecodedMemberBatch:
    members: tuple[tuple[int, dict[str, object], str], ...]
    next_member_ordinal: int
    next_byte_offset: int
    complete: bool


def decode_event_member_batch(
    payload_json: str,
    *,
    member_ordinal: int,
    member_byte_offset: int,
    limit: int,
) -> DecodedMemberBatch:
    ...
```

Use `json.JSONDecoder.raw_decode` once per member. The initial call scans JSON
syntax to the top-level `markets` array without decoding that array; resumed
calls begin at the authenticated byte offset. Cross-check delimiters, ordinal,
array termination, duplicate top-level `markets` keys, and trailing data.
Never call `json.loads(payload_json)` or decode an unbounded prefix of member
objects.

Add RED/GREEN tests for escaped strings containing `"markets"`, nested keys,
empty arrays, scalar/invalid `markets`, malformed commas/trailing data, restart
offset mismatch, and exact 500/500/200 decoder-call counts.

- [ ] **Step 7: Implement strict extraction and tagged commitments**

Canonical raw-member JSON uses sorted keys and compact separators. Commitment
rows are explicitly tagged while reusing the existing source-event domain:

```python
member_chain.update((
    STRUCTURE_EVENT_MEMBER_METADATA_CONTRACT,
    event_id,
    event_ordinal,
    member_ordinal,
    market_id,
    group_id,
    member_kind,
    active,
    closed,
    payload_hash,
))
```

The invalid-member commitment uses the same tag and exact nullable envelope.

- [ ] **Step 8: Implement authenticated cursor checkpoints and bounded CAS**

Read the current parent event text by PK and decode only one bounded batch:

```python
batch = decode_event_member_batch(
    payload_json,
    member_ordinal=progress.member_ordinal,
    member_byte_offset=progress.member_byte_offset,
    limit=remaining,
)
```

Add a canonical checkpoint digest binding source receipt digest, parent payload
hash, event ID, member ordinal, character/byte offset, row count, and member/
diagnostic states. Validate it before every resume. Persist a direct character
offset alongside byte offset so non-ASCII resume never encodes/decodes or scans
the committed prefix.

Do not call `json_each` or whole-event `json.loads`. When the current event is
exhausted, keyset-load the next event and spend only the remaining member
budget. Insert sidecar rows and update both durable cursor fields inside one
`BEGIN IMMEDIATE` identity CAS. At terminal state, insert the receipt before
marking progress complete.

- [ ] **Step 9: Implement validated status and scheduler ordering**

Scheduler order under the existing producer lock is:

```python
if not member_status["sealed"]:
    advance_structure_event_member_staging_chunk(...)
    return
```

This runs after event staging is frozen and before publication/generation/drift.
Keep Quote double-priority and attempt ledger semantics unchanged. Same sealed
source identity is never rederived; a new window starts exactly once.

- [ ] **Step 10: Close health and resident Polywatch chain**

Expose a constant-size `snapshot:structure_event_members` check in strict and
reachability health payloads. Recovering shows cursor/rows; invalid receipt or
terminal derivation failure fails with exact code. Polywatch prioritizes the
specific sidecar failure over generic snapshot cancellation and uses the
existing component state for first alert, suppression, reminder, and recovery.

- [ ] **Step 11: Run GREEN and proportional regression**

```bash
uv run pytest -q tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py -k 'event_member or event_source'
uv run pytest -q tests/m1-perception/test_scheduler.py -k 'structure or quote_priority'
uv run pytest -q tests/m1-perception/test_polywatch_healthz_watcher.py
uv run ruff check src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_event_members.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py
git diff --check
```

- [ ] **Step 12: Commit**

```bash
git add src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_event_members.py src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_structure_sync_window.py tests/m1-perception/test_structure_generation_publication.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py docs/M1-市场感知平台使用手册.md
git commit -m "feat(m1): derive and seal event member sidecar"
```

Review the full chain: immutable event payload → bounded extraction → sidecar
CAS → terminal receipt → validated status → health → resident alert/recovery.

### Task 3: Switch Fresh Projection to the Sealed Sidecar

**Files:**
- Modify: `src/polyarb/perception/structure_drift.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_drift_performance.py`
- Modify: `docs/dev/structure-drift-operations.md`
- Modify: `docs/learning/46-Structure漂移安全切换.md`
- Create: `docs/superpowers/plans/2026-08-03-m1-durable-event-member-staging-SUMMARY.md`

**Interfaces:**
- Consumes: Tasks 1–2 sealed sidecar and validated receipt.
- Produces: sidecar-only event-member anti-join for the existing `FreshProjectionCursor`, `FreshProjectionChunk`, and `FreshProjectionCommitment` interfaces.
- Produces: clean re-review boundary for parent classifier-recovery Task 3.

- [ ] **Step 1: Write RED receipt/source binding tests**

Projection must reject missing, invalid, mixed-window, mixed-source, or tampered
member receipts before reading candidate rows. Assert no counts/root/samples are
returned on failure and no production table is mutated.

- [ ] **Step 2: Write RED sidecar-only query-plan test**

Trace and EXPLAIN the event-only reader and assert:

```python
assert not any("json_each" in sql.lower() for sql in statements)
assert not any("payload_json,'$.markets'" in sql for sql in statements)
assert max(candidate_rows_per_call) <= 500
assert not any("USE TEMP B-TREE" in row for row in explain_rows)
```

The keyset uses `(market_sort_key,event_id,event_ordinal,member_ordinal)` with
separate first-page/resume SQL, never nullable-OR.

- [ ] **Step 3: Write RED completeness/diagnostic regressions**

Use 1,200 sidecar rows, duplicate identities, invalid nullable/padded fields,
certified quarantine plus global conflict, true inactive-A/active-sibling-B
cross-event membership, and generation omission. For limits 1, 17, and 500,
assert identical count/root/diagnostics/samples and exact terminal cursor.

- [ ] **Step 4: Replace raw JSON expansion with indexed sidecar reads**

Delete the `json_each`/whole-event duplicate/sibling paths from fresh projection.
Bulk-load at most 500 candidate sidecar rows, relation cardinalities, market
payloads, and quarantine receipts. Global conflict precedes duplicate/local
quarantine. Invalid sidecar rows produce nullable envelopes; only exact
11-field identities enter `projection-member`.

- [ ] **Step 5: Bind sidecar receipt to projection commitment**

Extend `FreshProjectionCommitment` identity with the validated member receipt
digest. `matches_generation()` is false when the sidecar receipt/source identity
differs even if member count/root coincidentally match.

- [ ] **Step 6: Run behavior, performance, docs, and full proportional gates**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py -k 'projection or event_member or omission or conflict'
uv run pytest -q tests/m1-perception/test_structure_drift_performance.py -k 'projection'
uv run ruff check src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_performance.py
make docs-m1-check
make planning-status
git diff --check
```

Expected: all pass; no reader call processes more than 500 candidates, no raw
event-array expansion remains, and the complete v2 gate retains >=2x the old
classifier-v1 gate.

- [ ] **Step 7: Update operator/learning docs and summary**

Document the 30-second model, sidecar derivation/seal, recovery behavior,
receipt-invalid alert, indexed projection, migration/bootstrap, and the reason
the relation table could not be extended. The SUMMARY records all RED/GREEN,
work-bound counters, performance ratios, reviews/fixes, and parent Task 3
handoff.

- [ ] **Step 8: Commit**

```bash
git add src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_performance.py docs/dev/structure-drift-operations.md docs/learning/46-Structure漂移安全切换.md docs/superpowers/plans/2026-08-03-m1-durable-event-member-staging-SUMMARY.md
git commit -m "feat(m1): project fresh structure from member sidecar"
```

Request an independent complete-amendment review. Only a clean verdict resumes
the parent plan at classifier-recovery Task 4; parent Task 3 is then recorded as
complete over commits `eb9dd8d..HEAD`.
