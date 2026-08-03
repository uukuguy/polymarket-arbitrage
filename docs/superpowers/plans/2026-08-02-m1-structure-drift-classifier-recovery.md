# M1 Structure Drift Classifier Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the deterministic stale classifier-v1 comparison with a complete, independently projected, diagnosable classifier-v2 gate that safely authorizes generation 848 or fails closed with immutable actionable evidence.

**Architecture:** Version classifier semantics independently from row-chain framing, compute a complete fresh projection from the frozen market/event union and global relation graph, add one narrowly authenticated group-ineligible removal class, and persist append-only failure receipts for deterministic stale outcomes. The scheduler starts a new contract exactly once, health exposes only receipt-validated evidence, and resident Polywatch delivers deduplicated Telegram alert/recovery messages.

**Tech Stack:** Python 3.12, SQLite, Starlette health endpoints, stdlib `hashlib/json/dataclasses`, pytest, Ruff, uv, Fly.io, resident Polywatch.

## Global Constraints

- Exact classifier contract: `structure-drift-classifier-v2`; migrated historical rows use `structure-drift-classifier-v1`.
- Hash framing remains `row-chain-sha256-v2`; add only `class/fresh-group-ineligible` and `diagnostic/unclassified` domains.
- `fresh-group-ineligible` accepts only exact 11-field legacy identity, one global event identity, standard `complete-unsupported`, and reason `standard-neg-risk-has-non-tradable-members`.
- Any `incomplete-source`, conflict, augmented group, invalid flags/membership, unknown reason, missing evidence, or identity mismatch remains unclassified.
- Fresh projection keyspace is market staging union event-member anti-join; it never derives expected rows from generation.
- Every read/write is bounded by the existing 500-row, 100-chunk, 45-second cooperative and 75-second parent contracts.
- Same frozen identity plus same stale classifier contract is never retried; a new contract or source identity starts exactly once.
- No comparison step may mutate pointer, serving, publication, generation components, frozen source, exact receipts, read mode, or Quote state.
- Production observer queries remain PK/unique-key only and must complete in under two seconds.
- No new Python dependency; use `uv`, never `pip`.
- All executable operator surfaces remain Makefile-backed.
- Every production code change follows RED → observed failure → GREEN → review → commit.
- Shared `.superpowers/sdd/*` changes are user-owned and never included in task commits.

---

## File Responsibility Map

- `src/polyarb/perception/structure_contract.py`: classifier versions, phase vocabulary, class tags, diagnostic-code order.
- `src/polyarb/perception/structure_drift.py`: canonical evidence/candidate/diagnostic types, total diagnostic decision function, pure drift classifier and projection rules.
- `src/polyarb/storage/row_chain_sha256.py`: strict registry for the two new domains.
- `src/polyarb/storage/schemas.py`: canonical v2 progress/authorization/failure-receipt schema and immutable triggers.
- `src/polyarb/storage/sqlite_store.py`: restart-safe migration, complete source projection readers, comparison state machine, terminal receipts, status validation.
- `src/polyarb/daemon/scheduler.py`: current-contract initialization before terminal short-circuit and exactly-once concurrent tick behavior.
- `src/polyarb/http/health.py`: receipt-validated Structure drift check for both strict and reachability payloads.
- `scripts/polywatch/healthz_watcher.py`: code-specific L1 drift alert priority, dedupe/reminder/recovery consumption.
- `tests/m1-perception/*`: pure classifier, schema, projection, state-machine, scheduler, health, Polywatch, performance and production-shaped regressions.
- `docs/dev/structure-drift-operations.md`, `docs/M1-市场感知平台使用手册.md`, `docs/learning/46-Structure漂移安全切换.md`: operator and learning contracts.

### Task 1: Freeze Classifier-v2 Vocabulary and Pure Classification Semantics

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py`
- Modify: `src/polyarb/perception/structure_drift.py`
- Modify: `src/polyarb/storage/row_chain_sha256.py`
- Modify: `tests/m1-perception/test_structure_drift_classification.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`
- Modify: `tests/storage/test_row_chain_sha256.py`

**Interfaces:**
- Produces: `STRUCTURE_DRIFT_CLASSIFIER_V1`, `STRUCTURE_DRIFT_CLASSIFIER_V2`, `STRUCTURE_DRIFT_CLASS_TAGS_V2`, `STRUCTURE_DRIFT_DIAGNOSTIC_CODES`.
- Produces: `FreshGroupEvidence`, `StructureDriftCandidateEnvelope`,
  `StructureDriftDiagnostic`, and `diagnose_unresolved_member` with the exact
  signature defined in Step 4.
- Produces: `StructureMemberDriftResult.diagnostics`, `diagnostic_counts`, and the `fresh-group-ineligible` class used by Task 4.

- [ ] **Step 1: Write RED vocabulary and domain tests**

Add exact assertions:

```python
assert STRUCTURE_DRIFT_CLASSIFIER_V2 == "structure-drift-classifier-v2"
assert STRUCTURE_DRIFT_CLASS_TAGS_V2 == (
    "shared", "fresh-addition", "current-nontradable",
    "event-only-quarantine", "market-side-quarantine",
    "fresh-source-absent", "fresh-group-ineligible",
    "overlap-conflict", "unclassified",
)
assert ROW_CHAIN_DOMAINS - old_domains == {
    "class/fresh-group-ineligible",
    "diagnostic/unclassified",
}
```

Run:

```bash
uv run pytest -q tests/storage/test_row_chain_sha256.py tests/m1-perception/test_structure_drift_classification.py -k 'classifier_contract or domain_registry'
```

Expected: RED because the constants and domains do not exist.

- [ ] **Step 2: Write RED sibling and global-conflict classifier tests**

Build a two-member legacy group. For fresh evidence, make member A inactive and
member B active with global group truth:

```python
FreshGroupEvidence(
    event_id="event-1",
    group_id="group-1",
    neg_risk_type="standard",
    quality="complete-unsupported",
    reason="standard-neg-risk-has-non-tradable-members",
    membership_hash=expected_hash,
    global_relation_conflict=False,
)
```

Assert A is `current-nontradable`, B is `fresh-group-ineligible`, and no row
is unclassified. Add a second fixture where B also appears in event 2 and assert
global `incomplete-source/conflicting-event-membership` wins and B is
unclassified with diagnostic code `conflicting-event-membership`.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_classification.py -k 'group_ineligible or global_conflict'
```

Expected: RED; the active sibling remains unclassified and no diagnostic exists.

- [ ] **Step 3: Write RED total/exclusive decision-table tests**

Parameterize both one-sided paths across the 19 codes from the design. For every
case assert exactly one diagnostic, exact side/code, canonical nullable envelope,
and no class assignment. Add predicate permutations where two lower-priority
predicates are true and assert the first-match code wins.

```python
result = diagnose_unresolved_member(
    side="legacy-only",
    member=member,
    evidence=evidence,
    authorized_removal_reasons=(),
)
assert result.code == "other-zero-removal-reason"
assert result.side == "legacy-only"
assert result.envelope.identity_fields["market_id"] == "market-1"
```

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_classification.py -k 'diagnostic_total or diagnostic_precedence or nullable_envelope'
```

Expected: RED because the decision function and envelope are absent.

- [ ] **Step 4: Implement the minimal pure contract**

Add immutable dataclasses and pure functions. The group-ineligible predicate
must be explicit:

```python
@dataclass(frozen=True)
class FreshGroupEvidence:
    event_id: str
    group_id: str
    neg_risk_type: str
    quality: str
    reason: str | None
    membership_hash: str
    global_relation_conflict: bool


@dataclass(frozen=True)
class StructureDriftCandidateEnvelope:
    side: Literal["legacy-only", "generation-only"]
    event_id: str | None
    group_id: str | None
    market_id: str
    member_kind: str | None
    active: bool | None
    closed: bool | None
    condition_id: str | None
    yes_token_id: str | None
    no_token_id: str | None
    neg_risk: bool | None
    incomplete: bool | None
    source_ordinal: int | None
    member_ordinal: int | None
    raw_event_hash: str | None
    raw_market_hash: str | None


@dataclass(frozen=True)
class StructureDriftDiagnostic:
    side: Literal["legacy-only", "generation-only"]
    code: str
    envelope: StructureDriftCandidateEnvelope
    predicate_bits: tuple[bool, ...]


def _is_fresh_group_ineligible(
    member: StructuralMemberIdentity,
    evidence: FreshMemberEvidence,
) -> bool:
    truth = evidence.group_truth
    return (
        evidence.generation_certified
        and evidence.event_source_count == 1
        and evidence.exact_source_member == member
        and evidence.current_active
        and not evidence.current_closed
        and not evidence.event_only_quarantine
        and not evidence.market_side_quarantine
        and truth is not None
        and truth.event_id == member.event_id
        and truth.group_id == member.group_id
        and truth.neg_risk_type == "standard"
        and truth.quality == "complete-unsupported"
        and truth.reason == "standard-neg-risk-has-non-tradable-members"
        and not truth.global_relation_conflict
    )
```

Implement the ordered diagnostic table as one function whose branches appear in
the same order as `STRUCTURE_DRIFT_DIAGNOSTIC_CODES`. Extend reconstruction
tags with `fresh-group-ineligible` only on the legacy side. Add the two strict
row-chain domains.

- [ ] **Step 5: Run GREEN and proportional regression**

```bash
uv run pytest -q tests/storage/test_row_chain_sha256.py tests/m1-perception/test_structure_drift_classification.py tests/m1-perception/test_structure_drift_projection.py
uv run ruff check src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_drift.py src/polyarb/storage/row_chain_sha256.py tests/storage/test_row_chain_sha256.py tests/m1-perception/test_structure_drift_classification.py
```

Expected: all pass; no existing v1 classification behavior changes unless the
caller selects classifier v2.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/perception/structure_contract.py src/polyarb/perception/structure_drift.py src/polyarb/storage/row_chain_sha256.py tests/storage/test_row_chain_sha256.py tests/m1-perception/test_structure_drift_classification.py tests/m1-perception/test_structure_drift_projection.py
git commit -m "feat(m1): define drift classifier v2"
```

Request independent review for exclusivity, global-conflict precedence, class
reconstruction, domain strictness, and accidental v1 behavior changes.

### Task 2: Version Authority Storage and Add Immutable Terminal Receipts

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`

**Interfaces:**
- Consumes: Task 1 classifier constants and domain registry.
- Produces: v2 progress/receipt columns, `structure_generation_drift_terminal_receipts`, migration, digest helpers, and classifier-bound comparison IDs for Tasks 4–6.

- [ ] **Step 1: Write RED fresh/migrated schema lockstep tests**

Assert progress and authorization receipt contain
`classifier_contract_version`. Assert progress contains canonical diagnostic
JSON/state fields. Assert the terminal table contains comparison identity,
terminal reason, class/diagnostic commitments, samples JSON/digest, timestamps,
and receipt digest, plus update/delete rejection triggers.

Downgrade a fixture to the c3f1d7c shape, migrate twice, and compare
`PRAGMA table_info`, indexes, and triggers against a fresh database.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py -k 'classifier_schema or terminal_receipt_schema or classifier_migration'
```

Expected: RED on missing columns/table/triggers.

- [ ] **Step 2: Write RED rollback and historical labeling tests**

Inject failures after progress rename, authorization-receipt rename, and terminal
table creation. Assert the transaction restores the old authority schema and
all business-table counts. Re-run `init_schema()` twice and assert recovery.
Assert historical c3f1d7c progress/receipts are labeled
`structure-drift-classifier-v1`.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py -k 'classifier_migration_rollback or historical_classifier_label'
```

Expected: RED before the migration exists.

- [ ] **Step 3: Write RED comparison identity and receipt-digest tests**

Create otherwise identical v1/v2 identities and assert distinct comparison IDs.
Using a test-side fixed field tuple and manual SHA-256 oracle, prove that changing
classifier version, terminal reason, diagnostic counts/root, samples JSON, or
samples digest invalidates the corresponding receipt.

```python
assert v1_id != v2_id
assert manual_terminal_digest(payload) == stored_digest
assert status_after_tamper["reason"] == "structure-drift-terminal-receipt-invalid"
```

- [ ] **Step 4: Implement schema and restart-safe migration**

Add canonical DDL and rebuild helpers under one savepoint. Define separate fixed
field tuples and digest functions for authorization and terminal receipts. The
terminal receipt is append-only with exact trigger errors:

```text
structure-drift-terminal-receipt-sealed
```

Include classifier version in progress/receipt uniqueness and comparison identity.
Do not default a new v2 progress row to classifier v1; pass v2 explicitly at
initialization.

- [ ] **Step 5: Run GREEN, Ruff, and data-plane immutability checks**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py -k 'schema or migration or classifier or terminal_receipt'
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py
git diff --check
```

Expected: all pass; pointer/publication/source/serving/generation row counts and
digests are byte-identical before/after migration.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): version drift classifier authority"
```

Request review for rebuild rollback, historical audit compatibility, fixed-field
independent oracle, uniqueness, triggers, and data-plane write absence.

### Task 3: Build the Complete Global Fresh Projection Reader

> **Blocking prerequisite added 2026-08-03:** execute and independently review
> `2026-08-03-m1-durable-event-member-staging.md` first. Raw event JSON cannot
> satisfy the database-side 500-member work bound. This task is complete only
> when its event-only reader consumes the sealed per-ordinal sidecar and the
> amendment plan's three task gates pass.

**Files:**
- Modify: `src/polyarb/perception/structure_drift.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_drift_performance.py`

**Interfaces:**
- Consumes: Task 1 candidate/evidence types and diagnostic function.
- Produces:
  `fetch_structure_drift_fresh_projection_chunk(self, *, publication_id: str,
  generation_snapshot_id: int, cursor: FreshProjectionCursor | None,
  limit: int, trace_callback: Callable[[str], None] | None = None) ->
  FreshProjectionChunk`.
- Produces: complete projection count/root independent from generation.

- [ ] **Step 1: Write RED market-union and event-only tests**

Create a frozen window where:

- one eligible market exists in both catalogues;
- one exact event-only active member has a valid quarantine;
- one event-only active member has no valid quarantine;
- generation contains only the eligible market.

Assert the chunk returns the eligible 11-field tuple, excludes the certified
quarantine, emits `uncertified-event-only-member` for the third candidate, and
advances a deterministic union cursor.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py -k 'projection_union or uncertified_event_only'
```

Expected: RED because only generation-driven projection exists.

- [ ] **Step 2: Write RED global relation-graph tests**

Create event A with an inactive member and active sibling, then relate the sibling
to event B in the pinned relation table. Assert the reader derives
`incomplete-source/conflicting-event-membership` for the group regardless of
chunk boundary or local event order. Add chunk sizes 1, 17, and 500.

```python
assert chunks[0].diagnostics[0].code == "conflicting-event-membership"
assert all(result.root == results[0].root for result in results)
```

Expected: RED because current evidence normalization is per-member/per-event.

- [ ] **Step 3: Write RED generation-omission proof**

Construct two fully eligible fresh projected members but persist only one
generation member. Assert every emitted generation row mirrors successfully yet
the complete projection count/root differs and the gate cannot authorize.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py -k 'complete_projection_detects_generation_omission'
```

Expected: RED because the current projection iterates generation rows.

- [ ] **Step 4: Implement bounded union readers and global evidence**

Use separate cursor branches, never a nullable-OR query:

```python
@dataclass(frozen=True)
class FreshProjectionCursor:
    stream: Literal["market", "event-only"]
    market_id: str | None
    event_id: str | None
    source_ordinal: int | None
    member_ordinal: int | None


@dataclass(frozen=True)
class FreshProjectionChunk:
    cursor: FreshProjectionCursor | None
    members: tuple[StructuralMemberIdentity, ...]
    diagnostics: tuple[StructureDriftDiagnostic, ...]
    candidates_processed: int
```

For each <=500-row batch, bulk-load all relation cardinalities and event payloads
with one parameterized `IN` list, calculate global conflict before local truth, normalize the
complete 11-field tuple, and return one canonical `FreshProjectionChunk`.
Trace tests must prove no per-member SELECT and no temporary order B-tree.
Invalid candidates use the nullable canonical envelope from Task 1.

- [ ] **Step 5: Run GREEN and performance gates**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py -k 'projection or event_only or global_relation or omission'
uv run pytest -q tests/m1-perception/test_structure_drift_performance.py -k 'projection'
uv run ruff check src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_performance.py
```

Expected: all pass; each reader call processes at most 500 candidates, query
plans are indexed/keyset, and the complete projection path retains >=2x v1
estimated gate performance.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_performance.py
git commit -m "feat(m1): project complete fresh structure"
```

Request review for union completeness, global conflict precedence, 11-field
identity, duplicate handling, SQL bounds, chunk invariance, and generation
independence.

### Task 4: Integrate Classifier-v2 State, Diagnostics, and Terminal Finalization

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/perception/structure_drift.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`

**Interfaces:**
- Consumes: Task 2 authority schema and Task 3 complete projection reader.
- Produces: phase order with `fresh-projection-members`, atomic diagnostic checkpoints, sealed authorization receipts, stale terminal receipts, and receipt-validated status.

- [ ] **Step 1: Write RED phase/resume/chunk-invariance tests**

Run the production-shaped fixture at chunk sizes 1, 17, and 500. Assert identical
phase order, projection/class/diagnostic counts, roots, samples, terminal reason,
and receipt digest. Kill/reopen after each phase and after the market→event-only
union cursor boundary.

Expected phase order:

```python
(
    "source-events", "source-markets", "fresh-projection-members",
    "generation-members", "legacy-members", "fresh-group-truth",
    "sealed",
)
```

- [ ] **Step 2: Write RED stale terminal atomicity tests**

Create one unresolved conflict. Assert finalization writes
`drift-overlap-conflict`, progress stale, and exactly one immutable terminal
receipt in one transaction. Inject receipt insert failure and assert progress
remains pre-terminal. Reopen the database and assert status validates the receipt
before exposing diagnostics. Remove/tamper/mix the receipt and assert
`structure-drift-terminal-receipt-invalid` with no samples/class counts exposed.

- [ ] **Step 3: Write RED successful sibling-recovery test**

Use the confirmed two-member production mechanism. Assert inactive member count
1 in `current-nontradable`, active sibling count 1 in
`fresh-group-ineligible`, unclassified/conflict 0, complete projection equals
generation, receipt seals, and both reconstruction roots validate.

- [ ] **Step 4: Implement v2 phase and atomic checkpoints**

Initialize v2 with `fresh-projection-members` empty row-chain state and
diagnostic/unclassified empty state. Persist count, state, samples, and cursor in
the same CAS update after every chunk. Finalize class roots with
`class/fresh-group-ineligible`; include the class only in legacy reconstruction.

Keep at most the three lexicographically smallest canonical sample envelopes per
diagnostic code. On failure, insert the terminal receipt before the stale CAS
commits. On success, insert the authorization receipt and cross-bind classifier,
complete projection, generation audit/mirror, group truth, class commitments,
diagnostics, and reconstruction roots. Status selects only the current
classifier and validates the authorization receipt for sealed state or terminal
failure receipt for stale state.

- [ ] **Step 5: Run GREEN and tamper matrix**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py -k 'drift'
uv run ruff check src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py
git diff --check
```

Expected: all pass. The tamper matrix changes every new count/root/version/reason/
sample field independently; every mutation fails closed.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): seal classifier v2 drift evidence"
```

Request review for terminal atomicity, receipt authentication, diagnostic sample
bounds, class reconstruction, v1 audit visibility, and data-plane immutability.

### Task 5: Make Scheduler Contract Supersession Exactly Once

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/daemon/scheduler.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_scheduler.py`

**Interfaces:**
- Consumes: Task 2 classifier-bound identity and Task 4 terminal state machine.
- Produces: outer scheduler initialization-before-short-circuit and no same-contract retry storm.

- [ ] **Step 1: Write RED current stale-v1 recovery test**

Install the production state: current pointer identity plus a stale classifier-v1
row. Call `advance_current_structure_drift_chunk`. Assert one classifier-v2 row
is created at cursor zero and advanced, while v1 stays immutable.

Expected: RED because current code returns at `phase == "stale"` before
initialization.

- [ ] **Step 2: Write RED concurrent-tick and same-contract tests**

Launch two `_maybe_advance_structure_drift` calls against one database and
producer lock. Assert one v2 comparison and one active child attempt. Then mark
that v2 comparison stale, tick twice, and assert no new comparison/attempt.

```python
assert count_v2_progress_rows() == 1
assert count_active_drift_attempts() <= 1
assert attempts_after_same_contract_stale == attempts_before
```

- [ ] **Step 3: Implement initialization-before-terminal decision**

Resolve current expected classifier identity first. Within `BEGIN IMMEDIATE`,
revalidate current identities, supersede only older active contracts, preserve
older terminal rows, and `INSERT OR IGNORE` the deterministic v2 row. Return the
current-contract ID and inspect only that row for terminal short-circuit.

The scheduler keeps Quote double-priority, producer lock, max rows/chunks/slice,
parent timeout, and attempt ledger semantics unchanged.

- [ ] **Step 4: Run GREEN scheduler and child suites**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_scheduler.py -k 'drift or classifier'
uv run ruff check src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_scheduler.py
```

Expected: all pass; concurrent ticks start once, current stale v1 recovers, same
v2 stale does not retry, and Quote priority tests remain green.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/storage/sqlite_store.py src/polyarb/daemon/scheduler.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_scheduler.py
git commit -m "fix(m1): recover drift on classifier upgrade"
```

Request review for transaction ordering, terminal short-circuit, concurrency,
attempt truth, retry storms, and Quote priority.

### Task 6: Close Health and Resident Polywatch Chain-Truth

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`

**Interfaces:**
- Consumes: Task 4 receipt-validated status.
- Produces: code-specific `snapshot:structure_generation_drift` payload and L1 Polywatch alert/dedupe/reminder/recovery behavior.

- [ ] **Step 1: Write RED health receipt-validation tests**

For valid terminal evidence assert both health variants contain:

```python
check["status"] == "fail"
check["observedValue"] == "terminal-stale"
"drift-unclassified" in check["output"]
"other-zero-removal-reason" in check["output"]
comparison_id in check["output"]
```

For invalid/missing terminal receipt assert output is
`structure-drift-terminal-receipt-invalid` and contains no untrusted samples or
counts. For a later seal assert pass with `drift-safe-sealed`.

- [ ] **Step 2: Write RED Polywatch priority and lifecycle tests**

Build a healthz payload with both a generic cancelled snapshot attempt and a
validated drift terminal check. Assert `decide_l1` chooses the precise drift
reason first. Feed component state through alert, suppress, reminder, and
recovery ticks; assert Telegram message text includes contract/comparison/code
and recovery clears only after healthy drift status.

Run:

```bash
uv run pytest -q tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py -k 'drift'
```

Expected: RED because `decide_l1` currently prioritizes generic snapshot state.

- [ ] **Step 3: Implement bounded health and watcher projection**

Add only constant-size validated fields to the health output. In
`decide_l1`, read `snapshot:structure_generation_drift` before snapshot age
and latest attempt. Return:

```python
(
    "push",
    "L1 Structure drift terminal "
    f"(contract={contract}, comparison={comparison_id}, "
    f"reason={terminal_reason}, diagnostics={diagnostic_counts})",
)
```

Keep existing component notification state as the delivery/dedupe authority;
do not auto-unpause or mutate production state for a deterministic drift stale.

- [ ] **Step 4: Run GREEN chain and full watcher decision suite**

```bash
uv run pytest -q tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_structure_drift_end_to_end.py -k 'health or polywatch or terminal'
uv run pytest -q tests/m1-perception/test_polywatch_healthz_watcher.py
uv run ruff check src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py
```

Expected: all pass; no existing L1/L2/opportunity/dashboard decision regresses.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_structure_drift_end_to_end.py
git commit -m "feat(m1): alert on authenticated drift terminal"
```

Request chain-truth review from terminal write through status, healthz, resident
decision state, Telegram alert, dedupe/reminder, and recovery.

### Task 7: Performance, Documentation, Full Gates, and Deploy SHA

**Files:**
- Modify: `tests/m1-perception/test_structure_drift_performance.py`
- Modify: `docs/dev/structure-drift-operations.md`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `docs/learning/46-Structure漂移安全切换.md`
- Create: `docs/superpowers/plans/2026-08-02-m1-structure-drift-classifier-recovery-TASK-7-SUMMARY.md`

**Interfaces:**
- Consumes: Tasks 1–6 complete implementation.
- Produces: reproducible performance evidence, operator teaching, full repository evidence, and one independently approved exact deployment SHA.

- [ ] **Step 1: Extend deterministic performance regression**

Seed 120,000 production-shaped markets, 5,000 events, 24 members/group, global
relation conflicts, and event-only anti-join candidates outside timing. Warm each
path and record medians for complete projection, classification+diagnostics,
generation mirror, legacy scan, and terminal receipt. Assert:

```python
assert old_complete_gate_median / classifier_v2_complete_gate_median >= 2.0
assert max_child_slice_s < 45.0
assert projection_query_count <= bounded_chunk_query_budget
```

Use relative ratios only except the contractual child deadline.

- [ ] **Step 2: Run focused behavior, performance, docs, and static gates**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_classification.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_health_endpoint.py tests/m1-perception/test_polywatch_healthz_watcher.py tests/m1-perception/test_structure_drift_performance.py
uv run ruff check src tests scripts/polywatch
make docs-m1-check
make planning-status
git diff --check
```

Expected: zero failures; docs and planning gates exit zero.

- [ ] **Step 3: Update operator and learning contracts**

Document classifier v1→v2 supersession, complete projection union, global
conflict precedence, group-ineligible semantics, terminal failure receipt,
diagnostic codes/samples, Polywatch alert/recovery, same-contract no-retry, and
the two-step generation-read/Quote rollout. Include 30-second mental model,
current file:line references, trade-offs, adversarial self-check questions, and
FAQ increment.

- [ ] **Step 4: Run complete repository gates**

```bash
uv run pytest --collect-only -q
uv run pytest -q --junitxml=/tmp/m1-classifier-v2-full.xml
uv run ruff check src tests scripts
make docs-m1-check
make planning-status
git diff --check
```

Expected: collection count is recorded; JUnit has zero failures/errors and only
repository-known skip/xfail entries.

- [ ] **Step 5: Write Task 7 summary and commit**

The summary records every RED failure, GREEN command, performance ratio, full
test count/time, review finding/fix, and deploy candidate SHA.

```bash
git add tests/m1-perception/test_structure_drift_performance.py docs/dev/structure-drift-operations.md docs/M1-市场感知平台使用手册.md docs/learning/46-Structure漂移安全切换.md docs/superpowers/plans/2026-08-02-m1-structure-drift-classifier-recovery-TASK-7-SUMMARY.md
git commit -m "docs(m1): operate drift classifier v2"
```

- [ ] **Step 6: Independent final review**

Review the complete range from this plan's design commit through HEAD for:
classification exclusivity, global projection completeness, terminal receipt
immutability, migration rollback, scheduler retry semantics, health/Polywatch
chain-truth, performance methodology, exact tests, and dirty-file exclusion.
Only an explicit `DEPLOY_SHA_APPROVE <40-char SHA>` authorizes Task 8.

### Task 8: Exact Production Rollout and Natural Acceptance

**Files:**
- Modify after evidence: `.planning/JOURNAL.md`
- Modify after evidence: `.planning/threads/market-observation-architecture.md`
- Create: `docs/superpowers/plans/2026-08-02-m1-structure-drift-classifier-recovery-TASK-8-SUMMARY.md`

**Interfaces:**
- Consumes: Task 7 exact approved SHA.
- Produces: sealed drift-safe receipt, generation read cutover, Quote restoration, natural-generation evidence, and planning handoff to candidate lifecycle qualification.

- [ ] **Step 1: Deploy exact SHA in protected mode**

Build from a detached exact-SHA worktree. Deploy one image to both machines with
`Structure=true`, `Quote=false`, `read_mode=legacy`, drift enabled, and
500/100/45 unchanged. Verify release source SHA, tag, manifest digest, env, app
service check, cron state, and volume before proceeding.

- [ ] **Step 2: Prove natural classifier supersession and immutable data plane**

Using only <2-second PK/unique-key queries, prove classifier-v1 stale evidence is
unchanged, classifier-v2 starts at cursor zero, and pointer 848/publication/
window/exact receipt/legacy serving remain unchanged. Record every natural drift
attempt's phase, cursor, rows, chunks, elapsed time, and failure.

- [ ] **Step 3: Observe natural seal and run read-only gate**

Wait for the scheduler to reach one valid classifier-v2 authorization receipt.
No manual advance, rewind, pointer write, restart, or full observer scan is
allowed. Run:

```bash
make structure-generation-drift-compare
make polywatch-healthz-dry
```

Expected: comparator exits zero with `drift-safe-sealed`; Polywatch reports no
unresolved drift incident and resident state/log evidence is current.

- [ ] **Step 4: Switch generation read with Quote still disabled**

Create a config-only release of the same image with
`read_mode=generation`, `Quote=false`. Verify generation reader identity,
strict non-Quote health gates, pointer/publication/receipt consistency, and no
legacy/generation mixing.

- [ ] **Step 5: Enable Quote on the same image**

Create a second config-only release with `Quote=true`. Verify strict
`/health`, `quote_feed:last_complete_age_seconds < 300`, collector state,
opportunity endpoint HTTP !=503, and authenticated feed revision.

- [ ] **Step 6: Observe one complete natural Structure generation**

From a new natural window's zero state through publication and Quote handoff,
record continuous two-minute samples proving Quote age always <300 and
opportunity never 503. Require no mixed identity, timeout, observer-induced
failure, or manual recovery. Require scheduler failure counter to reset through
natural success.

- [ ] **Step 7: Close artifacts and route the remaining M1 gate**

Update Journal/thread, write Task 8 summary, and run `make planning-status`.
Commit only the closure artifacts. Then execute the existing
`docs/superpowers/plans/2026-08-01-m1-candidate-lifecycle-queue.md`; M1 is not
complete until that lifecycle plan and final production UAT pass.
