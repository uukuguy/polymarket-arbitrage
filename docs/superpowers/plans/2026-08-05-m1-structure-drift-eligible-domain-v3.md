# M1 Structure Drift Eligible-Domain Classifier v3 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an authenticated classifier-v3 partition that separates eligible standard neg-risk members, reason-bound expected exclusions, and genuinely unresolved source candidates so the current production identity can seal without weakening fail-closed behavior.

**Architecture:** Extend the independent fresh-projection stream with a third canonical outcome, `FreshProjectionExclusion`, and domain-separated count/root commitments per closed exclusion reason. Version persistence and receipt hashing under `structure-drift-classifier-v3`, preserve immutable v1/v2 evidence, and require exact candidate conservation plus the existing projection/generation equality before authorization.

**Tech Stack:** Python 3.12, dataclasses, SQLite, `RowChainSHA256`, pytest, uv, Ruff, Fly.io production read-only verification.

## Global Constraints

- The supported serving universe remains active/open named members of standard `complete-supported` neg-risk groups.
- Exact classifier contract wire value: `structure-drift-classifier-v3`.
- Exact exclusion reasons: `non-neg-risk-market`, `market-side-quarantine`, `non-neg-risk-event-member`, `current-nontradable-event-member`, `augmented-group`, `fresh-group-ineligible`, `event-only-quarantine`.
- There is no catch-all exclusion. Unknown, malformed, conflicting, duplicate, missing-quarantine, and unapproved group states remain unresolved diagnostics.
- Candidate conservation is exact: `candidate = eligible + exclusion + diagnostic`.
- Historical v1/v2 progress and receipts remain immutable and validate under their original digest field sets.
- The implementation must not switch Structure read mode, enable Quote, mutate published generation rows, or perform a manual pointer/cleanup write.
- Every behavior change follows red-green-refactor; observe each requested RED failure before editing production code.
- Package commands use `uv`; do not install with `pip`.
- Existing user-owned `.superpowers/sdd/*` modifications must remain untouched.
- No new operator command is introduced, so no Makefile target is required; verification uses existing `make test-m1`, `make docs-m1-check`, and `make planning-status` surfaces.

---

## File Structure

- `src/polyarb/perception/structure_contract.py` — owns v3 wire constants and the closed exclusion taxonomy.
- `src/polyarb/perception/structure_drift.py` — owns pure exclusion values, canonical tuples, chunk conservation, and incremental exclusion commitments.
- `src/polyarb/storage/sqlite_store.py` — owns source partitioning, v3 migration, progress checkpoints, receipt creation, and authenticated status reads.
- `src/polyarb/http/health.py` — projects authenticated v3 exclusion evidence into `/health` and `/healthz` without converting expected exclusions into failures.
- `tests/m1-perception/test_structure_drift_projection.py` — pure/chunk and market/event-only partition RED/GREEN coverage.
- `tests/m1-perception/test_structure_drift_classification.py` — diagnostic precedence and no-catch-all regression coverage.
- `tests/m1-perception/test_structure_drift_end_to_end.py` — migration, supersession, checkpoint, receipt, tamper, and terminal integration coverage.
- `tests/m1-perception/test_structure_drift_performance.py` — exact production-shaped aggregate and bounded runtime/query coverage.
- `tests/m1-perception/test_health_endpoint.py` — authenticated exclusion health output and tamper-failure coverage.
- `docs/learning/50-Classifier-v3候选守恒.md` and `docs/learning/00-INDEX.md` — operator/developer mental model.
- `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md` and `.planning/JOURNAL.md` — durable implementation and next-deploy handoff.

---

### Task 1: Pure v3 exclusion model and row-chain commitment

**Files:**
- Modify: `src/polyarb/perception/structure_contract.py:41-69`
- Modify: `src/polyarb/perception/structure_drift.py:37-205`
- Test: `tests/m1-perception/test_structure_drift_projection.py`

**Interfaces:**
- Consumes: `StructureDriftCandidateEnvelope`, `RowChainSHA256`, and `structure_drift_diagnostic_tuple`.
- Produces: `STRUCTURE_DRIFT_CLASSIFIER_V3`, `STRUCTURE_PROJECTION_EXCLUSION_REASONS`, `FreshProjectionExclusion`, `structure_projection_exclusion_tuple()`, and v3-aware `FreshProjectionChunk` / `FreshProjectionCommitment`.

- [ ] **Step 1: Write failing tests for the closed taxonomy and conservation**

Add tests that construct one member, one exclusion, and one diagnostic, then prove the exact three-way partition and reason-domain root:

```python
from polyarb.perception.structure_contract import (
    STRUCTURE_DRIFT_CLASSIFIER_V3,
    STRUCTURE_PROJECTION_EXCLUSION_REASONS,
)
from polyarb.perception.structure_drift import (
    FreshProjectionChunk,
    FreshProjectionCommitment,
    FreshProjectionExclusion,
    advance_fresh_projection_commitment,
    structure_projection_exclusion_tuple,
)


def _member(market_id: str) -> StructuralMemberIdentity:
    return StructuralMemberIdentity(
        event_id="event-1",
        group_id="group-1",
        market_id=market_id,
        member_kind="named",
        active=True,
        closed=False,
        condition_id=f"condition-{market_id}",
        yes_token_id=f"yes-{market_id}",
        no_token_id=f"no-{market_id}",
        neg_risk=True,
        incomplete=False,
    )


def _candidate_envelope(market_id: str) -> StructureDriftCandidateEnvelope:
    return StructureDriftCandidateEnvelope(
        side="generation-only",
        event_id=None,
        group_id=None,
        market_id=market_id,
        member_kind=None,
        active=None,
        closed=None,
        condition_id=None,
        yes_token_id=None,
        no_token_id=None,
        neg_risk=None,
        incomplete=None,
        source_ordinal=None,
        member_ordinal=None,
        raw_event_hash=None,
        raw_market_hash="b" * 64,
    )


def _v3_commitment() -> FreshProjectionCommitment:
    return FreshProjectionCommitment.initial(
        publication_id="publication-1",
        generation_snapshot_id=1,
        member_receipt_digest="a" * 64,
        classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )


def test_v3_projection_commitment_conserves_all_candidate_outcomes() -> None:
    member = _member("eligible")
    envelope = _candidate_envelope("ordinary")
    exclusion = FreshProjectionExclusion(
        reason="non-neg-risk-market",
        stream="market",
        envelope=envelope,
        group_truth=None,
    )
    diagnostic = diagnose_unresolved_member(
        side="generation-only",
        member=_candidate_envelope("unknown"),
        evidence=None,
        authorized_removal_reasons=(),
    )
    commitment = FreshProjectionCommitment.initial(
        publication_id="publication-1",
        generation_snapshot_id=1,
        member_receipt_digest="a" * 64,
        classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )
    advanced = advance_fresh_projection_commitment(
        commitment,
        FreshProjectionChunk(
            cursor=None,
            members=(member,),
            diagnostics=(diagnostic,),
            candidates_processed=3,
            exclusions=(exclusion,),
        ),
    )
    assert advanced.complete is True
    assert advanced.candidates_processed == 3
    assert advanced.member_count == 1
    assert advanced.exclusion_count == 1
    assert advanced.diagnostic_count == 1
    assert advanced.exclusion_counts == {"non-neg-risk-market": 1}
    assert set(advanced.exclusion_roots) == {"non-neg-risk-market"}
    assert structure_projection_exclusion_tuple(exclusion)[0] == (
        "non-neg-risk-market"
    )


def test_v3_projection_commitment_rejects_nonconserving_chunk() -> None:
    commitment = _v3_commitment()
    with pytest.raises(ValueError, match="fresh-projection-candidate-conservation"):
        advance_fresh_projection_commitment(
            commitment,
            FreshProjectionChunk(
                cursor=None,
                members=(),
                diagnostics=(),
                candidates_processed=1,
                exclusions=(),
            ),
        )


def test_v3_projection_exclusion_reason_is_closed() -> None:
    assert STRUCTURE_PROJECTION_EXCLUSION_REASONS == (
        "non-neg-risk-market",
        "market-side-quarantine",
        "non-neg-risk-event-member",
        "current-nontradable-event-member",
        "augmented-group",
        "fresh-group-ineligible",
        "event-only-quarantine",
    )
    with pytest.raises(ValueError, match="invalid-projection-exclusion-reason"):
        FreshProjectionExclusion(
            reason="other",
            stream="market",
            envelope=_candidate_envelope("other"),
            group_truth=None,
        )
```

- [ ] **Step 2: Run the new tests and observe RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
  -k 'v3_projection_commitment or v3_projection_exclusion_reason'
```

Expected: collection/import failure because the v3 constants and exclusion type do not exist.

- [ ] **Step 3: Implement the minimal pure model**

Add the exact constants, a validated frozen exclusion value, canonical serialization, and v3-only conservation. Preserve v2 behavior by defaulting old callers to v2:

```python
STRUCTURE_DRIFT_CLASSIFIER_V3 = "structure-drift-classifier-v3"
STRUCTURE_PROJECTION_EXCLUSION_REASONS = (
    "non-neg-risk-market",
    "market-side-quarantine",
    "non-neg-risk-event-member",
    "current-nontradable-event-member",
    "augmented-group",
    "fresh-group-ineligible",
    "event-only-quarantine",
)


@dataclass(frozen=True)
class FreshProjectionExclusion:
    reason: str
    stream: Literal["market", "event-only"]
    envelope: StructureDriftCandidateEnvelope
    group_truth: FreshGroupEvidence | None

    def __post_init__(self) -> None:
        if self.reason not in STRUCTURE_PROJECTION_EXCLUSION_REASONS:
            raise ValueError("invalid-projection-exclusion-reason")
        if self.stream not in {"market", "event-only"}:
            raise ValueError("invalid-projection-exclusion-stream")


def structure_projection_exclusion_tuple(
    exclusion: FreshProjectionExclusion,
) -> tuple[object, ...]:
    truth = exclusion.group_truth
    return (
        exclusion.reason,
        exclusion.stream,
        *exclusion.envelope.identity_fields.values(),
        exclusion.envelope.source_ordinal,
        exclusion.envelope.member_ordinal,
        exclusion.envelope.raw_event_hash,
        exclusion.envelope.raw_market_hash,
        None if truth is None else truth.event_id,
        None if truth is None else truth.group_id,
        None if truth is None else truth.neg_risk_type,
        None if truth is None else truth.quality,
        None if truth is None else truth.reason,
        None if truth is None else truth.membership_hash,
    )
```

Add these exact fields to `FreshProjectionCommitment` after
`member_receipt_digest` and before `cursor`:

```python
classifier_contract_version: str
```

Add these exact fields after `member_digest_state` and before
`diagnostic_count`:

```python
exclusion_count: int
exclusion_counts_json: str
exclusion_digest_states_json: str
```

`initial()` defaults `classifier_contract_version` to v2 for compatibility and
initializes the v3 exclusion JSON objects to `{}`. Expose parsed
`exclusion_counts: dict[str, int]` and final
`exclusion_roots: dict[str, str]` properties. Update one `RowChainSHA256` domain
per reason, reject unknown keys, and require the v3 three-way count equality
before returning an advanced commitment.

- [ ] **Step 4: Run pure tests and the full projection file**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py
```

Expected: all tests pass; existing v2 fixtures retain their old two-outcome semantics.

- [ ] **Step 5: Commit the pure model**

```bash
git add src/polyarb/perception/structure_contract.py \
  src/polyarb/perception/structure_drift.py \
  tests/m1-perception/test_structure_drift_projection.py
git commit -m "feat(m1): commit classifier v3 exclusions"
```

---

### Task 2: Partition market and event-only candidates without masking defects

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:5922-6821`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`
- Modify: `tests/m1-perception/test_structure_drift_classification.py`

**Interfaces:**
- Consumes: Task 1's `FreshProjectionExclusion`, closed reason tuple, and canonical candidate envelope.
- Produces: `fetch_structure_drift_fresh_projection_chunk(publication_id: str, generation_snapshot_id: int, cursor: FreshProjectionCursor | None, limit: int, classifier_contract: str = STRUCTURE_DRIFT_CLASSIFIER_V2, trace_callback: Callable[[str], None] | None = None, inspection_callback: Callable[[str, int], None] | None = None, sqlite_progress_callback: Callable[[], int] | None = None) -> FreshProjectionChunk`, retaining the v2 compatibility path.

- [ ] **Step 1: Write RED tests for every v3 market exclusion and malformed lookalike**

Use `_published_source_store()` to create these distinct cases and assert the exact outcome:

```python
def _fetch_v3_chunk(store: SQLiteStore) -> FreshProjectionChunk:
    return store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )


@pytest.mark.parametrize(
    ("store_kwargs", "reason"),
    [
        ({"raw_market_overrides": {"negRisk": False}}, "non-neg-risk-market"),
        ({"orphan_market": True}, "market-side-quarantine"),
        ({"raw_event_overrides": {"negRiskAugmented": True}}, "augmented-group"),
        (
            {"raw_member_overrides": {"active": False}},
            "fresh-group-ineligible",
        ),
    ],
)
def test_v3_market_candidate_has_expected_exclusion(
    tmp_path: Path, store_kwargs: dict[str, object], reason: str
) -> None:
    store = _published_source_store(tmp_path, event_count=1, **store_kwargs)
    chunk = store.fetch_structure_drift_fresh_projection_chunk(
        publication_id="publication-1",
        generation_snapshot_id=1,
        cursor=None,
        limit=500,
        classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
    )
    assert chunk.members == ()
    assert [row.reason for row in chunk.exclusions] == [reason]
    assert chunk.diagnostics == ()
    assert chunk.candidates_processed == 1


def test_v3_missing_neg_risk_boolean_is_not_an_expected_exclusion(
    tmp_path: Path,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        raw_market_overrides={"negRisk": None},
    )
    chunk = _fetch_v3_chunk(store)
    assert chunk.exclusions == ()
    assert [row.code for row in chunk.diagnostics] == [
        "invalid-neg-risk-classification"
    ]
```

The quarantine fixture must insert the exact generated issue. Add a paired
test that changes one hash byte and asserts `market-side-quarantine` is not
emitted and the row remains diagnostic.

- [ ] **Step 2: Write RED tests for ordinary, non-tradable, and quarantined event-only candidates**

```python
def _all_v3_chunks(store: SQLiteStore) -> FreshProjectionChunk:
    cursor = None
    members: list[StructuralMemberIdentity] = []
    exclusions: list[FreshProjectionExclusion] = []
    diagnostics: list[StructureDriftDiagnostic] = []
    processed = 0
    while True:
        chunk = store.fetch_structure_drift_fresh_projection_chunk(
            publication_id="publication-1",
            generation_snapshot_id=1,
            cursor=cursor,
            limit=500,
            classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
        )
        members.extend(chunk.members)
        exclusions.extend(chunk.exclusions)
        diagnostics.extend(chunk.diagnostics)
        processed += chunk.candidates_processed
        cursor = chunk.cursor
        if cursor is None:
            return FreshProjectionChunk(
                cursor=None,
                members=tuple(members),
                diagnostics=tuple(diagnostics),
                candidates_processed=processed,
                exclusions=tuple(exclusions),
            )


@pytest.mark.parametrize(
    ("event_overrides", "member_overrides", "issue", "reason"),
    [
        (
            {"negRisk": False, "enableNegRisk": False, "negRiskMarketID": None},
            {"active": True, "closed": False},
            False,
            "non-neg-risk-event-member",
        ),
        ({}, {"active": False, "closed": False}, False,
         "current-nontradable-event-member"),
        ({}, {"active": True, "closed": True}, False,
         "current-nontradable-event-member"),
        ({}, {"active": True, "closed": False}, True,
         "event-only-quarantine"),
    ],
)
def test_v3_event_only_candidate_has_one_expected_outcome(
    tmp_path: Path,
    event_overrides: dict[str, object],
    member_overrides: dict[str, object],
    issue: bool,
    reason: str,
) -> None:
    store = _published_source_store(
        tmp_path,
        event_count=1,
        raw_event_overrides=event_overrides,
        raw_member_overrides=member_overrides,
        event_only_members=(("event-only", issue),),
    )
    event_only = _all_v3_chunks(store).exclusions
    assert [row.reason for row in event_only if row.stream == "event-only"] == [reason]
```

Add explicit RED cases for invalid `active`, duplicate identity, global
conflict, missing exact quarantine, and an unknown `complete-unsupported`
reason. Each must have zero exclusions and one existing diagnostic.

- [ ] **Step 3: Run the partition tests and observe RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_drift_classification.py \
  -k 'v3_market_candidate or v3_event_only_candidate or expected_exclusion'
```

Expected: failure because the reader has no contract parameter and still emits v2 diagnostics.

- [ ] **Step 4: Implement the ordered v3 partition**

Add `classifier_contract` to both public/private projection readers, validate it
against v2/v3, and keep the existing branch untouched for v2. In the v3 branch,
append exactly one outcome through small local helpers:

```python
def add_exclusion(
    *,
    reason: str,
    stream: Literal["market", "event-only"],
    envelope: StructureDriftCandidateEnvelope,
    truth: FreshGroupEvidence | None,
) -> None:
    exclusions.append(FreshProjectionExclusion(reason, stream, envelope, truth))


if raw_market.get("negRisk") is False:
    add_exclusion(
        reason="non-neg-risk-market",
        stream="market",
        envelope=diagnostic_envelope,
        truth=None,
    )
    continue
if type(raw_market.get("negRisk")) is not bool:
    diagnostics.append(
        diagnose_unresolved_member(
            side="generation-only",
            member=diagnostic_envelope,
            evidence=FreshMemberEvidence(
                source_present=True,
                current_active=raw_market.get("active") is True,
                current_closed=raw_market.get("closed") is True,
                projector_matches=False,
                generation_certified=True,
                event_only_quarantine=False,
                market_side_quarantine=False,
                absent_from_event_catalog=not event_ids,
                absent_from_market_catalog=False,
                invalid_neg_risk_classification=True,
            ),
            authorized_removal_reasons=(),
        )
    )
    continue
if exact_market_quarantine_matches:
    add_exclusion(
        reason="market-side-quarantine",
        stream="market",
        envelope=diagnostic_envelope,
        truth=None,
    )
    continue
```

Implement the remaining order exactly as sections 4.1 and 4.2 of the approved
spec. Do not infer quarantine from payload shape alone: bulk-load the immutable
`structure_generation_issues` row and require exact recomputation. Return:

```python
return FreshProjectionChunk(
    cursor=next_cursor,
    members=tuple(members),
    diagnostics=tuple(diagnostics),
    candidates_processed=len(candidates),
    exclusions=tuple(exclusions),
)
```

- [ ] **Step 5: Run projection/classification regression tests**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_drift_classification.py
```

Expected: all tests pass, including existing conflict and quarantine tamper cases.

- [ ] **Step 6: Commit the source partition**

```bash
git add src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_drift_classification.py
git commit -m "fix(m1): partition classifier v3 candidate domain"
```

---

### Task 3: Version schema and receipt digests without rewriting v2 evidence

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:970-1285,1482-1595,2890-2960`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py:1-2200`

**Interfaces:**
- Consumes: v3 contract constant and reason-keyed exclusion JSON from Tasks 1-2.
- Produces: `_migrate_structure_drift_classifier_v3_exclusions()`, contract-aware receipt field selectors, and nullable v3 database columns that are mandatory only for v3 rows.

- [ ] **Step 1: Write RED migration tests**

Extend the schema expectations and create a pre-v3 database containing one
sealed v2 authorization and one stale v2 terminal receipt. Reopen with the new
schema and assert:

```python
_V3_RECEIPT_EXCLUSION_FIELDS = {
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
}
_V3_PROGRESS_EXCLUSION_FIELDS = {
    *_V3_RECEIPT_EXCLUSION_FIELDS,
    "projection_exclusion_digest_states_json",
}


def test_v3_migration_preserves_v2_receipt_bytes_and_adds_nullable_fields(
    tmp_path: Path,
) -> None:
    # Add `_downgrade_to_classifier_v2_shape()` beside the existing v1
    # downgrade helper. It rebuilds only the three drift authority tables,
    # omitting the four v3 columns, then restores their existing immutable
    # triggers. `_authority_row()` returns the exact tuple from
    # `SELECT * FROM <authority_table> WHERE comparison_id=?`.
    store, v2_authorization, v2_terminal = _pre_v3_receipt_store(tmp_path)
    store.init_schema()
    with sqlite3.connect(store.db_path) as con:
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_receipts"
        )
        assert _V3_RECEIPT_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_terminal_receipts"
        )
        assert _V3_PROGRESS_EXCLUSION_FIELDS <= _columns(
            con, "structure_generation_drift_progress"
        )
        assert _receipt_bytes(con, v2_authorization[0]) == v2_authorization
        assert _terminal_bytes(con, v2_terminal[0]) == v2_terminal
```

Add fault-hook cases after each `ALTER TABLE` group and assert transaction
rollback leaves the pre-v3 schema intact.

- [ ] **Step 2: Write RED digest-oracle tests**

```python
def test_v3_receipt_digest_binds_every_exclusion_field() -> None:
    payload = _valid_v3_authorization_payload()
    expected = _independent_sha256(
        tuple(payload[field] for field in _V3_AUTHORIZATION_FIELDS)
    )
    assert _structure_drift_receipt_digest(payload) == expected
    for field in _V3_RECEIPT_EXCLUSION_FIELDS:
        assert _structure_drift_receipt_digest(
            {**payload, field: _changed(payload[field])}
        ) != expected


def test_v2_receipt_digest_field_oracle_is_unchanged() -> None:
    payload = _valid_v2_authorization_payload()
    assert _structure_drift_receipt_digest(payload) == _existing_v2_digest(payload)
```

Repeat for terminal receipt payloads.

Implement the test helpers in this task, not in production code:

```python
def _columns(con: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in con.execute(f"PRAGMA table_info({table})")}


def _independent_sha256(values: tuple[object, ...]) -> str:
    return hashlib.sha256(
        json.dumps(values, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def _changed(value: object) -> object:
    if type(value) is int:
        return value + 1
    return f"changed-{value}"
```

`_pre_v3_receipt_store()` must use existing `_drift_store()`,
`_install_sealed_drift_authority()`, `_terminal_receipt_payload()`, and the new
`_downgrade_to_classifier_v2_shape()`; it returns the store plus exact `SELECT
*` tuples captured after downgrade and before `init_schema()`. Payload helpers
must derive values from the existing `_STRUCTURE_DRIFT_*_FIELDS_V2` tuples and
the exact four v3 fields, so the independent oracle never calls the production
digest helper.

- [ ] **Step 3: Run schema/digest tests and observe RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  -k 'v3_migration or v3_receipt_digest or v2_receipt_digest_field'
```

Expected: failures for missing columns, migration, and contract-aware digest fields.

- [ ] **Step 4: Implement the additive migration and contract-aware digests**

Add the four receipt exclusion columns to sealed and terminal receipt tables in
one savepoint. Add those same four plus
`projection_exclusion_digest_states_json` to progress. Progress uses non-null
defaults (`0`, `0`, `'{}'`, `'{}'`, `'{}'`); receipt columns remain nullable so
historical rows are byte-for-byte unchanged. Restore all immutable triggers
before releasing the savepoint.

Rename the current exact field tuple to
`_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2` without changing a field or its
order, then derive v3 as follows:

```python
_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V3 = (
    *_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2[:-1],
    "projection_candidate_count",
    "projection_exclusion_count",
    "projection_exclusion_counts_json",
    "projection_exclusion_roots_json",
    "created_at_ms",
)


def _structure_drift_receipt_fields(contract: str) -> tuple[str, ...]:
    if contract in {STRUCTURE_DRIFT_CLASSIFIER_V1, STRUCTURE_DRIFT_CLASSIFIER_V2}:
        return _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V2
    if contract == STRUCTURE_DRIFT_CLASSIFIER_V3:
        return _STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS_V3
    raise ValueError("invalid-structure-drift-classifier-contract")


def _structure_drift_receipt_digest(payload: Mapping[str, object]) -> str:
    contract = str(payload.get("classifier_contract_version") or "")
    fields = _structure_drift_receipt_fields(contract)
    if set(payload) != set(fields):
        raise ValueError("invalid-structure-drift-receipt-fields")
    return _canonical_tuple_sha256(tuple(payload[field] for field in fields))
```

Apply the same pattern to terminal fields. Never update historical receipt
digests during this migration.

- [ ] **Step 5: Run migration, tamper, and full end-to-end tests**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py
```

Expected: all tests pass; v1/v2 immutable rows remain accepted.

- [ ] **Step 6: Commit schema and digest versioning**

```bash
git add src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_end_to_end.py
git commit -m "feat(m1): version classifier v3 exclusion receipts"
```

---

### Task 4: Checkpoint v3 exclusions, supersede v2, and enforce final conservation

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:7059-8558`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_scheduler.py`

**Interfaces:**
- Consumes: v3-aware commitments and schema/digest helpers.
- Produces: scheduler-owned v3 comparison initialization, exclusion checkpoint CAS, final candidate conservation, sealed/terminal v3 receipts, and unchanged bounded follow-up semantics.

- [ ] **Step 1: Write RED supersession and same-identity recovery test**

```python
def test_terminal_v2_identity_starts_v3_without_mutating_v2(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    v2_id = _complete_v2_as_stale(store)
    before = _terminal_receipt_bytes(store, v2_id)
    v3_id = store.initialize_structure_drift_comparison(now_ms=4_000)
    assert v3_id != v2_id
    assert _progress_contract(store, v3_id) == STRUCTURE_DRIFT_CLASSIFIER_V3
    assert _terminal_receipt_bytes(store, v2_id) == before
```

Add a second call assertion returning the same active v3 ID rather than
creating/retrying another row.

- [ ] **Step 2: Write RED checkpoint/restart and final conservation tests**

Build a mixed source containing one candidate per exclusion reason plus one
eligible member. Advance with limits 1, 17, and 500 and assert identical final
counts/roots:

```python
expected_counts = {
    "non-neg-risk-market": 1,
    "market-side-quarantine": 1,
    "non-neg-risk-event-member": 1,
    "current-nontradable-event-member": 1,
    "augmented-group": 1,
    "fresh-group-ineligible": 1,
    "event-only-quarantine": 1,
}
assert status["projection_candidate_count"] == 8
assert status["projection_member_count"] == 1
assert status["projection_exclusion_count"] == 7
assert status["projection_diagnostic_count"] == 0
assert status["projection_exclusion_counts"] == expected_counts
```

Tamper each count relationship independently and assert exact failure reasons:

```text
structure-drift-candidate-conservation-invalid
structure-drift-candidate-source-count-invalid
structure-drift-exclusion-commitment-invalid
```

- [ ] **Step 3: Run checkpoint tests and observe RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_scheduler.py \
  -k 'starts_v3 or v3_checkpoint or candidate_conservation'
```

Expected: v2 remains the selected contract and exclusion state is not persisted.

- [ ] **Step 4: Implement v3 initialization and checkpoint persistence**

Change the current expected contract to v3 in comparison ID generation,
initialization, member classification, and status lookup. Initialize one empty
row-chain state per exclusion reason. Pass v3 explicitly to the projection
reader and commitment:

```python
chunk = self._fetch_structure_drift_fresh_projection_chunk(
    publication_id=str(progress[1]),
    generation_snapshot_id=int(progress[0]),
    cursor=cursor,
    limit=max_rows,
    classifier_contract=STRUCTURE_DRIFT_CLASSIFIER_V3,
)
commitment = FreshProjectionCommitment(
    publication_id=str(progress[1]),
    generation_snapshot_id=int(progress[0]),
    member_receipt_digest=member_receipt_digest,
    cursor=cursor,
    candidates_processed=int(counts.get("projection_candidate_count", 0)),
    member_count=int(counts.get("projection_member_count", 0)),
    member_digest_state=member_state,
    diagnostic_count=int(counts.get("projection_diagnostic_count", 0)),
    diagnostic_digest_state=str(progress[7]),
    complete=False,
    classifier_contract_version=STRUCTURE_DRIFT_CLASSIFIER_V3,
    exclusion_count=int(progress_exclusion_count),
    exclusion_counts_json=str(progress_exclusion_counts_json),
    exclusion_digest_states_json=str(progress_exclusion_states_json),
)
```

Read the four `progress_exclusion_*` values directly from the new progress
columns selected in the same read transaction:

```python
progress_exclusion_count = int(progress[16])
progress_exclusion_counts_json = str(progress[17])
progress_exclusion_states_json = str(progress[18])
```

Pass `progress_exclusion_counts_json` and
`progress_exclusion_states_json` into the two exact commitment fields rather
than reconstructing either object from in-memory chunk results.

Persist member, exclusion, and diagnostic states in the same existing
`BEGIN IMMEDIATE`/checkpoint-CAS transaction. A crash cannot advance one
outcome without the other two.

- [ ] **Step 5: Implement independent source count and final receipt gates**

Add one read-only helper using indexed frozen tables:

```python
def _fresh_projection_expected_candidate_count(
    con: sqlite3.Connection, *, window_id: str
) -> int:
    market_count = con.execute(
        "SELECT COUNT(*) FROM structure_sync_market_staging WHERE window_id=?",
        (window_id,),
    ).fetchone()[0]
    event_only_count = con.execute(
        "SELECT COUNT(*) FROM structure_sync_event_member_staging member "
        "WHERE member.window_id=? AND NOT EXISTS (SELECT 1 FROM "
        "structure_sync_market_staging market WHERE market.window_id=member.window_id "
        "AND market.market_id=member.market_id)",
        (window_id,),
    ).fetchone()[0]
    return int(market_count) + int(event_only_count)
```

Before sealing, require source-count equality, three-way conservation, zero
diagnostics, known exclusion keys, valid reason roots/counts, and the unchanged
member/generation equality. Bind all four v3 exclusion fields into sealed or
terminal receipt insertion.

- [ ] **Step 6: Prove scheduler follow-up remains bounded**

Extend scheduler tests so an active v3 projection checkpoint with exclusion
rows sets `_checkpoint_pending=True`, while sealed/stale/no-progress results do
not. Run:

```bash
uv run pytest -q tests/m1-perception/test_scheduler.py \
  -k 'structure_generation or drift'
```

Expected: all selected tests pass, including the release-238 100 ms follow-up contract.

- [ ] **Step 7: Run end-to-end files and commit**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_scheduler.py
git add src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_scheduler.py
git commit -m "feat(m1): seal conserved classifier v3 comparisons"
```

Expected: tests pass, then the commit succeeds through `.githooks`.

---

### Task 5: Authenticate exclusion evidence in status and health

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:8560-9210`
- Modify: `src/polyarb/http/health.py`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_health_endpoint.py`

**Interfaces:**
- Consumes: v3 sealed/terminal receipts and reason-keyed roots.
- Produces: `structure_generation_drift_status()` fields `projection_candidate_count`, `projection_exclusion_count`, `projection_exclusion_counts`, and `projection_exclusion_roots`; authenticated health detail with unchanged pass/fail policy.

- [ ] **Step 1: Write RED status and health tests**

```python
def test_v3_sealed_status_exposes_authenticated_expected_exclusions(
    tmp_path: Path,
) -> None:
    store = _drift_store(tmp_path)
    comparison_id = store.initialize_structure_drift_comparison(now_ms=3_000)
    terminal = _run_drift_to_terminal(store, comparison_id)
    assert terminal == "sealed"
    status = store.structure_generation_drift_status()
    assert status["authorized"] is True
    assert status["projection_candidate_count"] == (
        status["projection_member_count"] + status["projection_exclusion_count"]
    )
    assert sum(status["projection_exclusion_counts"].values()) == (
        status["projection_exclusion_count"]
    )
    assert set(status["projection_exclusion_counts"]) == set(
        status["projection_exclusion_roots"]
    )


def test_health_expected_exclusions_are_observable_but_not_failure(
    client, sealed_v3_status: dict[str, object]
) -> None:
    response = client.get("/health")
    check = _check(response.json(), "snapshot:structure_generation_drift")
    assert check["status"] == "pass"
    assert check["projectionExclusionCount"] == 7
    assert check["projectionExclusionCounts"]["augmented-group"] == 1
```

Add tamper tests for unknown reason, count sum, one root, receipt digest, and
v3-null fields. Each status becomes unavailable/fail; none falls back to v2.

- [ ] **Step 2: Run tests and observe RED**

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_health_endpoint.py \
  -k 'v3_sealed_status or expected_exclusions or exclusion_tamper'
```

Expected: missing public fields or unauthorized v3 receipt validation.

- [ ] **Step 3: Implement contract-aware status validation**

Select the superset schema, build the payload with the field selector from
Task 3, then reject before exposure unless:

```python
valid_exclusions = (
    set(exclusion_counts) <= set(STRUCTURE_PROJECTION_EXCLUSION_REASONS)
    and set(exclusion_counts) == set(exclusion_roots)
    and all(type(value) is int and value > 0 for value in exclusion_counts.values())
    and sum(exclusion_counts.values()) == projection_exclusion_count
    and candidate_count
    == projection_member_count + projection_exclusion_count + diagnostic_count
)
```

Return canonical sorted dictionaries only after receipt digest and every
existing identity/reconstruction check passes. Historical v1/v2 status follows
its existing validator and never fabricates exclusion fields.

- [ ] **Step 4: Implement health projection**

For authenticated v3 status, add bounded camelCase fields:

```python
check["projectionCandidateCount"] = status["projection_candidate_count"]
check["projectionExclusionCount"] = status["projection_exclusion_count"]
check["projectionExclusionCounts"] = status["projection_exclusion_counts"]
check["projectionExclusionRoots"] = status["projection_exclusion_roots"]
```

Do not alter health severity for expected exclusions. Keep diagnostics,
terminal stale, invalid receipt, and unavailable evidence as failures.

- [ ] **Step 5: Run status/health tests and commit**

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_health_endpoint.py
git add src/polyarb/storage/sqlite_store.py src/polyarb/http/health.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_health_endpoint.py
git commit -m "feat(m1): expose authenticated v3 exclusions"
```

Expected: both complete files pass and the commit succeeds.

---

### Task 6: Production-shaped invariance, performance, and learning material

**Files:**
- Modify: `tests/m1-perception/test_structure_drift_performance.py`
- Create: `docs/learning/50-Classifier-v3候选守恒.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- Consumes: complete v3 pipeline and authenticated status.
- Produces: exact production-count regression, bounded performance/query evidence, and the required learning artifact.

- [ ] **Step 1: Write the exact production-shaped aggregate test**

Generate deterministic candidates in bounded chunks using the production
partition, without copying production payloads:

```python
PRODUCTION_V3_PARTITION = {
    "non-neg-risk-market": 82_346,
    "market-side-quarantine": 193,
    "augmented-group": 11_069,
    "fresh-group-ineligible": 312,
    "non-neg-risk-event-member": 13_655,
    "current-nontradable-event-member": 17_515,
    "event-only-quarantine": 68,
}
PRODUCTION_V3_ELIGIBLE = 41_768
PRODUCTION_V3_CANDIDATES = 166_926


@pytest.mark.parametrize("limit", [1, 17, 500])
def test_166926_production_shaped_v3_partition_is_chunk_invariant(limit: int) -> None:
    result = _commit_production_v3_partition(limit=limit)
    assert result.candidates_processed == PRODUCTION_V3_CANDIDATES
    assert result.member_count == PRODUCTION_V3_ELIGIBLE
    assert result.exclusion_count == 125_158
    assert result.diagnostic_count == 0
    assert result.exclusion_counts == PRODUCTION_V3_PARTITION
    assert result.root == EXPECTED_PRODUCTION_MEMBER_ROOT
    assert result.exclusion_roots == EXPECTED_PRODUCTION_EXCLUSION_ROOTS
```

Generate and freeze the golden roots only after an independent one-shot oracle
over canonical tuples matches the incremental chain for all three limits.

- [ ] **Step 2: Add bounded SQL/runtime assertions and observe RED if limits regress**

Reuse the existing 120k performance harness. Assert each projection page
processes at most 500 candidates, emits a query count independent of page size,
uses the drift-scan/sidecar indexes, and completes within the existing Makefile
performance gate. Add an `EXPLAIN QUERY PLAN` assertion that the final candidate
count anti-join does not full-scan the market table per sidecar row.

Run:

```bash
uv run pytest -q tests/m1-perception/test_structure_drift_performance.py \
  -k '166926_production_shaped_v3 or 120k_production_shaped_complete_classifier_gate'
```

Expected: PASS within the existing gate; any index/query regression fails explicitly.

- [ ] **Step 3: Write the learning document**

Create `docs/learning/50-Classifier-v3候选守恒.md` with:

- a 30-second model: complete scan does not mean every row belongs to the strategy;
- code pointers to the outcome type, ordered partition, checkpoint CAS, receipt
  finalizer, and status validator;
- why expected exclusion differs from warning/diagnostic;
- the exact `166,926 = 41,768 + 125,158 + 0` production proof;
- design trade-offs and five adversarial self-check questions;
- an empty `FAQ 增量` section.

Append document 50 to `docs/learning/00-INDEX.md` in reading order.

- [ ] **Step 4: Run docs checks and commit**

```bash
make docs-m1-check
uv run pytest -q tests/m1-perception/test_structure_drift_performance.py \
  -k '166926_production_shaped_v3 or 120k_production_shaped_complete_classifier_gate'
git add tests/m1-perception/test_structure_drift_performance.py \
  docs/learning/50-Classifier-v3候选守恒.md docs/learning/00-INDEX.md
git commit -m "test(m1): lock classifier v3 production partition"
```

Expected: docs and both performance selections pass.

---

### Task 7: Full verification, independent review, and durable handoff

**Files:**
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md`
- Modify: `.planning/JOURNAL.md`
- Modify if new reusable knowledge arose: `.planning/threads/market-observation-architecture.md`

**Interfaces:**
- Consumes: final implementation tree from Tasks 1-6.
- Produces: verified exact deploy candidate SHA and explicit production acceptance checklist; does not deploy.

- [ ] **Step 1: Run focused v3 verification from the final tree**

```bash
uv run pytest -q \
  tests/m1-perception/test_structure_drift_projection.py \
  tests/m1-perception/test_structure_drift_classification.py \
  tests/m1-perception/test_structure_drift_end_to_end.py \
  tests/m1-perception/test_structure_drift_performance.py \
  tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_scheduler.py
```

Expected: zero failures/errors.

- [ ] **Step 2: Run static and full M1 gates**

```bash
uv run ruff check src tests
uv run python -m compileall -q src
make docs-m1-check
make test-m1
make planning-status
```

Expected: Ruff/compile/docs pass, full M1 exits 0, and planning status reports no drift.

- [ ] **Step 3: Request independent code review and resolve findings with TDD**

Review the complete range from `e8ee0fe` through current HEAD for:

- candidate partition completeness and precedence;
- quarantine authenticity;
- chunk/cursor restart invariance;
- v1/v2 receipt preservation;
- v3 digest and status tamper resistance;
- scheduler immediate-follow-up and terminal no-busy-loop behavior;
- SQLite query/index bounds.

For every valid behavior finding, add a failing test, observe RED, implement the
minimal correction, rerun focused tests, and commit atomically. Do not accept a
review suggestion that weakens a fail-closed invariant.

- [ ] **Step 4: Update durable project evidence**

Append the implementation commits, exact test totals/durations, review verdict,
and protected rollout boundary to the existing `05.6-02-SUMMARY.md`. Append a
new JOURNAL session with:

```text
[FIXED LOCAL] classifier-v3 exact candidate conservation
[FULL GATE] focused/full/static/planning evidence
[BOUNDARY] no deploy/read-mode/Quote/manual production mutation
[NEXT] obtain DEPLOY_SHA_APPROVE <exact 40-char HEAD>
```

Update the architecture thread only if implementation discovered a reusable
chain-truth rule not already recorded.

- [ ] **Step 5: Commit the handoff and re-run planning guard**

```bash
git add .planning/JOURNAL.md \
  .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md
git add .planning/threads/market-observation-architecture.md  # only if changed
git commit -m "docs(m1): record classifier v3 deployment gate"
make planning-status
git status --short
git rev-parse HEAD
```

Expected: no planning drift; only the five pre-existing user-owned dirty files
remain; print one exact 40-character deploy candidate SHA.

- [ ] **Step 6: Stop at the exact-SHA approval gate**

Report the focused/full verification evidence and request:

```text
DEPLOY_SHA_APPROVE <exact 40-character HEAD>
```

Do not deploy until that exact approval is received. The later production run
must keep drift enabled, generation reads on legacy, and Quote disabled while
proving v3 seal, authenticated health, immediate checkpoint continuation, and
terminal no-busy-loop behavior.
