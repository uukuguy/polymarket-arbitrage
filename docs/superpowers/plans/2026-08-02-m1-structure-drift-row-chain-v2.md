# M1 Structure Drift Row-Chain v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace drift comparison's pure-Python serializable SHA-256 streams with an authenticated, resumable C-backed row chain and indexed member scans while preserving every fail-closed and immutable-data-plane invariant.

**Architecture:** A focused `RowChainSHA256` primitive owns the exact approved framing, domain registry, strict state codec, and finalized roots. SQLite migration transactionally versions progress/receipts, supersedes active v1 progress into a new v2 cursor-zero row, and adds ordered member indexes. Drift phases use v2 streams end to end; authorizers reject v1 drift receipts while retaining them for audit.

**Tech Stack:** Python 3.12, `hashlib`, SQLite WAL, pytest, Ruff, uv.

## Global Constraints

- Exact algorithm identifier: `row-chain-sha256-v2`.
- Preserve the byte framing, canonical JSON, domains, and empty-root definition in spec commit `530acfa` verbatim.
- Existing sealed v1 evidence remains queryable but never authorizes the v2 drift gate.
- Active v1 progress becomes stale with exact reason `drift-hash-algorithm-superseded`; v2 restarts at `source-events` cursor `NULL` in the same transaction.
- Do not mutate the generation pointer, exact receipt, publication, staging rows, legacy serving tables, read mode, or deployment state.
- No deployment in this plan.

---

### Task 1: Row-chain primitive and tamper contract

**Files:**
- Create: `src/polyarb/storage/row_chain_sha256.py`
- Create: `tests/storage/test_row_chain_sha256.py`

**Interfaces:**
- Produces: `ROW_CHAIN_SHA256_V2`, `ROW_CHAIN_DOMAINS`, and mutable `RowChainSHA256` with `new(domain)`, `from_json(encoded, expected_domain=...)`, `update(row)`, `to_json()`, and `hexdigest()`.
- Consumes: only standard-library `hashlib`, `json`, and `dataclasses`.

- [ ] **Step 1: Write RED framing, empty-root, partition, and state-decoder tests**

```python
def test_row_chain_empty_root_matches_spec_formula() -> None:
    chain = RowChainSHA256.new("source-market")
    expected = hashlib.sha256(
        _frame_for_test("root", "source-market")
        + (0).to_bytes(8, "big")
        + hashlib.sha256(_frame_for_test("init", "source-market")).digest()
    ).hexdigest()
    assert chain.hexdigest() == expected

@pytest.mark.parametrize("cuts", ((500,), (1, 499), (17, 100, 82, 301)))
def test_row_chain_root_is_chunk_boundary_independent(cuts: tuple[int, ...]) -> None:
    rows = [(index, {"z": index, "a": "值"}) for index in range(500)]
    assert resume_across(rows, cuts).hexdigest() == uninterrupted(rows).hexdigest()
```

Also parameterize every allowed domain, every strict-state rejection (extra/missing key, wrong algorithm/domain, negative/bool count, uppercase/non-hex/wrong-length state), and row/order/add/delete/duplicate/domain tampering.

- [ ] **Step 2: Run RED tests**

Run: `uv run pytest -q tests/storage/test_row_chain_sha256.py`

Expected: collection/import failure because `row_chain_sha256.py` does not exist.

- [ ] **Step 3: Implement the exact primitive**

```python
ROW_CHAIN_SHA256_V2 = "row-chain-sha256-v2"
_PREFIX = b"polyarb.structure-drift.row-chain-sha256-v2\x00"

def _frame(operation: str, domain: str) -> bytes:
    operation_bytes = operation.encode("ascii")
    domain_bytes = domain.encode("ascii")
    return (
        _PREFIX
        + len(operation_bytes).to_bytes(2, "big") + operation_bytes
        + len(domain_bytes).to_bytes(2, "big") + domain_bytes
    )

def _canonical(row: object) -> bytes:
    return json.dumps(
        row, sort_keys=True, ensure_ascii=False, allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")
```

`update()` computes the length-framed leaf and ordered chain transition; `hexdigest()` computes the count-bound root without mutating state. `from_json()` validates exact keys and lowercase canonical serialization inputs.

- [ ] **Step 4: Run GREEN tests and Ruff**

Run: `uv run pytest -q tests/storage/test_row_chain_sha256.py && uv run ruff check src/polyarb/storage/row_chain_sha256.py tests/storage/test_row_chain_sha256.py`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/storage/row_chain_sha256.py tests/storage/test_row_chain_sha256.py
git commit -m "feat(m1): add drift row-chain v2"
```

### Task 2: Crash-safe schema migration and ordered member indexes

**Files:**
- Modify: `src/polyarb/storage/schemas.py:2910-2930,3054-3146`
- Modify: `src/polyarb/storage/sqlite_store.py:130-330,1830-2030`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`

**Interfaces:**
- Produces: progress/receipt `hash_algorithm`; progress `terminal_reason`; uniqueness including algorithm; v2 member scan indexes.
- Produces internal `_migrate_structure_drift_hash_v2(con, fault_hook=None) -> None`.
- Consumes: Task 1's `ROW_CHAIN_SHA256_V2` constant.

- [ ] **Step 1: Write RED old-schema, injected-crash, re-init, lock, and plan tests**

Create a small v1-only drift schema with one active progress row and one sealed receipt. Assert:

```python
with pytest.raises(RuntimeError, match="injected-after-progress-rename"):
    _migrate_structure_drift_hash_v2(
        con,
        fault_hook=lambda step: (_ for _ in ()).throw(RuntimeError(step))
        if step == "after-progress-rename" else None,
    )
assert old_schema_and_rows_are_intact(con)
_migrate_structure_drift_hash_v2(con)
_migrate_structure_drift_hash_v2(con)  # idempotent re-init
```

Hold `BEGIN IMMEDIATE` from a second connection and prove the migration times out without a partial renamed table, then succeeds after release. On 120,000 synthetic memberships, assert startup index creation completes under the test ceiling, preserves all rows, and resumed generation/legacy `EXPLAIN QUERY PLAN` contains the v2 index and excludes `USE TEMP B-TREE FOR ORDER BY`.

- [ ] **Step 2: Run RED migration tests**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py -k 'hash_v2_migration or member_scan_index'`

Expected: failures for missing columns, helper, and indexes.

- [ ] **Step 3: Implement one-savepoint table rebuild and startup indexes**

Use one savepoint around both table rebuilds. Label copied rows `serializable-sha256-v1`; populate legacy terminal reasons deterministically; recreate append-only receipt triggers before releasing the savepoint. Add:

```sql
CREATE INDEX IF NOT EXISTS idx_structure_generation_memberships_drift_scan
ON structure_generation_memberships(
  snapshot_id,market_id,event_id,neg_risk_market_id,member_kind,active,closed
);
CREATE INDEX IF NOT EXISTS idx_event_market_memberships_drift_scan
ON event_market_memberships(
  snapshot_id,market_id,event_id,neg_risk_market_id,member_kind,active,closed
);
```

Run targeted `ANALYZE` during `init_schema()` before producers start. Branch member SQL on null/non-null cursor so resumed SQL uses `m.market_id>?` directly.

- [ ] **Step 4: Run GREEN migration/index tests and Ruff**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_projection.py -k 'migration or member_scan or source_chunk' && uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_projection.py`

Expected: all selected tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_projection.py
git commit -m "feat(m1): version drift storage and scans"
```

### Task 3: Atomic v1 supersession and v2 identity binding

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:480-520,3751-3900,4906-5115`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_generation_readers.py`

**Interfaces:**
- Consumes: Task 1 state constructor and Task 2 columns.
- Produces: v2 `comparison_id`, atomic stale/restart initialization, and v2-only drift authorization.

- [ ] **Step 1: Write RED atomicity and cross-version authorization tests**

Cover active v1 with current immutable identities, injected failure between stale CAS and v2 insert, sealed v1 receipt, exact authorization, and pointer/source identity drift. Assert active v1 supersession yields exactly:

```python
assert rows == [
    ("serializable-sha256-v1", "stale", "drift-hash-algorithm-superseded", old_cursor),
    ("row-chain-sha256-v2", "source-events", None, None),
]
assert immutable_pointer_exact_publication_and_serving_rows_after == before
```

- [ ] **Step 2: Run RED identity tests**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py -k 'algorithm or supersed or v1_receipt'`

Expected: v1 progress resumes or authorizes incorrectly.

- [ ] **Step 3: Bind algorithm and implement atomic initialization**

Add `hash_algorithm` to `_STRUCTURE_DRIFT_RECEIPT_DIGEST_FIELDS`, comparison identity JSON, comparison ID, status lookup, receipt validation, and progress selection. Within the existing `BEGIN IMMEDIATE`, revalidate identities, CAS active v1 to stale, and insert a cursor-zero v2 row before commit. Never update a sealed v1 receipt.

- [ ] **Step 4: Run GREEN identity tests and Ruff**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py -k 'drift' && uv run ruff check src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): restart drift comparison under v2"
```

### Task 4: Move every drift stream and reconstruction commitment to v2

**Files:**
- Modify: `src/polyarb/storage/sqlite_store.py:3900-4810`
- Modify: `src/polyarb/perception/structure_drift.py:80-150`
- Modify: `tests/m1-perception/test_structure_drift_end_to_end.py`
- Modify: `tests/m1-perception/test_structure_drift_projection.py`

**Interfaces:**
- Consumes: `RowChainSHA256` and the fixed domain registry.
- Produces: v2 source, group-truth, projection, generation, class, reconstruction, source-identity, and receipt roots.

- [ ] **Step 1: Write RED phase partition/tamper tests**

Run the same fixture at chunk sizes `1`, `17`, and `500`; assert byte-identical final v2 receipt/root/count/class JSON. Parameterize mutations across every canonical source/member/group/class field and prove each changes the bound root or makes the comparison stale. Assert no `SerializableSHA256` state survives in a v2 progress row.

- [ ] **Step 2: Run RED phase tests**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_projection.py -k 'chunk or tamper or row_chain'`

Expected: receipt roots differ across algorithms or v1 state remains.

- [ ] **Step 3: Replace each phase state with its exact domain**

Use row objects, not pre-encoded ad hoc JSON:

```python
source_event_chain.update((ordinal, event_id, raw))
source_market_chain.update((market_id, raw, event_ids))
projection_chain.update(expected_member_tuple)
generation_chain.update(actual_member_tuple)
class_chain.update((tag, *member_tuple))
group_truth_chain.update(group_truth_tuple)
```

Use one-row v2 commitments for `source-identity`, `legacy-reconstruction`, and `generation-reconstruction`. Final receipt creation and status validation must reject any algorithm/root/domain mismatch.

- [ ] **Step 4: Run GREEN drift suites and Ruff**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py tests/m1-perception/test_scheduler.py tests/m1-perception/test_snapshot_cli_json.py && uv run ruff check src/polyarb/storage/row_chain_sha256.py src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py`

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/perception/structure_drift.py src/polyarb/storage/sqlite_store.py tests/m1-perception/test_structure_drift_projection.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_generation_readers.py
git commit -m "feat(m1): hash drift evidence with row-chain v2"
```

### Task 5: Performance proof, operator docs, review, and complete gates

**Files:**
- Create: `tests/m1-perception/test_structure_drift_performance.py`
- Modify: `docs/dev/structure-drift-operations.md`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `docs/learning/19-M1-生产安全切换.md`

**Interfaces:**
- Consumes: completed v2 behavior.
- Produces: reproducible 120k performance evidence and operator-visible version/restart semantics.

- [ ] **Step 1: Add deterministic performance regression**

Build 120,000 lightweight production-shaped rows outside measured setup. Warm each path, compare median v1 root work with median v2 root work, and assert `v1_median / v2_median >= 2.0` for source events, source markets, and three member roots. Record query plans and member scan medians without flaky absolute CPU thresholds; retain only a generous startup index-build ceiling.

- [ ] **Step 2: Run performance and focused gates**

Run: `uv run pytest -q tests/m1-perception/test_structure_drift_performance.py tests/m1-perception/test_structure_drift_end_to_end.py tests/m1-perception/test_structure_drift_projection.py`

Expected: all pass and each measured ratio is at least 2x.

- [ ] **Step 3: Update operations and learning docs**

Document v2 algorithm/status output, the expected one-time v1 stale reason, cursor-zero restart, startup index migration, preserved exact authorization, and the prohibition on treating stale v1 progress as failure/degradation of the data plane.

- [ ] **Step 4: Run all static, documentation, and repository gates**

Run:

```bash
uv run ruff check src tests
make docs-m1-check
make planning-status
uv run pytest --collect-only -q
uv run pytest -q
git diff --check
```

Expected: Ruff/docs/planning clean; exact collected count recorded; full suite exits zero with only repository-known skip/xfail.

- [ ] **Step 5: Independent review and final commit**

Request review specifically for framing equivalence, migration rollback/re-init, algorithm downgrade/cross-version acceptance, data-plane write absence, query plans, benchmark validity, and receipt authentication. Fix blockers with RED tests before final gates.

```bash
git add tests/m1-perception/test_structure_drift_performance.py docs/dev/structure-drift-operations.md docs/M1-市场感知平台使用手册.md docs/learning/19-M1-生产安全切换.md
git commit -m "docs(m1): operate drift row-chain v2"
```
