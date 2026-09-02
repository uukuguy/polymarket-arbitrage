# M1 Quote Index Physical Capacity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the current Quote research page available while returning a superseded candidate generation's physical PostgreSQL space after every successful certification.

**Architecture:** Quote batches write a candidate generation to a separate staging table. The fenced certifier replaces the active reader table from that staging generation in one transaction, then truncates the now-empty staging table. Dashboard readers continue using only the active table.

**Tech Stack:** Python 3.12, psycopg 3, PostgreSQL 16, Alembic, pytest, Fly, Supabase.

## Global Constraints

- M1 remains read-only market perception: no trading, order, P&L, or opportunity-policy change.
- `m1_business_quote_rows` remains the only dashboard reader authority and never exposes an uncertified candidate.
- Pointer switch, active-row replacement, and staging cleanup share the same fenced certification transaction.
- Use `uv`; every schema change has an Alembic contract test and production migration proof.
- Treat `available + count=0` as a real zero; availability must never be fabricated by the capacity change.

---

### Task 1: Add a fenced Quote staging relation

**Files:**
- Create: `alembic/versions/045_m1_quote_research_staging.py`
- Create: `tests/alembic/test_045.py`
- Modify: `src/polyarb/control_plane/schema_contract.py`
- Modify: `src/polyarb/control_plane/db_role_contract.py`

**Interfaces:** Produces `public.m1_business_quote_staging_rows(generation_key, token_id, payload)` with the same primary key/page ordering as the active table. The runtime role receives only the minimum table privileges needed to stage and promote rows.

- [ ] **Step 1: Write the failing migration and schema-head tests**

```python
def test_revision_045_declares_generation_bound_quote_staging() -> None:
    text = Path("alembic/versions/045_m1_quote_research_staging.py").read_text()
    assert 'revision = "045"' in text
    assert 'down_revision = "044"' in text
    assert "m1_business_quote_staging_rows" in text
    assert "generation_key" in text
    assert "token_id" in text
    assert "payload" in text
```

- [ ] **Step 2: Run the isolated test and verify RED**

Run: `uv run pytest tests/alembic/test_045.py -q`

Expected: FAIL because revision 045 does not exist.

- [ ] **Step 3: Implement the forward-only migration**

```python
op.create_table(
    "m1_business_quote_staging_rows",
    sa.Column("generation_key", sa.Text(), nullable=False),
    sa.Column("token_id", sa.Text(), nullable=False),
    sa.Column("payload", postgresql.JSONB(), nullable=False),
    sa.PrimaryKeyConstraint(
        "generation_key", "token_id", name="pk_m1_business_quote_staging_rows"
    ),
)
op.create_index(
    "m1_business_quote_staging_rows_page",
    "m1_business_quote_staging_rows",
    ["generation_key", "token_id"],
)
```

Set revision 045 as the sole schema head and include staging in the runtime data-role contract.

- [ ] **Step 4: Run migration contracts and verify GREEN**

Run: `uv run pytest tests/alembic/test_045.py tests/alembic/test_control_plane_schema_contract.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add alembic/versions/045_m1_quote_research_staging.py tests/alembic/test_045.py src/polyarb/control_plane/schema_contract.py src/polyarb/control_plane/db_role_contract.py
git commit -m "feat(m1): add quote research staging relation"
```

### Task 2: Stage candidates and atomically promote them at certification

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`
- Modify: `tests/m1-perception/test_transactional_quote_worker.py`

**Interfaces:** `stage_business_quote_rows()` and receipt-coupled batch commits write only to `m1_business_quote_staging_rows`. `certify_quote_generation()` invokes `_promote_staged_business_quote_rows_cursor(cursor, generation_key=...)` after its pointer fence. Dashboard readers retain their existing active-table queries.

- [ ] **Step 1: Write failing promotion tests**

```python
def test_quote_certification_replaces_active_research_rows_from_one_staged_generation(...) -> None:
    # Stage candidate rows; retain a distinct active generation.
    # Certify candidate and assert active contains candidate only, staging is empty.
    assert active_rows == [(candidate, "token-a"), (candidate, "token-b")]
    assert staged_rows == []

def test_quote_certification_refuses_staging_rows_for_a_second_generation(...) -> None:
    with pytest.raises(ControlPlaneError, match="quote staging contains another generation"):
        control_plane.certify_quote_generation(...)
```

- [ ] **Step 2: Run the tests and verify RED**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k 'staged_generation or staging_rows' -q`

Expected: FAIL because batch staging and promotion do not exist.

- [ ] **Step 3: Implement the one-transaction promotion**

```python
def _promote_staged_business_quote_rows_cursor(self, cursor, *, generation_key: str) -> None:
    cursor.execute(
        "SELECT DISTINCT generation_key FROM m1_business_quote_staging_rows "
        "WHERE generation_key <> %s LIMIT 1",
        (generation_key,),
    )
    if cursor.fetchone() is not None:
        raise ControlPlaneError("quote staging contains another generation")
    cursor.execute("DELETE FROM m1_business_quote_rows")
    cursor.execute(
        "INSERT INTO m1_business_quote_rows(generation_key, token_id, payload) "
        "SELECT generation_key, token_id, payload FROM m1_business_quote_staging_rows "
        "WHERE generation_key=%s ORDER BY token_id",
        (generation_key,),
    )
    cursor.execute("TRUNCATE public.m1_business_quote_staging_rows")
```

Call it within the existing certification cursor after the predecessor pointer fence and before enqueuing opportunity certification. Replace receipt-coupled and direct Quote index inserts with staging writes. Do not change Structure indexing or Dashboard reader SQL.

- [ ] **Step 4: Run focused worker, Postgres and Dashboard contracts**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_worker.py tests/m1-perception/test_business_dashboard_contract.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_transactional_quote_worker.py
git commit -m "fix(m1): rotate quote research staging on certification"
```

### Task 3: Deploy and prove post-publication capacity recovery

**Files:**
- Modify: `docs/learning/106-M1日常业务情报操作指南.md`
- Create: `docs/superpowers/plans/2026-09-02-m1-quote-index-physical-capacity-SUMMARY.md`

**Interfaces:** The daily guide distinguishes temporary candidate staging growth from a failure to truncate it. Production evidence binds migrated schema, a successor Quote pointer, available business overview, empty staging relation, and capacity below warning.

- [ ] **Step 1: Add the operational assertion**

Document that a certified Quote must show one active generation, zero staged rows, and a `healthy` capacity result after certification. A critical capacity state after certification is an escalation, not normal cadence.

- [ ] **Step 2: Execute migration and deploy one exact release**

Run the existing scoped migration and Fly machine update workflow against the exact revision-045 image. Do not print DSNs or mutate unrelated provider settings.

- [ ] **Step 3: Observe a full successor generation**

Run: `make smoke-control-plane-prod`, `make control-plane-business-brief`, `make control-plane-status limit=20`, and `make control-plane-capacity`.

Expected: matching Structure/Quote/Opportunity lineage, business overview available, active current Quote rows only, staging empty, and capacity below 60% after publication.

- [ ] **Step 4: Verify the Dashboard in the existing authenticated browser session**

Reload `/business`, then inspect `/business/quotes` in the same Playwright session. Expected: available Quote coverage and paginated current-generation rows, not unavailable or a fabricated zero view.

- [ ] **Step 5: Commit documentation and summary**

```bash
git add docs/learning/106-M1日常业务情报操作指南.md docs/superpowers/plans/2026-09-02-m1-quote-index-physical-capacity-SUMMARY.md
git commit -m "docs(m1): record quote index capacity recovery"
```

## Self-Review

- Single-reader authority is preserved: only `m1_business_quote_rows` serves business pages.
- Candidate rows remain non-public until existing fenced certification commits.
- `TRUNCATE` occurs only after candidate rows have copied into active rows and staging contains no other generation.
- Production acceptance requires a successor generation and physical capacity result, not merely a passing unit test.

