# Structure Market Intelligence Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the raw Structure component index with a bounded, lineage-safe market research workspace.

**Architecture:** Materialize one current-generation event/group intelligence projection from certified Structure artifacts. Publish it only after coverage and capacity checks; the Dashboard uses explicit generation-bound APIs and never mixes generations.

**Tech Stack:** Python 3.12, PostgreSQL 16, psycopg 3, Alembic, FastAPI, Next.js, TypeScript, pytest, Playwright.

## Global Constraints

- M1 remains observation-only and does not claim executable price, orders, or P&L.
- Projection plus indexes are capped at 45 MB; no full markets/tags/memberships PostgreSQL mirror.
- Event JSON is capped at 4 KiB; unknown values are null plus `missing_fields`, never zero.
- Current-generation pointer gating, 1–100 list limit, allowlisted filtering/sorting, keyset pagination, and 450 MB capacity peak gate are mandatory.

---

### Task 1: Source corrections and bounded schema

**Files:** Create `alembic/versions/046_m1_structure_intelligence.py`, `tests/alembic/test_046.py`; modify `src/polyarb/control_plane/schema_contract.py`, `src/polyarb/control_plane/db_role_contract.py`, `src/polyarb/control_plane/structure_worker.py`, `tests/m1-perception/test_transactional_structure_worker.py`.

**Interfaces:** `m1_structure_intelligence_events`, `m1_structure_intelligence_groups`, and `m1_structure_intelligence_summaries`, each keyed by generation; event rows enforce `payload_octets BETWEEN 2 AND 4096`.

- [ ] **Step 1: Write failing tests.**

```python
def test_revision_046_declares_intelligence_schema() -> None:
    text = Path("alembic/versions/046_m1_structure_intelligence.py").read_text()
    assert 'revision = "046"' in text
    assert "m1_structure_intelligence_events" in text
    assert "payload_octets" in text

def test_event_index_keeps_normalized_end_time() -> None:
    payload = _business_structure_research_rows("events", ({"id": "e", "end_time_ms": 7},))[0][1]
    assert payload["end_time_ms"] == 7
```

- [ ] **Step 2: Verify RED.** Run `uv run pytest tests/alembic/test_046.py tests/m1-perception/test_transactional_structure_worker.py -k '046 or end_time' -q`; expect failure for missing revision and field.

- [ ] **Step 3: Implement minimal schema/source correction.** Create event/group/summary relations and essential generation + page indexes. Emit normalized `end_time_ms` plus actual group `quality`/`reason`; remove nonexistent `complete`/`supported` fields.

- [ ] **Step 4: Verify GREEN.** Run `uv run pytest tests/alembic/test_046.py tests/alembic/test_control_plane_schema_contract.py tests/m1-perception/test_transactional_structure_worker.py -q`; expect pass.

- [ ] **Step 5: Commit.** Stage exactly Task 1 files, then commit `feat(m1): add bounded structure intelligence schema`.

### Task 2: Candidate-bound projection and pointer-gated reads

**Files:** Create `src/polyarb/control_plane/structure_intelligence.py`, `tests/m1-perception/test_structure_intelligence.py`; modify `src/polyarb/control_plane/postgres.py`, `tests/m1-perception/test_control_plane_postgres.py`.

**Interfaces:** `build_structure_intelligence(rows_by_component) -> StructureIntelligenceBundle`; `stage_structure_intelligence`, `publish_structure_intelligence`, `structure_intelligence_summary`, `structure_intelligence_events`, `structure_intelligence_groups`, and `structure_intelligence_event`.

- [ ] **Step 1: Write failing tests.**

```python
def test_bundle_keeps_missing_source_explicit() -> None:
    bundle = build_structure_intelligence(FIXTURE_COMPONENT_ROWS)
    assert bundle.events[0].payload["market_count"] == 2
    assert bundle.events[0].payload["liquidity"] is None
    assert "liquidity" in bundle.events[0].payload["missing_fields"]

def test_reader_rejects_noncurrent_generation(control_plane) -> None:
    page = control_plane.structure_intelligence_events(generation_key="structure:old", limit=50, cursor=None, filters={})
    assert page["reason_code"] == "generation-not-current"
```

- [ ] **Step 2: Verify RED.** Run `uv run pytest tests/m1-perception/test_structure_intelligence.py -q`; expect missing builder/readers.

- [ ] **Step 3: Implement projection.** Aggregate normalized events, markets, tags and groups into event-centric rows; preserve nulls/missing fields; enforce payload cap and capacity admission before staging; publish only after expected coverage checks. Use fixed SQL maps plus bound values for filters/sorts and filter-digest-bound cursors.

- [ ] **Step 4: Verify GREEN.** Run `uv run pytest tests/m1-perception/test_structure_intelligence.py tests/m1-perception/test_control_plane_postgres.py -k 'structure_intelligence or generation_not_current' -q`; expect pass.

- [ ] **Step 5: Commit.** Stage exactly Task 2 files, then commit `feat(m1): materialize structure intelligence`.

### Task 3: Public API and capacity-fenced operation

**Files:** Modify `src/polyarb/control_plane/api.py`, `src/polyarb/cli_control_plane.py`, `Makefile`, `tests/m1-perception/test_control_plane_api.py`, `tests/m1-perception/test_control_plane_cli.py`, `tests/m1-perception/test_makefile_contract.py`.

**Interfaces:** Add `/perception/business/structure/summary`, `/events`, `/groups`, `/events/{event_id}` and `make control-plane-structure-intelligence-backfill enable=1 generation_key=<published-generation>`.

- [ ] **Step 1: Write failing route/Makefile contracts.** Assert events returns `m1.structure-intelligence-events.v1` for `limit=50&state=open&sort=ending_soon`; assert Makefile requires both `enable=1` and `generation_key`.

- [ ] **Step 2: Verify RED.** Run `uv run pytest tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_cli.py tests/m1-perception/test_makefile_contract.py -k 'structure_intelligence' -q`; expect failure.

- [ ] **Step 3: Implement guarded routes/materializer.** Bound `limit`; accept only `state=open|closed|all` and `sort=ending_soon|liquidity|volume`; read authenticated R2 artifacts in bounded batches; apply capacity gate before staging and return receipt JSON.

- [ ] **Step 4: Verify GREEN.** Repeat the Step 2 command; expect pass.

- [ ] **Step 5: Commit.** Stage exactly Task 3 files, then commit `feat(m1): expose structure intelligence API`.

### Task 4: Structure Market Intelligence Dashboard

**Files:** Create `dashboard/lib/structure-intelligence.ts`, `dashboard/app/business/structure/[eventId]/page.tsx`, `dashboard/test/structure-intelligence.test.ts`; modify `dashboard/app/business/structure/page.tsx`, `dashboard/app/business/business-ui.tsx`, `tests/m1-perception/test_business_dashboard_contract.py`.

**Interfaces:** `readStructureIntelligence(generationKey)` strictly decodes summary/events/groups and rejects generation mismatch.

- [ ] **Step 1: Write failing tests.** Assert different summary/events generations do not join; render null liquidity with visible `Source missing` rather than `0` or `—`.

- [ ] **Step 2: Verify RED.** Run `cd dashboard && npm test -- --run structure-intelligence.test.ts`; expect missing decoder/workspace.

- [ ] **Step 3: Implement workspace.** Render trust/coverage strip, universe metrics, event table (title/tags/status/end/market count/liquidity/volume/neg-risk), risk queue, lineage disclosure and event detail. Do not render generic raw `ResearchTable` for Structure.

- [ ] **Step 4: Verify GREEN.** Run `cd dashboard && npm test -- --run structure-intelligence.test.ts && npm run typecheck`; expect pass.

- [ ] **Step 5: Commit.** Stage exactly Task 4 files, then commit `feat(m1): render structure market intelligence`.

### Task 5: Production materialization, browser UAT, and guide

**Files:** Modify `docs/learning/106-M1日常业务情报操作指南.md`, `tests/m1-perception/test_m1_manual_contract.py`; create `docs/superpowers/plans/2026-09-02-structure-market-intelligence-SUMMARY.md`.

**Interfaces:** Guide distinguishes source missing from zero and explains summary/event/risk views.

- [ ] **Step 1: Write failing guide test.** Assert the guide contains `Structure Market Intelligence` and `Source missing`.

- [ ] **Step 2: Verify RED.** Run `uv run pytest tests/m1-perception/test_m1_manual_contract.py -k 'structure_source_missing' -q`; expect failure.

- [ ] **Step 3: Materialize and verify.** Run `make control-plane-capacity`, then capacity-admitted backfill for the current Structure generation, `make smoke-control-plane-prod`, and `make control-plane-business-brief format=json`. Reuse the authenticated browser session for `/business/structure` and event detail at 1440px and 375px.

- [ ] **Step 4: Verify GREEN.** Run `uv run pytest tests/m1-perception/test_m1_manual_contract.py -k 'structure_source_missing' -q && cd dashboard && npm run typecheck`; expect pass.

- [ ] **Step 5: Commit.** Stage Task 5 files, then commit `docs(m1): explain structure market intelligence`.

## Self-Review

- Tasks 1–2 cover source correctness, bounded projection, pointer fencing and capacity.
- Task 3 covers public contracts and an auditable Makefile operation.
- Task 4 replaces the raw engineering view with business research; Task 5 verifies production and teaches use.
- All shared names are consistent: `generation_key`, `missing_fields`, and `structure_intelligence`.
