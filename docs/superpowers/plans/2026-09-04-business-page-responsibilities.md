# M1 Business Page Responsibilities Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Structure, Quote Coverage, and Analysis three focused business views with distinct current-lineage responsibilities.

**Architecture:** Keep current, bounded Structure and Quote projections as the only data sources. Add explicit API read modes instead of client-side filtering: Structure defaults to active/unexpired events, Quote Coverage returns group-level coverage-health facts, and Analysis ranks current positive candidates by executable economic value. The Dashboard renders each mode with its own metrics and table contract.

**Tech Stack:** Python 3.12, psycopg/PostgreSQL JSONB, Starlette, Next.js 15/TypeScript, pytest, pnpm.

## Global Constraints

- Use only the active Quote generation and its exact parent Structure generation for operational defaults.
- Closed or expired records remain bounded audit facts but never enter default operational tables or positive candidates.
- Quote Coverage is an audit/health view; it must not rank price extremity or imply opportunities.
- Analysis candidates remain non-certified; only the existing opportunity projection may claim a Certified opportunity.
- Preserve 200-row API page bounds and database read/write timeouts.
- Expose any new operator command through `Makefile`; do not restart the stopped coordinator.

---

### Task 1: Establish focused API contracts and Structure default scope

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/control_plane/api.py`
- Modify: `dashboard/lib/business-research.ts`
- Test: `tests/m1-perception/test_control_plane_api.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces `business_structure_page(*, generation_key: str | None, limit: int, after: str, open_only: bool = True)` as the default operational reader.
- Produces `business_quote_coverage_page(*, generation_key: str | None, limit: int, after: str) -> dict[str, object]`.
- Produces API routes `/perception/business/structure/events?open_only=true` and `/perception/business/quote-coverage`.

- [ ] **Step 1: Write failing route contracts.**

```python
def test_quote_coverage_route_is_not_the_quote_discovery_route() -> None:
    class Reader:
        def business_quote_coverage_page(self, *, generation_key, limit, after):
            return {"schema_version": "m1.quote-coverage-page.v1", "status": "available", "items": [], "limit": limit, "next_after": None}
    with TestClient(create_control_plane_app(control_plane=Reader())) as client:
        response = client.get("/perception/business/quote-coverage?limit=10")
    assert response.status_code == 200
    assert response.json()["schema_version"] == "m1.quote-coverage-page.v1"
```

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_control_plane_api.py -k quote_coverage -q`  
Expected: FAIL because the route is absent.

- [ ] **Step 3: Implement the API dispatch.** Add a dedicated `quote_coverage` handler that validates `generation_key`, `limit`, and `after` exactly as other business readers do, invokes `business_quote_coverage_page`, and returns 503 with `quote-coverage-unavailable` on authority failures. Do not route this request through `business_quote_page`.

- [ ] **Step 4: Make Structure operational by default.** In `StructurePage`, call `readStructureIntelligencePage("events", { openOnly: true })`; extend the TypeScript reader to append `open_only=true`. Retain an explicit archive mode only when the caller passes `openOnly: false`.

- [ ] **Step 5: Add a repository regression.** Seed one open-future, one closed, and one ended Structure event; assert the default operational page returns only the open-future event, while archive mode may read all three.

- [ ] **Step 6: Verify GREEN.**

Run: `uv run pytest tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py -k 'quote_coverage or structure_intelligence' -q`  
Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add src/polyarb/control_plane/postgres.py src/polyarb/control_plane/api.py dashboard/lib/business-research.ts tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(m1): separate structure and coverage readers"
```

### Task 2: Materialize bounded Quote Coverage health facts

**Files:**
- Create: `src/polyarb/control_plane/quote_coverage.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `src/polyarb/cli_control_plane.py`
- Modify: `Makefile`
- Test: `tests/m1-perception/test_quote_coverage.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces `build_quote_coverage_group(group, event, quotes, evaluated_at_ms) -> dict[str, object]`.
- Produces a page whose rows contain `group_id`, `coverage_state`, `expected_member_count`, `quoted_member_count`, `executable_member_count`, `freshness_state`, `event`, and optional defect codes.
- Reuses the existing current candidate projection rows and never stores individual raw order-book levels.

- [ ] **Step 1: Write failing pure-contract tests.**

```python
def test_coverage_marks_expired_event_before_quote_quality() -> None:
    row = build_quote_coverage_group(
        group={"event_id": "e", "expected_member_count": 2, "quality": "complete-supported"},
        event={"is_open": True, "end_time_ms": 1},
        quotes=(),
        evaluated_at_ms=2,
    )
    assert row["coverage_state"] == "expired-or-closed"

def test_coverage_marks_missing_executable_leg_as_actionable_defect() -> None:
    row = build_quote_coverage_group(
        group={"event_id": "e", "expected_member_count": 2, "quality": "complete-supported"},
        event={"is_open": True, "end_time_ms": 9_999_999_999_999},
        quotes=({"terminal_state": "executable", "best_ask_price": 0.4, "best_ask_size": 10},),
        evaluated_at_ms=1,
    )
    assert row["coverage_state"] == "incomplete-executable-coverage"
    assert row["defect_codes"] == ["missing-executable-leg"]
```

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_quote_coverage.py -q`  
Expected: FAIL with missing module/function.

- [ ] **Step 3: Implement bounded coverage classification.** Define states in priority order: `expired-or-closed`, `context-unavailable`, `incomplete-executable-coverage`, `stale-quote`, and `healthy`. Do not include price distance from 0.5 in the payload, score, or order.

- [ ] **Step 4: Implement the repository page.** Build group-level rows from the exact current Structure/Quote lineage. Order operational rows by state priority, then incomplete-leg count descending, then event end time ascending, then group ID. Limit to 200 and return a stable cursor.

- [ ] **Step 5: Add the explicit one-time materializer.** Follow `analysis-candidate-backfill`: check database capacity before writing, retain only current generation facts, use batched insert, and expose:

```make
make control-plane-quote-coverage-backfill enable=1 generation_key=<current-quote-generation>
```

- [ ] **Step 6: Verify GREEN.**

Run: `uv run pytest tests/m1-perception/test_quote_coverage.py tests/m1-perception/test_control_plane_postgres.py -k 'quote_coverage' -q`  
Expected: PASS.

- [ ] **Step 7: Commit.**

```bash
git add src/polyarb/control_plane/quote_coverage.py src/polyarb/control_plane/postgres.py src/polyarb/cli_control_plane.py Makefile tests/m1-perception/test_quote_coverage.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(m1): publish quote coverage health"
```

### Task 3: Rank Analysis by executable economic value

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `tests/m1-perception/test_analysis_candidates.py`
- Modify: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- `business_analysis_page` orders `positive-edge` rows by `gross_edge_bps × max_bundle_size × bundle_cost`, then `gross_edge_bps`, then `group_id`.
- All non-positive states remain after positive candidates and remain explicitly non-opportunity outcomes.

- [ ] **Step 1: Write the failing ranking test.**

```python
def test_analysis_prioritizes_executable_economic_value_not_raw_edge() -> None:
    # 100 bps × 100 bundles must precede 500 bps × 0.1 bundles.
    page = control_plane.business_analysis_page(generation_key=None, limit=10, after="")
    assert [item["group_id"] for item in page["items"][:2]] == ["deep", "thin"]
```

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k executable_economic_value -q`  
Expected: FAIL under the current gross-edge-only order.

- [ ] **Step 3: Implement only the new ordering expression.** Compute the value in SQL from persisted candidate fields or payload numeric values, require all operands to be finite positive numbers, and rank missing values below positive candidates. Do not alter candidate classification or certification.

- [ ] **Step 4: Verify GREEN.**

Run: `uv run pytest tests/m1-perception/test_analysis_candidates.py tests/m1-perception/test_control_plane_postgres.py -k 'analysis or executable_economic_value' -q`  
Expected: PASS.

- [ ] **Step 5: Commit.**

```bash
git add src/polyarb/control_plane/postgres.py tests/m1-perception/test_analysis_candidates.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(m1): rank analysis by executable value"
```

### Task 4: Render the three focused business pages

**Files:**
- Modify: `dashboard/app/business/structure/page.tsx`
- Modify: `dashboard/app/business/quotes/page.tsx`
- Modify: `dashboard/app/business/analysis/page.tsx`
- Modify: `dashboard/lib/business-research.ts`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`

**Interfaces:**
- Structure shows active-unexpired research only and an explicit archive affordance.
- Quotes consumes `readQuoteCoveragePage()` and presents coverage defects before healthy groups.
- Analysis labels positive rows as candidates and displays executable economic value.

- [ ] **Step 1: Write failing source contracts.**

```python
def test_business_pages_have_non_overlapping_responsibilities() -> None:
    structure = Path("dashboard/app/business/structure/page.tsx").read_text()
    quotes = Path("dashboard/app/business/quotes/page.tsx").read_text()
    analysis = Path("dashboard/app/business/analysis/page.tsx").read_text()
    assert "openOnly: true" in structure
    assert "readQuoteCoveragePage" in quotes
    assert "price_extremity_bps" not in quotes
    assert "executable economic value" in analysis
```

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q`  
Expected: FAIL because Quote Coverage still renders discovery score/extremity.

- [ ] **Step 3: Render Structure.** Replace the default event-list lead with “Active research universe”, showing event title, end time, liquidity/activity, active market count, and structural completeness. Move the current all-record count into secondary lineage detail.

- [ ] **Step 4: Render Quote Coverage.** Replace `Research leads` with `Coverage health`. Use columns `Coverage state`, `Event / group`, `Expected / quoted / executable`, `Freshness`, and `Action`. Do not render discovery score, price extremity, or “non-neutral YES”.

- [ ] **Step 5: Render Analysis.** Change the first metric from raw Structure/Quote counts to `eligible groups`, `positive candidates`, and `certified opportunities`; show executable economic value alongside gross edge and executable bundle size.

- [ ] **Step 6: Verify GREEN.**

Run: `make dashboard-typecheck dashboard-build && uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q`  
Expected: Typecheck, build, and tests PASS.

- [ ] **Step 7: Commit.**

```bash
git add dashboard/app/business/structure/page.tsx dashboard/app/business/quotes/page.tsx dashboard/app/business/analysis/page.tsx dashboard/lib/business-research.ts tests/m1-perception/test_business_dashboard_contract.py
git commit -m "feat(m1): focus business research pages"
```

### Task 5: Deploy and verify current business truth

**Files:**
- Modify: `docs/learning/00-INDEX.md` only if a new guide is added.
- Create: `docs/learning/12-business-research-flow.md`

**Interfaces:**
- Operators use `/business/structure`, `/business/quotes`, and `/business/analysis` as the three-step research flow.

- [ ] **Step 1: Add the short operator guide.** Explain page purpose, default scope, non-opportunity labels, archival data, and the exact three-page navigation flow.

- [ ] **Step 2: Run full verification.**

Run:
```bash
uv run pytest tests/m1-perception/test_quote_coverage.py tests/m1-perception/test_analysis_candidates.py tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_business_dashboard_contract.py -q
make dashboard-typecheck dashboard-build
make supabase-migrate
make control-plane-capacity
```

Expected: all tests/build pass; migration succeeds; capacity remains below the 75% materialization admission threshold.

- [ ] **Step 3: Materialize and deploy.**

```bash
make control-plane-quote-coverage-backfill enable=1 generation_key=<current-quote-generation>
flyctl deploy --config output/control-plane-rollout-current/fly-control-api.toml --app polyarb-control-api --ha=false
make smoke-control-plane-prod
```

- [ ] **Step 4: Verify the live contracts.**

```bash
curl --fail --silent 'https://polyarb-control-api.fly.dev/perception/business/quote-coverage?limit=20' | jq
curl --fail --silent 'https://polyarb-control-api.fly.dev/perception/business/analysis?limit=20' | jq
```

Use the existing authenticated Playwright browser session to inspect all three pages. Confirm Structure’s default list excludes closed records, Quote Coverage has no extremity score, and Analysis begins with positive candidates.

- [ ] **Step 5: Commit and push.**

```bash
git add docs/learning/00-INDEX.md docs/learning/12-business-research-flow.md
git commit -m "docs(m1): explain business research flow"
git push origin main
```

## Self-Review

- Structure default scoping: Task 1 and Task 4.
- Quote Coverage health-only semantics and bounded materialization: Task 2 and Task 4.
- Analysis economic-value ordering: Task 3 and Task 4.
- Current-lineage, capacity, production verification, and learning material: Task 5.
- No certification, worker restart, or raw order-book mirror changes are included.
