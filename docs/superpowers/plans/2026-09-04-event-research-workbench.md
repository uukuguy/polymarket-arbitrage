# Event Research Workbench Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a single, current-lineage Event research workbench that links the three M1 business main tables to investment-relevant structural, coverage, and group-economics evidence.

**Architecture:** The control-plane fixes one current Quote pointer and its exact parent Structure generation inside one read-only authority call, then assembles event-local group facts from existing bounded projections. The Dashboard consumes that one envelope rather than composing paginated endpoints. Group-leg evidence is a separate bounded current-generation read. No durable projection, raw market mirror, or order-book mirror is added.

**Tech Stack:** Python 3.12, psycopg/PostgreSQL JSONB, Starlette, Next.js 15/TypeScript, pytest, pnpm, Playwright CLI.

## Global Constraints

- Serve only open, unexpired events from the current Quote generation and its exact parent Structure generation.
- Use explicit unavailable/not-published/lagging semantics; unavailable is never zero.
- Keep all API pages bounded to 200 rows and all route parameters length-bounded and allowlisted.
- Do not persist a new detail projection or copy raw R2 markets/order-book levels into Supabase.
- Keep Quote Coverage free of price-extremity and opportunity-style ranking.
- Candidate, gross-profit, and ROI evidence remains research-only; only the existing opportunity projection may claim Certified opportunity.
- Do not restart the stopped coordinator.

---

### Task 1: Correct candidate economics before exposing investment detail

**Files:**
- Modify: `src/polyarb/control_plane/analysis_candidates.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Modify: `dashboard/app/business/analysis/page.tsx`
- Test: `tests/m1-perception/test_analysis_candidates.py`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`

**Interfaces:**
- Produces `gross_profit_usd = (1 - bundle_cost) * max_bundle_size` for positive candidates.
- Produces `capital_required_usd = bundle_cost * max_bundle_size` and `gross_roi_bps = gross_profit_usd / capital_required_usd * 10_000` only when all operands are finite and positive.
- `business_analysis_page()` ranks positive rows by `gross_profit_usd DESC`, then gross ROI, then group ID; it does not emit the legacy proxy as money.

- [ ] **Step 1: Write failing economic truth tests.**

```python
def test_positive_candidate_economics_use_bundle_payout_not_rank_proxy() -> None:
    economics = candidate_economics(bundle_cost=0.9, max_bundle_size=15.0)
    assert economics == {
        "capital_required_usd": 13.5,
        "gross_profit_usd": 1.5,
        "gross_roi_bps": 1111.11111111,
    }
```

Add a dashboard source contract asserting that Analysis uses `gross_profit_usd`, `capital_required_usd`, and `gross_roi_bps`, and that it does not render `executable_economic_value` as dollar value.

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_analysis_candidates.py -k economics -q`  
Expected: FAIL because the economics helper and response fields do not exist.

- [ ] **Step 3: Implement one finite-input economics helper.**

```python
def candidate_economics(*, bundle_cost: object, max_bundle_size: object) -> dict[str, float] | None:
    if not (_finite_number(bundle_cost) and _finite_number(max_bundle_size)):
        return None
    cost, size = float(bundle_cost), float(max_bundle_size)
    if not 0 < cost < 1 or size <= 0:
        return None
    capital = cost * size
    profit = (1 - cost) * size
    return {
        "capital_required_usd": round(capital, 8),
        "gross_profit_usd": round(profit, 8),
        "gross_roi_bps": round(profit / capital * 10_000, 8),
    }
```

Attach this derived object in `business_analysis_page()` at response time for legacy/current candidate rows. Change the SQL positive sort expression to `(1 - bundle_cost) * max_bundle_size`; do not rewrite or rematerialize the 10k-row projection.

- [ ] **Step 4: Render only truthful labels.**

Replace the Analysis money line with `gross profit`, `capital required`, and `gross ROI`; retain bundle cost and limiting size as supporting evidence. Add the text `fees, slippage, and simultaneous execution not assessed` next to the research-only warning.

- [ ] **Step 5: Verify GREEN and commit.**

Run: `uv run pytest tests/m1-perception/test_analysis_candidates.py tests/m1-perception/test_business_dashboard_contract.py -q && make dashboard-typecheck`  
Expected: PASS.

```bash
git add src/polyarb/control_plane/analysis_candidates.py src/polyarb/control_plane/postgres.py \
  dashboard/app/business/analysis/page.tsx tests/m1-perception/test_analysis_candidates.py \
  tests/m1-perception/test_business_dashboard_contract.py
git commit -m "fix(m1): expose truthful candidate economics"
```

### Task 2: Publish one fenced Event research detail authority

**Files:**
- Modify: `src/polyarb/control_plane/api.py`
- Modify: `src/polyarb/control_plane/postgres.py`
- Test: `tests/m1-perception/test_control_plane_api.py`
- Test: `tests/m1-perception/test_control_plane_postgres.py`

**Interfaces:**
- Produces `business_event_detail(*, event_id: str, focus_group_id: str | None, observed_generation: str | None) -> dict[str, object]`.
- Produces `business_event_group_legs(*, event_id: str, group_id: str, limit: int, after: str) -> dict[str, object]`.
- Exposes `GET /perception/business/events/{event_id}` and `GET /perception/business/events/{event_id}/groups/{group_id}/legs` before generic business routes.

- [ ] **Step 1: Write failing route and lineage tests.**

```python
def test_event_detail_route_transports_one_fenced_authority_envelope() -> None:
    class Reader:
        def business_event_detail(self, *, event_id, focus_group_id, observed_generation):
            assert (event_id, focus_group_id, observed_generation) == ("event:1", "group:1", "quote:old")
            return {"schema_version": "m1.event-research-detail.v1", "status": "available", "event_id": event_id}
    with TestClient(create_control_plane_app(control_plane=Reader())) as client:
        response = client.get("/perception/business/events/event%3A1?focus_group_id=group%3A1&observed_generation=quote%3Aold")
    assert response.status_code == 200
```

Seed an exact Structure/Quote/current candidate lineage with one future event and groups covering positive-edge, incomplete-coverage, and no-edge cases. Assert the detail returns only that event's groups, distinct observed/executable/non-executable/missing leg counts, and `changed_since_entry=True` for an old observed generation. Seed an ended event and assert `event-not-operational`, not an empty available detail.

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py -k event_detail -q`  
Expected: FAIL because neither endpoint nor repository reader exists.

- [ ] **Step 3: Add strict transport handlers.**

Validate `event_id`, optional `focus_group_id`, optional `observed_generation`, `limit`, and `after` with the existing 256-character/NUL guard. `from` is parsed only by the Dashboard and never passed to SQL. Return `400` for malformed input and `503 event-research-unavailable` only for authority failures.

- [ ] **Step 4: Implement the single-snapshot repository read.**

Inside one read-only transaction:

1. Read `quote:current`, its `m1_quote_generation_inputs.structure_generation_key`, and the matching candidate projection.
2. Read the event from `m1_structure_intelligence_events` using that exact Structure generation; require `is_open IS TRUE` and `sort_end_time_ms > now_ms`.
3. Read event-owned `m1_structure_intelligence_groups`, left join matching candidate rows, and aggregate `m1_business_quote_rows` by `neg_risk_market_id` into observed/executable/non-executable counts.
4. Derive missing count as `max(expected - observed, 0)`, coverage state from distinct counts, and economics only for positive-edge groups with valid candidate operands.
5. Sort group summaries: positive gross candidates by `gross_profit_usd DESC`, then ROI; coverage gaps with the fewest blocking legs next; then context-unavailable and no-edge. Limit to 200.
6. Return one `m1.event-research-detail.v1` envelope containing `anchor`, `event`, `research_stage`, `blockers`, `structure`, `quote_coverage`, `analysis`, `groups`, and optional `focused_group`.

The legs reader performs the same pointer/event/group ownership fence, reads only the focused group's compact quote rows, exposes token/market identifier, ask, size, terminal state, and a bounded cursor. It contains the fixed caution that top-of-book evidence does not prove simultaneous multi-leg execution.

- [ ] **Step 5: Verify GREEN and commit.**

Run: `uv run pytest tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py -k 'event_detail or event_group_legs' -q`  
Expected: PASS.

```bash
git add src/polyarb/control_plane/api.py src/polyarb/control_plane/postgres.py \
  tests/m1-perception/test_control_plane_api.py tests/m1-perception/test_control_plane_postgres.py
git commit -m "feat(m1): publish fenced event research detail"
```

### Task 3: Build the workbench and link all three main tables

**Files:**
- Create: `dashboard/app/business/events/[event_id]/page.tsx`
- Modify: `dashboard/lib/business-research.ts`
- Modify: `dashboard/app/business/structure/page.tsx`
- Modify: `dashboard/app/business/quotes/page.tsx`
- Modify: `dashboard/app/business/analysis/page.tsx`
- Test: `tests/m1-perception/test_business_dashboard_contract.py`

**Interfaces:**
- Produces `readEventResearchDetail(eventId, { focusGroupId?, observedGeneration? })` with a strict decoder for `m1.event-research-detail.v1`.
- Produces accessible event links `/business/events/<event_id>?from=<source>[&focus_group_id=<group>]`.
- The workbench renders Structural evidence, Quote Coverage, Analysis, risks/unknowns, and a collapsed lineage panel.

- [ ] **Step 1: Write failing dashboard contracts.**

```python
def test_all_business_main_tables_link_events_to_one_workbench() -> None:
    for page in ("structure", "quotes", "analysis"):
        source = Path(f"dashboard/app/business/{page}/page.tsx").read_text()
        assert "/business/events/" in source
    detail = Path("dashboard/app/business/events/[event_id]/page.tsx").read_text()
    assert "Structure evidence" in detail
    assert "Quote coverage" in detail
    assert "Gross profit" in detail
    assert "not assessed" in detail
```

Add decoder tests rejecting malformed lineage, non-finite economics, negative count fields, invalid `focus_group_id`, and unavailable results pretending to contain group facts.

- [ ] **Step 2: Verify RED.**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -k event_workbench -q`  
Expected: FAIL because the route, reader, and links do not exist.

- [ ] **Step 3: Add one strict frontend reader and workbench page.**

The page reads one event detail envelope server-side, chooses its initially emphasized section from the allowlisted `from` query parameter, and never fetches Structure/Quote/Analysis independently. Render:

- a research header with status, end time, liquidity, volume, market breadth, research stage, and blockers;
- Structure evidence with group quality/reasons;
- Quote Coverage totals and defect-first group facts;
- Analysis positive candidates with capital, theoretical gross profit, ROI, and research-only warning;
- a `not assessed` risk/unknowns card;
- a collapsed lineage/provenance `<details>` panel.

Use `Unknown` for absent fields and product-specific unavailable panels for `not-published`, `lagging`, and `unavailable`; do not render a numerical zero unless the envelope explicitly proves one.

- [ ] **Step 4: Add contextual main-table links.**

Wrap Event titles, not raw IDs, in anchors. Structure passes `from=structure`; Quote Coverage and Analysis pass their `group_id` as `focus_group_id` and their current generation as `observed_generation`. Preserve table layout, but ensure links have a visible focus style and concise title text.

- [ ] **Step 5: Verify GREEN and commit.**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q && make dashboard-typecheck dashboard-build`  
Expected: PASS.

```bash
git add dashboard/app/business/events/[event_id]/page.tsx dashboard/lib/business-research.ts \
  dashboard/app/business/structure/page.tsx dashboard/app/business/quotes/page.tsx \
  dashboard/app/business/analysis/page.tsx tests/m1-perception/test_business_dashboard_contract.py
git commit -m "feat(m1): add event research workbench"
```

### Task 4: Deploy and validate the investment-research path

**Files:**
- Modify: `docs/learning/00-INDEX.md`
- Create: `docs/learning/12-event-research-workbench.md`
- Test evidence: `output/playwright/m1-event-research-workbench-2026-09-04.png`

- [ ] **Step 1: Add the short teaching guide.**

Explain the 30-second mental model: Event detail is the event-level research decision path; Structure establishes what exists, Quote Coverage establishes whether its legs are usable, and Analysis computes non-certified group evidence. Include the profit/capital/ROI formulas, the difference between observed and executable legs, and why `not assessed` risks cannot be treated as zero.

- [ ] **Step 2: Verify all targeted tests and build.**

Run:

```bash
uv run pytest tests/m1-perception/test_analysis_candidates.py \
  tests/m1-perception/test_control_plane_api.py \
  tests/m1-perception/test_control_plane_postgres.py \
  tests/m1-perception/test_business_dashboard_contract.py -q
make dashboard-typecheck dashboard-build smoke-control-plane-prod
```

Expected: all commands pass.

- [ ] **Step 3: Deploy the API and verify live authority.**

```bash
flyctl deploy --config output/control-plane-rollout-current/fly-control-api.toml \
  --app polyarb-control-api --ha=false
curl -fsS 'https://polyarb-control-api.fly.dev/perception/business/events/<active-event-id>' | jq
```

Confirm the response has a current Quote/Structure anchor, no raw order-book payload, clear blockers, and no Certified-opportunity claim.

- [ ] **Step 4: Verify live navigation in the existing browser session.**

Use the existing Playwright session only. Open a Structure event link, a Quote Coverage event link, and an Analysis event link. Confirm they reach the same event route, render the appropriate initial section/focused group, expose `not assessed` risks, and display gross profit rather than legacy proxy dollars. Check at desktop and 375px widths and save one screenshot.

- [ ] **Step 5: Commit guide and evidence note.**

```bash
git add docs/learning/00-INDEX.md docs/learning/12-event-research-workbench.md
git commit -m "docs(m1): explain event research workbench"
```

## Plan self-review

- Spec coverage: Tasks 1–4 cover economic truth, one fenced authority, source-aware navigation, bounded leg drilldown, unavailable semantics, capacity preservation, teaching, and production verification.
- No placeholders: every implementation task names exact files, interfaces, tests, commands, and expected behavior.
- Type consistency: the plan uses `business_event_detail`, `business_event_group_legs`, `readEventResearchDetail`, `event_id`, and `focus_group_id` consistently across API, repository, and Dashboard layers.
