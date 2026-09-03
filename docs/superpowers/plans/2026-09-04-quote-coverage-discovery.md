# Quote Coverage Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Quote Coverage into a current-generation, explainable research-lead view ranked by executable notional and non-neutral YES pricing.

**Architecture:** Keep raw Quote evidence immutable in the bounded JSONB research index. A pure Python contract module validates quote fields, derives discovery evidence, and round-trips an opaque cursor; the Postgres reader applies the same score/order across the full current generation and joins only the exact Structure parent projection. The Dashboard validates additive response fields and renders dense business-readable research leads.

**Tech Stack:** Python 3.12, psycopg/Postgres JSONB, Starlette, Next.js/TypeScript, pnpm, pytest.

## Global Constraints

- R2 remains the complete immutable source; do not add a full Structure/Quote mirror or a new unbounded table.
- `terminal_state == "executable"` is a prerequisite, not an opportunity claim.
- `executable_notional_usd = ask × size`; `price_extremity_bps = abs(ask - 0.5) × 10,000`; score is `ln(1 + notional) × extremity`.
- Current Quote data may join only its exact parent Structure generation from `m1_quote_generation_inputs`.
- Keep schema version `m1.business-research-page.v1` and pointer/index-integrity failures fail-closed.
- Preserve user-owned dirty JOURNAL, `.wrangler/`, and `output/` artifacts unstaged.

---

### Task 1: Discovery evidence and opaque cursor contract

**Files:**
- Create: `src/polyarb/control_plane/quote_discovery.py`
- Create: `tests/m1-perception/test_quote_discovery.py`

**Interfaces:**
- Produces `quote_discovery(payload: Mapping[str, object]) -> dict[str, object]`.
- Produces `encode_discovery_cursor(score: float, notional: float, token_id: str) -> str` and `decode_discovery_cursor(value: str) -> tuple[float, float, str] | None`.
- Closed reasons: `meaningful-executable-depth`, `non-neutral-yes-price`, `insufficient-executable-depth`, `missing-or-invalid-quote`, `not-executable`.

- [ ] **Step 1: Write the failing test**

```python
def test_executable_deep_non_neutral_quote_is_a_research_lead() -> None:
    evidence = quote_discovery(
        {"terminal_state": "executable", "best_ask_price": 0.70, "best_ask_size": 57.0}
    )
    assert evidence["executable_notional_usd"] == 39.9
    assert evidence["price_extremity_bps"] == 2000
    assert evidence["score"] > 0
    assert evidence["reasons"] == [
        "meaningful-executable-depth", "non-neutral-yes-price"
    ]


def test_non_executable_or_invalid_quote_is_explicitly_demoted() -> None:
    assert quote_discovery({"terminal_state": "missing-book"})["reasons"] == ["not-executable"]
    assert quote_discovery(
        {"terminal_state": "executable", "best_ask_price": 1.2, "best_ask_size": 5}
    )["reasons"] == ["missing-or-invalid-quote"]


def test_discovery_cursor_round_trips_and_rejects_malformed_input() -> None:
    cursor = encode_discovery_cursor(7770.0, 39.9, "token:abc")
    assert decode_discovery_cursor(cursor) == (7770.0, 39.9, "token:abc")
    assert decode_discovery_cursor("not-a-cursor") is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_quote_discovery.py -q`  
Expected: FAIL because `quote_discovery` does not exist.

- [ ] **Step 3: Write minimal implementation**

Use `math.log1p`, finite numeric validation, the formula above, and URL-safe base64 JSON containing exactly `score`, `notional`, and `token_id`. Reject non-finite values, unknown keys, empty token IDs, and cursors longer than 256 characters. Cursor is a pagination position, not authority, so it is not signed.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/m1-perception/test_quote_discovery.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/quote_discovery.py tests/m1-perception/test_quote_discovery.py
git commit -m "feat(m1): define quote discovery evidence"
```

### Task 2: Server-side whole-generation ordering and parent-bound context

**Files:**
- Modify: `src/polyarb/control_plane/postgres.py:9640-9691`
- Modify: `tests/m1-perception/test_control_plane_postgres.py:14903-15048`
- Modify: `tests/m1-perception/test_control_plane_api.py`

**Interfaces:**
- `business_quote_page(..., after)` accepts the opaque discovery cursor.
- Available items gain `discovery`, `event_context`, and `neg_risk_context`.
- `next_after` is an opaque cursor for the final returned globally sorted row.

- [ ] **Step 1: Write the failing test**

```python
def test_business_quote_page_orders_full_generation_by_discovery_then_token(
    control_plane: PostgresControlPlane,
) -> None:
    current, structure = _published_quote_and_parent(control_plane)
    control_plane.stage_business_quote_rows(generation_key=current, rows=(
        ("token:shallow", {"event_id": "event-a", "terminal_state": "executable",
                            "best_ask_price": 0.001, "best_ask_size": 20.0}),
        ("token:deep", {"event_id": "event-a", "terminal_state": "executable",
                         "best_ask_price": 0.70, "best_ask_size": 57.0}),
        ("token:missing", {"event_id": "event-a", "terminal_state": "missing-book"}),
    ))
    _stage_parent_structure_context(control_plane, structure)

    page = control_plane.business_quote_page(generation_key=None, limit=2, after="")

    assert [item["token_id"] for item in page["items"]] == ["token:deep", "token:shallow"]
    assert page["items"][0]["event_context"]["title"] == "Event A"
    assert page["items"][0]["neg_risk_context"]["status"] == "available"
    next_page = control_plane.business_quote_page(
        generation_key=None, limit=2, after=page["next_after"]
    )
    assert [item["token_id"] for item in next_page["items"]] == ["token:missing"]
```

Add a second test that materializes `structure:newer` only and asserts the Quote item returns `{"status": "not-indexed"}`, plus API coverage that malformed opaque `after` returns HTTP 400.

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k business_quote_page tests/m1-perception/test_control_plane_api.py -q`  
Expected: FAIL because token-ID order and discovery/context fields remain.

- [ ] **Step 3: Write minimal implementation**

Read the exact parent key from `m1_quote_generation_inputs`. In one read-only CTE, left-join `m1_structure_intelligence_events` and `m1_structure_intelligence_groups` with that parent key; derive safe finite numeric ask/size values with regex-guarded casts. Globally order by:

```sql
ORDER BY discovery_score DESC,
         executable_notional_usd DESC,
         token_id ASC
```

Decode the cursor before SQL. Continue after it with the lexicographic predicate:

```sql
(discovery_score, executable_notional_usd, token_id)
  < (%(after_score)s, %(after_notional)s, %(after_token)s)
```

Use the Task 1 helper to create reasons and output cursor. Event context is title/open/end-time; neg-risk context is short group ID, quality, and expected member count. Absent parent rows return exactly `{"status": "not-indexed"}`; newer/unrelated Structure data is never joined.

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/m1-perception/test_control_plane_postgres.py -k business_quote_page tests/m1-perception/test_control_plane_api.py -q`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/control_plane/postgres.py tests/m1-perception/test_control_plane_postgres.py tests/m1-perception/test_control_plane_api.py
git commit -m "feat(m1): rank quote research leads server-side"
```

### Task 3: Typed Dashboard reader and dense research-lead table

**Files:**
- Modify: `dashboard/lib/business-research.ts:1-47`
- Modify: `dashboard/app/business/quotes/page.tsx:1-25`
- Modify: `tests/m1-perception/test_business_dashboard_contract.py`

**Interfaces:**
- `decodeBusinessResearchPage` validates optional Quote-only discovery and context fields; malformed data returns `null`.
- Quote Coverage calls every row a research lead, never an opportunity.

- [ ] **Step 1: Write the failing test**

```python
def test_quote_coverage_renders_discovery_evidence_not_an_opportunity_claim() -> None:
    page = Path("dashboard/app/business/quotes/page.tsx").read_text()
    assert "Research leads" in page
    assert "executable notional" in page
    assert "research priority, not a certified opportunity" in page
    assert "event_context" in page
    assert "neg_risk_context" in page
    assert "discovery" in page
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q`  
Expected: FAIL because the existing table only renders raw fields.

- [ ] **Step 3: Write minimal implementation**

Add a Quote-specific validator: all discovery numbers must be finite/non-negative; reasons must be from Task 1; context is either `{status: "not-indexed"}` or bounded known fields. Keep Structure decoding unchanged.

Replace headings with `Research signal`, `Market`, `Executable quote`, `Event`, `Neg-risk context`, and `Data quality`. Render reason labels first, then score/extremity; show executable notional with ask/depth; show event title/state/end; show a shortened group ID plus quality/member count; retain a full ID detail line for audit. Header copy is: “Ordered by executable depth and non-neutral YES price; research priority, not a certified opportunity.”

- [ ] **Step 4: Run test to verify it passes**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q && make dashboard-typecheck dashboard-build`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add dashboard/lib/business-research.ts dashboard/app/business/quotes/page.tsx tests/m1-perception/test_business_dashboard_contract.py
git commit -m "feat(m1): render quote discovery leads"
```

### Task 4: Operator guidance and production visual acceptance

**Files:**
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/superpowers/plans/2026-09-04-quote-coverage-discovery-SUMMARY.md`

**Interfaces:**
- Operator guide distinguishes research priority from certified opportunity and explains score/reasons.

- [ ] **Step 1: Write the failing test**

```python
def test_m1_operator_guide_explains_quote_discovery_priority() -> None:
    guide = Path("docs/M1-市场感知平台使用手册.md").read_text()
    assert "Quote Coverage" in guide
    assert "research priority" in guide
    assert "not a certified opportunity" in guide
```

- [ ] **Step 2: Run test to verify it fails**

Run: `uv run pytest tests/m1-perception/test_business_dashboard_contract.py -q`  
Expected: FAIL before guide update.

- [ ] **Step 3: Deploy and perform read-only visual acceptance**

Run local gates, push to `main`, and use the existing authenticated Playwright session at `/business/quotes` on desktop and narrow widths. Verify nonzero leads appear first; event/group context is readable or explicitly `not-indexed`; essential fields are not clipped; and the no-opportunity disclaimer is visible. Do not restart the coordinator or any write lane.

- [ ] **Step 4: Update guide and summary**

Document formula, reason labels, pagination scope, current-generation lineage condition, and the distinction from Certified opportunities. Record exact tests, rollout, and visual evidence in the summary.

- [ ] **Step 5: Commit and push**

```bash
git add docs/M1-市场感知平台使用手册.md docs/superpowers/plans/2026-09-04-quote-coverage-discovery-SUMMARY.md
git commit -m "docs(m1): explain quote discovery research leads"
git push origin main
```

## Plan Self-Review

- Spec coverage: Tasks 1–3 implement the discovery, lineage, Dashboard, and failure contracts; Task 4 records operator and visual evidence.
- No placeholders: every task names files, interfaces, tests, commands, and expected outcome.
- Consistency: `quote_discovery` owns reason classification; Postgres owns global sort; Dashboard consumes only decoded additive fields.

