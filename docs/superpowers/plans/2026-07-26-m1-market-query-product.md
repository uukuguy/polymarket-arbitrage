# M1 Market Query Product Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give operators one production market brief plus explainable event and market drill-down commands backed by the verified truth from Plan 1.

**Architecture:** Published snapshots append a compact market-history projection while retaining the existing current market view. A focused query service constructs brief/event/market JSON from SQLite and the verified quote projection. A read-only operator script combines the L1 product response with L1/L2 health so users see market information first and operational eligibility alongside it.

**Tech Stack:** Python 3.12, SQLite/WAL, Starlette, dataclasses, Typer/stdlib HTTP, pytest, uv, Make.

## Global Constraints

- Plan 1 (`2026-07-26-m1-market-truth-foundation.md`) is a prerequisite.
- Use verified source snapshot, membership hash, universe hash, and quote run identities from Plan 1.
- A query may be useful for research while `m2_input_allowed=false`; it must state why.
- All production operator commands are read-only GETs.
- The brief leads with market information, not raw health JSON.
- All executable commands have Makefile targets and documentation.
- No Dashboard or real order behavior is implemented in this plan.
- Every code task uses RED → GREEN TDD and ends in an atomic commit.

---

## File Structure

- Modify `src/polyarb/storage/schemas.py`: compact `market_fact_history`.
- Modify `src/polyarb/storage/sqlite_store.py`: append history only for published complete truth.
- Create `src/polyarb/perception/query_service.py`: brief/event/market read models.
- Create `src/polyarb/http/perception.py`: bounded JSON endpoints.
- Modify `src/polyarb/http/app.py`: three read-only routes.
- Create `scripts/m1_market_brief.py`: combine product JSON and health eligibility.
- Modify `Makefile`: `market-brief-prod`, `show-event-prod`, `show-market-prod`.
- Modify M1 manual and learning index.
- Add focused tests under `tests/m1-perception/`.

### Task 1: Append compact market facts for change detection

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Test: `tests/m1-perception/test_sqlite_store.py`
- Test: `tests/m1-perception/test_sqlite_store_migration.py`

**Interfaces:**
- Produces table `market_fact_history`.
- Only `publish_markets=True` snapshots append history.
- Preserves the existing `markets` table as the current read model.

- [ ] **Step 1: Write failing append/no-append tests**

```python
def test_published_snapshot_appends_compact_market_history(db_path) -> None:
    store = _truth_store(db_path)
    snapshot_id = _write_complete(
        store,
        [_market("m1", bid=0.40, ask=0.45, liquidity=1000, end_time_ms=9_000)],
    )
    with sqlite3.connect(db_path) as con:
        row = con.execute(
            "SELECT snapshot_id, market_id, best_bid_price, best_ask_price, "
            "liquidity_usd, end_time_ms FROM market_fact_history"
        ).fetchone()
    assert row == (snapshot_id, "m1", 0.40, 0.45, 1000.0, 9_000)


def test_incomplete_snapshot_does_not_append_market_history(db_path) -> None:
    store = _truth_store(db_path)
    _write_incomplete(store, [_market("partial", bid=0.1, ask=0.9)])
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT count(*) FROM market_fact_history").fetchone() == (0,)
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py \
  -k 'market_history' -q
```

Expected: `no such table: market_fact_history`.

- [ ] **Step 3: Add the exact history table**

```sql
CREATE TABLE IF NOT EXISTS market_fact_history (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  market_id TEXT NOT NULL,
  event_id TEXT,
  active INTEGER NOT NULL CHECK(active IN (0,1)),
  closed INTEGER NOT NULL CHECK(closed IN (0,1)),
  liquidity_usd REAL,
  best_bid_price REAL,
  best_ask_price REAL,
  best_bid_size REAL,
  best_ask_size REAL,
  end_time_ms INTEGER,
  PRIMARY KEY(snapshot_id, market_id)
);
CREATE INDEX IF NOT EXISTS idx_market_fact_history_market
  ON market_fact_history(market_id, snapshot_id DESC);
```

Within the complete publication transaction, project exactly these columns
from `market_rows`. Do not append on incomplete coverage.

- [ ] **Step 4: Run tests and commit**

```bash
uv run pytest tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py -q
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py
git commit -m "feat(m1): retain compact market fact history"
```

### Task 2: Build typed brief, event, and market read models

**Files:**
- Create: `src/polyarb/perception/query_service.py`
- Test: `tests/m1-perception/test_perception_query_service.py`

**Interfaces:**
- Produces `PerceptionQueryService.brief(now_s: float | None = None) -> MarketBrief`.
- Produces `PerceptionQueryService.event(event_id: str) -> EventView | None`.
- Produces `PerceptionQueryService.market(market_id: str) -> MarketView | None`.
- Every returned dataclass implements `to_dict()`.

- [ ] **Step 1: Write failing query-service tests**

```python
def test_brief_prioritizes_changes_and_m2_eligibility(perception_db) -> None:
    service = PerceptionQueryService(perception_db)
    brief = service.brief(now_s=10_000.0)
    payload = brief.to_dict()
    assert payload["coverage"] == "complete"
    assert payload["counts"] == {"events": 2, "markets": 4}
    assert payload["changes"]["new_market_ids"] == ["m4"]
    assert payload["changes"]["closed_or_removed_market_ids"] == ["m0"]
    assert payload["neg_risk"]["supported_standard_groups"] == 1
    assert payload["neg_risk"]["unsupported_augmented_groups"] == 1
    assert payload["m2_input_allowed"] is True
    assert payload["identity"]["membership_universe_hash"] == "universe-2"


def test_event_view_contains_every_structural_member(perception_db) -> None:
    view = PerceptionQueryService(perception_db).event("e-aug")
    assert view is not None
    payload = view.to_dict()
    assert payload["quality"] == "complete-unsupported"
    assert payload["reason"] == "augmented-neg-risk-not-supported"
    assert [member["kind"] for member in payload["members"]] == [
        "named", "other", "inactive-reserved"
    ]


def test_market_view_combines_current_quote_and_history(perception_db) -> None:
    view = PerceptionQueryService(perception_db).market("m1")
    assert view is not None
    payload = view.to_dict()
    assert payload["market_id"] == "m1"
    assert payload["latest_quote"]["quote_run_id"] == 8
    assert [point["snapshot_id"] for point in payload["history"]] == [2, 1]
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_perception_query_service.py -q
```

Expected: module import failure.

- [ ] **Step 3: Implement focused read-model dataclasses**

```python
@dataclass(frozen=True)
class MarketBrief:
    as_of_ms: int
    coverage: str
    identity: Mapping[str, object]
    counts: Mapping[str, int]
    changes: Mapping[str, object]
    neg_risk: Mapping[str, int]
    opportunity_summary: Mapping[str, object]
    attention: tuple[Mapping[str, object], ...]
    m2_input_allowed: bool
    m2_block_reason: str | None

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class PerceptionQueryService:
    def __init__(self, db_path: Path | str) -> None:
        self._db_path = Path(db_path)
```

Use one read-only SQLite connection per call. `brief()` selects the latest two
completed coverage snapshots, current markets/events/group truth, and latest
complete verified quote run. It reports both additions and IDs that disappeared
from the current active set, labeled `closed_or_removed_market_ids` because a
read-only diff cannot infer a stronger lifecycle claim. Change lists are
deterministic and capped at 20:

```sql
SELECT current.market_id
FROM market_fact_history AS current
LEFT JOIN market_fact_history AS previous
  ON previous.snapshot_id = ? AND previous.market_id = current.market_id
WHERE current.snapshot_id = ? AND previous.market_id IS NULL
ORDER BY current.market_id
LIMIT 20
```

`event()` joins `neg_risk_group_truth`, `event_market_memberships`, current
markets, and latest quote rows. `market()` joins current markets, its event/group
truth, latest quote, and the newest five history rows.

- [ ] **Step 4: Define deterministic attention rules**

Add only evidence-backed rules:

```python
if ask is not None and bid is not None and ask - bid >= 0.10:
    attention.append({"type": "wide-spread", "market_id": market_id,
                      "spread": round(ask - bid, 6)})
if end_time_ms is not None and 0 <= end_time_ms - now_ms <= 72 * 3600 * 1000:
    attention.append({"type": "near-end", "market_id": market_id,
                      "end_time_ms": end_time_ms})
if group.quality != "complete-supported":
    attention.append({"type": "group-rejected", "event_id": group.event_id,
                      "reason": group.reason})
```

Sort attention by `(type, event_id, market_id)` and cap at 50.

- [ ] **Step 5: Run tests and commit**

```bash
uv run pytest tests/m1-perception/test_perception_query_service.py -q
uv run ruff check src/polyarb/perception/query_service.py \
  tests/m1-perception/test_perception_query_service.py
git add src/polyarb/perception/query_service.py \
  tests/m1-perception/test_perception_query_service.py
git commit -m "feat(m1): build explainable market query models"
```

### Task 3: Expose read-only perception HTTP APIs

**Files:**
- Create: `src/polyarb/http/perception.py`
- Modify: `src/polyarb/http/app.py`
- Test: `tests/m1-perception/test_perception_http.py`

**Interfaces:**
- Produces `GET /perception/brief`.
- Produces `GET /perception/events/{event_id}`.
- Produces `GET /perception/markets/{market_id}`.

- [ ] **Step 1: Write failing endpoint tests**

```python
def test_brief_endpoint_returns_product_contract(http_test_client, monkeypatch) -> None:
    monkeypatch.setattr(
        "polyarb.http.perception.PerceptionQueryService.brief",
        lambda self: _BriefFixture(),
    )
    response = http_test_client.get("/perception/brief")
    assert response.status_code == 200
    assert response.json()["m2_input_allowed"] is True


@pytest.mark.parametrize(
    ("path", "method"),
    [
        ("/perception/events/missing", "event"),
        ("/perception/markets/missing", "market"),
    ],
)
def test_missing_resource_is_bounded_404(http_test_client, monkeypatch, path, method) -> None:
    monkeypatch.setattr(
        f"polyarb.http.perception.PerceptionQueryService.{method}",
        lambda self, identity: None,
    )
    response = http_test_client.get(path)
    assert response.status_code == 404
    assert response.json() == {"error": "perception resource not found"}


def test_query_database_error_is_bounded_503(http_test_client, monkeypatch) -> None:
    monkeypatch.setattr(
        "polyarb.http.perception.PerceptionQueryService.brief",
        lambda self: (_ for _ in ()).throw(sqlite3.Error("private path")),
    )
    response = http_test_client.get("/perception/brief")
    assert response.status_code == 503
    assert response.json() == {"error": "perception read model unavailable"}
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_perception_http.py -q
```

- [ ] **Step 3: Implement handlers and routes**

```python
def _service(request: Request) -> PerceptionQueryService:
    return PerceptionQueryService(request.app.state.sqlite_store.db_path)


async def brief(request: Request) -> JSONResponse:
    try:
        return JSONResponse(_service(request).brief().to_dict())
    except sqlite3.Error:
        return JSONResponse(
            {"error": "perception read model unavailable"}, status_code=503
        )
```

Event/market handlers read `request.path_params`, return the same bounded 503
on storage errors and the exact bounded 404 above for missing identities.

Register:

```python
Route("/perception/brief", brief, methods=["GET"]),
Route("/perception/events/{event_id}", event, methods=["GET"]),
Route("/perception/markets/{market_id}", market, methods=["GET"]),
```

- [ ] **Step 4: Run HTTP tests and commit**

```bash
uv run pytest tests/m1-perception/test_perception_http.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py -q
uv run ruff check src/polyarb/http/perception.py src/polyarb/http/app.py
git add src/polyarb/http/perception.py src/polyarb/http/app.py \
  tests/m1-perception/test_perception_http.py
git commit -m "feat(m1): expose market perception queries"
```

### Task 4: Add one-command production brief and drill-down commands

**Files:**
- Create: `scripts/m1_market_brief.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Create: `tests/m1-perception/test_m1_market_brief.py`

**Interfaces:**
- Produces script arguments `--l1-url`, `--l2-url`, `--timeout-s`.
- Produces Make targets `market-brief-prod`, `show-event-prod`, `show-market-prod`.
- Script exits 0 when market facts are available; `m2_input_allowed=false` is data, not a transport failure.
- Script exits 2 on unavailable/invalid product response.

- [ ] **Step 1: Write failing script tests**

```python
def test_brief_combines_market_product_and_operational_eligibility(monkeypatch, capsys) -> None:
    monkeypatch.setattr(
        brief_module, "_get_json",
        _responses(
            brief={"coverage": "complete", "counts": {"markets": 4},
                   "m2_input_allowed": True},
            l1={"status": "pass"},
            l2={"status": "warn", "checks": {
                "l3:membership_convergence": [_check("converged", "pass")]
            }},
        ),
    )
    assert brief_module.main(["--l1-url", "l1", "--l2-url", "l2"]) == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["market"]["counts"]["markets"] == 4
    assert payload["operations"]["m2_input_allowed"] is True


def test_invalid_brief_is_not_rendered_as_zero(monkeypatch) -> None:
    monkeypatch.setattr(
        brief_module, "_get_json",
        lambda url, timeout_s: (_ for _ in ()).throw(ValueError("invalid")),
    )
    assert brief_module.main(["--l1-url", "l1", "--l2-url", "l2"]) == 2
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_m1_market_brief.py \
  tests/m1-perception/test_makefile_contract.py -q
```

- [ ] **Step 3: Implement the bounded aggregator**

```python
def main(argv: Sequence[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        brief = _get_json(f"{args.l1_url}/perception/brief", args.timeout_s)
        l1_health = _get_json(f"{args.l1_url}/health", args.timeout_s)
        l2_health = _get_json(f"{args.l2_url}/health", args.timeout_s)
        _validate_brief(brief)
    except (OSError, ValueError, json.JSONDecodeError):
        print(json.dumps({"kind": "market-brief-unavailable"}))
        return 2
    operational = (
        l1_health.get("status") == "pass"
        and _strict_l3_pass(l2_health)
        and brief["m2_input_allowed"] is True
    )
    print(json.dumps({
        "market": brief,
        "operations": {
            "l1_status": l1_health.get("status"),
            "l2_status": l2_health.get("status"),
            "m2_input_allowed": operational,
        },
    }, ensure_ascii=False, indent=2, sort_keys=True))
    return 0
```

Use stdlib `urllib.request` with a hard timeout and a maximum 2 MiB body.

- [ ] **Step 4: Add Makefile targets**

```make
## market-brief-prod: One-page production market facts + M2 eligibility; read-only.
market-brief-prod:
	@uv run python scripts/m1_market_brief.py \
	  --l1-url https://polyarb-l1.fly.dev --l2-url https://polyarb-l2.fly.dev

## show-event-prod: Read one verified production event. Usage: make show-event-prod event_id=<id>
show-event-prod:
	@test -n "$(event_id)" || { echo "usage: make show-event-prod event_id=<id>"; exit 2; }
	@curl -fsS "https://polyarb-l1.fly.dev/perception/events/$(event_id)" | uv run python -m json.tool

## show-market-prod: Read one production market and recent facts. Usage: make show-market-prod market_id=<id>
show-market-prod:
	@test -n "$(market_id)" || { echo "usage: make show-market-prod market_id=<id>"; exit 2; }
	@curl -fsS "https://polyarb-l1.fly.dev/perception/markets/$(market_id)" | uv run python -m json.tool
```

- [ ] **Step 5: Run tests and commit**

```bash
uv run pytest tests/m1-perception/test_m1_market_brief.py \
  tests/m1-perception/test_makefile_contract.py -q
uv run ruff check scripts/m1_market_brief.py \
  tests/m1-perception/test_m1_market_brief.py
git add scripts/m1_market_brief.py Makefile \
  tests/m1-perception/test_m1_market_brief.py \
  tests/m1-perception/test_makefile_contract.py
git commit -m "feat(m1): add production market brief commands"
```

### Task 5: Document, qualify, and deploy the query product

**Files:**
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/26-从市场简报到M2候选.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `tests/m1-perception/test_m1_manual_contract.py`

**Interfaces:**
- Manual makes `make market-brief-prod` the primary daily entry.
- Learning document traces one event from brief → event → market → verified opportunity.

- [ ] **Step 1: Add failing manual-contract assertions**

```python
def test_manual_leads_with_market_information_workflow() -> None:
    text = MANUAL.read_text()
    assert "make market-brief-prod" in text
    assert "make show-event-prod event_id=" in text
    assert "make show-market-prod market_id=" in text
    assert text.index("make market-brief-prod") < text.index("make smoke-health-prod")
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_m1_manual_contract.py -q
```

- [ ] **Step 3: Update the manual and learning path**

The first operational workflow must be:

```bash
make market-brief-prod
make show-event-prod event_id=<event_id>
make show-market-prod market_id=<market_id>
make diagnose-arb-feed-prod min_edge_bps=0
```

Explain that health supports the market answer; it is not the market answer.
Document all response identities and `m2_input_allowed`.

- [ ] **Step 4: Run local qualification**

```bash
uv run pytest tests/m1-perception -q
uv run ruff check src/polyarb/perception/query_service.py \
  src/polyarb/http/perception.py scripts/m1_market_brief.py
make docs-m1-check
make planning-status
```

- [ ] **Step 5: Commit**

```bash
git add docs tests/m1-perception/test_m1_manual_contract.py
git commit -m "docs(m1): teach market-first production workflow"
```

- [ ] **Step 6: Deploy L1 and verify read-only production behavior**

Run:

```bash
git status --short
git rev-parse HEAD
make deploy
make smoke-health-prod
make market-brief-prod
make show-event-prod event_id=111080
make show-market-prod market_id=969762
make diagnose-arb-feed-prod min_edge_bps=0
```

Expected:

- deployed release ID is exact;
- brief shows market/event counts and M2 eligibility;
- event 111080 shows 33 structural members, augmented status, and unsupported reason;
- market 969762 is present under the event;
- opportunity feed never emits event 111080.

- [ ] **Step 7: Record and commit evidence**

Append commands, exact response identities, timestamps, and unchanged L2
diagnostic window identity to `.planning/JOURNAL.md`, M1 `STATE.md`, and the
active phase log. Then:

```bash
make planning-status
git add .planning
git commit -m "docs(m1): record market query product qualification"
git push
```
