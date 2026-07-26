# M1 Market Truth Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace truncated Gamma offset discovery with provably complete event/market truth and make the production neg-risk feed reject incomplete or augmented groups.

**Architecture:** Gamma keyset pagination publishes a snapshot only after both market and event cursors terminate normally. Event membership is normalized into durable group truth, and the quote worker quotes only `complete-supported` standard neg-risk groups. The scanner returns verified candidates plus bounded rejection counts; incomplete source truth returns HTTP 503 rather than false zero or false profit.

**Tech Stack:** Python 3.12, httpx, Pydantic settings, SQLite/WAL, Starlette, Typer, pytest/respx, uv, Make.

## Global Constraints

- Use `uv`; do not install with `pip`.
- All new executable commands require `make <verb>-<noun>` targets and `make help` visibility.
- Keep the current release-75 production process running as a diagnostic window; this plan performs no production mutation until its explicit deploy task.
- Only standard neg-risk buy-all is supported; `negRiskAugmented=true` is always `complete-unsupported`.
- A partial cursor walk, repeated cursor, event/market mismatch, missing required member, stale quote, or mixed quote run fails closed.
- M1 outputs gross-before-fees facts only and never authorizes a real order.
- Every code task uses RED → GREEN TDD and ends in an atomic commit.

---

## File Structure

- Modify `src/polyarb/clients/gamma_client.py`: keyset cursor transport and completion proof.
- Modify `src/polyarb/snapshot/normalizer.py`: event structure and membership normalization.
- Create `src/polyarb/perception/market_truth.py`: immutable coverage/group/member contracts and hashes.
- Modify `src/polyarb/storage/schemas.py`: durable source coverage, group truth, and membership tables.
- Modify `src/polyarb/storage/sqlite_store.py`: atomic publication of complete truth without replacing the last good market view on source failure.
- Modify `src/polyarb/snapshot/orchestrator.py`: join keyset completion, normalization, validation, and publication.
- Modify `src/polyarb/routing/neg_risk_quote_store.py`: select only verified standard groups and bind quote legs to membership hashes.
- Modify `src/polyarb/routing/neg_risk_quote_collector.py`: support a valid empty verified universe and preserve group identity.
- Modify `src/polyarb/routing/opportunity_scanner.py`: verified candidate/rejection result.
- Modify `src/polyarb/http/arbitrage.py`: strict response contract and source-incomplete 503.
- Modify `src/polyarb/routing/opportunity_diagnosis.py`: diagnose the verified response schema.
- Modify `src/polyarb/http/health.py`: coverage and product-health checks.
- Modify `src/polyarb/cli_arbitrage.py` and `Makefile`: local/production operator surfaces.
- Test in the existing `tests/m1-perception/`, `tests/routing/`, and `tests/cli/` suites.

### Task 1: Keyset pagination with explicit completion proof

**Files:**
- Modify: `src/polyarb/clients/gamma_client.py`
- Test: `tests/m1-perception/test_gamma_client.py`

**Interfaces:**
- Produces: `PaginationResult[T](items_yielded: int, pages_fetched: int, completed: bool, final_cursor: str | None)`.
- Produces: `GammaClient.iter_active_markets(coverage: PaginationCoverage)`.
- Produces: `GammaClient.iter_active_events(coverage: PaginationCoverage)`.
- Removes: the special case that treats offset HTTP 422 as successful termination.

- [ ] **Step 1: Write failing cursor and truncation tests**

```python
async def test_markets_use_keyset_until_missing_next_cursor() -> None:
    settings = _fast_settings()
    page_1 = {"markets": [_make_market_dict(i) for i in range(100)], "next_cursor": "c1"}
    page_2 = {"markets": [_make_market_dict(100)], "next_cursor": None}
    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url, assert_all_called=True) as router:
        route = router.get("/markets/keyset").mock(
            side_effect=[httpx.Response(200, json=page_1), httpx.Response(200, json=page_2)]
        )
        async with GammaClient(settings) as client:
            rows = [row async for row in client.iter_active_markets(coverage)]
    assert len(rows) == 101
    assert coverage.result == PaginationResult(101, 2, True, None)
    assert route.calls[0].request.url.params.get("after_cursor") is None
    assert route.calls[1].request.url.params["after_cursor"] == "c1"


async def test_repeated_keyset_cursor_is_not_complete() -> None:
    settings = _fast_settings()
    coverage = PaginationCoverage(source="markets")
    page = {"markets": [_make_market_dict(1)], "next_cursor": "same"}
    with respx.mock(base_url=settings.gamma_url) as router:
        router.get("/markets/keyset").mock(return_value=httpx.Response(200, json=page))
        async with GammaClient(settings) as client:
            with pytest.raises(PaginationIntegrityError, match="repeated cursor"):
                _ = [row async for row in client.iter_active_markets(coverage)]
    assert coverage.result.completed is False


async def test_keyset_http_error_never_becomes_successful_short_page() -> None:
    settings = _fast_settings()
    coverage = PaginationCoverage(source="markets")
    with respx.mock(base_url=settings.gamma_url) as router:
        router.get("/markets/keyset").mock(return_value=httpx.Response(422, json={"error": "cap"}))
        async with GammaClient(settings) as client:
            with pytest.raises(_NonRetryableHTTPError):
                _ = [row async for row in client.iter_active_markets(coverage)]
    assert coverage.result.completed is False
```

- [ ] **Step 2: Run the focused tests and confirm RED**

Run:

```bash
uv run pytest tests/m1-perception/test_gamma_client.py \
  -k 'keyset or repeated_cursor' -q
```

Expected: collection/import failures for `PaginationCoverage`, `PaginationResult`, or `PaginationIntegrityError`.

- [ ] **Step 3: Implement the keyset contracts and iterator**

```python
@dataclass(frozen=True)
class PaginationResult:
    items_yielded: int
    pages_fetched: int
    completed: bool
    final_cursor: str | None


@dataclass
class PaginationCoverage:
    source: str
    result: PaginationResult = field(
        default_factory=lambda: PaginationResult(0, 0, False, None)
    )


class PaginationIntegrityError(RuntimeError):
    pass


async def _paginate_keyset(
    self,
    *,
    path: str,
    array_key: str,
    params: dict[str, str],
    keep_fields: frozenset[str],
    coverage: PaginationCoverage,
) -> AsyncIterator[dict]:
    cursor: str | None = None
    seen: set[str] = set()
    items = pages = 0
    while True:
        request_params = {**params, "limit": str(self.PAGE_LIMIT)}
        if cursor is not None:
            request_params["after_cursor"] = cursor
        payload = await self._get(path, request_params)
        if not isinstance(payload, dict) or not isinstance(payload.get(array_key), list):
            raise PaginationIntegrityError(f"{path} keyset response has invalid shape")
        pages += 1
        for raw in payload[array_key]:
            if not isinstance(raw, dict):
                continue
            raw["_page_fetched_at_ms"] = int(time.time() * 1000)
            projected = {key: value for key, value in raw.items() if key in keep_fields}
            if projected.get("active") is True and projected.get("closed") is not True:
                items += 1
                yield projected
        next_cursor = payload.get("next_cursor")
        if next_cursor in (None, ""):
            coverage.result = PaginationResult(items, pages, True, None)
            return
        if not isinstance(next_cursor, str) or next_cursor in seen:
            coverage.result = PaginationResult(items, pages, False, cursor)
            raise PaginationIntegrityError(f"{path} repeated cursor")
        seen.add(next_cursor)
        cursor = next_cursor
        if pages >= self.MAX_PAGES:
            coverage.result = PaginationResult(items, pages, False, cursor)
            raise PaginationIntegrityError(f"{path} exceeded {self.MAX_PAGES} pages")
```

Wire markets to `/markets/keyset` with `array_key="markets"` and events to
`/events/keyset` with `array_key="events"`. Preserve per-page timestamps and
client-side `active=true/closed=false` filtering.

- [ ] **Step 4: Run Gamma tests and Ruff**

Run:

```bash
uv run pytest tests/m1-perception/test_gamma_client.py -q
uv run ruff check src/polyarb/clients/gamma_client.py tests/m1-perception/test_gamma_client.py
```

Expected: all Gamma tests PASS and Ruff exits 0.

- [ ] **Step 5: Commit**

```bash
git add src/polyarb/clients/gamma_client.py tests/m1-perception/test_gamma_client.py
git commit -m "fix(m1): require complete Gamma keyset pagination"
```

### Task 2: Normalize event membership and classify neg-risk groups

**Files:**
- Create: `src/polyarb/perception/__init__.py`
- Create: `src/polyarb/perception/market_truth.py`
- Modify: `src/polyarb/clients/gamma_client.py`
- Modify: `src/polyarb/snapshot/normalizer.py`
- Test: `tests/m1-perception/test_market_truth.py`
- Test: `tests/m1-perception/test_normalizer.py`

**Interfaces:**
- Produces: `EventMember`, `GroupTruth`, `SourceCoverage`.
- Produces: `normalize_events(raw_events: list[dict]) -> tuple[list[dict], list[dict], dict[str, str], list[EventMember], list[GroupTruth]]`.
- Produces: `membership_hash(event_id, group_id, members) -> str`.

- [ ] **Step 1: Write RED tests for a faithful augmented event and a standard event**

```python
def _michigan_event() -> dict:
    active = [
        {"id": str(969760 + i), "groupItemTitle": title, "active": True,
         "closed": False, "negRiskOther": False}
        for i, title in enumerate(
            ["Kent Benham", "Fred Heurtebise", "Mike Rogers",
             "Genevieve Scott", "Bernadette Smith", "Andrew Kamal"]
        )
    ]
    other = [{"id": "969766", "groupItemTitle": "Other", "active": False,
              "closed": False, "negRiskOther": True}]
    reserved = [
        {"id": str(969767 + i), "groupItemTitle": f"Candidate {chr(65 + i)}",
         "active": False, "closed": False, "negRiskOther": False}
        for i in range(26)
    ]
    return {
        "id": "111080",
        "slug": "michigan-republican-senate-primary-winner-954",
        "title": "Michigan Republican Senate Primary Winner",
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": True,
        "negRiskMarketID": "group-mi",
        "markets": active + other + reserved,
        "tags": [],
    }


def test_augmented_event_is_complete_but_unsupported() -> None:
    _, _, _, members, groups = normalize_events([_michigan_event()])
    assert len(members) == 33
    assert groups == [
        GroupTruth(
            event_id="111080",
            group_id="group-mi",
            neg_risk_type="augmented",
            expected_member_count=33,
            active_named_count=6,
            membership_hash=membership_hash("111080", "group-mi", members),
            quality="complete-unsupported",
            reason="augmented-neg-risk-not-supported",
        )
    ]


def test_standard_group_hash_is_order_independent() -> None:
    left = [EventMember("e1", "g1", "m1", "named", True, False),
            EventMember("e1", "g1", "m2", "named", True, False)]
    assert membership_hash("e1", "g1", left) == membership_hash(
        "e1", "g1", list(reversed(left))
    )
```

- [ ] **Step 2: Run and confirm RED**

Run:

```bash
uv run pytest tests/m1-perception/test_market_truth.py \
  tests/m1-perception/test_normalizer.py -q
```

Expected: import/signature failures.

- [ ] **Step 3: Implement immutable truth contracts**

```python
Quality = Literal[
    "complete-supported",
    "complete-unsupported",
    "incomplete-source",
    "incomplete-quotes",
]


@dataclass(frozen=True)
class EventMember:
    event_id: str
    group_id: str
    market_id: str
    member_kind: Literal["named", "other", "inactive-reserved"]
    active: bool
    closed: bool


@dataclass(frozen=True)
class GroupTruth:
    event_id: str
    group_id: str
    neg_risk_type: Literal["standard", "augmented"]
    expected_member_count: int
    active_named_count: int
    membership_hash: str
    quality: Quality
    reason: str | None


def membership_hash(event_id: str, group_id: str, members: Sequence[EventMember]) -> str:
    canonical = [
        (m.market_id, m.member_kind, m.active, m.closed)
        for m in sorted(members, key=lambda item: item.market_id)
    ]
    raw = json.dumps([event_id, group_id, canonical], separators=(",", ":"))
    return hashlib.sha256(raw.encode()).hexdigest()
```

Classify `negRiskOther=true` as `other`, inactive non-Other as
`inactive-reserved`, and active non-Other as `named`. Standard groups are
supported only when every structural member is active, named, and open.
Augmented groups are always complete-unsupported when the event payload is
complete.

- [ ] **Step 4: Preserve structural fields in Gamma event projection and normalize**

Add to `_EVENT_KEEP`:

```python
"negRisk", "enableNegRisk", "negRiskAugmented", "negRiskMarketID"
```

Project nested markets as:

```python
raw["markets"] = [
    {
        "id": market.get("id"),
        "active": market.get("active"),
        "closed": market.get("closed"),
        "negRiskOther": market.get("negRiskOther"),
        "groupItemTitle": market.get("groupItemTitle"),
    }
    for market in markets
    if isinstance(market, dict)
]
```

Return the five-value normalized tuple and update all existing callers/tests.

- [ ] **Step 5: Run tests and commit**

```bash
uv run pytest tests/m1-perception/test_market_truth.py \
  tests/m1-perception/test_normalizer.py tests/m1-perception/test_gamma_client.py -q
uv run ruff check src/polyarb/perception src/polyarb/snapshot/normalizer.py \
  src/polyarb/clients/gamma_client.py tests/m1-perception/test_market_truth.py
git add src/polyarb/perception src/polyarb/clients/gamma_client.py \
  src/polyarb/snapshot/normalizer.py tests/m1-perception/test_market_truth.py \
  tests/m1-perception/test_normalizer.py
git commit -m "feat(m1): model authoritative event membership"
```

### Task 3: Persist coverage and publish only complete market truth

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/snapshot/orchestrator.py`
- Test: `tests/m1-perception/test_sqlite_store.py`
- Test: `tests/m1-perception/test_sqlite_store_migration.py`
- Test: `tests/m1-perception/test_orchestrator.py`
- Test: `tests/m1-perception/test_schema_lockstep.py`

**Interfaces:**
- Produces tables `snapshot_source_coverage`, `event_market_memberships`, `neg_risk_group_truth`.
- Expands `SQLiteStore.write_snapshot` with exact keyword-only arguments
  `source_coverage: SourceCoverage`, `event_members: list[EventMember]`,
  `group_truths: list[GroupTruth]`, and `publish_markets: bool`.
- Invariant: failed source coverage records diagnostic snapshot metadata but leaves the last complete `markets` view intact.

- [ ] **Step 1: Write failing storage/publication tests**

```python
def test_incomplete_source_does_not_replace_last_complete_markets(db_path) -> None:
    store = SQLiteStore(db_path)
    store.init_schema()
    first = store.write_snapshot(
        taken_at_ms=1, finished_at_ms=2, mode="subset", parquet_path="a",
        is_valid=True, market_rows=[_market("complete-market")], issues=[],
        source_coverage=SourceCoverage.complete(10, 3),
        event_members=[], group_truths=[], publish_markets=True,
    )
    second = store.write_snapshot(
        taken_at_ms=3, finished_at_ms=4, mode="subset", parquet_path="b",
        is_valid=False, market_rows=[_market("partial-market")], issues=[],
        source_coverage=SourceCoverage.incomplete("markets", 2, 100, "http-422"),
        event_members=[], group_truths=[], publish_markets=False,
    )
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT market_id, snapshot_id FROM markets").fetchall() == [
            ("complete-market", first)
        ]
        assert con.execute(
            "SELECT completed FROM snapshot_source_coverage WHERE snapshot_id=?", (second,)
        ).fetchone() == (0,)


def test_group_truth_and_membership_are_same_snapshot_transaction(db_path) -> None:
    store = SQLiteStore(db_path)
    store.init_schema()
    members = [
        EventMember("e1", "g1", "m1", "named", True, False),
        EventMember("e1", "g1", "m2", "named", True, False),
    ]
    truth = GroupTruth(
        event_id="e1",
        group_id="g1",
        neg_risk_type="standard",
        expected_member_count=2,
        active_named_count=2,
        membership_hash=membership_hash("e1", "g1", members),
        quality="complete-supported",
        reason=None,
    )
    snapshot_id = store.write_snapshot(
        taken_at_ms=1, finished_at_ms=2, mode="subset", parquet_path="truth",
        is_valid=True, market_rows=[_market("m1"), _market("m2")], issues=[],
        source_coverage=SourceCoverage.complete(2, 1),
        event_members=members, group_truths=[truth], publish_markets=True,
    )
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT market_id FROM event_market_memberships "
            "WHERE snapshot_id=? ORDER BY market_id", (snapshot_id,)
        ).fetchall() == [("m1",), ("m2",)]
        assert con.execute(
            "SELECT membership_hash FROM neg_risk_group_truth WHERE snapshot_id=?",
            (snapshot_id,),
        ).fetchone() == (truth.membership_hash,)
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py \
  tests/m1-perception/test_orchestrator.py -q
```

Expected: new keyword/table failures.

- [ ] **Step 3: Add exact SQLite tables and insert projections**

```sql
CREATE TABLE IF NOT EXISTS snapshot_source_coverage (
  snapshot_id INTEGER PRIMARY KEY REFERENCES snapshots(id),
  completed INTEGER NOT NULL CHECK(completed IN (0,1)),
  market_items INTEGER NOT NULL CHECK(market_items >= 0),
  event_items INTEGER NOT NULL CHECK(event_items >= 0),
  failure_source TEXT,
  failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS event_market_memberships (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  event_id TEXT NOT NULL,
  neg_risk_market_id TEXT NOT NULL,
  market_id TEXT NOT NULL,
  member_kind TEXT NOT NULL CHECK(member_kind IN ('named','other','inactive-reserved')),
  active INTEGER NOT NULL CHECK(active IN (0,1)),
  closed INTEGER NOT NULL CHECK(closed IN (0,1)),
  PRIMARY KEY(snapshot_id, event_id, market_id)
);

CREATE TABLE IF NOT EXISTS neg_risk_group_truth (
  snapshot_id INTEGER NOT NULL REFERENCES snapshots(id),
  event_id TEXT NOT NULL,
  neg_risk_market_id TEXT NOT NULL,
  neg_risk_type TEXT NOT NULL CHECK(neg_risk_type IN ('standard','augmented')),
  expected_member_count INTEGER NOT NULL CHECK(expected_member_count > 0),
  active_named_count INTEGER NOT NULL CHECK(active_named_count >= 0),
  membership_hash TEXT NOT NULL,
  quality TEXT NOT NULL CHECK(quality IN (
    'complete-supported','complete-unsupported','incomplete-source','incomplete-quotes'
  )),
  reason TEXT,
  PRIMARY KEY(snapshot_id, neg_risk_market_id)
);
```

Insert coverage, event rows, membership, group truth, and market publication in
one `BEGIN IMMEDIATE`. Execute `DELETE FROM markets` only when
`publish_markets=True`.

- [ ] **Step 4: Wire orchestrator completeness**

Construct separate `PaginationCoverage("events")` and
`PaginationCoverage("markets")`. Set:

```python
source_complete = event_coverage.result.completed and market_coverage.result.completed
publish_markets = source_complete and not any(
    issue.category == Category.API_UNREACHABLE for issue in issues
)
```

When false, mark the run invalid, persist coverage failure, skip current-market
replacement, Supabase current-view mirror, event publication, and quote-universe
refresh. R2/diagnostic snapshot metadata may still record the failed attempt.

- [ ] **Step 5: Run storage/orchestrator/schema tests**

```bash
uv run pytest tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py \
  tests/m1-perception/test_orchestrator.py \
  tests/m1-perception/test_schema_lockstep.py -q
uv run ruff check src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  src/polyarb/snapshot/orchestrator.py
```

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  src/polyarb/snapshot/orchestrator.py tests/m1-perception/test_sqlite_store.py \
  tests/m1-perception/test_sqlite_store_migration.py \
  tests/m1-perception/test_orchestrator.py tests/m1-perception/test_schema_lockstep.py
git commit -m "feat(m1): publish only complete market truth"
```

### Task 4: Bind quote runs to verified standard memberships

**Files:**
- Modify: `src/polyarb/storage/schemas.py`
- Modify: `src/polyarb/storage/sqlite_store.py`
- Modify: `src/polyarb/routing/neg_risk_quote_store.py`
- Modify: `src/polyarb/routing/neg_risk_quote_collector.py`
- Test: `tests/routing/test_neg_risk_quote_store.py`
- Test: `tests/routing/test_neg_risk_quote_collector.py`

**Interfaces:**
- Produces `VerifiedQuoteUniverse(snapshot_id, taken_at_ms, universe_hash, legs, rejections)`.
- Expands `UniverseLeg` with `event_id` and `membership_hash`.
- Expands quote run rows with `universe_hash`.

- [ ] **Step 1: Write failing verified-universe tests**

```python
def test_latest_universe_excludes_augmented_and_reports_reason(quote_db) -> None:
    _seed_group_truth(quote_db, "g-standard", "standard", "complete-supported", None)
    _seed_group_truth(
        quote_db, "g-augmented", "augmented", "complete-unsupported",
        "augmented-neg-risk-not-supported",
    )
    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()
    assert {leg.neg_risk_market_id for leg in universe.legs} == {"g-standard"}
    assert universe.rejections == (
        GroupRejection("g-augmented", "complete-unsupported",
                       "augmented-neg-risk-not-supported"),
    )


def test_missing_required_market_rejects_whole_standard_group(quote_db) -> None:
    _seed_standard_truth(quote_db, group_id="g1", expected=3, active_named=3)
    _seed_markets(quote_db, group_id="g1", count=2)
    universe = NegRiskQuoteStore(quote_db).latest_verified_universe()
    assert universe.legs == ()
    assert universe.rejections[0].reason == "membership-market-mismatch"
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py -q
```

Expected: missing verified-universe contracts.

- [ ] **Step 3: Implement verified selection and durable identity**

```python
@dataclass(frozen=True)
class GroupRejection:
    group_id: str
    quality: str
    reason: str


@dataclass(frozen=True)
class VerifiedQuoteUniverse:
    snapshot_id: int
    taken_at_ms: int
    universe_hash: str
    legs: tuple[UniverseLeg, ...]
    rejections: tuple[GroupRejection, ...]
```

Select the latest `snapshot_source_coverage.completed=1` snapshot whose market
rows were published. Join `neg_risk_group_truth` to current markets by
snapshot/group, require:

```text
quality=complete-supported
neg_risk_type=standard
count(markets)=expected_member_count=active_named_count
all market rows active=1, closed=0, incomplete=0, yes_token_id non-empty
```

Hash sorted `(group_id, membership_hash, market_id, yes_token_id)` into one
`universe_hash`. Store that hash on `neg_risk_quote_runs`; store event and
membership hash on every run leg and terminal quote.

- [ ] **Step 4: Support a valid empty verified universe**

If source coverage is complete but no standard group is supported, create and
complete a zero-leg run without calling CLOB:

```python
if not universe.legs:
    run_id = quote_store.begin_verified_run(universe, quoted_at_ms=clock())
    completed = quote_store.complete_run(
        run_id, completed_at_ms=clock(), successful_response_count=0
    )
    return QuoteCollectionResult.from_run(completed, elapsed_ms=0)
```

An empty supported universe is not the same as unavailable source truth;
`latest_verified_universe()` raises `QuoteUniverseUnavailableError` only when
there is no complete coverage snapshot.

- [ ] **Step 5: Run tests and commit**

```bash
uv run pytest tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py -q
uv run ruff check src/polyarb/routing/neg_risk_quote_store.py \
  src/polyarb/routing/neg_risk_quote_collector.py
git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py \
  src/polyarb/routing/neg_risk_quote_store.py \
  src/polyarb/routing/neg_risk_quote_collector.py \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py
git commit -m "feat(m1): bind quotes to verified event memberships"
```

### Task 5: Return verified candidates and bounded rejection summaries

**Files:**
- Modify: `src/polyarb/routing/opportunity_scanner.py`
- Modify: `src/polyarb/http/arbitrage.py`
- Modify: `src/polyarb/routing/opportunity_diagnosis.py`
- Modify: `src/polyarb/cli_arbitrage.py`
- Test: `tests/routing/test_opportunity_scanner.py`
- Test: `tests/m1-perception/test_arbitrage_opportunities_http.py`
- Test: `tests/routing/test_opportunity_diagnosis.py`
- Test: `tests/cli/test_arbitrage_cli.py`

**Interfaces:**
- Produces `OpportunityScanResult(opportunities, rejections, source_snapshot_id, universe_hash, quote_run_id)`.
- Candidate adds `event_id`, `membership_hash`, `quality`.
- HTTP coverage becomes `verified-standard-neg-risk`.

- [ ] **Step 1: Write failing scanner/HTTP contract tests**

```python
def test_verified_scan_exposes_identity_and_rejections(quote_db) -> None:
    result = scan_verified_neg_risk_quote_run(quote_db, now_s=lambda: QUOTE_NOW_S)
    assert result.opportunities[0].quality == "complete-supported"
    assert result.opportunities[0].membership_hash == "membership-g1"
    assert result.universe_hash == "universe-hash"
    assert result.rejections["augmented-neg-risk-not-supported"] == 1


def test_http_never_calls_partial_feed_zero(http_test_client, monkeypatch) -> None:
    monkeypatch.setattr(
        "polyarb.http.arbitrage.scan_verified_neg_risk_quote_run",
        lambda *_a, **_k: (_ for _ in ()).throw(
            QuoteUniverseUnavailableError("source coverage incomplete")
        ),
    )
    response = http_test_client.get("/arbitrage/opportunities")
    assert response.status_code == 503
    assert response.json() == {"error": "verified market universe unavailable"}
```

Pin the successful body:

```python
{
    "strategy": "neg-risk-buy-all",
    "profit_basis": "gross-before-fees",
    "coverage": "verified-standard-neg-risk",
    "source_snapshot_id": 10,
    "universe_hash": "u1",
    "quote_run_id": 20,
    "quote_sla_seconds": 300,
    "count": 0,
    "rejections": {
        "augmented-neg-risk-not-supported": 4,
        "incomplete-quotes": 2,
    },
    "opportunities": [],
}
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/routing/test_opportunity_scanner.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_diagnosis.py tests/cli/test_arbitrage_cli.py -q
```

- [ ] **Step 3: Implement result contract and safe error vocabulary**

```python
@dataclass(frozen=True)
class OpportunityScanResult:
    opportunities: tuple[NegRiskOpportunity, ...]
    rejections: Mapping[str, int]
    source_snapshot_id: int
    universe_hash: str
    quote_run_id: int
```

The HTTP layer catches source/coverage exceptions and returns only
`{"error": "verified market universe unavailable"}`. Diagnostics accept a 200
zero only when coverage, source snapshot, universe hash, quote run, rejections,
count, and opportunities all validate.

- [ ] **Step 4: Run focused tests and commit**

```bash
uv run pytest tests/routing/test_opportunity_scanner.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_diagnosis.py tests/cli/test_arbitrage_cli.py -q
uv run ruff check src/polyarb/routing/opportunity_scanner.py \
  src/polyarb/http/arbitrage.py src/polyarb/routing/opportunity_diagnosis.py \
  src/polyarb/cli_arbitrage.py
git add src/polyarb/routing/opportunity_scanner.py src/polyarb/http/arbitrage.py \
  src/polyarb/routing/opportunity_diagnosis.py src/polyarb/cli_arbitrage.py \
  tests/routing/test_opportunity_scanner.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_diagnosis.py tests/cli/test_arbitrage_cli.py
git commit -m "fix(m1): reject unverified neg-risk opportunities"
```

### Task 6: Coverage health, operator entry points, and production qualification

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `Makefile`
- Modify: `tests/m1-perception/test_health_endpoint.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`
- Modify: `tests/m1-perception/test_makefile_contract.py`
- Modify: `docs/M1-市场感知平台使用手册.md`
- Modify: `docs/learning/23-生产机会流.md`
- Create: `docs/learning/25-市场全集不是请求成功.md`
- Modify: `docs/learning/00-INDEX.md`

**Interfaces:**
- Produces health checks `market_truth:coverage` and `market_truth:last_complete_age_seconds`.
- Produces `read_market_truth_health(path, now_s) -> MarketTruthHealth`.
- Polywatch alerts on coverage fail even if snapshot scheduler/process is green.
- Preserves `make diagnose-arb-feed-prod`; its output now validates verified coverage.

- [ ] **Step 1: Write failing health and monitor tests**

```python
def test_market_truth_health_fails_on_latest_incomplete_attempt(tmp_path) -> None:
    path = tmp_path / "state.db"
    store = SQLiteStore(path)
    store.init_schema()
    _seed_complete_coverage(store, taken_at_ms=1_000)
    _seed_incomplete_coverage(store, taken_at_ms=2_000, reason="http-422")
    result = read_market_truth_health(path, now_s=3.0)
    assert result.coverage_status == "fail"
    assert result.coverage_value == "incomplete-source"
    assert result.last_complete_age_seconds == pytest.approx(2.0)


def test_polywatch_alerts_on_market_truth_coverage_failure() -> None:
    payload = _healthy_l1()
    payload["checks"]["market_truth:coverage"] = [_check("incomplete-source", "fail")]
    assert decide_l1(payload) == ("push", "L1 market truth coverage failed")
```

- [ ] **Step 2: Run and confirm RED**

```bash
uv run pytest tests/m1-perception/test_health_endpoint.py \
  tests/m1-perception/test_polywatch_healthz_watcher.py \
  tests/m1-perception/test_makefile_contract.py -q
```

- [ ] **Step 3: Implement health and documentation truth**

Health reads the latest attempt and the latest complete published truth:

```python
checks["market_truth:coverage"] = [
    check(
        observed_value="complete" if latest_coverage.completed else "incomplete-source",
        status="pass" if latest_coverage.completed else "fail",
        output=f"markets={latest_coverage.market_items} events={latest_coverage.event_items}",
    )
]
```

Update the manual matrix from “Neg-risk 可成交 ask 机会输入已验证可用” to
“暂停供 M2 使用，直到 verified-standard-neg-risk 生产验收”。Explain the
2100-row truncation and augmented exclusion in learning document 25.

- [ ] **Step 4: Run full local qualification**

```bash
uv run pytest tests/m1-perception tests/routing tests/cli -q
uv run ruff check src/polyarb/clients/gamma_client.py src/polyarb/perception \
  src/polyarb/snapshot src/polyarb/storage/sqlite_store.py \
  src/polyarb/routing/neg_risk_quote_store.py \
  src/polyarb/routing/neg_risk_quote_collector.py \
  src/polyarb/routing/opportunity_scanner.py src/polyarb/http/arbitrage.py \
  src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py
make docs-m1-check
make planning-status
```

Expected: all commands exit 0; no planning drift.

- [ ] **Step 5: Commit local qualification**

```bash
git add src scripts Makefile tests docs
git commit -m "docs(m1): expose verified market truth operations"
```

- [ ] **Step 6: Deploy L1 only after exact preflight**

Before deployment record:

```bash
git status --short
git rev-parse HEAD
make smoke-health-prod
make smoke-l2-health-strict-prod
make polywatch-resident-status
```

Require a clean tree and preserve the L2 release/boot and the ongoing diagnostic
window. Trigger only the L1 workflow:

```bash
make deploy
```

Do not deploy L2 or modify its manifest.

- [ ] **Step 7: Verify production market truth**

```bash
make smoke-health-prod
make diagnose-arb-feed-prod min_edge_bps=0
make polywatch-healthz-dry
```

Expected:

- L1 release ID equals the deployed SHA;
- `market_truth:coverage` PASS;
- opportunity response coverage is `verified-standard-neg-risk`;
- Michigan event `111080` appears in rejections as
  `augmented-neg-risk-not-supported`, never as an opportunity;
- no candidate lacks event ID, membership hash, source snapshot, universe hash,
  or quote run ID.

- [ ] **Step 8: Record production evidence and commit**

Append the exact release, timestamps, counts, response hashes, Michigan
rejection proof, and unchanged L2 identity to the active phase log,
`.planning/JOURNAL.md`, and M1 `STATE.md`. Run:

```bash
make planning-status
git add .planning
git commit -m "docs(m1): record verified market truth deployment"
git push
```

Expected: clean worktree and no drift.
