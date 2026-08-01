# M1 Opportunity Feed Double-Buffer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the recurring Structure-to-Quote opportunity HTTP 503 window while preserving the existing 300-second fail-closed boundary and atomic version truth.

**Architecture:** Add one pure shared policy that evaluates whether a certified feed is current, a bounded previous revision, or unavailable. The opportunity endpoint and strict health consume that same decision; `QuoteWorkerRuntime` already provides the required atomic immutable feed pointer, so no persistence schema or runtime queue changes are needed.

**Tech Stack:** Python 3.12, Starlette, immutable dataclasses, SQLite read-only health metadata, pytest, uv, Fly.io, Polywatch.

## Global Constraints

- Serve only one already certified projection/opportunity pair; never rescan or publish a partial feed in HTTP.
- A previous feed is serveable only when Quote age is `<= 300` seconds, handoff age is `<= 300` seconds, and universe age is `<= 50_400` seconds.
- A matching feed is `pass` below 240 seconds, `warn` from 240 through 300 seconds, and `fail` above 300 seconds.
- A feed revision greater than the latest durable Structure revision is an integrity failure.
- Every successful opportunity response exposes `refreshing` and `latest_structure_snapshot_id`; existing source/run/opportunity fields describe the served feed.
- Source-truth reads remain bounded to one second. Missing or unreadable truth fails closed.
- No new dependency, database table, wallet access, signing, order placement, or trading action.
- Use TDD for every behavior change and preserve the existing Makefile operator surface.

---

## File Map

- Create `src/polyarb/routing/feed_handoff.py`: pure shared availability decision and stable reason strings.
- Create `tests/m1-perception/test_feed_handoff.py`: boundary and revision-order policy tests.
- Modify `src/polyarb/http/arbitrage.py`: bounded truth metadata read, policy use, and response version fields.
- Modify `src/polyarb/http/health.py`: add a distinct Structure completion-age fact without changing existing taken-at freshness semantics, then consume the policy.
- Modify `tests/m1-perception/test_arbitrage_opportunities_http.py`: current/refreshing/failure/atomic-swap contracts.
- Modify `scripts/polywatch/healthz_watcher.py`: recognize the explicit warning and validate response-version coherence.
- Modify `tests/m1-perception/test_quote_feed_health.py`: warning and hard-boundary contracts.
- Modify `tests/m1-perception/test_polywatch_healthz_watcher.py`: bounded refresh warning behavior.
- Modify `docs/learning/44-M1生产恢复边界.md` and Phase 05.6 state artifacts: operator model and exact closure evidence.

---

### Task 1: Shared Certified-Feed Availability Policy

**Files:**
- Create: `src/polyarb/routing/feed_handoff.py`
- Create: `tests/m1-perception/test_feed_handoff.py`

**Interfaces:**
- Consumes: `QUOTE_SLA_SECONDS` and `UNIVERSE_SLA_SECONDS` from `polyarb.routing.opportunity_scanner`.
- Produces: `FeedAvailability(available: bool, refreshing: bool, reason: str | None)` and `decide_feed_availability(...) -> FeedAvailability`.

- [x] **Step 1: Write failing policy tests**

Create table-driven tests covering current, bounded previous, missing truth,
source regression, quote expiry, universe expiry, missing handoff age, and exact
300-second boundaries:

```python
import pytest

from polyarb.routing.feed_handoff import (
    FeedAvailability,
    decide_feed_availability,
)


def _decision(**overrides):
    values = {
        "source_snapshot_id": 10,
        "latest_structure_snapshot_id": 10,
        "quote_age_seconds": 10.0,
        "universe_age_seconds": 10.0,
        "handoff_age_seconds": 0.0,
    }
    values.update(overrides)
    return decide_feed_availability(**values)


def test_current_certified_feed_is_available() -> None:
    assert _decision() == FeedAvailability(True, False, None)


def test_previous_feed_is_available_at_both_exact_hard_boundaries() -> None:
    assert _decision(
        latest_structure_snapshot_id=11,
        quote_age_seconds=300.0,
        handoff_age_seconds=300.0,
    ) == FeedAvailability(
        True,
        True,
        "source-snapshot-refreshing-serving-previous",
    )


@pytest.mark.parametrize(
    ("overrides", "reason"),
    (
        ({"latest_structure_snapshot_id": None}, "source-truth-unavailable"),
        ({"latest_structure_snapshot_id": 9}, "source-revision-ahead"),
        ({"quote_age_seconds": 300.1}, "stale-quote"),
        ({"universe_age_seconds": 50_400.1}, "stale-universe"),
        (
            {"latest_structure_snapshot_id": 11, "handoff_age_seconds": None},
            "source-snapshot-mismatch",
        ),
        (
            {"latest_structure_snapshot_id": 11, "handoff_age_seconds": 300.1},
            "source-snapshot-mismatch",
        ),
    ),
)
def test_unavailable_feed_reasons(overrides, reason: str) -> None:
    assert _decision(**overrides) == FeedAvailability(False, False, reason)
```

- [x] **Step 2: Run the focused tests and prove RED**

```bash
uv run pytest tests/m1-perception/test_feed_handoff.py -q
```

Expected: collection fails with `ModuleNotFoundError: No module named 'polyarb.routing.feed_handoff'`.

- [x] **Step 3: Implement the minimal pure policy**

```python
from dataclasses import dataclass

from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    UNIVERSE_SLA_SECONDS,
)


@dataclass(frozen=True)
class FeedAvailability:
    available: bool
    refreshing: bool
    reason: str | None


def decide_feed_availability(
    *,
    source_snapshot_id: int,
    latest_structure_snapshot_id: int | None,
    quote_age_seconds: float,
    universe_age_seconds: float,
    handoff_age_seconds: float | None,
) -> FeedAvailability:
    def unavailable(reason: str) -> FeedAvailability:
        return FeedAvailability(False, False, reason)

    if latest_structure_snapshot_id is None:
        return unavailable("source-truth-unavailable")
    if source_snapshot_id > latest_structure_snapshot_id:
        return unavailable("source-revision-ahead")
    if quote_age_seconds > QUOTE_SLA_SECONDS:
        return unavailable("stale-quote")
    if universe_age_seconds > UNIVERSE_SLA_SECONDS:
        return unavailable("stale-universe")
    if source_snapshot_id == latest_structure_snapshot_id:
        return FeedAvailability(True, False, None)
    if handoff_age_seconds is None or handoff_age_seconds > QUOTE_SLA_SECONDS:
        return unavailable("source-snapshot-mismatch")
    return FeedAvailability(
        True,
        True,
        "source-snapshot-refreshing-serving-previous",
    )
```

- [x] **Step 4: Run policy tests and lint**

```bash
uv run pytest tests/m1-perception/test_feed_handoff.py -q
uv run ruff check src/polyarb/routing/feed_handoff.py tests/m1-perception/test_feed_handoff.py
```

Expected: all tests pass and Ruff exits zero.

- [x] **Step 5: Commit the policy**

```bash
git add src/polyarb/routing/feed_handoff.py tests/m1-perception/test_feed_handoff.py
git commit -m "feat(m1): define certified feed handoff policy"
```

---

### Task 2: Opportunity Endpoint Bounded Previous-Revision Serving

**Files:**
- Modify: `src/polyarb/http/arbitrage.py`
- Modify: `tests/m1-perception/test_arbitrage_opportunities_http.py`

**Interfaces:**
- Consumes: `decide_feed_availability(...)`, `MarketTruthHealth`, and `read_market_truth_health(...)`.
- Produces: successful fields `refreshing: bool` and `latest_structure_snapshot_id: int`; preserves served-feed identity.

- [x] **Step 1: Change endpoint tests to RED**

Replace ID-only monkeypatches with truth metadata and require current-version
metadata:

```python
def _truth(snapshot_id: int | None, age_s: float = 0.0):
    return SimpleNamespace(
        last_complete_snapshot_id=snapshot_id,
        last_complete_finished_age_seconds=age_s,
    )


def test_current_feed_exposes_version_state(http_test_client, monkeypatch) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(10),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["refreshing"] is False
    assert response.json()["latest_structure_snapshot_id"] == 10
```

Replace the old advance-means-503 test with:

```python
def test_endpoint_serves_fresh_previous_feed_during_structure_refresh(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(11, 30.0),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    response = http_test_client.get("/arbitrage/opportunities")

    assert response.status_code == 200
    assert response.json()["refreshing"] is True
    assert response.json()["latest_structure_snapshot_id"] == 11
    assert response.json()["source_snapshot_id"] == 10
    assert response.json()["quote_run_id"] == 20
```

Use these exact failure and swap assertions (extend `_publish_feed` with
`snapshot_id` and `run_id` keyword parameters so it constructs a matching
projection/result pair):

```python
@pytest.mark.parametrize(
    ("latest_id", "handoff_age"),
    ((9, 1.0), (11, 300.1)),
)
def test_endpoint_rejects_unavailable_revision_handoffs(
    http_test_client, monkeypatch, latest_id: int, handoff_age: float
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(latest_id, handoff_age),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    assert http_test_client.get("/arbitrage/opportunities").status_code == 503


def test_endpoint_atomically_switches_to_new_certified_feed(
    http_test_client, monkeypatch
) -> None:
    runtime = QuoteWorkerRuntime()
    _publish_feed(runtime, snapshot_id=10, run_id=20)
    http_test_client.app.state.quote_worker_runtime = runtime
    monkeypatch.setattr(
        "polyarb.http.arbitrage._market_truth",
        lambda _path, _now_s: _truth(11, 30.0),
    )
    monkeypatch.setattr("polyarb.http.arbitrage.time.time", lambda: NOW_S)

    before = http_test_client.get("/arbitrage/opportunities").json()
    _publish_feed(runtime, snapshot_id=11, run_id=21)
    after = http_test_client.get("/arbitrage/opportunities").json()

    assert (before["source_snapshot_id"], before["quote_run_id"], before["refreshing"]) == (10, 20, True)
    assert (after["source_snapshot_id"], after["quote_run_id"], after["refreshing"]) == (11, 21, False)
```

Keep the forbidden-rescan assertion active during the refreshing request. For
missing truth, monkeypatch `_market_truth` to return `_truth(None, 0.0)` and
assert HTTP 503.

- [x] **Step 2: Run endpoint tests and prove RED**

```bash
uv run pytest tests/m1-perception/test_arbitrage_opportunities_http.py -q
```

Expected: failures show missing `_market_truth`, missing response fields, and the old 503 behavior.

- [x] **Step 3: Add exact completion age and implement bounded policy mapping**

Extend `MarketTruthHealth` with:

```python
last_complete_finished_age_seconds: float | None
```

Set it to `None` in the empty value. Change the complete-row query to select
`s.taken_at_ms,s.finished_at_ms`; preserve `last_complete_age_seconds` from
`taken_at_ms` and compute the new field only from `finished_at_ms`:

```python
complete_age = (
    max(0.0, now_s - complete[1] / 1000.0)
    if complete is not None
    else None
)
complete_finished_age = (
    max(0.0, now_s - complete[2] / 1000.0)
    if complete is not None and isinstance(complete[2], (int, float))
    else None
)
```

This must not alter the existing `market_truth:last_complete_age_seconds`
health check, whose semantics remain anchored to `taken_at_ms`.

Replace the ID-only helper:

```python
def _market_truth(db_path: Path, now_s: float) -> MarketTruthHealth:
    return read_market_truth_health(db_path, now_s)
```

Capture `now_s` once and read `_market_truth(db_path, now_s)` through the
existing one-second `asyncio.wait_for`. Compute ages from the immutable feed,
then call:

```python
availability = decide_feed_availability(
    source_snapshot_id=feed.projection.universe_snapshot_id,
    latest_structure_snapshot_id=market_truth.last_complete_snapshot_id,
    quote_age_seconds=quote_age_seconds,
    universe_age_seconds=universe_age_seconds,
    handoff_age_seconds=market_truth.last_complete_finished_age_seconds,
)
```

Map `stale-quote` to the existing `StaleQuoteRunError` message,
`stale-universe` to `StaleUniverseError`, and every other unavailable
decision to `QuoteUniverseUnavailableError(reason)`. Do not query durable IDs
until availability passes. Continue using
`structure_revision=result.source_snapshot_id`.

Add:

```python
"refreshing": availability.refreshing,
"latest_structure_snapshot_id": market_truth.last_complete_snapshot_id,
```

- [x] **Step 4: Run endpoint and cross-contract tests**

```bash
uv run pytest tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_feed_handoff.py -q
uv run ruff check src/polyarb/http/arbitrage.py tests/m1-perception/test_arbitrage_opportunities_http.py
```

Expected: all tests pass, no rescan occurs, and Ruff exits zero.

- [x] **Step 5: Commit the endpoint contract**

```bash
git add src/polyarb/http/arbitrage.py tests/m1-perception/test_arbitrage_opportunities_http.py
git commit -m "feat(m1): serve bounded feed during structure refresh"
```

---

### Task 3: Strict Health and Polywatch Handoff Semantics

**Files:**
- Modify: `src/polyarb/http/health.py`
- Modify: `scripts/polywatch/healthz_watcher.py`
- Modify: `tests/m1-perception/test_quote_feed_health.py`
- Modify: `tests/m1-perception/test_polywatch_healthz_watcher.py`

**Interfaces:**
- Consumes: `decide_feed_availability(...)` and `MarketTruthHealth.last_complete_finished_age_seconds`.
- Produces: `warn/source-snapshot-refreshing-serving-previous` while the exact endpoint feed is serveable; `fail` after either hard bound.

- [x] **Step 1: Write health RED tests**

Update both fresh mismatch tests:

```python
entry = checks["quote_feed:last_complete_age_seconds"][0]
assert entry["observedValue"] == 10.0
assert entry["status"] == "warn"
assert entry["output"] == "source-snapshot-refreshing-serving-previous"
assert overall == "warn"
```

The existing `test_quote_age_boundaries` already proves quote ages `300.0` and
`300.1`; the policy tests prove stale universe and revision ordering. Add the
HTTP-health handoff integration boundary by parameterizing the inserted
Structure completion timestamp:

```python
@pytest.mark.parametrize(
    ("handoff_age_s", "expected_status"),
    ((300.0, "warn"), (300.1, "fail")),
)
def test_structure_handoff_age_boundary(
    tmp_path, handoff_age_s: float, expected_status: str
) -> None:
    settings = _settings(tmp_path, enabled=True)
    _complete_run(settings, age_s=10)
    runtime = QuoteWorkerRuntime()
    projection = NegRiskQuoteStore(settings.db_path).latest_complete_projection()
    assert projection is not None
    runtime.publish_certified_projection(projection)
    _insert_complete_structure(
        settings.db_path,
        finished_at_ms=NOW_MS - int(handoff_age_s * 1_000),
    )

    checks, overall = _quote_check(settings, runtime=runtime)

    assert checks["quote_feed:last_complete_age_seconds"][0]["status"] == expected_status
    assert overall == expected_status
```

Extract `_insert_complete_structure(db_path, *, finished_at_ms)` from the two
existing mismatch fixtures; it inserts one complete Structure snapshot and its
coverage row using the current test SQL.

Update both Polywatch fixtures to
`output="source-snapshot-refreshing-serving-previous"` and
`observedValue=10.0`. Add response-version validation cases:

```python
@pytest.mark.parametrize(
    "payload",
    (
        {"refreshing": True, "latest_structure_snapshot_id": 10, "source_snapshot_id": 10},
        {"refreshing": False, "latest_structure_snapshot_id": 11, "source_snapshot_id": 10},
    ),
)
def test_opportunity_rejects_incoherent_version_state(payload) -> None:
    payload.update(
        strategy="neg-risk-buy-all",
        profit_basis="gross-before-fees",
        coverage="verified-standard-neg-risk",
        opportunities=[],
    )
    assert WATCHER.decide_opportunity(payload)[0] == "push"
```

- [x] **Step 2: Run health tests and prove RED**

```bash
uv run pytest tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py -q
```

Expected: old warning text, missing serving age, and new boundaries fail.

- [x] **Step 3: Use the shared policy in health**

Whenever a projection exists, compute:

```python
quote_age_s = max(0.0, now_s - quote_run.quoted_at_ms / 1000.0)
universe_age_s = max(0.0, now_s - quote_run.universe_taken_at_ms / 1000.0)
availability = decide_feed_availability(
    source_snapshot_id=quote_run.universe_snapshot_id,
    latest_structure_snapshot_id=market_truth.last_complete_snapshot_id,
    quote_age_seconds=quote_age_s,
    universe_age_seconds=universe_age_s,
    handoff_age_seconds=market_truth.last_complete_finished_age_seconds,
)
```

Map it exactly:

```python
if not availability.available:
    quote_status = "fail"
    quote_output = (
        None if availability.reason == "stale-quote" else availability.reason
    )
elif availability.refreshing:
    quote_status = "warn"
    quote_output = availability.reason
elif quote_age_s < QUOTE_WARN_SECONDS:
    quote_status = "pass"
else:
    quote_status = "warn"
```

Keep `observedValue=round(quote_age_s, 1)` whenever a projection exists.
Preserve cold-cache, collector, and retention checks.

In `scripts/polywatch/healthz_watcher.py`, replace both exact old warning
comparisons with `source-snapshot-refreshing-serving-previous`. For a non-null
opportunity payload require boolean `refreshing` plus integer source/latest
IDs, then enforce:

```python
refreshing = payload.get("refreshing")
latest_id = payload.get("latest_structure_snapshot_id")
source_id = payload.get("source_snapshot_id")
if type(refreshing) is not bool or type(latest_id) is not int or type(source_id) is not int:
    return "push", "Opportunity response version state is invalid"
if refreshing != (source_id < latest_id):
    return "push", "Opportunity response version state is incoherent"
```

- [x] **Step 4: Run focused cross-surface verification**

```bash
uv run pytest tests/m1-perception/test_feed_handoff.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py -q
uv run ruff check src/polyarb/routing/feed_handoff.py src/polyarb/http/arbitrage.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_feed_handoff.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py
```

Expected: all focused tests pass and Ruff exits zero.

- [x] **Step 5: Commit health semantics**

```bash
git add src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py
git commit -m "fix(m1): align health with bounded feed handoff"
```

---

### Task 4: Documentation, Full Gates, Exact Deployment, and Natural Handoff Proof

**Files:**
- Modify: `docs/learning/44-M1生产恢复边界.md`
- Modify: `.planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Modify: `.planning/workstreams/m1-perception/ROADMAP.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/threads/market-observation-architecture.md`

**Interfaces:**
- Consumes: passing Tasks 1-3 and the exact clean Git SHA.
- Produces: operator guidance, exact release evidence, one natural no-503 handoff trace, and completed Phase 05.6 state.

- [ ] **Step 1: Update the learning contract**

Add:

```markdown
## Structure→Quote 交接不是“混用新旧数据”

运行时只服务一个完整版本。`refreshing=true` 表示最新 Structure 已推进，
但响应中的 `source_snapshot_id`、`quote_run_id` 和全部机会仍共同属于上一个
已认证版本。旧 Quote 年龄或交接年龄任一超过 300 秒，接口和严格健康都会
503；成功认证新 Quote 后一次性切换为 `refreshing=false`。
```

Document `make scan-arb-live min_edge_bps=0`, strict `/health`, and
`make polywatch-resident-status`.

- [ ] **Step 2: Run final local verification**

```bash
uv run pytest tests/m1-perception/test_feed_handoff.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py -q
make test
uv run ruff check src/polyarb/routing/feed_handoff.py src/polyarb/http/arbitrage.py src/polyarb/http/health.py scripts/polywatch/healthz_watcher.py tests/m1-perception/test_feed_handoff.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_quote_feed_health.py tests/m1-perception/test_polywatch_healthz_watcher.py
git diff --check
make docs-m1-check
make planning-status
```

Expected: focused and full pytest pass with only repository-declared skip/xfail,
Ruff and diff exit zero, docs pass, and planning has no drift.

- [ ] **Step 3: Commit verified local documentation**

Keep Plan 05.6-02 `in_progress` until production proof:

```bash
git add docs/learning/44-M1生产恢复边界.md .planning/JOURNAL.md .planning/workstreams/m1-perception/STATE.md .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md .planning/threads/market-observation-architecture.md
git commit -m "docs(m1): explain atomic opportunity handoff"
```

- [ ] **Step 4: Deploy exact clean SHA**

```bash
test -z "$(git status --porcelain)"
git rev-parse HEAD
make deploy
FLY_API_TOKEN= flyctl status -a polyarb-l1 --json | jq '{machines:[.Machines[]|{id:.id,state:.state,image:.config.image}]}'
curl -fsS https://polyarb-l1.fly.dev/health | jq '{status,quote:.checks["quote_feed:last_complete_age_seconds"]}'
make scan-arb-live min_edge_bps=0
```

Expected: deployed `POLYARB_RELEASE_ID` equals the clean SHA, the app machine
is started on the new image, strict health is HTTP 200, and opportunities
contain both new fields.

- [ ] **Step 5: Observe one natural Structure-to-Quote crossing**

Poll strict health and opportunities at least every 10 seconds, retaining
timestamp, HTTP status, `refreshing`, latest Structure ID, served source ID,
Quote run ID, and quote age. The trace must contain:

```text
before:      HTTP 200, refreshing=false, latest_structure_snapshot_id=N, source_snapshot_id=N
handoff:     HTTP 200, refreshing=true,  latest_structure_snapshot_id=N+1, source_snapshot_id=N
after swap:  HTTP 200, refreshing=false, latest_structure_snapshot_id=N+1, source_snapshot_id=N+1
```

Reject the release if any opportunity sample is HTTP 503, IDs mix, strict
health becomes HTTP 503, or no matching Quote appears within 300 seconds.
After the swap run:

```bash
make polywatch-resident-status
make scan-arb-live min_edge_bps=0
```

Expected: resident incident state is empty and the live scan serves the new revision.

- [ ] **Step 6: Close planning only from exact evidence**

Record release SHA, Fly machine/image identity, timestamped three-state trace,
final Quote run, opportunity count, strict-health result, and Polywatch state.
Only then mark SUMMARY and ROADMAP complete and run:

```bash
make planning-status
make docs-m1-check
git diff --check
```

Expected: no drift and no documentation error.

- [ ] **Step 7: Commit production closure**

```bash
git add .planning/JOURNAL.md .planning/workstreams/m1-perception/STATE.md .planning/workstreams/m1-perception/ROADMAP.md .planning/workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-02-SUMMARY.md .planning/threads/market-observation-architecture.md docs/learning/44-M1生产恢复边界.md
git commit -m "docs(m1): close continuous production handoff proof"
```

Expected: commit passes the SUMMARY hook and the worktree is clean.
