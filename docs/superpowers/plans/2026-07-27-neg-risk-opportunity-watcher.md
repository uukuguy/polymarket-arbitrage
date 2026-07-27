# Neg-risk Opportunity Watcher Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the cloud M1 loop that discovers verified standard neg-risk buy-all observations, persists their lifecycle, focuses CLOB polling on active opportunities, alerts Telegram without claiming execution, and exposes the same facts to M2.

**Architecture:** Complete Structure revisions and atomic complete Quote runs remain the only global-discovery truth. A new assessment layer classifies every group; an SQLite ledger commits state, append-only evidence, and notification outbox rows atomically. A focused worker reads only active opportunity legs every 15 seconds and rechecks their Structure membership before recording a top-of-book observation.

**Tech Stack:** Python 3.12, asyncio, Starlette, SQLite/WAL, httpx, existing read-only `ClobReaderClient`, Pydantic Settings, pytest, Make.

## Global Constraints

- Observer-only: no wallet, signing, order, balance, or real-money code.
- Only `standard` + `complete-supported` groups from one complete Quote run can enter `observe`; default threshold is 100 gross bps and is never labelled net profit.
- Every fact retains event, group, all legs, membership hash, Structure revision, Quote run or focused timestamp, and state-transition reason.
- Missing/stale/mismatched facts are `unavailable` or `invalidated`, never inferred as zero price or `no-edge`.
- Defaults: map refresh/max age 1,800s; global Quote 120s; focused polling 15s; material edge 25bps; capacity 20%; detailed focus evidence 30 days then daily summaries.
- Opportunity masters are permanent; observations and notification attempts are append-only. Persist market fact before Telegram I/O.
- Telegram states `仅观察，未扣手续费、滑点和多腿成交风险` and `execution_status=not-verified`.
- All operator actions are Make targets calling cloud HTTP; they never mutate the local DB.
- Keep `/arbitrage/opportunities` compatible for current M2 consumers while adding persistent watcher read APIs.
- Migrations are add-only in `SQLiteStore.init_schema`; never drop/rewrite deployed history.

## File Structure

| File | Responsibility |
|---|---|
| `routing/opportunity_scanner.py` | Complete per-group assessment plus legacy candidate adapter. |
| `routing/opportunity_ledger.py` | Atomic master, observation, outbox, replay, retention storage. |
| `routing/focused_quote_collector.py` | Identity-bound top-of-book checks for active legs. |
| `daemon/opportunity_watcher.py` | Global reconciliation, focus loop, notification delivery, runtime state. |
| `daemon/quote_worker.py` | Calls watcher after full certification; supports immediate scan request. |
| `storage/schemas.py`, `storage/sqlite_store.py` | Ledger DDL and idempotent migration. |
| `http/market_map.py`, `http/control.py`, `http/app.py` | Map/watch reads, signed cloud triggers, route wiring. |
| `cli_perception.py`, `Makefile` | Signed cloud controls and user command entry points. |
| `config.py`, `fly.toml`, M1 manual, learning note 28 | Production defaults, operation and teaching material. |

## Task 1: Complete group assessment

**Files:** Modify `src/polyarb/routing/opportunity_scanner.py`; create `tests/routing/test_opportunity_scanner.py`.

**Interfaces:** Consume `CompleteQuoteProjection`; produce `GroupAssessment`, `AssessmentResult`, and `assess_certified_neg_risk_quote_projection()`. Keep `scan_certified_neg_risk_quote_projection()` as a compatibility adapter selecting `observe` rows.

- [ ] **Step 1: Write the failing test**

```python
def test_assessment_distinguishes_no_edge_from_unavailable(complete_projection):
    result = assess_certified_neg_risk_quote_projection(complete_projection, min_edge_bps=100, now_s=lambda: NOW_S)
    by_group = {item.group_id: item for item in result.assessments}
    assert by_group["cheap-complete"].status == "observe"
    assert by_group["fair-complete"].status == "no-edge"
    assert by_group["missing-leg"].status == "unavailable"
```

Also assert augmented/rejected groups preserve their reason and are not turned into `no-edge`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/routing/test_opportunity_scanner.py -q`

Expected: FAIL because the assessment interface does not yet exist.

- [ ] **Step 3: Implement the minimal assessment pass**

```python
@dataclass(frozen=True)
class GroupAssessment:
    group_id: str
    event_id: str | None
    membership_hash: str | None
    status: Literal["observe", "no-edge", "unavailable"]
    reason: str | None
    bundle_cost: float | None
    gross_edge_bps: float | None
    max_bundle_size: float | None
    legs: tuple[OpportunityLeg, ...]
    structure_revision: int
    quote_run_id: int
    quoted_at_ms: int
```

Validate projection freshness first. Require valid identity and executable top asks for every leg; otherwise return bounded `unavailable`. For valid groups calculate cost, edge, and minimum size, then return `observe` at threshold or `no-edge` below it. Adapt the existing scanner to select only observed results.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/routing/test_opportunity_scanner.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/cli/test_arbitrage_cli.py -q`

Expected: PASS. Commit with `git add src/polyarb/routing/opportunity_scanner.py tests/routing/test_opportunity_scanner.py && git commit -m "feat(m1): classify verified neg-risk quote groups"`.

## Task 2: Durable lifecycle ledger and outbox

**Files:** Modify `src/polyarb/storage/schemas.py`, `src/polyarb/storage/sqlite_store.py`; create `src/polyarb/routing/opportunity_ledger.py`, `tests/routing/test_opportunity_ledger.py`.

**Interfaces:** Consume Task 1 assessments. Produce `OpportunityLedger.reconcile_global()`, `record_focused()`, `pending_notifications()`, delivery updates, replay, and compaction. A single `BEGIN IMMEDIATE` transaction writes transition + observation + outbox; network delivery occurs after commit.

- [ ] **Step 1: Write the failing lifecycle tests**

```python
def test_first_crossing_creates_master_observation_and_outbox(ledger, observe_assessment):
    transition = ledger.reconcile_global(observe_assessment, observed_at_ms=NOW_MS)
    assert transition.kind == "entered"
    assert ledger.current_opportunities()[0]["status"] == "observe"
    assert ledger.pending_notifications(now_ms=NOW_MS)[0].reason == "entered-gross-edge-threshold"

def test_stable_observation_dedupes_but_25bps_change_notifies(ledger, observe_assessment):
    ledger.reconcile_global(observe_assessment, observed_at_ms=NOW_MS)
    assert ledger.reconcile_global(observe_assessment, observed_at_ms=NOW_MS + 120_000).kind == "unchanged"
    changed = replace(observe_assessment, gross_edge_bps=observe_assessment.gross_edge_bps + 25)
    assert ledger.reconcile_global(changed, observed_at_ms=NOW_MS + 240_000).kind == "edge-changed"
```

Cover ±20% capacity, below-threshold close, incomplete-data unavailable, membership invalidation, failed delivery retry, transaction rollback, and 30-day compaction.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/routing/test_opportunity_ledger.py -q`

Expected: FAIL because ledger tables and module are absent.

- [ ] **Step 3: Implement DDL, migration, and ledger**

Create master, append-only observation, notification, and daily-summary tables. Facts include identity, initial and current Structure/Quote references, source (`global`/`focused`), state/reason, cost/edge/capacity, changed token, timestamps, delivery attempts/error. Add indexes for active masters, replay order, and pending notifications. Use additive `init_schema` migration columns only.

```python
class OpportunityLedger:
    def reconcile_global(self, assessment: GroupAssessment, *, observed_at_ms: int) -> OpportunityTransition: ...
    def record_focused(self, observation: FocusedObservation, *, observed_at_ms: int) -> OpportunityTransition: ...
    def pending_notifications(self, *, now_ms: int, limit: int = 20) -> tuple[PendingNotification, ...]: ...
    def mark_notification_delivered(self, notification_id: int, *, delivered_at_ms: int) -> None: ...
    def mark_notification_failed(self, notification_id: int, *, attempted_at_ms: int, error_kind: str) -> None: ...
```

Open on first observe; close only with complete valid below-threshold facts; mark unavailable for incomplete facts; invalidate on changed membership. Enqueue only entered, material edge/capacity, closed, and invalidated. Focused input never opens a master.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/routing/test_opportunity_ledger.py tests/routing/test_neg_risk_quote_store.py -q`

Expected: PASS; failed Telegram leaves durable market fact and retryable outbox. Commit with `git add src/polyarb/storage/schemas.py src/polyarb/storage/sqlite_store.py src/polyarb/routing/opportunity_ledger.py tests/routing/test_opportunity_ledger.py && git commit -m "feat(m1): persist neg-risk opportunity lifecycle evidence"`.

## Task 3: Global reconciliation and alert delivery

**Files:** Modify `src/polyarb/daemon/quote_worker.py`, `src/polyarb/daemon/alerts.py`; create `src/polyarb/daemon/opportunity_watcher.py`, `tests/daemon/test_opportunity_watcher.py`; modify `tests/daemon/test_quote_worker.py`.

**Interfaces:** Consume Tasks 1–2 after a certified `CompleteQuoteProjection`. Produce `OpportunityWatcher.reconcile_global_projection()`, `deliver_pending_notifications()`, and a compact runtime snapshot. Failed/collecting Quote runs cannot affect lifecycle facts; Telegram failure cannot invalidate a global feed.

- [ ] **Step 1: Write failing watcher tests**

```python
async def test_global_reconciliation_runs_only_after_certification(settings, ledger, complete_projection):
    watcher = OpportunityWatcher.for_test(settings, ledger=ledger)
    await watcher.reconcile_global_projection(complete_projection)
    assert ledger.current_opportunities()[0]["status"] == "observe"

async def test_telegram_failure_is_retryable_without_losing_observation(settings, ledger):
    watcher = OpportunityWatcher.for_test(settings, ledger=ledger, send_telegram=AsyncMock(side_effect=OSError()))
    await watcher.deliver_pending_notifications()
    assert ledger.current_opportunities()[0]["status"] == "observe"
```

Assert a stable run sends no duplicate, close sends one close card, and every card has the required warning plus `execution_status=not-verified`.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/daemon/test_opportunity_watcher.py -q`

Expected: FAIL because `OpportunityWatcher` is absent.

- [ ] **Step 3: Implement watcher and hook**

```python
async def reconcile_global_projection(self, projection: CompleteQuoteProjection) -> None:
    assessed = await asyncio.to_thread(assess_certified_neg_risk_quote_projection, projection, min_edge_bps=self._settings.neg_risk_observe_min_edge_bps)
    for item in assessed.assessments:
        await asyncio.to_thread(self._ledger.reconcile_global, item, observed_at_ms=self._clock_ms())
    await self.deliver_pending_notifications()
```

Add `send_opportunity_alert(settings, card)` to `alerts.py` using the direct Telegram transport and raising on send failure. Build one watcher in `build_production_quote_worker`, invoke it after certification and before publishing the compact feed, and catch watcher errors separately so a complete Quote feed remains true.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/daemon/test_opportunity_watcher.py tests/daemon/test_quote_worker.py tests/m1-perception/test_quote_feed_health.py -q`

Expected: PASS. Commit with `git add src/polyarb/daemon/quote_worker.py src/polyarb/daemon/alerts.py src/polyarb/daemon/opportunity_watcher.py tests/daemon/test_opportunity_watcher.py tests/daemon/test_quote_worker.py && git commit -m "feat(m1): reconcile global opportunity observations"`.

## Task 4: Focused tracker

**Files:** Create `src/polyarb/routing/focused_quote_collector.py`, `tests/routing/test_focused_quote_collector.py`; modify `src/polyarb/daemon/opportunity_watcher.py`, `src/polyarb/daemon/main.py`, `tests/daemon/test_opportunity_watcher.py`.

**Interfaces:** Consume active ledger masters, `ClobReaderClient.get_books(..., projection="top")`, and current verified Structure membership. Produce `FocusedObservation` states `observe`, `no-edge`, `unavailable`, `invalidated`. Focused records reference their original global Quote run but never write `neg_risk_quote_runs`.

- [ ] **Step 1: Write failing targeted collection tests**

```python
async def test_focused_collector_rechecks_all_durable_legs(active_opportunity, reader, membership_reader):
    result = await collect_focused_observation(active_opportunity, reader=reader, membership_reader=membership_reader)
    assert result.status == "observe"
    assert result.bundle_cost == 0.97

async def test_membership_change_invalidates_before_clob_fetch(active_opportunity, reader, changed_membership_reader):
    result = await collect_focused_observation(active_opportunity, reader=reader, membership_reader=changed_membership_reader)
    assert result.status == "invalidated"
    assert reader.requests == []
```

Add missing-book unavailable and valid below-threshold close cases.

- [ ] **Step 2: Run the red test**

Run: `uv run pytest tests/routing/test_focused_quote_collector.py -q`

Expected: FAIL because the focused collector is absent.

- [ ] **Step 3: Implement collector and periodic loop**

```python
async def collect_focused_observation(opportunity, *, reader, membership_reader, now_ms):
    current = await asyncio.to_thread(membership_reader.current_group, opportunity.event_id, opportunity.group_id)
    if current is None or current.membership_hash != opportunity.membership_hash:
        return FocusedObservation.invalidated(opportunity, reason="structure-membership-changed")
    books = await reader.get_books(list(current.yes_token_ids), projection="top")
    return build_focused_observation(opportunity, current, books, observed_at_ms=now_ms())
```

Reuse existing top-of-book parsing rules. `OpportunityWatcher.run(stop_event)` loads active masters, collects with the existing limiter, writes `record_focused`, drains retries, then waits 15 seconds. Start/cancel/await it in `daemon.main` beside scheduler and Quote worker.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/routing/test_focused_quote_collector.py tests/daemon/test_opportunity_watcher.py tests/daemon/test_quote_worker.py -q`

Expected: PASS; empty watchlist makes no CLOB call and cancellation preserves committed facts. Commit with `git add src/polyarb/routing/focused_quote_collector.py src/polyarb/daemon/opportunity_watcher.py src/polyarb/daemon/main.py tests/routing/test_focused_quote_collector.py tests/daemon/test_opportunity_watcher.py && git commit -m "feat(m1): focus-track observed neg-risk opportunities"`.

## Task 5: Cloud read models, triggers, and Make controls

**Files:** Create `src/polyarb/http/market_map.py`, `src/polyarb/cli_perception.py`, `tests/m1-perception/test_opportunity_watcher_http.py`; modify `src/polyarb/http/arbitrage.py`, `src/polyarb/http/app.py`, `src/polyarb/http/control.py`, `src/polyarb/daemon/scheduler.py`, `src/polyarb/daemon/quote_worker.py`, `src/polyarb/config.py`, `fly.toml`, `Makefile`, `tests/test_makefile.py`.

**Interfaces:** Public reads are `/market-map`, `/market-map?event_id=`, `/opportunity-watch/status`, `/arbitrage/opportunities`, and `/arbitrage/opportunities/<id>/history`. HMAC writes are `/control/market-map/build` and `/control/neg-risk/scan`.

- [ ] **Step 1: Write failing HTTP and Make tests**

```python
def test_market_map_exposes_scannable_and_rejected_groups(http_test_client):
    response = http_test_client.get("/market-map")
    assert response.status_code == 200
    assert response.json()["scannable_groups"][0]["quality"] == "complete-supported"

def test_opportunity_targets_are_discoverable_and_cloud_only():
    for target in ("build-market-map", "inspect-market-map", "scan-neg-risk-map", "watch-opportunities-status", "watch-opportunities", "watch-opportunity-history"):
        assert f"{target}:" in _make("help").stdout
        assert "data/state.db" not in _make_recipe(target)
```

Also test no map = bounded 503, missing HMAC = 401, valid trigger = `202 queued`, duplicate trigger = `200 already_queued`, and public reads never invoke CLOB/scheduler.

- [ ] **Step 2: Run the red tests**

Run: `uv run pytest tests/m1-perception/test_opportunity_watcher_http.py tests/test_makefile.py -q`

Expected: FAIL because routes, controls, settings, and Make targets are absent.

- [ ] **Step 3: Implement exact cloud contracts**

`market_map.py` performs bounded read-only SQLite reads for revision/age, scannable groups, rejected groups/reasons, event detail, current durable opportunities, replay history, and watcher state. Enrich legacy feed with durable opportunity id and `execution_status="not-verified"` without removing old fields.

Add event-backed `request_now()` methods to Scheduler and Quote worker so a trigger queues one normal cycle and never starts a concurrent child. Existing Control HMAC protects both new routes, which return `202 queued`, `200 already_queued`, or `409 unavailable` for disabled/paused producers. `cli_perception.py` serializes JSON, calculates HMAC from `POLYARB_SCAN_SHARED_SECRET`, exits before a request if secret is empty, and never logs it.

Set Settings defaults to 100bps, 15s focus, 1,800s map age, and 30d retention. Change Fly Structure cadence from 300 to 1,800 seconds, keep global Quote at 120 seconds, and keep the measured 1GB VM unchanged.

Add Make targets: `build-market-map`, `inspect-market-map`, `scan-neg-risk-map`, `watch-opportunities-status`, `watch-opportunities`, `watch-opportunity-history`. Control targets call `cli_perception`; read targets call public cloud GETs. Require `opportunity_id` for replay and document every target as cloud/read-only/no-order.

- [ ] **Step 4: Verify and commit**

Run: `uv run pytest tests/m1-perception/test_opportunity_watcher_http.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/m1-perception/test_scheduler.py tests/daemon/test_quote_worker.py tests/test_makefile.py -q && make help`

Expected: PASS and all six commands appear. Commit with `git add src/polyarb/http/market_map.py src/polyarb/http/arbitrage.py src/polyarb/http/app.py src/polyarb/http/control.py src/polyarb/daemon/scheduler.py src/polyarb/daemon/quote_worker.py src/polyarb/cli_perception.py src/polyarb/config.py fly.toml Makefile tests/m1-perception/test_opportunity_watcher_http.py tests/test_makefile.py && git commit -m "feat(m1): expose cloud opportunity watcher controls"`.

## Task 6: Documentation, verification, and production rollout

**Files:** Modify `docs/M1-市场感知平台使用手册.md`, `docs/learning/00-INDEX.md`, `.planning/JOURNAL.md`; create `docs/learning/28-自动机会盯盘.md`; create the current plan SUMMARY after the implementation commits.

- [ ] **Step 1: Update operator and learning documentation**

Document the exact flow: `make build-market-map`; `make inspect-market-map`; `make scan-neg-risk-map`; `make watch-opportunities-status`; `make watch-opportunities min_edge_bps=100`; `make watch-opportunity-history opportunity_id=<id>`. Explain `observe` is only gross observation, and L2/L3 remain future execution confirmation.

Learning note 28 must contain: 30-second model, code map with `file:line`, why 2-minute global vs 15-second focused work differs, why only top-of-book evidence is retained, a Telegram card walkthrough, four self-check questions, and FAQ 增量 heading.

- [ ] **Step 2: Run complete local gates**

Run: `uv run pytest tests/routing/test_opportunity_scanner.py tests/routing/test_opportunity_ledger.py tests/routing/test_focused_quote_collector.py tests/daemon/test_opportunity_watcher.py tests/m1-perception/test_opportunity_watcher_http.py tests/daemon/test_quote_worker.py tests/m1-perception/test_arbitrage_opportunities_http.py tests/test_makefile.py -q && uv run ruff check src/polyarb/routing/opportunity_scanner.py src/polyarb/routing/opportunity_ledger.py src/polyarb/routing/focused_quote_collector.py src/polyarb/daemon/opportunity_watcher.py src/polyarb/http/market_map.py src/polyarb/cli_perception.py && uv run python -m compileall -q src && make planning-status`

Expected: PASS, no Ruff diagnostics, successful compilation, no planning drift.

- [ ] **Step 3: Deploy and perform honest cloud acceptance**

Deploy the exact tested commit with the existing release procedure; confirm release id in `/health`. Then run `make build-market-map`, `make inspect-market-map`, `make scan-neg-risk-map`, `make watch-opportunities-status`, `make watch-opportunities min_edge_bps=100`, and `make diagnose-arb-feed-prod min_edge_bps=100`.

Observe one map refresh and two global Quote cycles. Do not lower the live threshold to manufacture a card. Record either natural ≥100bps lifecycle evidence or verified no-edge/health evidence, together with release id, Structure revision, Quote run id, and watcher status.

- [ ] **Step 4: Commit documentation and handoff**

Commit with `git add docs/M1-市场感知平台使用手册.md docs/learning/28-自动机会盯盘.md docs/learning/00-INDEX.md .planning/JOURNAL.md .planning/workstreams/m1-perception/phases && git commit -m "docs(m1): record opportunity watcher verification"`. Do not claim profitability, auto-execution readiness, or L2/L3 execution confirmation.
