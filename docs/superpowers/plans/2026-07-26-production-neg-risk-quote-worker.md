# Production Neg-Risk Quote Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use test-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep the production known-universe neg-risk opportunity feed fresh by
running one fail-soft quote collector inside the L1 app process.

**Architecture:** A focused `QuoteWorker` owns one sequential collection loop
and process-local attempt state. Durable quote-run tables remain the source of
truth for feed availability; L1 health reads the newest complete run and warns
before the existing 300-second route cutoff.

**Tech Stack:** Python 3.12, asyncio, Starlette, SQLite WAL, pytest, uv, Fly.io.

## Global Constraints

- Public CLOB reads only; no wallet, signing, or order API.
- Production cadence is exactly 120 seconds.
- Route quote SLA remains exactly 300 seconds.
- Universe SLA remains exactly 50,400 seconds.
- Worker is disabled by default and enabled explicitly only in `fly.toml`.
- Failed attempts never replace or refresh the last complete run.
- Production verification must span at least three complete runs.

---

### Task 1: Sequential fail-soft quote worker

**Files:**
- Create: `src/polyarb/daemon/quote_worker.py`
- Create: `tests/daemon/test_quote_worker.py`
- Modify: `src/polyarb/config.py`

**Interfaces:**
- Produces: `QuoteWorkerRuntime`, `QuoteWorker`, and
  `build_production_quote_worker(settings) -> QuoteWorker | None`.
- Consumes: `collect_neg_risk_quotes`, `NegRiskQuoteStore`,
  `ClobReaderClient`, `Settings.db_path`.

- [ ] **Step 1: Write failing worker tests**

Create tests with an injected async `collect_once`:

```python
async def test_worker_collects_immediately_then_waits_for_interval(): ...
async def test_worker_never_overlaps_collections(): ...
async def test_worker_failure_is_recorded_and_next_attempt_can_succeed(): ...
async def test_worker_cancellation_propagates_without_failure_count(): ...
def test_builder_is_disabled_by_default_and_uses_settings_db_when_enabled(): ...
```

Use a real `QuoteCollectionResult`. Assert run ID, requested/accepted counts,
duration, attempt count, consecutive failures, and bounded state.

- [ ] **Step 2: Run RED**

```bash
uv run pytest -q tests/daemon/test_quote_worker.py
```

Expected: import failure because `polyarb.daemon.quote_worker` is absent.

- [ ] **Step 3: Implement minimal worker and settings**

The production collector is:

```python
async def collect_once() -> QuoteCollectionResult:
    return await collect_neg_risk_quotes(
        quote_store=NegRiskQuoteStore(settings.db_path),
        reader=ClobReaderClient(settings),
    )
```

`run(stop_event)` performs one immediate attempt, catches ordinary exceptions,
waits via `asyncio.wait_for(stop_event.wait(), timeout=interval_s)`, and
propagates `CancelledError`. Runtime state stores only the exception class.

Add:

```python
neg_risk_quote_worker_enabled: bool = False
neg_risk_quote_interval_s: int = Field(default=120, gt=0, le=240)
```

- [ ] **Step 4: Run GREEN**

```bash
uv run pytest -q tests/daemon/test_quote_worker.py
```

Expected: all worker tests pass.

### Task 2: Lifecycle and health chain-truth

**Files:**
- Modify: `src/polyarb/daemon/main.py`
- Modify: `src/polyarb/http/app.py`
- Modify: `src/polyarb/http/health.py`
- Modify: `src/polyarb/http/arbitrage.py`
- Modify: `src/polyarb/routing/opportunity_scanner.py`
- Modify: `fly.toml`
- Create: `tests/m1-perception/test_quote_feed_health.py`
- Create: `tests/m1-perception/test_l1_quote_worker_wiring.py`
- Modify: `tests/m1-perception/test_arbitrage_opportunities_http.py`

**Interfaces:**
- Consumes: `QuoteWorker.runtime` and `QuoteWorker.run(stop_event)`.
- Produces: `quote_feed:last_complete_age_seconds` and
  `quote_feed:collector_state` in both health endpoints.

- [ ] **Step 1: Write failing health and wiring tests**

```python
def test_enabled_health_fails_when_no_complete_run(): ...
def test_quote_age_239_seconds_passes(): ...
def test_quote_age_240_seconds_warns(): ...
def test_quote_age_300_seconds_warns(): ...
def test_quote_age_over_300_seconds_fails(): ...
def test_worker_error_warns_while_complete_run_is_fresh(): ...
def test_disabled_worker_registers_no_quote_checks(): ...
def test_create_app_exposes_quote_worker_runtime(): ...
def test_l1_main_starts_and_cancels_quote_worker_with_scheduler(): ...
def test_fly_enables_worker_at_120_seconds(): ...
```

Use temporary real SQLite storage for age boundaries. Patch Uvicorn, scheduler,
and worker for the lifecycle test.

- [ ] **Step 2: Run RED**

```bash
uv run pytest -q \
  tests/m1-perception/test_quote_feed_health.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py
```

Expected: missing health checks, lifecycle wiring, and Fly enablement.

- [ ] **Step 3: Implement shared SLA and health**

Expose and reuse:

```python
QUOTE_SLA_SECONDS = 300
UNIVERSE_SLA_SECONDS = 50_400
```

Extend `_build_health_checks` with optional runtime state. When enabled, read
`NegRiskQuoteStore(settings.db_path).latest_complete_run()`. Missing or
too-old durable success fails health; a process-local error warns while the
durable success remains fresh.

- [ ] **Step 4: Wire lifecycle and production config**

`create_app` stores optional `quote_worker_runtime`. `main` builds and starts
the worker after Uvicorn binds, cancels it on shutdown, and includes it in the
five-second bounded gather.

```toml
POLYARB_NEG_RISK_QUOTE_WORKER_ENABLED = "true"
POLYARB_NEG_RISK_QUOTE_INTERVAL_S = "120"
```

- [ ] **Step 5: Run focused GREEN**

```bash
uv run pytest -q \
  tests/daemon/test_quote_worker.py \
  tests/m1-perception/test_quote_feed_health.py \
  tests/m1-perception/test_l1_quote_worker_wiring.py \
  tests/m1-perception/test_arbitrage_opportunities_http.py \
  tests/routing/test_opportunity_scanner.py \
  tests/routing/test_neg_risk_quote_store.py \
  tests/routing/test_neg_risk_quote_collector.py
```

Expected: all focused tests pass.

### Task 3: Documentation, deployment, and repeated proof

**Files:**
- Modify: `docs/M1-市场感知平台使用手册.md`
- Create: `docs/learning/23-生产机会流.md`
- Modify: `docs/learning/00-INDEX.md`
- Modify: `.planning/JOURNAL.md`
- Modify: `.planning/CURRENT.md`
- Modify: `.planning/workstreams/m1-perception/STATE.md`
- Create: `.planning/workstreams/m1-perception/phases/05.5-production-opportunity-feed/05.5-01-SUMMARY.md`

**Interfaces:**
- Consumes: Task 2 production health and opportunity surfaces.
- Produces: plain-language operating and limitation guidance.

- [ ] **Step 1: Update operator documentation**

Document `make diagnose-arb-feed-prod min_edge_bps=0`, fresh zero, positive,
stale quote, stale universe, unavailable, the 120/240/300-second timing, and
the known-universe/gross-before-fees/no-order boundary.

- [ ] **Step 2: Run full local verification**

```bash
uv run pytest -q
uv run ruff check src tests
uv run python -m compileall -q src
make docs-m1-check
make planning-status
git diff --check
docker build -t polyarb:quote-worker .
```

Expected: every command exits zero.

- [ ] **Step 3: Commit, push, and deploy**

Create the required summary, commit all phase files, push the exact commit, and
verify the Fly image label matches it. Do not change secrets, VM counts, wallet
state, or database schema.

- [ ] **Step 4: Verify repeated production runs**

Over at least 260 seconds, capture three increasing complete run IDs, response
coverage, quote health age below 240 seconds, repeated HTTP 200 responses, and
unchanged snapshot/process health.

- [ ] **Step 5: Close state accurately**

Record exact source/image/run evidence. Mark this operational only as a
known-universe discovery feed, never fee-adjusted or real-money ready.

## Self-review

- Spec coverage: cadence, no overlap, fail-soft behavior, same-volume
  placement, health, shutdown, docs, deployment, and repeated proof are mapped.
- Placeholder scan: no TBD/TODO or unspecified implementation remains.
- Type consistency: worker runtime, lifecycle, and shared SLA names match.
