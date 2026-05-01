---
phase: 01
plan: 2
wave: 2
status: complete
subsystem: clients
tags: [m1-perception, http-clients, gamma, clob, async, retry]
started_at: 2026-04-29T07:49:43Z
completed_at: 2026-04-29T08:21:53Z
duration_minutes: 32
note: "Resumed after socket-crash mid-Wave-2 (T1 committed by previous executor; T2-T5 by this executor). Total wall-clock includes the gap between crash and resume."
dependency_graph:
  requires:
    - "01-1: polyarb.config.Settings (gamma_url, clob_url, rate caps, retry knobs, http_timeout_s)"
  provides:
    - "polyarb.clients.gamma_client.GammaClient — paginated Gamma metadata fetcher"
    - "polyarb.clients.clob_client.ClobReaderClient — async-wrapped sync py-clob-client (books + prices)"
  affects:
    - "01-4 (orchestrator): consumes both clients; will normalize raw payloads"
    - "01-3 (validator): get_books + get_prices_buy_sell are the two ground-truth sources for ghost-book detection (issue #180)"
tech_stack:
  added: []
  pinned:
    - "py-clob-client==0.34.6"
    - "httpx==0.27.2"
    - "tenacity==8.5.0"
    - "aiolimiter==1.2.1"
    - "respx==0.21.1"
    - "pytest-asyncio==0.23.8"
  patterns:
    - "Long-lived AsyncClient (HTTP/2, max_connections=20, follow_redirects=False)"
    - "tenacity AsyncRetrying with retry_if_exception_type whitelist + reraise=True"
    - "Custom _NonRetryableHTTPError to keep 4xx out of tenacity retry"
    - "asyncio.to_thread + manual chunking for sync SDKs"
key_files:
  created:
    - src/polyarb/clients/gamma_client.py
    - src/polyarb/clients/clob_client.py
    - tests/m1-perception/test_gamma_client.py
    - tests/m1-perception/test_clob_client.py
  modified: []
decisions:
  - "Pre-classify 4xx-non-429 into _NonRetryableHTTPError BEFORE r.raise_for_status() so tenacity sees an exception type that is NOT in retry_if_exception_type, guaranteeing exactly 1 attempt for client errors."
  - "get_prices_buy_sell uses dict.update merge — get_prices returns {token_id: {SIDE: price-as-str}}, confirmed via T1 fixture. If a future SDK version returns a list, callers will see TypeError immediately rather than silent data loss."
  - "No retry decoration on ClobReaderClient — py-clob-client has no async retry hook and asyncio.to_thread blocks the worker. Orchestrator (Plan 4) will categorize failures."
  - "follow_redirects=False is httpx's current default but pinned explicitly (F-2) to prevent silent SSRF if a future httpx default flips."
metrics:
  tasks_completed: 5
  tests_total_in_plan: 11
  tests_passing: 11
  tests_passing_full_m1: 51
---

# Phase 01 Plan 02: Async HTTP Clients Summary

Two thin async client classes that talk to Polymarket: `GammaClient` (httpx
async + aiolimiter + tenacity for the metadata REST API) and
`ClobReaderClient` (py-clob-client v0.34.6 sync calls wrapped in
`asyncio.to_thread` + manual batching at 500 per call for both order books
and prices). Returns are RAW SDK output verbatim — normalization is owned
by Plan 4. Issue #180 ghost-book defense is wired by exposing both
`get_books` (sizes) and `get_prices_buy_sell` (independent ground-truth
prices) as separate methods that Plan 3's Layer 4 validator cross-references.

## Per-task table

| Task | Description | Commit | Files | Status |
|------|-------------|--------|-------|--------|
| T1   | Record live API fixtures (real one-shot calls; F-4 sanitized) | `bde8303` | tests/m1-perception/fixtures/{__init__.py, gamma_sample.json, clob_sample.json} | ✅ done by previous executor (pre-crash) |
| T2   | GammaClient (httpx async + aiolimiter + tenacity + pagination) | `46f6997` | src/polyarb/clients/gamma_client.py | ✅ |
| T3   | ClobReaderClient (sync SDK + asyncio.to_thread + batching) | `3902c66` | src/polyarb/clients/clob_client.py | ✅ |
| T4   | GammaClient unit tests (6 tests, respx mocked) | `677fe6c` | tests/m1-perception/test_gamma_client.py | ✅ |
| T5   | ClobReaderClient unit tests (5 tests, patch.object mocked) | `7735c8e` | tests/m1-perception/test_clob_client.py | ✅ |

**Verification (final):**
```bash
python -m pytest tests/m1-perception/test_gamma_client.py tests/m1-perception/test_clob_client.py -v
# 11 passed in 0.67s

python -c "from polyarb.clients.gamma_client import GammaClient; \
  from polyarb.clients.clob_client import ClobReaderClient; \
  from polyarb.config import Settings; s=Settings(); \
  GammaClient(s); ClobReaderClient(s); print('CLIENTS_OK')"
# CLIENTS_OK

# Full m1-perception suite (regression check)
python -m pytest tests/m1-perception/
# 51 passed in 1.14s
```

## Empirical findings (resolves RESEARCH.md Open Questions)

These are now-confirmed against live Polymarket data (recorded in T1) and
hard-coded into the test mocks; downstream plans should rely on them.

### CLOB book token-id field name → `asset_id`
The `get_order_books` response is a `list[OrderBookSummary]` (not list of
plain dict — it's a dataclass-like object with `__dict__`). The token-id is
exposed as the **`asset_id`** attribute. The `market` field on the same
object is the *conditionId* (a different identifier, e.g.
`0x9c1a953fe92c8357f1b646ba25d983aa83e90c525992db14fb726fa895cb5763`).

Plan 3 validator already keys off `asset_id` → token_id (confirmed by reading
the recorded fixture).

### `get_prices` return shape → nested per-token-side dict
```python
get_prices([BookParams(token_id=t, side="BUY") for t in tids])
# returns: {token_id: {"BUY": "0.46"}}

get_prices([BookParams(token_id=t, side="SELL") for t in tids])
# returns: {token_id: {"SELL": "0.47"}}
```

Therefore `get_prices_buy_sell` merges per-side via `dict.update` and exposes:
```python
{
    "buy":  {token_id: {"BUY":  "0.46"}},
    "sell": {token_id: {"SELL": "0.47"}},
}
```

### Price values are strings, not floats
Every price in both sources is `str` (e.g. `"0.46"`, `"0.530"`). Callers
**must coerce** with `float()` defensively. Plan 3 validator already wraps
this in `_safe_float` (F-1) — orchestrator (Plan 4) does the same.

### `bids[0].price` and `bids[0].size` are also strings
Confirmed in fixture: `{"price": "0.01", "size": "1024915.06"}`. F-1 try/except
on float parsing is mandatory in any consumer.

## API surface for Plan 04 orchestrator

```python
from polyarb.clients.gamma_client import GammaClient
from polyarb.clients.clob_client import ClobReaderClient

# GammaClient
class GammaClient:
    PAGE_LIMIT: int = 100
    MAX_PAGES: int = 1000  # F-2 ceiling
    def __init__(self, settings: Settings) -> None: ...
    async def aclose(self) -> None: ...
    async def __aenter__(self) -> GammaClient: ...
    async def __aexit__(self, *_) -> None: ...
    async def fetch_all_active_markets(self) -> list[dict]: ...
        # raises RuntimeError on >=MAX_PAGES (F-2) or non-list response
        # raises _NonRetryableHTTPError on 4xx (non-429)
        # raises httpx.HTTPStatusError on 5xx after retry exhaustion
        # raises httpx.RequestError / TimeoutException after retry exhaustion
        # raises json.JSONDecodeError immediately (NOT retried — F-6)

# ClobReaderClient
class ClobReaderClient:
    def __init__(self, settings: Settings) -> None: ...
    async def get_books(self, token_ids: list[str]) -> list[OrderBookSummary]: ...
        # OrderBookSummary attrs: market, asset_id, bids, asks, timestamp,
        #                        min_order_size, neg_risk, tick_size,
        #                        last_trade_price, hash
        # bids/asks are list[OrderSummary] with .price (str) and .size (str)
        # Empty token_ids → [] (no SDK call)
    async def get_prices_buy_sell(
        self, token_ids: list[str]
    ) -> dict[str, dict[str, dict[str, str]]]: ...
        # Returns: {"buy": {tid: {"BUY": price-str}}, "sell": {tid: {"SELL": price-str}}}
        # Empty token_ids → {"buy": {}, "sell": {}} (no SDK calls)
```

## Deviations from plan

**None requiring Rule 1/2/3 fixes.** All tasks executed exactly as written
in `01-2-PLAN.md`. The implementation choices (e.g. how to make 4xx
non-retryable, how to merge `get_prices` results) were explicitly enumerated
in the plan as decision points and resolved in line with the plan's
preferred path.

### Notable in-plan refinements (not deviations)

- **4xx non-retry mechanism**: plan listed "OR add a custom `_NonRetryable`"
  as the option chosen. Implemented as `_NonRetryableHTTPError(Exception)`
  raised before `r.raise_for_status()` propagates the original
  `HTTPStatusError` — the original is preserved on `__cause__` for
  introspection.

- **`get_prices` merge logic**: plan said "merge with `update` if
  dict-of-token-id, else extend list". The fixture confirmed dict-of-token-id
  shape, so we use `dict.update`. Documented in module docstring.

## Resumption / recovery notes

This plan was **resumed after a socket crash mid-Wave-2**. Effects on this
SUMMARY:

- T1 was committed by the previous (crashed) executor at `bde8303` before
  the crash. This SUMMARY's `started_at` reflects T1's commit time.
- T2-T5 were executed and committed in this resumed run.
- The pre-crash `record_fixtures.py` working artifact at the project root is
  not present in `git status` — either the previous executor cleaned it up
  before crashing, or it was never persisted. Either way, fixtures are
  committed and good; no re-record needed.
- **Pyproject.toml retro-add NOT needed**: `respx`, `pytest-asyncio`,
  `aiolimiter`, `tenacity` are all already installed in `.venv` from the
  Wave-1 `pip install -e .[dev]`. Confirmed via `importlib.metadata.version`.

## Self-Check

Performed before writing this section.

- [x] `src/polyarb/clients/gamma_client.py` exists (FOUND)
- [x] `src/polyarb/clients/clob_client.py` exists (FOUND)
- [x] `tests/m1-perception/test_gamma_client.py` exists (FOUND)
- [x] `tests/m1-perception/test_clob_client.py` exists (FOUND)
- [x] Commit `46f6997` (T2 GammaClient) in git log (FOUND)
- [x] Commit `3902c66` (T3 ClobReaderClient) in git log (FOUND)
- [x] Commit `677fe6c` (T4 gamma tests) in git log (FOUND)
- [x] Commit `7735c8e` (T5 clob tests) in git log (FOUND)
- [x] All 11 plan tests pass (`6 + 5 = 11 passed in 0.67s`)
- [x] All 51 m1-perception tests pass (no regression in Wave-1 work)

## Self-Check: PASSED

## Open items for Plan 04/05

- **Rate-limit observation**: aiolimiter caps are configured (Gamma 280/10s,
  CLOB 450/10s) but never observed against real CLOB load. Plan 5 fixture
  E2E test should record actual request timings to confirm we never get
  HTTP 429 in practice.
- **Retry latency**: `retry_min_wait_s=1.0, retry_max_wait_s=4.0` defaults
  mean a single transient failure can add up to 4s to a snapshot. With
  ~20k markets and 200 batches, even 1% failure rate could add seconds.
  Plan 5 should benchmark and tune.
- **No CLOB retry**: by design (no async retry hook, blocking worker).
  If Plan 5 reveals frequent CLOB failures, add a manual retry loop to
  `ClobReaderClient.get_books` / `get_prices_buy_sell` (NOT tenacity-based).
