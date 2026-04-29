---
phase: 01
plan: 2
type: execute
wave: 2
depends_on: [01-1]
files_modified:
  - src/polyarb/clients/gamma_client.py
  - src/polyarb/clients/clob_client.py
  - tests/m1-perception/fixtures/__init__.py
  - tests/m1-perception/fixtures/gamma_sample.json
  - tests/m1-perception/fixtures/clob_sample.json
  - tests/m1-perception/test_gamma_client.py
  - tests/m1-perception/test_clob_client.py
autonomous: true
requirements: []
must_haves:
  truths:
    - "GammaClient.fetch_all_active_markets() paginates /markets and returns list[dict] of raw responses"
    - "GammaClient retries 3x with exponential backoff on httpx.RequestError / 5xx / 429 / timeout, raises tenacity.RetryError on exhaustion"
    - "GammaClient does NOT retry on 4xx (other than 429)"
    - "ClobReaderClient.get_books(token_ids) batches at 500 per call to py_clob_client.get_order_books, returns list[dict]"
    - "ClobReaderClient.get_prices_buy_sell(token_ids) returns {'buy': <map>, 'sell': <map>}"
    - "All py-clob-client sync calls run inside asyncio.to_thread"
    - "Both clients respect aiolimiter caps (Gamma 280/10s, CLOB 450/10s)"
  artifacts:
    - path: src/polyarb/clients/gamma_client.py
      provides: "Async Gamma metadata client (pagination + rate + retry)"
      exports: ["GammaClient"]
    - path: src/polyarb/clients/clob_client.py
      provides: "Async wrapper over sync py-clob-client (batching + dual-source)"
      exports: ["ClobReaderClient"]
    - path: tests/m1-perception/fixtures/gamma_sample.json
      provides: "Recorded Gamma /markets response shape"
    - path: tests/m1-perception/fixtures/clob_sample.json
      provides: "Recorded CLOB get_order_books + get_prices response shapes"
  key_links:
    - from: "GammaClient.__init__"
      to: "polyarb.config.Settings"
      via: "constructor reads gamma_url, gamma_rate_per_10s, retry_*, http_timeout_s"
      pattern: "settings\\.gamma_url"
    - from: "ClobReaderClient.get_books"
      to: "py_clob_client.client.ClobClient.get_order_books"
      via: "asyncio.to_thread + BookParams chunks of 500"
      pattern: "asyncio\\.to_thread"
---

<objective>
Build two HTTP clients that talk to Polymarket: `GammaClient` (httpx async + aiolimiter + tenacity) and `ClobReaderClient` (sync py-clob-client wrapped in asyncio.to_thread + batching at 500 per call). Both return RAW dicts; normalization is owned by Plan 4. Issue #180 ghost-book defense lives in Plan 3 validator, but Plan 2's `ClobReaderClient` MUST expose both `get_books` (for sizes) and `get_prices_buy_sell` (for ground-truth prices) so Plan 3 has both data sources to compare.

Output: 2 client modules + recorded fixtures + 2 unit tests using respx (httpx mock) and unittest.mock (sync SDK mock).
</objective>

<execution_context>
@$HOME/.claude/get-shit-done/workflows/execute-plan.md
@$HOME/.claude/get-shit-done/templates/summary.md
</execution_context>

<context>
@.planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md
@.planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md
@.planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md
@.planning/workstreams/m1-perception/phases/01-/01-1-SUMMARY.md
@src/polyarb/config.py
@3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py
</context>

<interfaces>
Settings (from Plan 1, src/polyarb/config.py):
- gamma_url, clob_url, gamma_rate_per_10s, clob_batch_rate_per_10s, clob_batch_size,
  retry_attempts, retry_min_wait_s, retry_max_wait_s, http_timeout_s

py-clob-client v0.34.6 sync API (verify exact signatures by reading the installed package — `python -c "import inspect; from py_clob_client.client import ClobClient; print(inspect.signature(ClobClient.__init__))"`):
- `ClobClient(host: str)` — L0 read-only, no wallet/key needed
- `.get_order_books(params: list[BookParams]) -> list[<book_dict>]`  (batch up to 500)
- `.get_prices(params: list[BookParams]) -> dict`  (BookParams must include side="BUY"|"SELL")
- `BookParams(token_id: str, side: str | None = None)`
</interfaces>

## Goal

Two client classes that downstream code calls with constructor-injected `Settings`. Clients return raw API responses verbatim — no JSON-string parsing, no field renaming, no flattening. The `ClobReaderClient` exposes BOTH books and prices as separate methods so Plan 3 Layer 4 validator can detect ghost-books (issue #180) by cross-referencing.

<tasks>

<task type="auto">
  <id>T1</id>
  <name>Task 1: Record live API fixtures (real one-shot calls)</name>
  <files>
    tests/m1-perception/fixtures/__init__.py,
    tests/m1-perception/fixtures/gamma_sample.json,
    tests/m1-perception/fixtures/clob_sample.json
  </files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Open Questions #1, #5 — ground truth for SDK return shape)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 5 — fixture rationale)
  </read_first>
  <action>
    Create `tests/m1-perception/fixtures/__init__.py` empty.

    Run a one-shot recording script (do NOT commit the script) that:
    1. Calls `httpx.get("https://gamma-api.polymarket.com/markets", params={"active":"true","closed":"false","archived":"false","limit":5,"offset":0}, timeout=15.0)` — write `r.json()` to `tests/m1-perception/fixtures/gamma_sample.json` with `json.dump(..., indent=2)`. Must be a list of >= 1 market dict.
    2. Take `clobTokenIds` from first market: `tids = json.loads(markets[0]["clobTokenIds"])` (it's a JSON string per RESEARCH.md Pitfall 2 — must `json.loads`).
    3. Call sync `from py_clob_client.client import ClobClient; from py_clob_client.clob_types import BookParams; c = ClobClient("https://clob.polymarket.com")`:
       - `books = c.get_order_books([BookParams(token_id=t) for t in tids])`
       - `pb = c.get_prices([BookParams(token_id=t, side="BUY") for t in tids])`
       - `ps = c.get_prices([BookParams(token_id=t, side="SELL") for t in tids])`
    4. Write to `tests/m1-perception/fixtures/clob_sample.json`:
       ```json
       {"token_ids": [...], "books": [...], "prices_buy": ..., "prices_sell": ...}
       ```
       **F-4 SECURITY**: Do NOT use unbounded `default=lambda o: o.__dict__`. py-clob-client
       SDK objects may have `__dict__` containing client config (host, internal session) and,
       if a future SDK version adds wallet support and a developer leaves keys in env, those
       could land in serialized fixtures committed to git. Use an explicit whitelist:
       ```python
       def _safe_default(o):
           # BookParams: extract only token_id + side
           if hasattr(o, "token_id"):
               return {"token_id": getattr(o, "token_id", None), "side": getattr(o, "side", None)}
           # Anything else: stringify (defensive — should not happen for typical book/prices output)
           return str(o)
       json.dumps(obj, indent=2, default=_safe_default)
       ```

    5. **F-4 SECURITY** — fixture commit policy: fixtures ARE committed to git (so CI can
       run mocked tests without network). Before completing this task, run a credential-leak
       check on both files:
       ```bash
       grep -iE "authorization|cookie|x-api-key|bearer|secret|private[_-]?key" tests/m1-perception/fixtures/*.json
       ```
       This grep MUST return nothing (exit 1). If anything matches, remove the offending
       field manually and re-run. Add a regression test in Plan 5 (T1 conftest) that scans
       fixtures on every test run.

    After saving, REPORT in the plan summary (this is the resolution of RESEARCH.md Open Q#1):
    - The exact key name CLOB book uses for token id (`asset_id` / `market` / `token_id`)
    - The exact shape of `get_prices` return (flat dict vs nested)
    - Whether `bids[0].price` is `str` or `float`

    Network-unavailable fallback: write minimal stub fixtures matching the shapes documented in RESEARCH.md Pattern 1 / Pattern 2 / Pitfall 2, and flag in summary that fixtures are stubs (must re-record before Plan 5).
  </action>
  <verify>
    <automated>test -f tests/m1-perception/fixtures/gamma_sample.json && test -f tests/m1-perception/fixtures/clob_sample.json && python -c "import json; g=json.load(open('tests/m1-perception/fixtures/gamma_sample.json')); assert isinstance(g, list) and len(g) >= 1 and 'clobTokenIds' in g[0]; c=json.load(open('tests/m1-perception/fixtures/clob_sample.json')); assert {'token_ids','books','prices_buy','prices_sell'} <= set(c.keys()); print('FIXTURES_OK')" && ! grep -iEq "authorization|cookie|x-api-key|bearer|secret|private[_-]?key" tests/m1-perception/fixtures/*.json && echo F4_CLEAN</automated>
  </verify>
  <done>Both fixtures exist, parse, gamma_sample is non-empty list with clobTokenIds field, clob_sample has all 4 expected keys; F-4 credential-leak grep returns clean</done>
</task>

<task type="auto">
  <id>T2</id>
  <name>Task 2: Implement GammaClient (httpx async + aiolimiter + tenacity + pagination)</name>
  <files>src/polyarb/clients/gamma_client.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 1 lines 260-308; Pitfall 5 — gather without limit; Anti-Patterns #7 — only retry transient)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 2 — gamma_client analog; "fail loud, categorize")
    - 3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py (lines 143-167 — pagination shape; DO NOT copy per-call `async with httpx.AsyncClient` — must be long-lived)
    - src/polyarb/config.py
  </read_first>
  <action>
    Create `src/polyarb/clients/gamma_client.py` exporting one class `GammaClient`:

    Required structure:
    - `from __future__ import annotations` first line
    - Imports: `httpx`, `aiolimiter.AsyncLimiter`, `loguru.logger`, `tenacity` (`AsyncRetrying`, `retry_if_exception_type`, `stop_after_attempt`, `wait_exponential`), `polyarb.config.Settings`
    - Class `GammaClient`:
      - Class attr `PAGE_LIMIT = 100`
      - Class attr `MAX_PAGES = 1000`  # F-2 SECURITY: ceiling on pagination — Polymarket has ~20k active markets, 1000 pages × 100 = 100k is far above any realistic size. Prevents OOM on a buggy/hostile endpoint that returns full pages forever.
      - `__init__(self, settings: Settings)` — store settings, build `self._limiter = AsyncLimiter(settings.gamma_rate_per_10s, 10)`, build LONG-LIVED `self._http = httpx.AsyncClient(timeout=settings.http_timeout_s, limits=httpx.Limits(max_connections=20, max_keepalive_connections=10), headers={"User-Agent": "polyarb/0.1"}, http2=True, follow_redirects=False)`
        - **F-2 SECURITY**: `follow_redirects=False` is httpx's current default but pin it explicitly to prevent silent SSRF exposure if a future httpx default flips. Polymarket's CDN should never redirect us.
      - `async def aclose(self) -> None` — `await self._http.aclose()`
      - `async __aenter__/__aexit__` — async context manager support
      - `async def _get(self, path: str, params: dict) -> list[dict] | dict`:
        - Use `AsyncRetrying(stop=stop_after_attempt(settings.retry_attempts), wait=wait_exponential(multiplier=1, min=settings.retry_min_wait_s, max=settings.retry_max_wait_s), retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException)), reraise=True)`
        - Inside the retry block: `async with self._limiter: r = await self._http.get(f"{settings.gamma_url}{path}", params=params); r.raise_for_status(); return r.json()`
        - DO NOT catch exceptions — let `RetryError` (or the underlying error after exhaustion) propagate. The orchestrator (Plan 4) categorizes as `api_unreachable`.
        - **F-6 (deferred to in-flight)**: `json.JSONDecodeError` is NOT in `retry_if_exception_type` — it propagates directly. Document in module docstring: "JSON parse errors at the httpx boundary are non-retryable; orchestrator categorizes as API_UNREACHABLE. Rationale: a 200 with malformed JSON usually indicates CDN/cache misconfiguration, not transient network."
      - `async def fetch_all_active_markets(self) -> list[dict]`:
        - Loop with `offset = 0`, `pages_fetched = 0`, params `{"active":"true","closed":"false","archived":"false","limit": PAGE_LIMIT, "offset": offset}`
        - Append page to `out`; break when `len(page) < PAGE_LIMIT`; else `offset += PAGE_LIMIT; pages_fetched += 1`
        - **F-2 SECURITY**: After incrementing, if `pages_fetched >= MAX_PAGES`: `raise RuntimeError(f"Gamma pagination exceeded {MAX_PAGES} pages — possible runaway response")` (orchestrator catches and records as API_UNREACHABLE)
        - Raise `RuntimeError` if a page is not a list
        - `logger.info(f"Gamma fetched {len(out)} active markets in {pages_fetched} pages")` at end

    Type hints: Python 3.12 syntax only (`list[dict] | dict`, never `Optional[X]`, never `List[Dict]`).
    No normalization. No JSON-string parsing. No retry on 4xx other than 429 (the `r.raise_for_status()` + `retry_if_exception_type(HTTPStatusError)` will retry all HTTPStatusError; refine by adding an `if 400 <= r.status_code < 500 and r.status_code != 429: raise <non-retryable>` guard — use `httpx.HTTPStatusError` raised manually that tenacity won't retry, OR add a custom `_NonRetryable(Exception)` and don't include it in `retry_if_exception_type`).
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "from polyarb.clients.gamma_client import GammaClient; from polyarb.config import Settings; c = GammaClient(Settings()); assert hasattr(c,'fetch_all_active_markets'); assert hasattr(c,'aclose'); assert c.PAGE_LIMIT == 100; assert c.MAX_PAGES == 1000; print('OK')" && grep -q "from aiolimiter" src/polyarb/clients/gamma_client.py && grep -q "wait_exponential" src/polyarb/clients/gamma_client.py && grep -q "from __future__ import annotations" src/polyarb/clients/gamma_client.py && grep -q "follow_redirects=False" src/polyarb/clients/gamma_client.py && grep -q "MAX_PAGES" src/polyarb/clients/gamma_client.py && echo IMPORTS_OK</automated>
  </verify>
  <done>GammaClient class exists, instantiable with Settings, has fetch_all_active_markets + aclose; aiolimiter + tenacity wired; long-lived httpx client (not per-call); 4xx (non-429) does not retry; F-2 follow_redirects=False explicit; F-2 MAX_PAGES=1000 ceiling enforced</done>
</task>

<task type="auto">
  <id>T3</id>
  <name>Task 3: Implement ClobReaderClient (sync SDK + asyncio.to_thread + batching)</name>
  <files>src/polyarb/clients/clob_client.py</files>
  <read_first>
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Pattern 2 lines 310-351 — ENTIRE spec; Pitfall 1 — issue #180; Open Q#1, #3 — RESOLVED in T1 fixture recording)
    - .planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md (Plan 2 — clob_client greenfield)
    - tests/m1-perception/fixtures/clob_sample.json (T1 — actual SDK return shape)
    - src/polyarb/config.py
  </read_first>
  <action>
    Create `src/polyarb/clients/clob_client.py` exporting one class `ClobReaderClient`:

    Required structure:
    - `from __future__ import annotations`
    - Imports: `asyncio`, `aiolimiter.AsyncLimiter`, `loguru.logger`, `py_clob_client.client.ClobClient`, `py_clob_client.clob_types.BookParams`, `polyarb.config.Settings`
    - Class `ClobReaderClient`:
      - `__init__(self, settings: Settings)`:
        - `self._settings = settings`
        - `self._client = ClobClient(settings.clob_url)`  # L0 read-only, no wallet
        - `self._limiter = AsyncLimiter(settings.clob_batch_rate_per_10s, 10)`
      - `async def get_books(self, token_ids: list[str]) -> list[dict]`:
        - Chunk into batches of `settings.clob_batch_size` (default 500)
        - For each chunk: `params = [BookParams(token_id=t) for t in chunk]`
        - `async with self._limiter: books = await asyncio.to_thread(self._client.get_order_books, params)`
        - Extend `out` with the returned list
        - Log `logger.debug(f"CLOB books chunk {i}/{n_chunks}: {len(chunk)} tokens")`
        - Return `out`
      - `async def get_prices_buy_sell(self, token_ids: list[str]) -> dict[str, object]`:
        - For each side in ["BUY", "SELL"]:
          - Chunk; build `[BookParams(token_id=t, side=side) for t in chunk]`
          - `async with self._limiter: result = await asyncio.to_thread(self._client.get_prices, params)`
          - Merge into accumulator (the SDK returns dict — merge is `acc.update(result)` if dict-of-token-id, else extend list — exact merge logic depends on T1-recorded shape)
        - Return `{"buy": <buy_acc>, "sell": <sell_acc>}`
      - DO NOT add retry decoration — py-clob-client has no async retry hook and `to_thread` blocks the worker. Let exceptions propagate; orchestrator will categorize. (If we discover frequent failures in Plan 5, add manual retry loop in T3 follow-up.)
      - DO NOT add wallet/key loading — read-only L0 endpoint per RESEARCH.md Pattern 2 line 327.

    Reference T1's recorded `clob_sample.json` to confirm:
    - Return shape of `get_prices` (whether to merge with `update` or build a manual dict)
    - The token id field name in book dict (used by Plan 3 validator, not here)

    If `get_prices` returns a list of dicts rather than a dict map, change accumulator from `dict.update` to `list.extend` and document in plan summary.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && python -c "from polyarb.clients.clob_client import ClobReaderClient; from polyarb.config import Settings; c = ClobReaderClient(Settings()); assert hasattr(c,'get_books'); assert hasattr(c,'get_prices_buy_sell'); print('OK')" && grep -q "asyncio.to_thread" src/polyarb/clients/clob_client.py && grep -q "from py_clob_client" src/polyarb/clients/clob_client.py && grep -q "from aiolimiter" src/polyarb/clients/clob_client.py && echo IMPORTS_OK</automated>
  </verify>
  <done>ClobReaderClient instantiable with Settings; get_books and get_prices_buy_sell methods exist; both wrap sync SDK calls in asyncio.to_thread; batching at clob_batch_size; aiolimiter applied per chunk; no wallet code</done>
</task>

<task type="auto">
  <id>T4</id>
  <name>Task 4: Unit tests for GammaClient (respx mock pagination + retry behavior)</name>
  <files>tests/m1-perception/test_gamma_client.py</files>
  <read_first>
    - src/polyarb/clients/gamma_client.py (T2 output)
    - tests/m1-perception/fixtures/gamma_sample.json (T1 output)
    - .planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md (Validation Architecture — `respx` for httpx)
  </read_first>
  <action>
    Create `tests/m1-perception/test_gamma_client.py` with these test functions (use `pytest.mark.asyncio` per `asyncio_mode=auto` from Plan 1):

    1. `test_fetch_all_paginates_until_short_page` — respx mocks 3 pages: page0=100 markets, page1=100 markets, page2=42 markets. Assert total = 242, assert 3 GET calls were made, assert each call's `offset` param incremented by 100.

    2. `test_fetch_all_single_page_terminates_immediately` — respx mocks 1 GET returning 5 markets (from gamma_sample.json). Assert total == 5, assert exactly 1 GET call.

    3. `test_retry_on_500_then_succeeds` — respx mocks: first 2 calls return 500, third returns valid list. Assert success after 3 attempts. Use `respx.route(...).side_effect = [Response(500), Response(500), Response(200, json=[...])]`.

    4. `test_retry_exhausts_then_raises` — respx mocks: all 3 calls return 500. Assert that calling `fetch_all_active_markets()` raises `httpx.HTTPStatusError` (or `tenacity.RetryError` depending on `reraise` setting).

    5. `test_no_retry_on_404` — respx mocks 404 on first call. Assert that the call raises `httpx.HTTPStatusError` after EXACTLY 1 attempt (not 3) — verifies the "don't retry 4xx" rule. Inspect call count.

    6. `test_aclose_closes_http_client` — Create client, call `await client.aclose()`, then assert `client._http.is_closed is True`.

    Each test must:
    - Use `respx.mock(base_url=settings.gamma_url)` decorator or fixture
    - Use `from polyarb.config import Settings; settings = Settings(retry_min_wait_s=0.001, retry_max_wait_s=0.01)` to keep tests fast (override retry waits to ~ms)
    - Use real fixture data from `tests/m1-perception/fixtures/gamma_sample.json` for the response body where realistic shape matters

    Top-level imports: `import httpx, json, pytest, respx; from pathlib import Path; from polyarb.clients.gamma_client import GammaClient; from polyarb.config import Settings`.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_gamma_client.py -xvs 2>&1 | tail -30</automated>
  </verify>
  <done>All 6 tests pass; pagination test verifies offset increments correctly; retry test verifies 3 attempts on 500; no-retry test verifies 1 attempt on 404; aclose test verifies cleanup</done>
</task>

<task type="auto">
  <id>T5</id>
  <name>Task 5: Unit tests for ClobReaderClient (mock sync SDK)</name>
  <files>tests/m1-perception/test_clob_client.py</files>
  <read_first>
    - src/polyarb/clients/clob_client.py (T3 output)
    - tests/m1-perception/fixtures/clob_sample.json (T1 output)
  </read_first>
  <action>
    Create `tests/m1-perception/test_clob_client.py`:

    py-clob-client is sync — mock with `unittest.mock.patch.object(client._client, 'get_order_books', return_value=[...])` since respx only mocks httpx.

    Tests:

    1. `test_get_books_single_chunk` — Settings(clob_batch_size=500). Pass 10 token IDs. Patch `client._client.get_order_books` to return the books list from fixtures/clob_sample.json. Call `await client.get_books(token_ids)`. Assert: returned list length == 2 (from fixture); patch called exactly 1 time; the BookParams list passed had length 10.

    2. `test_get_books_multiple_chunks` — Settings(clob_batch_size=3). Pass 7 token IDs. Patch return_value to a 1-element list each call. Assert patch called ceil(7/3)=3 times, with chunks of [3, 3, 1] BookParams sizes.

    3. `test_get_books_empty_token_ids` — Pass `[]`. Assert returned `[]`, patch called 0 times.

    4. `test_get_prices_buy_sell_uses_correct_side` — Patch `client._client.get_prices`. Call `await client.get_prices_buy_sell(["t1","t2"])`. Assert patch called 2 times (once for BUY, once for SELL); first call's BookParams list has all `side="BUY"`; second call has all `side="SELL"`.

    5. `test_propagates_sdk_exceptions` — Patch `client._client.get_order_books` to raise `RuntimeError("boom")`. Assert `await client.get_books(["t1"])` raises `RuntimeError`. (Confirms no exception swallowing.)

    Use `from unittest.mock import patch, MagicMock` and `from polyarb.clients.clob_client import ClobReaderClient`.

    To inspect BookParams that were passed: use `mock.call_args_list` and inspect `.args[0]` (the list of BookParams). Each BookParams has `.token_id` and `.side` attributes — assert via attribute access.
  </action>
  <verify>
    <automated>cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage && pytest tests/m1-perception/test_clob_client.py -xvs 2>&1 | tail -30</automated>
  </verify>
  <done>All 5 tests pass; chunking math verified via mock call counts; BUY/SELL side correctness verified via BookParams attribute inspection; exception propagation verified</done>
</task>

</tasks>

## Verification

```bash
pytest tests/m1-perception/test_gamma_client.py tests/m1-perception/test_clob_client.py -xvs
python -c "from polyarb.clients.gamma_client import GammaClient; from polyarb.clients.clob_client import ClobReaderClient; from polyarb.config import Settings; s = Settings(); GammaClient(s); ClobReaderClient(s); print('CLIENTS_OK')"
test -s tests/m1-perception/fixtures/gamma_sample.json
test -s tests/m1-perception/fixtures/clob_sample.json
```

## Success Criteria

- Both client classes import and instantiate cleanly with Settings
- All ≥11 client tests pass (6 Gamma + 5 CLOB)
- Real fixtures recorded (or stubbed with explicit summary note)
- Plan summary documents resolved Open Questions: CLOB book token-id field name, get_prices return shape

## must_haves (this plan delivers)

- Phase outcome 9 partial: clients/ subsystem
- Foundation for outcomes 1, 2, 6 (orchestrator and validator depend on these clients)

<output>
Create `.planning/workstreams/m1-perception/phases/01-/01-2-SUMMARY.md` documenting:
- The actual CLOB book dict's token-id field name (resolves RESEARCH.md Open Q#1)
- The actual `get_prices` return shape (dict vs list)
- Whether fixtures were recorded live or stubbed
- Any deviation from the planned retry semantics (e.g., if tenacity reraise behavior surprises)
- Concrete pin: which `py-clob-client` patch version got installed
</output>
