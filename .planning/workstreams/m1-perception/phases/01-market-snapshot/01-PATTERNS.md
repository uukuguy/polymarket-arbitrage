# Phase 1: 完整市场快照工具 — Pattern Map

**Mapped:** 2026-04-29
**Workstream:** m1-perception
**Phase:** 01 (snapshot tool)
**Files mapped:** 32 (project source + tests + scaffolding)
**Analogs source priority:**
1. `3th-party/polymarket-kalshi-weather-bot/` (verified-readable Python reference)
2. `01-RESEARCH.md` Code Examples (when no real analog exists)
3. Greenfield (when neither — explicit call-out)

> Greenfield context: this is the **first production code in the project**. There is no own-codebase analog for any file (only `Makefile`, `.planning/`, no `src/`). All "analogs" below are either reference-impl borrowings or RESEARCH.md sections to use as primary anchors.

---

## File Classification

### Plan 1 — Skeleton (Wave 1)

| File | Role | Data flow position | Closest analog | Match quality |
|---|---|---|---|---|
| `pyproject.toml` | scaffold (build config) | wiring | `3th-party/polymarket-kalshi-weather-bot/requirements.txt` (style only — they use `requirements.txt`, not pyproject) | weak — RESEARCH.md "Installation" + "Pitfall 7" is the primary anchor |
| `src/polyarb/__init__.py` | scaffold (package init) | wiring | `3th-party/polymarket-kalshi-weather-bot/backend/__init__.py` | role-match (empty / minimal init) |
| `src/polyarb/clients/__init__.py` | scaffold | wiring | `backend/data/__init__.py` (78B, exports nothing meaningful) | role-match |
| `src/polyarb/storage/__init__.py` | scaffold | wiring | none | greenfield |
| `src/polyarb/snapshot/__init__.py` | scaffold | wiring | `backend/core/__init__.py` | role-match |
| `src/polyarb/validator/__init__.py` | scaffold | wiring | none | greenfield — see RESEARCH.md Pattern 5 for `Issue` / `Category` exports |
| `config/snapshot.yaml` | config (declarative) | input boundary | none in reference (they use pydantic-settings env-only) | greenfield — RESEARCH.md "Standard Stack" + "User Constraints" define schema scope |
| `Makefile` (MODIFY) | scaffold (CLI entry) | wiring | project root `Makefile` (existing, has `help`/`status`/`journal` targets — see lines 17-39) | exact — extend in-place |

### Plan 2 — Clients (Wave 2)

| File | Role | Data flow position | Closest analog | Match quality |
|---|---|---|---|---|
| `src/polyarb/clients/gamma_client.py` | client (HTTP) | input boundary | `backend/data/btc_markets.py` (httpx Async + Gamma `/events` pagination); `backend/data/kalshi_markets.py:107-162` (cursor-paginated `while True` loop) | role-match — both use httpx async + Gamma; reference is **single-page**, ours needs full pagination loop. RESEARCH.md Pattern 1 supplies the missing 280-rps + retry decoration. |
| `src/polyarb/clients/clob_client.py` | client (sync SDK wrapped async) | input boundary | none in reference (reference impl never touches CLOB) | greenfield — RESEARCH.md Pattern 2 is primary anchor (asyncio.to_thread + BookParams batching) |
| `src/polyarb/clients/rate_limiter.py` (if extracted) | utility | wiring | none | greenfield — RESEARCH.md Standard Stack (`aiolimiter`) is the entire spec; see "Open Questions for Planner" |
| `tests/m1-perception/test_gamma_client.py` | test (HTTP unit) | output verification | none (reference has no tests) | greenfield — RESEARCH.md "Validation Architecture" → `respx` for httpx mock |
| `tests/m1-perception/test_clob_client.py` | test (SDK unit) | output verification | none | greenfield |

### Plan 3 — Storage + Validator (Wave 3)

| File | Role | Data flow position | Closest analog | Match quality |
|---|---|---|---|---|
| `src/polyarb/storage/sqlite_store.py` | storage (writer) | output boundary | `backend/models/database.py` (SQLAlchemy ORM — wrong pattern for our case; we use stdlib `sqlite3`) | partial — they use ORM; we don't. RESEARCH.md Pattern 3 is primary anchor (BEGIN IMMEDIATE + DELETE + executemany). |
| `src/polyarb/storage/parquet_writer.py` | storage (writer) | output boundary | none | greenfield — RESEARCH.md Pattern 4 is primary (pyarrow schema + tmp + os.replace atomic) |
| `src/polyarb/storage/schemas.py` | model (schema declarations) | pure data | `backend/models/database.py:20-146` (SQLAlchemy table classes — structural reference for column lists) | role-match — same intent (declare tables), different mechanism (DDL string + pyarrow.Schema vs ORM classes) |
| `src/polyarb/validator/layer1_count.py` | pure logic (validation) | pure logic | none | greenfield — RESEARCH.md Pattern 5 is primary |
| `src/polyarb/validator/layer2_fields.py` | pure logic (validation) | pure logic | none | greenfield — RESEARCH.md Pattern 5 is primary |
| `src/polyarb/validator/layer4_cross_source.py` | pure logic (validation) | pure logic | none | greenfield — RESEARCH.md Pattern 5 + Pitfall 1 (ghost book detection) |
| `src/polyarb/validator/category.py` | pure data (enum) | pure data | `backend/models/database.py:8` (`import enum`) | role-match — pattern: `class X(str, Enum)` |
| `tests/m1-perception/test_storage.py` | test (storage unit) | output verification | none | greenfield |
| `tests/m1-perception/test_validator.py` | test (logic unit) | output verification | none | greenfield |

### Plan 4 — Orchestrator + CLI (Wave 4)

| File | Role | Data flow position | Closest analog | Match quality |
|---|---|---|---|---|
| `src/polyarb/snapshot/orchestrator.py` | orchestrator (async coordinator) | wiring | `backend/core/scheduler.py:55-80` (job-style coordinator); `backend/data/btc_markets.py:169-218` (multi-step async fetch) | role-match — orchestration intent matches; structure differs (we're one-shot, they're scheduled). RESEARCH.md "Atomic SQLite + Parquet 编排" code example is primary anchor. |
| `src/polyarb/snapshot/cli.py` | controller (entry) | input boundary | `run.py` (uvicorn launcher — too thin to be useful) | weak — RESEARCH.md Standard Stack (typer) is the spec |
| `src/polyarb/snapshot/__main__.py` | scaffold (module entry) | wiring | none | greenfield — standard `from .cli import app; app()` 3-liner |
| `tests/m1-perception/test_orchestrator.py` | test (integration unit) | output verification | none | greenfield |

### Plan 5 — Tests + Integration verification (Wave 5)

| File | Role | Data flow position | Closest analog | Match quality |
|---|---|---|---|---|
| `tests/m1-perception/test_integration.py` | test (end-to-end) | output verification | none | greenfield — RESEARCH.md "Validation Architecture" → mock both APIs, run full orchestrator, assert SQLite rows + parquet readable |
| `tests/m1-perception/conftest.py` | test (fixtures) | wiring | none | greenfield — RESEARCH.md "Wave 0 Gaps" lists exact fixtures needed |
| `tests/m1-perception/fixtures/gamma_sample.json` | test (recorded data) | input | reference impl uses no recorded fixtures | greenfield — record once via real API call (RESEARCH.md Open Question #1 Wave 0 task) |
| `tests/m1-perception/fixtures/clob_sample.json` | test (recorded data) | input | none | greenfield — same as above |

---

## Pattern Assignments

### Plan 1 — Skeleton

#### `pyproject.toml`

**Analog:** none (reference uses `requirements.txt`)
**Primary reference:** RESEARCH.md "Installation" section + "Pitfall 7" (hatchling src layout config)

**Dependency-pinning shape to copy** from `3th-party/polymarket-kalshi-weather-bot/requirements.txt:1-34`:

```text
# Backend
pydantic==2.5.3
pydantic-settings==2.1.0

# Data fetching
httpx==0.26.0
aiohttp==3.9.1

# Logging
structlog>=24.1.0

# Utils
python-dotenv==1.0.0
orjson==3.9.10
```

→ Translate to pyproject.toml `[project.dependencies]` table form. **Pin major version, allow minor updates** (`^X.Y` semver in pyproject is fine; reference uses exact pins which is too tight for a research project).

**Critical addition** (from RESEARCH.md Pitfall 7):
```toml
[tool.hatch.build.targets.wheel]
packages = ["src/polyarb"]
```

#### `src/polyarb/__init__.py`

**Analog:** `3th-party/polymarket-kalshi-weather-bot/backend/__init__.py` (sub-100B, just package marker)

→ Same minimal pattern: empty file or `__version__ = "0.1.0"` only. **Do not export** anything from sub-modules at the package root (the reference doesn't, and lazy imports keep CLI startup fast).

#### `Makefile` (MODIFY)

**Analog:** project root `Makefile:1-49` (existing — has `help`/`status`/`journal` targets)

**Existing target structure to copy** (lines 17-23):
```makefile
## help: List all available commands with descriptions
help:
	@echo "Polymarket Arbitrage — Available commands"
	@echo ""
	@echo "Usage: make <target>"
	@echo ""
	@grep -E '^## ' $(MAKEFILE_LIST) | sed -E 's/^## /  /' | sort
```

The `## description:` comment-as-doc convention is already established. New targets must follow it (the `make help` parser depends on the `## name:` prefix).

**Existing placeholder for snapshot-markets** at lines 47-49 (commented-out example) should be replaced, not added next to:
```makefile
#   ## snapshot-markets: Capture full Polymarket market snapshot to parquet
#   snapshot-markets:
#       python -m polymarket.snapshot --output ...
```

→ Replace with real `python -m polyarb.snapshot` invocation per CONTEXT.md D-MK1/MK2.

---

### Plan 2 — Clients

#### `src/polyarb/clients/gamma_client.py`

**Primary analog:** `3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py:143-167` (httpx Async + Gamma)
**Pagination pattern analog:** `backend/data/kalshi_markets.py:104-162` (cursor-paginated `while True`)
**Primary reference:** RESEARCH.md Pattern 1 (rate-limit-aware version of below)

**Imports pattern** (from `btc_markets.py:1-13`):
```python
import httpx
import json
import logging
import re
import time
from datetime import datetime, timezone
from typing import Optional, List
from dataclasses import dataclass

logger = logging.getLogger("trading_bot")

GAMMA_API = "https://gamma-api.polymarket.com"
```

→ Adopt: `GAMMA_API` constant naming; module-level logger; `from datetime import ... timezone`. **Replace** `logging.getLogger(...)` with `loguru.logger` per project CLAUDE.md preference.

**Httpx async-with pattern** (from `btc_markets.py:152-156`):
```python
async with httpx.AsyncClient(timeout=10.0) as client:
    try:
        response = await client.get(url, params=params)
        response.raise_for_status()
        events = response.json()
```

→ **Don't copy verbatim**: reference uses per-call `async with` (creates fresh client every call → no connection pool). Our gamma_client must be **long-lived `httpx.AsyncClient`** (RESEARCH.md Pattern 1 lines 273-277) wrapped by class with explicit `aclose()`.

**Pagination loop pattern** (from `kalshi_markets.py:107-162`):
```python
cursor = None
while True:
    params = {"series_ticker": series, "status": "open", "limit": 200}
    if cursor:
        params["cursor"] = cursor
    data = await client.get_markets(params)
    raw_markets = data.get("markets", [])
    # ... process ...
    cursor = data.get("cursor")
    if not cursor or not raw_markets:
        break
```

→ Gamma uses **offset-based** pagination (not cursor — see RESEARCH.md Pattern 1 fetch_all_active_markets). Adapt loop shape, swap `cursor` → `offset += LIMIT`, terminate on `len(page) < LIMIT`.

**JSON-string field parsing** (from `btc_markets.py:99-109` — RESEARCH.md Pitfall 2 explicit reference):
```python
outcome_prices = market.get("outcomePrices", "")
up_price = 0.5
down_price = 0.5
if outcome_prices:
    try:
        prices = json.loads(outcome_prices) if isinstance(outcome_prices, str) else outcome_prices
        if isinstance(prices, list) and len(prices) >= 2:
            up_price = float(prices[0])
            down_price = float(prices[1])
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
```

→ **Move this into `normalizer.py`** (per RESEARCH.md "Recommended Project Structure"), not gamma_client. Gamma client returns raw dicts; normalizer does the JSON-string-to-list unwrapping. Apply same logic to `clobTokenIds`.

**Error handling pattern** (from `btc_markets.py:163-166`):
```python
except Exception as e:
    logger.debug(f"Failed to fetch BTC market {slug}: {e}")
    return None
```

→ **Don't copy**: reference swallows-and-returns-None which loses the failure for our `validation_issues` table. We need RESEARCH.md Pattern 1 tenacity decorator (3 retries, exponential 1s/2s/4s, only on `RequestError`/`HTTPStatusError`/`5xx`/`429`/`timeout` — see RESEARCH.md "Anti-Patterns to Avoid" #7).

#### `src/polyarb/clients/clob_client.py`

**Analog:** none in reference (reference impl has no CLOB code)
**Primary reference:** RESEARCH.md Pattern 2 (sync SDK + asyncio.to_thread + batched BookParams) **— this is the entire spec**

Quote: RESEARCH.md "Pattern 2: py-clob-client (sync) → async via `asyncio.to_thread`" (lines 310-351 of RESEARCH.md). The 30-line code block there is the implementation spec for this file.

**Critical defense** for issue #180 (RESEARCH.md Pitfall 1, lines 632-639): the client must expose **both** `get_books()` (size info) **and** `get_prices_buy_sell()` (price ground truth). Do NOT collapse into one method.

**Authentication note:** RESEARCH.md Pattern 2 line 327 — `ClobClient("https://clob.polymarket.com")` is **L0 read-only, no wallet/key required**. Verified via py-clob-client docs (RESEARCH.md Sources). Don't add wallet-loading boilerplate — the reference impl's Kalshi RSA-PSS pattern (`backend/data/kalshi_client.py:39-64`) is **NOT** applicable here.

#### `src/polyarb/clients/rate_limiter.py`

**Analog:** none
**Primary reference:** RESEARCH.md Standard Stack — `aiolimiter==1.2.1`

→ Most likely **no separate file needed**. `aiolimiter.AsyncLimiter` is one line per client. Only extract if planner finds shared retry/limit composition logic across both clients (see Open Questions).

#### `tests/m1-perception/test_gamma_client.py` / `test_clob_client.py`

**Analog:** none (reference has zero tests)
**Primary reference:** RESEARCH.md "Validation Architecture" → `pytest 8.2 + pytest-asyncio 0.23 + respx` for httpx mock; mock `client.get_order_books` directly via `unittest.mock.patch`.

**Decision-to-test map** from RESEARCH.md "Phase Requirements → Test Map":
- `test_retry_then_fail` (D-E1/E2): mock 3x failure → assert `api_unreachable` issue raised
- pagination loop terminates on partial page

---

### Plan 3 — Storage + Validator

#### `src/polyarb/storage/sqlite_store.py`

**Reference-impl analog:** `3th-party/polymarket-kalshi-weather-bot/backend/models/database.py` (SQLAlchemy ORM — **wrong abstraction for us**)
**Primary reference:** RESEARCH.md Pattern 3 (lines 360-462) — full DDL + writer pattern

**What NOT to copy from reference:**
- SQLAlchemy `Base = declarative_base()` (we use stdlib `sqlite3` per RESEARCH.md Standard Stack — no ORM)
- `engine = create_engine(...)` + `SessionLocal` factory pattern
- `init_db()` + `ensure_schema()` migration scaffolding (overkill for one-shot snapshot)

**What TO copy from reference (`database.py:1-2, 4-7, 12-17`):**
```python
"""Database models and connection for BTC 5-min trading bot."""
from datetime import datetime
from typing import Optional
```
→ Module docstring style; single-purpose imports.

**Primary spec (RESEARCH.md Pattern 3 lines 366-419):**
- DDL string with `PRAGMA journal_mode=WAL`, three CREATE TABLE statements
- Three tables: `snapshots`, `markets`, `validation_issues`
- Indexes: `idx_markets_liquidity`, `idx_markets_end_time`, `idx_issues_snapshot`, `idx_issues_category`

**Writer pattern (RESEARCH.md Pattern 3 lines 422-454):**
- `@contextmanager` for connection lifecycle
- `con = sqlite3.connect(db_path, isolation_level=None)` — explicit transaction control
- `BEGIN IMMEDIATE` + `DELETE FROM markets` + `executemany(...)` + `COMMIT` — **single transaction**
- Rollback on any exception, **don't swallow**

**Anti-pattern call-out** (RESEARCH.md "Anti-Patterns to Avoid" #1):
> `INSERT OR REPLACE` 而不先 DELETE → 会导致旧 snapshot 已删的市场仍残留在表里，**违反"覆盖式更新"语义**

This anti-pattern is the #1 risk if a junior implementer simplifies the writer.

#### `src/polyarb/storage/parquet_writer.py`

**Analog:** none in reference (reference does not use Parquet)
**Primary reference:** RESEARCH.md Pattern 4 (lines 467-510) — full pyarrow schema + atomic write

**Spec is the entire Pattern 4 code block.** Three load-bearing details:

1. **Explicit `pa.schema([...])`** — never `pa.Table.from_pylist(rows)` without a schema (RESEARCH.md Pitfall 3: token_id must be `pa.string()`, not inferred to int).
2. **`compression="snappy"`** — DuckDB-friendly, fast write (RESEARCH.md Pattern 4 lines 506-510).
3. **`tmp + os.replace` atomicity** (RESEARCH.md Pattern 4 lines 504-507):
   ```python
   tmp = out_path.with_suffix(out_path.suffix + ".tmp")
   pq.write_table(table, tmp, compression="snappy")
   os.replace(tmp, out_path)
   ```
   Failed write leaves only `.tmp`, which the orchestrator can `unlink()` on rollback.

#### `src/polyarb/storage/schemas.py`

**Reference structural analog:** `backend/models/database.py:20-146` — column list shape per table

**Pattern from reference** (`database.py:20-46` Trade table):
```python
class Trade(Base):
    """Simulated trades for tracking P&L."""
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    market_ticker = Column(String, index=True)
    platform = Column(String)
    direction = Column(String)
    entry_price = Column(Float)
    timestamp = Column(DateTime, default=datetime.utcnow)
    settled = Column(Boolean, default=False)
```

→ **Not the mechanism**: we don't use SQLAlchemy. **The shape**: column list with type + index + default — translates 1:1 to RESEARCH.md Pattern 3 DDL string + RESEARCH.md Pattern 4 pa.Schema.

**This file owns** (per RESEARCH.md "Recommended Project Structure" line 230):
- `DDL: str` (the SQLite CREATE statements from RESEARCH.md Pattern 3 lines 366-419)
- `SNAPSHOT_SCHEMA: pa.Schema` (the pyarrow schema from RESEARCH.md Pattern 4 lines 476-499)
- These two must stay **column-aligned** (same names, compatible types) — call this out as a comment in the file.

#### `src/polyarb/validator/category.py`

**Reference structural analog:** `backend/models/database.py:8`
```python
import enum
```
(Reference uses enum but not as `str, Enum` mixin.)

**Primary spec (RESEARCH.md Pattern 5 lines 525-532):**
```python
class Category(str, Enum):
    ZOMBIE_MARKET   = "zombie_market"
    RESOLVING       = "resolving"
    API_JITTER      = "api_jitter"
    API_UNREACHABLE = "api_unreachable"
    CLOB_MISSING    = "clob_missing"
    GHOST_BOOK      = "ghost_book"   # ⚠️ from RESEARCH.md Pitfall 1
    UNKNOWN         = "unknown"
```

The `(str, Enum)` mixin is critical — lets enum values serialize to SQLite TEXT directly.

#### `src/polyarb/validator/layer1_count.py` / `layer2_fields.py` / `layer4_cross_source.py`

**Analog:** none
**Primary reference:** RESEARCH.md Pattern 5 (lines 542-589) — three pure functions with `Issue` dataclass return

Each layer is **pure logic** — no IO, no side effects. Function signature:
```python
def layerN_xxx(...) -> list[Issue]: ...
```

**Critical Layer 4 detail (RESEARCH.md Pitfall 1, lines 580-589):**
```python
ba = book.get("asks", [{}])[0].get("price")
bb = book.get("bids", [{}])[0].get("price")
ref_buy = prices_by_token.get(tid, {}).get("buy")
if ba and bb and float(ba) > 0.98 and float(bb) < 0.02 and ref_buy:
    if abs(float(ref_buy) - float(ba)) > 0.05:
        out.append(Issue(4, Category.GHOST_BOOK, ...))
```

This is the **issue #180 defense** — load-bearing for D-D4 root-cause categorization.

---

### Plan 4 — Orchestrator + CLI

#### `src/polyarb/snapshot/orchestrator.py`

**Closest analog:** `3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py:169-218` (multi-step async fetch with two methods + dedup + sort)
**Secondary analog:** `backend/core/scheduler.py:55-80` (job-style coordinator — but we're one-shot, not scheduled)
**Primary reference:** RESEARCH.md "Atomic SQLite + Parquet 编排" (lines 720-770) **— this is the spec**

**Reference analog excerpt (`btc_markets.py:169-218`)** — multi-step async coordinator pattern:
```python
async def fetch_active_btc_markets() -> List[BtcMarket]:
    markets: List[BtcMarket] = []
    seen_slugs = set()

    # Method 1: Compute expected slugs and fetch directly
    expected_slugs = _compute_window_slugs(count=6)
    for slug in expected_slugs:
        market = await fetch_btc_market_by_slug(slug)
        if market and market.slug not in seen_slugs:
            seen_slugs.add(market.slug)
            markets.append(market)

    # Method 2: Search by series as fallback/supplement
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            ...
    except Exception as e:
        logger.debug(f"BTC series search fallback failed: {e}")
    ...
    logger.info(f"Fetched {len(markets)} active BTC 5-min markets")
    return markets
```

→ **Adopt:** sequential phases with explicit comments, dedup-via-set pattern, single logger.info summary at end.
→ **Don't adopt:** `try/except: log; pass` swallowing — orchestrator must surface errors to validator/exit code per RESEARCH.md D-E2 + D-D3.

**Primary spec (RESEARCH.md "Atomic SQLite + Parquet 编排" lines 724-770):**

Seven explicit steps with `# 1. ... # 2. ...` comments:
1. Gamma 全量 fetch + normalize
2. Subset/full mode → token list
3. CLOB batch fetch (books + prices)
4. Validation (Layer 1/2/4 → issues)
5. Parquet atomic write
6. SQLite single-transaction write
7. CLI one-liner output + exit code

**`fetched_at_ms` discipline (RESEARCH.md Pitfall 6, lines 670-680):** every market row must record CLOB-fetch-completion time; snapshot meta records `taken_at_ms`/`finished_at_ms`. Don't shortcut to a single timestamp.

**Critical (RESEARCH.md "Anti-Patterns to Avoid" #2):** no streaming write — **collect everything, validate, then write in one transaction**. Per D-E3 "不做部分成功补拉".

#### `src/polyarb/snapshot/cli.py`

**Analog:** `run.py` (uvicorn launcher, 20 lines, too thin to be useful as a structural reference)
**Primary reference:** RESEARCH.md Standard Stack — typer

The reference impl `run.py:1-20` shows only a uvicorn launch — not applicable (we're a CLI, not a web server). Use `typer.Typer()` per RESEARCH.md "Standard Stack" Supporting table.

**CLI shape from RESEARCH.md User Constraints D-F1/F2/F3:**
- Default: silent single-line summary (D-F1 line 111-113)
- `--full` flag (D-A2): full mode vs subset (default)
- `--verbose`: progress bar + phase timings
- `--config PATH`: optional YAML override
- Exit code: 0 if `is_valid=true`, 1 otherwise (D-F3)

#### `src/polyarb/snapshot/__main__.py`

**Analog:** none. Standard 3-liner:
```python
from .cli import app
if __name__ == "__main__":
    app()
```

This enables `python -m polyarb.snapshot` per CONTEXT.md D-MK2 example.

---

### Plan 5 — Tests + Integration

#### `tests/m1-perception/conftest.py`

**Analog:** none (reference has no tests)
**Primary reference:** RESEARCH.md "Wave 0 Gaps" (lines 902-907) — explicit fixture list

Required fixtures:
- `tmp_db_path` — temp SQLite file (use `tmp_path` pytest fixture)
- `tmp_parquet_dir` — temp parquet output dir
- `mock_gamma_client` — respx + sample JSON
- `mock_clob_client` — `unittest.mock.patch` on `py_clob_client.client.ClobClient.get_order_books`
- `frozen_time` (optional) — freezegun for `fetched_at_ms` determinism

#### `tests/m1-perception/fixtures/gamma_sample.json` / `clob_sample.json`

**Analog:** none
**Primary reference:** RESEARCH.md Open Question #1 (lines 817-820) — Wave 0 task is to **record real API responses once** and commit as fixtures. This is the **only** way to verify CLOB return-shape (`asset_id` vs `market` vs `token_id` field name).

→ Planner Wave 0 gate: don't write `clob_client.py` until one real `get_order_books` call has been made and printed; commit that response shape as `clob_sample.json`.

#### `tests/m1-perception/test_integration.py`

**Analog:** none
**Primary reference:** RESEARCH.md "Validation Architecture — Phase Requirements → Test Map" (lines 880-893)

End-to-end shape:
1. Mock both gamma + clob to return fixtures
2. Run orchestrator with `mode=subset`
3. Assert SQLite has 1 snapshot row, N market rows, expected `is_valid`
4. Assert parquet file exists, readable by pyarrow, row count matches
5. Assert `make snapshot-markets` smoke (`make -n` is enough — D-MK validation)

---

## Shared Patterns (cross-cutting, applied to all relevant plans)

### Async HTTP (httpx) — Gamma client

**Source:** `backend/data/btc_markets.py:152-156` + RESEARCH.md Pattern 1
**Apply to:** `gamma_client.py` only (CLOB is sync SDK)

```python
async with httpx.AsyncClient(timeout=10.0) as client:
    response = await client.get(url, params=params)
    response.raise_for_status()
    data = response.json()
```

→ **Modification per RESEARCH.md Pattern 1**: long-lived client (one per `GammaClient` instance, not per-call), `Limits(max_connections=20, max_keepalive_connections=10)`, explicit `aclose()`.

### Logging

**Source (don't copy — replace):** `backend/data/btc_markets.py:11`
```python
logger = logging.getLogger("trading_bot")
```

**Apply to all source files:** use `loguru` per project CLAUDE.md preference + RESEARCH.md Standard Stack:
```python
from loguru import logger
```

The reference uses stdlib logging because it's a 2024-era starter. New project standard is loguru per global CLAUDE.md and RESEARCH.md Standard Stack table.

### Error handling — fail loud, categorize, don't swallow

**Anti-pattern from reference** (`btc_markets.py:163-166`, `kalshi_markets.py:164-165`):
```python
except Exception as e:
    logger.debug(f"Failed: {e}")
    return None
```

**Don't copy.** Per CONTEXT.md D-D3 + D-E2, every failure is data — must end up in `validation_issues` with a `category`. Pattern:

```python
try:
    result = await fetch_with_retry(...)
except RetryError as e:
    issues.append(Issue(layer=N, category=Category.API_UNREACHABLE, ...))
    return None  # only after recording
```

**Apply to:** all Plan 2 client code, all Plan 4 orchestrator code.

### Type hints — Python 3.12 syntax

**Source:** project CLAUDE.md "Python ... type annotations for all functions (Python 3.12+ syntax: `list[str]`, `str | None`)"

**Reference uses (legacy 3.8 syntax)** — `backend/data/btc_markets.py:8`:
```python
from typing import Optional, List
def fn() -> Optional[BtcMarket]: ...
```

**Project standard (don't copy reference):**
```python
def fn() -> BtcMarket | None: ...
def fn() -> list[BtcMarket]: ...
```

**Apply to:** all new source files in `src/polyarb/`.

### Test naming convention

**Source:** project CLAUDE.md "Test files prefixed with `test_`" + "Test files in `tests/{branch}/`"

**Apply to:** all Plan 2/3/4/5 tests live under `tests/m1-perception/test_*.py`.

---

## Files With No Reference-Impl Analog

These files have **no analog** in the reference impl. Planner should treat the cited RESEARCH.md sections as the implementation spec:

| File | Reason no analog exists | Primary spec source |
|---|---|---|
| `src/polyarb/clients/clob_client.py` | Reference impl never calls CLOB API | RESEARCH.md Pattern 2 |
| `src/polyarb/storage/parquet_writer.py` | Reference impl uses SQLAlchemy/SQLite only, no Parquet | RESEARCH.md Pattern 4 |
| `src/polyarb/validator/*.py` | Reference impl has no validation layer (BTC bot is single-purpose) | RESEARCH.md Pattern 5 |
| `src/polyarb/snapshot/orchestrator.py` (atomic-write coordinator) | Reference impl is scheduled-job, not transactional snapshot | RESEARCH.md "Atomic SQLite + Parquet 编排" code block |
| `config/snapshot.yaml` | Reference impl uses pydantic-settings env-only, no YAML | RESEARCH.md User Constraints (cfg fields scope) + Standard Stack (`pydantic-settings` + `pyyaml`) |
| `tests/m1-perception/**` (all test files) | Reference impl has zero tests | RESEARCH.md "Validation Architecture" + "Wave 0 Gaps" |
| `tests/m1-perception/fixtures/*.json` | No real API recordings exist | RESEARCH.md Open Question #1 — record live in Wave 0 |
| `pyproject.toml` | Reference uses `requirements.txt` | RESEARCH.md "Installation" + Pitfall 7 |

---

## Open Questions for Planner

These are file-boundary ambiguities the planner must resolve before/during Plan 1-5 drafting:

1. **`rate_limiter.py`: extract or inline?**
   The file is listed conditionally ("if extracted"). `aiolimiter.AsyncLimiter(280, 10)` is a one-liner per client. **Recommendation:** do not create a separate file unless planner discovers shared retry-composition logic across gamma + clob. Inline `aiolimiter` instantiation in each client is more readable and easier to tune per-endpoint.

2. **Where does `normalizer.py` live?**
   RESEARCH.md "Recommended Project Structure" (line 224) places it in `snapshot/normalizer.py`. The file list above (`<file_list_to_map>`) does **not** include it explicitly. **Question:** is normalization a Plan 2 concern (clients return raw, normalize in Plan 4) or part of `gamma_client.py`? RESEARCH.md leans Plan 4 (snapshot/). Recommend planner explicitly add `src/polyarb/snapshot/normalizer.py` to Plan 4.

3. **Where does `config.py` (pydantic Settings + YAML loader) live?**
   RESEARCH.md "Recommended Project Structure" (line 218) places it at `src/polyarb/config.py` (top-level), not inside `snapshot/`. The `<file_list_to_map>` does **not** include this file. **Question:** is config loading deferred or expected to share scope with `cli.py`? Recommend planner add `src/polyarb/config.py` to Plan 1 or Plan 4.

4. **Schema split: one file or three?**
   `storage/schemas.py` is listed once but holds both SQLite DDL string and pyarrow `pa.Schema`. **Recommendation:** one file is fine for Phase 1 (~150 lines total). Split only if Phase 2 (WS) adds more tables.

5. **Layer 2 / Layer 4 `is_valid=false` threshold (RESEARCH.md Open Question #2 / Assumption A6)**
   Not a file-boundary question but blocks `validator/__init__.py` API design. RESEARCH.md recommends "no threshold" for Phase 1 — every issue logged but only Layer 1 mismatch flips `is_valid`. Planner should confirm and document this in Plan 3.

6. **Issue dataclass: where does it live?**
   RESEARCH.md "Recommended Project Structure" (line 235) places it in `validator/issues.py`. The `<file_list_to_map>` puts the Category enum in `validator/category.py` but doesn't list `issues.py`. **Recommendation:** keep the dataclass + the enum in **`validator/category.py`** (or rename to `validator/issues.py`) — they're tightly coupled, splitting is overkill.

7. **`__main__.py` entry vs `[project.scripts]`**
   Two ways to launch: `python -m polyarb.snapshot` (needs `__main__.py`) or `polyarb` console script (needs `pyproject.toml [project.scripts]`). CONTEXT.md Makefile examples show the former. **Recommendation:** support both — `__main__.py` for module invocation + `[project.scripts] polyarb = "polyarb.cli:app"` for ergonomics. Plan 1 owns the `[project.scripts]` entry.

---

## Metadata

- **Reference-impl files read:** 7
  - `3th-party/polymarket-kalshi-weather-bot/requirements.txt`
  - `3th-party/polymarket-kalshi-weather-bot/run.py`
  - `3th-party/polymarket-kalshi-weather-bot/backend/config.py`
  - `3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py` (full, 261 lines)
  - `3th-party/polymarket-kalshi-weather-bot/backend/data/markets.py` (full)
  - `3th-party/polymarket-kalshi-weather-bot/backend/data/kalshi_markets.py` (180 lines)
  - `3th-party/polymarket-kalshi-weather-bot/backend/data/kalshi_client.py` (80 lines)
  - `3th-party/polymarket-kalshi-weather-bot/backend/models/database.py` (full, 207 lines)
  - `3th-party/polymarket-kalshi-weather-bot/backend/core/scheduler.py` (head 80 lines)
- **clawfirm files checked:** 5 `.whip` files (orchestration only, no Polymarket touchpoints — caveat confirmed)
- **Project root files checked:** `Makefile` (49 lines, has existing patterns)
- **Reference impl test coverage:** 0 (zero test files exist) — all test patterns are greenfield from RESEARCH.md
- **Pattern extraction date:** 2026-04-29
