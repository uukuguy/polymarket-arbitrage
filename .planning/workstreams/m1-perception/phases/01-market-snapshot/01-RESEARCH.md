# Phase 1: 完整市场快照工具 - Research

**Researched:** 2026-04-29
**Domain:** Polymarket market data ingestion (Gamma + CLOB), local persistence (SQLite + Parquet), 异步并发 + 限流, 数据完整性归类
**Confidence:** HIGH for API mechanics & data layer; MEDIUM for V1-vs-V2 SDK choice (open bug); HIGH for storage / asyncio patterns

---

## Summary

Phase 1 是一个**离线数据采集器**：拉 Gamma 全量 markets 元数据 + CLOB 顶档行情，落 SQLite (热, 覆盖式) + Parquet (冷, 单文件 per snapshot)，附带分层校验（数量/字段完整/跨源一致）和根因归类（zombie/resolving/clob_missing/api_jitter/api_unreachable/unknown）。

技术栈是窄而稳的：**Python 3.12 + httpx (async) + py-clob-client v0.34.6 (sync, 包装进 thread executor) + aiolimiter + pyarrow + sqlite3 stdlib + tenacity (retry) + tqdm (进度) + pydantic (schema 校验) + click 或 typer (CLI) + pyyaml (config)**。

最大的实现风险不是性能或限流，而是 **CLOB v1 SDK 的 `/book` 端点已知返回幽灵数据（bid=0.01/ask=0.99）的 bug（GitHub issue #180, 2025-11-24, 至今 OPEN）**。规避方案：用 `get_order_books` 同时调用 `get_prices` 双源比对，价格冲突时以 `get_price` 为真，本 phase 把这个对齐做进 Layer 4 跨源一致性校验。

**Primary recommendation:** 用 V1 客户端 (`py-clob-client==0.34.6`) 作为本 phase 主路径（生态最成熟、读写都不需要 wallet），但模块化封装让 V2 可平替；CLOB 调用走 `get_order_books([BookParams(token_id)])` 批量 + `get_prices([(tid,"BUY"),(tid,"SELL")])` 校验，CLOB 不是瓶颈，**Gamma `/markets` 300 req/10s 才是限流瓶颈**。

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|---|---|---|---|
| Gamma API HTTP 调用 | clients/gamma_client | — | 单源访问层封装；分页+限流+重试 |
| CLOB API HTTP 调用 | clients/clob_client | — | py-clob-client 是同步库，需 `asyncio.to_thread` 包装；批量端点是关键 |
| Snapshot 编排 | snapshot/orchestrator | clients/* | 跨两个 client 的并发协调 + 时序记录 (`fetched_at_ms`) |
| 字段标准化（API → 内部 schema） | snapshot/normalizer | — | Gamma JSON string 字段 (`outcomePrices`, `clobTokenIds`) 解 JSON、token id 保留为 str |
| 校验（Layer 1/2/4） | validator/ | — | 输入是已标准化的 dict，输出是 (is_valid, [Issue]) |
| SQLite 热表写入 | storage/sqlite_writer | — | 一次 BEGIN IMMEDIATE 内 INSERT OR REPLACE 全量 markets + 写 snapshots / validation_issues |
| Parquet 冷归档写入 | storage/parquet_writer | — | 显式 schema + tmp + os.replace 原子化 |
| CLI 入口 | cli/snapshot_cmd | snapshot/orchestrator | 解析 `--full` `--verbose` `--config` 参数, 退出码语义化 |
| 配置加载 | config/loader | — | YAML → pydantic Settings, 提供默认值 |

**Why this matters:** 把 HTTP/IO/存储/校验/编排切干净，每个目录只服务一种关心点；后续 phase 接 WebSocket、异常检测时只换 orchestrator + validator，不动 clients/storage。

---

## User Constraints (from CONTEXT.md)

### Locked Decisions（research 必须遵守，不研究替代方案）

- **D-A1**：Gamma 全量 + CLOB 顶档（best_bid/ask 价 + 量），不取多档深度
- **D-A2**：双模式 — 默认 `liquidity_usd > $1000` 子集，`--full` 全量
- **D-A3**：本 phase 不接 Subgraph、Polygon RPC，但 schema 预留 `condition_id` 跨源 join
- **D-C1**：SQLite 放最新 snapshot（覆盖式），Parquet 是历史归档
- **D-C2**：单文件 per snapshot，路径 `data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet`
- **D-C3**：SQLite 主表 = `markets` / `snapshots` / `validation_issues`
- **D-D1**：实施 Layer 1 数量、Layer 2 字段完整、Layer 4 跨源一致
- **D-D2**：本 phase **不做** Layer 3 业务规则（YES+NO≠1）、历史漂移
- **D-D3**：严格模式 — 校验失败仍落库 (`is_valid=false`)，stderr 摘要 + 非零退出码
- **D-D4**：`validation_issues.category` 必填，已知类目 zombie_market / resolving / api_jitter / api_unreachable / clob_missing / unknown
- **D-B1**：手动触发，不引入 cron/timer
- **D-E1/E2/E3**：API 失败 3 次指数退避（1s/2s/4s）→ 标 `api_unreachable` 落库；**不做**部分成功补拉
- **D-F1/F2/F3**：默认静默单行总结，`--verbose` 进度条 + 分阶段耗时
- **D-MK1/MK2/MK3**：`make snapshot-markets` / `make snapshot-markets-full` 必须有

### Claude's Discretion（planner 可决定）

- 模块拆分细节（gamma_client / clob_client / storage / validator / orchestrator 的接口边界）
- 配置 YAML 具体 schema
- 限流实现（aiolimiter / 自实现 token bucket / 简单 semaphore + sleep）
- 进度条库（tqdm / rich.progress）
- 日志框架（loguru / structlog / stdlib logging）
- CLI 框架（click / typer / argparse）
- 测试 mock 策略（respx for httpx / 录制式 cassette / 直接 stub）

### Deferred Ideas (OUT OF SCOPE)

- CLOB 多档深度抓取 → m1-perception Phase 3 异常检测时按需二次拉取
- Subgraph / 链上 RPC → 后续独立 phase
- 历史漂移检测、Layer 3 业务规则 → Phase 3
- negrisk group 关系图 → 累积进 threads/market-microstructure.md
- 定时调度 / 守护进程 / 告警 → m5-industrialize
- Dashboard / TUI → m1-perception Phase 4

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---|---|---|---|
| `python` | 3.12+ | 解释器 | PROJECT.md 锁定 [VERIFIED: PROJECT.md] |
| `httpx[http2]` | 0.27+ | Gamma API 异步 HTTP 客户端 | 业内 async 默认；reference impl `polymarket-kalshi-weather-bot` 也用它（`backend/data/btc_markets.py`）[VERIFIED: 文件读取] |
| `py-clob-client` | 0.34.6 (2026-02-19) | Polymarket CLOB SDK | 官方 SDK；月下载 1.1M、1.2k stars；仍在主动维护（81 个 release）[CITED: github.com/Polymarket/py-clob-client] |
| `aiolimiter` | 1.2.1 | Async leaky bucket 限流 | 1 行包住 `async with` 即可；专门为 asyncio 设计 [CITED: aiolimiter.readthedocs.io] |
| `pyarrow` | 17.0+ | Parquet 写入 | DuckDB 同源、官方推荐，schema 显式控制能力强 [CITED: arrow.apache.org/docs/python/parquet.html] |
| `sqlite3` | stdlib | SQLite 客户端 | 不需要 ORM，覆盖式更新就是 INSERT OR REPLACE，stdlib 够用 [VERIFIED: docs.python.org] |
| `tenacity` | 8.4+ | 重试装饰器（指数退避） | 业内事实标准，`@retry(stop=stop_after_attempt(3), wait=wait_exponential(...))` 一行搞定 D-E1 [ASSUMED] |
| `pydantic` | 2.7+ | 配置 + API 响应字段标准化 | 项目栈未锁定 ORM，但 reference impl 也用 pydantic 2.5；Polymarket/agents `gamma.py` 也用 pydantic 解析 markets [CITED: github.com/Polymarket/agents] |
| `pyyaml` | 6.0+ | 配置加载 | PROJECT.md 锁定 YAML 配置 [VERIFIED: PROJECT.md] |

### Supporting

| Library | Version | Purpose | When to Use |
|---|---|---|---|
| `typer` | 0.12+ | CLI | 比 argparse 易写、自带 help、支持 `--full` 标志 [ASSUMED — discretion 项, planner 选定] |
| `tqdm` | 4.66+ | 进度条 | `--verbose` 模式分阶段进度 [ASSUMED] |
| `loguru` | 0.7+ | 日志 | 用户全局偏好（CLAUDE.md "use loguru instead of standard logging"），讨论留在 plan 阶段 [VERIFIED: ~/.claude/CLAUDE.md] |
| `pytest` | 8.2+ | 测试框架 | 配合 `pytest-asyncio` 测异步代码 [VERIFIED: ~/.claude/CLAUDE.md preferred command] |
| `respx` | 0.21+ | httpx mock | mock Gamma 响应不打真实网络，CI 不需 secrets [ASSUMED] |
| `freezegun` | 1.5+ | 冻结时间测 fetched_at_ms | 可选，验 snapshot 的时间戳记录正确性 [ASSUMED] |
| `duckdb` | 1.0+ | （只为本 phase 验收 + Phase 3 准备） | `make verify-snapshot` 这种回头查 parquet 的命令好用 [ASSUMED — 是否本 phase 引入由 planner 决定] |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|---|---|---|
| `py-clob-client` v1 | `py-clob-client-v2` (v1.0.0, 2026-04-17) | V2 是新版，仍 early-stage（4 open issues、5 个 release），但 issue #180 还未确认在 V2 修复；用 V1 是稳妥默认 |
| `httpx` | `aiohttp` | reference impl 两个都用了；httpx 接口跟 requests 最像、async 模型直接，团队学习曲线短 |
| `aiolimiter` | 自写 token bucket / 纯 semaphore | aiolimiter 是 leaky bucket（更接近 Cloudflare 的实际算法），semaphore 只控并发不控 RPS。Polymarket 是限 RPS 不限并发 |
| `pyarrow` | `polars` / `fastparquet` | polars 写 parquet 也快，但 schema 控制不如 pyarrow 直接；fastparquet 对 uint 类型有兼容问题（apache/arrow #256） |

### Installation

```bash
# 推荐 pyproject.toml 的 [project.dependencies]
httpx = "^0.27"
py-clob-client = "^0.34.6"
aiolimiter = "^1.2"
pyarrow = "^17.0"
tenacity = "^8.4"
pydantic = "^2.7"
pydantic-settings = "^2.4"
pyyaml = "^6.0"
typer = "^0.12"
tqdm = "^4.66"
loguru = "^0.7"

# [project.optional-dependencies.dev]
pytest = "^8.2"
pytest-asyncio = "^0.23"
respx = "^0.21"
duckdb = "^1.0"
```

**Version verification (2026-04-29):**

- `py-clob-client`: latest **0.34.6** released 2026-02-19 [CITED: github.com/Polymarket/py-clob-client - "v0.34.6"]
- `py-clob-client-v2`: latest **1.0.0** released 2026-04-17 [CITED: github.com/Polymarket/py-clob-client-v2]
- `aiolimiter`: latest **1.2.1** [CITED: aiolimiter.readthedocs.io]
- `httpx`: 0.27.x is current stable [ASSUMED — pip install will resolve]
- `pyarrow`: 17.0+ is current generation as of mid-2026 [ASSUMED]

⚠️ Plan 阶段第一件事是 `pip index versions <pkg>` 跑一遍把这些钉死成 lockfile。

---

## Architecture Patterns

### System Architecture Diagram

```
                         ┌──────────────┐
        CLI (typer)  ──> │ orchestrator │ (asyncio main, 时序记录)
                         └──────┬───────┘
                                │
            ┌───────────────────┼─────────────────────┐
            ▼                   ▼                     ▼
   ┌─────────────────┐ ┌─────────────────┐  ┌─────────────────┐
   │ gamma_client    │ │ clob_client     │  │ config (YAML)   │
   │ httpx Async     │ │ py-clob-client  │  │ pydantic        │
   │ paginate /      │ │ wrap sync→async │  │ settings        │
   │ markets         │ │ via to_thread   │  └─────────────────┘
   │ aiolimiter:     │ │ aiolimiter:     │
   │   300/10s       │ │   500/10s batch │
   │ tenacity retry  │ │ tenacity retry  │
   └────────┬────────┘ └────────┬────────┘
            │                   │
            ▼                   ▼
       Gamma JSON          batch [BookParams]
       (12k+ markets)      → 顶档 bids/asks (×500/req)
            │                   │
            └─────────┬─────────┘
                      ▼
            ┌──────────────────┐
            │ normalizer       │  解 JSON 字符串字段
            │                  │  (outcomePrices, clobTokenIds)
            │                  │  规范 token_id (str)
            └────────┬─────────┘
                     ▼
            ┌──────────────────┐
            │ validator        │  Layer 1: 数量
            │ (3 layers)       │  Layer 2: 字段
            │                  │  Layer 4: 跨源
            │                  │  → [(category, market_id, raw)]
            └────────┬─────────┘
                     ▼
        ┌────────────┴─────────────┐
        ▼                          ▼
  ┌──────────────┐         ┌──────────────────┐
  │ sqlite_writer│         │ parquet_writer   │
  │ BEGIN IMMED. │         │ tmp + os.replace │
  │ INSERT OR    │         │ explicit schema  │
  │ REPLACE      │         │ snappy           │
  │ markets/     │         │                  │
  │ snapshots/   │         │ data/snapshots/  │
  │ issues       │         │ YYYY/MM/DD/      │
  │              │         │ HH-MM-SS.parquet │
  └──────────────┘         └──────────────────┘
```

数据流向：**单向**。orchestrator 是唯一调度方；clients 不知道下游存在；validator 不知道存储；writers 不知道 API。

### Recommended Project Structure

```
polymarket-arbitrage/
├── pyproject.toml              # hatchling 后端, src layout, [project.scripts] polyarb=polyarb.cli:app
├── Makefile                    # +snapshot-markets / snapshot-markets-full
├── config/
│   └── snapshot.yaml           # liquidity_threshold / api urls / rate / retry / output_dir
├── src/
│   └── polyarb/
│       ├── __init__.py
│       ├── cli.py              # typer app, snapshot_cmd
│       ├── config.py           # pydantic Settings + YAML loader
│       ├── snapshot/
│       │   ├── __init__.py
│       │   ├── orchestrator.py # async main, 时序记录, fetched_at_ms
│       │   └── normalizer.py   # Gamma raw → 内部 dict (解 JSON 字符串字段)
│       ├── clients/
│       │   ├── __init__.py
│       │   ├── gamma_client.py # httpx AsyncClient + aiolimiter + tenacity
│       │   └── clob_client.py  # py-clob-client + asyncio.to_thread + aiolimiter
│       ├── storage/
│       │   ├── __init__.py
│       │   ├── schema.py       # pyarrow.Schema + SQLite DDL
│       │   ├── sqlite_writer.py# BEGIN IMMEDIATE + INSERT OR REPLACE
│       │   └── parquet_writer.py# tmp + os.replace
│       └── validator/
│           ├── __init__.py
│           ├── issues.py       # ValidationIssue dataclass + Category enum
│           └── layers.py       # layer1_count / layer2_fields / layer4_cross_source
├── tests/
│   └── m1-perception/
│       ├── conftest.py
│       ├── fixtures/           # 录制的 Gamma / CLOB 响应样本
│       ├── test_gamma_client.py# respx mock
│       ├── test_clob_client.py # mock py-clob-client.get_order_books
│       ├── test_normalizer.py  # 字符串字段解析、token_id str 保持
│       ├── test_validator.py   # 三层各自的归类
│       ├── test_sqlite_writer.py
│       ├── test_parquet_writer.py
│       └── test_orchestrator.py# 端到端 mock 跑通
└── data/
    ├── polyarb.db              # SQLite (gitignore)
    └── snapshots/              # Parquet 树（gitignore）
```

⚠️ `src/` layout 选择：CLAUDE.md global 偏好 src 布局，避免 import 时 picking up 当前 cwd 的同名目录。`hatchling` 是当前推荐 build backend [CITED: packaging.python.org]。

### Pattern 1: httpx AsyncClient + aiolimiter for Gamma pagination

**What:** Gamma `/markets` 限流 300/10s（单端点），分页 limit/offset 格式。
**When to use:** 抓全量元数据 12k+ 时分页循环，每次 limit=100 → 120 个请求。

```python
# Source: 综合 https://www.python-httpx.org/advanced/resource-limits/
#         + https://aiolimiter.readthedocs.io/
#         + reference impl polymarket-kalshi-weather-bot/backend/data/btc_markets.py
import httpx
from aiolimiter import AsyncLimiter
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

GAMMA = "https://gamma-api.polymarket.com"

class GammaClient:
    def __init__(self):
        self._limiter = AsyncLimiter(280, 10)  # 留 ~7% 安全余量
        self._http = httpx.AsyncClient(
            timeout=15.0,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "polyarb/0.1"},
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=4),
        retry=retry_if_exception_type((httpx.RequestError, httpx.HTTPStatusError)),
    )
    async def _get(self, path, params):
        async with self._limiter:
            r = await self._http.get(f"{GAMMA}{path}", params=params)
            r.raise_for_status()
            return r.json()

    async def fetch_all_active_markets(self):
        page, offset, LIMIT = [], 0, 100
        out = []
        while True:
            page = await self._get("/markets", {
                "active": "true", "closed": "false", "archived": "false",
                "limit": LIMIT, "offset": offset,
            })
            out.extend(page)
            if len(page) < LIMIT:
                break
            offset += LIMIT
        return out

    async def aclose(self):
        await self._http.aclose()
```

`aiolimiter(280, 10)` 是 280 req per 10s — 留 7% 余量，因为 Polymarket 用 Cloudflare 限流是 throttle（排队加延迟）非 reject，超了不会立即 429 但会变慢，留余量更准确。[CITED: docs.polymarket.com/quickstart/introduction/rate-limits — "throttled (delayed/queued) rather than immediately rejected"]

### Pattern 2: py-clob-client (sync) → async via `asyncio.to_thread`

**What:** py-clob-client 是同步库（V1 v0.34.6 [VERIFIED: agentbets reference]）。要在 asyncio 中调要包一层。
**When to use:** orchestrator 需要并发拉 N 个 batch 的 order books。

```python
# Source: https://context7.com/polymarket/py-clob-client/llms.txt
# https://docs.python.org/3/library/asyncio-task.html#asyncio.to_thread
import asyncio
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import BookParams
from aiolimiter import AsyncLimiter

class ClobReaderClient:
    BATCH_SIZE = 500  # CLOB get_order_books 上限 [CITED: agentbets py-clob-client guide]

    def __init__(self):
        self._client = ClobClient("https://clob.polymarket.com")  # L0 read-only, 无需 wallet
        # CLOB batch 端点限流 500/10s [CITED: agentbets polymarket-rate-limits-guide]
        self._batch_limiter = AsyncLimiter(450, 10)

    async def get_books(self, token_ids: list[str]) -> list[dict]:
        out = []
        for i in range(0, len(token_ids), self.BATCH_SIZE):
            chunk = token_ids[i:i + self.BATCH_SIZE]
            params = [BookParams(token_id=t) for t in chunk]
            async with self._batch_limiter:
                books = await asyncio.to_thread(self._client.get_order_books, params)
            out.extend(books)
        return out

    async def get_prices_buy_sell(self, token_ids: list[str]) -> dict[str, dict]:
        # 用作对 /book 幽灵价格 (issue #180) 的对照
        params_buy  = [BookParams(token_id=t, side="BUY")  for t in token_ids]
        params_sell = [BookParams(token_id=t, side="SELL") for t in token_ids]
        async with self._batch_limiter:
            buy  = await asyncio.to_thread(self._client.get_prices, params_buy)
        async with self._batch_limiter:
            sell = await asyncio.to_thread(self._client.get_prices, params_sell)
        # 返回结构由 SDK 决定，plan 阶段需手测 1 次确认形状
        return {"buy": buy, "sell": sell}
```

**Why batch:** 单 token `get_order_book` 是 1500 req/10s，**batch `/books` 是 500 req/10s 但每个请求带最多 500 token** → 250k token/10s 上限，远高于 12k market × 2 token = 24k token 一次的需求。Gamma 才是瓶颈。[CITED: agentbets polymarket-rate-limits-guide]

### Pattern 3: SQLite 覆盖式更新（用一个 BEGIN IMMEDIATE 包住）

**What:** snapshot 完整一次 = 一次写事务，从 reader 视角看是原子的（旧数据 → 新数据 切换瞬间）。
**When to use:** D-C1 — markets 表只保留最新 snapshot。

```python
# Source: https://docs.python.org/3/library/sqlite3.html
# https://www3.sqlite.org/cgi/forum/info/f116e2e40067cae58a79fd36d4df73c91b707d29bb9455bfa865bb1dd0085999
import sqlite3
from contextlib import contextmanager

DDL = """
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS snapshots (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  taken_at_ms     INTEGER NOT NULL,                  -- 起拍时刻 (UTC epoch ms)
  finished_at_ms  INTEGER NOT NULL,                  -- 落库完成时刻
  mode            TEXT NOT NULL CHECK (mode IN ('subset','full')),
  market_count    INTEGER NOT NULL,
  is_valid        INTEGER NOT NULL,                  -- 0/1
  parquet_path    TEXT NOT NULL,
  notes           TEXT
);

CREATE TABLE IF NOT EXISTS markets (
  market_id        TEXT PRIMARY KEY,                  -- gamma id
  condition_id     TEXT NOT NULL,
  slug             TEXT,
  question         TEXT,
  yes_token_id     TEXT,
  no_token_id      TEXT,
  mid_price        REAL,                              -- gamma reported, 仅展示
  liquidity_usd    REAL,
  volume_usd       REAL,
  best_bid_price   REAL,                              -- CLOB
  best_bid_size    REAL,
  best_ask_price   REAL,
  best_ask_size    REAL,
  end_time_ms      INTEGER,
  active           INTEGER,
  closed           INTEGER,
  neg_risk         INTEGER,
  neg_risk_market_id TEXT,
  fetched_at_ms    INTEGER NOT NULL,                  -- 单 row 抓取时刻 (CLOB 与 Gamma 时间不同)
  snapshot_id      INTEGER NOT NULL REFERENCES snapshots(id),
  incomplete       INTEGER NOT NULL DEFAULT 0         -- Layer 2 字段缺失标志
);

CREATE INDEX IF NOT EXISTS idx_markets_liquidity ON markets(liquidity_usd);
CREATE INDEX IF NOT EXISTS idx_markets_end_time   ON markets(end_time_ms);

CREATE TABLE IF NOT EXISTS validation_issues (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  snapshot_id     INTEGER NOT NULL REFERENCES snapshots(id),
  layer           INTEGER NOT NULL,                  -- 1, 2, 4
  category        TEXT NOT NULL,                     -- zombie_market | resolving | api_jitter | ...
  market_id       TEXT,                              -- nullable (Layer 1 不属于具体 market)
  detail          TEXT,                              -- 文字描述
  raw_payload     TEXT                               -- JSON 缺失/异常的原始片段，便于事后查
);
CREATE INDEX IF NOT EXISTS idx_issues_snapshot ON validation_issues(snapshot_id);
CREATE INDEX IF NOT EXISTS idx_issues_category ON validation_issues(category);
"""

@contextmanager
def open_writer(db_path):
    con = sqlite3.connect(db_path, isolation_level=None)  # 显式管事务
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA synchronous=NORMAL")
    try:
        yield con
    finally:
        con.close()

def write_snapshot(db_path, snapshot_meta, markets, issues):
    with open_writer(db_path) as con:
        con.executescript(DDL)
        con.execute("BEGIN IMMEDIATE")  # 立即拿写锁，让 reader 还能读旧数据
        try:
            con.execute("DELETE FROM markets")  # 先清空（覆盖式）
            cur = con.execute(
                "INSERT INTO snapshots(taken_at_ms,finished_at_ms,mode,market_count,is_valid,parquet_path,notes) VALUES (?,?,?,?,?,?,?)",
                snapshot_meta,
            )
            snapshot_id = cur.lastrowid
            con.executemany(
                "INSERT INTO markets(market_id,condition_id,slug,question,yes_token_id,no_token_id,mid_price,liquidity_usd,volume_usd,best_bid_price,best_bid_size,best_ask_price,best_ask_size,end_time_ms,active,closed,neg_risk,neg_risk_market_id,fetched_at_ms,snapshot_id,incomplete) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(m + (snapshot_id,)) for m in markets],
            )
            con.executemany(
                "INSERT INTO validation_issues(snapshot_id,layer,category,market_id,detail,raw_payload) VALUES (?,?,?,?,?,?)",
                [((snapshot_id,) + i) for i in issues],
            )
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
```

**关键点：**
- `WAL` 模式 → 写不阻塞读，下一 phase（异常检测、dashboard）实时读 markets 不会被本工具卡 [CITED: sqlite.org]
- `BEGIN IMMEDIATE` → 立即拿写锁，避免两个 snapshot 工具同时跑导致 "database is locked" [CITED: sqlite.org/forum/forumpost/04ed1d235b]
- `executemany` 用一次而非循环 → 10k 行从 30+s 降到 100ms 级 [CITED: sqlprostudio.com benchmark]
- `DELETE FROM markets` 而非 `INSERT OR REPLACE`：覆盖式语义最干净，全量替换不要保留任何旧 row（如果新 snapshot 不再有某 market，旧的 `INSERT OR REPLACE` 会留它，是 bug）

### Pattern 4: Parquet 写入（显式 schema + 临时文件 + 原子 rename）

**What:** Parquet 是冷归档，schema 必须稳定（DuckDB 跨文件 scan 假定列一致）。
**When to use:** 每次 snapshot 完成后写一份历史归档。

```python
# Source: https://arrow.apache.org/docs/python/parquet.html
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import os, tempfile

# 显式 schema 比 pa.Table.from_pylist 推断稳得多
SNAPSHOT_SCHEMA = pa.schema([
    pa.field("market_id",          pa.string()),
    pa.field("condition_id",       pa.string()),
    pa.field("slug",               pa.string(), nullable=True),
    pa.field("question",           pa.string(), nullable=True),
    pa.field("yes_token_id",       pa.string()),    # ⚠️ uint256 → 必须 string
    pa.field("no_token_id",        pa.string()),
    pa.field("mid_price",          pa.float64()),
    pa.field("liquidity_usd",      pa.float64()),
    pa.field("volume_usd",         pa.float64()),
    pa.field("best_bid_price",     pa.float64(), nullable=True),
    pa.field("best_bid_size",      pa.float64(), nullable=True),
    pa.field("best_ask_price",     pa.float64(), nullable=True),
    pa.field("best_ask_size",      pa.float64(), nullable=True),
    pa.field("end_time_ms",        pa.int64(),   nullable=True),
    pa.field("active",             pa.bool_()),
    pa.field("closed",             pa.bool_()),
    pa.field("neg_risk",           pa.bool_()),
    pa.field("neg_risk_market_id", pa.string(),  nullable=True),
    pa.field("fetched_at_ms",      pa.int64()),
    pa.field("snapshot_taken_at_ms", pa.int64()),
    pa.field("snapshot_id",        pa.int64()),
    pa.field("incomplete",         pa.bool_()),
])

def write_parquet_atomic(rows: list[dict], out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)
    # Atomic write: tmp 同目录 → os.replace
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    pq.write_table(table, tmp, compression="snappy")  # snappy = DuckDB 默认友好
    os.replace(tmp, out_path)  # POSIX & Windows 都原子
```

**为什么 snappy：** ZSTD 压缩比 SNAPPY 多 15-20% 体积优势但读时差 < 1%、写时显著慢；本 phase 单文件大小预估 < 100 MB，压缩比不是首要问题，**写延迟 + 兼容性更重要**。SNAPPY 是 PyArrow 默认、DuckDB 默认支持，最稳。[CITED: dev.to/ldsands/snappy-vs-zstd-for-parquet-in-pyarrow]

**`os.replace` 跨平台：** Python docs 明确 `os.replace` 在 POSIX 和 Windows 都是原子文件替换 [CITED: docs.python.org/3/library/os.html#os.replace]。

### Pattern 5: 校验三层独立运行 + 归类

**What:** D-D1 + D-D4。每层独立失败、每个 issue 必有 category。
**When to use:** 在落库**前**跑，让 issues 进 SQLite + 决定 `is_valid`。

```python
# Source: 综合 CONTEXT.md + threads/data-quality.md
from dataclasses import dataclass
from enum import Enum
from typing import Optional

class Category(str, Enum):
    ZOMBIE_MARKET   = "zombie_market"     # liquidity ~0
    RESOLVING       = "resolving"          # end_time 临近
    API_JITTER      = "api_jitter"         # API 报告数值前后不一致
    API_UNREACHABLE = "api_unreachable"   # 多次重试仍 fail
    CLOB_MISSING    = "clob_missing"       # Gamma 有但 CLOB 拉不到簿
    GHOST_BOOK      = "ghost_book"         # ⚠️ 新增: bid=0.01/ask=0.99 但 price 端点正常 (issue #180)
    UNKNOWN         = "unknown"            # 持续 unknown = 系统欠债

@dataclass
class Issue:
    layer: int                # 1/2/4
    category: Category
    market_id: Optional[str]
    detail: str
    raw_payload: Optional[str]  # JSON 字符串

# Layer 1: Gamma 报告 active count vs 实际拉到的数。规则: 严格相等
def layer1_count(reported_total: int, fetched_count: int) -> list[Issue]:
    if reported_total != fetched_count:
        return [Issue(1, Category.API_JITTER, None,
                     f"Gamma reported {reported_total} active markets, fetched {fetched_count}",
                     None)]
    return []

# Layer 2: 每个 market 必有核心字段
REQUIRED = ("market_id","condition_id","yes_token_id","no_token_id",
            "mid_price","liquidity_usd","end_time_ms")

def layer2_fields(markets: list[dict]) -> list[Issue]:
    out = []
    for m in markets:
        missing = [k for k in REQUIRED if m.get(k) in (None, "", [])]
        if missing:
            cat = Category.RESOLVING if (m.get("end_time_ms") and within_24h(m["end_time_ms"])) \
                  else Category.ZOMBIE_MARKET if (m.get("liquidity_usd",0) < 10) \
                  else Category.UNKNOWN
            out.append(Issue(2, cat, m.get("market_id"),
                            f"missing: {missing}",
                            json.dumps({k: m.get(k) for k in REQUIRED}, default=str)))
            m["incomplete"] = True   # 不丢弃, 标记
    return out

# Layer 4: Gamma 有 CLOB 没有 + 价格幽灵识别
def layer4_cross(markets: list[dict], books_by_token: dict, prices_by_token: dict) -> list[Issue]:
    out = []
    for m in markets:
        for token_field in ("yes_token_id", "no_token_id"):
            tid = m.get(token_field)
            if not tid: continue
            book = books_by_token.get(tid)
            if book is None:
                out.append(Issue(4, Category.CLOB_MISSING, m["market_id"],
                                f"CLOB has no book for {token_field}={tid}", None))
                continue
            # ⚠️ Issue #180 防御: 顶档报 0.01/0.99 而 /price 不一致 → 幽灵簿
            ba = book.get("asks", [{}])[0].get("price")
            bb = book.get("bids", [{}])[0].get("price")
            ref_buy = prices_by_token.get(tid, {}).get("buy")
            if ba and bb and float(ba) > 0.98 and float(bb) < 0.02 and ref_buy:
                if abs(float(ref_buy) - float(ba)) > 0.05:  # /price 显著不同
                    out.append(Issue(4, Category.GHOST_BOOK, m["market_id"],
                                    f"book reports bid={bb}/ask={ba} but /price reports {ref_buy}",
                                    None))
    return out

def is_valid_overall(issues: list[Issue]) -> bool:
    # Layer 1 任何不一致 → 整体失败 (D-D1: 严格相等)
    if any(i.layer == 1 for i in issues):
        return False
    # Layer 2 / Layer 4 进 issues 表但不一定让整体失败 (除非超阈值, X 在 plan 阶段决定)
    return True
```

**Why:** Issue 是数据的，is_valid 是布尔的。两个解耦让"能落但需要警惕"和"不能落"清晰分离。Layer 1 偏严（数量必须严格相等，符合 D-D1 "必须严格相等"），Layer 2/4 偏宽（标记不丢）。

### Anti-Patterns to Avoid

- **`INSERT OR REPLACE` 而不先 DELETE**：会导致旧 snapshot 已删的市场仍残留在表里，**违反"覆盖式更新"语义**。
- **Snapshot 边采集边写入存储**：与 D-E3 "不做部分成功补拉" 相违；应该全部采集完 + 校验完 + 一次事务写入。
- **CLOB 用 `get_order_book` 单点循环 12k+**：浪费 24x request 配额（500 batch + 500 tokens/batch 是设计意图），还更容易撞 issue #180。
- **把 `outcomePrices`、`clobTokenIds` 当 list 直接读**：Gamma 返的是 **JSON 字符串**（reference impl `btc_markets.py:103-109` 明确 `json.loads`），不解析直接当 list 是错的 [VERIFIED: 文件读取]。
- **token_id 存 INTEGER**：uint256 256-bit 远超 SQLite/Parquet INTEGER 64-bit 范围，必须 TEXT/string [VERIFIED: 由市场样本 token_id 长度推断 — 70-75 位十进制]。
- **time fields 存 ISO string**：`endDate` 是 ISO string 但**应转为 epoch ms 存**（DuckDB 跨文件聚合时数值更友好）。
- **重试包到 HTTP 500 + 4xx**：4xx (除 429) 通常是请求结构问题，重试只浪费配额，应只在 RequestError + 5xx + 429 + timeout 时重试。

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| 异步限流 | semaphore + asyncio.sleep | `aiolimiter.AsyncLimiter` | leaky bucket 对应 Cloudflare throttle 行为；自己写要处理跨任务时间窗、reentry、cancel |
| 重试指数退避 | 自写 try/except + sleep | `tenacity` 装饰器 | 边界条件多（jitter、retry-on-exception、abort 条件） |
| Polymarket CLOB SDK | 手写 HMAC + 签名 + endpoint URL | `py-clob-client` | 即使读模式也已经包好 endpoint + 反序列化 |
| Parquet 写 | 手写文件 IO | `pyarrow.parquet.write_table` | schema 验证、压缩、列式编码不能自做 |
| HTTP 客户端 | requests + threading | `httpx.AsyncClient` | async + connection pool + http2 + timeout 一体 |
| 时间格式化 | 手写 ISO 解析 | `datetime.fromisoformat()` + epoch ms 转换 | reference impl 也踩过坑 ('Z' vs '+00:00')，stdlib 够 |
| CLI 解析 | argparse 自定义 | `typer` 或 `click` | flag 默认值、help、exit code 一体 |

**Key insight：** 这是 IO 重 + 数据重 + 校验重的工具，"少写代码" 是首要美德。Python 生态对每个层都有事实标准库，自实现只会埋将来踩的坑。

---

## Common Pitfalls

### Pitfall 1: ⚠️ CLOB `/book` 返回幽灵 0.01/0.99（Issue #180, OPEN）

**What goes wrong:** `client.get_order_book(token_id)` / `client.get_order_books([...])` 对**活跃流动市场**会偶发返回 `bids=[{price: 0.01}]`, `asks=[{price: 0.99}]`，与真实价格无关。
**Why it happens:** Polymarket 后端的 /book endpoint 服务的是断开/陈旧的快照，与 /price endpoint 数据不一致。GitHub issue #180 (2025-11-24) 报告至今 OPEN，无官方修复。[CITED: github.com/Polymarket/py-clob-client/issues/180]
**How to avoid:**
1. 同时调 `get_prices` 拿 BUY/SELL 顶档价，与 `get_order_books` 的顶档比对
2. 检测到差异 > 5% 且 book 报 ~0.01/0.99 → 标 `Category.GHOST_BOOK`，是否 `is_valid=false` 看占比（plan 阶段定阈值）
3. **本 phase 接受这个限制**：顶档量信息只能从 `/book` 拿（`/price` 不带 size），所以仍要调 `/book` —— 但要把价格交叉校验做进 Layer 4
**Warning signs:** `best_ask_price > 0.98 AND best_bid_price < 0.02` 在 `liquidity_usd > $1000` 的市场上出现 → 几乎必是幽灵簿。

### Pitfall 2: Gamma 字符串字段不是真 list

**What goes wrong:** `outcomePrices`、`clobTokenIds` 在 Gamma 返回里是 **JSON 字符串** `"[\"0.5\", \"0.5\"]"` 不是 `["0.5", "0.5"]`。直接当 list 读会得到 char 索引。
**Why it happens:** 旧 API 设计兼容性。reference impl `btc_markets.py:103-108` 明确 `json.loads()` 解一层 [VERIFIED: 文件读取]。
**How to avoid:** normalizer 层统一 `json.loads()`，并对值做 float 转换。
**Warning signs:** test 里看到价格是 "0" 或 "1"（单字符）就是没解。

### Pitfall 3: token_id 是 uint256 字符串

**What goes wrong:** Polymarket CTF token id 是 256-bit unsigned int (e.g., `"71321045679252212594626385532706912750332728571942532289631379312455583992563"`)。当 INTEGER 处理会溢出。
**Why it happens:** ERC-1155 链上 ID 设计。
**How to avoid:** SQLite 列 TEXT，Parquet schema `pa.string()`，Python 全程 str。比较用 `==` 不要转 int。
**Warning signs:** Pandas 读 parquet 时 token_id 列是科学记数法 → 已经丢精度，回不去。

### Pitfall 4: SQLite 默认非 WAL，自动事务边界吞性能

**What goes wrong:** 默认 journal mode 是 `delete`（写阻塞读），插入 10k 行不开事务 → 30s+。
**Why it happens:** Python `sqlite3` 默认每个 statement 自带 commit；非 WAL 时 reader 阻塞。
**How to avoid:**
- `PRAGMA journal_mode=WAL` 一次性设
- `isolation_level=None` 显式管事务，`BEGIN IMMEDIATE` + `executemany` + `COMMIT`
**Warning signs:** "database is locked" 错误 / 落库时间 >> 几秒。

### Pitfall 5: `asyncio.gather` 不带限流 → 一次发 12k 请求

**What goes wrong:** `await asyncio.gather(*[fetch_one(t) for t in tokens])` 直接打爆限流。
**Why it happens:** gather 默认无限并发。
**How to avoid:** 用 `aiolimiter` 包每个底层 HTTP 调用；或 `asyncio.Semaphore(N)` 控制并发。本研究推荐前者（更贴 Cloudflare throttle 行为）。
**Warning signs:** 响应延迟突然从 100ms 跳到 5s+ → 已经在排队。

### Pitfall 6: snapshot 时间错位混淆

**What goes wrong:** Gamma 拉了 12k market 用 30s，CLOB 拉了 24k token book 又用 5min。整个 snapshot 横跨 5+min，**不能** claim "T 时刻的市场切片"。
**Why it happens:** 对"原子快照"的理想化假设。
**How to avoid:**
1. 每条 row 记 `fetched_at_ms`（CLOB 顶档调用结束时刻）
2. snapshot 元数据记 `taken_at_ms`（开始）+ `finished_at_ms`（落库完成）
3. **文档明确**：本系统是"best-effort consistent" 不是 "transactional snapshot"
4. 时间漂移 > 阈值（如 10 分钟）→ Layer 2 标 `incomplete=true` 或加专门 category（plan 决定是否本 phase 加）
**Warning signs:** Phase 3 异常检测发现"价格突变"，要先排除是 snapshot 内部时间漂移导致的虚警。

### Pitfall 7: pyproject.toml + src layout + hatchling 找不到包

**What goes wrong:** `pip install -e .` 后 `import polyarb` 失败。
**Why it happens:** hatchling 默认从根扫，src 布局要显式声明。
**How to avoid:**
```toml
[tool.hatch.build.targets.wheel]
packages = ["src/polyarb"]
```
**Warning signs:** wheel 里没有 `polyarb/` 目录、import 报 ModuleNotFoundError。

---

## Code Examples

### Top-of-book 批量拉取（生产形态草稿）

```python
# Source: 综合 https://context7.com/polymarket/py-clob-client + py-clob-client/issues/180
async def fetch_books_with_validation(token_ids: list[str], clob: ClobReaderClient):
    """
    返回 (books_by_token, prices_by_token) 给 Layer 4 双源校验用.
    """
    # batch 1: order books (含 size)
    books = await clob.get_books(token_ids)
    books_by_token = {}
    for b in books:
        # py-clob-client v1 返回 dict 或 OrderBookSummary, 取 token / asset_id 字段
        tid = b.get("asset_id") or b.get("market") or b.get("token_id")
        books_by_token[tid] = b

    # batch 2: 单点价格（issue #180 对照）
    prices = await clob.get_prices_buy_sell(token_ids)
    return books_by_token, prices
```

### Atomic SQLite + Parquet 编排

```python
# Source: 整合上面 Pattern 3 + 4
import time

async def run_snapshot(mode: str, cfg, db_path, out_dir):
    started = int(time.time() * 1000)

    # 1. Gamma 全量
    gamma = GammaClient()
    raw_markets = await gamma.fetch_all_active_markets()
    markets = [normalize(m) for m in raw_markets]

    # 2. 子集 / 全量决定 token 列表
    if mode == "subset":
        target = [m for m in markets if (m.get("liquidity_usd") or 0) > cfg.liquidity_threshold]
    else:
        target = markets
    token_ids = [t for m in target for t in (m["yes_token_id"], m["no_token_id"]) if t]

    # 3. CLOB 批量拉
    clob = ClobReaderClient()
    books_by_token, prices_by_token = await fetch_books_with_validation(token_ids, clob)

    fetched_ms = int(time.time() * 1000)
    for m in markets:
        for tf, sf_price, sf_size in [("yes_token_id","best_yes_*"), ...]:
            attach_top_of_book(m, books_by_token)
        m["fetched_at_ms"] = fetched_ms

    # 4. 校验
    issues = (
        layer1_count(len(raw_markets), len(markets))
        + layer2_fields(markets)
        + layer4_cross(markets, books_by_token, prices_by_token)
    )
    is_valid = is_valid_overall(issues)

    # 5. 写 Parquet (失败回滚 = tmp 删)
    finished = int(time.time() * 1000)
    parquet_path = compute_path(out_dir, started)  # YYYY/MM/DD/HH-MM-SS.parquet
    write_parquet_atomic(markets, parquet_path)

    # 6. 写 SQLite (一个事务包全)
    write_snapshot(db_path, (started, finished, mode, len(markets),
                             int(is_valid), str(parquet_path), None),
                   to_rows(markets), to_issue_tuples(issues))

    # 7. CLI 输出
    print_one_liner(len(markets), mode, len(issues), parquet_path)
    sys.exit(0 if is_valid else 1)
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| `requests` + `threading` for HTTP | `httpx.AsyncClient` | 2020+ | async/await 是默认；reference impl 已转 |
| 同步 py-clob-client + 串行循环 | `asyncio.to_thread` 包 sync SDK + 批量 endpoint | 2024+ | 单 RPS 1500 → 批量 250k tokens/10s |
| 单 endpoint `get_order_book` | 批量 `get_order_books([BookParams,...])` | py-clob-client 0.10+ | 10x-100x 吞吐 |
| Parquet snappy 默认 | snappy 仍是默认；ZSTD 节省体积但写慢 | — | 本 phase 选 snappy |
| sqlite3 自动事务 | `isolation_level=None` + 显式 BEGIN IMMEDIATE | Python 3.6+ 可用，社区共识 | 写性能 100x，并发安全 |
| `pkg_resources` setup.py | `pyproject.toml` + hatchling | PEP 621 (2021) → 2024 普及 | src layout + entry points 干净 |

**Deprecated/outdated:**
- `setuptools setup.py` 单文件 — 推荐迁 pyproject.toml
- `aiohttp` 不是错的，但 httpx 接口更现代化（reference impl 两个都用是历史包袱）
- `fastparquet` — pyarrow 主流；fastparquet 对 uint 类型有兼容 bug

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | py-clob-client v1 SDK 选定为本 phase 主路径 | Standard Stack | 若 V2 更稳，迁移成本中等（接口大体兼容，构造函数 + import 路径变） |
| A2 | tenacity 是项目标准重试库 | Standard Stack | 项目无现存代码，planner 可改 stamina 或自写 |
| A3 | typer 作为 CLI 框架 | Standard Stack | 改 click 工作量极小 |
| A4 | tqdm 作为进度条 | Standard Stack | 改 rich.progress 需要重写 verbose 输出 |
| A5 | loguru 作为日志（用户全局偏好） | Standard Stack | 用户 CLAUDE.md 偏好 loguru，但项目无现存代码 |
| A6 | Layer 2/4 issues 不导致 is_valid=false（除非超 X% 阈值） | Validator | 阈值 X 由 plan 阶段决定；当前推荐先无阈值（任何 issue 都让 is_valid=true 但记录），观察一段再加 |
| A7 | aiolimiter(280, 10) for Gamma — 7% 安全余量 | Pattern 1 | 实测 RPS 受限于 Cloudflare 抖动，可能要降到 250 |
| A8 | fetched_at_ms 记 CLOB 调用**结束**时刻而非开始 | SQLite schema | 后续时间分析的语义影响；plan 阶段确认 |
| A9 | issue #180 (`get_order_book` ghost data) 在 active liquid markets 上稳定可识别 | Pitfall 1 | 若发生模式与 0.01/0.99 不同（如 0.05/0.95），检测逻辑要放宽 |
| A10 | py-clob-client v1 是同步库 | Pattern 2 | agentbets 文档未明说 sync/async；plan 阶段第一件事跑 `inspect.iscoroutinefunction(client.get_order_book)` 验一下 |
| A11 | hatchling 是项目 build backend 默认 | Project Structure | 改 setuptools / poetry 不会破坏架构 |
| A12 | snappy 压缩对 DuckDB 跨文件 scan 性能足够 | Pattern 4 | ZSTD 与 snappy 读取差 < 1%，影响很小 |

⚠️ **Risk-weighted recommendations:**
- A1 + A10 → plan 阶段的第一个任务（Wave 0）应该是手跑一次 read-only `ClobClient` + `get_order_books` 验证 sync/async 行为和 issue #180 的实际表现。
- A6 → 推荐 plan 决议: **本 phase 不引入 Layer 2/4 阈值**，让所有 issue 进表，观察一周再决定阈值。

---

## Open Questions

1. **CLOB `get_order_books` 返回结构的 token_id 字段名是什么？**
   - What we know: agentbets 引用文档显示 `book.market` 和 `book.bids/asks`，但 dict-form 返回似乎是 `asset_id` (Polymarket convention)。
   - What's unclear: V0.34.6 SDK 的实际返回 dict key。
   - Recommendation: plan 阶段 Wave 0 跑一次真实调用 `print(client.get_order_books([BookParams(token_id=...)]))[0]`，把 key 名钉死后再写 Layer 4。

2. **Layer 2/4 issues 触发 `is_valid=false` 的阈值 X**
   - What we know: D-D3 严格模式说"校验失败仍落库"，但**严格的边界**没定。
   - What's unclear: Layer 2 缺字段 = 0 才是 valid? 还是 < 1% 可容忍?
   - Recommendation: 先无阈值（任何 issue 都不让 valid 翻 false，仅记录）；Phase 3 异常检测真正用到这些数据后再回头校准。

3. **CLOB 频繁调用 prices + books 双源是否会撞限流？**
   - What we know: 单端点 1500/10s, batch 500/10s。Gamma 才是限流瓶颈（300/10s）。
   - What's unclear: 实际并发执行 `get_prices(BUY)` + `get_prices(SELL)` + `get_order_books` 三批是否会因一个 IP / 全局上限被限。
   - Recommendation: plan Wave 0 并发跑 3 batch 测一次，看延迟分布。

4. **Gamma `/markets` vs `/events` — 哪个更合适？**
   - What we know: docs.polymarket.com 推荐 `/events?active=true&closed=false`，但本 phase 关心的是 markets 而非 events。Polymarket/agents `gamma.py` 用的是 `/markets`。
   - What's unclear: `/events` 嵌套的 markets 是否带全字段，还是要再调 `/markets/{id}`。
   - Recommendation: 用 `/markets` 直接路径（Polymarket/agents 已验证）；不嵌套 → 简单 + 限流配额匹配。

5. **`liquidity_usd` 字段在 Gamma 是 `liquidity` 还是 `liquidityNum`？**
   - What we know: 字段同时存在；reference 实现(`btc_markets.py:138`)读的是 `volume`(数值)。
   - What's unclear: `liquidityNum` 是 float, `liquidity` 可能是 string 化数值，行为 plan 阶段必须实测一次。
   - Recommendation: normalizer 层尝试 `liquidityNum`，回退 `liquidity` (float())，保留二者原始值在 incomplete 标记里以备查。

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| Python 3.12+ | 解释器 | 待 plan 阶段 `python --version` 验 | — | — |
| Internet → gamma-api.polymarket.com | Gamma client | ✓（公网） | — | — |
| Internet → clob.polymarket.com | CLOB client | ✓（公网） | — | — |
| pip / pip-tools | 包管理 | — | 待验 | — |
| sqlite3 (stdlib) | SQLite writer | ✓ Python 自带 | 3.40+ stdlib | — |
| 磁盘 — `data/` 5GB+ | Parquet 归档 | 待 plan 阶段 `df -h` 验 | — | 子集模式可降到 1GB |

**Missing dependencies with no fallback:**
- 无 — 公网 + Python 3.12 + pip 即可。读取模式不需要 wallet/钱包/API key。

**Missing dependencies with fallback:**
- DuckDB (验证 parquet 用) — 不装也行，本 phase 测试可改用 `pyarrow.parquet.read_table`。

---

## Validation Architecture

> .planning/config.json 不存在或未明示 `nyquist_validation: false` — 按启用处理。

### Test Framework

| Property | Value |
|---|---|
| Framework | pytest 8.2+ + pytest-asyncio 0.23+ |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]`（待 plan Wave 0 创建） |
| Quick run command | `pytest tests/m1-perception -x -q` |
| Full suite command | `pytest tests/m1-perception` |

### Phase Requirements → Test Map

本项目无 REQ-ID 体系，按 CONTEXT.md decisions 编号映射：

| Decision ID | Behavior | Test Type | Automated Command | File Exists? |
|---|---|---|---|---|
| D-A1 | Gamma 全量 + CLOB 顶档 | unit | `pytest tests/m1-perception/test_gamma_client.py -x` | ❌ Wave 0 |
| D-A2 | 双模式 subset/full | unit | `pytest tests/m1-perception/test_orchestrator.py::test_subset_filter -x` | ❌ Wave 0 |
| D-C1 | SQLite 覆盖式 | unit | `pytest tests/m1-perception/test_sqlite_writer.py::test_overwrite -x` | ❌ Wave 0 |
| D-C2 | Parquet 单文件 + 路径模式 | unit | `pytest tests/m1-perception/test_parquet_writer.py::test_path_format -x` | ❌ Wave 0 |
| D-C3 | 三表 schema | unit | `pytest tests/m1-perception/test_sqlite_writer.py::test_schema -x` | ❌ Wave 0 |
| D-D1 | Layer 1/2/4 各自工作 | unit | `pytest tests/m1-perception/test_validator.py -x` | ❌ Wave 0 |
| D-D3 | is_valid=false 仍落库 | unit | `pytest tests/m1-perception/test_orchestrator.py::test_invalid_still_writes -x` | ❌ Wave 0 |
| D-D4 | issue category 必填 | unit | `pytest tests/m1-perception/test_validator.py::test_category_present -x` | ❌ Wave 0 |
| D-E1/E2 | retry 3x → api_unreachable | unit (mock 故障) | `pytest tests/m1-perception/test_gamma_client.py::test_retry_then_fail -x` | ❌ Wave 0 |
| D-E3 | 不部分补拉 | unit | `pytest tests/m1-perception/test_orchestrator.py::test_no_partial_refetch -x` | ❌ Wave 0 |
| D-F1 | 单行总结 | unit | `pytest tests/m1-perception/test_cli.py::test_silent_summary -x` | ❌ Wave 0 |
| D-MK1/MK2 | Makefile 入口存在 | smoke | `make -n snapshot-markets && make -n snapshot-markets-full` | — |

### Sampling Rate

- **Per task commit:** `pytest tests/m1-perception -x -q` (~3-5s 单元测试)
- **Per wave merge:** `pytest tests/m1-perception` 全套
- **Phase gate:** 全套绿 + 一次真实环境跑通 `make snapshot-markets`（cli + 写出文件 + sqlite 行数 > 0 + parquet 可被 duckdb 读 + 退出码 0）

### Wave 0 Gaps

- [ ] `pyproject.toml` — 不存在，必须先建（Wave 0 第一件事）
- [ ] `tests/m1-perception/conftest.py` — 共享 fixture（mock httpx 客户端、临时 SQLite db、临时 parquet dir）
- [ ] `tests/m1-perception/fixtures/gamma_sample.json` — 录制的 Gamma 真实响应样本（plan 阶段跑一次实际 API 录下来）
- [ ] `tests/m1-perception/fixtures/clob_book_sample.json` — 同样录制
- [ ] Framework install: `pip install pytest pytest-asyncio respx`
- [ ] CI: 暂不需（本 phase 测试全 mock，无 secrets 依赖）

---

## Project Constraints (from CLAUDE.md)

> 来自项目根 `CLAUDE.md` 的硬规则，planner 必须验证不被违反:

- **C1** (技术栈锁定)：Python 3.12+，**禁** LangChain/LangGraph，**只**用 SQLite + Parquet + YAML — 本 phase 完全符合。
- **C2** (Makefile 入口)：所有命令必须有 `make <verb>-<noun>` 入口，本 phase 必须新增 `make snapshot-markets` + `make snapshot-markets-full` — D-MK1/MK2 已锁。
- **C3** (Plan 必须列 Makefile target)：plan 文档显式产出列表中要含 Makefile 命名 — 提醒 planner.
- **C4** (no over-engineering)：项目原则"研发即学习" + reference impl 简洁 → planner 拆 5 PLAN 时不应过度抽象（如不要为 1 个 client 建抽象基类 + 注册器）.
- **C5** (Chinese docs)：除 CLAUDE.md / README.md / 代码注释，docs/* 用中文 — 本 phase RESEARCH/PLAN 是 Chinese.
- **C6** (Test files in `tests/{branch}/`)：本 phase 测试落 `tests/m1-perception/`（用 workstream 名替代分支名）.
- **C7** (代码风格)：Black 100 col + isort + type hint Python 3.12 syntax + `pathlib.Path` over os.path + loguru.

---

## Sources

### Primary (HIGH confidence)

- **Context7 `/polymarket/py-clob-client`** — 官方 CLOB SDK 文档，read-only 初始化、get_order_book/get_order_books/get_midpoint/get_price 签名 [VERIFIED]
- **Context7 `/polymarket/py-clob-client-v2`** — V2 SDK，L0 unauthenticated mode、batch market data API、最新 1.0.0 (2026-04-17) [VERIFIED]
- **github.com/Polymarket/py-clob-client** (v0.34.6, 2026-02-19) — 主代码 + 版本现状 [VERIFIED]
- **github.com/Polymarket/py-clob-client/issues/180** (2025-11-24, OPEN) — `/book` 幽灵数据 bug，**关键** [VERIFIED]
- **github.com/Polymarket/agents/blob/main/agents/polymarket/gamma.py** — 真实 Gamma 客户端实现（pagination loop, `active=true&closed=false&archived=false`） [VERIFIED]
- **docs.polymarket.com/quickstart/introduction/rate-limits** — 官方限流文档（Cloudflare throttle, 不立即 429） [CITED]
- **docs.polymarket.com/developers/gamma-markets-api/fetch-markets-guide** — Gamma 端点文档 [CITED]
- **arrow.apache.org/docs/python/parquet.html** — pyarrow 写 parquet 官方文档 [CITED]
- **docs.python.org/3/library/sqlite3.html** — Python sqlite3 stdlib 文档（threadsafety, isolation_level） [CITED]

### Secondary (MEDIUM confidence)

- **agentbets.ai/guides/polymarket-rate-limits-guide/** — 限流详细表（per-endpoint rate）[CITED, 与 polymarket docs 交叉验证]
- **agentbets.ai/guides/py-clob-client-reference/** — py-clob-client 完整方法表 v0.34.6 [CITED]
- **agentbets.ai/guides/polymarket-gamma-api-guide/** — Gamma 字段全列表 [CITED]
- **3th-party/polymarket-kalshi-weather-bot/backend/data/btc_markets.py** — 项目内 reference impl（httpx async + Gamma /events 实战代码） [VERIFIED 文件读取]
- **aiolimiter.readthedocs.io** — leaky bucket 文档 [CITED]
- **tech-insider.org/sqlite-python-tutorial-fts5-wal-mode-2026/** + **sqlite.org/forum** — WAL + BEGIN IMMEDIATE [CITED]
- **www.python-httpx.org/advanced/resource-limits/** — httpx 连接池 + Limits 文档 [CITED]

### Tertiary (LOW confidence — 仅参考)

- **medium / DEV.to** Snappy vs ZSTD benchmark — 单一作者实测，方向正确但具体数字仅参考
- **packaging.python.org / hatch discussion #1051** — hatchling src layout 配置最佳实践

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — 主要库都通过 Context7 + GitHub release 验证最新版本
- Architecture: HIGH — 模块边界与 CONTEXT.md decisions 严格对齐，每个 Pattern 有官方代码示例支撑
- Pitfalls: HIGH — Pitfall 1 (Issue #180) 是 GitHub 公开 issue，Pitfall 2/3 通过 reference impl + token id 实样验证
- Open questions: MEDIUM — Q1/Q2/Q5 需要 plan 阶段 Wave 0 一次实际 API 调用确认，本 research 没有跑实际网络
- Validation architecture: MEDIUM — 测试结构是合理推断，未来如发现 Polymarket API 增加 secrets/auth 依赖可能需调整

**Research date:** 2026-04-29
**Valid until:** 2026-05-29 (30 天) — Polymarket 仍在活跃迭代（issue #180 未修、V2 SDK 1.0.0 刚发），关注:
- py-clob-client v0.34.7+ release（issue #180 修复）
- py-clob-client-v2 stability signal（达到 production-ready 时考虑迁）
- Polymarket 限流任何变化（账户级配额引入等）
