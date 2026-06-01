# Phase 05: WS /book + /prices 增量推送（L2→L3 升级）- Research

**Researched:** 2026-06-01
**Domain:** L3 升级 — full book depth 持久化 + OHLC K 线视图 + 自动 promote + dashboard `/l3/[asset_id]`
**Confidence:** HIGH (核心栈与代码集成点) · MEDIUM (OHLC view 性能) · LOW (Promote churn 在 prod 实际 rate)

---

## Summary

Phase 05 是把 Phase 03 已 ship 的 L2 通路（WS market channel + l2_top_of_book + l2_trades 已 prod-verified）做**金字塔上一层升级**：自动 promote top-5 锁定集 + 全 depth 持久化（`l2_book_levels`）+ OHLC 1m/5m/1h regular view + dashboard K 线页。CONTEXT.md 16 条 D-XX 决策已锁，但其中 **D-03 OHLC strategy 含一处关键事实错误**需 planner 在 Discuss-phase 第二轮或 RESEARCH 接收时即时修正：

**Primary correction (HIGH confidence):** CONTEXT D-03 写 "复用 Postgres `time_bucket` (含在 Supabase Pro 默认 ext)" — **不正确**。`time_bucket` 是 TimescaleDB extension 函数，而 TimescaleDB 在 Supabase Postgres 17 上**已 deprecate / 不可用**（Postgres 17 是 Supabase 当前 default；旧 Postgres 15 项目支持期延至 2026-05 EoL）。**修法**：改用纯 PG `date_trunc('minute', ts)` + window function `FIRST_VALUE / LAST_VALUE` 写 OHLC view。语义等价、性能在 26M 行 / 年量级配合 BRIN(ts) 索引下 sub-100ms 可达，且零扩展依赖。

**Primary recommendation:** 
1. 立即修 D-03 = 用 `date_trunc(...)` 替代 `time_bucket(...)`，view 命名保持 `l2_ohlc_1m/5m/1h`
2. 沿用 CONTEXT 其余决策（D-01..D-16 全部 honor）
3. WS 动态 subscribe 需在 `ws_market_client` 新增 send-after-connect API（现有客户端只在 (re)connect 时 subscribe 一次，CONTEXT D-11 假设的 "动态加订" 实际上需要新方法，非纯应用层一行代码）
4. lightweight-charts v5.2.0（2026-04-24 最新）+ Next.js 15 dynamic import + `ssr: false` 是标准模式

---

## User Constraints (from CONTEXT.md)

### Locked Decisions（D-01..D-16 全部 honor，按 CONTEXT 原文复述要点）

#### Scope & Boundary
- **D-01**：Phase 05 = L2 → L3 升级（full depth + tick + OHLC + 自动 promote），不是重写 WS plumbing
- **D-12**：Done = 5 L3 markets 跑 24h + OHLC view 返回数据 + book depth 入库 + dashboard `/l3/[asset_id]` 画 K 线；24h soak 而非 7-day

#### L3 Promote
- **D-02**：L2 信号自动 promote（SQL rule 查 `l2_top_of_book`）
- **D-05**：N=5（双 token = 10 subscription，仍在 WS no-limit 安全区）
- **D-09**：复用 Phase 01.1 scanner recipe 框架 → 新建 `src/polyarb/scan_recipes/l3-promote.yaml`，走相同 4 层 SQL injection defense
- **D-13**：v1 阈值 baseline（spread<0.02 / depth_yes_usd>500 / last_trade_ts>now()-3600s / ORDER BY depth_yes_usd DESC LIMIT 5），**yaml 不进 env**（CLAUDE.md "experiment values never touch baseline defaults"）
- **D-14**：5 min recompute（复用 AsyncIOScheduler cron）

#### OHLC
- **D-03**：⚠️ **CONTEXT 写 `time_bucket`，本 RESEARCH 修为 `date_trunc`**（详 §State of the Art + §Common Pitfalls / Pitfall 1）。其余 "SQL window view on l2_top_of_book"、"零新进程"、"精度 = L2 写入频率" 维持不变
- **D-06**：1m + 5m + 1h regular (non-materialized) view，命名 `l2_ohlc_1m/5m/1h`，数据源 `l2_top_of_book.mid_price`

#### Full Book Depth
- **D-04**：新表 `l2_book_levels` 存 top-10 levels/边
- **D-07**：top-10 每边 = 20 rows/snapshot；prod ~144M rows/年 在 8GB compute 容量内
- **D-10**：统一 `l2_*` 命名（不另起 `l3_*`），L3/L2 区别在写入策略而非表名

#### WS Subscription
- **D-11**：复用现有 `ws_market_client` 动态 subscribe 加 L3 token；不开新 WS、不重连、不动 Phase 04.1 watchdog liveness gate。**RESEARCH 修正**：现有 client 仅在 (re)connect 时 subscribe，需新增 `add_subscriptions()` / `remove_subscriptions()` send-after-connect API（详 §Code Examples）

#### Dashboard
- **D-08**：`/l3/[asset_id]` 动态页 → 主区 K 线（`l2_ohlc_1m`）+ 右侧 top-10 depth ladder（`l2_book_levels` 最新 ts 20 行）+ candidates 页加 "L3 promoted" 标签

#### Deploy
- **D-15**：同进程作为 polyarb-l2 fly app 的 asyncio task（不新起 polyarb-l3）
- **D-16**：不等未 deploy 的 2 块代码（04.1 code-review fixes + GAP-401 watchdog liveness），Phase 05 在 main 上干

### Claude's Discretion（按 CONTEXT 原文）
- Promote 规则 SQL 写法（yaml 内嵌 vs 引用 `.sql`）— planner 看现有 recipe 体例决定
- L3 promoter task 是独立 asyncio task 还是融进 `l2_candidate_refresh` — executor 看 Phase 03 D-05 event bus 设计决定
- **Dashboard K 线库 — researcher 倾向 lightweight-charts**（本 RESEARCH 已研究，结论见 §Standard Stack）
- `l2_book_levels` 索引设计（composite PK vs surrogate id+UNIQUE）— 本 RESEARCH 推荐 composite PK，详 §Standard Stack
- `l3_candidates` 是表 / view / daemon 内存态 — 本 RESEARCH 推荐 **daemon 内存态 `_l3_active_set: set[str]`** 作为 source of truth + mirror 到 `l2_top_of_book.l3_active` 或独立 `l3_candidates` view，详 §Architecture Patterns
- soak 期间 OHLC 数据真假对照 — verifier 24h 后做 spot check（与平台 K 线对照）

### Deferred Ideas（OUT OF SCOPE，不研究不规划）
- Yes/No 双 token L3 单边 promote（v2 optimization；v1 假设双 token 都进 L3）
- L3 启动期历史回填 / cold-start backfill（`l2_book_levels` 历史空 → 推 Phase 06）
- Vercel deployment protection 对 `/l3` 路由（EMAIL_WHITELIST 行为，已知）
- 多 OHLC 粒度（15m/4h/1d）— 起步只做 1m/5m/1h
- Materialized view + pg_cron — regular view 起步
- `prices-history` REST backfill 作 source of truth（closed market 12h 颗粒退化，已 cite）
- L3 信号策略（M4 范畴）
- Promote 阈值动态调整 / 进 ENV（v1 锁 yaml + audit trail）

---

## Phase Requirements

> Phase 05 没有 m1-perception REQUIREMENTS.md 显式 REQ-ID 映射（该文件不存在）。下表用本 RESEARCH 推导的 derived requirement ID（PHASE05-Rxx）让 planner 能 task ↔ requirement 对齐；这些 ID 与 CONTEXT D-XX 一对一映射，不引入新需求。

| Derived REQ ID | Description (from CONTEXT D-xx) | Research Support |
|---|---|---|
| PHASE05-R01 | L3 promote 机制（5-min cron + SQL rule, N=5）  | §Architecture Patterns / Pattern 1 (Promote 拓扑) + §Standard Stack (AsyncIOScheduler) |
| PHASE05-R02 | `l2_book_levels` 表 + 写入路径（top-10 levels/边） | §Standard Stack (l2_book_levels DDL) + §Code Examples (`_book_levels_rows_from_frame`) |
| PHASE05-R03 | OHLC view 1m/5m/1h | §Code Examples (date_trunc OHLC view) + §Common Pitfalls / Pitfall 1 |
| PHASE05-R04 | WS 动态 subscribe（add/remove L3 token, 不重连） | §Standard Stack (websockets 15+) + §Common Pitfalls / Pitfall 2 (现有 client 无 send-after-connect) |
| PHASE05-R05 | Dashboard `/l3/[asset_id]` 动态页（K 线 + depth ladder） | §Standard Stack (lightweight-charts v5) + §Code Examples |
| PHASE05-R06 | L3 daemon 复用 polyarb-l2 fly app（同进程 asyncio task） | §Architecture Patterns (集成到 l2_main) + §Don't Hand-Roll |
| PHASE05-R07 | `/health` L3 sub-checks（chain-truth surface） | §Architecture Patterns (Pattern 3 chain-truth) + §Common Pitfalls / Pitfall 5 |
| PHASE05-R08 | 24h prod soak verdict + OHLC spot check | §Validation Architecture |

---

## Project Constraints (from CLAUDE.md)

| Constraint | Impact on Phase 05 |
|---|---|
| 每个 plan 末必须落 SUMMARY，pre-commit hook 强制 | plan-checker 已编码；plan 数（推 4-6）= SUMMARY 数 |
| `make planning-status` zero drift 才能开新工作 | Phase 04.1 已 CLOSED + 04.1 quick task SUMMARY 已 ship；研究开始时已 verified |
| 命令入口约定 — 所有可执行命令入 Makefile | 推断需新增 `make l3-promote-dry-run` / `make ohlc-spot-check` / `make smoke-l3-dashboard` |
| chaos image-aware（python:3.12-slim 无 procps）| Phase 05 不引新 chaos primitive（不在 scope）；若 verify 步骤需 chaos，复用 Phase 04.1 G-03 `/control/chaos/ws-test-kill` |
| chain-truth 纪律（fail-soft 必须 surface /health） | L3 promoter / book_levels writer 都需 /health sub-check；不能 gate 在不存在的 config 字段 |
| Experiment values never touch baseline defaults | D-13 阈值锁 yaml，不进 env（CONTEXT 已 honor） |
| 教学文档（docs/learning/NN-*.md）持续产出 | Phase 末需补 `docs/learning/11-L3-K线.md`（编号续 10-L2-跟踪.md） |
| 工程纪律 — chain-first diagnosis；先读代码再写决策 | RESEARCH 已 trace `ws_market_client.subscribe` 路径，发现 D-11 实际需新增方法 |
| pyproject.toml uv 主管 | Phase 05 不引新依赖（lightweight-charts 是 dashboard 侧 npm；后端无新 deps） |

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|---|---|---|---|
| L3 promote SQL execute (yaml recipe) | API / Backend (polyarb-l2 daemon) | Database (Supabase l2_top_of_book) | Promote 决策必须在 backend 持续计算，依赖 daemon 内存态；DB 只做数据源 |
| WS 动态 subscribe / unsubscribe | API / Backend (polyarb-l2 daemon) | — | WS 长连接是 backend 工艺；前端无关 |
| `l2_book_levels` 写入 | API / Backend (polyarb-l2 daemon) | Database (Supabase write) | mirror_book_levels 同款 fail-soft envelope；写量 ~Δl2_top_of_book × 20 |
| OHLC 视图查询 | Database (Postgres view evaluation) | API (Supabase REST) | regular view = 查询时 run；Postgres 直接出 OHLC 行 |
| K 线渲染 | Browser / Client (lightweight-charts canvas) | Frontend Server (Next.js SSR pass-through props) | lightweight-charts 是 client-only canvas lib；SSR 不可用 |
| `/l3/[asset_id]` 路由 + 数据拉取 | Frontend Server (Next.js Server Component) | Database (Supabase via anon RLS) | Phase 02/03 既有模式：SC 拉数据 → 客户端组件渲染图 |
| `/health` L3 sub-checks | API / Backend (polyarb-l2 starlette) | — | chain-truth — write-side 即读侧；前端不可观察后端健康 |
| L3 候选集状态 | API / Backend (daemon 内存 `_l3_active_set`) | Database (mirror 到 `l2_top_of_book.l3_active` 或独立 view) | source of truth = daemon 内存；DB 是 dashboard 读 view |

---

## Standard Stack

### Core

| Library | Version | Purpose | Why Standard |
|---|---|---|---|
| `lightweight-charts` | 5.2.0 (2026-04-24, [VERIFIED: npm view]) | K 线 + volume + crosshair canvas 图 | TradingView 出品；~50KB minified；PolyChart Chrome 扩展用同一库；prod 同行用得最多 |
| `websockets` (Python) | 15+ (已 pinned，§ws_market_client) | Polymarket WS client | 现有 Phase 03 已 ship 15+ `async for ws in connect(...)` reconnect-iterator 模式 |
| `supabase-py` | (现有 mirror 已用) | l2_book_levels 写入 | 复用 `L2SupabaseMirror` envelope verbatim |
| `apscheduler.AsyncIOScheduler` | (现有) | 5-min cron L3 promoter task | 复用 Phase 02 D-15 已建模式 |
| `psutil>=5.9,<7` | (pyproject.toml runtime dep [VERIFIED: grep]) | /health process:rss_kb | Phase 04.1 G-04 已 promote 到 runtime |
| `asyncpg` | (现有) | 仍是事件总线候选（promoter 若 emit `l3.promoted`） | B1 spawn constraint = 默认 FALSE，opt-in only |

### Supporting (Postgres-native)

| Feature | Version | Purpose | When to Use |
|---|---|---|---|
| `date_trunc('minute'\|'hour', ts)` | Postgres core 9.0+ [CITED: postgresql.org/docs] | OHLC bucket 函数 | **D-03 修法 — 替代 time_bucket** |
| Window functions (FIRST_VALUE/LAST_VALUE/min/max) | Postgres core 11+ | OHLC open/high/low/close | 每 bucket 内取 first ts 的 mid 为 open，last 为 close |
| BRIN index on ts | Postgres core | append-only 时序压缩索引 | l2_book_levels 同款（10× 小于 btree-only） |
| RLS anon SELECT policy | Postgres core | dashboard read 走 anon key | 已在 Alembic 003 建立模板 |

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|---|---|---|
| `date_trunc('minute', ts)` | TimescaleDB `time_bucket('1 minute', ts)` | TimescaleDB extension 在 Supabase Postgres 17 已 deprecate；time_bucket 不可用，会运行时 `function time_bucket does not exist` 失败（实证：Supabase discussion #23365 已有用户撞坑 [CITED]） |
| `date_trunc(...)` (regular view) | Materialized view + `REFRESH MATERIALIZED VIEW` cron | Mat view 引入 pg_cron 或 daemon refresh task + 1-min lag；regular view 在 26M 行/年 + BRIN 上 sub-100ms（[CITED: medium.com/@vbahadircan PG 10M 行调优文]）。CONTEXT D-06 已选 regular，对齐 |
| lightweight-charts | recharts | recharts 是 SVG, K 线性能差；不擅长 canvas + crosshair |
| lightweight-charts | uPlot | 体积最小（~40KB）但社区小，K 线 + volume + crosshair 要自己拼；无 OHLC primitive |
| lightweight-charts | Chart.js + financial 插件 | Chart.js 5.x 但 financial plugin 升级滞后；社区 prod 案例少 |
| lightweight-charts | Plotly | 完整但 ~500KB（10× 体积），dashboard 没现成依赖 |
| `composite PK (asset_id, ts, side, level)` | surrogate `id BIGSERIAL + UNIQUE` | 复合 PK 自然反映 WS book frame 语义；查 latest depth ladder = `ORDER BY ts DESC LIMIT 20`；Alembic 003 既有体例混用（l2_top_of_book 用 surrogate id, l2_event_cursor 用 composite）— **推荐 surrogate id + UNIQUE (asset_id, ts, side, level)** 与 l2_top_of_book / l2_trades 风格一致 |

### Installation

后端 — 不引入新 Python 依赖；新数据库表 + view via Alembic 005。

前端 — dashboard 加 lightweight-charts:
```bash
cd dashboard && pnpm add lightweight-charts@^5.2.0
```

### Version Verification

[VERIFIED: npm view lightweight-charts version time]
- `5.2.0` published 2026-04-24
- 项目当前 dashboard `package.json` 不含 lightweight-charts — 新增依赖
- Next.js `^15.1.3`、React `^19.0.0`（[VERIFIED: grep package.json]）— lightweight-charts v5 兼容（client-only，与 React 18/19 无关）

---

## Architecture Patterns

### System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    polyarb-l2 fly app (D-15 same process)               │
│                                                                          │
│   ┌──────────────┐    ┌──────────────────┐    ┌──────────────────────┐ │
│   │ L1 NOTIFY    │───▶│ event listener   │───▶│ candidate refresh    │ │
│   │ (Supabase)   │    │ (asyncpg LISTEN) │    │ (60s debounce)       │ │
│   └──────────────┘    └──────────────────┘    └──────────┬───────────┘ │
│                                                            │             │
│   ┌──────────────┐    ┌──────────────────┐    ┌───────────▼──────────┐ │
│   │ AsyncIO      │    │ L3 promoter task │    │ candidate_set        │ │
│   │ Scheduler    │───▶│ (5-min cron)     │───▶│ (~ ≤500 assets)      │ │
│   │ (D-14)       │    │ scanner +        │    └───────────┬──────────┘ │
│   └──────────────┘    │ l3-promote.yaml  │                │             │
│                       └────────┬─────────┘                │             │
│                                │                          │             │
│                                ▼                          │             │
│                       ┌──────────────────┐                │             │
│                       │ _l3_active_set   │                │             │
│                       │ (≤10 token,      │                │             │
│                       │  5 markets × 2)  │                │             │
│                       └────────┬─────────┘                │             │
│                                │                          │             │
│   ┌────────────────────────────▼─────────────────────────▼─────────┐  │
│   │              ws_market_client (single connection)               │  │
│   │  - initial: subscribe(candidate_set + l3_active_set)            │  │
│   │  - mid-conn (NEW for Phase 05): add_subscriptions(l3 added)     │  │
│   │  - mid-conn (NEW for Phase 05): remove_subscriptions(l3 removed)│  │
│   │  - watchdog (D-03 stale_s=30 LOCKED, GAP-401 liveness LOCKED)   │  │
│   └────────────────────────────┬────────────────────────────────────┘  │
│                                │                                         │
│         price_change ┌─────────┼─────────┐ last_trade_price              │
│         best_bid_ask │         │         │ book                          │
│                      ▼         ▼         ▼                                │
│   ┌──────────────────────────────────────────────┐                       │
│   │  ws_consumer._on_event dispatcher (l2_main)  │                       │
│   │  by event_type:                              │                       │
│   │   - price_change/best_bid_ask/book           │                       │
│   │     → _tob_row_from_frame → mirror.push_tob  │                       │
│   │   - book + asset_id ∈ _l3_active_set         │                       │
│   │     → _book_levels_rows_from_frame (NEW)     │                       │
│   │     → mirror.push_book_levels (NEW)          │                       │
│   │   - last_trade_price → mirror.push_trades    │                       │
│   └──────────────────────────────────────────────┘                       │
│                                                                          │
│   Starlette /health:                                                     │
│     - existing ws/event_bus/mirror sub-checks (Phase 03/04 已 ship)      │
│     - NEW Phase 05 sub-checks (chain-truth):                             │
│        l3:active_count  l3:last_promote_at_s                             │
│        l3:last_book_levels_write_at_s                                    │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ Supabase REST (mirror writes)
┌─────────────────────────────────────────────────────────────────────────┐
│                          Supabase Postgres                               │
│  Existing tables (Alembic 003): l2_top_of_book / l2_trades /            │
│    l2_candidates / l2_signals / l2_event_cursor                          │
│                                                                          │
│  NEW (Alembic 005):                                                      │
│    l2_book_levels    (asset_id, ts, side, level, price, size)            │
│    VIEW l2_ohlc_1m   (date_trunc 'minute', OHLC via window functions)    │
│    VIEW l2_ohlc_5m   (date_trunc 5min via to_timestamp(floor(...)))      │
│    VIEW l2_ohlc_1h   (date_trunc 'hour')                                 │
│    (Optional) VIEW l3_candidates  (active L3 set view)                   │
└─────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ anon SELECT via RLS
┌─────────────────────────────────────────────────────────────────────────┐
│                  dashboard (Vercel Next.js 15 App Router)                │
│  Existing pages: /candidates /asset/[id]/tob /asset/[id]/trades /signals │
│                                                                          │
│  NEW Phase 05:                                                           │
│    /l3/[asset_id]  Server Component                                      │
│       ├── fetch ohlc rows (l2_ohlc_1m last 24h)                          │
│       ├── fetch latest book_levels (top 20 rows by ts)                   │
│       └── render via <KlineChart /> ("use client")                       │
│                   uses lightweight-charts v5 dynamic import ssr:false    │
│                                                                          │
│    /candidates  +  "L3" badge column (read l3_candidates view or         │
│                      l2_top_of_book.l3_active flag, executor decides)    │
└─────────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure

```
src/polyarb/
├── daemon/
│   ├── l2_main.py             # 加 promoter_task + _book_levels_rows_from_frame
│   ├── ws_consumer.py         # 加 add_subscriptions/remove_subscriptions API
│   └── l3_promoter.py         # NEW — AsyncIOScheduler 5-min cron + scanner call
├── clients/
│   └── ws_market_client.py    # 加 send-after-connect subscribe/unsubscribe payload
├── storage/
│   └── l2_supabase_mirror.py  # 加 push_book_levels method (复用 _project + _chunk)
├── observation/
│   ├── scanner.py             # 不动（复用 4 层 SQL defense）
│   ├── l3_promote.py          # NEW — _l3_active_set 单例 + diff utils
│   └── recipes.py             # （视 executor 选项）若 l3-promote 走 builtin path 加这里
├── scan_recipes/
│   └── l3-promote.yaml        # NEW — D-13 4 阈值 SQL
└── http/
    └── l2_health.py           # 加 l3:active_count / l3:last_book_levels_write_at_s sub-checks

alembic/versions/
└── 005_l2_book_levels_ohlc.py # NEW — l2_book_levels 表 + 3 OHLC view + RLS

dashboard/
├── app/
│   └── l3/[asset_id]/page.tsx # NEW — server component
└── lib/supabase/
    └── l2-queries.ts          # 加 getOhlcForAsset / getBookLevelsLatest

docs/learning/
└── 11-L3-K线.md               # NEW — phase 末教学
```

### Pattern 1: L3 Promote 拓扑（D-02 + D-09 + D-14 落地）

**What**: 复用 Phase 01.1 scanner recipe 框架 + Phase 02 AsyncIOScheduler。L3 promoter task 每 5 min 跑：
1. 调用 scanner runner with `l3-promote.yaml` 直接在 Supabase Postgres 上查（**不走 SQLite 本地**，因为 l2_top_of_book 不在本地 SQLite — 关键修正于 §Common Pitfalls / Pitfall 3）
2. 拿到 top-5 markets → 展开成 ≤10 tokens（每 market 2 token Yes/No）
3. 与 `_l3_active_set` diff → 调 `ws_consumer.add_subscriptions(added)` / `remove_subscriptions(removed)`
4. 更新 `_l3_active_set` + 记录 `_last_promote_at_s` for /health
5. 可选：mirror 到 `l3_candidates` view 或 `l2_top_of_book.l3_active` flag（executor 定）

**When to use**: Phase 05 唯一 promote 路径。**不要**写 hand-rolled SQL — 用 scanner.py 经 4 层 SQL injection defense

**Caveat**: scanner.run_recipe 当前签名是 `(db_path: Path, recipe) -> DataFrame`（[VERIFIED: src/polyarb/observation/scanner.py:131]），即**只接受 SQLite 文件路径**。L3 promote 数据源是 Supabase Postgres `l2_top_of_book` → 必须新增 scanner-Postgres 适配器（或 promoter 自己直接 supabase-py 查询，**不走 scanner 框架**）。Phase 04 Plan 02 已建过类似适配（`l2_temp_db.build_temp_db` 把 Supabase 行写入 named-temp-file SQLite 给 scanner 用）— 同款体例可复用：promoter 先 supabase.table("l2_top_of_book").select(...) → 写入 temp DB → 调 scanner.run_recipe。详 §Code Examples。

### Pattern 2: l2_book_levels 写入（D-04 + D-07 落地）

**What**: 在 `l2_main._on_event` 现有 `book` 分支增强：
1. 现有：`_tob_row_from_frame(frame)` → `mirror.push_top_of_book([row])`（不动）
2. NEW：if `frame.asset_id in _l3_active_set:` → `_book_levels_rows_from_frame(frame)` 投到 20 行 → `mirror.push_book_levels(rows)`

**Write throughput**:
- `book` event rate: 实证 Phase 03/04 prod 主要由 `price_change` 主导，`book` 仅 initial_dump / 全簿刷新时 emit
- 估算：top-5 markets × 双 token × 每分钟若干 book 事件 ≈ 几十次 / min × 20 行 / event = ~1k rows/min → **远低于 Supabase REST 体积上限**（`_CHUNK_SIZE=1000` 已设）
- 全年：~5M-10M 行（远小于 D-07 估的 144M — 因为 book event 远不如 price_change 高频，且只 L3 锁定集才写）
- **不需要 streaming COPY**；继续走 supabase-py `.insert()` REST 即可

**Schema (Alembic 005 推荐)**：

```sql
CREATE TABLE l2_book_levels (
    id          BIGSERIAL PRIMARY KEY,
    asset_id    TEXT NOT NULL,
    ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
    side        TEXT NOT NULL,   -- 'BUY' | 'SELL'
    level       SMALLINT NOT NULL,  -- 1..10
    price       NUMERIC(10,6) NOT NULL,
    size        NUMERIC(14,4) NOT NULL,
    UNIQUE (asset_id, ts, side, level)  -- 防同 frame 重复
);
CREATE INDEX idx_l2_book_levels_asset_ts ON l2_book_levels (asset_id, ts);
CREATE INDEX idx_l2_book_levels_ts_brin ON l2_book_levels USING BRIN (ts);
ALTER TABLE l2_book_levels ENABLE ROW LEVEL SECURITY;
CREATE POLICY anon_read ON l2_book_levels FOR SELECT USING (true);
```

### Pattern 3: chain-truth /health surface（CLAUDE.md / Phase 04 D-08 强制）

每新增的 L3 子系统都必须有 `/health` 子检查，**门控在 write-side 真在 mutate 的字段**（不门控在不存在的 config flag — Phase 03 Inj L2-2 教训）：

```python
# l3_promote.py
_l3_active_set: set[str] = set()
_last_promote_at_s: float | None = None
_last_book_levels_write_at_s: float | None = None

def get_l3_active_count() -> int: return len(_l3_active_set)
def get_last_promote_at_s() -> float | None: return _last_promote_at_s
def get_last_book_levels_write_at_s() -> float | None:
    return _last_book_levels_write_at_s

# l2_health.py 加 3 sub-checks
# l3:active_count       — informational pass，<5 时 warn（未充满 N=5 promote 槽）
# l3:last_promote_at_s  — warn at 2× cron interval (10min), fail at 6× (30min)
# l3:last_book_levels_write_at_s — warn 2× WS event rate window，fail 10×
```

### Pattern 4: Dashboard K 线渲染（D-08 落地）

**Server Component (`/l3/[asset_id]/page.tsx`)**:
```typescript
export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function L3Page({ params }: { params: Promise<{ asset_id: string }> }) {
  const { asset_id } = await params;
  const ohlc = await getOhlcForAsset(asset_id, "1m", 1440);    // 24h × 60 = 1440 bars
  const ladder = await getBookLevelsLatest(asset_id);          // 20 rows最新
  return (
    <main style={{ padding: 24, display: "grid", gridTemplateColumns: "1fr 320px", gap: 16 }}>
      <KlineChart ohlc={ohlc} />        {/* "use client" component */}
      <DepthLadder rows={ladder} />     {/* server-rendered table */}
    </main>
  );
}
```

**Client Component (`KlineChart.tsx`)** — lightweight-charts v5 client-only:
```typescript
"use client";
import { useEffect, useRef } from "react";
import dynamic from "next/dynamic";

// lightweight-charts uses window/document → must client-only
const KlineChart = ({ ohlc }) => {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => {
    let chart: any;
    (async () => {
      const { createChart, CandlestickSeries } = await import("lightweight-charts");
      if (!ref.current) return;
      chart = createChart(ref.current, { width: 800, height: 400 });
      const series = chart.addSeries(CandlestickSeries, {
        upColor: "#26a69a", downColor: "#ef5350",
        wickUpColor: "#26a69a", wickDownColor: "#ef5350",
        borderVisible: false,
      });
      series.setData(ohlc.map(r => ({
        time: Math.floor(new Date(r.bucket_ts).getTime() / 1000),
        open: Number(r.open), high: Number(r.high),
        low: Number(r.low), close: Number(r.close),
      })));
    })();
    return () => chart?.remove();
  }, [ohlc]);
  return <div ref={ref} />;
};
export default KlineChart;
```

### Anti-Patterns to Avoid

- **❌ 用 `time_bucket(...)` in OHLC view** — 在 Supabase Postgres 17 上运行时 `function does not exist`；用 `date_trunc(...)` (详 §Pitfall 1)
- **❌ scanner.run_recipe 直接传 Supabase DSN** — scanner 当前签名只接 SQLite Path；要么走 temp-DB pattern (l2_temp_db) 要么 promoter 自查不走 scanner
- **❌ 在 `_on_event` dispatcher 里 dispatch L3 promote 决策** — promote 走独立 5-min cron task，不在 hot path
- **❌ materialized view + pg_cron** — Supabase 不默认装 pg_cron；regular view 起步（CONTEXT D-06 已锁）
- **❌ 用 SSR 渲染 K 线** — lightweight-charts 是 client-only canvas 库（CITED 官方 docs："not designed to work on the server side"）；必须 dynamic import + `useEffect`
- **❌ 修改 Phase 04.1 watchdog `stale_s=30`** — D-03 LOCKED；GAP-401 liveness gate 同 LOCKED
- **❌ 默认 `POLYARB_EVENT_BUS_ENABLED=True`** — B1 spawn constraint：默认 FALSE，即使 L3 promoter 想 emit `l3.promoted` event
- **❌ 写入 `l3_*` namespace 表名** — D-10 已锁 `l2_*` 统一前缀
- **❌ 把 D-13 4 阈值进 ENV** — yaml-only audit trail (CLAUDE.md "experiment values never touch baseline defaults")

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| OHLC SQL aggregation | 自己写 trade-by-trade 累积器 | Postgres view `date_trunc + FIRST_VALUE/LAST_VALUE/min/max` | 零状态、查询时 run、断电重启不会丢；mid_price 是 derived field 不丢 |
| WS reconnect with subscription state | 手写 reconnect 队列 | 现有 `websockets` 15+ `async for ws in connect(...)` reconnect iterator + `initial_dump=True` on (re)subscribe | Phase 03 已 prod-verified |
| K 线 + volume + crosshair | Canvas / D3 自己画 | `lightweight-charts` v5 | ~50KB minified；TradingView 出品；K线类标准 |
| Time series query optimization | 自己分区 / 自建 timescale | Postgres BRIN(ts) + btree(asset_id, ts) | Alembic 003 已建过模板；BRIN 10× 小于 btree-only |
| SQL injection defense in promote recipe | Allowed-keywords 自己 regex | 复用 `polyarb.observation.scanner` 4 层 defense | yaml 不可信路径强制 trust=False；Phase 01.1 已加固 |
| Cron scheduling | 写自己的 asyncio.sleep loop | `apscheduler.AsyncIOScheduler` | Phase 02 D-15 已落地，跨重启状态可管理 |
| Sentry breadcrumb + fail-soft envelope | 自己 try/except | 复用 `L2SupabaseMirror` dual-anchor breadcrumb 模式 | Phase 02.1 P1 双锚点已 prod-verified |
| HMAC-gated control endpoint（若要手工 L3 promote trigger） | 自己写 middleware | 复用 `polyarb.http.control.ControlAuthMiddleware` | Phase 02.1 D-03 已 ship；Phase 04.1 G-03 chaos endpoint 同款 |

**Key insight**: Phase 05 是"在已建的 L2 通路上盖一层 L3 加工"，凡是 Phase 03 ship 过的 envelope 都直接 verbatim 复用；新增量主要在 (1) Alembic 005 新表/view、(2) lightweight-charts 前端、(3) ws_market_client 新方法、(4) l3_promoter 调度。

---

## Common Pitfalls

### Pitfall 1：`time_bucket(...)` 在 Supabase Postgres 17 不存在 ⚠️ CRITICAL

**What goes wrong**: CONTEXT D-03 写 "复用 Postgres `time_bucket` (含在 Supabase Pro 默认 ext)"，若 planner / executor 据此写 view `SELECT time_bucket('1 minute', ts) AS bucket, ...`，prod (Supabase Postgres 17 default) 会运行时报 `function time_bucket(unknown, timestamp with time zone) does not exist`。

**Why it happens**: `time_bucket` 是 **TimescaleDB extension** 函数，**不是** Postgres core。
- TimescaleDB 在 Supabase Postgres 17 项目里 **已 deprecate / 不可用** [CITED: supabase.com/docs/guides/database/extensions/timescaledb 2026-06]
- 旧 Postgres 15 项目可用，但 PG 15 在 Supabase 平台 EoL 2026-05（已过期）
- 新建 Supabase Pro 项目 default 是 Postgres 17 → TimescaleDB 不可启用
- 实证：[GitHub supabase discussion #23365](https://github.com/orgs/supabase/discussions/23365) "timescale extension enabled: function time_bucket does not exist"

**How to avoid**: 改用 Postgres core `date_trunc('minute', ts)`。语义对齐：
```sql
-- 1m bucket
date_trunc('minute', ts)
-- 5m bucket (date_trunc 不支持任意分钟，要 floor)
to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300) AT TIME ZONE 'UTC'
-- 1h bucket
date_trunc('hour', ts)
```

**Warning signs**: prod 部署 Alembic 005 后 `/l3/[asset_id]` 页面 500 / Supabase 日志 `ERROR: function time_bucket does not exist`。

**Verification gate**: 在 Discuss-phase 第二轮（如果还有）或 Plan 阶段，确认 `SELECT version();` on Supabase 实际是 PG 17.x；planner 用 `date_trunc` 写 view。

---

### Pitfall 2：现有 `ws_market_client` 不支持 send-after-connect subscribe

**What goes wrong**: CONTEXT D-11 写 "现有 `ws_market_client` 动态 subscribe 加 L3 token … 把整个 §2.2 动态切换跟踪集问题降级为应用层 subscribe/unsubscribe 一行代码" — 假设客户端已支持。

**实情** ([VERIFIED: src/polyarb/clients/ws_market_client.py 全文 + ws_consumer.py 全文]):
- `stream_market_events()` 当前只在 (re)connect 后**一次性** `await ws.send(json.dumps(sub_payload))`（line 89-90）
- 然后 `async for raw in ws:` 进入 receive 循环
- **没有暴露 send 方法**给外部 mid-connection 调用
- `WsConsumer._subscribed_assets` mutation 当前依赖 "等下次 reconnect 重读" — 不是 "in-flight subscribe"
- Phase 04 D-04 `bootstrap_asset_ids` + candidate refresh 路径全是 "改 list，等 reconnect" 模式

**Why it happens**: 协议层 Polymarket WS **确实** 支持 `{"operation": "subscribe", "assets_ids": [...]}` 动态加订 [CITED: thread §2.2 Q1]，但**客户端代码未实现该路径**。

**How to avoid**: 
1. `stream_market_events` 需要暴露当前 ws 对象给 caller (GAP-401 已建 `on_connect` 钩子 — 可复用)
2. `WsConsumer` 新增 `async def add_subscriptions(asset_ids)` / `async def remove_subscriptions(asset_ids)` 方法，内部 `await self._current_ws.send(json.dumps({"operation": "subscribe", "assets_ids": [...]}))`
3. 必须 thread-safety / async-safety：consumer loop 同时 `async for raw in ws` 和外部调 `ws.send` — websockets 15+ 支持 concurrent send + recv [CITED: websockets 文档 "It is safe to send from one task while receiving from another"]

**Warning signs**: L3 promoter 5-min 跑完，flyctl logs 显示 `_l3_active_set` 已 mutate 但实际 WS 没收到新 token 的 frames（要等下次 watchdog 30s timeout → reconnect 才生效）。

**Alternative fallback**: 如果不想改 ws_market_client.py，可以人为触发 reconnect（`ws_consumer.run` 内部 stop → restart），但代价是丢 watchdog 状态 + 短暂 30s WS 空窗 + Phase 04.1 watchdog liveness gate 失效——**强烈不推荐**。

---

### Pitfall 3：scanner.run_recipe 只接 SQLite，不接 Supabase Postgres

**What goes wrong**: CONTEXT D-09 "复用 Phase 01.1 scanner recipe 框架 ... 走相同的 4 层 SQL injection defense"，但 D-09 + D-02 暗示 promote rule 查的是 `l2_top_of_book`（在 Supabase Postgres 上），不是本地 SQLite。

**实情** ([VERIFIED: scanner.py:131-157]): `run_recipe(db_path: Path, recipe)` 只接 `Path` 参数，内部硬编码 `sqlite3.connect(f"file:{db_path}?mode=ro")`。

**Why it happens**: Phase 01.1 scanner 是为本地 L1 SQLite 设计；4 层 defense 全部 SQLite-specific（`sqlite_master` table allowlist 等）。

**How to avoid**: 两种修法（executor 选其一）：
1. **Promoter 不走 scanner，直接 supabase-py 查 `l2_top_of_book`**：损失 4 层 SQL defense 中"yaml 不可信路径"的保护，但 yaml 仍 `safe_load` + 严格 schema 校验 + service_role 只读 + Supabase RLS 兜底
2. **Phase 04 Plan 02 temp-DB pattern 复用** (`l2_temp_db.build_temp_db`)：promoter 先 `supabase.table('l2_top_of_book').select('asset_id, mid_price, spread, depth_yes_usd, ...').execute()` → 行写入 named-temp-file SQLite → 调 `scanner.run_recipe(tmp_path, recipe)`。**优点**：4 层 SQL defense 全继承；**缺点**：增加 temp file IO

**推荐**：方案 2（与 Phase 04 D-02 一致），但 D-13 v1 阈值是固定的，yaml 不暴露给外部 → 可视为 builtin recipe（`_is_trusted=True`，绕过 strict validators）— 让 promote.yaml 安全等级降到与 builtin recipe 相同；这是 the agent's discretion 范畴，planner 与 executor 协商。

**Warning signs**: 试图把 Supabase DSN 传给 `scanner.run_recipe(...)` → `sqlite3.OperationalError: unable to open database file`。

---

### Pitfall 4：OHLC view 在数据稀疏窗口返回空 / 误导 K 线

**What goes wrong**: `l2_top_of_book` 写入受 WS 事件驱动，prod 实证（Phase 04.1 D-06）撞低活跃窗口时 T1/T2 全是 `WAITING_FOR_EVENT`。OHLC view 在数据稀疏窗口（凌晨美东 / Supabase pause 期 / WS 断连期）会**没行**，dashboard K 线呈空白或断点。

**Why it happens**:
- regular view 是查询时 run；底层无数据 → view 无行
- 不像 broker 撮合数据，prediction market mid_price 在无 trade 时**仍应有值**（best_bid_ask 还在），但 WS `best_bid_ask` 事件只在 bid/ask 变化时 emit
- 用户看 K 线时假设 "每分钟有 1 根" — view 缺失 bucket 误导

**How to avoid**:
1. **接受 sparse**：dashboard 渲染时按 bucket time 排序，前端 lightweight-charts 接 sparse data 不报错；用户解释（教学文档 11 提示 "无 trade 时段无 K 线"）
2. **Optional carry-forward view**（不推荐起步）：用 PG `generate_series` 生成 1m grid LEFT JOIN ohlc，缺失 bucket 用前向填充 — view 复杂度上升，不在 v1 scope
3. **24h soak verification**：spot check 选 5 L3 markets 至少 80% 时间窗有 OHLC 行；低于则 alert

**Warning signs**: dashboard 显示 K 线只有几根 + 大段空白；prod /health `l3:active_count=5` 但 `l3:last_book_levels_write_at_s` 老 → 实际是低活跃，不是 bug。

---

### Pitfall 5：L3 promote 期间 5-min 撞 candidate refresh 60s debounce

**What goes wrong**: candidate refresh 已有 `REFRESH_DEBOUNCE_S=60` ([VERIFIED: l2_candidate_refresh.py:48])。L3 promoter 5-min cron 触发 `add_subscriptions(l3_token)` → 修改 `ws_consumer._subscribed_assets` → 若 candidate refresh 紧接来一刀（snapshot_complete NOTIFY 进），会用 candidate_set 覆盖 L3 token。

**Why it happens**: `on_snapshot_complete` 直接 `ws_consumer._subscribed_assets = list(new_asset_ids)` (line 416) — **整 list 覆写**，不做集合差集 union。

**How to avoid**:
1. 把 `_subscribed_assets` 拆成 `_candidate_set: set[str]` + `_l3_active_set: set[str]`，每次 mutation 后重算 union
2. 或：L3 promoter 写完后立即 trigger 一次 candidate refresh（让 candidate refresh 知道当前 L3 token），把 L3 token 强行加入 candidate_set 输出

**推荐**：方案 1（数据模型更干净）。executor 在 Phase 05 plan 里设计 `WsConsumer` 内部 `_compute_active_assets()` helper return union。

**Warning signs**: L3 promoter run 后 3 分钟内 `_subscribed_assets` 从 N+10 token 回退到 N token，flyctl logs 显示 `candidate refresh: +0 -10` 把 L3 token 全删掉。

---

### Pitfall 6：BRIN(ts) 在 frequent UPDATE 表上失效；本表 append-only 是必要前提

**What goes wrong**: 若以后想给 `l2_book_levels` 加 "UPDATE size on cancel" 路径（hand-roll book reconstruction），BRIN(ts) 索引效率会迅速崩盘（block range visibility map fragmenting）。

**Why it happens**: BRIN 假设 block 内 ts 单调递增；UPDATE 破坏这一假设 [CITED: crunchydata BRIN blog]。

**How to avoid**: 严守 append-only：每个 WS `book` event 是一次完整 top-10 snapshot，直接 INSERT 20 新行 + 老行不 DELETE 也不 UPDATE。查 latest depth ladder 走 `ORDER BY ts DESC LIMIT 20` 在 `(asset_id, ts)` btree 索引下走 index-only scan。

**Warning signs**: 一段时间后 `EXPLAIN ANALYZE` 上 `idx_l2_book_levels_ts_brin` 走的 row 估算与实际差大 / scan 时间飙升。

---

### Pitfall 7：Phase 04.1 SESSION 33 GAP-401 watchdog liveness gate 必须保持

**What goes wrong**: Phase 05 改 ws_market_client / ws_consumer 时，若意外移除 `_liveness_check` 或 `_stash_ws` 钩子，回归 GAP-401 watchdog false-trip bug（quiet socket → false reconnect）。

**Why it happens**: 新增 `add_subscriptions` 时如果 refactor `stream_market_events` 把 on_connect 钩子重新封装，容易漏 hook。

**How to avoid**:
1. Phase 05 Plan 必须有 "regression test: GAP-401 liveness gate intact" 一项 — 运行 quick task 260531 的 10 个测试集
2. plan-checker 验证：新加方法不破坏 `WsConsumer._liveness_check` 写入路径

**Warning signs**: prod 安静窗口（凌晨）watchdog 仍 false-trip 重连 — GAP-401 复发。

---

### Pitfall 8：`l3_active` 标记如何在 candidates 页面 surface

**What goes wrong**: D-08 写 "candidates 页加 'L3 promoted' 标签" — dashboard 怎么知道哪些 asset 是 L3？

**Options & tradeoff**:

| Option | Source of truth | dashboard read 方式 | Cost |
|---|---|---|---|
| A) `l3_candidates` view | daemon 写 — 但 daemon 内存才是 SoT | `SELECT asset_id FROM l3_candidates` | 需新 view + daemon 维护写入 |
| B) `l2_top_of_book.l3_active` boolean 列 | 每行更新（不 append-only） | 拉 latest tob 即可 | 破坏 append-only 假设；不推荐 |
| C) `l2_candidates` 加 `l3_promoted_at_ts TIMESTAMPTZ` 列 | append-only | filter is not null | 复用现有表 |
| D) 独立 `l3_promotions` 表 | append-only history | join 取 latest | 新表 |

**推荐**：**Option C** — 复用现有 `l2_candidates` schema，加 `l3_promoted_at_ts` nullable column（Alembic 005 add-only），dashboard `/candidates` 页 filter `l3_promoted_at_ts IS NOT NULL AND removed_at_ts IS NULL`。理由：复用现有 RLS / index / mirror 写路径；最小改动。

---

## Runtime State Inventory

> Phase 05 主要是新建 + 增量；现有运行时状态调整面如下。

| Category | Items Found | Action Required |
|---|---|---|
| Stored data | Supabase tables 全新 — `l2_book_levels`, `l3_promoted_at_ts` 列 | Alembic 005 add-only |
| Live service config | fly-l2.toml secrets — 无需新增（仍用现有 SUPABASE/URL/SERVICE_KEY/SCAN_SHARED_SECRET） | 无 |
| OS-registered state | 无新 cron / launchd | 无 |
| Secrets/env vars | 无新 env var；D-13 阈值锁 yaml | 无 |
| Build artifacts | dashboard 加 lightweight-charts 进 pnpm-lock.yaml；Vercel 重新 build | `cd dashboard && pnpm install` after add |
| daemon 内存态 | NEW: `_l3_active_set: set[str]`, `_last_promote_at_s: float \| None`, `_last_book_levels_write_at_s: float \| None`（promoter 单例 / module-level） | 全新；只读 getter 给 /health |
| dashboard env | Vercel env vars — 无新增 | 无 |
| Sentry / Axiom integration | 复用现有 service tag `polyarb-l2`；新 breadcrumb category=`l3-promote` / `l3-book-levels` 区分 | 无配置变动 |

**Nothing in category** (verified by §code_context scan):
- No new OS-level services
- No new env vars in `.env.example`
- No git-tracked schema files to delete (additive only)

---

## Code Examples

### Example 1: Alembic 005 — `l2_book_levels` + 3 OHLC views (date_trunc 修法)

```python
# alembic/versions/005_l2_book_levels_and_ohlc.py
"""l2_book_levels + 3 OHLC views (Phase 05 D-04/D-06)

Revision ID: 005
Revises: 004
Create Date: 2026-06-01

⚠ time_bucket is TimescaleDB extension and NOT available on Supabase
Postgres 17 (deprecated). Use date_trunc + window functions instead.
Source: https://supabase.com/docs/guides/database/extensions/timescaledb
"""
from __future__ import annotations
import sqlalchemy as sa
from alembic import op

revision = "005"
down_revision = "004"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── l2_book_levels (Phase 05 D-04 + D-07) ────────────────────────────────
    op.create_table(
        "l2_book_levels",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False,
                  server_default=sa.text("now()")),
        sa.Column("side", sa.String(8), nullable=False),     # 'BUY' | 'SELL'
        sa.Column("level", sa.SmallInteger, nullable=False),  # 1..10
        sa.Column("price", sa.Numeric(10, 6), nullable=False),
        sa.Column("size", sa.Numeric(14, 4), nullable=False),
        sa.UniqueConstraint("asset_id", "ts", "side", "level",
                            name="uq_l2_book_levels_asset_ts_side_level"),
    )
    op.create_index("idx_l2_book_levels_asset_ts",
                    "l2_book_levels", ["asset_id", "ts"])
    op.execute("CREATE INDEX idx_l2_book_levels_ts_brin "
               "ON l2_book_levels USING BRIN (ts);")
    op.execute("ALTER TABLE l2_book_levels ENABLE ROW LEVEL SECURITY;")
    op.execute("CREATE POLICY anon_read ON l2_book_levels "
               "FOR SELECT USING (true);")

    # ── l2_candidates.l3_promoted_at_ts (Phase 05 D-08 surface) ─────────────
    op.add_column(
        "l2_candidates",
        sa.Column("l3_promoted_at_ts", sa.TIMESTAMP(timezone=True),
                  nullable=True),
    )
    op.create_index("idx_l2_candidates_l3_promoted",
                    "l2_candidates", ["l3_promoted_at_ts"])

    # ── OHLC views (date_trunc, not time_bucket — Pitfall 1) ────────────────
    # Each view buckets l2_top_of_book.mid_price into 1m/5m/1h windows.
    # Uses DISTINCT ON for open (first per bucket) and last_value frame for
    # close (last per bucket) — semantics-equivalent to TimescaleDB's
    # first()/last() aggregates.
    op.execute("""
        CREATE OR REPLACE VIEW l2_ohlc_1m AS
        SELECT
            asset_id,
            date_trunc('minute', ts) AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]  AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1] AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id, date_trunc('minute', ts);
    """)

    op.execute("""
        CREATE OR REPLACE VIEW l2_ohlc_5m AS
        SELECT
            asset_id,
            to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300)
                AT TIME ZONE 'UTC'                      AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]   AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1]  AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id,
                 to_timestamp(floor(EXTRACT(epoch FROM ts) / 300) * 300)
                     AT TIME ZONE 'UTC';
    """)

    op.execute("""
        CREATE OR REPLACE VIEW l2_ohlc_1h AS
        SELECT
            asset_id,
            date_trunc('hour', ts) AS bucket_ts,
            (array_agg(mid_price ORDER BY ts ASC))[1]   AS open,
            MAX(mid_price)                              AS high,
            MIN(mid_price)                              AS low,
            (array_agg(mid_price ORDER BY ts DESC))[1]  AS close,
            COUNT(*)                                    AS sample_count
        FROM l2_top_of_book
        WHERE mid_price IS NOT NULL
        GROUP BY asset_id, date_trunc('hour', ts);
    """)

    # Views inherit base table's RLS — but explicit GRANT keeps surface
    # whitelisted (Phase 02 D-19 pattern).
    op.execute("GRANT SELECT ON l2_ohlc_1m TO anon;")
    op.execute("GRANT SELECT ON l2_ohlc_5m TO anon;")
    op.execute("GRANT SELECT ON l2_ohlc_1h TO anon;")


def downgrade() -> None:
    op.execute("DROP VIEW IF EXISTS l2_ohlc_1h;")
    op.execute("DROP VIEW IF EXISTS l2_ohlc_5m;")
    op.execute("DROP VIEW IF EXISTS l2_ohlc_1m;")
    op.drop_index("idx_l2_candidates_l3_promoted", table_name="l2_candidates")
    op.drop_column("l2_candidates", "l3_promoted_at_ts")
    op.drop_table("l2_book_levels")
```

**Performance estimate** (regular view on 26M rows/year `l2_top_of_book`):
- Query `SELECT * FROM l2_ohlc_1m WHERE asset_id=? AND bucket_ts > now() - interval '24 hours'`
- With `(asset_id, ts)` btree → index scan on time range
- 1 day window × ~minute resolution × top-5 markets → ~1440 buckets × small aggregation per asset
- Estimated **<100ms** [CITED: medium.com/@vbahadircan PG 10M rows tuned sub-second]; user reports up to 28s on un-tuned mat view aggregation [CITED: openspaceservices.com] — but those are 10× our data and pre-BRIN/btree

### Example 2: `_book_levels_rows_from_frame` projector

```python
# src/polyarb/daemon/l2_main.py — add alongside _tob_row_from_frame / _trade_row_from_frame

def _book_levels_rows_from_frame(frame: dict, max_levels: int = 10) -> list[dict]:
    """Project a WS book frame into up to 2*max_levels l2_book_levels rows.

    Polymarket book frame shape (empirical, thread §2.2 Q1):
        {event_type: "book", asset_id, market, bids: [{price, size}, ...],
         asks: [{price, size}, ...], timestamp, hash}

    Returns up to 2 × max_levels rows (top-N per side). Empty frame → [].
    Phase 05 D-07: max_levels=10 → 20 rows/snapshot/asset.

    Side normalization: bids → 'BUY', asks → 'SELL' (consistent w/ l2_trades.side).
    """
    asset_id = frame.get("asset_id")
    if not asset_id:
        return []
    ts_iso = _isoformat_ts(frame.get("timestamp") or frame.get("ts"))
    if ts_iso is None:
        return []
    rows: list[dict] = []
    for side_key, side_norm in (("bids", "BUY"), ("asks", "SELL")):
        levels = frame.get(side_key) or []
        for idx, entry in enumerate(levels[:max_levels], start=1):
            if not isinstance(entry, dict):
                continue
            try:
                price = float(entry.get("price"))
                size = float(entry.get("size", 0))
            except (TypeError, ValueError):
                continue
            if size <= 0:
                continue
            rows.append({
                "asset_id": asset_id,
                "ts": ts_iso,
                "side": side_norm,
                "level": idx,
                "price": price,
                "size": size,
            })
    return rows
```

### Example 3: `mirror.push_book_levels` 复用 envelope verbatim

```python
# src/polyarb/storage/l2_supabase_mirror.py — add method to class L2SupabaseMirror

_NARROW_BOOK_LEVELS_COLUMNS: tuple[str, ...] = (
    "asset_id", "ts", "side", "level", "price", "size",
)

def push_book_levels(self, rows: list[dict]) -> bool:
    """Bulk insert l2_book_levels rows. Fail-soft — never raises.

    Phase 05 D-04/D-07. Mirrors push_top_of_book envelope verbatim:
    narrow projection + 1000-row chunks + dual-anchor breadcrumb.
    """
    try:
        narrow = _project(rows, _NARROW_BOOK_LEVELS_COLUMNS)
        for chunk in _chunk(narrow, _CHUNK_SIZE):
            self._client.table("l2_book_levels").insert(chunk).execute()
        sentry_sdk.add_breadcrumb(
            category="l3-book-levels", level="info",
            message=f"push_book_levels ok rows={len(rows)}",
            data={"rows": len(rows), "table": "l2_book_levels"},
        )
        logger.info(f"l3-mirror: pushed {len(rows)} book_levels rows")
        # Update process-local freshness anchor for /health surface.
        from polyarb.observation import l3_promote
        l3_promote._last_book_levels_write_at_s = _time.time()
        return True
    except Exception as e:  # noqa: BLE001 — fail-soft per D-12 envelope
        logger.error(
            f"l3-mirror push_book_levels failed rows={len(rows)}: {str(e)[:200]}"
        )
        sentry_sdk.add_breadcrumb(
            category="l3-book-levels", level="warning",
            message=f"push_book_levels failed rows={len(rows)}",
            data={"rows": len(rows), "table": "l2_book_levels",
                  "error": str(e)[:200]},
        )
        return False
```

### Example 4: WS dynamic subscribe (NEW method on WsConsumer)

```python
# src/polyarb/daemon/ws_consumer.py — add to class WsConsumer

import json as _json

async def add_subscriptions(self, asset_ids: list[str]) -> bool:
    """Mid-connection subscribe to new asset_ids (Phase 05 D-11).

    Sends {"operation": "subscribe", "assets_ids": [...]} on the current
    live ws without reconnecting. Protocol-level support documented in
    thread §2.2 Q1 [CITED: docs.polymarket.com 2025-05-28 changelog].

    Updates _subscribed_assets to keep state coherent. Safe to call
    concurrently with the receive loop — websockets 15+ allows
    send/recv from different tasks.

    Returns False if no live ws (cold start), True on send success.
    """
    if not asset_ids:
        return True
    if self._current_ws is None:
        logger.warning(
            f"add_subscriptions called with no live ws — adding "
            f"{len(asset_ids)} to _subscribed_assets only "
            f"(will be picked up on next reconnect)"
        )
        for aid in asset_ids:
            if aid not in self._subscribed_assets:
                self._subscribed_assets.append(aid)
        return False
    try:
        payload = {"operation": "subscribe", "assets_ids": asset_ids,
                   "initial_dump": True}
        await self._current_ws.send(_json.dumps(payload))
        for aid in asset_ids:
            if aid not in self._subscribed_assets:
                self._subscribed_assets.append(aid)
        logger.info(f"ws add_subscriptions: +{len(asset_ids)} assets in-flight")
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(f"add_subscriptions send failed: {e!r}")
        return False

async def remove_subscriptions(self, asset_ids: list[str]) -> bool:
    """Mid-connection unsubscribe (Phase 05 D-11 churn out of L3)."""
    if not asset_ids:
        return True
    if self._current_ws is None:
        for aid in asset_ids:
            if aid in self._subscribed_assets:
                self._subscribed_assets.remove(aid)
        return False
    try:
        payload = {"operation": "unsubscribe", "assets_ids": asset_ids}
        await self._current_ws.send(_json.dumps(payload))
        for aid in asset_ids:
            if aid in self._subscribed_assets:
                self._subscribed_assets.remove(aid)
        logger.info(
            f"ws remove_subscriptions: -{len(asset_ids)} assets in-flight"
        )
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(f"remove_subscriptions send failed: {e!r}")
        return False
```

### Example 5: l3-promote.yaml + promoter task

```yaml
# src/polyarb/scan_recipes/l3-promote.yaml
# Phase 05 D-09 + D-13. v1 阈值 baseline — 调参靠改本文件 + commit (yaml-only audit
# trail). DO NOT promote to env var (CLAUDE.md "experiment values never touch
# baseline defaults").

recipes:
  l3-promote:
    description: |
      Top-5 L3 锁定集 — 优先 depth_yes_usd 高 + spread 紧 + 最近活跃。
      Phase 05 D-02 trigger + D-05 N=5 + D-13 阈值。
    where: |
      spread < 0.02
      AND depth_yes_usd > 500
      AND ts > (now() - interval '1 hour')
    order_by: depth_yes_usd DESC
    limit: 5
```

```python
# src/polyarb/observation/l3_promote.py (NEW)
"""L3 promote task — 5-min cron, scanner-driven candidate lock (Phase 05 D-02/D-09/D-14).

Module-level state (single L2 daemon process):
- _l3_active_set: current locked tokens (≤10 = 5 markets × 2)
- _last_promote_at_s: wall-clock of last successful promote (chain-truth)
- _last_book_levels_write_at_s: wall-clock of last l2_book_levels write
  (chain-truth, mutated by L2SupabaseMirror.push_book_levels)

Public API:
- promote_run(settings, mirror, ws_consumer) -- one cron tick
- get_l3_active_count(), get_last_promote_at_s(),
  get_last_book_levels_write_at_s() — /health readers
"""
from __future__ import annotations
import time
from pathlib import Path
from typing import Any
from loguru import logger
from supabase import create_client

# Promoter state
_l3_active_set: set[str] = set()
_last_promote_at_s: float | None = None
_last_book_levels_write_at_s: float | None = None


def get_l3_active_set() -> set[str]:
    return set(_l3_active_set)


def get_l3_active_count() -> int:
    return len(_l3_active_set)


def get_last_promote_at_s() -> float | None:
    return _last_promote_at_s


def get_last_book_levels_write_at_s() -> float | None:
    return _last_book_levels_write_at_s


async def promote_run(
    *, settings: Any, mirror: Any, ws_consumer: Any,
    recipe_yaml_path: Path,
) -> dict:
    """One promote tick (called by AsyncIOScheduler 5-min cron).

    Returns diff summary {added: [...], removed: [...]} for logging.
    Fail-soft per D-12 envelope — exceptions logged + Sentry breadcrumb,
    never crashes daemon.
    """
    global _l3_active_set, _last_promote_at_s
    # 1) Fetch l2_top_of_book latest snapshot per asset_id from Supabase
    #    (executor decides exact query: window-latest or 60s-recent rows)
    # 2) Apply l3-promote.yaml SQL via scanner (temp-DB pattern from Phase 04
    #    Plan 02 — l2_temp_db.build_temp_db) OR direct supabase-py query
    # 3) Get top-5 markets → expand to ≤10 token_ids (Yes+No per market)
    # 4) Diff vs _l3_active_set, call ws_consumer.add/remove_subscriptions
    # 5) Mirror to l2_candidates.l3_promoted_at_ts (D-08 surface)
    # 6) Update _last_promote_at_s for /health

    # ... (full impl in executor's plan task)
    new_set: set[str] = set()  # populated by scanner result
    added = new_set - _l3_active_set
    removed = _l3_active_set - new_set

    if added:
        await ws_consumer.add_subscriptions(sorted(added))
    if removed:
        await ws_consumer.remove_subscriptions(sorted(removed))

    _l3_active_set = new_set
    _last_promote_at_s = time.time()
    logger.info(f"l3-promote: +{len(added)} -{len(removed)} "
                f"total={len(new_set)}")
    return {"added": sorted(added), "removed": sorted(removed)}
```

### Example 6: dashboard server query helpers + chart component

```typescript
// dashboard/lib/supabase/l2-queries.ts — add new helpers

export interface L2OhlcRow {
  asset_id: string;
  bucket_ts: string;        // ISO timestamp
  open: number;
  high: number;
  low: number;
  close: number;
  sample_count: number;
}

export interface L2BookLevel {
  asset_id: string;
  ts: string;
  side: "BUY" | "SELL";
  level: number;
  price: number;
  size: number;
}

export async function getOhlcForAsset(
  assetId: string,
  granularity: "1m" | "5m" | "1h" = "1m",
  hours: number = 24,
  supabase?: SupabaseClient,
): Promise<L2OhlcRow[]> {
  const client = supabase ?? (await getServerSupabase());
  const cutoff = new Date(Date.now() - hours * 3600 * 1000).toISOString();
  const view = `l2_ohlc_${granularity}`;
  const { data, error } = await client
    .from(view)
    .select("asset_id, bucket_ts, open, high, low, close, sample_count")
    .eq("asset_id", assetId)
    .gte("bucket_ts", cutoff)
    .order("bucket_ts", { ascending: true });
  if (error) throw error;
  return (data ?? []) as L2OhlcRow[];
}

export async function getBookLevelsLatest(
  assetId: string,
  supabase?: SupabaseClient,
): Promise<L2BookLevel[]> {
  const client = supabase ?? (await getServerSupabase());
  // Latest 20 rows by ts desc — caller renders ladder
  const { data, error } = await client
    .from("l2_book_levels")
    .select("asset_id, ts, side, level, price, size")
    .eq("asset_id", assetId)
    .order("ts", { ascending: false })
    .limit(20);
  if (error) throw error;
  return (data ?? []) as L2BookLevel[];
}
```

```typescript
// dashboard/app/l3/[asset_id]/page.tsx — server component
import { getOhlcForAsset, getBookLevelsLatest } from "@/lib/supabase/l2-queries";
import KlineChart from "./KlineChart"; // client component (next file)

export const dynamic = "force-dynamic";
export const revalidate = 0;

export default async function L3Page({
  params,
}: { params: Promise<{ asset_id: string }> }) {
  const { asset_id } = await params;
  const assetId = decodeURIComponent(asset_id);

  let ohlc, ladder, errorMsg: string | null = null;
  try {
    [ohlc, ladder] = await Promise.all([
      getOhlcForAsset(assetId, "1m", 24),
      getBookLevelsLatest(assetId),
    ]);
  } catch (e) {
    errorMsg = e instanceof Error ? e.message : "Supabase unreachable";
    ohlc = []; ladder = [];
  }

  return (
    <main style={{ padding: 24 }}>
      <h1 style={{ fontSize: 22 }}>L3 — {assetId.slice(0, 12)}…</h1>
      {errorMsg && <div className="banner-warn">Supabase warn: {errorMsg}</div>}
      <div style={{ display: "grid",
                    gridTemplateColumns: "1fr 320px", gap: 16 }}>
        <KlineChart ohlc={ohlc!} />
        <DepthLadder rows={ladder!} />
      </div>
    </main>
  );
}

function DepthLadder({ rows }: { rows: L2BookLevel[] }) {
  // Group by side, render top-10 per side
  const bids = rows.filter(r => r.side === "BUY")
                   .sort((a, b) => a.level - b.level);
  const asks = rows.filter(r => r.side === "SELL")
                   .sort((a, b) => a.level - b.level);
  return (
    <table style={{ fontSize: 12 }}>
      {/* ... bids top-10 + asks top-10 ladder ... */}
    </table>
  );
}
```

```typescript
// dashboard/app/l3/[asset_id]/KlineChart.tsx — client component
"use client";
import { useEffect, useRef } from "react";
import type { L2OhlcRow } from "@/lib/supabase/l2-queries";

export default function KlineChart({ ohlc }: { ohlc: L2OhlcRow[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    let chart: any;
    let resizeObserver: ResizeObserver | null = null;
    (async () => {
      // Dynamic import — lightweight-charts is client-only (no SSR)
      const { createChart, CandlestickSeries } = await import("lightweight-charts");
      chart = createChart(containerRef.current!, {
        width: containerRef.current!.clientWidth,
        height: 400,
        layout: { background: { color: "#0a0a0a" }, textColor: "#888" },
        grid: { vertLines: { color: "#1a1a1a" }, horzLines: { color: "#1a1a1a" } },
        timeScale: { timeVisible: true, secondsVisible: false },
      });
      const series = chart.addSeries(CandlestickSeries, {
        upColor: "#26a69a", downColor: "#ef5350",
        wickUpColor: "#26a69a", wickDownColor: "#ef5350",
        borderVisible: false,
      });
      series.setData(
        ohlc.map(r => ({
          time: Math.floor(new Date(r.bucket_ts).getTime() / 1000),
          open: Number(r.open), high: Number(r.high),
          low: Number(r.low), close: Number(r.close),
        })),
      );
      // Resize responsiveness (Next.js 15 SSR-then-CSR safe)
      resizeObserver = new ResizeObserver((entries) => {
        const w = entries[0]?.contentRect.width;
        if (w && chart) chart.applyOptions({ width: w });
      });
      resizeObserver.observe(containerRef.current!);
    })();
    return () => {
      resizeObserver?.disconnect();
      chart?.remove();
    };
  }, [ohlc]);

  return <div ref={containerRef} style={{ width: "100%", height: 400 }} />;
}
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| TimescaleDB `time_bucket` for time-series bucketing on Supabase | `date_trunc` + window functions (or pg_partman partitioning) | 2026-04 Supabase Postgres 17 default; TimescaleDB deprecated | **Phase 05 CONTEXT D-03 must use date_trunc**; mat view 推迟到性能瓶颈出现 |
| WS per-token connection / 100-token cap | Single connection, no token cap via `assets_ids: []` + dynamic ops | Polymarket changelog 2025-05-28 | Phase 03 已 ship；Phase 05 复用 |
| lightweight-charts v4 `addCandlestickSeries(opts)` | v5 `addSeries(CandlestickSeries, opts)` | v5 GA 2025-05-09 (≈) | 使用 v5 API；调用方式改变 |
| Next.js Pages Router (`getServerSideProps`) | Next.js 15 App Router + RSC | dashboard 已用 App Router (Phase 02) | Phase 05 直接对齐 |
| Polymarket `prices-history` 全粒度可用 | closed market 退化 12h 颗粒 [CITED: py-clob-client issue #216] | 2025-12-22 issue 仍 open | OHLC backfill 不能 source-of-truth — 改成 WS 累积 (Phase 03 D-08) |

**Deprecated / outdated:**
- TimescaleDB on new Supabase projects (PG 17) — use date_trunc or pg_partman
- WS 100-token cap — removed 2025-05-28
- lightweight-charts v4 series-creation API — superseded by v5 `addSeries(SeriesType, opts)`
- `prices-history` 1m/1h granularity for closed markets — silent empty response

---

## Assumptions Log

> 本研究中的 `[ASSUMED]` 标记项，需要 user / planner 确认。

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | Supabase Pro 项目当前 Postgres 主版本是 17（CONTEXT D-03 假设是 15 with time_bucket）[ASSUMED — 实际项目版本需 `SELECT version();` 验证] | Pitfall 1 | 若实际是 PG 15，time_bucket 可用，但 EoL 2026-05 已过 — 改 date_trunc 仍是正确长期方向 |
| A2 | `book` event 频率在 prod 远低于 `price_change`，l2_book_levels 写入 ~5-10M 行/年 [ASSUMED — 实际数据待 24h soak 测出] | Pattern 2 Write throughput | 若实际 144M 行（D-07 估），Supabase Pro 8GB 仍够 1 年；但 BRIN(ts) 退化风险下调 |
| A3 | `array_agg(... ORDER BY ts ASC/DESC)[1]` 在 PG 17 上正确实现 OHLC open/close 语义 [ASSUMED — 等价于 first()/last() 聚合] | Example 1 OHLC view | 实测 RED test 失败时改用 `DISTINCT ON` + subquery 等价写法 |
| A4 | candidate refresh 60s debounce + L3 promoter 5-min cron 不会触发竞态 (D-14) [ASSUMED — 看 Pattern 5 修法] | Pitfall 5 | 若实际触发竞态，方案 1（拆 `_candidate_set` + `_l3_active_set`）必须 ship |
| A5 | `lightweight-charts@^5.2.0` 与 React 19 / Next.js 15 兼容 [VERIFIED 官方 lib 是 client-only canvas; React 版本无关；but 实际 build 流程仍要 `pnpm install` smoke] | Standard Stack | smoke test 必须在 Plan 中明列 |
| A6 | 双 token (Yes + No) 都进 L3（5 markets × 2 = 10 subscriptions）— CONTEXT Deferred 段暗示 [ASSUMED] | Phase Requirements | 若用户后续选单边 promote，l3-promote.yaml 改 `LIMIT 10` 直接选 token-level，不改架构 |
| A7 | Supabase RLS anon SELECT policy 可应用到 view（不只 base table）[ASSUMED — 实际需 `GRANT SELECT ON view TO anon` 显式赋权，Alembic 005 已含] | Example 1 | smoke test：anon key 在 dashboard 能否读取 `l2_ohlc_1m` |

---

## Open Questions

1. **L3 promoter 是独立 task 还是融入 candidate refresh?**
   - What we know: D-14 5-min vs candidate refresh 60s debounce 不同步；CONTEXT Claude's Discretion 明列
   - What's unclear: 不同步会不会形成 set 覆写竞态（Pitfall 5）
   - Recommendation: **独立 task**（清晰分层；ws_consumer 内拆 `_candidate_set` / `_l3_active_set` 解决竞态）

2. **L3 promote 是否 emit `l3.promoted` event (Postgres NOTIFY)?**
   - What we know: Phase 03 D-05 event bus 已建；B1 default-FALSE
   - What's unclear: M4 策略层尚未存在，没有 consumer
   - Recommendation: **暂不 emit**（YAGNI；M4 启动时再加 emit）

3. **OHLC view 在数据稀疏窗口要不要 forward-fill？**
   - What we know: prediction market 薄 — Phase 04.1 实证撞低活跃 T1/T2 WAITING_FOR_EVENT
   - What's unclear: 用户 K 线视觉体验偏好
   - Recommendation: **不 forward-fill**（v1 保持 sparse；教学文档解释；avoid 误导 "无活跃 = 价格不变" 假象）

4. **`l3_active` 在 candidates dashboard 表面怎么标？**
   - What we know: 4 个选项已列（§Pitfall 8）
   - What's unclear: 用户优先级
   - Recommendation: **Option C** — `l2_candidates.l3_promoted_at_ts` nullable column（最小改动 + 复用现有 mirror 写路径）

5. **L3 promoter 失败 (Supabase outage) 时是 freeze 老 set 还是 fail-loud?**
   - What we know: candidate refresh 已建 fail-soft `_last_known_markets_rows` fallback
   - What's unclear: L3 promoter 重要度更高（影响数据写入路径）
   - Recommendation: **沿用 candidate refresh 模式 — freeze 老 `_l3_active_set` + /health surface fetch_age** (chain-truth)

6. **24h soak verdict 失败后怎么办？**
   - What we know: D-12 不强求 7-day；24h 是 L3 标准；CONTEXT 明列
   - What's unclear: 24h 内 OHLC view 无行 / dashboard 空白时算 fail 吗？
   - Recommendation: **soak verdict 拆 3 子指标**：(a) `_l3_active_set` 24h 保持 ≥ 3（promote 不崩盘）；(b) `l2_book_levels` 24h 写入行数 > 0；(c) `l2_ohlc_1m` 在 5 L3 markets 中 ≥ 3 个有数据 — 3 pass 才 GREEN

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| Supabase Pro (Postgres 17) | OHLC view + l2_book_levels 写入 | ✓ (project 已有 — Phase 03 D-01 决定 stay Free + GHA cron；实际为 Free tier) | 17 (assumed; verify via `SELECT version()`) | 无 — Phase 05 不引入新 DB |
| `time_bucket` (TimescaleDB) | ❌ CONTEXT D-03 假设 | ✗ | — | `date_trunc('minute', ts)` (Postgres core) |
| `psutil>=5.9,<7` | /health process:rss_kb sub-check | ✓ (pyproject.toml runtime dep [VERIFIED]) | 5.9+ | — |
| `apscheduler.AsyncIOScheduler` | L3 promoter 5-min cron | ✓ (Phase 02 D-15 已用) | — | — |
| `websockets` 15+ concurrent send/recv | dynamic subscribe | ✓ (Phase 03 已 pin 15+) | 15+ | — |
| `lightweight-charts@^5.2.0` | K 线 dashboard | ✗ (需 `pnpm add`) | 5.2.0 | — |
| Vercel build + deploy | `/l3/[asset_id]` 上线 | ✓ (Phase 02 已建) | — | — |
| Fly polyarb-l2 1GB VM | promoter + book_levels writer 同进程 | ✓ (Phase 02 D-23 streaming 后充足) | — | — |

**Missing dependencies with no fallback:** None blocking.
**Missing dependencies with fallback:**
- `time_bucket` → `date_trunc` (上文 Pitfall 1)
- `lightweight-charts` → 待 install（不阻塞 plan，executor 装）

---

## Validation Architecture

> nyquist_validation 默认 enabled（`.planning/config.json` 未显式 false）。本节由 orchestrator 生成 VALIDATION.md 时使用。

### Test Framework

| Property | Value |
|---|---|
| Framework | pytest（[VERIFIED: pyproject.toml — pytest>=7]）+ dashboard pnpm test (Vitest, if added) |
| Config file | `pytest.ini` / `pyproject.toml [tool.pytest.ini_options]` |
| Quick run command | `pytest tests/m1-perception/ -x -k phase_05` |
| Full suite command | `pytest tests/m1-perception/ -x` + `cd dashboard && pnpm typecheck && pnpm build` |

### Phase Requirements → Test Map

| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|---|---|---|---|---|
| PHASE05-R01 | L3 promoter 5-min cron 调度 + scanner-recipe 跑通 | unit | `pytest tests/m1-perception/test_l3_promoter.py::test_promote_run_top_5 -x` | ❌ Wave 0 |
| PHASE05-R01 | L3 promote 阈值 (D-13) filter 行为正确 | unit | `pytest tests/m1-perception/test_l3_promoter.py::test_yaml_thresholds_applied -x` | ❌ Wave 0 |
| PHASE05-R02 | `_book_levels_rows_from_frame` 投 20 行 | unit | `pytest tests/m1-perception/test_l2_main_book_levels.py::test_book_levels_rows_top10 -x` | ❌ Wave 0 |
| PHASE05-R02 | `mirror.push_book_levels` fail-soft envelope | unit | `pytest tests/m1-perception/test_l2_supabase_mirror_book_levels.py -x` | ❌ Wave 0 |
| PHASE05-R02 | Alembic 005 forward + reverse | integration | `make supabase-migrate-test` (新 target) | ❌ Wave 0 |
| PHASE05-R03 | `date_trunc` OHLC view 返回正确 open/high/low/close | integration | `pytest tests/m1-perception/test_alembic_005_ohlc_views.py::test_ohlc_1m_open_close -x` | ❌ Wave 0 |
| PHASE05-R03 | OHLC view 在 sparse 数据下不报错（≥1 行即 GREEN） | integration | `pytest tests/m1-perception/test_alembic_005_ohlc_views.py::test_sparse_data -x` | ❌ Wave 0 |
| PHASE05-R04 | `ws_consumer.add_subscriptions` 发送正确 payload | unit | `pytest tests/m1-perception/test_ws_consumer_dynamic_subscribe.py::test_add_sends_subscribe -x` | ❌ Wave 0 |
| PHASE05-R04 | `ws_consumer.remove_subscriptions` 同款 | unit | `pytest tests/m1-perception/test_ws_consumer_dynamic_subscribe.py::test_remove_sends_unsubscribe -x` | ❌ Wave 0 |
| PHASE05-R04 | GAP-401 watchdog liveness gate 仍 intact (regression) | unit | `pytest tests/m1-perception/test_ws_watchdog_liveness.py -x` (existing) | ✅ (Phase 04.1 quick task 260531) |
| PHASE05-R05 | dashboard `/l3/[asset_id]` 渲染 K 线 + ladder | smoke (manual or playwright) | `make smoke-l3-dashboard` (新 target) | ❌ Wave 0 |
| PHASE05-R06 | promoter task 不阻塞 ws_consumer / health endpoint | integration | `pytest tests/m1-perception/test_l2_daemon_integration.py::test_promoter_concurrent_with_ws -x` | ❌ Wave 0 |
| PHASE05-R07 | `/health` 含 3 个新 L3 sub-checks | unit | `pytest tests/m1-perception/test_l2_health_l3_subchecks.py -x` | ❌ Wave 0 |
| PHASE05-R08 | 24h prod soak: `_l3_active_set` 保持 ≥3 + l2_book_levels >0 行 + ≥3 markets 有 OHLC | manual (verifier) | `make ohlc-spot-check` (新 target) + flyctl logs grep | ❌ Wave 0 |

**Observable patterns** (chain-truth — write-side actually mutates):
- `l3:active_count` reads `len(l3_promote._l3_active_set)`
- `l3:last_promote_at_s` reads `l3_promote._last_promote_at_s` — mutated by `promote_run` success
- `l3:last_book_levels_write_at_s` reads `l3_promote._last_book_levels_write_at_s` — mutated by `mirror.push_book_levels` success
- 不门控在 config flag！

### Sampling Rate

- **Per task commit**: `pytest tests/m1-perception/test_<task_file>.py -x` (< 30s)
- **Per wave merge**: `pytest tests/m1-perception/ -x` + `cd dashboard && pnpm typecheck`
- **Phase gate**: Full suite green + dashboard build green + 24h prod soak verdict GREEN

### Wave 0 Gaps

- [ ] `tests/m1-perception/test_l3_promoter.py` — covers PHASE05-R01
- [ ] `tests/m1-perception/test_l2_main_book_levels.py` — covers PHASE05-R02 frame projector
- [ ] `tests/m1-perception/test_l2_supabase_mirror_book_levels.py` — covers PHASE05-R02 mirror
- [ ] `tests/m1-perception/test_alembic_005_ohlc_views.py` — covers PHASE05-R02 schema + R03 view
- [ ] `tests/m1-perception/test_ws_consumer_dynamic_subscribe.py` — covers PHASE05-R04
- [ ] `tests/m1-perception/test_l2_daemon_integration.py` (extend existing) — covers PHASE05-R06
- [ ] `tests/m1-perception/test_l2_health_l3_subchecks.py` — covers PHASE05-R07
- [ ] dashboard smoke target — `make smoke-l3-dashboard` (covers PHASE05-R05)
- [ ] `make ohlc-spot-check` Makefile target — covers PHASE05-R08

*(共 9 Wave 0 gaps；test framework + fixture infrastructure 已存在，Phase 04.1 Plan 02 同款体例。)*

---

## Security Domain

> security_enforcement default-enabled.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---|---|---|
| V2 Authentication | yes (admin endpoint 若需) | HMAC of body via existing `ControlAuthMiddleware` (Phase 02.1 D-03/D-04) — do not reinvent |
| V3 Session Management | no | (stateless backend; dashboard 走 Supabase Auth — Phase 02 已建) |
| V4 Access Control | yes | Supabase RLS anon SELECT on l2_book_levels + 3 OHLC views (Alembic 005 already enforces) |
| V5 Input Validation | yes | scanner 4-layer SQL defense for l3-promote.yaml (复用 Phase 01.1 — no new); asset_id URL param 走 `decodeURIComponent` + 不直传 SQL |
| V6 Cryptography | no | (no new crypto — 复用 Phase 02 HMAC pattern; lightweight-charts 是 client lib) |

### Known Threat Patterns for L3 stack

| Pattern | STRIDE | Standard Mitigation |
|---|---|---|
| SQL injection through l3-promote.yaml threshold copy-paste | Tampering | scanner 4-layer SQL defense (Phase 01.1) — `_FORBIDDEN` regex + `_ORDER_BY_OK` allowlist + `_validate_limit` + sqlite ro URI (or Supabase service_role read-only role) |
| asset_id URL param injection on `/l3/[asset_id]` | Tampering | Supabase RPC 不接 raw asset_id 入 SQL — supabase-py `.eq("asset_id", x)` 是 parameterized binding by design [CITED: supabase-py docs] |
| dashboard /l3 page leaking service_role data | Information Disclosure | l2-queries.ts L3-L9 invariant — ONLY anon key; RLS gates write surface |
| WS payload "operation": "subscribe" sent on wrong connection | Tampering / Repudiation | `_current_ws` reset to None on disconnect (already done for GAP-401); `add_subscriptions` returns False on no-ws |
| L3 promoter run mid-storm overwhelming WS | Denial of Service | 5-min cron interval + max diff size bounded by N=5 markets × 2 token = 10 subs/unsubs per run; rate well below WS server limits |
| Dashboard XSS via market metadata | XSS | Next.js 15 React 19 auto-escapes string interpolation; no `dangerouslySetInnerHTML` used; lightweight-charts canvas-only (no DOM injection) |
| `time_bucket` SQL error leaking server stack | Information Disclosure | Pitfall 1 — 在 Plan 阶段就修，不让 prod 上 view 报错 |

---

## Sources

### Primary (HIGH confidence)

- `src/polyarb/clients/ws_market_client.py` (本仓库) — WS client 实际 API
- `src/polyarb/daemon/ws_consumer.py` (本仓库) — WsConsumer state machine + GAP-401 liveness gate
- `src/polyarb/daemon/l2_main.py` (本仓库) — dispatcher 集成点 + 现有 builders
- `src/polyarb/storage/l2_supabase_mirror.py` (本仓库) — mirror envelope 模板
- `src/polyarb/observation/scanner.py` + `l2_candidate_refresh.py` (本仓库) — scanner 4-layer defense + temp-DB pattern
- `src/polyarb/http/l2_health.py` (本仓库) — chain-truth /health 模板
- `alembic/versions/003_l2_tables.py` + `004_add_yes_token_id.py` (本仓库) — Alembic 体例
- `.planning/threads/market-observation-architecture.md` §1 / §1.6 / §2.2 / §2.6 — WS Q1-Q5 + DB tier
- `.planning/workstreams/m1-perception/phases/04.1-d01-restart-robustness-chaos-redesign/04.1-CONTEXT.md` — D-03 stale_s=30 LOCKED
- `.planning/quick/260531-gap-401-watchdog-false-trip/SUMMARY.md` — GAP-401 liveness gate
- Supabase TimescaleDB deprecation docs: https://supabase.com/docs/guides/database/extensions/timescaledb (fetched 2026-06-01)
- Polymarket WS Market Channel: https://docs.polymarket.com/api-reference/wss/market (cited via thread §2.2)
- Lightweight Charts v5 official docs: https://tradingview.github.io/lightweight-charts/docs (fetched 2026-06-01)
- npm `lightweight-charts@5.2.0` published 2026-04-24 [VERIFIED: npm view]

### Secondary (MEDIUM confidence)

- Supabase Postgres 17 release notes discussion: https://github.com/orgs/supabase/discussions/35851
- TimescaleDB time_bucket not exist on Supabase: https://github.com/orgs/supabase/discussions/23365
- Crunchy Data BRIN indexes: https://www.crunchydata.com/blog/postgresql-brin-indexes-big-data-performance-with-minimal-storage
- Medium PostgreSQL OHLC pattern: https://medium.com/elpassion/how-to-create-candlestick-charts-with-postgresql-80cb89893af2
- Polymarket prices-history 12h bug: https://github.com/Polymarket/py-clob-client/issues/216
- nevuamarkets/poly-websockets (book event schema): https://github.com/nevuamarkets/poly-websockets
- PolyChart Chrome ext (lightweight-charts real-world Polymarket use): https://chromewebstore.google.com/detail/polychart-candlestick-cha/gdfcfkbghfcpbepdeehfnofadgmjbmio

### Tertiary (LOW confidence)

- 各 OSS Polymarket charting repo (samples only, not authoritative): polymarket-trade-tracker, PolyWorld

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — npm-verified lightweight-charts 5.2.0; psutil/asyncpg/websockets 现有 prod-verified
- Architecture: HIGH — 直接 trace 现有 Phase 03/04 code，集成点明确；唯一 NEW 模式（dynamic WS subscribe）有 websockets 文档 + 协议规范双重支撑
- OHLC view perf on 26M rows/年: MEDIUM — 推论自 PG BRIN + date_trunc 公开 benchmark，未本地实测 prod-scale load
- L3 promote churn rate: LOW — depends on D-13 阈值实际命中率 + 市场动态，prod 24h 才有数据
- Pitfall 1 (time_bucket on PG 17): HIGH — Supabase 官方文档 explicit deprecation
- Pitfall 2 (WS client 无 send-after-connect): HIGH — 代码全 trace 确认

**Research date:** 2026-06-01
**Valid until:** 2026-07-01（30 天；Supabase / Polymarket / Next.js 都是 fast-moving，>30 天前先重新 verify TimescaleDB / lightweight-charts 状态）

---

*Phase: 05-ws-book-prices*
*Researcher confidence: HIGH (core) / MEDIUM (perf estimates)*
