# Phase 05: WS /book + /prices 增量推送 — Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-06-01
**Phase:** 05-ws-book-prices
**Areas discussed:** Scope / L3 trigger / OHLC strategy / Book depth / Top-N / Bars granularity / Levels count / Done definition / Rule location / Table namespace / Dashboard / WS subscription / Thresholds / Recompute freq / Carry-over coordination / Deploy target

---

## Round 1 — Core architecture (4 gray areas)

### Scope 收口

| Option | Description | Selected |
|--------|-------------|----------|
| **L3 升级** | Phase 05 = L2→L3 跨层升级: full book depth + tick 历史持久化 + OHLC 聚合 + L3 候选锁定机制。工程量大但金字塔顺序对 | ✓ |
| WS coverage 收口 + thread §2.2 验证 | 把现有 WS 通路补齐 (full book depth 落库 + initial_dump 后台校验 + WS 通路冗余监控), 不做 OHLC | |
| 纯 OHLC 数据源 (Polymarket /prices-history REST) | 不做 WS 增量, 只做 K 线: REST /prices-history 拉历史 + DB view 聚合 minute/hour K 线 | |

**User's choice:** L3 升级
**Notes:** Phase 03 已建 WS 全链路 (price_change/best_bid_ask/last_trade_price/book → l2_top_of_book + l2_trades), 字面标题已被覆盖。ROADMAP "作为 L3 单市场 K 线的数据源" 指向 L3 升级才是真目标。

### L3 候选选择 / 跟踪触发

| Option | Description | Selected |
|--------|-------------|----------|
| **L2 信号自动 promote** | L2 候选扫描 + 简单规则 (spread<2c, depth>$500, recent trade) 自动选 top-N markets 进 L3。事件驱动, 不用人盯 | ✓ |
| 手工 watchlist (YAML / Supabase 表) | 用户在 watchlist.yaml 或 Supabase 表里手填 condition_id, daemon 起来时读 | |
| Dashboard 'promote' 按钮 | Vercel dashboard 加按钮, click 后 HMAC POST 到 daemon 加订阅 | |
| 暂时不做 (out of Phase 05 scope) | Phase 05 只做数据通道, 跟踪触发推到下个 phase | |

**User's choice:** L2 信号自动 promote

### OHLC (K 线) 怎么生产

| Option | Description | Selected |
|--------|-------------|----------|
| **SQL window view on l2_top_of_book** | Postgres regular/materialized view 用 time_bucket 聚合现有数据, 1m/5m/1h K 线由 view 算出。零新进程 | ✓ |
| 独立 aggregator 进程实时累积 | daemon 内新 task 订阅 last_trade_price 实时累积 OHLC bar, 每 1m flush 到 l2_ohlc 表。精度高但多一个状态机 | |
| REST /prices-history 拉历史 backfill + WS 追加 | 用 REST 拉历史 12h/1m, WS 增量追加。冷启动友好但 REST history 在 closed market 退化到 12h 颗粒度 | |
| 暂时不做 OHLC | Phase 05 只做数据落库, K 线推到下个 phase | |

**User's choice:** SQL window view on l2_top_of_book (推荐起步)

### Full book depth 持久化

| Option | Description | Selected |
|--------|-------------|----------|
| **新表 l2_book_levels 存 top-10 levels** | 新表 (asset_id, ts, side, level, price, size), top-10 levels 每边。深度信号 (depth>$500) 能算, 写量可控 ~10x of l2_top_of_book | ✓ |
| 存完整 array as JSONB | l2_top_of_book 加 bids_jsonb / asks_jsonb 列, 每条 book event 整 dump。最简但查询要 JSON 操作, BRIN 索引帮不上 | |
| 不存深度, 计算 depth-at-price 时实时调 REST /book | DB 只存 top-of-book, 真要算深度时回拉 CLOB /book | |
| 只在 'L3 锁定' market 上存深度 | L2 候选只存 top-of-book; L3 升级后的 1-5 markets 才落 full depth。两套写路径 | |

**User's choice:** 新表 l2_book_levels 存 top-10 levels

---

## Round 2 — Engineering parameters (4 follow-ups)

### Top-N (L3 锁定集大小)

| Option | Description | Selected |
|--------|-------------|----------|
| **N=5** | 贴 thread §1 金字塔 ("L3 = 锁定 1-5 markets")。双-token (Yes+No) 是 10 tokens, WS 订阅量可控 | ✓ |
| N=10 | 双倍规模, 看到更多候选。WS 订阅 20 tokens, 还在 §2.2 'no limit' 安全区 | |
| N 可配 (默认 5) | 环境变量 POLYARB_L3_TOP_N, dev=5 / prod 可调。弹性但多一个 tune 点 | |

**User's choice:** N=5

### OHLC bars 粒度

| Option | Description | Selected |
|--------|-------------|----------|
| **1m + 5m + 1h regular view** | 三个 regular (non-materialized) view, time_bucket 实时算。SQL 写一次, 查询时才 run, 实时性最高 | ✓ |
| 1m + 5m + 1h materialized view + cron refresh | materialized view + Supabase pg_cron 每分钟 refresh。查询快但带 1-min lag 且要装 pg_cron extension | |
| 只做 1m 先跳通 (最小) | 单 1m view, 其它粒度下个 phase。最快交付 MVP | |

**User's choice:** 1m + 5m + 1h regular view

### Book levels 存几级

| Option | Description | Selected |
|--------|-------------|----------|
| top-5 每边 (10 rows/snapshot) | 每个 book event 写 10 行。写量 = l2_top_of_book 的 10x。足够算 'depth at top-5' 信号 | |
| **top-10 每边 (20 rows/snapshot)** | 20 行。够算深度信号 + flash-crash detection (level 6-10 出现代表主动挂单) | ✓ |
| top-20 每边 (40 rows/snapshot) | 40 行。Polymarket book event 原生带多层, 全落。写量 ×20, prod ~144M rows/年 | |
| 全部存 (变长) | 不限制 levels, 全赔进去。最丰富但 BRIN 指定 level 查询会退化 | |

**User's choice:** top-10 每边 (20 rows/snapshot)

### Phase 05 完成判定 (Goal MET)

| Option | Description | Selected |
|--------|-------------|----------|
| **5 L3 markets 在 prod 跑 24h, OHLC view 返回数据, depth 入库, dashboard 能画 K 线** | 实打门: prod deploy + soak window 验证 + UI 能看。包含 Vercel dashboard 加一页 (复用 Phase 02 架构) | ✓ |
| 5 L3 markets 跑通 + DB 有数 (不要求 dashboard) | 仅要求后端闭环, K 线可查 but 不画。Dashboard 另起 phase | |
| 单元/集成测试 验证、不走 prod chaos | TDD-only verify。快交付 but 不能算 'L3 生产级' (§1 纪律) | |

**User's choice:** 5 L3 markets 在 prod 跑 24h, OHLC view 返回数据, depth 入库, dashboard 能画 K 线

---

## Round 3 — Implementation details (4 follow-ups)

### L3 promote 规则表达

| Option | Description | Selected |
|--------|-------------|----------|
| **复用 Phase 01.1 scanner recipe 框架** | 写一个 l3-promote.yaml recipe (复用 scan_recipes 机制), SQL 取 L2 上 spread<2c & depth>$500 & last_trade<60min top-5。与现有沉淀一致 | ✓ |
| 硬编码 Python 函数 in l2_promote.py | 独立模块 compute_l3_candidates() 返回锁定集。更快但脱离 scanner 体系 | |
| Supabase view + L3 daemon 轮询 | Postgres view 'l3_candidates' 封装规则, daemon 每 5min 读。贴 DB-as-source-of-truth 但 view 同步问题 | |

**User's choice:** 复用 Phase 01.1 scanner recipe 框架

### L3 存不存独立表

| Option | Description | Selected |
|--------|-------------|----------|
| **统一以 l2_* 命名, 靠 promote 表区分** | 仅 l2_book_levels + l2_ohlc_1m view + l2_ohlc_5m view + l2_ohlc_1h view。'L3-only' 是写入策略不是表名。可避免 schema fragmentation | ✓ |
| 新 l3_* namespace | l3_book_levels / l3_ohlc_*。金字塔明确分层 但 schema 双套 | |
| L2 独立 depth 表 + L3 view aggregated | depth 在 l2_book_levels (all promoted markets), OHLC view L3-scoped。折中 | |

**User's choice:** 统一以 l2_* 命名

### Dashboard 新页

| Option | Description | Selected |
|--------|-------------|----------|
| **/l3/[asset_id] 动态页 K 线 + depth ladder** | 每个 L3 market 一个页面。K 线用 1m view, 右侧 top-10 depth ladder。复用现有 Next.js + Supabase RLS anon-read | ✓ |
| 单页 /l3 带 5 markets 列表 + 右侧详情面板 | 一个页抱清单 + 详情。SPA 感, 但状态多 | |
| 仅加 candidates 页一个 'L3 promoted' 标签 | 不单独加页, 现有页加标签 → click 跳到外部/{asset_id} = static plot URL。最快交付 | |

**User's choice:** /l3/[asset_id] 动态页 K 线 + depth ladder

### WS 订阅架构

| Option | Description | Selected |
|--------|-------------|----------|
| **现有 ws 动态 subscribe 加 L3 token** | L3 candidate 推进后, 复用 thread §2.2 Q1 已记的 dynamic subscribe payload 动态加订。不重连不开新连接 | ✓ |
| 独立 L3 WS 连接 | L3 daemon 另起 ws_consumer, full-depth subs 走这条。隔离性好但 +1 连接状态机 | |
| L3 全量重连 | L3 candidate 变动 → close ws 重建。简但重连期间丢事件 | |

**User's choice:** 现有 ws 动态 subscribe 加 L3 token

---

## Round 4 — Final parameters & coordination (4 follow-ups)

### Promote 阈值具体值

| Option | Description | Selected |
|--------|-------------|----------|
| **完全采用 baseline** | spread<0.02 & depth_yes_usd>500 & last_trade_ts > now()-3600s, ORDER BY depth DESC LIMIT 5。v1 baseline, prod 看效果后调 | ✓ |
| 阈值 env-tunable | POLYARB_L3_SPREAD_MAX / POLYARB_L3_DEPTH_MIN / POLYARB_L3_RECENCY_SEC 三个 env。弹性但同 CLAUDE.md "experiment values never touch baseline defaults" 需 default = baseline | |
| promote rule 全部在 yaml recipe (不加 env) | scan_recipes/l3-promote.yaml 里写完整 SQL, 调参 = 改 yaml + commit。加 audit 迹但不能运行期调 | |

**User's choice:** 完全采用 baseline (作为 v1 baseline)

### L3 promote 频率

| Option | Description | Selected |
|--------|-------------|----------|
| **5 min** | L2 是分钟级, 5min recompute 足够, 不会在 promoted 集上 flapping。sched_interval cron 复用 | ✓ |
| 1 min | 更敏感, 但 last_trade<60min 阈值下集合变化慢, 提高频率收益不大 | |
| 15 min | 保守, L3 锁定中不频繁 churn。适合低活跃市场 | |
| 仅在 snapshot_complete 事件后重算 | 复用 Phase 03 asyncpg LISTEN/NOTIFY 事件。事件驱动 但频率 = L1 snapshot 频率 (现在 ~5min) | |

**User's choice:** 5 min

### 与未 deploy 代码的关系

| Option | Description | Selected |
|--------|-------------|----------|
| **不等, Phase 05 独立推进** | 两块未 deploy 都是 happy-path 等价、向后兼容。Phase 05 plan/execute 都在 main 上干, 下次 L2 deploy 会一起上 prod。Phase 05 是代码主线, 不被阻 | ✓ |
| 先 deploy + 验证 GAP-401 修复, 再起 Phase 05 | 先拿下一个安静窗口 ship + 复验 watchdog 不再 false-trip, 才进 Phase 05。护航但阻 chain | |
| Phase 05 plan 阶段加一个 'Wave-0 deploy + verify' 前置 | 进 Phase 05 但第一阶段是 deploy 2 块 + soak 查验, 后面才动 schema。中间路径 | |

**User's choice:** 不等, Phase 05 独立推进

### L3 deploy target

| Option | Description | Selected |
|--------|-------------|----------|
| **同进程作为 polyarb-l2 的一个 asyncio task** | L3 promoter + book_levels writer + ohlc view 全部住在 polyarb-l2 fly app。复用 ws_consumer + supabase_mirror, 零新运维面。1GB Fly VM 够 | ✓ |
| 独立 polyarb-l3 fly app | L3 独立 隔离。多一个 app 要管, 费用 ×2 (多一个 Fly VM), 但隔离清晰 | |
| 本地跑 L3 daemon, prod 只 L2 | L3 是研发环境产物, prod 仅 ws + l2_top_of_book。快交付 但与 §1 '生产级' 纪律冲突 | |

**User's choice:** 同进程作为 polyarb-l2 的一个 asyncio task

---

## Round 5 — Final 'anything else' check

### Phase 05 灯区还有什么要拿上桌

| Option | Description | Selected |
|--------|-------------|----------|
| **不再问, 写 CONTEXT.md** | 决策足够, plan-phase 可以沉下去 (research 取 thread §2.2 + 003_l2_tables.py 为 baseline)。决策包含 16 个 D-XX | ✓ |
| 补问 Yes/No token 双边处理 | L3 锁 5 markets = 10 tokens 还是 5 tokens? 是否双 token 及 depth 都足才入 L3? | |
| 补问 备份 / 回填 逻辑 | L3 promoter 重启后, OHLC view 是否补历史? Phase 03 GAP-301 类 cold-start 变体? | |
| 补问 Vercel dashboard 路由供能状态 | Phase 02 EMAIL_WHITELIST + Phase 04 未验证状态对 /l3 路由的影响, 是否要进 Phase 05 scope | |

**User's choice:** 不再问, 写 CONTEXT.md

---

## Agent's Discretion (delegated to downstream agents)

- Promote 规则 SQL 的具体写法 (yaml 内嵌 SQL vs 引用 .sql 文件) — planner/executor 看 scan_recipes 现有体例决定
- L3 promoter task 是独立 asyncio task 还是融进 `l2_candidate_refresh` — executor 看 Phase 03 D-05 event bus 设计决定
- Dashboard K 线库 (lightweight-charts vs recharts vs uPlot) — researcher 调研后定
- `l2_book_levels` 索引设计 (PK 是 `(asset_id, ts, side, level)` 还是 surrogate id+UNIQUE) — planner 与 Phase 03 003_l2_tables.py 风格一致
- `l3_candidates` 是表还是 view 还是 daemon 内存态 — executor 定具体形式
- soak 期间 OHLC 数据真假对照 (用 REST `/prices-history` 验证 mid 累积出来的 K 线 vs 平台 K 线) — verifier 在 24h 后做 spot check

## Deferred Ideas

- Yes/No 双 token L3 处理细节 (v2 optimization)
- 历史回填 / cold-start backfill via REST `/book` (Phase 06 待研究)
- Vercel deployment protection 对 /l3 路由 (Phase 02 EMAIL_WHITELIST 限制)
- 多 OHLC 粒度 (15m / 4h / 1d) — Phase 06+
- Materialized view + pg_cron (查询慢再升级)
- `prices-history` REST 拉历史 K 线 backfill (Phase 06)
- L3 信号策略 (M4 workstream)
- Promote 阈值动态调整 prod adaptive (v2)
