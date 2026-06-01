# Phase 05: WS /book + /prices 增量推送 — Context

**Gathered:** 2026-06-01
**Status:** Ready for planning
**Workstream:** m1-perception

<domain>
## Phase Boundary

**Phase 05 = L2 → L3 跨层升级**: 把 Phase 03 已落地的 WS market 通道 (price_change / best_bid_ask / last_trade_price / book) 从「L2 top-of-book 分钟级」推进到「L3 单市场完整深度 + tick 历史 + OHLC K 线 + 自动 promote 锁定 top-5 markets」。

**Scope 重定义 (D-01)**: 字面标题 "WS /book + /prices 增量推送" 已被 Phase 03 ws_consumer + l2_main 覆盖 (`price_change` / `best_bid_ask` / `last_trade_price` / `book` 全部 4 个 event types 在跑)。ROADMAP 描述 "作为 L3 单市场 K 线的数据源" 才是真目标。本 phase 围绕 L3 升级展开:

**In-scope**:
- 自动 promote 机制 (L2 candidates → top-5 L3 锁定集) 复用 scanner recipe 框架
- Full book depth 持久化 (top-10 levels/边) 到新表 `l2_book_levels`
- OHLC K 线 (1m/5m/1h regular views on `l2_top_of_book`)
- L3 promote 状态机 (5-min recompute, env-tunable 阈值)
- Dashboard `/l3/[asset_id]` 动态页 (K 线 + depth ladder, 复用 Phase 02/03 Vercel 架构)
- L3 daemon 复用 polyarb-l2 fly app, 同进程 asyncio task
- Prod 24h soak: 5 L3 markets, OHLC view 返回数据, depth 入库, dashboard 能画 K 线

**Out-of-scope** (推到下一 phase):
- 多 OHLC 粒度 (15m/4h/1d)
- 历史 backfill via REST `/prices-history` (closed market 退化到 12h 精度, Phase 06 待研究)
- L3 信号策略 (M4 workstream)
- Materialized view + pg_cron (regular view 起步, 量大再升级)

</domain>

<decisions>
## Implementation Decisions

### Scope & Boundary
- **D-01 (Scope)**: Phase 05 = L2 → L3 升级 (full depth + tick + OHLC + 自动 promote)。WS 字面通路在 Phase 03 已实现, 本 phase 在其上做"金字塔上一层"扩展, **不是重写 WS plumbing**。
- **D-12 (Done definition)**: Goal MET 门槛 = 5 L3 markets 在 prod 跑 24h + OHLC view 返回数据 + book depth 入库 + dashboard `/l3/[asset_id]` 能画 K 线。soak window 不强求 7 天 (区别于 Phase 02 L1 生产级判定), 24h 已足够验证 L3 通路 end-to-end。

### L3 Promote (候选锁定机制)
- **D-02 (Trigger)**: L2 信号自动 promote — 通过 SQL 规则查 `l2_top_of_book` 自动选 top-N markets, 不用人盯。
- **D-05 (Top-N)**: **N=5**, 贴 thread §1 金字塔 ("L3 = 锁定 1-5 markets")。double-token (Yes+No) = 10 token subscriptions, 仍在 §2.2 Q1 "WS no-limit since 2025-05-28" 安全区。
- **D-09 (Rule location)**: 复用 Phase 01.1 scanner recipe 框架。新建 `src/polyarb/scan_recipes/l3-promote.yaml` (SQL 写在 yaml 里), 与 6 个现有 recipe 共栈, 走相同的 4 层 SQL injection defense。
- **D-13 (Thresholds, v1 baseline)**:
  - `spread < 0.02` (USD)
  - `depth_yes_usd > 500`
  - `last_trade_ts > now() - 3600s` (recent_trade in 60min)
  - `ORDER BY depth_yes_usd DESC LIMIT 5`
  - prod 看效果后调; 不进 env (符合 CLAUDE.md "experiment values never touch baseline defaults" — yaml 改 = audit trail; env 改会污染 default path)
- **D-14 (Recompute frequency)**: **5 min** — 与 L2 粒度一致, last_trade<60min 门槛下 churn 慢, 5min 足够; 减少 promoter task 复杂度。复用 Phase 02 AsyncIOScheduler cron 模式。

### OHLC (K 线生产)
- **D-03 (Strategy)**: **SQL window view on `l2_top_of_book`** 起步。复用 Postgres `time_bucket` (含在 Supabase Pro 默认 ext), 不引 TimescaleDB。零新进程, 精度 = L2 写入频率 (~分钟级)。
- **D-06 (Bars/granularity)**: **1m + 5m + 1h regular (non-materialized) view**。三个 view 是 SQL 写一次, 查询时 run, 查询响应 sub-100ms (thread §2.6 实测 PG 10M 行调优负载)。
  - 不用 materialized view 避免 pg_cron extension 依赖 + 1-min lag
  - View 命名: `l2_ohlc_1m` / `l2_ohlc_5m` / `l2_ohlc_1h`
  - 数据源 = `l2_top_of_book.mid_price` (有 best_bid + best_ask 算出的 mid)

### Full Book Depth 持久化
- **D-04 (Strategy)**: 新表 `l2_book_levels` 存 top-10 levels/边。
- **D-07 (Level count)**: **top-10 每边 = 20 rows/snapshot**。可算 depth-at-price + flash-crash detection (level 6-10 出现代表主动挂单)。写量 ~ l2_top_of_book × 20, prod ~144M rows/年 在 Supabase Pro 8GB compute 容量内。
- **D-10 (Table namespace)**: 统一 `l2_*` 命名 (l2_book_levels), **不另起 `l3_*` namespace**。L3 vs L2 区别是「写入策略」(L3 = promoted markets 才写 depth) 不是「表名前缀」。避免 schema fragmentation; `l3_candidates` 决定哪些 asset_id 走 depth 写路径, 表只一份。

### WS 订阅 (Phase 03 客户端复用)
- **D-11 (WS subscription)**: 现有 `ws_market_client` 动态 subscribe 加 L3 token。
  - 复用 thread §2.2 Q1 已记的 `{"operation": "subscribe", "assets_ids": [...]}` 动态加订 (无需重连)
  - L3 promoter task 每 5min 计算 new set, diff 出 add/remove tokens, send subscribe/unsubscribe payload
  - 不开新 WS 连接, 不重连, **不动 Phase 04.1 已修的 watchdog liveness gate** (那是另一个独立约束)

### Dashboard
- **D-08 (UI scope)**: `/l3/[asset_id]` 动态页 (Next.js App Router dynamic segment)
  - 主区: OHLC K 线 (从 `l2_ohlc_1m` view 取数据, 用 lightweight-charts 或类似)
  - 右侧: top-10 depth ladder (从 `l2_book_levels` 取最新 ts 的 20 行)
  - List 入口在 candidates 页加 "L3 promoted" 标签 → click 进 `/l3/[asset_id]`
  - 复用 Phase 02/03 已建的 lib/supabase server-client split + RLS anon-read pattern

### Deploy & Runtime
- **D-15 (Deploy target)**: 同进程作为 polyarb-l2 fly app 的一个 asyncio task。
  - L3 promoter + book_levels writer 都在 polyarb-l2 内部, 复用现有 ws_consumer + supabase_mirror
  - 1GB Fly VM (Phase 02 D-23 streaming 后充足) + 现有 polyarb-l2 secret 集合 — 零新运维面
  - 避免新起 polyarb-l3 fly app 的成本 + 隔离开销
- **D-16 (Carry-over coordination)**: Phase 05 不等未 deploy 的 2 块代码 (04.1 code-review fixes WR-02/03/IN-01 + GAP-401 watchdog liveness gate)。两块都是 happy-path 等价 / 向后兼容, 随下次 L2 deploy 一起上 prod。Phase 05 是代码主线, 在 main 上干, 不被阻。
  - GAP-401 prod 复验仍 carry-over 到下次安静窗口观察 (与本 phase 解耦)。

### Agent's Discretion
- Promote 规则 SQL 的具体写法 (yaml 内嵌 SQL vs 引用 .sql 文件) — planner/executor 看 scan_recipes 现有体例决定
- L3 promoter task 是独立 asyncio task 还是融进 `l2_candidate_refresh` — executor 看 Phase 03 D-05 event bus 设计决定
- Dashboard K 线库 (lightweight-charts vs recharts vs uPlot) — researcher 调研后定, 倾向 lightweight-charts (TradingView 系, 体积小, prod 用得多)
- `l2_book_levels` 索引设计 (PRIMARY KEY 是 `(asset_id, ts, side, level)` 还是 surrogate id+UNIQUE constraint) — planner 与 Phase 03 003_l2_tables.py 风格一致
- `l3_candidates` 是表还是 view — 用户在 D-09 round 1 看了三选项, 选 yaml recipe (不是 view), 这里 candidates 应是 daemon 内存态 / 临时表, executor 定具体形式
- soak 期间 OHLC 数据真假对照 (用 REST `/prices-history` 验证 mid 累积出来的 K 线 vs 平台 K 线) — verifier 在 24h 后做 spot check, 不在 plan 必须项

### Folded Todos
(none — m2 carry-over 不属于本 phase scope; GAP-401 prod 复验是 deploy 协调事项不是本 phase 工作)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture & Decisions (跨 phase)
- `.planning/threads/market-observation-architecture.md` §1 (L1/L2/L3 金字塔生产级判定) — Phase 05 的目标定义
- `.planning/threads/market-observation-architecture.md` §1.5 (框架抽象 A/B/C) — 统一市场状态 + 时序后端 + 事件总线
- `.planning/threads/market-observation-architecture.md` §2.2 (Polymarket WS 调研 Q1-Q5) — `assets_ids` 数组 / dynamic sub-unsub / staleness watchdog / heartbeat / silent freeze pitfall / rate limits
- `.planning/threads/market-observation-architecture.md` §2.6 (DB tier selection) — Supabase Pro 8GB 容量约束, TimescaleDB 不必要论证
- `.planning/threads/market-observation-architecture.md` §1.6 (chain-truth discipline) — fail-soft 必须 surface 到 /health

### Phase 03 落地 (代码 baseline)
- `src/polyarb/clients/ws_market_client.py` — 现有 WS 客户端, `stream_market_events` async iterator, 4 MiB max_size, on_connect callback (Phase 04.1 SESSION 33)
- `src/polyarb/daemon/ws_consumer.py` — 消费者闭包 + watchdog liveness gate (GAP-401 SESSION 33)
- `src/polyarb/daemon/ws_watchdog.py` — staleness 30s + liveness_check (GAP-401 fix)
- `src/polyarb/daemon/l2_main.py` — daemon entry, `_tob_row_from_frame` / `_trade_row_from_frame` 投影器, dispatch by event_type
- `src/polyarb/storage/l2_supabase_mirror.py` — Supabase mirror writer (l2_top_of_book + l2_trades)
- `src/polyarb/observation/l2_candidate_refresh.py` — Phase 03 candidate refresh on snapshot_complete (event bus 实例)
- `alembic/versions/003_l2_tables.py` — `l2_top_of_book` + `l2_trades` DDL + BRIN/btree 索引 + RLS anon-read (新 alembic 005 应贴这风格)

### Phase 04/04.1 落地
- `.planning/workstreams/m1-perception/phases/04.1-d01-restart-robustness-chaos-redesign/04.1-CONTEXT.md` — D-03 stale_s=30 LOCKED 不动
- `.planning/quick/260531-gap-401-watchdog-false-trip/SUMMARY.md` — GAP-401 liveness gate 实现 (本 phase 必须保持其不变)
- `src/polyarb/http/l2_health.py` — /health checks, 新增 L3 子检查应 follow chain-truth (D-08 Phase 04 pattern)
- `src/polyarb/http/l2_control.py` — /control HMAC pattern, L3 promote 手工触发 endpoint 应复用

### Phase 01.1 (scanner recipe 体例)
- `src/polyarb/scanner/` — scanner recipe runner, 4 层 SQL injection defense
- `src/polyarb/scan_recipes/*.yaml` — 6 个现有 recipe 体例, l3-promote.yaml 新成员
- `docs/learning/07-观察市场.md` — scanner 教学文档

### Dashboard
- `dashboard/` — Phase 02/03 已建 Next.js App Router pages (candidates / top_of_book / trades / signals)
- `dashboard/lib/supabase/` — server-client split for SSR + anon-read
- `dashboard/.env.example` — Vercel env vars 体例 (5 个已 set, /l3 不需要新 env)

### Project-level
- `.planning/PROJECT.md` — m1-perception 章程
- `.planning/workstreams/m1-perception/ROADMAP.md` — Phase 05 行
- `.planning/workstreams/m1-perception/STATE.md` — 当前进度

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets (Phase 05 直接复用)
- **`ws_market_client.stream_market_events`** — async iterator + `on_connect` 钩子, L3 token 用 `{"operation": "subscribe", "assets_ids": [...]}` 动态加订 (D-11)
- **`ws_consumer.WsConsumer`** — 消费者 + watchdog + liveness gate (GAP-401, 不动)
- **`l2_main._tob_row_from_frame`** — 已知能把 `book` event 的 `bids[0]/asks[0]` 投到 top-of-book; 本 phase 加 `_book_levels_rows_from_frame` 投到全 20 行
- **`scanner` 框架** — 4 层 SQL defense, yaml recipe 体例 (D-09 l3-promote.yaml 走这条)
- **`l2_supabase_mirror`** — Supabase upsert/insert helpers, l2_book_levels 写入应加 `mirror_book_levels` 同款 (fail-soft + breadcrumb)
- **AsyncIOScheduler** (Phase 02 D-15) — 5-min recompute cron (D-14)
- **Vercel Next.js App Router** (Phase 02/03 dashboard) — `app/l3/[asset_id]/page.tsx` dynamic segment
- **Supabase RLS anon-read 模式** (003_l2_tables.py L158-161) — `l2_book_levels` 同款 policy, `l2_ohlc_*` views 默认 anon SELECT

### Established Patterns (Phase 05 应遵守)
- **3-point lockstep** (Phase 02 D-21): 新表 DDL → schemas.py 列 → INSERT SQL 三处必须同步, test 防回归
- **chain-truth /health surfacing** (Phase 04 D-08, SESSION 29 实证): L3 sub-system 加 `/health` 子检查 `l3:active_count` / `l3:last_book_levels_at_s` / `l3:last_promote_at_s`, 不门控在不存在的 config 字段
- **Fail-soft mirror** (Phase 02 LEARNINGS P5): l2_book_levels mirror 失败 → audit log + Sentry breadcrumb, **不中断 daemon**
- **chaos image-aware** (Phase 03 LEARNINGS): Phase 05 若加 chaos primitive, 必先 `make chaos-l2-fly-image-check` 验工具可用
- **scanner SQL 4 层防御** (Phase 01.1 LEARNINGS): l3-promote.yaml 走 normalize → param-bind → AST validate → table allowlist
- **event bus opt-in default-FALSE** (Phase 03 D-05): L3 promoter 若 emit `l3.promoted` event, 默认 FALSE, chaos PASS 后才 opt-in

### Integration Points
- **Alembic migration 005**: 新 `l2_book_levels` 表 + 3 个 OHLC view (1m/5m/1h)。版本号 005 (004 是 Phase 04 D-07 `yes_token_id` add)
- **`l2_main` dispatcher**: 现有 `if event_type in ("price_change", "best_bid_ask", "book"):` 分支增强 — `book` 同时写 top-of-book (已有) + 新走 `_book_levels_rows_from_frame` 写 l2_book_levels (新增)
- **AsyncIOScheduler**: 添加 `l3_promoter` job, 5min cron, 调用 scanner recipe `l3-promote.yaml`, 写入 daemon 内存的 `_l3_active_set: set[str]`
- **ws_consumer subscribe state**: 暴露 `add_subscriptions(asset_ids)` / `remove_subscriptions(asset_ids)` API, 由 l3_promoter 比较 active_set diff 后调用
- **dashboard candidates page**: 加 "Promoted to L3" 标签列 (read from Supabase `l3_candidates` 视图 或 daemon 推到 `l2_top_of_book.l3_active` flag 列 — executor 定具体)
- **/health l3 子检查**: `l2_app.py` 加 4 个新子检查 (active_count / last_promote_at_s / last_book_levels_write_at_s / book_levels_rss_kb 可选)

</code_context>

<specifics>
## Specific Ideas

- **K 线视觉风格**: 用户没特别指定, 但 Phase 02 dashboard 已建立 minimal/dense 风格 → 沿用 (无背景色 K 线 + grid + Y 轴刻度); lightweight-charts 默认风格基本符合
- **Thresholds 不进 env**: 用户主动选 yaml-only (CLAUDE.md "Experiment values never touch baseline defaults" 精神) — 调参 = 改 yaml + commit + audit trail; v2 若真要 prod 动态调, 再讨论
- **24h soak vs 7-day**: 用户没异议 24h, 这是 L3 标准比 L1 低一档的合理决定 (L3 锁定集天然更小, 误差容忍更高, 不需要 7-day 长跑验证 ≥1 自然故障)

</specifics>

<deferred>
## Deferred Ideas

- **Yes/No 双 token L3 处理细节**: 用户在 round-4 三选项里没选 "补问 Yes/No 双边", 暗示双 token 都进 L3 (10 subscriptions/5 markets) 是 the agent's discretion。 planner 应假设双 token 都纳 L3, 但若 1 边 depth 远低于另一边可以单边 promote — 这是 v2 optimization, 不在 v1 必须项。
- **历史回填 / cold-start backfill**: Phase 03 GAP-301 类问题 — L3 promoter 重启后 OHLC view 仍有数据 (因为基于 l2_top_of_book), 但 l2_book_levels 历史是空。是否要 REST `/book` 重启回填 → 推到 Phase 06 (需要先观察 prod 实际有多大空洞)
- **Vercel deployment protection 对 /l3 路由**: Phase 02 已知 EMAIL_WHITELIST 让 anon 看到 401。`/l3/[asset_id]` 跟 candidates 一样吃这个限制, 不进 Phase 05 scope。用户已在 memory CURRENT-CALL 里记着。
- **多 OHLC 粒度 (15m / 4h / 1d)**: 1m + 5m + 1h 起步, 若策略侧 (M4) 需要更多周期, Phase 06+ 再加。
- **Materialized view + pg_cron**: 当查询变慢再升级。Phase 05 regular view 是第一道路径。
- **`prices-history` REST 拉历史 K 线 backfill**: thread §2.2 Q5 已记 closed market 退化到 12h 颗粒度, **不能** 依赖此做 source of truth — 但可作为 spot-check 对照工具, 推到下个 phase 决策。
- **L3 信号策略 (信号产出, 不只是数据)**: 属于 M4 workstream / 跨能力线, 不动。
- **Promote 阈值动态调整 (prod adaptive)**: v1 baseline 锁死 yaml, prod 用 1-2 周后看 churn rate / 命中率, 再决定是否进 ENV。

### Reviewed Todos (not folded)
- m2-combinatorial T2 validation tests (≥3 fee-differential + IMDEA Type-2) — 不属于 m1-perception, 在 m2 workstream 独立推进
- GAP-401 prod 复验 — 与 Phase 05 解耦, 下次 L2 deploy 后开安静窗口观察

</deferred>

---

*Phase: 05-ws-book-prices*
*Context gathered: 2026-06-01*
