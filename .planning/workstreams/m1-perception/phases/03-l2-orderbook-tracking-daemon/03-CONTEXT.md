---
phase: 03
phase_name: "L2 Orderbook Tracking (分钟级 daemon)"
workstream: "m1-perception"
status: ready-to-research-and-plan
discuss_completed: "2026-05-23"
decisions_locked: 9
gray_areas_decided: 3
prior_phases:
  - 02 (L1 production-grade, hard gate passed via chaos injection 替代 7-day soak)
  - 02.1 (3 bug fix-up, 全闭环 2026-05-22)
research_refs:
  - thread market-observation-architecture.md RESEARCH UPDATE 2026-05-23 (line 762+)
---

# Phase 03 CONTEXT — L2 Orderbook Tracking (分钟级 daemon)

## TL;DR

m1-perception 三层金字塔的中段, 桥接 L1 全市场观察与 L3 策略执行. 候选子集 10-100 markets, 通过 Polymarket WS market channel 实时跟踪 top-of-book + 成交流, 信号识别 / 进场触发. 新独立 daemon `polyarb-l2.fly.dev`, Supabase Free + GHA cron 保活, 复用 Phase 02 Wave 4 dashboard 模式做 user surface.

---

## Locked Decisions

### D-01: DB tier = Supabase Free + GHA cron keepalive

**决策**: 继续 Supabase Free, 用 GHA cron `.github/workflows/supabase-keepalive.yml` 每 24h wget Supabase REST endpoint 保活, 不升 Pro.

**理由**:
- Phase 02 已踩过 7-day pause 坑, 但用户判断 $25/mo 在 Phase 03 阶段不值
- GHA cron 完全免费, 24h ping 频率 < 7-day 阈值
- Better Stack 加 heartbeat (24h tolerance) 监控 GHA 健康
- 长期: 升 Pro 推 Phase 04 (L3) 或 M3 (实盘) 触发

**风险面**:
- GHA cron 失败 → Supabase 4 天后 pause → /health = fail (recovery 路径已 Phase 02.1 验证, 不灾难)
- 一旦 paused, manual restart Supabase project (大约 5 分钟)

**待 plan-phase 验证**:
- GHA workflow 文件具体 wget command (REST + DB DSN 至少一个)
- Better Stack heartbeat 设置 + 24h tolerance 阈值
- 加监控信号到 03-VALIDATION.md (acceptance criteria 含 "Supabase ping verified 7 day window")

**Source**: research §2.6 (Supabase Pro 推荐), 用户 D-01 决策 (反 research 推荐, 选 cost-saving 路径)

### D-02: 采集方式 = WS market channel 主 + REST backfill 混合

**决策**: L2 daemon 主路径 = WS subscribe 整个 candidate-set 的 token (市场 channel), 复用 Phase 02 clob_client.py REST 调用作 backfill + fallback.

**采集事件类型** (per thread §2.2 research):
- `price_change` (orderbook delta) → 写 L2 top_of_book 表
- `best_bid_ask` (custom feature) → 同上, faster 路径
- `last_trade_price` (trade execution) → 写 L2 trades 表 (D-08)
- `book` (full snapshot) → 启动 + 重连时拿 baseline

**REST 角色**:
- 启动 backfill: `/prices-history` interval=1m, 拉过去 7 天 baseline (注意 closed markets 12h 退化, issue #216)
- WS 冻结时 fallback: staleness watchdog 触发后短期 REST polling 维持
- candidate refresh 时: Gamma `/events/{id}` 拿新 markets 的 clob_token_ids

**理由**:
- Polymarket WS market channel **单连接订阅无上限** (2025-05-28 取消 100 token 限制) → 整个 candidate-set 一个 WS connection 够用
- WS = 实时 (毫秒级), REST polling 1-5min lag 对 L2 时序数据有信号 loss 风险
- Research recommendation 强 evidence-based

**Source**: thread §2.2 RESEARCH UPDATE 2026-05-23 (Q1 + Q5 + Q4), 用户 D-02 决策

### D-03: WS staleness watchdog = 30s 无 event → 重连 + initial_dump=true

**决策**:
- 业务层 watchdog: 30s 无 WS event 触发硬重连
- 重连 payload: `initial_dump=true` (request full orderbook snapshot)
- 指数退避: 1s, 2s, 4s, capped 30s
- 重连成功后 idempotent re-subscribe (服务器不持久化订阅状态)

**理由**:
- issue #292 silent freeze bug (TCP 没断 / PONG 仍回 / event 流冻结) 仍 open 2026-03 → 业务层心跳必须独立于 TCP keepalive
- 30s 是 research 推荐值, 平衡误报 vs 时延 (L2 candidate top-of-book 高频, 30s 静默 = 异常)
- 低流动性 candidate 可能误报 → 加 INFO log + Sentry breadcrumb (audit trail), 不告警

**Source**: thread §2.2 RESEARCH UPDATE Q4 (Heartbeat + Troubleshooting), 用户 D-03 决策

### D-04: 候选集 selection = Phase 01.1 scanner recipe + 手选 watchlist 混合

**决策**:
- **复用 Phase 01.1 scanner recipe 体系** — YAML 写 ranking 规则 (例: `liquidity > $10k & volume > $5k & exclude tags ["unrelated"]`)
- 多个 recipes 并行运行, 各自产出独立 candidate-set
- 手选 watchlist (Phase 01.1 已有 YAML) 作为 override layer (用户必须跟踪的 market 不被 ranking 漏掉)
- 候选集 union: scanner-result ∪ user-watchlist

**理由**:
- 复用 Phase 01.1 04+05+06 现成代码 (scanner engine + watchlist 加载) → 零额外底层投入
- 数据驱动 (auto ranking) + 用户控制 (watchlist) 双轨, M2/M4 策略需要数据驱动 surface
- 不一次性 over-design — Phase 03 初期可只有 1-2 个 recipe + 5-10 watchlist

**Source**: Phase 01.1 LEARNINGS (scanner + watchlist patterns), 用户 D-04 决策

### D-05: candidate refresh 触发 = L1 snapshot.complete event 驱动

**决策**:
- L1 cron 每次 snapshot 跑完 → emit `snapshot.complete(snapshot_id)` event
- L2 daemon 订阅此 event → 重算 candidate-set (scanner recipes + watchlist)
- diff old_set vs new_set → WS dynamic subscribe (新 tokens) + unsubscribe (去除 tokens)
- 不动 candidate-set 时: WS 状态保持

**Event bus 实现 (待 plan-phase 选)**:
- 初期 (Phase 03 launch): Supabase realtime channel (复用 Phase 02 Supabase mirror 投资) 或 Postgres NOTIFY (低延迟跨进程)
- 长期 (Phase 04+ 多 daemon): Redis Pub/Sub

**理由**:
- thread §1.5.C event-driven 架构, 三层金字塔自然连续
- L1 频率 ~6h/snapshot, candidate refresh 自然 follow → 不浪费 compute
- 跨进程 (D-06 polyarb-l2 新 daemon) 必须经事件总线

**Source**: thread §1.5.C event bus 设计, 用户 D-05 决策

### D-06: L2 daemon 进程边界 = 新独立 daemon polyarb-l2

**决策**: 新 Fly app `polyarb-l2.fly.dev`, 完全独立部署. 与 polyarb-l1 通过 event bus 通信.

**Fly setup**:
- Fly app `polyarb-l2`, 同 region `ams` (与 polyarb-l1 同 region, 私网延迟最小)
- 初期 1 micro VM ($1.94/mo), Phase 03 单 connection WS, 资源够
- 复用 Phase 02 Wave 4 secrets (Sentry / Telegram / Supabase / R2)
- 复用 Phase 02 部署 stack: Dockerfile / fly.toml / GHA deploy.yml 三件套

**理由**:
- WS 长连接 + L1 cron 不抢 CPU
- crash 隔离 (L1 / L2 互不影响)
- chaos verification 独立 (一个 daemon 跑 chaos 不影响另一个)
- 未来 daemon 扩展架构清晰 (L3 后续可以再起 polyarb-l3)

**Source**: 用户 D-06 决策, 复用 Phase 02 deployment thread §2.1.7 Fly app 模式

### D-07: dashboard surface = Supabase mirror + Vercel dashboard 复用

**决策**:
- L2 daemon 写 4 个 Supabase 表 (fail-soft, D-12 invariant 继承):
  - `l2_candidates` (current candidate-set + recipe attribution)
  - `l2_top_of_book` (per-asset best_bid_ask 时序)
  - `l2_trades` (per-asset trade 时序, D-08)
  - `l2_signals` (signal events, Phase 03 后期写)
- Vercel dashboard 加 4 个新页面 (复用 Phase 02 Wave 4 schema lockstep + Auth pattern)

**理由**:
- 复用 Phase 02 dashboard 架构 + Auth/RLS 投资, 零迁移成本
- Supabase Free 表数无上限, 4 个新表加上 L1 已有的不超过额度
- 用户日常 visibility 强 (而非 SQLite + CLI 查询)

**待 plan-phase**:
- 写入 Supabase 频率: L2 fail-soft 写, 每 1-5min batch upsert (避免每 event 单条 INSERT 太频)
- Alembic 002 migration 加 4 个新表 + RLS policy

**Source**: 用户 D-07 决策, 复用 Phase 02 Plan 03 SupabaseMirror + Plan 06 Vercel dashboard

### D-08: trades 自累积 = WS last_trade_price 全量存 + REST 启动 backfill

**决策**:
- WS subscribe candidate-set 全部 token 的 `last_trade_price` event → 每笔 trade 写 `l2_trades` 表 (asset_id, price, size, side, ts)
- daemon 启动时, REST Data API `/trades` 拉过去 7 天 backfill 进表
- candidate-set churn 时, 离开的 tokens **保留**已累积 trades (不 delete) — 否则未来 backtest 历史 candidate 时数据缺失

**理由**:
- issue #216: closed markets `prices-history` 退化到 12h 颗粒度 → REST 历史不可靠
- 必须 Phase 03 一开始就开 WS 累积自己的 trades 真数据源
- M4 LLM 策略后期 backtest 必须有细粒度 trades 历史
- D-12 fail-soft: 写 trades 失败不阻塞 daemon (mirror disabled 路径)

**Source**: thread §2.2 RESEARCH UPDATE Q5, 用户 D-08 决策

### D-09 (cross-cutting): Phase 02.1 LEARNINGS 应用

**继承 Phase 02.1 LEARNINGS** (9D / 8L / 7P / 5S, 详见 02.1-LEARNINGS.md):

| Lesson / Pattern | 在 Phase 03 如何应用 |
|---|---|
| L1 fail-soft 与 breadcrumb 上传 | L2 任何 fail-soft skip 路径必须双锚点 (loguru INFO + Sentry breadcrumb, category='l2-mirror' / 'l2-ws') |
| L2 cross-bug 必须前置识别 | 本 CONTEXT 已明确 D-01 (Supabase Free pause 风险) 跨依赖 D-07 (dashboard 写) — plan-phase 时 wave 排序要避免撞 |
| L4 loguru StringIO sink | L2 daemon test 用 StringIO sink, 不用 caplog |
| L5 Sentry API region | DSN 还是 EU region (de.sentry.io), API 调用 prefix `de.sentry.io/api/0/` |
| L8 容器内验证 fallback | L2 chaos verification 设计含 "如果 Fly proxy 路径阻塞, 在容器 localhost:8080 验证" |
| P1 双锚点 audit | L2 WS event 处理路径 emit log + breadcrumb |
| P3 独立 middleware | L2 daemon 如果加 /control/* endpoint, 独立 ControlAuthMiddleware (不 import scan.py / control.py) |
| P5 helper-first refactor | L2 共享 check 逻辑 (e.g. `_build_l2_health_checks()`), /health + /healthz 模式 |
| P6 VALIDATION.md frontmatter ledger | 03-VALIDATION.md 三字段 (status / nyquist_compliant / wave_0_complete) 同时翻 |
| Verification ownership | Claude 用 Sentry API + flyctl ssh 自验证, 不让用户翻 UI |

---

## Out of Scope (delegated to later phases)

- 完整 orderbook 深度 (top-of-book only, Phase 03)
- 策略执行层 (信号识别 only, 不下单 — Phase 04+ 或 m2-combinatorial)
- WebSocket 全市场流 (L3 候选 - Phase 04)
- M4 LLM 价值判断 (m4-smart-strategies 独立 workstream)
- Redis Pub/Sub event bus (Phase 03 初期用 Supabase realtime / Postgres NOTIFY)
- 候选集 ML ranking (Phase 04+ 用户数据积累后)

---

## Dependencies + Pre-requisites

- ✅ Phase 02.1 closed (3 bug 修完, prod ops 路径完整)
- ✅ thread §2.2 + §2.6 research done (RESEARCH UPDATE 2026-05-23 block)
- ⏳ Phase 02.2 backlog (truth 2 修法 A) — 非阻塞, 可推到 Phase 03 后或并行做
- ⏳ Supabase Free keepalive GHA workflow (Phase 03 Plan 01 task)

---

## Next Steps

1. **`/gsd-plan-phase 03 --ws m1-perception`** — spawn gsd-phase-researcher + gsd-planner
   - Researcher 输出 03-RESEARCH.md (refine 8 decisions 到 implementation-ready)
   - Planner 输出 03-PATTERNS.md + 03-VALIDATION.md + N 个 03-NN-PLAN.md
2. Plan checker iteration (Phase 02.1 用 2 轮收敛)
3. Execute Phase 03 plans (推荐 wave-based parallel like Phase 02.1)

---

## Plan Outline (preliminary, for planner reference)

Estimated 5-7 plans across 3-4 waves:

- **Plan 01** (Wave 1, autonomous): GHA cron supabase-keepalive workflow + Better Stack heartbeat + 03-VALIDATION acceptance criteria for "Supabase ping verified" (D-01)
- **Plan 02** (Wave 1, autonomous): polyarb-l2 Fly app setup — Dockerfile + fly.toml + GHA deploy (D-06, 复用 Phase 02 Plan 04 pattern)
- **Plan 03** (Wave 2): L2 daemon entry — Starlette + scheduler + L2-specific config (D-06)
- **Plan 04** (Wave 2): WS client + market channel subscribe/unsubscribe + staleness watchdog (D-02 + D-03)
- **Plan 05** (Wave 2-3): candidate refresh engine — scanner recipe execution + watchlist union + event subscriber (D-04 + D-05)
- **Plan 06** (Wave 3): Supabase 4 表 mirror (Alembic 002) + REST backfill (D-07 + D-08)
- **Plan 07** (Wave 4, checkpoint): chaos verification + 03-SOAK-LOG.md
- **Plan 08** (Wave 4): docs/learning/10-L2-tracking.md + 03-VALIDATION flip

---

*Discuss-phase complete 2026-05-23. 8 decisions locked + 1 cross-cutting (LEARNINGS application). Ready for /gsd-plan-phase.*
