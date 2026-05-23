---
phase: 03
phase_name: "L2 Orderbook Tracking (分钟级 daemon)"
workstream: "m1-perception"
status: discuss-pre-research (research-blocked)
started: "2026-05-23"
prior_phases:
  - 02 (L1 production-grade, hard gate passed via chaos injection 替代 7-day soak)
  - 02.1 (3 bug fix-up, 全闭环 2026-05-22)
gray_areas_identified: 3
gray_areas_decided: 0
gray_areas_pending_research: 3
---

# Phase 03 CONTEXT.md (pre-research draft, 2026-05-23)

> **状态**: discuss-phase 进行中, 但因 thread §2.2 + §2.6 未答问题导致**3 个 gray area 都需先做 thread 调研才能锁决策**. 本文件先记录 gray-area 边界 + 用户授权的调研顺序, **不锁定方案**. researcher 完成 thread 补足后, discuss-phase 续作.

---

## 用户决策 (本次 discuss session)

### 优先级 + 依赖顺序

用户选择**三个 gray area 都讨论, 按依赖顺序**:

1. **DB 选型重审** (7-day soak deviation 回补) — **基础层**, 影响 L2 写入后端
2. **WS vs REST polling** — top-of-book 采集方式, 决定写入频率
3. **候选集选择机制** — 10-100 markets, 物理容量需求受上两者约束

### 节奏

**先 thread 调研再 discuss**. 用户授权 spawn researcher (gsd-phase-researcher 或独立 thread-extender subagent) 补 thread §2.2 (Polymarket WS 5 未答问题) + §2.6 (DB 选型现状, 含 Supabase Pro / Neon / Postgres 自管价格 + L2 写入频率约束). 不在认知 gap 上做架构决策.

**理由**: Phase 02 LEARNINGS L6 (HTTP 200 ≠ user notified) + L7 (SESSION 20 E2E 自我欺骗) 的延伸 — 不在未验证的假设上 lock decision. Phase 02.1 也用了同样的纪律 (verification ownership memory).

---

## Gray Areas (3 个, 全部 research-blocked)

### Gray Area 1: DB 选型重审 (7-day soak deviation 回补)

**问题**: thread §1 三层金字塔硬规则要求 "L1 7-day soak + uptime ≥99%" 才能开 L2. Phase 02 用 chaos injection 替代 (soak-gate-deviation thread), 但**未来回补点**明确写在 thread:

> "Phase 03 (L2 orderbook tracking) discuss-phase 时 must-haves 必须包含 'real 7-day soak (with paid Supabase Pro or alternative DB)' 作为门槛".

**核心 trade-off**:
- L2 写入频率 (10-100 markets × 1-5min = 12-1440 行/min) **远高于** L1 (1-2 行/day × 12k markets)
- 累计估算 (thread §2.6.B): 50 markets × 1min × 24h = **72k 行/天 × 365 天 = 26M/年** — 单表完全够用, 不要过度选型
- **但** Supabase Free tier 7 天无活动 auto-pause — Phase 02 已撞这个坑被迫做 deviation

**待调研问题** (research-blocked):
- Supabase Pro $25/mo 是否覆盖 L2 量级? Pro 是否还有 idle pause?
- Neon Postgres 价格 + auto-pause 行为? Free tier 是否能 cover L2 量级?
- 自管 Postgres (Fly Postgres / Railway) 成本 + 运维成本对比?
- TimescaleDB extension 是否有必要 (L2 26M 行/年其实**不大**, 普通 Postgres 完全够用)?
- 现 Supabase Free 数据 → 升级 / 迁移路径 (有没有 zero-downtime migration)?

**决策悬置**, 待 researcher 出 thread §2.6 补 (定价 + 实测能力 + 迁移路径) 后续作.

**临时假设 (用于其他 gray area 估算, 不锁定)**: Supabase Pro $25/mo + Fly Postgres backup. 这只是 placeholder, 不是 decision.

### Gray Area 2: WS vs REST polling (top-of-book 采集方式)

**问题**: L2 top-of-book + 成交流, 用 Polymarket `/book` WS channel 还是 REST polling? thread §2.2 列了 5 个未答问题:

1. `/book` 通道是 per-token 订阅还是支持批量? 一个连接最多订几个?
2. `/prices` 通道粒度? 事件级还是 token 级?
3. 有 "event 下所有 markets 一次订完" 的快捷方式吗?
4. 速率限制 / 连接数 / 重连策略?
5. 历史 trades 有 REST 接口吗 (深度多深)? 还是只能从 WS 流自累积?

**核心 trade-off**:
- WS: 实时 + 低 lag + 高复杂度 (重连 / 跟踪集动态切换 / 状态机)
- REST polling: 简单 + 1-5min lag (L2 频率本来就 1-5min, lag 不致命) + 容易重试 + 已有 clob_client.py 模板
- **重要约束**: 如果 WS 必须 per-token 且连接数有限, "动态切换跟踪集" 就是核心架构挑战 — REST polling 不存在这个问题

**待调研问题** (research-blocked):
- 调研 3th-party (polymarket-kalshi-weather-bot / clawfirm) 的实际 WS 代码
- Context7 + DeepWiki 查 Polymarket clob WS 官方 docs
- WS-vs-REST decision matrix (latency / complexity / 重连成本 / 跟踪集动态切换 / 实测稳定性)
- L2 "1-5min" 频率约束下 REST polling 的可达性 (rate limit + 12k token universe 是否能支撑)

**决策悬置**, 待 researcher 出 thread §2.2 补足 5 个未答问题 + 实际 OSS 代码摸底后续作.

**初步倾向 (不锁定)**: REST polling 优先 (L2 lag 容忍度高 + 复杂度低 + 已有 clob_client.py 模板). WS 等 L3 真需要 tick 级时再上.

### Gray Area 3: 候选集选择机制 (10-100 markets)

**问题**: L1 12k markets → L2 10-100 markets, 怎么 surface? thread §1 三层金字塔表只列 "候选子集 10-100 个", 没指定 selection mechanism.

**核心 trade-off**:
- **手选 (watchlist)**: 用户自决, dashboard UI 编辑. 简单, 但需 dashboard 投入, 且不 scale (M2/M4 策略需要数据驱动 surface)
- **自动 ranking**: 基于 L1 字段 (liquidity / volume / tag / price drift). 数据驱动, 复用 Phase 01.1 scanner 引擎. 但需 surface ranking 标准 + UI 让用户验证
- **混合**: 用户定义 watchlist 模板 (例: "ranking liquidity > $10k & volume > $5k & exclude tags X" 作为 recipe) + 自动 refresh. 复用 Phase 01.1 scanner recipes
- **触发机制**: cron (L1 snapshot 完后) vs event-driven (snapshot.complete event 触发 L2 candidate refresh, thread §1.5.C event bus)

**待调研问题** (research-blocked, 部分依赖 GA1/GA2):
- L1 哪些字段已经 expose 在 SQLite? (Phase 01.1 + Phase 02 schema 是否够支撑 ranking?)
- 已有 scanner recipe 是否够复用 (Phase 01.1 6 内置 + YAML 自定义)?
- 跟踪集 churn 频率 (每 cron 都全量重算还是 incremental refresh)?
- Phase 01.1 watchlist YAML (`docs/learning/07-观察市场.md` § watchlist) 是否能直接复用为 L2 input?
- 物理容量: 10 markets × 5min vs 100 markets × 1min, 哪个更现实? (依赖 GA1 DB 选型 + GA2 采集方式)

**决策悬置**, 但**初步设计取舍**:
- 复用 Phase 01.1 scanner recipe 体系作为 L2 candidate source — 用户在 yaml 里写 ranking 规则
- L1 cron snapshot.complete → L2 candidate refresh (event-driven, thread §1.5.C)
- 初期手选 (10 watchlist) + 后续自动 ranking 并行 — 不一次性 over-design

**这个 gray area 受 GA1/GA2 物理容量约束, 必须最后决**.

---

## 下一步 (按用户授权顺序)

1. **Spawn researcher**: spawn `gsd-phase-researcher` subagent (或自定义 thread-extender), 任务:
   - thread §2.2: 补足 5 个 Polymarket WS 未答问题 (Context7 + DeepWiki + 3th-party OSS 代码摸底)
   - thread §2.6: DB 选型现状 (Supabase Pro / Neon / Fly Postgres 价格 + 实测 + 迁移路径 + L2 量级压测建议)
   - 输出: `.planning/threads/market-observation-architecture.md` § 2.2 + § 2.6 增量 update (不 overwrite, append RESEARCH UPDATE block)

2. **Discuss-phase 续作 (本 CONTEXT.md 转 final)**: thread 补足后, 重启 `/gsd-discuss-phase 03 --ws m1-perception`, 锁定:
   - D-01..D-03 DB 选型 + 迁移路径 + soak gate 回补方式
   - D-04..D-06 采集方式 + L2 daemon 状态机
   - D-07..D-08 候选集 selection mechanism + dashboard surface

3. **Plan-phase**: discuss 完成后, spawn `gsd-phase-researcher` 二次 + `gsd-planner`, 产出 RESEARCH.md + PATTERNS.md + VALIDATION.md + N 个 PLAN.md

---

## Phase 02.1 LEARNINGS 应用 (process discipline)

Phase 03 一开始就遵守 Phase 02.1 LEARNINGS:

| Lesson | 在 Phase 03 怎么应用 |
|---|---|
| L1 fail-soft 与 Sentry breadcrumb 上传交互 | L2 daemon 任何 fail-soft skip 必须双锚点 (loguru + breadcrumb), `category` 区分 state vs error (P1/P2) |
| L2 cross-bug interaction 必须前置识别 | 本 CONTEXT 已经把 GA1/GA2/GA3 的依赖关系明确写出, 不允许下游撞依赖才发现 |
| L3 dev .env 渗透 | unit test fixture 显式 nil 真凭证 (P3 inherits) |
| L4 loguru StringIO sink | 所有 loguru-message assertion 用 StringIO 不用 caplog |
| L5 Sentry API region 路由 | 任何 Sentry API 调用先 parse DSN 拿 region 前缀 |
| L7 Pyright false positive | uv venv 项目下 Pyright import error 默认忽略, 除非真 runtime failure |
| L8 容器内验证比修 Fly proxy 路径快 | L2 daemon 任何 plan 设计 chaos 时考虑 "如果 prod 路径阻塞了, 在容器内怎么验" |

| Decision | 在 Phase 03 怎么应用 |
|---|---|
| D-01 fail-soft 双锚点 | L2 daemon 写 Supabase/R2 失败的处理 inherits (P1) |
| D-03 独立 middleware | 如果 L2 daemon 加 control endpoint (例: pause/resume tracker), 独立 ControlAuthMiddleware, 不 import scan.py |
| D-05 两 endpoint health | L2 daemon 沿用 /health (IETF) + /healthz (Fly-friendly) 模式, 共享 `_build_health_checks()` helper (P5) |
| D-06 body schema full mirror | L2 加新 check (例: tracker_age) 自动 propagate 到两 endpoint |
| verification-ownership | Claude 自己用 Sentry API / flyctl ssh + script 验证, 不让用户翻 UI |

---

*Pre-research draft. 待 thread §2.2 + §2.6 调研完成后续作 discuss-phase, 转 final CONTEXT.md.*
