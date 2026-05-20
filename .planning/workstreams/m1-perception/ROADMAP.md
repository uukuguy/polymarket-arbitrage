# Roadmap: M1 Market Perception

> 能力线，不是里程碑。持续运行 + 持续增强。
> Phase 由 `gsd-tools phase add "..."` 动态长出，不预先列。

## Overview

构建对 Polymarket 整个市场的实时、完整、准确的视图。是 M2/M3/M4 所有策略的底座。

不"完成"，只"够用了"。每当有新策略需要新数据维度，本 workstream 长一个新 phase。

## Phases

(待 `gsd-tools phase add` 长出第一个)

### Phase 1: 完整市场快照工具 ✅ COMPLETE

**Goal:** Polymarket 全市场快照工具（subset/full 双模式），SQLite + Parquet 落库
**Status:** Verified 2026-05-01 (LIVE-RUN-005, 20353 markets, 119 tests green)
**Plans:** 5 plans complete

### Phase 1.1: 市场观察工具与中文化（Observation Toolkit + Translation）

**Goal:** 把 Phase 1 的原始 snapshot 数据转成"可观察的市场"——配方化扫描命令、跨快照对比、单市场详情、中英对照、类别统计、watchlist。让初学者能 30 秒判断一个市场该不该碰。
**Status:** ⏳ Plan complete (2026-05-01 SESSION 12) — 待 execute
**Depends on:** Phase 1
**Plans:** 6 plans (4 waves)

Plans:
- [x] 01.1-01-PLAN.md — Wave 1: schema 升级（Amendment 01: events + event_tags + question_translations + markets.event_id；删除 markets.category/tags）+ normalizer 双源融合 + deps + 新基线 snapshot
- [x] 01.1-02-PLAN.md — Wave 2: T2 翻译完整 vertical slice（OpenAI 兼容 SDK + 缓存表 + 批量 orchestrator + CLI + Makefile + .env.example）
- [x] 01.1-03-PLAN.md — Wave 3: T3 scanner 引擎 + 6 内置配方 Batch 1（amendment: scan-by-tag 替代 scan-by-category）+ 自定义 yaml 加载（4 层防御 + trust split）
- [ ] 01.1-04-PLAN.md — Wave 4: T4 跨 snapshot diff（duckdb FULL OUTER JOIN）+ 单市场时序 tracker（read_parquet glob + union_by_name）
- [ ] 01.1-05-PLAN.md — Wave 4: T5 show-market 多源拼装 + T7 watchlist（yaml.safe_load + 受限 AST 求值，禁内置任意求值器）
- [ ] 01.1-06-PLAN.md — Wave 5: 教学文档 07-观察市场.md + Makefile help 整理 + 端到端 9 步验收 + 对手测试 5 题（含 human-verify checkpoint）

Goals:
- [x] T1 markets 表补 event 关联（Amendment 01: event_id FK + events + event_tags 表）— covered by plan 01
- [x] T2 question 中文翻译（独立 question_translations 表，OpenAI 兼容 API）— covered by plan 02
- [x] T3 配方化扫描命令（thick-but-slippery / near-end / ghost-suspicious / coin-flip / neg-risk-incomplete / by-tag）— covered by plan 03
- [ ] T4 跨 snapshot 对比命令（duckdb 跨 parquet）— covered by plan 04
- [ ] T5 单市场详情命令（中英对照 + 完整字段 + 时间维度）— covered by plan 05
- [x] T6 类别统计命令 — covered by plan 03 (scan-by-tag, amendment 01 replacement for scan-by-category)
- [ ] T7 watchlist（YAML 或 SQLite 表）— covered by plan 05

### Phase 2: L1 production-grade long-running

**Goal:** 把 Phase 01.1 的 L1 观察工具从"研发期单次跑通"升级到"云上 7×24 自主跑 + 健康监控 + 一键部署"。L1 达到生产级判定标准（thread §1）后才能开 L2 工作。
**Status:** 🟢 Ready for discuss — Phase 01.1 LEARNINGS 已映射 6 条 must-haves
**Depends on:** Phase 1.1（observation toolkit + acceptance amendments + deployment thread locked）
**Refs:**
- `.planning/threads/market-observation-architecture.md` §1 (L1 生产级判定标准) + §1.5 (框架抽象 A/B/C)
- `.planning/threads/deployment-architecture.md` §0.1 (locked 4 decisions) + §2.6 (云栈调研)
- `.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/01.1-LEARNINGS.md` (D11/D14/L2/L11/L12/S4)

Scope (从 LEARNINGS carry-over):
- Makefile CLI 入口 smoke test（修 L11/S5 silent failure 根因）
- Snapshot 健康判定升级到三态（D14/L12 已在 amendment 落地，phase 02 补 parquet/SQLite 双校验）
- 框架抽象 A 落地（统一市场状态模型 + 显式分离 stamp 时间 vs 抓取时间，L2/D5/P3）
- 一键部署链路（Dockerfile + fly.toml + GHA workflow，D11/S4 region eu，PaaS 单 DB）
- L1 云上 7×24 长跑 + 健康监控（thread §1 生产级判定标准）
- Dashboard 雏形（Vercel + Supabase 单库，CN 友好非约束）

不在 scope：
- KMS / 私钥栈（D11 决策延到 M3 实盘前）
- Tiger Cloud 双库（D11 单库先撞墙）
- L2/L3 定向跟踪与 WebSocket（thread §1 纪律：L1 未到生产级禁开下一层）

Plans:
- [x] 02-01-PLAN.md — Wave 1: page_fetched_at_ms 列 + L11 silent-failure triple-check（make triple-check 三重契约门）
- [x] 02-02-PLAN.md — Wave 2: Starlette HTTP daemon (/health + /scan HMAC) + AsyncIOScheduler + loguru JSON sink
- [x] 02-03-PLAN.md — Wave 2: Supabase mirror + Cloudflare R2 sync + Alembic 001 schema + 双 URL 约定
- [x] 02-08-PLAN.md — Wave 2.5 retro: Plan 03 F-01..F-05 fix-up（Supabase upsert / Polymarket offset cap / 工具链）
- [x] 02-04-PLAN.md — Wave 3: Dockerfile multi-stage + fly.toml + GHA deploy workflow + 首次 prod deploy（256MB）
- [ ] 02-09-PLAN.md — **Wave 3.5 emergent**: 流式分页根因修复（D-23 amendment） — `_paginate` → `AsyncIterator[dict]` + orchestrator streaming consumer + PyArrow ParquetWriter chunked write + 内存回归测试。**触发**: 2026-05-15 Plan 02-04 首次 prod deploy 后 daemon OOM-killed（256MB Fly VM）。**Blocks**: Wave 4-5（daemon 不稳态下装观测/跑 soak 无意义）。
- [ ] 02-05-PLAN.md — Wave 4: Sentry + Axiom + Better Stack + Telegram alert 集成
- [ ] 02-06-PLAN.md — Wave 4: Vercel Next.js dashboard 雏形 + scan trigger
- [ ] 02-07-PLAN.md — Wave 5: chaos test + 7-day soak + 教学文档 08

### Phase 02.1: Phase 02 fix-up: 2 P1 backlog + /health 503 trade-off (INSERTED)

**Goal:** [Urgent work - to be planned]
**Requirements**: TBD
**Depends on:** Phase 02
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd-plan-phase 02.1 to break down)

### Phase 3: WebSocket 增量数据流（L3 候选）

**Goal:** /book + /prices 频道实时增量推送，作为 L3 单市场 K 线的数据源
**Status:** ⏸️ Pending Phase 2 完成 + L2 中间层定义（thread §1 三层金字塔纪律）
**Depends on:** Phase 2（L1 生产级判定通过）+ thread §2.2 (Polymarket WS 真实能力调研)
**Note:** 原 Phase 2 (SESSION 12 时代锁)。Phase 01.1 架构纠偏后推迟到 L3 上下文。

---

*Workstream: m1-perception*
