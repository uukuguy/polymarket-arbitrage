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

### Phase 2: WebSocket 增量数据流

**Goal:** /book + /prices 频道实时增量推送替代轮询
**Status:** ⏸️ Pending Phase 1.1 完成（先用低频观察建立直觉，再上高频管道）
**Depends on:** Phase 1.1（observation toolkit 用起来后才知道 WS 该订阅什么）

---

*Workstream: m1-perception*
