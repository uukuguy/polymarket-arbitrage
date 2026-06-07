# Roadmap: M5 Industrialize

> 能力线，不是里程碑。
> Phase 由 `gsd-tools phase add "..."` 动态长出，不预先列。

## Overview

部署、监控、风控、可观测性、自动调参。承载所有其它能力线的运行环境。
任何能力线证明需要工业化支持时启动 phase（如 m1 需要 24h 守护进程时，m5 长一个 phase 提供）。

## Phases

### Phase 01: Polywatch MVP

> INSERTED 2026-05-26 — Polywatch 自动化基建第一批落地, healthz-watcher MVP 已 ship 作 baseline (commit 6a77e06)。

**Goal:** Polywatch 自动化基建第一批 4 trial 跑通 + harness 形态验证。让本项目从"alert 到达但被忽略"升级到"alert 到达 + 自动响应/缓解 + ledger 留底"。healthz-watcher 已是 baseline,本 phase 把它正式纳入 phase 治理 + 扩展另外 3 个 trial + 决出 4 个待定架构决策 (D-Polywatch-1..4)。
**Status:** 🟡 Planning — Plan 01-1 written, ready for execute
**Depends on:** Phase 02.1 BUG-8 (`/control/unpause` HMAC endpoint, 已 ship); Phase 03.1 Plan 07 (GAP-103 notes field + chaos Inj finalization)
**Plan:** 01-1-PLAN.md — 4 trials + global skill, single phase plan (trials independent, no wave-splitting needed)
**Refs:**
- memory `architecture_polywatch-decision-framework.md` — 4 条件 + 8 应用点 + 3 红线 + 决策树
- memory `project_polywatch-mvp-shipped-2026-05.md` — healthz-watcher MVP baseline + 6 GHA secrets + 上线状态
- `.planning/threads/polywatch-architecture.md` — D-Polywatch-1..4 待定 + 三件套架构图
- memory `feedback_alert-chain-discipline-2026-05.md` — alert 到达 ≠ 介入 (Polywatch 的存在理由)
- memory `feedback_dashboard-access-autonomous-2026-05.md` — playwright-cli edge profile (后续 Sentry breadcrumb fetch 复用)
- memory `feedback_verification-ownership-2026-05.md` — Claude 自闭环验证规模化

Scope (4 trial, discuss-phase 决具体边界):

**Trial 1: healthz-watcher (Cron — baseline, 已 ship)**
- 现状: `.github/workflows/polywatch-healthz.yml` + `scripts/polywatch/healthz_watcher.py`, GHA cron `*/15`
- 本 phase 任务: 形式化纳入 phase (写 plan SUMMARY 兜底现有 commit) + 接入 Phase 03.1 修好的 fail-reason / `l2_mirror_enabled` 字段 + Sentry breadcrumb auto-fetch (MVP 砍掉的, 现在补)

**Trial 2: chaos-inj-replay (Cron)**
- 每晚跑 Phase 03 既有 Inj 1/2/3 (+ Phase 03.1 跑完后纳入 L2-3b/L2-4/L2-5) — verdict=PASS streak
- 副作用边界: 只 dry-run / staging,不动 prod 数据

**Trial 3: memory-sanity-check (Ralph Loop)**
- 单会话 ralph-loop, grep MEMORY.md 所有 VERIFIED 条目里的 `file:line` 引用, 验证代码仍在
- 失效条目 propose patch (不自动 commit, 走人 review)

**Trial 4: autoresearch-validation-tuning (AutoResearch)**
- L4 validation rule 阈值校准 — 跑 N 天历史 snapshot, 搜索 L4 tolerance 最佳值
- verdict=signal:noise ratio, results.tsv append-only
- **首次跑通 autoresearch 形态** — 验证三件套之一能否在本项目落地

**4 个待 discuss 决的架构决策 (D-Polywatch-1..4)**:
- D-1: trial 状态承载位置 (`.planning/polywatch/trials.tsv` vs `~/.polywatch/` vs Supabase)
- D-2: cron 触发位置 (GHA vs Fly machine vs 本地 launchd) — 当前 healthz 用 GHA
- D-3: 是否封装成 global skill (现在 vs 本 phase 跑通后再抽)
- D-4: trial 失败的 escalation 级别 (silent / breadcrumb / Telegram / GH issue)

不在 scope:
- 接入 M2 backtest trial (推 m5 phase 02-polywatch-extend, 等 M2 T2 进入实操)
- 接入 L2 资产集自动 curate (推 m5 phase 03-polywatch-l2-curate, 等 L2 数据足)
- 抽 global skill (推 m5 phase 04-polywatch-globalize, 本项目跑实战后再说)
- 真实下单 / 资金 / push prod main / ADR 锁决 (永远红线)

Plans:
- 01-1-PLAN.md — Phase-wide plan: 4 trials + global skill (2026-06-07, SESSION 29)

---

*Workstream: m5-industrialize*
