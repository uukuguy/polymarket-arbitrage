---
workstream: m2-combinatorial
created: 2026-04-28
last_updated: 2026-05-20
---

# Project State — m2-combinatorial（组合套利能力线）

## Current Position
**Status:** Phase 2 T2 走向锁定 (Option B, Revision 4 落地) → 待执行 T2 IMDEA Type-2 测试补充
**Current Phase:** Phase 2 — 套利执行引擎（02-arbitrage-engine）
**Last Activity:** 2026-05-20
**Last Activity Description:** 02-1-PLAN.md Revision 3+4 — 加 Revision History + DRIFT NOTICE + Pending Decision (CLOSED Option B) + T2 body 改写对齐 fee-differential 代码 + IMDEA Type-2 validation requirement

## Phase 2 Plan Progress (`02-1-PLAN.md` — Revision 4 locked 2026-05-20)
- ✅ **T1** signal & execution models — `models/signal.py` + `models/slippage.py` (commit `08a13d3`)。Body 仍未对齐 (signal.py 概念膨胀), 推到 T2 完成后回头校正
- 🟡 **T2** Slippage Model = fee-differential cross-venue (Revision 4 locked) — code 已落地 320 行,4 测试 green。**剩**: 补 ≥3 个 IMDEA Type-2 validation 测试 (cross-venue fee differential 经济学量级断言)
- ⏸ **T3** Routing Engine — `routing/engine.py` 雏形 + 17 routing tests 已 commit。T2 IMDEA 验证完后 T3 接 `estimate_cross_execution_savings` 做 venue selection
- ⏸ **T4** Execution Pipeline — `execution/engine.py` 雏形已 commit,sequential flow 未完
- ⏸ **T5** Position Tracker — `routing/position_tracker.py` 已 commit
- ⏸ **T6** Settings — `routing/config.py` 落地 (含 RoutingConfig/ExecutionConfig/PositionConfig/AppConfig)
- ⏸ **T7** CLI Integration — `arbitrage evaluate/run/status` subcommand 未做
- ⏸ **T8** E2E test — 未做

## Test Count (m2)
- 21 tests green: `test_engine` 6 + `test_signal` 11 + `test_slippage` 4

## Session Continuity
**Stopped At:** SESSION 11 cleanup 完成；Phase 2 T2-T8 待续
**Resume File:** None

## ⚠️ Plan-vs-Code 偏离审计 — 历史快照 (SESSION 11 EOD 发现 + SESSION 21 考古)

> **2026-05-20 update**: T2 偏离已通过 02-1-PLAN.md Revision 4 解决 (Option B, plan body 改写对齐代码)。T1/T3/T4 偏离**仍未对齐**, 但优先级低于 T2 (T2 是核心信号层)。下面段落保留作历史。

SESSION 10 落地的 m2 代码方向**与 02-1-PLAN.md Revision 1 不完全一致**:

| PLAN Revision 1 期望 | SESSION 10 实际产出 (Revision 2 代码) | 差距 | 2026-05-20 状态 |
|---|---|---|---|
| T1 `ArbitrageLeg/ArbitrageSignal/ExecutionResult` 三个核心 dataclass | `models/signal.py` 350+ 行,多个 enum/class | 概念膨胀 | 仍未对齐, 推到 T2 完成后校正 |
| T2 `SlippageModel` 基于 Phase 1 `OrderBookSummary` 估算 AMM 深度 | `models/slippage.py` 是 CLOB↔PM 双场所 fee differential 模型 | **方向不同** | ✅ Revision 4 锁定 fee-differential 为正,depth-curve 废弃 |
| T3 `RoutingDecision` enum (EXECUTE/SKIP/NEEDS_HEDGE/UNCERTAIN) | 实现成了 dataclass | 类型选择不一致 | 仍未对齐, 推到 T3 执行时校正 |
| T4 ArbitragePipeline sequential flow + 4 paths | `execution/engine.py` 4K 雏形 | 实现深度未知 | 仍未对齐, 推到 T4 执行时校正 |

**判断 (2026-05-20 update)**: T2 已解决 (Revision 4),T1/T3/T4 推到对应 task 执行时按需校正。"按 plan 盲推" 反模式已通过 02-1-PLAN.md 顶部 DRIFT NOTICE + Revision History 制度防范。

## Next Action (2026-05-20)

**T2 执行准备就绪** — code 已有,只缺 IMDEA Type-2 validation 测试。下次会话可:
1. 启动一个新 plan (e.g., `02-2-PLAN.md`) 专做 T2 IMDEA validation + 写完 T3 routing engine,**或**
2. 在现有 02-1-PLAN.md 框架下走 T2 IMDEA 测试补全,然后 sequential 推 T3-T8

执行前 prereq: **m1 Phase 02.1 backlog 必须先消化** (用户 2026-05-20 决策,见 m1-perception STATE)。M2 T2 与 m1 Phase 02.1 解耦不冲突, 优先级看你想先看哪条线进展。

---

## Cross-workstream Cleanup (SESSION 11)（已完成）
- Phase 目录改名规则统一：`m2/phases/02-arbitrage-engine/`（原 `02-/` 因中文 phase 名 slug 化为空）
- m2 文档从 `.planning/phases/02-/` 错位位置搬正
- `src/polyarb/config/` 包目录与 `polyarb/config.py` 模块冲突：m2 dataclass 搬到 `routing/config.py`，`config.py` 单文件保持作为 m1 应用 Settings 命名空间
- T1 commit 漏 import 依赖（models/）已补全 commit `08a13d3`
