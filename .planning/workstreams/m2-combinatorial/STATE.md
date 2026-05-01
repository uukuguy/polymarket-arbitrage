---
workstream: m2-combinatorial
created: 2026-04-28
---

# Project State — m2-combinatorial（组合套利能力线）

## Current Position
**Status:** Phase 2 partial — T1 实施完毕，剩 7 task
**Current Phase:** Phase 2 — 套利执行引擎（02-arbitrage-engine）
**Last Activity:** 2026-05-01
**Last Activity Description:** 清理跨 session 散件 — config namespace 修复、补 T1 漏依赖、目录改名

## Phase 2 Plan Progress (`02-1-PLAN.md`)
- ✅ **T1** signal & execution models — `models/signal.py` + `models/slippage.py`（运行时 commit `08a13d3`）
- ⏸ **T2** Slippage Model — `tests/models/test_slippage.py` 已有 4 tests，模型代码 `models/slippage.py` 已落地，但和 T2 的 `PolymarketDepthCurve` / `DepthCurve` Protocol 还没对接
- ⏸ **T3** Routing Engine — `routing/engine.py` 雏形 + 17 routing tests 已 commit，但完整路由分支仍待补
- ⏸ **T4** Execution Pipeline — `execution/engine.py` 雏形已 commit，sequential flow 未完
- ⏸ **T5** Position Tracker — `routing/position_tracker.py` 已 commit
- ⏸ **T6** Settings — `routing/config.py` 落地（含 RoutingConfig/ExecutionConfig/PositionConfig/AppConfig）
- ⏸ **T7** CLI Integration — `arbitrage evaluate/run/status` subcommand 未做
- ⏸ **T8** E2E test — 未做

## Test Count (m2)
- 21 tests green: `test_engine` 6 + `test_signal` 11 + `test_slippage` 4

## Session Continuity
**Stopped At:** SESSION 11 cleanup 完成；Phase 2 T2-T8 待续
**Resume File:** None

## ⚠️ Plan-vs-Code 偏离审计（SESSION 11 EOD 发现）

SESSION 10 落地的 m2 代码方向**与 02-1-PLAN.md 不完全一致**，未来推进前需要先 reconcile：

| PLAN 期望 | SESSION 10 实际产出 | 差距 |
|---|---|---|
| T1 `ArbitrageLeg/ArbitrageSignal/ExecutionResult` 三个核心 dataclass | `models/signal.py` 350+ 行，多个 enum/class（包含 SignalStatus/SignalSide/LegSide/PipelineOutcome/MarketOutcome/MarketSignal 等）| 概念膨胀，需对齐到 PLAN 的最小集 |
| T2 `SlippageModel` 基于 Phase 1 `OrderBookSummary` 估算 AMM 深度 | `models/slippage.py` 实现的是抽象费用模型（taker/maker bps + impact_coef）| **方向错了** — 没接 Phase 1 数据，没有 `DepthCurve` Protocol 也没有 `PolymarketDepthCurve` |
| T3 `RoutingDecision` enum (EXECUTE/SKIP/NEEDS_HEDGE/UNCERTAIN) | 实现成了 dataclass | 类型选择不一致 |
| T4 ArbitragePipeline sequential flow + 4 paths | `execution/engine.py` 4K 雏形 | 实现深度未知，需读代码 |

**判断**：SESSION 10 的实施方向**部分自洽但偏离 PLAN**。继续盲目推 T3-T8 会让偏离继续放大。

## Recommended Next Action

**A. 先做"对齐审计"**：逐文件对比 `models/signal.py` / `models/slippage.py` / `routing/engine.py` / `execution/engine.py` 与 02-1-PLAN.md 的 T1-T4，写一份 RECONCILIATION.md，决定每处偏离是 (a) 接受（PLAN 要修订）还是 (b) 重做（对齐 PLAN）

**B. 直接重做 T2**：如果决定 T2 必须接 Phase 1 OrderBookSummary（套利第一红线 mid vs ask 的工程实现），那 `models/slippage.py` 当前的费用模型代码可能要重写或移到别的地方

**C. 先看 m1 Phase 2 (WebSocket) 决定 m2 是否需要等数据**：m2 套利触发依赖实时 BBO，m1 polling-only 时 m2 信号识别滞后秒级。WebSocket 可能改变 m2 设计

**推荐 A**：m2 现在最不该做的事是"继续按 PLAN 推 T2-T8" — 偏离已经存在，要先承认或纠正。审计成本低（读 ~600 行代码），收益高（避免连锁错位）

---

## Cross-workstream Cleanup (SESSION 11)（已完成）
- Phase 目录改名规则统一：`m2/phases/02-arbitrage-engine/`（原 `02-/` 因中文 phase 名 slug 化为空）
- m2 文档从 `.planning/phases/02-/` 错位位置搬正
- `src/polyarb/config/` 包目录与 `polyarb/config.py` 模块冲突：m2 dataclass 搬到 `routing/config.py`，`config.py` 单文件保持作为 m1 应用 Settings 命名空间
- T1 commit 漏 import 依赖（models/）已补全 commit `08a13d3`
