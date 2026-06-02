---
workstream: m2-combinatorial
created: 2026-04-28
last_updated: 2026-06-02
---

# Project State — m2-combinatorial（组合套利能力线）

## Current Position
**Status:** Phase 2 T2 + T3 ✅ closed (SESSION 36) → T4 Execution Pipeline 是下一步
**Current Phase:** Phase 2 — 套利执行引擎（02-arbitrage-engine）
**Last Activity:** 2026-06-02
**Last Activity Description:** SESSION 36 — T2 IMDEA validation (4→7 tests) + T3 RoutingEngine slippage-aware venue selection (6→12 tests, +SlippageCalculator wiring + estimate_cross_execution_savings consultation + caller override path)。Engine no longer hardcodes "polymarket-first"; PM still wins under default params but mechanism flips on param change (test_param_flip_makes_clob_cheaper_for_buy locks it)。

## Phase 2 Plan Progress (`02-1-PLAN.md` — Revision 4 locked 2026-05-20)
- ✅ **T1** signal & execution models — `models/signal.py` + `models/slippage.py` (commit `08a13d3`)。Body 仍未对齐 (signal.py 概念膨胀), 推到 T2 完成后回头校正
- ✅ **T2** Slippage Model = fee-differential cross-venue (Revision 4 locked, SESSION 36 closed) — code 320 行 + 7 测试 green (4 existing + 3 IMDEA Type-2)。fee_diff_bps BUY+clob_maker = 60bps locked; SELL matrix locked; estimate_cross_execution_savings unit-economics 在 [$0.10, $20] IMDEA band 内
- ✅ **T3** Routing Engine slippage-aware (Revision 6 locked, SESSION 36 closed) — `routing/engine.py` extended to inject `SlippageCalculator`, `_select_venue` consults `estimate_cross_execution_savings` for PM-vs-CLOB selection, `ExecutionLeg.estimated_cost` now reflects `SlippageResult.net_cost_dollars()` not naive `price × size`。6 → 12 routing tests green (added: default-params BUY→PM lock, SELL→PM-tie-break, estimated_cost reflects slippage, param flip→CLOB, caller override respected, backward-compat)
- 🟡 **T4** Execution Pipeline — `execution/engine.py` 雏形已 commit,sequential flow 未完。下一步: 接 RoutingDecision 跑顺序成交
- ⏸ **T4** Execution Pipeline — `execution/engine.py` 雏形已 commit,sequential flow 未完
- ⏸ **T5** Position Tracker — `routing/position_tracker.py` 已 commit
- ⏸ **T6** Settings — `routing/config.py` 落地 (含 RoutingConfig/ExecutionConfig/PositionConfig/AppConfig)
- ⏸ **T7** CLI Integration — `arbitrage evaluate/run/status` subcommand 未做
- ⏸ **T8** E2E test — 未做

## Test Count (m2)
- 30 tests green (SESSION 36): `test_engine` 12 (6 existing + 6 T3 venue selection) + `test_signal` 11 + `test_slippage` 7 (4 existing + 3 IMDEA Type-2)

## Session Continuity
**Stopped At:** SESSION 36 — T2 + T3 done, T4 Execution Pipeline 是下一步 (接 RoutingDecision sequential flow)
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

## Next Action (2026-06-02)

**T4 Execution Pipeline** — 现状: `src/polyarb/execution/engine.py` 雏形已 commit, sequential 成交 flow 未完。下一步:
1. 读 `execution/engine.py` 现状, 看 sequential flow 和 4 paths (atomic/best-effort/abort/retry) 实现到哪
2. 接 RoutingEngine 输出的 RoutingDecision → 按 plan.legs 顺序成交
3. 写 RED → GREEN tests: happy path / partial fill / first-leg-fail abort / network retry
4. T5 Position Tracker 在 T4 GREEN 后接 — 跟踪 open positions, hedge ratios, P&L

---

## Cross-workstream Cleanup (SESSION 11)（已完成）
- Phase 目录改名规则统一：`m2/phases/02-arbitrage-engine/`（原 `02-/` 因中文 phase 名 slug 化为空）
- m2 文档从 `.planning/phases/02-/` 错位位置搬正
- `src/polyarb/config/` 包目录与 `polyarb/config.py` 模块冲突：m2 dataclass 搬到 `routing/config.py`，`config.py` 单文件保持作为 m1 应用 Settings 命名空间
- T1 commit 漏 import 依赖（models/）已补全 commit `08a13d3`
