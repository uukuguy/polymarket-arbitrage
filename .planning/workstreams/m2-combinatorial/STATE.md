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

## Cross-workstream Cleanup (SESSION 11)
- Phase 目录改名规则统一：`m2/phases/02-arbitrage-engine/`（原 `02-/` 因中文 phase 名 slug 化为空）
- m2 文档从 `.planning/phases/02-/` 错位位置搬正
- `src/polyarb/config/` 包目录与 `polyarb/config.py` 模块冲突：m2 dataclass 搬到 `routing/config.py`，`config.py` 单文件保持作为 m1 应用 Settings 命名空间
- T1 commit 漏 import 依赖（models/）已补全 commit `08a13d3`

## Session Continuity
**Stopped At:** SESSION 11 cleanup 完成；Phase 2 T2-T8 待续
**Resume File:** None

## Recommended Next Action
执行 Phase 2 后续 task — 优先 T2 Slippage Model 完成 PolymarketDepthCurve（Phase 1 OrderBookSummary 数据已经可用）
