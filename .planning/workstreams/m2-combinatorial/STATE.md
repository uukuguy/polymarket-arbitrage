---
workstream: m2-combinatorial
created: 2026-04-28
last_updated: 2026-06-06
---

# Project State — m2-combinatorial（组合套利能力线）

## Current Position
**Status:** Phase 2 T2 + T3 + T4 + T5 + T7 ✅ closed (SESSION 37) → T6 Settings / T8 E2E / 真实 venue adapter 是下一步
**Current Phase:** Phase 2 — 套利执行引擎（02-arbitrage-engine）
**Last Activity:** 2026-06-06
**Last Activity Description:** SESSION 37 — T5 Position Tracker realization 闭环。position_tracker.py 179→320 lines (Fill 事件模型 + StopLossEvent 富返回 + close_position_with_fill 生产关仓路径 + open_positions() public view + roi_pct AttributeError bug fix)。execution/engine.py 230→290 lines (fill_provider production hook + paper_close flag + _maybe_close_for_leg 仅成功 leg fires + ExecutionResult.stop_loss surface + 大小写规范化 bug fix — BUY/SELL 大小写不一致致 PnL 符号反向, T4 没关仓所以是 latent)。cli_arbitrage.py 230→320 lines (run --paper-close + status 用 snapshot 暴露 realized PnL/balance/roi_pct + close 子命令 + make close-arb)。m2 test 42→63 green (+14 tracker + 4 execution close + 3 CLI close)。Pre-T5 两个 latent bug (roi_pct AttributeError + 大小写翻转 PnL) 被关仓路径强制暴露后修+锁。

## Phase 2 Plan Progress (`02-1-PLAN.md` — Revision 9 locked 2026-06-06)
- ✅ **T1** signal & execution models — `models/signal.py` + `models/slippage.py` (commit `08a13d3`)。Body 仍未对齐 (signal.py 概念膨胀), 推到 T2 完成后回头校正
- ✅ **T2** Slippage Model = fee-differential cross-venue (Revision 4 locked, SESSION 36 closed) — code 320 行 + 7 测试 green (4 existing + 3 IMDEA Type-2)。fee_diff_bps BUY+clob_maker = 60bps locked; SELL matrix locked; estimate_cross_execution_savings unit-economics 在 [$0.10, $20] IMDEA band 内
- ✅ **T3** Routing Engine slippage-aware (Revision 6 locked, SESSION 36 closed) — `routing/engine.py` extended to inject `SlippageCalculator`, `_select_venue` consults `estimate_cross_execution_savings` for PM-vs-CLOB selection, `ExecutionLeg.estimated_cost` now reflects `SlippageResult.net_cost_dollars()` not naive `price × size`。6 → 12 routing tests green
- ✅ **T4** Execution Pipeline orchestration shell (Revision 7 locked, SESSION 36 closed) — `execution/engine.py` rewrote 130→230 lines: pluggable `leg_executor` callable, atomic abort-on-first-leg-fail invariant, per-leg retry, structured `ExecutionLegResult`, `ExecutionStatus.ABORTED` distinct from PARTIAL/FAILED, position tracker only mutates on success。0 → 8 execution tests green
- ✅ **T5** Position Tracker realization (Revision 9 locked, SESSION 37 closed) — `routing/position_tracker.py` 179→320 lines: `Fill` event model + `StopLossEvent` rich return + `close_position_with_fill` production close path + `open_positions()` public iterator + `check_stop_loss_event` 富返回。`execution/engine.py` 230→290 lines: `fill_provider` production hook + `paper_close` paper-mode lifecycle + `_maybe_close_for_leg` 仅成功 leg fires + `ExecutionResult.stop_loss` post-execution surface + side normalization bug fix。`cli_arbitrage.py` 230→320 lines: `run --paper-close` + `status` snapshot-driven (balance/realized_pnl/roi_pct/stop_loss) + `close` subcommand + `make close-arb`。 0 → 14 tracker tests + 8 → 12 execution tests + 4 → 7 CLI tests. Pre-T5 latent bugs caught: `PositionSnapshot.roi_pct` AttributeError (referenced non-existent field) + 大小写 BUY/SELL 不一致致 PnL 符号反向
- ⏸ **T6** Settings — `routing/config.py` 落地 (含 RoutingConfig/ExecutionConfig/PositionConfig/AppConfig); 后续可整合 m1 `polyarb/config.py` Settings + env-var 入口
- ✅ **T7** CLI Integration (Revision 8 locked, SESSION 36 closed) — `src/polyarb/cli_arbitrage.py`: `evaluate`/`run`/`status` typer commands。Makefile: `make eval-arb`/`run-arb`/`status-arb`。4 smoke tests green。T5 续扩了 status snapshot + close subcommand (+3 tests)
- ⏸ **T8** E2E test — 未做。范围: 整链 chaos test (partial fail / retry exhaust / network blowup) + paper-close 全 lifecycle 集成 + stop-loss 实际触发场景

## Test Count (m2)
- **63 tests green** (SESSION 37): `routing/test_engine` 12 + `routing/test_position_tracker` 14 (new — T5) + `models/test_signal` 11 + `models/test_slippage` 7 + `execution/test_engine` 12 (+4 T5) + `cli/test_arbitrage_cli` 7 (+3 T5)
- m2 test progression: 21 (post-T1) → 30 (T2/T3) → 38 (T4) → 42 (T7) → **63 (T5)**

## Session Continuity
**Stopped At:** SESSION 37 — T5 Position Tracker realization 闭环。`make run-arb paper_close=1` 全 lifecycle 可见; `make close-arb market_id=… exit_price=…` operator close。T6 Settings / T8 E2E / real venue adapter 是下一步
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

## Next Action (2026-06-06)

T5 已闭环, 选项按"用户价值面 / 工程纵深 / 跨线"分:

**A. 真实 venue adapter** (最直接的下一步用户价值) — 写非-no-op `leg_executor` + `fill_provider` 调 py-clob-client + 钱包 (Polygon EOA + USDC) + risk control (size cap / blacklist)。T4 已把 `leg_executor` 做成可插拔, T5 已把 `fill_provider` 做成可插拔, 这是接入点。从 paper 跨到真实成交的关键跳跃。需要: Polymarket API key + 测试钱包 + 小额初始资金。
   - 阻塞性问题: Polymarket 不接受新美国账户 (用户已知); 需要确认是否有可用账户

**B. T8 E2E chaos test** (工程纵深) — 整链 chaos: partial-fail / retry-exhaust / network blowup, paper-close 全 lifecycle 集成, stop-loss 实际触发场景。把 m2 pipeline 从"happy path 闭环"提升到"failure mode 闭环"。
   - 优势: 不依赖外部资源, 完全可在本地推进
   - 产出: 信任 m2 可投入小额实盘的工程证据

**C. T6 Settings consolidation** — `routing/config.py` 已存在 (RoutingConfig/ExecutionConfig/PositionConfig/AppConfig), 但 m2 跟 m1 `polyarb/config.py` Settings 还没整合。CLI 里这些是 typer flag 一对一映射, 没走 env var。T6 做法: 把 m2 dataclasses 接入 pydantic-settings 或 env-var 模式, 让生产部署能从 .env 配置整套 m2 行为。

**D. 跨线** — m1 Phase 05 D-13 阈值校准 / m5 phase 01 polywatch-mvp plan。两者都是其它能力线的下一步, 不阻塞 m2。

**已交付给用户 (累积)**:
- `make eval-arb mid=0.45 stake=1000` — 看 routed decision + slippage cost
- `make run-arb mid=0.45 stake=500` — paper-mode 端到端 execution (positions stay open)
- `make run-arb paper_close=1` — paper-mode 全 lifecycle (open then close at est. price)
- `make status-arb` — tracker state with balance/realized_pnl/roi_pct/stop_loss
- `make close-arb market_id=… exit_price=…` — operator-driven close via synth Fill

---

## Cross-workstream Cleanup (SESSION 11)（已完成）
- Phase 目录改名规则统一：`m2/phases/02-arbitrage-engine/`（原 `02-/` 因中文 phase 名 slug 化为空）
- m2 文档从 `.planning/phases/02-/` 错位位置搬正
- `src/polyarb/config/` 包目录与 `polyarb/config.py` 模块冲突：m2 dataclass 搬到 `routing/config.py`，`config.py` 单文件保持作为 m1 应用 Settings 命名空间
- T1 commit 漏 import 依赖（models/）已补全 commit `08a13d3`
