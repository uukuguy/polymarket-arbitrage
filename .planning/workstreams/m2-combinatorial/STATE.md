---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
current_phase: 6
status: ready
stopped_at: Phase 6 planned; H-004 ready for TDD execution
last_updated: "2026-07-17T04:33:10.644Z"
last_activity: 2026-07-17
progress:
  total_phases: 5
  completed_phases: 4
  total_plans: 5
  completed_plans: 4
  percent: 80
---

# Project State — m2-combinatorial（组合套利能力线）

## Current Position

Phase: 6 (Unit-Safe Execution Accounting)
Plan: 0 of 1 complete
**Status:** Ready for execution
**Current Phase:** 6
**Last Activity:** 2026-07-17
**Last Activity Description:** Phase 05 planning complete — 1 plans ready

## Phase 2 Plan Progress (`02-1-PLAN.md` — ✅ CLOSED 2026-06-07)

- ✅ **T1** signal & execution models — `models/signal.py` + `models/slippage.py` (commit `08a13d3`)
- ✅ **T2** Slippage Model = fee-differential cross-venue (SESSION 36) — code 320 行 + 7 tests
- ✅ **T3** Routing Engine slippage-aware (SESSION 36) — 6 → 12 routing tests
- ✅ **T4** Execution Pipeline orchestration shell (SESSION 36) — 0 → 8 execution tests
- ✅ **T5** Position Tracker realization (SESSION 37) — Fill + close_path + StopLoss + CLI close; 42 → 63 tests
- ✅ **T6** Settings env-var (SESSION ~38, 2026-06-07) — `routing/config.py` BaseSettings POLYARB_ prefix; 16 tests
- ✅ **T7** CLI Integration (SESSION 36) — `evaluate`/`run`/`status` + Makefile; 4+3 tests
- ✅ **T8** E2E chaos tests (SESSION ~38, 2026-06-07) — 25 E2E tests: all 4 outcomes + stop-loss + paper-close + fill-provider

## Phase 3 Plan Progress (`03-01-PLAN.md` — ✅ CLOSED 2026-07-17)

- ✅ Repository contract + in-memory copy/commit/rollback/replay semantics
- ✅ SQLite account/open-position/operation projection under `BEGIN IMMEDIATE`
- ✅ Repository-backed tracker with shared reads and domain transition closures
- ✅ Stable engine operation IDs for open and paper/venue close paths
- ✅ Cross-process CLI lifecycle with `db=` Makefile entry points
- ✅ Teaching chapter 13, phase summary, learnings, and operator smoke

## Phase 4 Plan Progress (`04-01-PLAN.md` — ✅ CLOSED 2026-07-17)

- ✅ Public immutable `OperationReceipt` lookup in memory and SQLite
- ✅ Tracker receipt delegation and stable venue `Fill.fill_id`
- ✅ Retry-safe operator close identity in CLI and Makefile
- ✅ True subprocess response-loss recovery proof
- ✅ Teaching, SUMMARY, learnings, JOURNAL, and H-002 climb closure

## Phase 5 Plan Progress (`05-01-PLAN.md` — READY)

- ✅ Frozen Money value and exact tracker domain
- ✅ Transactional SQLite v2 migration and INTEGER authority
- ✅ Tagged money receipts and CLI restart recovery
- ✅ Teaching chapter 14, SUMMARY, learnings, and zero planning drift
- ✅ H-003 climb planning/unit/integration/CLI/restart = 100/100 each

## Test Count (m2)

- **104 tests green**: `routing/test_engine` 12 + `routing/test_position_tracker` 14 + `routing/test_config` 16 (T6) + `models/test_signal` 11 + `models/test_slippage` 7 + `execution/test_engine` 12 + `execution/test_arbitrage_e2e` 25 (T8) + `cli/test_arbitrage_cli` 7
- m2 test progression: 21 → 30 → 38 → 42 → 63 → **104**
- **Phase 3 corrected full gate: 130 tests green** (repository 14 + tracker 18 + expanded engine/CLI/config/process coverage)
- **Phase 4 corrected full gate: 145 tests green**; climb H-002 planning/unit/integration/CLI/restart = 100/100 each
- **Phase 5 corrected full gate: 187 tests green**; exact migration/restart smoke proves INTEGER micros and tagged receipt

## Session Continuity

**Last Resumed:** 2026-07-17
**Stopped At:** Phase 6 planned; H-004 ready for TDD execution
**Resume File:** .planning/workstreams/m2-combinatorial/phases/06-unit-safe-execution-accounting/06-01-PLAN.md

## Next Action (2026-07-17)

H-004 已规划：先拆分 exact shares Quantity 与 pUSD cost basis，再进入 H-005 partial fills。

第一条命令：

`uv run pytest tests/routing/test_quantity.py -q`

**已交付给用户 (累积)**:

- `make eval-arb mid=0.45 stake=1000` — 看 routed decision + slippage cost
- `make run-arb mid=0.45 stake=500` — paper-mode 端到端 execution
- `make run-arb paper_close=1` — paper-mode 全 lifecycle (open then close)
- `make status-arb` — tracker state with balance/realized_pnl/roi_pct/stop_loss
- `make close-arb market_id=… exit_price=…` — operator-driven close
- `make close-arb market_id=… exit_price=… operation_id=…` — 响应丢失后跨进程恢复原 close receipt
- `make run-arb/status-arb/close-arb db=…` — 跨进程共享 SQLite paper account
- Env var config: `POLYARB_MIN_PROFIT_THRESHOLD_PCT=2.0 make eval-arb` etc.

---

## Cross-workstream Cleanup (SESSION 11)（已完成）

- Phase 目录改名规则统一
- m2 文档从错位位置搬正
- `polyarb.config` namespace 冲突修复
- T1 commit 漏 import 依赖补全
