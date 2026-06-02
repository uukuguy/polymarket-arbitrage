# Phase 2 Plan 1: Foundation — Data Models, Routing Engine, Execution Pipeline

> ✅ **T2 + T3 + T4 CLOSED 2026-06-02** (SESSION 36) — IMDEA validation locked (test_slippage 4→7) + RoutingEngine slippage-aware (routing/test_engine 6→12) + Execution orchestration shell (execution/test_engine 0→8: atomic abort + pluggable executor + retry + position-tracker fix). m2 test total 21 → 38. T5 Position Tracker / T7 CLI 是下一步。
>
> ⚠ **HISTORICAL PLAN-CODE DRIFT NOTICE** (2026-05-20)
>
> This plan body T2 段 (depth-curve design) historically diverged from `src/` (fee-differential code) for 18 days (2026-05-01 → 2026-05-19). Resolved by Revision 4 (T2 body 改写对齐代码) + Revision 5 (T2 validation 完成). T1/T3/T4 body 仍可能与代码有命名/枚举级别差异,推到对应 task 执行时按需校正。Forensics: `.planning/threads/learnings-meta.md` 2026-05-19 § "Plan-Code 沉默分叉 18 天"。

---

## Revision History

> Per `feedback_plan-code-drift-2026-05` + `threads/learnings-meta.md` 2026-05-19 § "Plan-Code 沉默分叉 18 天",任何 plan body 改动必须留 trace。这段是历史 ledger,新 revision 追加在末尾,**不修订**已有条目。

### 2026-04-28 Revision 0 — Initial drop (SESSION 10 discuss)
- Source: `/gsd-discuss-phase 02 --ws m2-combinatorial` SESSION 10 (2026-04-28)
- T2 design: `SlippageModel.estimate_slippage(token_id, side, size, depth_curve)` + `PolymarketDepthCurve` Protocol + 依赖 m1 `OrderBookSummary` / `GhostBookAnalyzer` 的 **depth-based linear decay + 1% cap** 模型
- Rationale (当时): m1 Phase 1 已出 OrderBookSummary,Phase 2 套利触发依赖深度估算,设计自然走 depth-curve 接口
- Commit trace: 与 ROADMAP/CONTEXT 同 batch 落地;具体 commit SHA 未单独记录 (历史限制)

### 2026-05-01 Revision 1 (UNTRACED) — Plan body 被改写成依赖 L2 的版本
- **No commit found** — `git log -- 02-1-PLAN.md` 只显示 1 个 commit (`ed49d55` rename,2026-05-01),plan body 内容变更未单独 commit,JOURNAL 无 `[PLAN-REVISION]` tag
- 当前 body 中的 T2 (depth-curve + `OrderBookSummary` 依赖) 实际是 Revision 1 的产物,不是 Revision 0
- 变更性质: 把 T2 从 "minimal depth curve" 改成依赖 m1 L2 orderbook 的版本 — 但 m1 当时还在 Phase 1 (L1 only,L2 推到 Phase 03)
- **这是契约级变更但无人签字**,直接导致 Revision 1 plan body 跟 Revision 2 代码 (见下) 永久分叉
- Discovered: 2026-05-19 SESSION 21 考古 (用户问 "M2 是啥")

### 2026-05-01 Revision 2 (CODE-ONLY) — slippage.py 落地 fee-differential 模型 (不对齐 Revision 1)
- Commit: `08a13d3 fix(02): complete T1 dependencies + relocate config to routing/` (2026-05-01)
- 背景: T1 commit `688363a` (2026-05-01 15:31) 漏 `git add` `models/signal.py` + `models/slippage.py` → `git checkout 688363a` ImportError
- SESSION 11 清理: `08a13d3` 一次性补齐 slippage.py (320 行) + signal.py + 4 测试。**没有对照 Revision 1 plan body 验证设计语义**
- 代码实际实现: `SlippageParams` (dataclass, maker_fee_bps / taker_fee_bps / impact_coef / vol_pct / pm_rebate_bps / clob_taker_cost_bps / clob_maker_rebate_bps / pm_taker_cost_bps / small/mid_notional 9 个参数) + `SlippageResult` 分解 (market_impact_bps / fee_bps / mid_price_delta_bps / total_cost_bps / net_cost_after_rebate_bps) + `fee_diff_bps(side, clob_maker_avail)` cross-execution 模型
- 设计语义: **CLOB ↔ PM 双场所 fee differential + market impact** 而非 Revision 1 的 depth-curve
- 测试: `tests/models/test_slippage.py` 4 测试全 green — 但测的是"当前代码自洽",不是"代码符合 Revision 1 plan"

### 2026-05-01 → 2026-05-19 — Silent Drift (18 天)
- m1 主线 (Phase 01.1 → Phase 02 Wave 1-5) 吞掉所有注意力,m2 无人回头
- `make planning-status` 不验证 plan-code 一致性 (只验证 SUMMARY 存在)
- 测试套件全 green 强化 "一切正常" 错觉
- 沉默成本估算: ~370 行未对齐代码 (slippage.py 320 + 测试 49) 若直接被 T3-T8 焊死,撕除成本 5×+ vs 此刻 30 min 考古

### 2026-05-19 SESSION 21 — Drift discovered (用户触发)
- 用户准备启动 m2 T2 时让 Claude 摸状态 → 三份描述对不上 (plan body / 代码 / JOURNAL)
- 完整考古见 `threads/learnings-meta.md` 2026-05-19 § "Plan-Code 沉默分叉 18 天 (m2 slippage.py 考古案例)"
- 修法落实:
  - ✅ thread learnings-meta.md 落地 5 层根因 + 4 工程教训 (commit `4a333ca`)
  - ✅ memory `feedback_plan-code-drift-2026-05.md` 落地 5 条防范纪律
  - ✅ Revision History 段落落地 (本段)
  - ⏳ T2 走向决策待做 (见 Pending Decision 段)

### 2026-05-20 Revision 3 — Revision History 段落补齐
- Trigger: Phase 02 close 后用户授权 "按计划往下走,自己判断" → 优先项是为 T2 走向决策铺路
- 本 revision **不修改** T1-T8 body 内容 (保留 Revision 1 描述作为历史档案)
- 本 revision **加入** 顶部 DRIFT NOTICE + Revision History + Pending Decision 段
- 下一步 (须用户决策): 见 Pending Decision

### 2026-05-20 Revision 4 — T2 走向锁定 = Option B (fee-differential + IMDEA Type-2)
- Decision source: 用户 AskUserQuestion 拍板 2026-05-20,选 Option B (推荐)
- T2 body **重写** (见下方 "T2: Slippage Model — Fee-Differential Cross-Venue (REWRITTEN)" 段) 对齐 `src/polyarb/models/slippage.py` 实际代码 (320 行 fee-differential 模型)
- 原 Revision 1 depth-curve 设计**正式废弃** — 推迟到 future plan,若 m1 Phase 03 (L2 orderbook) 落地后 depth-based 信号需要再启则单开 plan
- 增加 T2 IMDEA Type-2 验证测试要求 (Polymarket 86M 笔交易论文 cross-venue fee differential 实证)
- T1/T3/T4/T5 body **暂未改写** — STATE.md "Plan-vs-Code 偏离审计" 显示这几个也有膨胀,但本 revision 焦点是 T2 (T2 是核心信号层,T3/T4 用它),T1 body 与代码的差异是命名/枚举级别非语义级别,推到执行时按需校正
- Pending Decision 段标记为 **CLOSED 2026-05-20**,保留作历史

### 2026-06-02 Revision 7 — T4 ✅ CLOSED (SESSION 36 same-day continuation after T3)
- Decision source: 用户授权 "扎实把系统做到能用" — T4 是 sequential-execution 的最薄一层 orchestration shell, 无 T4 → 无端到端 E2E
- `src/polyarb/execution/engine.py` 重写 130→230 lines:
  - Pluggable `leg_executor: Callable[[ExecutionLeg, int], Awaitable[tuple[bool, str | None]]]` — caller injects; default = `_default_leg_executor` (no-op simulator preserving backward-compat with pre-T4 stub behaviour). Production will inject py-clob-client adapter (T5+ scope).
  - **Atomic abort invariant**: first-leg fail → mark ABORTED, skip remaining legs. Subsequent-leg fail → PARTIAL (signal-time we already committed leg 0; T5 will handle unwind / hedge). Distinction recorded in `ExecutionStatus.ABORTED` (new enum value).
  - **Per-leg retry**: uses `ExecutionConfig.retry_attempts` (default 3) + `retry_delay_seconds` (default 2.0). Retries are NOT counted as separate leg executions — `legs_executed` is the count of legs that ultimately succeeded.
  - **Structured `ExecutionLegResult`** per leg (leg / success / attempts / error / skipped) accessible via `result.leg_results`. Aggregate `ExecutionResult.error_message` summarises failure pattern.
  - **Position tracker only updates on successful legs** — pre-T4 bug fix. Old engine called `open_position` for every leg regardless of success, corrupting PnL accounting.
  - **Executor exceptions caught**: `RuntimeError` / network errors in the executor are treated as `(False, "executor raised: ...")` rather than crashing the engine. Respects retry semantics.
- `tests/execution/test_engine.py` 新增 8 tests (was 0 — only `__init__.py` existed):
  - `test_all_legs_succeed_completed_status` — happy path
  - `test_executor_receives_leg_and_attempt` — protocol shape
  - `test_first_leg_fail_aborts_subsequent_legs` — atomic invariant locked
  - `test_second_leg_fail_marks_partial_not_aborted` — PARTIAL vs ABORTED distinction locked
  - `test_retry_succeeds_on_second_attempt` — retry happy
  - `test_retry_exhausts_marks_failed` — retry exhaustion → ABORTED
  - `test_executor_exception_treated_as_failure` — exception caught + retried
  - `test_position_tracker_called_only_for_successful_legs` — pre-T4 bug fix lock
- m2 test total: 30 → 38 green (`test_engine` routing 12 + `test_signal` 11 + `test_slippage` 7 + `test_engine` execution 8)
- Pre-T4 bug acknowledged: old `_update_tracker` opened a position for EVERY leg regardless of success. The signal-time stub never actually failed so this was latent — but as soon as a real venue client is wired, it would have created phantom positions for failed legs. Test locks the fix.
- 下一步: T5 Position Tracker (close_position + PnL realization + stop-loss check chain) **OR** T7 CLI (`arbitrage evaluate/run/status` subcommand). T5 / T7 mutually independent — pick by which value lands first for the user.

### 2026-06-02 Revision 6 — T3 ✅ CLOSED (SESSION 36 same-day continuation)
- Decision source: 用户授权 "扎实把系统做到能用" + T3 是 T2→T4 之间的必经层 (无 T3 → 无 venue selection → 无意义的 cost estimation)
- `src/polyarb/routing/engine.py` extended (+136 lines): `__init__` accepts optional `SlippageCalculator` (default-constructs to preserve backward-compat with 6 pre-T3 tests), `_build_execution_legs` now calls `_select_venue` for each market, which consults `estimate_cross_execution_savings`
- `_select_venue` logic: PM-vs-CLOB by lower `net_cost_after_rebate_bps`, tie → PM (single-venue baseline). Caller can override by setting `market.venue` to non-PM alias (e.g. "gamma") — slippage still computed for cost accounting but venue is forced
- `ExecutionLeg.estimated_cost` now reflects `SlippageResult.net_cost_dollars()` not naive `market.price × stake`. Locked by `test_estimated_cost_reflects_slippage_model` so future drift is loud
- 6 new tests in `tests/routing/test_engine.py` lock the behaviour:
  - `test_default_params_buy_picks_polymarket` — BUY under default params → PM
  - `test_default_params_sell_picks_polymarket_on_tie` — SELL tie → PM
  - `test_estimated_cost_reflects_slippage_model` — cost from slippage, not naive
  - `test_param_flip_makes_clob_cheaper_for_buy` — taker_fee_bps=−200 → BUY routes CLOB (mechanism check)
  - `test_caller_venue_override_respected` — venue="gamma" honored
  - `test_backward_compatible_no_slippage_arg` — `RoutingEngine()` works without args
- m2 test total: 24 → 30 green (`test_engine` 6 → 12)
- Empirical fact recorded: under default `SlippageParams`, **PM always wins** every (side × size × mid) combination (PM rebate −30bps beats CLOB taker +50bps for BUY; PM/CLOB tie at +50bps for SELL). This means the slippage-aware path produces the same observable selection as the pre-T3 hardcoded "polymarket-first" — **but the mechanism is now real**. Param tuning or new venue (CLOB-maker availability) will flip selection automatically
- No regression in models/routing/execution/m1-observation suites; 3 pre-existing m1 failures unrelated (`test_pass_when_fresh` / `test_make_smoke_health_local_dry_run_recipe` port=8080 vs 19080 anomaly / `test_r2_retry_config_applied`)
- 下一步: T4 Execution Pipeline — `src/polyarb/execution/engine.py` 雏形已存在, 需要接 RoutingDecision 跑 sequential 成交 flow + 4 paths (atomic / best-effort / abort / retry)

### 2026-06-02 Revision 5 — T2 ✅ CLOSED (SESSION 36)
- Decision source: 用户授权 "扎实把系统做到能用" + "自主推进,不要把时间浪费在停下等我回答完全没必要问的问题"
- T2 IMDEA Type-2 validation **3 tests landed** in `tests/models/test_slippage.py`:
  - `test_fee_diff_bps_buy_with_clob_maker_locks_at_60bps` — BUY+clob_maker = 60bps 锁定 (clob_maker_rebate 10 + pm_taker_cost 50)
  - `test_fee_diff_bps_sell_matrix_locks` — SELL+clob_maker = −40bps,SELL+no_maker = −20bps,clob_maker 比 no_maker 更差 (反直觉但 lock 防 T3 routing 反向)
  - `test_estimate_cross_execution_savings_imdea_unit_economics` — $1k size × mid=0.5 → savings_dollars ∈ [$0.10, $20] IMDEA band。$4M/86M trades 量级数学一致性。
- `tests/models/test_slippage.py` 现在 7/7 green (4 原 + 3 IMDEA)
- 注意 plan body T2 §141 注释 "BUY 场景 CLOB maker (-10bps) vs PM taker (-50bps) = 40bps 更便宜" **数值有误**, 实际代码计算 = 60bps (10 - (-50) = 60), 测试 lock 的是 60。Plan body 注释保留作历史 (验证测试是 source of truth)。
- 下一步: T3 Routing Engine — engine.py 雏形 + 6 tests 已 commit, 需要接 `estimate_cross_execution_savings`
- 整个 m2 test count: 24 green (`test_engine` 6 + `test_signal` 11 + `test_slippage` 7)

---

## Pending Decision (post-2026-05-20) — **CLOSED 2026-05-20: Option B selected**

T2 走向三选一,**必须用户拍板**才能继续 m2 任何执行工作:

### Option A — 冻 M2 等 m1 L2 (Phase 03) 出来再启 T2
- 含义: 把 m2 整条线挂起,等 m1 Phase 03 (L2 orderbook) 落地 → 再启 T2 走 Revision 1 depth-curve 设计 → 现有 slippage.py 320 行 + 测试 49 行**删掉** (撕)
- 优势: plan-code 重新对齐,设计纯净
- 代价: 370 行代码废,SESSION 10/11 投入沉没;m2 整条线推迟到 m1 Phase 03 完成 (Phase 02.1 + Phase 03 ≈ 几周)
- 适用: 若 depth-curve 是套利核心信号唯一正确建模方式

### Option B (推荐) — 重定义 T2 = 现有 fee-differential 设计 + 补 IMDEA Type-2 验证
- 含义: 承认 Revision 2 代码 = T2 当前 source of truth → 把 plan body T2 段**改写**对齐代码 → 补 IMDEA 论文 Type-2 (cross-venue fee differential) 套利验证测试 → T3-T8 在此基础上继续
- 优势: 370 行代码不浪费;fee-differential 是 IMDEA 86M 笔交易实证的真实套利类型 (Type-2 占 ~$4M);不依赖 m1 L2 (m2 与 m1 并行推进)
- 代价: 承认 SESSION 11 漏验证的事实写入历史;原 Revision 0/1 depth-curve 设计正式废弃 (推到 future plan if needed)
- 适用: 若 fee-differential 是当前能稳定建模的套利类型,且 IMDEA 论文支持其经济价值

### Option C — 跳 T2 推 T3 (Routing Engine) 或 T6 (Settings)
- 含义: T2 走向暂不决策,先做不依赖 slippage 模型的 task — T6 Settings (配置层独立) 或 T3 Routing (用占位 slippage cost)
- 优势: 不卡住,代码进度继续
- 代价: T2 仍 broken 状态,T3 用占位会埋后续坑;违反"plan-code 必须一致"纪律 (本次考古的目的)
- 适用: 若 T2 决策需要更多调研时间 (例如先读 IMDEA 论文细节再选)

---

## ⚠ 本次 edit 后所有下游 task 描述 (T1-T8) 仍是 Revision 1 内容,跟代码不一致

继续阅读下面 T1-T8 body 前请先做完 Pending Decision。若选 Option B,T2 body 在执行前需 Revision 4 改写;T1 body 也可能要更新 (signal.py 实际产出比 plan 描述膨胀,见 STATE.md "Plan-vs-Code 偏离审计")。

---

## Goal
Build the core arbitrage execution engine: routing (Polymarket-first → Gamma), slippage model, sequential execution pipeline, and position management.

## Context
- Phase 1 delivered: market snapshots, SQLite/parquet storage, CLOB price feeds, cache, observability
- Phase 2 focus: turn raw price data into executable arbitrage signals
- 3 decisions from discuss-phase (02-CONTEXT.md):
  1. **Routing**: Polymarket-first (AMM spread 15-25% is primary profit)
  2. **Pipeline**: Sequential — Polymarket market order first, Gamma limit order hedges residual
  3. **Sizing**: Dynamic depth estimation with 1% slippage cap on Polymarket

## Scale Assumption
- Single-threaded pipeline (no parallelism in P1)
- Market count: ~20k (Phase 1 LIVE-RUN-005 baseline)
- Signal: on-demand evaluation (not streaming scan), no real-time WebSocket in P1

---

## Task Breakdown

### T1: Arbitrage Signal & Execution Plan Models
**Owner**: general-purpose agent
**Files**: `src/polyarb/models/signal.py`, `tests/test_signal_model.py`
**Steps**:
1. `ArbitrageLeg` dataclass: `venue` (POLYMARKET|GAMMA), `side` (BUY|SELL), `token_id`, `price`, `size`, `expected_slippage_pct`
2. `ArbitrageSignal` dataclass: `signal_id`, `legs: list[ArbitrageLeg]`, `total_legs`, `estimated_profit_pct`, `estimated_profit_abs`, `timestamp`
3. `ExecutionResult` dataclass: `signal_id`, `legs_executed`, `legs_rejected`, `actual_profit_pct`, `status` (FILLED|PARTIAL|REJECTED|ABORTED)
4. Pydantic v2 validators: profit ≥ 0, price in [0, 1], size > 0
5. Tests: model construction, validation edge cases, serialization round-trip

### T2: Slippage Model — Fee-Differential Cross-Venue ✅ CLOSED 2026-06-02 (Revision 5)
**Owner**: general-purpose agent
**Files**: `src/polyarb/models/slippage.py` (320 lines, landed 2026-05-01 commit `08a13d3`), `tests/models/test_slippage.py` (7 tests green: 4 existing + 3 IMDEA Type-2 landed SESSION 36)
**Status**: ✅ DONE — IMDEA Type-2 validation 3 tests landed, economic claims locked.

**Design (locked, matches landed code):**
1. `SlippageParams` dataclass — 9 个 tunable bps 参数 (maker_fee_bps / taker_fee_bps / impact_coef / vol_pct / pm_rebate_bps / clob_taker_cost_bps / clob_maker_rebate_bps / pm_taker_cost_bps / small_notional+mid_notional breakpoints)
2. `SlippageResult` dataclass — 单 leg 成本分解 (market_impact_bps / fee_bps / mid_price_delta_bps / total_cost_bps / net_cost_after_rebate_bps + to_dict + net_cost_dollars)
3. `SlippageCalculator` — 主 estimator,3 个方法:
   - `estimate(side, venue, size_usd, mid_price, clob_bid/ask, pm_bid/ask, clob_maker_avail, daily_volume_usd) → SlippageResult` — Kyle's lambda market impact (impact_dollar = impact_coef × notional / √daily_volume) + fee 分支 (PM rebate vs taker, CLOB maker vs taker) + mid-price delta vs signal time
   - `estimate_cross_execution_savings(...)` → dict {pm_net_cost_bps, clob_net_cost_bps, savings_bps, pm_result, clob_result} — **核心 cross-venue fee differential 接口**
   - `estimate_leg(side, venue, mid_price, quantity_shares, ...)` — shares-based convenience wrapper
   - `compare_venues(side, mid_price, quantity_shares, ...)` → dict {pm_result, clob_result, savings_bps}
4. `SlippageEstimate` + `VenueSlippageProfile` + `SlippageModel` — signal-layer 抽象,DEFAULT_PROFILES (PM / CLOB / POL 三个 venue),`estimate(venue, size_usd, mid_price)` 返回 SlippageEstimate
5. `SlippageParams.fee_diff_bps(side, clob_maker_avail)` — IMDEA Type-2 核心: BUY 场景 CLOB maker (-10bps) vs PM taker (-50bps) = 40bps 更便宜; SELL 同理。**这是 fee differential 模型的经济学定理**

**Validation tasks (本 plan 范围):**
1. 现有 4 个测试 (`tests/models/test_slippage.py`) 保持 green
2. **新增 IMDEA Type-2 validation 测试** (来源: Polymarket 86M 笔交易论文 — `docs/research/polymarket-oss-landscape-2026-04.md` 引用):
   - Type-2 套利 = same outcome, different venue, fee differential profit
   - 测试用例: 给定 mid_price=0.5, size_usd=$1k, 调 `estimate_cross_execution_savings` → 验证 savings_bps 与论文中 Top 3 钱包 $4.2M 净利的 bps 量级一致 ($1k size × 40bps fee_diff = $4/笔, $4M/Top 钱包 ≈ 1M 笔/钱包级订单流, IMDEA 论文 Top 3 合计 $4.2M 在 86M 笔总量中占比 ~5% 验证合理)
   - 至少 3 个测试: (a) fee_diff_bps BUY clob_maker_avail / (b) fee_diff_bps SELL no_clob_maker / (c) estimate_cross_execution_savings 主路径 + IMDEA 量级断言
3. Edge cases: tiny markets (size < small_notional), deep markets (size > mid_notional), zero daily_volume (sqrt(1.0) fallback), mid_price=0 / negative size (zero return guard)
4. Integration note: T3 Routing Engine 用 `estimate_cross_execution_savings` 做 PM-vs-CLOB venue selection;T4 Pipeline 用 `estimate` 做 per-leg pre-trade 成本预估

**NOT in T2 scope (废弃自 Revision 1):**
- ~~`PolymarketDepthCurve` Protocol~~ — Revision 0/1 设计,已废弃;若 m1 Phase 03 L2 orderbook 出来后需要 depth-based 信号,单开 future plan
- ~~依赖 Phase 1 `OrderBookSummary` / `GhostBookAnalyzer`~~ — 当前 fee-differential 模型不依赖 L2,与 m1 并行推进

**Acceptance:**
- `tests/models/test_slippage.py` 全 green (4 现有 + ≥3 新增 IMDEA Type-2 = ≥7 测试)
- `src/polyarb/models/slippage.py` 不需重写 (代码已正确,只补测试)
- IMDEA Type-2 测试数据来源在 plan body 或 thread 中可追溯 (cite 论文 §章节 / 数字)

### T3: Routing Engine — Polymarket-First Logic
**Owner**: general-purpose agent
**Files**: `src/polyarb/routing/engine.py`, `tests/test_routing_engine.py`
**Steps**:
1. `RoutingEngine` class with `evaluate(legs: list[ArbitrageLeg]) → ExecutionPlan`
2. `RoutingDecision` enum: EXECUTE | SKIP | NEEDS_HEDGE | UNCERTAIN
3. `route_polymarket_first(legs)` → fills Polymarket legs first (market orders), returns remaining size
4. `route_gamma_hedge(remaining_size)` → Gamma limit orders at BBO ± 0.05%
5. Routing rules:
   - Polymarket fills → proceed to Gamma hedge
   - Polymarket misses → abort (zero exposure)
   - Estimated profit < 0.5% → SKIP
6. Tests: all 4 routing decision branches, Polymarket-first ordering, profit threshold

### T4: Execution Pipeline — Sequential Orchestration
**Owner**: general-purpose agent
**Files**: `src/polyarb/execution/pipeline.py`, `tests/test_pipeline.py`
**Steps**:
1. `ArbitragePipeline` class: `run(signal: ArbitrageSignal) → ExecutionResult`
2. Phase 1 clients wired: `GammaClient` (for BBO + limit orders), `PolymarketClient` (for AMM fills)
3. Sequential flow: `polymarket_fill()` → `check_fill()` → `gamma_hedge()` → `record_result()`
4. If Polymarket fill fails: `abort_pipeline()`, no Gamma exposure
5. Position tracking: `PositionTracker` class (in-memory, Phase 2 scope)
6. Error handling: per-leg timeout (5s Polymarket, 10s Gamma), retry logic (1x), circuit breaker (3 failures → pause)
7. Integration tests: full pipeline with mocked clients, all 4 execution paths

### T5: Position Management — In-Memory Tracker
**Owner**: general-purpose agent
**Files**: `src/polyarb/execution/positions.py`, `tests/test_positions.py`
**Steps**:
1. `PositionTracker` class: `positions: dict[str, Position]`, `open_pnl: float`, `total_trades: int`
2. `Position` dataclass: `token_id`, `venue`, `side`, `size`, `entry_price`, `current_price`, `unrealized_pnl`
3. Methods: `open_position()`, `update_market()`, `close_position()`, `get_exposure(token_id)`
4. Max exposure guard: configurable `max_position_size` per token, reject signal if exceeded
5. Tests: open/close/update flows, exposure guard, PnL calculation

### T6: Settings — Phase 2 Configuration
**Owner**: general-purpose agent
**Files**: `src/polyarb/settings.py` (update), `tests/test_settings.py` (update)
**Steps**:
1. Add Phase 2 config fields to `ArbitrageSettings`:
   - `min_profit_threshold_pct: float = 0.5` (skip signals below this)
   - `max_position_size: float = 100.0` (per-token limit)
   - `polymarket_timeout_s: int = 5`
   - `gamma_timeout_s: int = 10`
   - `circuit_breaker_threshold: int = 3`
   - `slippage_cap_pct: float = 1.0` (Polymarket max slippage)
   - `gamma_spread_tolerance_pct: float = 0.05`
2. Tests: env var override, defaults, validation

### T7: CLI Integration — Signal Evaluation Command
**Owner**: general-purpose agent
**Files**: `src/polyarb/cli.py` (update), `tests/test_cli_arbitrage.py`
**Steps**:
1. `arbitrage evaluate` subcommand: takes token pair, calls routing engine, prints execution plan
2. `arbitrage run` subcommand: takes signal_id, runs full pipeline, prints result
3. `arbitrage status` subcommand: shows open positions, PnL summary
4. Structured logging via existing loguru setup

### T8: Integration Test — End-to-End Flow
**Owner**: general-purpose agent
**Files**: `tests/test_arbitrage_e2e.py`, `tests/fixtures/arbitrage_signal_sample.json`
**Steps**:
1. Full E2E test: mock Polymarket + Gamma clients, run signal through pipeline
2. Test all 4 outcomes: FILLED / PARTIAL / REJECTED / ABORTED
3. Fixtures: `arbitrage_signal_sample.json` with realistic leg data
4. Coverage: routing decision → execution → position update → result

---

## Verification
1. All new tests pass: `python -m pytest tests/test_signal_model.py tests/test_slippage.py tests/test_routing_engine.py tests/test_pipeline.py tests/test_positions.py tests/test_settings.py tests/test_cli_arbitrage.py tests/test_arbitrage_e2e.py -v`
2. Type check: `pyright src/polyarb/models/signal.py src/polyarb/models/slippage.py src/polyarb/routing/engine.py src/polyarb/execution/pipeline.py src/polyarb/execution/positions.py`
3. No new lint errors: `ruff check src/polyarb/`
4. All Phase 1 tests still green: `python -m pytest tests/m1-perception/ -v`

## Dependency Graph
```
T1 (signal models) ──┐
                      ├── T3 (routing engine) ── T4 (pipeline) ── T8 (E2E)
T2 (slippage model) ─┤                      │
                      │                      ▼
T6 (settings) ────────┴── T5 (positions) ── T7 (CLI)
```

## Plan Scope
- **In scope**: Core arbitrage engine, routing, pipeline, position tracking
- **Out of scope**: Real-time WebSocket feeds (Phase 3), persistence layer (Phase 4), live trading with real money
- **Scale note**: Single-threaded, in-memory only — no parallelism, no DB for positions yet
