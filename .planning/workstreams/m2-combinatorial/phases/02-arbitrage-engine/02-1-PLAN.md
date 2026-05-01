# Phase 2 Plan 1: Foundation — Data Models, Routing Engine, Execution Pipeline

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

### T2: Slippage Model — Polymarket Depth Estimation
**Owner**: general-purpose agent
**Files**: `src/polyarb/models/slippage.py`, `tests/test_slippage.py`
**Steps**:
1. `SlippageModel` class with `estimate_slippage(token_id, side, size, depth_curve)` → (slippage_pct, max_acceptable_size)
2. `PolymarketDepthCurve`: estimate AMM depth from order book (Phase 1 `OrderBookSummary` data)
3. `DepthCurve` protocol for pluggable curves
4. Unit tests: slippage at 0.5x/1x/2x depth, edge cases (tiny markets, deep markets)
5. Integration note: Phase 1 `GhostBookAnalyzer` output feeds this model

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
