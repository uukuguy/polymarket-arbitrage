---
name: "02-CONTEXT"
description: "Phase 2 discuss-phase decisions: routing, pipeline, slippage/sizing"
type: "phase-context"
status: "discussed"
discussed_at: "SESSION 09"
phase: 2
---

# Phase 2 Context

## Discuss Phase Decisions (SESSION 09)

### Area 1: Routing Strategy → Polymarket-first ✓
- Reason: Polymarket AMM spread (15-25%) is the primary profit source; fast AMM execution locks price first
- Gamma hedges residual at near-zero spread

### Area 2: Execution Pipeline → Sequential ✓
- Polymarket market order first (locks spread)
- Gamma limit order second (hedges residual)
- If Polymarket fills → proceed to Gamma. If Polymarket misses → abort, zero exposure.

### Area 3: Slippage & Sizing Model → Dynamic Depth Estimation ✓
- Polymarket: query AMM depth; max_size = cumulative depth at ≤ 1% slippage threshold
- Gamma: limit order at BBO ± 0.05%; reject if outside
- Expected profit = size × (spread_15-25% − slippage_≤1% − gamma_spread_0%)

### Area 4: Integration
- Routing: Polymarket-first
- Pipeline: Sequential (Polymarket → Gamma)
- Sizing: Dynamic depth estimation with 1% slippage cap on Polymarket

## Input/Output Contract
- Input: arbitrage signal (legs, size, venues)
- Output: execution plan (route, size per leg, order type, expected slippage)

## Phase 1 Dependencies
- Polymarket spread data from Phase 1 market microstructure analysis
- Gamma BBO and depth from Phase 1 CLOB structure
- Slippage model calibrated against Phase 1 LIVE-RUN observations
