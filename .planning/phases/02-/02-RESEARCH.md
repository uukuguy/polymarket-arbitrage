# Phase 2: Foundation — Routing Engine, Slippage Model, Execution Pipeline — Research

**Researched:** 2026-05-01
**Domain:** Arbitrage execution engine — routing, slippage estimation, sequential execution pipeline
**Confidence:** MEDIUM

## Summary

Phase 2 builds the core execution engine for the Polymarket-first arbitrage system. The scaffolding (models, routing, execution, position tracking) already exists from prior work — skeleton classes, dataclass models, async patterns. The actual implementation work falls into three buckets: (1) wiring the signal-layer data models to the routing engine's leg-building, (2) filling in the depth-estimation + slippage model for Polymarket AMM liquidity, and (3) implementing the sequential execution pipeline (Polymarket market order first, Gamma limit order hedges residual).

**Primary recommendation:** The scaffold is already in `src/polyarb/routing/` and `src/polyarb/execution/` — implement the actual routing logic in `routing/engine.py` (currently a stub), flesh out the Polymarket AMM depth estimator, and implement real execution in `execution/engine.py` (currently just `sleep(0.01)`).

---

## User Constraints (from CONTEXT.md)

### Locked Decisions
- **Routing:** Polymarket-first (AMM spread 15-25% is primary profit)
- **Pipeline:** Sequential — Polymarket market order first, Gamma limit order hedges residual
- **Sizing:** Dynamic depth estimation with 1% slippage cap on Polymarket
- **Implementation scope:** Memory model, sync REST calls, single-threaded

### Claude's Discretion
- Order management, retry logic, execution timeouts, position tracking details

### Deferred Ideas
- WebSocket real-time execution, multi-threaded pipeline, wallet/auth integration

---

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|---|---|---|---|
| Signal ingestion | API/Backend | — | Gamma REST → signal models |
| Routing decision | API/Backend | — | RoutingEngine.route() — CPU bound |
| Polymarket AMM depth estimation | API/Backend | — | CLOB order book + inferred AMM depth |
| Gamma limit order submission | API/Backend | — | py-clob-client `create_and_post_order()` |
| Sequential execution orchestration | API/Backend | — | ExecutionEngine.execute() |
| Position tracking | API/Backend | — | PositionTracker class |
| Slippage model | API/Backend | — | SlippageModel + SlippageCalculator |

---

## Standard Stack

### Core
| Library | Version | Purpose | Why Standard |
|---|---|---|---|
| `py-clob-client` | 0.34.6 | Gamma CLOB read/write | Official Polymarket SDK |
| `httpx` | via dependency | Async HTTP for Gamma REST | Already in use (GammaClient) |
| `asyncio` | stdlib | Sequential execution orchestration | Already in use |
| `dataclasses` | stdlib | Signal, leg, execution models | Already in use |
| `loguru` | in use | Structured logging | Already in use |
| `tenacity` | in use | Retry with exponential backoff | Already in use |

### Supporting
| Library | Version | Purpose | When to Use |
|---|---|---|---|
| `aiolimiter` | already present | Rate limiting on Gamma API calls | Throttling Gamma API calls |
| Phase 1 CLOB reader | existing | `ClobReaderClient` for order book data | Reading Polymarket liquidity |

**Installation:** No new packages needed. Phase 1 already installed `py-clob-client`, `httpx`, `aiolimiter`, `tenacity`, `loguru`.

---

## Architecture Patterns

### System Architecture Diagram

```
[ArbitrageSignal]
       │
       ▼
[RoutingEngine.route()] ──checks──▶ [RoutingConfig gates]
       │                                        │
       ▼                                        ▼
[Build Polymarket-first legs]           [Filter: min_profit_threshold, min_leg_size]
       │
       ├──────────────────────────────────────────────────────────┐
       ▼                                                          ▼
[Leg 1: Polymarket AMM]                                  [Leg 2: Gamma CLOB]
  Action: BUY/SELL                                          Action: HEDGE
  OrderType: MARKET (no limit)                              OrderType: LIMIT (BBO ± 0.05%)
  Size: dynamic (1% slippage cap)                           Size: residual after PM fill
       │                                                          │
       └────────────────────┬─────────────────────────────────────┘
                            ▼
                 [Sequential Executor]
                   1. Poll PM fill status
                   2. If FILLED → submit Gamma limit order
                   3. If MISSED → ABORT (zero exposure)
                   4. Track position in PositionTracker
                            │
                            ▼
                  [PipelineResult]
                     FILLED / PARTIAL / ABORTED
```

**Key insight from py-clob-client source audit:**
- `ClobClient.create_order()` creates + signs a limit order (L1 auth required, creates `SignedOrder` object)
- `ClobClient.post_order()` submits a signed order to the CLOB (L2 auth required)
- `ClobClient.create_and_post_order()` = convenience combo of the two above
- `ClobClient.create_market_order()` creates a market order — calls `calculate_market_price()` which reads the order book to compute fill price at given size
- `ClobClient.post_orders()` / `ClobClient.post_order()` = submission to CLOB
- **No market-order submit endpoint** — market orders are priced via `calculate_market_price()` against the CLOB book, then submitted as signed orders
- **Gamma does NOT have a true AMM market-order endpoint** — the "market order" on Polymarket is actually a CLOB market order against the order book, NOT the AMM directly
- The AMM is accessed via **AMM liquidity pool** — for hedging residual after PM fills, use CLOB limit orders

### Recommended Project Structure

```
src/polyarb/
├── routing/
│   ├── engine.py           # FILL IN: real Polymarket-first routing logic
│   ├── depth_estimator.py  # NEW: AMM depth estimation for sizing
│   ├── orchestrator.py     # Already exists (wire signal → routing → execution)
│   └── position_tracker.py # Already exists
├── execution/
│   ├── engine.py           # FILL IN: real sequential execution (PM first, Gamma hedge)
│   └── order_manager.py    # NEW: Polymarket PM + Gamma CLOB order submission
├── models/
│   ├── signal.py            # Already exists (ArbitrageSignal, ExecutionPlan, etc.)
│   ├── slippage.py         # FILL IN: AMM-specific slippage model
│   └── __init__.py
├── clients/
│   ├── gamma_client.py     # Already exists (Phase 1)
│   └── clob_client.py      # Already exists (Phase 1 ClobReaderClient)
└── config/
    └── settings.py         # Already exists (RoutingConfig, ExecutionConfig, etc.)
```

### Pattern 1: Polymarket-First Sequential Routing

**What:** Polymarket AMM fills first (market order), Gamma CLOB hedges residual (limit order).
**When to use:** All Phase 2 arbitrage routes.
**Example:**
```python
async def execute(self, decision: RoutingDecision) -> ExecutionResult:
    # Step 1: Execute Polymarket leg (market order via CLOB)
    pm_result = await self._execute_pm_leg(decision.legs[0])
    if not pm_result.success:
        return ExecutionResult(status=ExecutionStatus.ABORTED, ...)

    # Step 2: Hedge residual on Gamma (limit order)
    residual = decision.total_stake - pm_result.filled_size
    if residual > self.config.min_hedge_size:
        gamma_result = await self._execute_gamma_hedge(residual)
        # Gamma is best-effort — partial fill is OK

    return ExecutionResult(...)
```

### Pattern 2: AMM Depth Estimation via Order Book

**What:** Use CLOB order book (`asks`/`bids`) as proxy for AMM depth, since Polymarket routes AMM liquidity through CLOB.
**When to use:** Sizing Polymarket legs.
**Example:**
```python
def estimate_amm_depth(self, token_id: str, side: str, max_slippage_pct: float) -> float:
    """Return max $ size that can be filled at ≤ max_slippage_pct slippage."""
    book = self._clob.get_order_book(token_id)
    levels = book.asks if side == "BUY" else book.bids

    mid = self._mid_price(book)
    threshold = mid * (1 + max_slippage_pct / 100)

    cumulative = 0.0
    for level in levels:
        price = float(level.price)
        size = float(level.size)
        if side == "BUY" and price > threshold:
            break  # slippage exceeds cap
        cumulative += size * price

    return cumulative  # max $ size at ≤ max_slippage_pct
```

### Pattern 3: Limit Order on Gamma with BBO ± Tolerance

**What:** Submit Gamma limit order at current BBO ± tolerance (e.g., ±0.5%).
**When to use:** Hedging residual after Polymarket fills.
**Example:**
```python
async def submit_gamma_hedge(
    self,
    token_id: str,
    side: str,
    size: float,
    bbo_bid: float,
    bbo_ask: float,
    tolerance_pct: float = 0.5,
) -> OrderResult:
    mid = (bbo_bid + bbo_ask) / 2
    limit_price = mid * (1 - tolerance_pct / 100) if side == "BUY" else mid * (1 + tolerance_pct / 100)

    order_args = OrderArgs(
        token_id=token_id,
        price=limit_price,
        size=size,
        side=side,
    )
    return await self._clob.create_and_post_order(order_args)
```

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| Order signing | Build custom signing | `ClobClient.create_order()` / `py_builder_signing_sdk` | Signing requires secret keys, nonce management, proper hashing |
| Fee calculation | Hard-code fee rates | `ClobClient.get_fee_rate()` per token | Fees vary by token and maker/taker side |
| Market price estimation | Estimate manually | `ClobClient.calculate_market_price()` | Reads order book levels to compute VWAP for given size |
| AMM depth inference | Guess | CLOB order book as proxy | Phase 1 already has `ClobReaderClient.get_books()` |
| Retry logic | Implement from scratch | `tenacity` | Already in use in GammaClient |

---

## Common Pitfalls

### Pitfall 1: Polymarket AMM Depth vs. CLOB Order Book Confusion
**What goes wrong:** Implementing against the CLOB order book as if it IS the AMM depth — but they diverge.
**Why it happens:** Phase 1 delivered CLOB data (`/book` for sizes, `/prices` for true prices). But the Polymarket AMM has its own liquidity pool separate from CLOB. The AMM spread (15-25%) is a maker spread — it doesn't appear as CLOB book depth.
**How to avoid:** Use CLOB order book for Gamma hedging (CLOB-specific depth). For Polymarket leg sizing, use Phase 1 `liquidity_usd` from Gamma metadata as the primary depth signal, not CLOB book size.

### Pitfall 2: Sequential Execution Abort Path Missing
**What goes wrong:** If Polymarket order misses or fills at worse-than-1%-slippage, the system continues to Gamma and ends up with unhedged exposure.
**Why it happens:** The routing decision is made once; execution proceeds regardless of Polymarket fill quality.
**How to avoid:** Add an execution-time abort gate: after Polymarket fills, compute actual slippage. If actual slippage > 1%, abort Gamma and record the partial execution. Use `ExecutionStatus.ABORTED`.

### Pitfall 3: Two `RoutingDecision` Classes
**What goes wrong:** `signal.py` defines `RoutingDecision` with `plan: ExecutionPlan`, while `routing/engine.py` defines its own `RoutingDecision` with `legs: list[RoutedLeg]` — these are different shapes and `ExecutionEngine.execute()` only accepts the engine's version.
**Why it happens:** Two independent implementations written in parallel.
**How to avoid:** Consolidate to a single canonical `RoutingDecision` in `models/signal.py` (the more complete one). Have `routing/engine.py` return that. Update `ExecutionEngine.execute()` to accept the canonical type.

### Pitfall 4: `ExecutionEngine.execute()` Takes Wrong Type
**What goes wrong:** `ExecutionEngine.execute(decision: RoutingDecision)` calls `self.routing_engine.route(signal)` which returns the `routing/engine.py` RoutingDecision, but `RoutingOrchestrator.process()` passes `decision.legs` (a list of `RoutedLeg`) not the `RoutingDecision` object.
**Why it happens:** Type mismatch between routing.engine and routing.orchestrator.
**How to avoid:** Verify signature at integration time; consolidate to canonical type from signal.py.

### Pitfall 5: Gamma Limit Order — L1 vs. L2 Auth Confusion
**What goes wrong:** `ClobClient.create_order()` requires L1 auth (signer only), `ClobClient.post_order()` requires L2 auth (signer + creds). Calling `create_and_post_order()` without L2 creds configured raises at the post step.
**Why it happens:** Phase 1 used L0 (read-only) throughout; Phase 2 needs write access.
**How to avoid:** Check `mode` attribute before writing. Document that L2 auth (api_key, api_secret, api_passphrase) must be configured in `Settings` before Phase 2 execution is attempted.

---

## Key Findings

### Q1: Gamma Limit Order API

**[VERIFIED: py-clob-client source code]**

`ClobClient` supports limit orders via three methods:
- `create_order(order_args, options)` — creates and signs a limit order (L1 auth: signer only)
- `post_order(order, orderType, post_only)` — posts a signed order (L2 auth: signer + creds)
- `create_and_post_order(order_args, options)` — convenience combo of the above (L2 auth)

```python
# Create a limit order
from py_clob_client.clob_types import OrderArgs, PartialCreateOrderOptions, OrderType

order_args = OrderArgs(
    token_id="token_id_here",
    price=0.55,        # limit price
    size=100.0,       # size in shares
    side="BUY",
    fee_rate_bps=0,
)
# post_only=True means it won't take liquidity (maker only)
result = client.create_and_post_order(order_args)
```

**No conditional/hedge orders** — orders are independent. Hedging is implemented by submitting a Gamma limit order after Polymarket fill confirmation, not via conditional order logic.

**No market order submission** — `create_market_order()` computes fill price via `calculate_market_price()` against the CLOB book, then submits as a signed order. There is no true AMM market-order API.

### Q2: Polymarket AMM Depth Estimation

**[VERIFIED: Phase 1 CLOB reader + Gamma metadata]**

Polymarket operates an AMM where liquidity is provided via AMM liquidity pools (CLOB is the order-matching layer on top). Key facts:
- **AMM spread (15-25%)** is the **maker spread** between what you can buy YES and what you can sell YES — this is the primary profit source, not CLOB book depth
- **CLOB order book** provides the **hedge venue** (Gamma side) — CLOB book sizes are for CLOB liquidity, not AMM depth
- **`liquidity_usd` field** in Gamma `/markets` response is the best proxy for AMM depth — this was the `liquidity_threshold_usd` filter in Phase 1
- **No direct AMM depth API** — the AMM is not queryable directly; you infer depth from Gamma metadata + CLOB book

**For Phase 2 sizing:**
1. Use `liquidity_usd` from Gamma metadata as primary AMM depth signal (from Phase 1 SQLite)
2. For Gamma hedge leg, use CLOB order book (`asks`/`bids`) for sizing (via `ClobReaderClient.get_books()`)
3. CLOB book IS the Polymarket CLOB — it's the same venue; `ClobReaderClient` already fetches this

**IMDEA paper insight:** Top 3 wallets extracted $4.2M in 14 months — AMM spread is real and large, but depth is thin on less-liquid markets.

### Q3: Slippage Model Patterns

**[VERIFIED: existing `src/polyarb/models/slippage.py` + IMDEA data]**

The `SlippageCalculator` class in `src/polyarb/models/slippage.py` already exists and implements:
- Market impact via Kyle's lambda approximation (`impact_coef * notional / sqrt(daily_volume_usd)`)
- Fee/rebate model (Polymarket maker rebate +30bps, CLOB maker -10bps, CLOB taker +50bps)
- Cross-execution savings estimation

**What needs work:**
- Polymarket AMM-specific slippage (AMM is not CLOB — pricing is different)
- Calibration against Phase 1 LIVE-RUN observations (6m12s run recorded 20353 markets)

**AMM slippage model formula:**
```
For a market order buying YES at price P:
  effective_price = P * (1 + slippage_pct)
  slippage_pct = f(size, liquidity_usd, spread_pct)

empirical estimate: slippage_pct = (size_usd / liquidity_usd) * (spread_pct / 2)
```

### Q4: Execution Pipeline Patterns

**[VERIFIED: existing scaffold + py-clob-client + clawfirm reference]**

The scaffold is already in place:
- `routing/engine.py` — RoutingEngine with `route()` method (stub implementation)
- `execution/engine.py` — ExecutionEngine with `execute()` method (stub: `sleep(0.01)`)
- `routing/orchestrator.py` — RoutingOrchestrator wiring signal → routing → execution (already complete)
- `models/signal.py` — full data model (ArbitrageSignal, ExecutionPlan, ExecutionLeg, PipelineResult)
- `models/slippage.py` — SlippageCalculator and SlippageModel

**What exists vs. what needs implementation:**

| Component | Status | Action Required |
|---|---|---|
| Signal models | Complete | Use as-is |
| RoutingEngine.route() | Stub | Implement real Polymarket-first leg building |
| ExecutionEngine.execute() | Stub | Implement sequential PM → Gamma pipeline |
| RoutingOrchestrator | Complete | Use as-is |
| PositionTracker | Complete | Use as-is |
| SlippageCalculator | Partial | Extend for AMM-specific modeling |
| DepthEstimator | Missing | Build AMM depth from `liquidity_usd` + CLOB book |

---

## Code Examples

### Canonical Sequential Execution (from existing scaffold)

```python
# src/polyarb/execution/engine.py — what needs to replace the stub
async def execute(self, decision: RoutingDecision) -> ExecutionResult:
    """Execute Polymarket first, Gamma hedge second, abort on PM miss."""
    legs_executed = 0
    errors: list[str] = []

    for i, leg in enumerate(decision.legs):
        if i == 0:
            # Polymarket leg: market order, no limit price
            success, fill = await self._execute_pm_market(leg)
        else:
            # Gamma hedge: limit order at BBO ± tolerance
            success, fill = await self._execute_gamma_limit(leg)

        if not success:
            if i == 0:
                return ExecutionResult(  # PM miss = ABORT, no exposure
                    status=ExecutionStatus.ABORTED,
                    error_message=f"PM leg failed: {fill}",
                )
            errors.append(fill)
        else:
            legs_executed += 1

    return ExecutionResult(
        status=ExecutionStatus.COMPLETED if legs_executed == len(decision.legs) else ExecutionStatus.PARTIAL,
        legs_executed=legs_executed,
    )
```

### Gamma CLOB Limit Order Submission

```python
# Using py-clob-client's create_and_post_order (L2 auth required)
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs

# Requires L2 auth (api_key, api_secret, api_passphrase in Settings)
clob = ClobClient(host="https://clob.polymarket.com")

order_args = OrderArgs(
    token_id="1234567890",
    price=0.55,   # limit price at BBO
    size=50.0,    # shares to hedge
    side="BUY",
    fee_rate_bps=0,
)
result = clob.create_and_post_order(order_args)
```

### AMM Depth Estimation from Phase 1 Data

```python
# Use Phase 1 SQLite liquidity_usd for sizing
import sqlite3

conn = sqlite3.connect("data/state.db")
cursor = conn.execute(
    """
    SELECT condition_id, liquidity_usd
    FROM markets
    WHERE liquidity_usd > 1000
    ORDER BY liquidity_usd DESC
    """
)
# For max_size at 1% slippage:
# max_size = min(available_liquidity, budget)
# where available_liquidity = liquidity_usd * 2  (rough AMM pool estimate)
```

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| CLOB as primary venue | Polymarket AMM first (15-25% spread) | Phase 2 decision | Gamma is hedge, not primary |
| Limit orders everywhere | Polymarket: market order (fast), Gamma: limit order (hedge) | Phase 2 decision | Sequential vs. parallel execution |
| Fixed position sizing | Dynamic depth estimation (1% slippage cap) | Phase 2 decision | Adapts to market liquidity |
| In-memory position tracking | PositionTracker class (already exists) | Pre-Phase 2 | Position management in place |

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | Polymarket AMM does not have a direct depth API; `liquidity_usd` from Gamma is the best proxy | Q2 Key Findings | Sizing would be off — need to verify `liquidity_usd` correlates with AMM fill capacity |
| A2 | CLOB order book IS the Gamma venue (same endpoint, same liquidity) | Q2 Key Findings | Hedge sizing would reference wrong venue |
| A3 | `create_and_post_order()` in py-clob-client works as described (L2 auth, no conditional orders) | Q1 Key Findings | Would need alternative approach |
| A4 | Sequential execution (PM first, Gamma second) is achievable within execution timeout | Q4 Key Findings | May need to parallelize PM + Gamma |
| A5 | The scaffold in `src/polyarb/routing/` and `src/polyarb/execution/` is stable enough to build on | Architecture Patterns | May need refactor |

---

## Open Questions

1. **Gamma L2 auth setup**
   - What we know: `ClobClient` requires `api_key`, `api_secret`, `api_passphrase` for L2 auth
   - What's unclear: Does the project have these credentials? How are they injected into `Settings`?
   - Recommendation: Add `gamma_api_key`, `gamma_api_secret`, `gamma_api_passphrase` to `Settings` as optional fields

2. **AMM depth vs. CLOB book relationship**
   - What we know: Phase 1 has both Gamma metadata (`liquidity_usd`) and CLOB book data
   - What's unclear: Is there a correlation between CLOB book size and AMM fill capacity?
   - Recommendation: After Phase 2 implementation, run correlation analysis on Phase 1 LIVE-RUN data

3. **Slippage model calibration**
   - What we know: `SlippageCalculator` exists with hardcoded fee rates
   - What's unclear: What are the actual Polymarket/CLOB fee rates as of 2026-05?
   - Recommendation: Add a calibration step using Phase 1 LIVE-RUN-005 data (20353 markets, 72% ghost_book rate)

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| `py-clob-client` | Gamma CLOB write | ✓ | 0.34.6 | — |
| `httpx` | Gamma REST read | ✓ | in deps | — |
| `asyncio` | Sequential pipeline | ✓ | Python 3.12 | — |
| SQLite (Phase 1) | Market liquidity data | ✓ | Python 3.12 stdlib | — |
| Phase 1 CLOB reader | Order book depth | ✓ | existing `ClobReaderClient` | — |
| Gamma L2 credentials | Order submission | ✗ | — | Paper mode only (L0 read-only) |

**Missing dependencies with no fallback:**
- **Gamma L2 auth** (api_key + secret + passphrase) — required for actual order submission. Phase 2 implementation can proceed with L0 read-only for paper testing.

**Missing dependencies with fallback:**
- **Polymarket wallet** — not needed until live execution. Phase 2 runs in paper mode.

---

## Validation Architecture

> Included because `workflow.nyquist_validation` is absent (treated as enabled by default).

### Test Framework
| Property | Value |
|----------|-------|
| Framework | pytest (detected in existing tests/) |
| Config file | `pytest.ini` or `pyproject.toml` |
| Quick run command | `pytest tests/routing/ tests/execution/ -x --tb=short` |
| Full suite command | `pytest -x --tb=short` |

### Phase Requirements → Test Map
| Req ID | Behavior | Test Type | Automated Command | File Exists? |
|--------|----------|-----------|-------------------|-------------|
| REQ-2.1 | RoutingEngine.route() returns RoutingDecision for valid signals | unit | `pytest tests/routing/test_engine.py::test_route_valid_signal -x` | ❌ (new) |
| REQ-2.2 | RoutingEngine.route() returns None when profit < threshold | unit | `pytest tests/routing/test_engine.py::test_route_below_threshold -x` | ❌ (new) |
| REQ-2.3 | ExecutionEngine.execute() runs legs sequentially | unit | `pytest tests/execution/test_engine.py::test_execute_sequential -x` | ❌ (new) |
| REQ-2.4 | ExecutionEngine.abort_on_pm_miss() aborts Gamma if PM fails | unit | `pytest tests/execution/test_engine.py::test_abort_on_pm_miss -x` | ❌ (new) |
| REQ-2.5 | SlippageModel.estimate() returns SlippageEstimate for PM venue | unit | `pytest tests/models/test_slippage.py::test_estimate_pm -x` | ❌ (new) |
| REQ-2.6 | DepthEstimator.estimate_max_size() caps at 1% slippage threshold | unit | `pytest tests/routing/test_depth_estimator.py::test_max_size_slippage_cap -x` | ❌ (new) |

### Sampling Rate
- **Per task commit:** `pytest tests/routing/ tests/execution/ -x --tb=short`
- **Per wave merge:** `pytest -x --tb=short`
- **Phase gate:** Full suite green before `/gsd-verify-work`

### Wave 0 Gaps
- [ ] `tests/routing/test_engine.py` — covers REQ-2.1, REQ-2.2
- [ ] `tests/execution/test_engine.py` — covers REQ-2.3, REQ-2.4
- [ ] `tests/models/test_slippage.py` — covers REQ-2.5, REQ-2.6
- [ ] `tests/conftest.py` — shared fixtures (arbitrage signals, mock CLOB responses)
- [ ] Framework install: already present (`pytest` in project deps)

---

## Security Domain

> Required when `security_enforcement` is enabled (absent = enabled). Omit only if explicitly `false` in config.

### Applicable ASVS Categories

| ASVS Category | Applies | Standard Control |
|---|---|---|
| V2 Authentication | yes | L2 auth for Gamma CLOB write (api_key + secret + passphrase — never hardcode, use env/Settings) |
| V4 Access Control | partial | Phase 2 runs paper-only (L0 read-only); wallet/auth integration in m5-industrialize |
| V5 Input Validation | yes | All signal inputs validated before routing (ArbitrageSignal structure) |

### Known Threat Patterns for this stack

| Pattern | STRIDE | Standard Mitigation |
|---|---|---|
| Replay attack on signed CLOB orders | Tampering / Repudiation | Nonce management in `OrderArgs.nonce` |
| AMM slippage exceeds 1% cap | Information disclosure | Abort gate in ExecutionEngine — reject if slippage > threshold |
| Gamma L2 credentials in logs | Information disclosure | Loguru redacts sensitive fields; Settings env var injection |
| Polymarket-first routing to ghost books | Spoofing | Phase 1 already detects ghost books (issue #180) — use same filter |

---

## Sources

### Primary (HIGH confidence)
- `py-clob-client` v0.34.6 source — `/Users/sujiangwen/.../site-packages/py_clob_client/client.py` — all order submission methods (create_order, post_order, create_and_post_order, create_market_order)
- Phase 1 scaffold — `src/polyarb/routing/`, `src/polyarb/execution/`, `src/polyarb/models/signal.py`
- Phase 1 SQLite schema — `src/polyarb/storage/schemas.py`

### Secondary (MEDIUM confidence)
- IMDEA paper — Polymarket arbitrage ($40M extracted, 86M trades, top 3 wallets $4.2M) — supports AMM spread profitability claim
- `3th-party/polymarket-kalshi-weather-bot/VALIDATED_RESEARCH.md` — confirmed API endpoints + fee structure (0.10% Polymarket taker)

### Tertiary (LOW confidence)
- Slippage model calibration constants (hardcoded in `slippage.py`) — need empirical verification from Phase 1 LIVE-RUN data

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all libraries verified via existing project installation
- Architecture: MEDIUM — scaffold exists but integration points need verification (L2 auth setup, two RoutingDecision classes)
- Pitfalls: MEDIUM — identified from scaffold analysis, need runtime verification

**Research date:** 2026-05-01
**Valid until:** 2026-06-01 (polymarket-kalshi-weather-bot research validated ~2026-05, API unlikely to change in 30 days)