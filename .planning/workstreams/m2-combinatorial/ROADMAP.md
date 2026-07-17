# Roadmap: M2 Combinatorial Arbitrage

> 能力线，不是里程碑。
> Phase 由 `gsd-tools phase add "..."` 动态长出，不预先列。

## Overview

实现 IMDEA 论文 Type 2（跨市场组合套利）：自动发现 combinatorial 不一致，多腿原子下单，paper → 实盘闭环。

依赖 m1-perception 提供的市场状态视图。

## Capability Status

Phase 2–8 完成的是 execution/accounting foundation：路由壳、paper executor、持久仓位、
精确 cash/quantity、partial fill 与 venue-truth reconciliation contract。

Phase 9 已补上第一条真实 M1→M2 产品路径：从 fresh executable asks 发现 buy-all
neg-risk 机会。当前可做真实数据发现与 paper execution；仍缺持续 paper evidence、
fee-adjusted performance gate，以及经过明确授权的 live venue adapter。因此下方所有
Phase complete 仍不等于可用真实资金实盘。

## Phases

### Phase 2: Arbitrage Execution Engine ✅

**Goal:** Turn Type-2 cross-venue signals into slippage-aware routed executions with position lifecycle, environment-driven risk settings, CLI surfaces, and E2E failure-mode coverage.
**Status:** Complete — 2026-06-07
**Plans:** 1/1 complete

Plans:
- [x] `02-1-PLAN.md` — T1-T8 signal, slippage, routing, execution, position tracking, settings, CLI, and E2E chaos coverage

### Phase 3: Position Persistence ✅

**Goal:** Persist the paper account and open-position lifecycle so independent `run`, `status`, and `close` processes share crash-consistent, idempotent state.
**Status:** Complete — 2026-07-17
**Requirements**: Internal durability contract (no external credentials)
**Depends on:** Phase 2
**Plans:** 1/1 plans complete

Plans:
- [x] `03-01-PLAN.md` — repository boundary, transactional SQLite projection, stable operation identity, durable CLI/Makefile lifecycle, and teaching artifact

### Phase 4: Durable Close Receipts ✅

**Goal:** Recover already-committed operator and venue close results across process or response loss by replaying caller-owned immutable operation identities.
**Requirements**: Internal recovery contract (no live venue credentials)
**Depends on:** Phase 3
**Status:** Complete — 2026-07-17
**Plans:** 1/1 plans complete

Plans:
- [x] `04-01-PLAN.md` — public receipt lookup, immutable venue fill identity, retry-safe CLI/Makefile close, subprocess recovery proof, and teaching update

### Phase 5: Exact Cash Ledger ✅

**Goal:** Make paper-account cash state and close receipts exact across memory, SQLite migration, restart, and replay using integer micro-pUSD without rewriting market-price models.
**Requirements**: H-003 internal accounting contract (no live venue credentials)
**Depends on:** Phase 4
**Status:** Complete — 2026-07-17
**Plans:** 1/1 plans complete

Plans:
- [x] `05-01-PLAN.md` — Money value object, exact tracker state, additive SQLite migration, tagged receipts, compatibility surfaces, teaching, and climb proof

### Phase 6: Unit-Safe Execution Accounting ✅

**Goal:** Separate exact outcome-token quantity from pUSD collateral across routing,
positions, full fills, SQLite restart, and operator views.
**Requirements:** H-004 internal unit-safety contract (no live credentials)
**Depends on:** Phase 5
**Status:** Complete — 2026-07-17
**Plans:** 1/1 complete

Plans:
- [x] `06-01-PLAN.md` — exact Quantity, explicit execution/domain fields, correct cash flow, v3 migration, compatibility, teaching, and climb proof

### Phase 7: Durable Partial-Fill Accounting ✅

**Goal:** Apply immutable partial fills exactly once while preserving remaining quantity,
cost basis, cash, and replay state across restart.
**Requirements:** H-005 internal recovery contract (no live credentials)
**Depends on:** Phase 6
**Status:** Complete — 2026-07-17
**Plans:** 1/1 complete

Plans:
- [x] `07-01-PLAN.md` — residual allocation, canonical fill identity, restart/replay, engine/process proof, teaching, and climb closure

### Phase 8: Venue-Truth Fill Reconciliation ✅

**Goal:** Replace modeled fill cash with complete, terminal, exact venue-confirmed
quantity/cash/fee facts while preserving canonical fill identity and atomic restart replay.
**Requirements:** H-006 exact reconciliation contract (no live signing credentials)
**Depends on:** Phase 7
**Status:** Complete — 2026-07-17
**Plans:** 1/1 complete

Plans:
- [x] `08-01-PLAN.md` — terminal settlement domain, fingerprinted durable receipts, tracker/engine/operator reconciliation, restart proof, teaching, and climb closure

### Phase 9: Neg-Risk Opportunity Discovery ✅

**Goal:** Convert a complete fresh M1 neg-risk sibling set and executable asks into a
fail-closed M2 buy-all opportunity feed.
**Depends on:** M1 fresh snapshot production + Phase 8
**Status:** Complete — 2026-07-17
**Plans:** 1/1 complete

Plans:
- [x] `09-01-PLAN.md` — M1 subset completeness, executable ask scanner, HTTP/CLI/Makefile delivery, teaching, and live verification

---

*Workstream: m2-combinatorial*
