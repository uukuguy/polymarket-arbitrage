# Roadmap: M2 Combinatorial Arbitrage

> 能力线，不是里程碑。
> Phase 由 `gsd-tools phase add "..."` 动态长出，不预先列。

## Overview

实现 IMDEA 论文 Type 2（跨市场组合套利）：自动发现 combinatorial 不一致，多腿原子下单，paper → 实盘闭环。

依赖 m1-perception 提供的市场状态视图。

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

---

*Workstream: m2-combinatorial*
