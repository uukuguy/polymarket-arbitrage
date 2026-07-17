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

### Phase 3: Position Persistence

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 2
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd-plan-phase 3 to break down)

---

*Workstream: m2-combinatorial*
