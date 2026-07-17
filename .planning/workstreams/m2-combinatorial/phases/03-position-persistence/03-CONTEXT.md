# Phase 3: Position Persistence - Context

**Gathered:** 2026-07-17
**Status:** Ready for planning

<domain>
## Phase Boundary

Persist the local M2 paper account and open-position lifecycle so independent
`run`, `status`, and `close` processes share crash-consistent state. Real venue
access, partial fills, remote replication, and multi-host locking remain out of
scope.

</domain>

<decisions>
## Implementation Decisions

### Persistence boundary
- **D-01:** SQLite is the local source of truth; whole-state JSON and remote Supabase-primary storage are rejected for this phase.
- **D-02:** Keep PnL, balance, exposure, full-fill, and stop-loss rules in `PositionTracker`; persistence is injected through a repository transaction boundary.
- **D-03:** Unit tests retain an in-memory repository while real CLI processes construct the SQLite-backed tracker explicitly.

### Atomicity and idempotency
- **D-04:** Every money-changing transition uses WAL plus explicit `BEGIN IMMEDIATE`, updates account and positions together, records an applied operation, and rolls back/re-raises on failure.
- **D-05:** Stable operation IDs prevent retry/replay from double-debiting balance or double-booking realized PnL. `market_id` alone is not a permanent operation ID because a market may be reopened.
- **D-06:** Corrupt/incompatible durable state and busy/locked database failures fail closed; startup never silently creates a second empty account over an existing invalid state.
- **D-07:** Once durable state exists, it wins over a changed configured `initial_balance`; the mismatch is observable and no implicit reset occurs.

### Operator surface and verification
- **D-08:** `run`, `status`, and `close` share a configurable local position database; `evaluate` remains read-only.
- **D-09:** The proof must cross true subprocess boundaries: run in process 1, status in process 2, close in process 3, final status in process 4.
- **D-10:** Existing Makefile command names remain stable and accept a `db=<path>` override for isolated tests and operator control.
- **D-11:** Phase close requires restart/idempotency evidence, the M2 regression suite, zero planning drift, a plan SUMMARY, and a teaching document.

### Climb execution
- **D-12:** Climb is the autonomous outer loop and GSD remains the planning/quality-gate state machine.
- **D-13:** Ground truth is a five-part local score: planning integrity, repository/domain tests, execution integration, CLI lifecycle, and restart/idempotency.
- **D-14:** No external push, deployment, exchange request, credential use, or AI consultation is authorized by this phase.

### the agent's Discretion
- Exact repository Protocol and internal row-mapping helpers, provided domain arithmetic is not duplicated in CLI or SQLite code.
- Whether a compact transition-history table is added in addition to the required applied-operation records, only if it reduces rather than expands implementation complexity.
- Exact bounded SQLite busy timeout and diagnostic wording.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Approved design and execution plan
- `docs/superpowers/specs/2026-07-17-m2-position-persistence-design.md` — approved architecture, invariants, failure handling, testing, and non-goals.
- `docs/superpowers/plans/2026-07-17-m2-position-persistence.md` — task-by-task TDD execution plan.
- `docs/superpowers/plans/2026-07-17-polymarket-climb-adapter.md` — autonomous local-gate adapter plan.

### Existing M2 contracts
- `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/02-CONTEXT.md` — sequential routing/execution and sizing decisions.
- `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/02-1-SUMMARY.md` — tracker, engine, config, CLI, and 104-test baseline surface.
- `.planning/threads/market-microstructure.md` — execution-price and liquidity constraints that remain unchanged.

### Project disciplines
- `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline for fail-soft paths.
- `docs/learning/12-套利引擎.md` — current M2 mental model that Phase 3 extends.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `src/polyarb/routing/position_tracker.py`: authoritative `Position`, `Fill`, `PositionSnapshot`, PnL, exposure, and stop-loss behavior.
- `src/polyarb/execution/engine.py`: accepts an injected tracker and already has stable signal/leg identities available for operation IDs.
- `src/polyarb/routing/config.py`: existing `POLYARB_` pydantic-settings pattern.
- `src/polyarb/storage/sqlite_store.py` and `src/polyarb/translation/cache.py`: established WAL, parameterized SQL, `BEGIN IMMEDIATE`, rollback-and-reraise patterns.

### Established Patterns
- SQLite is local hot-state source of truth; explicit transactions are preferred over ORM abstraction.
- CLI modules stay flat (`cli_arbitrage.py`) to avoid namespace shadowing.
- Makefile is the required operator entry point.
- Paper/live are modes of the same code path, but Phase 3 stays paper/local because real venue authentication is separately blocked.

### Integration Points
- Replace `cli_arbitrage._TRACKER` dependency with an explicit tracker factory for `run`, `status`, and `close`.
- Pass stable open/close operation IDs from `ExecutionEngine` to tracker transitions.
- Add repository contract tests under `tests/routing/` and true subprocess lifecycle tests under `tests/cli/`.

</code_context>

<specifics>
## Specific Ideas

- The target operator experience is exactly: `make run-arb db=...` → a later `make status-arb db=...` sees the position → `make close-arb db=...` books PnL → a final status retains realized PnL.
- The mental model for teaching: the CLI is short-lived, SQLite is account memory, the tracker decides legality/arithmetic, and the repository guarantees all-or-nothing durability.

</specifics>

<deferred>
## Deferred Ideas

- Real `py-clob-client` leg executor/fill provider and wallet integration.
- Partial-fill aggregation and multiple lots per market.
- Supabase/Postgres replication and multi-host locking.
- Explicit account reset/funding/reconciliation commands.

</deferred>

---

*Phase: 03-position-persistence*
*Context gathered: 2026-07-17*
