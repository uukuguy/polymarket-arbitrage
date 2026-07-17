# M2 Exact Cash Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use subagent-driven-development or execute inline task-by-task. Steps use checkbox syntax for tracking.

**Goal:** Replace binary-float authority in the M2 paper-account ledger and close receipts with exact integer micro-pUSD while preserving float-facing callers.

**Architecture:** A focused frozen `Money` value object owns scale, conversion, rounding, range checks, and paper PnL. Domain state stores `Money`; SQLite stores authoritative INTEGER micros and dual-writes legacy REAL projections. Tagged receipt JSON preserves exact close replay while decoding Phase 4 receipts unchanged.

**Tech Stack:** Python 3.12, `decimal`, dataclasses, SQLite, pytest, Typer, uv.

## Global Constraints

- One pUSD equals exactly `1_000_000` micro-pUSD.
- Convert external numeric inputs with `Decimal(str(value))` and `ROUND_HALF_EVEN`.
- Reject booleans, non-finite values, and values outside signed SQLite 64-bit range.
- Prices/signals/slippage remain float-facing; only accounting cash is exact.
- All executable verification remains exposed through existing Makefile targets.
- Preserve immutable operation IDs, `BEGIN IMMEDIATE`, retry replay, and fail-closed state.
- Each RED test commit is followed by its GREEN implementation commit.

---

### Task 1: Money and exact domain state

**Files:**
- Create: `src/polyarb/routing/money.py`
- Modify: `src/polyarb/routing/position_repository.py`
- Modify: `src/polyarb/routing/position_tracker.py`
- Create: `tests/routing/test_money.py`
- Modify: `tests/routing/test_position_tracker.py`

**Interfaces:**
- Produces: `Money(micros: int)`, `Money.from_value(value)`, `Money.to_decimal()`, `Money.to_float()`, `Money.pnl_at(...)`.
- Produces: exact `PositionState.*_money` and `Position.stake_money` authority with legacy float-compatible properties.
- Consumes: existing tracker float inputs and repository transaction closures.

- [ ] Write failing money tests for exact positive/negative conversion, half-even ties, bool/non-finite/range rejection, addition/subtraction, and BUY/SELL price-delta PnL.
- [ ] Run `uv run pytest tests/routing/test_money.py -q`; expect collection/import failure because `money.py` does not exist.
- [ ] Implement the frozen Money value object with signed 64-bit validation and no generic float arithmetic.
- [ ] Run `uv run pytest tests/routing/test_money.py -q`; expect all tests to pass.
- [ ] Commit RED then GREEN as `test(m2): define exact money contract` and `feat(m2): add micro-pusd money value`.
- [ ] Write failing tracker tests that repeat decimal closes, inspect `*_money.micros`, exercise exact exposure/insufficient-balance/stop-loss decisions, and verify existing float properties/results.
- [ ] Run targeted tracker tests; expect missing exact fields and float drift failures.
- [ ] Move `Position` stake and `PositionState` account fields to Money authority; quantize opening stake once; compute/accumulate close PnL as Money; make close methods convert the committed Money result to float only at their public return boundary.
- [ ] Run `uv run pytest tests/routing/test_position_tracker.py tests/routing/test_money.py -q`; expect green.
- [ ] Commit RED then GREEN as `test(m2): require exact tracker cash state` and `feat(m2): book tracker cash in micro-pusd`.

### Task 2: SQLite v2 migration and tagged receipts

**Files:**
- Modify: `src/polyarb/routing/position_repository.py`
- Modify: `tests/routing/test_position_repository.py`

**Interfaces:**
- Consumes: `Money` and exact domain authority from Task 1.
- Produces: additive `*_micros INTEGER` schema, transactional Phase 4 backfill, exact dual-write, `_encode_result`/`_decode_result` tagged receipt codec.

- [ ] Add failing tests that create a literal Phase 4 schema/data set before repository construction, then assert integer micros, `typeof(...) = 'integer'`, exact open stake, legacy receipt replay, and idempotent restart.
- [ ] Add migration failure tests for non-finite REAL, missing/null authoritative micros after migration, invalid account cardinality, and rollback without partial state.
- [ ] Run the migration tests; expect missing-column/schema failures.
- [ ] Add integer columns to fresh schema and focused migration helpers. Acquire `BEGIN IMMEDIATE`, add missing nullable columns, backfill once from legacy REAL, validate types/ranges/cardinality, and commit atomically.
- [ ] Make `_load_state` read only integer authority. Make `_write_state` write integer authority and derived legacy REAL projections in the same transaction.
- [ ] Run repository migration tests and `git diff --check`; expect green/clean.
- [ ] Commit RED then GREEN as `test(m2): specify exact ledger migration` and `feat(m2): migrate sqlite ledger to integer micros`.
- [ ] Add failing receipt tests for tagged Money restart/replay, exact negative/zero micros, legacy bool/float/None preservation, malformed/unknown tag rejection, and operation identity conflict.
- [ ] Implement one strict result codec used by `apply()` and `get_receipt()` in both normal and replay paths. Encode Money as `{\"kind\":\"money\",\"micros\":N}` and never coerce legacy values.
- [ ] Run `uv run pytest tests/routing/test_position_repository.py -q`; expect green.
- [ ] Commit RED then GREEN as `test(m2): require tagged money receipts` and `feat(m2): persist exact close receipts`.

### Task 3: CLI/restart compatibility and true smoke

**Files:**
- Modify: `src/polyarb/cli_arbitrage.py`
- Modify: `tests/cli/test_arbitrage_cli.py`
- Modify: `tests/integration/test_position_persistence_process.py`
- Modify: `tests/test_makefile.py`

**Interfaces:**
- Consumes: Money-bearing close receipts and float-returning tracker methods.
- Produces: CLI receipt validation accepting exact Money, unchanged operator JSON fields, cross-process migration/response-loss proof.

- [ ] Add failing CLI tests that replay a tagged Money close receipt, reject malformed/non-money close receipts, and keep `realized_pnl`, `total_realized_pnl`, `replayed`, and `retry_safe` output compatible.
- [ ] Add a subprocess test that constructs a Phase 4 DB, migrates it, loses the first close response, retries with the same operation ID, and asserts one receipt plus exact raw INTEGER balance/PnL.
- [ ] Add/extend Makefile contract tests proving `operation_id=` and `db=` still reach the close command; no new long Python command is introduced.
- [ ] Run the targeted tests; expect CLI float-only receipt validation and missing micros to fail.
- [ ] Update CLI close receipt validation to accept only Money for new exact closes and legacy float for old close receipts, converting to float after validation. Preserve exit codes and output schema.
- [ ] Run `uv run pytest tests/cli/test_arbitrage_cli.py tests/integration/test_position_persistence_process.py tests/test_makefile.py -q`; expect green.
- [ ] Commit RED then GREEN as `test(m2): prove exact close recovery across restart` and `feat(m2): expose exact receipt replay through cli`.
- [ ] Run a true shell smoke against a temporary copied Phase 4 DB: migrate, open/close twice with caller IDs, discard one response, retry it, and query raw micros/receipt count. Expected: exact cumulative micros, one booking per ID, zero open positions.

### Task 4: Teaching, full verification, SUMMARY, and climb closure

**Files:**
- Create: `docs/learning/14-精确现金账本.md`
- Modify: `docs/learning/00-INDEX.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/05-exact-cash-ledger/05-01-SUMMARY.md`
- Create: `.planning/workstreams/m2-combinatorial/phases/05-exact-cash-ledger/05-LEARNINGS.md`
- Modify: `.planning/workstreams/m2-combinatorial/ROADMAP.md`
- Modify: `.planning/workstreams/m2-combinatorial/STATE.md`
- Modify: `.planning/JOURNAL.md`
- Modify via cycle: `docs/status/climb/*`

**Interfaces:**
- Consumes: verified implementation and raw smoke evidence.
- Produces: durable phase knowledge, adversarial teaching, zero planning drift, and H-003 local score.

- [ ] Write chapter 14 with a 30-second float-vs-ledger mental model, code map with file:line references, quantization and migration tradeoffs, tagged receipt flow, five adversarial questions, and FAQ increment section; add it to the index.
- [ ] Run the corrected non-overlapping M2 test gate and record the collected/pass count.
- [ ] Run `uv run pytest tests/test_makefile.py -q`, targeted Ruff on all modified production/test modules, `git diff --check`, `make planning-status`, and `make climb-check`.
- [ ] Create `05-01-SUMMARY.md` immediately after the final implementation commit with task commits, deviations, tests, raw smoke values, and self-check.
- [ ] Commit SUMMARY before any new plan-scoped work; rerun `make planning-status` and require zero drift.
- [ ] Write `05-LEARNINGS.md`, close Phase 5 ROADMAP/STATE, append JOURNAL with the exact next command, and commit closure metadata.
- [ ] Run `make climb-cycle hypothesis=H-003`; require planning/unit/integration/CLI/restart scores of 100 before confirming H-003. If any subscore is below 100, keep H-003 pending and fix the surfaced gate rather than overriding it.

## Self-review

- Spec coverage: Money, tracker authority, migration, compatibility projection, tagged/legacy receipts, CLI/restart, teaching, planning discipline, and climb closure are assigned.
- Type flow is consistent: repository transitions may return Money; tracker close APIs convert to float; receipts preserve Money until the presentation boundary.
- No venue adapter, partial-fill aggregation, SDK selection, or legacy-column removal leaked into scope.
- No placeholder steps remain.
