---
phase: 05-exact-cash-ledger
plan: 01
subsystem: exact-accounting
tags: [money, micro-pusd, sqlite-migration, receipts, cli, tdd]

requires:
  - phase: 04-durable-close-receipts
    provides: Transactional account projection and immutable close response replay
provides:
  - Frozen six-decimal Money value with signed SQLite range checks
  - Exact balance, stake, realized PnL, exposure, and stop-loss authority
  - Transactional Phase 4 REAL to INTEGER micros migration and dual-write projection
  - Tagged exact money receipts with legacy scalar compatibility
  - True migrated subprocess response-loss recovery proof
affects: [fees, risk-gates, venue-adapter, reconciliation, execution-audit]

tech-stack:
  added: []
  patterns: [integer-minor-unit-ledger, decimal-boundary-quantization, additive-schema-migration, tagged-money-receipt]

key-files:
  created:
    - src/polyarb/routing/money.py
    - tests/routing/test_money.py
    - docs/learning/14-精确现金账本.md
  modified:
    - src/polyarb/routing/position_repository.py
    - src/polyarb/routing/position_tracker.py
    - src/polyarb/cli_arbitrage.py
    - tests/routing/test_position_repository.py
    - tests/routing/test_position_tracker.py
    - tests/cli/test_arbitrage_cli_process.py
    - docs/learning/00-INDEX.md

key-decisions:
  - "Cash authority is signed integer micro-pUSD; prices remain float-facing until the accounting boundary."
  - "External values convert through Decimal(str(value)) and ROUND_HALF_EVEN exactly once."
  - "SQLite INTEGER values are authoritative; legacy REAL columns are derived compatibility projections only."
  - "New close receipts use tagged micros; valid Phase 4 bool/float/None receipts retain their original types."

patterns-established:
  - "Radar versus cash register: approximate market observations may be floats, but committed account state has one canonical minor-unit answer."
  - "Add/backfill/validate under BEGIN IMMEDIATE; never reset or reinterpret already-migrated integer authority."

requirements-completed: [H-003]

duration: 12min
completed: 2026-07-17
---

# Phase 5 Plan 01: Exact Cash Ledger Summary

**M2 paper-account money is now exact across memory, SQLite migration, restart, and immutable close replay without converting the market-price pipeline to fixed point.**

## Performance

- **Duration:** 12 minutes implementation and verification after planning
- **Started:** 2026-07-17T12:34:31+08:00
- **Completed:** 2026-07-17T12:46:48+08:00
- **Tasks:** 4
- **Corrected M2 gate:** 187 passed

## Accomplishments

- Added frozen `Money(micros)` with six-decimal HALF_EVEN quantization, non-finite/bool rejection, and signed 64-bit protection.
- Moved balance, snapshot balance, stake, realized PnL, exposure, fill equality, and stop-loss decisions to exact money authority while retaining float-compatible public views.
- Migrated literal Phase 4 databases additively under `BEGIN IMMEDIATE`, preserved account/position/operation identity, validated SQLite dynamic types, and dual-wrote derived REAL projections.
- Made new close receipts exact tagged micros and preserved valid legacy scalar receipts; malformed tags, ambiguous integers, bool-as-micros, NaN, overflow, and invalid JSON fail closed.
- Proved a Phase 4 database can migrate, close, lose its response, restart, and replay one exact receipt without a second cash booking.

## Task Commits

Each behavior was observed RED before its GREEN implementation:

1. **Money contract RED** — `8ff3b44`
2. **Money value GREEN** — `c34fa70`
3. **Exact tracker RED** — `357b97c`
4. **Exact tracker GREEN** — `3f10dc6`
5. **SQLite migration RED** — `31d1e05`
6. **Runtime authority corruption RED** — `e3de5bf`
7. **SQLite migration/dual-write GREEN** — `52f7d3d`
8. **Tagged receipt/restart RED** — `4feeb4c`
9. **Tagged receipt/CLI GREEN** — `09605d2`
10. **Teaching chapter** — `7169448`

**Exploration/design/planning:** `86b0532`, `7693af1`, `ef89ac5`, `dbbeee6`, `b6c9344`, `4d7dc0c`.

## Delivered Contracts

- `Money.from_value` treats inputs as pUSD-facing decimal text and yields exactly one signed integer micro count.
- `PositionState.*_money` and `Position.stake_money` are authority; legacy properties call `to_float()` only for compatibility/presentation.
- `Money.pnl_at` performs side-aware decimal price delta multiplication and rounds once after the cash result is known.
- Fresh SQLite schemas require `snapshot_balance_micros`, `balance_micros`, `realized_pnl_micros`, and `stake_micros` INTEGER columns.
- A Phase 4 schema is altered/backfilled/validated atomically. A complete existing v2 schema with NULL/REAL money authority fails rather than falling back to REAL.
- New receipt JSON is `{"kind":"money","micros":N}`. The decoder accepts only the exact shape and exact integer type.
- Tracker close methods return floats to existing callers after repository commit/replay; storage never downgrades new Money receipts.

## Verification Evidence

### RED → GREEN

- Money RED: missing module; GREEN: **21 passed**.
- Exact tracker RED: four failures covering missing authority, premature balance rejection, float exposure overflow, and false partial-fill mismatch; GREEN focused gate: **25 passed**.
- Migration RED: six missing-column/migration/fail-closed failures. Runtime corruption RED separately proved `int(1.5)` was silently accepted before the guard. GREEN repository gate: **31 passed**.
- Receipt/restart RED: eleven failures across Money result rejection, missing tag codec, malformed payload acceptance, float receipt type, and raw response-loss receipt. GREEN combined receipt/restart gate passed.

### Corrected full M2 gate

```bash
uv run pytest tests/models/test_slippage.py tests/routing tests/execution tests/cli -q
```

Result: **187 passed**, with the existing `datetime.utcnow` deprecation warnings only.

Additional gates:

- `uv run pytest tests/test_makefile.py -q` — **3 passed**
- `make climb-check` — **16 passed**
- targeted Ruff across all changed production/test modules — **all checks passed**
- `git diff --check` — clean
- `make planning-status` — no prior shipped-plan drift; Phase 5 correctly remained NOT-STARTED until this SUMMARY

### True migrated response-loss smoke

A temporary database was created with the literal Phase 4 REAL-only schema, then used by independent CLI processes:

1. `run` migrated the schema and opened `cond-0` with stake 100.
2. `close --operation-id h003-smoke-close` committed +10; stdout was discarded.
3. A new process retried the same ID and returned `replayed=true`, `retry_safe=true`, PnL 10.
4. Raw SQLite result:

```json
{"balance_micros":1010000000,"realized_pnl_micros":10000000,"balance_type":"integer","open_count":0,"close_receipts":1,"receipt_json":"{\"kind\":\"money\",\"micros\":10000000}"}
```

The validated temporary directory was removed after the query.

## Failure and Migration Semantics

- Boolean money input, NaN/infinity, half-unit ambiguity, and signed 64-bit overflow are decided before account mutation.
- Migration DDL and backfill share one transaction; invalid legacy money rolls back added columns as well as data.
- Once all micros columns exist, invalid/NULL/non-INTEGER authority is corruption, not a signal to re-backfill from REAL.
- Every `load()` checks SQLite dynamic types, preventing an externally written REAL from being silently truncated by `int()`.
- Integer authority wins over stale legacy projection. Normal writes repair projections by deriving them from Money.
- Receipt decode errors are `RepositoryStateError`; duplicate apply rolls back rather than returning invented success.

## Deviations from Plan

### Auto-fixed Issues

**1. Planner/checker subagents produced no artifact within bounded recovery windows**
- **Found during:** Phase 5 planning.
- **Fix:** interrupted both stalled workers, generated the GSD plan locally from the approved spec, and applied the complete plan-checker checklist.
- **Evidence:** valid GSD init found Phase 5, one plan, context, and research; placeholder/diff checks passed.

**2. Initial plan created an intermediate type-ordering break**
- **Found during:** local plan revision gate.
- **Issue:** returning `Money` receipts before the SQLite tag codec would break existing JSON paths between commits.
- **Fix:** kept Task 1 receipts temporarily float-compatible, then atomically switched tracker result + codec in Task 3.
- **Impact:** every GREEN commit retained the existing test baseline.

**3. A Money test vector confused half a micro with 500 micros**
- **Found during:** first Money GREEN.
- **Root cause:** `5e-9 × 100 = 5e-7 pUSD`, which HALF_EVEN correctly rounds to zero.
- **Fix:** changed the delta to `5e-6 × 100 = 0.0005 pUSD`; no production rounding change.

**4. Schema exactness needed runtime type validation, not initialization only**
- **Found during:** migration self-review.
- **Issue:** SQLite can dynamically store REAL `1.5` in an INTEGER-affinity column; `int()` would silently truncate it.
- **Fix:** added a RED test and validate authority on every repository load.

---

**Total deviations:** 4 auto-fixed issues.  
**Scope impact:** none; all fixes strengthened the approved exactness and plan-discipline contracts.

## User Setup Required

None — no external service, credential, live venue, dependency, or new command is required.

## Next Phase Readiness

- H-003 is ready for learnings extraction, metadata closure, and `make climb-cycle hypothesis=H-003`.
- Fee accounting can now use the same Money boundary without inventing another cash type.
- Live order/tick/SDK precision remains a separate hypothesis triggered by venue-adapter work and real venue truth.

---
*Phase: 05-exact-cash-ledger*
*Completed: 2026-07-17*
