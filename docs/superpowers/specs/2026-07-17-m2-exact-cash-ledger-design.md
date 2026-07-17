# M2 Exact Cash Ledger Design

**Date:** 2026-07-17  
**Workstream:** `m2-combinatorial`  
**Hypothesis:** H-003  
**Status:** approved for autonomous climb execution

## Problem

The Phase 4 recovery smoke proved that operation identity and transaction replay are
correct, but it also exposed a numerical defect: two logically exact `+10` closes left
SQLite `REAL` state at `19.999999999999996`. Presentation rounding hides the symptom;
it does not make the account ledger exact. Balance checks, exposure limits, stop-loss,
fees, realized PnL, and replayable close receipts need one canonical cash unit.

Polymarket pUSD amounts use six decimals. H-003 therefore makes integer micro-pUSD the
authoritative accounting representation while preserving the existing float-facing
market and CLI APIs.

## Considered approaches

### A. Keep floats and compare with tolerances

Smallest change, but it treats an accounting defect as a testing inconvenience.
Tolerance choices leak into risk gates and do not give receipts a canonical value.
Rejected.

### B. Use `Decimal` throughout routing and persistence

Exact decimal arithmetic is natural at boundaries, but storing/transporting arbitrary
`Decimal` values expands the public model change into signal, slippage, JSON, SQLite,
and configuration layers. It is larger than the observed problem and makes scale
implicit. Rejected for H-003.

### C. Integer micro-pUSD ledger with decimal quantization

Use a small `Money` value object backed by an integer number of micro-pUSD. Convert
float-compatible inputs once through `Decimal(str(value))`; keep market prices as
floats outside the accounting boundary. This gives SQLite and receipts a canonical
wire value without rewriting perception. Selected.

## Architecture

### `Money` value object

Add `polyarb.routing.money.Money`, a frozen value object with a single `micros: int`
field and fixed scale `1_000_000`.

- `Money.from_value(int | float | str | Decimal)` converts through decimal text and
  quantizes with `ROUND_HALF_EVEN` to one micro-pUSD.
- Non-finite values (`NaN`, positive/negative infinity), booleans, and out-of-range
  values fail closed.
- Addition/subtraction operate only on `Money`; price multiplication is not a generic
  operator because its rounding policy must stay visible.
- `to_decimal()` is the lossless boundary representation; `to_float()` exists only for
  compatibility views, logging, JSON presentation, and existing public return types.
- `pnl_at(stake, entry_price, exit_price, side)` converts prices through
  `Decimal(str(price))`, calculates the side-aware delta, and rounds the cash result once
  to micro-pUSD.

Python/SQLite signed 64-bit range is enforced before persistence so a value that cannot
fit in SQLite never partially commits.

### Domain state

`PositionState` stores `balance_money`, `snapshot_balance_money`, and
`realized_pnl_money`. `Position` stores `stake_money`. Existing `balance`,
`snapshot_balance`, `realized_pnl`, and `stake` attributes remain float-compatible
properties so callers and snapshots do not face a flag-day migration.

All tracker decisions use the money fields:

- opening quantizes stake once, compares exact balance/exposure, stores exact stake,
  and subtracts exact money;
- closing computes `Money` PnL centrally, restores exact stake plus PnL, and accumulates
  exact realized PnL;
- partial-fill equality compares quantized money amounts;
- stop-loss compares integer/decimal ratios and no longer needs a float epsilon;
- unrealized PnL remains a presentation estimate derived from current float prices.

Configuration remains float-compatible. `initial_balance`, maximum exposure, and
operator stake inputs are quantized at tracker/repository construction boundaries.

### Operation receipts

`TransitionResult` accepts `Money` in addition to the legacy `bool | float | None`.
New close transitions return `Money` to the repository, so retry/restart replay is
exact. Tracker methods continue returning floats to existing callers by converting the
committed/replayed `Money` only after repository `apply()` completes.

SQLite JSON uses an explicit tagged representation:

```json
{"kind":"money","micros":10000000}
```

The decoder accepts exactly this shape and validates the integer range. Existing JSON
float receipts remain float receipts and replay unchanged. Unknown tags, booleans used
as micros, malformed shapes, and non-finite legacy floats raise
`RepositoryStateError`; none are coerced silently.

### SQLite schema migration

Fresh databases contain both authoritative integer columns and temporary legacy
projection columns:

- account: `snapshot_balance_micros`, `balance_micros`,
  `realized_pnl_micros`;
- positions: `stake_micros`.

An existing Phase 4 database is migrated during repository initialization:

1. create the Phase 4 base tables if absent;
2. acquire `BEGIN IMMEDIATE`;
3. add each missing integer column as nullable;
4. read every legacy `REAL` value, convert via `Money.from_value`, and backfill;
5. verify every authoritative cell is an SQLite integer and non-null, account
   cardinality is one when present, position keys are coherent, and cash values fit the
   supported range;
6. commit, then run the normal v2 schema verification.

Migration is idempotent: a restart with all columns populated validates rather than
reinterpreting them. If a database contains an integer column with a value that
conflicts with its legacy projection, the integer value is authoritative; the legacy
projection is repaired on the next successful state write. Missing/invalid integer
values on an already migrated row fail closed rather than falling back indefinitely.

Every state write persists integer authority and derives legacy `REAL` projections
from it in the same transaction. This temporary dual-write keeps older inspection
tools useful without allowing them to drive calculations. Removing the legacy columns
is a later schema-cleanup phase, not H-003.

## Data flow

```text
float/string config or CLI stake
        │ Decimal(str(value)) + HALF_EVEN
        ▼
      Money(micros) ───────► exact risk checks / balance mutation
        │                                │
        │ price delta via Decimal        ▼
        ├──────────────────────────► Money PnL
        │                                │
        ▼                                ▼
SQLite INTEGER authority       tagged receipt JSON
        │                                │
        └──────── to_float presentation ─┘
```

## Failure semantics

- Invalid/non-finite money input fails before repository state changes.
- Migration and normal transitions remain under `BEGIN IMMEDIATE`; any conversion,
  schema, receipt, or invariant error rolls back the whole unit.
- Durable state still wins over a changed startup balance, compared in micros.
- Legacy receipt replay does not rewrite history. New exact receipts do not downgrade
  to floats inside storage.
- No schema mismatch or corrupt value triggers an implicit account reset.

## Testing strategy

Implementation follows RED→GREEN in three bounded slices:

1. **Money/domain RED:** conversion, half-micro/negative/non-finite/range cases,
   exact repeated closes in memory, exact exposure/stop-loss, and float-compatible
   views.
2. **Repository/migration RED:** fresh INTEGER schema, Phase 4 backfill, restart
   idempotence, open-position preservation, exact dual-write, tagged/legacy receipt
   round-trip, corrupt migration and tagged JSON rollback/fail-closed cases.
3. **CLI/restart RED:** response-loss retry returns the original exact receipt; repeated
   decimal closes leave raw integer balance/PnL exact; existing JSON keys and Makefile
   entry points remain compatible.

The completion gate is the corrected full M2 suite, Makefile contract tests, targeted
Ruff, `git diff --check`, zero planning drift, a true subprocess migration/replay smoke,
SUMMARY, learnings, teaching update, JOURNAL, and `make climb-cycle hypothesis=H-003`.

## Non-goals

- Converting market prices, percentages, signal scores, or slippage estimates wholesale
  to fixed point.
- Live venue order encoding/signing, tick-size normalization, production SDK selection,
  venue fee truth, reconciliation/outbox, or partial-fill aggregation.
- Dropping the legacy SQLite `REAL` columns in the same phase.

## Self-review

- No placeholder or unresolved design choice remains.
- Integer authority, compatibility projection, receipt format, migration ordering,
  rounding mode, range checks, and failure semantics are explicit.
- The scope is one implementation plan: exact paper-account cash state across memory,
  SQLite, restart, and replay. Venue-wire precision remains independent.
