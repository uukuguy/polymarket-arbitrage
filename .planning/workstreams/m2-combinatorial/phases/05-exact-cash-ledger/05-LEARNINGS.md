---
phase: 05
phase_name: "exact-cash-ledger"
project: "Polymarket Arbitrage"
generated: "2026-07-17T12:47:00+08:00"
counts:
  decisions: 4
  lessons: 4
  patterns: 3
  surprises: 4
missing_artifacts:
  - "05-VERIFICATION.md"
  - "05-UAT.md"
---

# Phase 05 Learnings: Exact Cash Ledger

## Decisions

### Integer micro-pUSD is the only cash authority

Balance, snapshot balance, stake, realized PnL, and new money receipts use a signed
integer number of micro-pUSD. Market prices and presentation APIs remain float-facing.

**Rationale:** The Phase 4 smoke proved binary `REAL` accumulation is unsuitable for an
account ledger, while a whole-pipeline fixed-point rewrite would expand beyond the
observed accounting defect.  
**Source:** `05-01-PLAN.md`, `05-01-SUMMARY.md`

---

### Decimal text conversion and HALF_EVEN happen once

External numeric values enter Money through `Decimal(str(value))` and round to six
decimals with `ROUND_HALF_EVEN`; modeled PnL rounds only after price delta multiplication.

**Rationale:** This preserves the caller-visible decimal rather than importing a float's
binary expansion, and provides one deterministic paper-account rule.  
**Source:** `05-CONTEXT.md`, `05-01-SUMMARY.md`

---

### SQLite migration is additive and transaction-bound

Phase 4 REAL tables gain micros columns, backfill and validate under `BEGIN IMMEDIATE`,
then read only integer authority. REAL columns remain derived compatibility projections.

**Rationale:** Additive migration preserves existing operation/position identity and
allows rollback without a destructive table rebuild or flag day.  
**Source:** `05-01-PLAN.md`, `05-01-SUMMARY.md`

---

### Money receipts are explicitly tagged

New close results use `{"kind":"money","micros":N}`; valid Phase 4 bool/float/None
receipts keep their original types.

**Rationale:** A bare JSON number is ambiguous, and Python bool is an int subclass. The
tag makes scale/type validation strict without rewriting history.  
**Source:** `05-CONTEXT.md`, `05-01-SUMMARY.md`

## Lessons

### Exact storage also requires dynamic-type checks on every read

Declaring an SQLite column INTEGER is not enough: SQLite can store REAL `1.5` in an
INTEGER-affinity column, and `int(1.5)` would silently truncate it.

**Context:** A dedicated RED test corrupted `balance_micros` after initialization; the
repository now validates `typeof(...) = 'integer'` on every load.  
**Source:** `05-01-SUMMARY.md`

---

### Task ordering must preserve the baseline between GREEN commits

Changing tracker close transitions to Money before the receipt codec existed would have
broken SQLite JSON serialization at an intermediate commit.

**Context:** The local plan revision kept Task 1 receipts temporarily float-compatible,
then switched transition result and codec atomically in Task 3.  
**Source:** `05-01-SUMMARY.md`

---

### Unit labels prevent plausible-looking numerical test mistakes

A proposed PnL vector treated half a micro-pUSD as 500 micros; the implementation's zero
result was correct under HALF_EVEN.

**Context:** Tracing `5e-9 price × 100 pUSD = 5e-7 pUSD` exposed the test's unit error;
the vector was corrected rather than weakening rounding.  
**Source:** `05-01-SUMMARY.md`

---

### Backward compatibility belongs outside authority

Keeping float properties and REAL projections is safe only when every mutation/read
decision uses Money/INTEGER first and compatibility values are derived afterward.

**Context:** Existing CLI/snapshot tests stayed green while raw SQLite and receipts
became exact.  
**Source:** `05-01-PLAN.md`, `05-01-SUMMARY.md`

## Patterns

### Radar versus cash register

Use approximate numeric representations for observations/ranking and a canonical minor
unit value for committed cash.

**When to use:** Whenever a strategy pipeline mixes volatile estimates with balances,
fees, limits, settlement, or audit receipts.  
**Source:** `05-01-SUMMARY.md`

---

### Add-backfill-validate-commit migration

Acquire the write lock, add nullable compatibility columns, convert every legacy row in
application code, validate dynamic types/invariants, and commit as one unit.

**When to use:** Evolving a durable SQLite projection where silent reset or partial
migration would corrupt business state.  
**Source:** `05-01-PLAN.md`, `05-01-SUMMARY.md`

---

### Tagged domain result at the receipt boundary

Serialize a domain value with a type tag and canonical minor units; decode legacy scalar
types explicitly and reject unknown shapes.

**When to use:** Idempotency/result ledgers that must evolve result types without
ambiguous JSON coercion or historical rewrites.  
**Source:** `05-01-PLAN.md`, `05-01-SUMMARY.md`

## Surprises

### Phase 4's rounded success hid a stored accounting defect

The CLI showed cumulative 20 while SQLite REAL contained `19.999999999999996`.

**Impact:** The smoke became the precise H-003 regression target and justified exact
cash authority before fees/live execution.  
**Source:** `05-CONTEXT.md`, `05-01-SUMMARY.md`

---

### SQLite INTEGER affinity is not an invariant

An external REAL write can live in an INTEGER-declared column.

**Impact:** Schema inspection at startup alone was insufficient; repository reads now
enforce runtime storage class.  
**Source:** `05-01-SUMMARY.md`

---

### A legacy REAL value can migrate to the intended exact decimal

`19.999999999999996` deterministically became `20_000_000` micros through
`Decimal(str(value))` and six-place quantization.

**Impact:** Existing Phase 4 drift is repaired at the defined accounting precision rather
than preserved as a binary artifact.  
**Source:** `05-01-SUMMARY.md`

---

### Autonomous planner/checker workers did not return

Both bounded collaboration attempts produced no artifact.

**Impact:** The main agent interrupted them, applied the loaded GSD plan/checker contracts
locally, caught a real task-order issue, and continued without blocking climb.  
**Source:** `05-01-SUMMARY.md`
