---
phase: 04
phase_name: "durable-close-receipts"
project: "Polymarket Arbitrage"
generated: "2026-07-17"
counts:
  decisions: 5
  lessons: 4
  patterns: 4
  surprises: 3
missing_artifacts:
  - "04-VERIFICATION.md"
  - "04-UAT.md"
---

# Phase 04 Learnings: durable-close-receipts

## Decisions

### Separate current projection from committed-operation truth

Open positions answer what exists now; `OperationReceipt` answers whether one immutable operation committed and what it returned.

**Rationale:** A successful close deliberately removes the position, so projection absence cannot distinguish “never closed” from “closed but response lost.”
**Source:** `04-CONTEXT.md`, `04-01-SUMMARY.md`

---

### Keep apply as the final idempotency authority

Receipt lookup is observational response recovery; `BEGIN IMMEDIATE`, unique ledger identity, and transaction-internal replay remain authoritative.

**Rationale:** Two processes can both miss a preflight lookup. Correctness cannot depend on a race-free read-before-write sequence.
**Source:** `04-01-PLAN.md`, `04-01-SUMMARY.md`

---

### Grant retry-safe status only to retained caller identity

Explicit caller IDs return `retry_safe: true`; CLI-generated compatibility IDs return `false` even though they are persisted and shown in a successful response.

**Rationale:** If the response containing a generated ID is lost, the caller cannot reconstruct that identity and therefore cannot recover by replay.
**Source:** `04-CONTEXT.md`, `04-01-SUMMARY.md`

---

### Validate receipt semantics before returning stored results

Replay requires operation type `close`, the requested market target, and a float result that is not bool.

**Rationale:** Primary-key equality alone must not let another type/market receipt cross an identity boundary or turn corrupt ledger data into financial success.
**Source:** `04-01-PLAN.md`, `04-01-SUMMARY.md`

---

### Prefer venue fill identity and label timestamp fallback honestly

`Fill.fill_id` produces the stable real-close identity; missing ID keeps compatibility but emits a durability warning.

**Rationale:** Local timestamps identify observations, not venue-confirmed fills; they change across retries and cannot establish exchange truth.
**Source:** `04-CONTEXT.md`, `04-01-SUMMARY.md`

---

## Lessons

### Response recovery must be tested after deliberately discarding success

A second close call that happens to return zero is not a recovery proof. The test must commit once, discard the first stdout, and ask a fresh process for the original receipt.

**Context:** The subprocess test proves recovered PnL, balance, cumulative PnL, empty projection, and one close ledger row.
**Source:** `04-01-SUMMARY.md`

---

### Python bool requires explicit exclusion from float receipt validation

`bool` is a subclass of `int`, and broad numeric checks can accidentally accept it as close PnL.

**Context:** CLI uses exact `type(result) is float` for committed close receipts while repository results continue supporting bool/float/None generically.
**Source:** `04-01-SUMMARY.md`

---

### Makefile optional arguments are safer with command branches than quote-filled variables

Shell variables do not re-interpret embedded quote characters during ordinary expansion.

**Context:** `close-arb` uses explicit with-ID/without-ID branches so `--operation-id "..."` remains exactly one argument on macOS `/bin/sh`.
**Source:** `04-01-SUMMARY.md`

---

### Read-after-write remains mandatory for GSD state tools

The planned-phase command updated activity text but left `current_phase` and the Current Position body on Phase 3.

**Context:** Phase 4 execution would have begun under contradictory durable state without explicit read-back and repair.
**Source:** `04-01-SUMMARY.md`

---

## Patterns

### Projection plus durable receipt

Maintain a compact current-state projection and a separate immutable operation-result ledger.

**When to use:** Commands remove or replace current rows but callers still need to recover the result of one acknowledged or unacknowledged mutation.
**Source:** `04-01-PLAN.md`, `04-01-SUMMARY.md`

---

### Replay-first operator command

For a caller-owned identity, validate and return its receipt before requiring the mutable target to still exist.

**When to use:** A successful operation naturally removes its own precondition, such as close, cancel, consume, or finalize.
**Source:** `04-01-PLAN.md`, `04-01-SUMMARY.md`

---

### Identity namespace includes semantic owner

Stable IDs combine signal, leg, operation role, and venue fill identity; type/target are independently stored and checked.

**When to use:** The same market/resource can be legitimately reopened or acted on repeatedly, so resource ID alone is not an operation ID.
**Source:** `04-CONTEXT.md`, `04-01-SUMMARY.md`

---

### Lost-response subprocess proof

Discard the first successful response, replay from another OS process, then inspect both observable state and ledger cardinality.

**When to use:** Claiming crash/retry safety at CLI, service, queue, or venue boundaries.
**Source:** `04-01-SUMMARY.md`

---

## Surprises

### Planning agents timed out while the local plan contract remained usable

Both bounded planner and checker collaboration attempts returned no artifact/output.

**Impact:** The main session used the fully loaded GSD schema/checklist, audited 15/15 locked decisions and all task fields, and documented the fallback rather than blocking implementation.
**Source:** `04-01-SUMMARY.md`

---

### Full M2 coverage grew by fifteen tests without a schema migration

The existing operation table already contained every receipt field.

**Impact:** H-002 added recovery, conflict, type-roundtrip, fill-identity, and Makefile proofs while keeping persisted database compatibility.
**Source:** `04-01-SUMMARY.md`

---

### Exact decimal expectations expose binary float representation

Two $10 closes produce a stored Python/SQLite REAL value of `19.999999999999996`, while CLI rounding correctly reports `20.0`.

**Impact:** Current paper semantics are unchanged, but a real cash-ledger phase should explicitly choose integer minor units or Decimal rather than treating binary floats as exact accounting values.
**Source:** `04-01-SUMMARY.md`

## Adversarial Coach Gate

1. A close commits and stdout is lost. Which exact value must the caller retain, and which table proves the result?
2. Why can an empty open-position projection mean both success and uncertainty?
3. Two processes both see no receipt. Which mechanism still prevents double-booking?
4. Why must the next legitimate close after reopen use a new ID, even at identical price and size?
5. What makes venue `fill_id` stronger evidence than local `filled_at`?

These are decision questions, not vocabulary checks. A wrong answer should route back to `docs/learning/13-仓位持久化.md` before a real venue adapter is authorized.
