---
phase: 03
phase_name: "position-persistence"
project: "Polymarket Arbitrage"
generated: "2026-07-17"
counts:
  decisions: 6
  lessons: 5
  patterns: 4
  surprises: 4
missing_artifacts:
  - "03-VERIFICATION.md"
  - "03-UAT.md"
---

# Phase 03 Learnings: position-persistence

## Decisions

### Keep domain arithmetic out of persistence adapters

PnL, exposure, open, and close rules remain in `PositionTracker`; repositories receive a complete-state transition closure and own only transaction/durability behavior.

**Rationale:** One domain formula must serve both fast in-memory tests and durable SQLite execution; duplicating formulas in a storage class would permit semantic drift.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Use a normalized three-table projection

The durable model separates singleton account state, current open positions, and applied operations instead of storing one JSON account blob.

**Rationale:** Primary keys and explicit cardinality checks enforce one current position per market, one paper account, and one result per operation ID.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Serialize writers before reading risk state

Every SQLite mutation begins with `BEGIN IMMEDIATE` before loading account and positions.

**Rationale:** Balance/exposure validation and the resulting write must observe one serialized state; read-then-write outside the lock permits concurrent over-allocation.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Memoize rejected as well as successful operations

An operation records its original bool/float/None result even when a domain gate returns `False`.

**Rationale:** The same operation must not change outcome merely because account state changes later; a new attempt needs a new identity.
**Source:** `03-01-SUMMARY.md`

---

### Durable state wins configuration conflicts

Once the account exists, a later `initial_balance` change logs a mismatch but never resets the account.

**Rationale:** Implicit reset on startup would manufacture capital and erase trading truth; reset requires a future explicit operator action.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Compose a fresh tracker per CLI command

`run`, `status`, and `close` each construct a repository-backed tracker for the selected path; unit tests inject one in-memory tracker through the factory.

**Rationale:** Real commands must share durable state rather than module memory, while unit tests should remain isolated and fast.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

## Lessons

### State equality is weaker than operation idempotency

A duplicate open could leave balance/positions unchanged because the domain rejects an already-open market, yet still append another ledger operation. Strict replay tests must inspect transition invocation or operation count.

**Context:** Execution replay was only proven after tests asserted one durable open operation and exactly open+close for paper-close.
**Source:** `03-01-SUMMARY.md`

---

### Pydantic aliases can invert explicit-over-env precedence

A single `validation_alias` made `PositionConfig(db_path=...)` invisible and allowed the environment value to win. `AliasChoices(field_name, env_name)` restored explicit argument precedence.

**Context:** The config precedence test failed after the first implementation even though env-only parsing worked.
**Source:** `03-01-SUMMARY.md`

---

### Verification paths must not overlap

Passing one test file and its parent directory in the same pytest command caused directory de-duplication and ran only 69 tests instead of the intended 130.

**Context:** The final gate was corrected to use non-overlapping `tests/models/test_slippage.py tests/routing tests/execution tests/cli` paths.
**Source:** `03-01-SUMMARY.md`

---

### Persisted timestamps require an explicit timezone contract

The pre-existing `datetime.utcnow` position default produced a naive ISO timestamp. A RED persistence test forced `Position.opened_at` to become UTC-aware.

**Context:** SQLite round-trip equality passed with handcrafted aware fixtures but tracker-created positions initially failed the timezone assertion.
**Source:** `03-01-SUMMARY.md`

---

### Numeric plan scope needs both lifecycle boundaries

Using the exact PLAN creation commit as a lower bound stopped old commits from making a new plan look started, but did not stop a newer workstream from inflating an already closed plan with the same `03-01` scope. Committed SUMMARY creation is the required upper bound.

**Context:** The final planning audit showed M1 `03-01` increase after M2 commits even though both plans had valid summaries.
**Source:** `03-01-SUMMARY.md`

---

## Patterns

### Copy–transition–publish

Load or deep-copy the complete state, run a domain transition on the candidate, and publish only after it returns successfully.

**When to use:** Mutable aggregate updates where validation may raise and no partial mutation may escape, both in memory and in durable stores.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Applied-operation ledger

Store operation identity, semantic type/target, and the JSON-safe original return value in the same transaction as the state projection.

**When to use:** At-least-once callers, crash/retry boundaries, or command replay where “already done” must return the first result without repeating effects.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### True subprocess lifecycle test

Exercise run → status → close → status with four OS processes and one temporary DB instead of multiple in-process CLI runner calls.

**When to use:** Any persistence claim whose correctness depends on process lifetime, module globals, environment loading, or CLI parsing.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

### Fail closed on state ambiguity

Validate schema shape and singleton cardinality, bound lock waits, and propagate storage errors rather than constructing a fresh empty account.

**When to use:** Financial/account state where an apparently available default balance is more dangerous than an explicit outage.
**Source:** `03-01-PLAN.md`, `03-01-SUMMARY.md`

---

## Surprises

### The planned full-suite command was not full

The original verification command passed overlapping pytest paths and silently omitted most routing tests.

**Impact:** The apparent 69-test pass could have hidden repository/tracker regressions; the corrected non-overlapping gate proved 130 tests.
**Source:** `03-01-SUMMARY.md`

---

### macOS/Python timestamp debt surfaced at the persistence boundary

Although Phase 2 tolerated naive `Position.opened_at`, serializing and restoring the value made timezone semantics observable.

**Impact:** `Position` is now UTC-aware; separate `Fill`/`PositionSnapshot` deprecation warnings remain explicitly deferred.
**Source:** `03-01-SUMMARY.md`

---

### A dedicated Makefile contract file did not exist

The plan referenced `tests/test_makefile.py`, but the repository only had M1-specific Makefile contracts.

**Impact:** Phase 3 created the root contract file to lock the M2 `db=` forwarding and help surface without coupling it to M1 tests.
**Source:** `03-01-SUMMARY.md`

---

### GSD reported a roadmap update without replacing placeholders

`phase complete 03` returned `roadmap_updated: true`, but the Phase 3 roadmap still said goal `TBD`, plan `TBD`, and `0/1` until the separate plan-progress operation and exact metadata repair.

**Impact:** Closure needed a read-after-write audit; trusting the command result alone would have preserved contradictory durable state.
**Source:** `03-01-SUMMARY.md`
