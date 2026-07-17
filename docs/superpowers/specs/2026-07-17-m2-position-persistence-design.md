# M2 Position Persistence Design

Date: 2026-07-17

## Goal

Make the M2 paper execution lifecycle durable across independent CLI processes. After this phase, a position opened by `make run-arb` must be visible to a later `make status-arb` and closable by a later `make close-arb` without relying on module-global memory.

Success means:

- `run`, `status`, and `close` share one durable account state.
- Opening and closing a position update balance, exposure, realized PnL, and the position set atomically.
- Replaying the same state-changing operation cannot double-book money or PnL.
- A process crash cannot leave a half-applied account transition.
- Existing in-memory construction remains available for isolated unit tests.
- Existing paper execution behavior and the 104-test M2 baseline remain compatible.
- Every executable surface remains available through the Makefile.

## Evidence and Problem Boundary

`src/polyarb/cli_arbitrage.py` constructs a module-level `PositionTracker`. That object survives multiple `CliRunner` calls in one test process, but every real `make` invocation starts a new Python process and reconstructs the initial balance with no positions. The current CLI warning accurately states that `make close-arb` cannot observe a position opened by an earlier `make run-arb`.

`PositionTracker` already owns the domain rules for balance, exposure, PnL, full-fill validation, and stop-loss evaluation. `ExecutionEngine` already accepts an injected tracker. The missing capability is therefore durable state and transaction boundaries, not a replacement execution engine.

This phase is local paper-account persistence. It does not add authenticated exchange access, wallet signing, multi-host coordination, or partial-fill aggregation.

## Approaches Considered

### 1. Transactional SQLite repository — selected

Store normalized account and position records in SQLite and apply each domain transition inside `BEGIN IMMEDIATE`. Keep `PositionTracker` as the domain-facing API and inject a repository-backed implementation or persistence collaborator.

This matches the project's locked SQLite-hot-storage stack and existing WAL/explicit-transaction patterns. It provides crash consistency, queryable state, and a clean path to later event/audit history without introducing remote infrastructure.

### 2. Whole-state JSON snapshot — rejected

Serialize the tracker into one JSON file after every mutation. This is compact, but it makes concurrent writers, atomic replacement, schema evolution, and operation idempotency harder to reason about. It also turns PnL auditing into snapshot diffing rather than explicit transitions.

### 3. Supabase/Postgres as primary state — deferred

Remote storage would support multiple machines, but Phase 3 only needs independent local CLI processes. Making network availability part of the execution critical path would add failure modes before a real venue adapter exists.

## Architecture

### Domain layer

`Position`, `Fill`, `PositionSnapshot`, and stop-loss semantics remain domain concepts. Persistence code may map them to rows but must not duplicate their arithmetic in CLI handlers.

`PositionTracker` continues to expose the current public behavior:

- validate/open a position;
- update prices;
- close through a confirmed `Fill`;
- inspect positions and snapshot metrics;
- evaluate realized-PnL stop loss.

The tracker gains an explicit durable-state boundary rather than reaching into SQLite ad hoc. The exact interface shape is planner discretion, but it must allow an in-memory implementation for tests and a SQLite implementation for CLI processes.

### Persistence layer

Add a focused M2 position store, separate from the market-snapshot `SQLiteStore`. It may use the same database file if configuration chooses that path, but it owns its schema and migrations independently so market ingestion and account transitions are not coupled in one class.

Minimum durable records:

1. **Account state singleton**
   - account or paper-ledger identifier;
   - initial/snapshot balance;
   - current available balance;
   - cumulative realized PnL;
   - schema/version metadata;
   - last-updated timestamp.

2. **Open positions**
   - stable position identifier;
   - market ID and condition ID;
   - execution leg ID where available;
   - side and outcome;
   - stake, entry price, and current price;
   - opened timestamp;
   - version or last-updated timestamp.

3. **Applied operations**
   - caller-supplied or deterministically derived operation ID;
   - operation type (`open`, `close`, or compatible future transition);
   - target position/market ID;
   - applied timestamp;
   - enough result metadata to return the original outcome on replay.

Closed positions or a full append-only event ledger are not required for the first implementation. The applied-operation record plus cumulative realized PnL is the minimum audit/idempotency surface. The planner may add a compact transition history if it reduces complexity rather than expanding the phase.

### CLI composition

Each `evaluate`, `run`, `status`, and `close` process constructs dependencies explicitly:

- `evaluate` remains read-only and does not require the position store.
- `run` opens the configured store, restores account state, and injects the durable tracker into `ExecutionEngine`.
- `status` reads a fresh snapshot from durable state rather than a module-global object.
- `close` restores the target position and commits the fill-driven close transition atomically.

The default database path must be configurable through the existing `POLYARB_` settings pattern. Tests must be able to pass an isolated `tmp_path` explicitly. Production commands must not silently fall back to a second database because a path is missing or invalid.

The existing Makefile commands remain the operator interface. Help text must remove the obsolete per-process limitation once the cross-process test is green.

## Transaction and Consistency Rules

Every money-changing operation uses one explicit SQLite transaction:

1. acquire the write transaction with `BEGIN IMMEDIATE`;
2. check whether the operation ID has already been applied;
3. load the current account and target position state;
4. validate the domain transition against that state;
5. update account balance/PnL and insert/delete the position as one unit;
6. record the applied operation;
7. commit;
8. rollback and re-raise on any exception.

Required invariants:

- available balance plus open exposure is derived from one committed state;
- a market cannot have two simultaneously open positions under the current M2 model;
- opening beyond balance or max exposure changes nothing;
- closing an unknown position changes nothing and returns the existing non-zero CLI error;
- closing with a mismatched full-fill size changes nothing and raises the existing validation error;
- replaying a successful close does not add realized PnL twice;
- readers never observe account changes without the corresponding position change;
- database busy/locked errors are surfaced with actionable context, not converted into a fresh empty account.

SQLite uses WAL mode, parameterized SQL, foreign keys where useful, and explicit transaction control. Schema initialization is idempotent and additive. Destructive automatic migrations are out of scope.

## Operation Identity

Idempotency cannot depend only on `market_id`: a market may be reopened after a legitimate close. State-changing entry points therefore need an operation identity.

For Phase 3 paper flows:

- execution opens derive a stable ID from the execution decision/signal plus leg identity;
- operator close accepts or derives a stable close operation ID from the target position and fill identity;
- retries of the same command path reuse the same ID within that logical operation;
- a later intentional reopen receives a new operation ID.

The planner must make identity generation observable in logs and tests. Random IDs generated only after a retry begins do not satisfy idempotency.

## Error Handling and Recovery

- Missing database: initialize the schema and one paper account using configured `initial_balance`.
- Existing database with no account row: create the singleton transactionally.
- Corrupt or incompatible schema: fail closed with a clear error; do not reset or overwrite the file.
- Transaction failure: rollback and propagate a non-zero CLI result.
- Busy database: use a bounded timeout and actionable error. Do not spin forever.
- Configuration conflict, such as a changed `initial_balance` after durable state exists: durable state wins and the mismatch is reported. Resetting an account requires a future explicit operator command, not an implicit startup side effect.
- Process crash after commit: the next process restores the committed state.
- Process crash before commit: SQLite rollback leaves the previous state intact.

## Testing and Verification

Implementation follows test-driven development.

### Repository and domain integration

- RED: first initialization creates exactly one account with configured balance.
- RED: open transition persists account and position atomically.
- RED: close transition deletes the open position and books PnL atomically.
- RED: failed validation leaves all durable rows unchanged.
- RED: duplicate open/close operation IDs are idempotent.
- RED: a legitimate close followed by reopen uses a new identity and succeeds.
- RED: two repository instances against the same file observe committed state.
- RED: simulated exception before commit rolls back both account and position changes.
- RED: incompatible/corrupt state fails closed rather than resetting.

### CLI lifecycle

Use real subprocess boundaries, not repeated `CliRunner` calls in one Python process:

1. invoke `run` with an isolated database path and leave a position open;
2. invoke `status` in a new process and assert the position and reduced balance are present;
3. invoke `close` in a third process;
4. invoke `status` in a fourth process and assert zero open positions plus the expected realized PnL.

Also cover command replay, unknown market, database lock/failure, and paper-close behavior.

### Regression and project gates

- focused position-store and tracker tests;
- M2 routing, execution, E2E chaos, config, and CLI suites;
- Makefile contract tests and `make help` visibility;
- `make planning-status` with every shipped plan SUMMARY present;
- restart/crash consistency evidence recorded in the plan SUMMARY;
- a teaching document in `docs/learning/` explaining repository/domain separation, transaction atomicity, and idempotency.

## Climb Execution Contract

Climb is the autonomous outer loop for this phase, while GSD remains the project-state and quality-gate system.

Ground truth is a phase-gate score composed of:

- planning integrity: roadmap/state/plan/SUMMARY consistency;
- repository/domain tests;
- execution and chaos regression tests;
- true cross-process CLI lifecycle;
- restart/idempotency evidence.

Each cycle proposes one bounded hypothesis, makes the smallest change needed to test it, evaluates the relevant local gates, records confirmed or falsified results, and advances without waiting for conversational approval. No cycle may weaken a baseline default, skip a SUMMARY, hide a failing gate, or touch a real exchange.

The project climb adapter and tracked state may be bootstrapped as execution infrastructure, but they are not allowed to broaden Phase 3 product scope. Any external push/deploy action remains disabled for this phase.

## Documentation and Planning Outputs

- Register Phase 3 in `.planning/workstreams/m2-combinatorial/ROADMAP.md` before discuss/plan artifacts are generated.
- Capture locked decisions in the Phase 3 CONTEXT and the alternatives in its DISCUSSION-LOG.
- Every code plan gets a matching SUMMARY before the next plan begins.
- Add the Phase 3 learning document and update `docs/learning/00-INDEX.md`.
- At phase close, run learning extraction and record 3–5 adversarial decision questions.
- Append the session outcome and exact resume command to `.planning/JOURNAL.md`.

## Non-goals

- Real Polymarket order submission, credentials, wallet signing, or fill polling.
- Partial-fill aggregation or multiple simultaneous lots per market.
- Supabase/Postgres replication or multi-host distributed locking.
- Automatic account reset, reconciliation against exchange balances, or funding flows.
- Strategy discovery, signal persistence, market-data persistence changes, or L2/L3 changes.
- Changing the PnL formula or stop-loss policy except where required to preserve current semantics across persistence.
