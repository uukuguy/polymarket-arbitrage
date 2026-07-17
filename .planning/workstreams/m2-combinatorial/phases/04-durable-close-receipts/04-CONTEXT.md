# Phase 4: Durable Close Receipts - Context

**Gathered:** 2026-07-17
**Status:** Ready for planning
**Source:** Approved H-002 design

<domain>
## Phase Boundary

Expose already-committed close results through durable receipts and make
caller-owned operator/venue identities replayable across response and process
loss. Live venue reconciliation, outbox workers, partial fills, and mandatory
adapter fill IDs remain out of scope.

</domain>

<decisions>
## Implementation Decisions

### Receipt contract
- **D-01:** Promote the applied-operation ledger row to frozen public `OperationReceipt(operation_id, operation_type, target_id, result)`.
- **D-02:** `PositionRepository.get_receipt()` is observational: unknown returns `None`; storage/JSON errors propagate; lookup never creates a success receipt.
- **D-03:** Both in-memory and SQLite repositories expose the same contract; the existing SQLite schema already has every required field, so no migration is added.
- **D-04:** `PositionTracker.operation_receipt()` is the only receipt surface used by callers; the CLI must not access repository internals or redo PnL arithmetic.

### Operator recovery
- **D-05:** `close --operation-id ID` looks for a receipt before requiring an open position. An exact close/market match returns its stored float result and reports `replayed: true`.
- **D-06:** Reuse of an ID for another operation type or target fails closed as an identity conflict. Unknown ID plus missing position remains an error.
- **D-07:** A fresh caller-supplied ID is passed unchanged into `close_position_with_fill`; repository `apply()` remains the race-safe final authority when processes compete.
- **D-08:** If the caller omits the ID, the CLI generates one before mutation and returns it for compatibility, but reports `retry_safe: false`; only retained caller-owned identities report `true`.
- **D-09:** Success JSON includes `operation_id`, `replayed`, `retry_safe`, per-close PnL, and cumulative PnL. Replayed stored PnL is authoritative.
- **D-10:** `make close-arb` forwards optional `operation_id=` and documents the retry-safe form.

### Venue identity
- **D-11:** `Fill` gains optional immutable `fill_id`; a real fill close uses `close:{signal_id}:{leg_id}:fill:{fill_id}`.
- **D-12:** Missing legacy `fill_id` retains the timestamp identity only for compatibility and logs that durable retry guarantees are unavailable.
- **D-13:** Paper close keeps its current deterministic `paper-close` identity without warnings.

### Proof and close criteria
- **D-14:** A true subprocess test discards the first close response, replays the same ID from a new process, proves PnL and ledger cardinality are unchanged, rejects a conflicting target, then proves reopen/close with a new ID.
- **D-15:** Phase closure requires corrected non-overlapping M2 tests, Makefile contract, Ruff, `git diff --check`, zero planning drift, SUMMARY, learnings, teaching FAQ, JOURNAL, and H-002 climb evaluation.

### the agent's Discretion
- Exact helper extraction and error wording, provided identity conflicts and corrupt receipt results are distinguishable and non-zero.
- Whether `get_receipt()` shares a private SQLite row decoder with replay logic, provided behavior and error propagation remain identical.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Approved design and implementation plan
- `docs/superpowers/specs/2026-07-17-m2-durable-close-receipts-design.md` — approved H-002 architecture, failure semantics, CLI response, tests, and non-goals.
- `docs/superpowers/plans/2026-07-17-m2-durable-close-receipts.md` — exact RED→GREEN tasks, commits, verification, and closure sequence.

### Existing durability contracts
- `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-CONTEXT.md` — repository boundary and operation-ledger decisions that remain locked.
- `.planning/workstreams/m2-combinatorial/phases/03-position-persistence/03-01-SUMMARY.md` — delivered SQLite/tracker/engine/CLI implementation and verified baseline.
- `docs/learning/13-仓位持久化.md` — teaching chapter to deepen rather than duplicate.

### Project disciplines
- `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline.
- `docs/status/climb/hypotheses.yaml` — H-002 claim and current experiment queue.

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable assets
- `src/polyarb/routing/position_repository.py`: operation ledger, replay conflict guard, in-memory and SQLite stores.
- `src/polyarb/routing/position_tracker.py`: authoritative close/PnL transition and `Fill` model.
- `src/polyarb/execution/engine.py`: signal/leg identities and paper/venue close paths.
- `src/polyarb/cli_arbitrage.py`: durable tracker factory and current operator close command.
- `tests/cli/test_arbitrage_cli_process.py`: real subprocess helper and four-process lifecycle proof.

### Integration flow
`caller operation_id / venue fill_id` → CLI or engine → tracker → repository
receipt ledger. Projection state answers what is open; receipt lookup answers
whether one immutable operation already committed.

</code_context>

<specifics>
## Specific Ideas

- The decisive experiment is not “closing twice works”; it is “the first close committed but its response vanished, and a later process recovered the exact result without changing money or ledger count.”
- The operator-safe command is `make close-arb db=... market_id=cond-0 exit_price=0.50 operation_id=close-001`.

</specifics>

<deferred>
## Deferred Ideas

- Live venue/wallet reconciliation and mandatory adapter `fill_id` enforcement.
- Background recovery/outbox workers and closed-position event sourcing.
- Partial-fill aggregation and multiple lots per market.
- Recovering a CLI-generated ID when the response containing that ID is itself lost.

</deferred>

---

*Phase: 04-durable-close-receipts*
*Context gathered: 2026-07-17 from approved H-002 spec*
