# M2 Durable Close Receipts Design

**Date:** 2026-07-17  
**Climb hypothesis:** H-002  
**Status:** Approved for implementation

## Problem

Phase 3 makes position mutations durable and idempotent when callers reuse an
operation ID. One recovery gap remains at the CLI/venue boundary:

1. a close transaction commits;
2. the process crashes or its response is lost;
3. the caller retries;
4. the position is already gone, so the current CLI exits with “no open
   position” instead of returning the committed close result.

The durable ledger already contains the original PnL result. The missing piece
is a public receipt lookup plus a caller-owned immutable close identity.

## Goal

When a caller supplies the same immutable close operation ID, a retry after an
already-committed close returns the original close result without changing
balance, realized PnL, positions, or ledger cardinality.

The response must tell the operator which operation ID was used and whether the
command found an existing receipt before attempting the transition.

## Non-goals

- Querying Polymarket or another live venue.
- Reconciling local positions against wallet/venue truth.
- Adding a background recovery daemon or outbox worker.
- Full closed-position history or event sourcing.
- Changing paper-close identity, which is already deterministic.
- Making automatically generated CLI identities recoverable after response
  loss. Recovery is guaranteed only when the caller supplies and retains the
  immutable ID.

## Considered Approaches

### A. Explicit durable receipt — selected

The caller supplies an operation/fill identity. Repository lookup exposes the
existing applied operation. A retry can therefore return the original result
even after the position projection no longer contains the market.

Advantages: unambiguous, minimal schema impact, works across crashes and
processes, and maps cleanly to a future venue fill ID.

Cost: callers that need recovery must retain an operation ID.

### B. Derive identity from market, price, size, and time — rejected

Derived identities collide when a market is reopened and closed at the same
price/size, while timestamp generation changes across retries. Including
`opened_at` helps the first attempt but cannot be reconstructed after the
position has already been removed unless more metadata is added.

### C. Full reconciliation/outbox subsystem — deferred

This would compare local and venue truth and replay incomplete work. It cannot
be verified honestly without a real venue identity/source and expands H-002
beyond its bounded recovery claim.

## Architecture

### OperationReceipt

Promote the existing internal applied-operation record into a read-only public
contract:

```python
@dataclass(frozen=True)
class OperationReceipt:
    operation_id: str
    operation_type: str
    target_id: str
    result: bool | float | None
```

`PositionRepository` gains:

```python
def get_receipt(self, operation_id: str) -> OperationReceipt | None: ...
```

Both repositories return detached values. SQLite reads
`m2_applied_operations`; no migration is required because the existing table
already stores every required field.

Receipt lookup is observational. It does not create an account, mutate state,
or convert storage errors into “not found.” Corrupt/busy database behavior
remains fail closed.

### Tracker boundary

`PositionTracker` exposes a narrow delegation method:

```python
def operation_receipt(self, operation_id: str) -> OperationReceipt | None: ...
```

PnL and close arithmetic remain in the existing transition closure. Receipt
lookup must not duplicate domain calculations in the CLI.

### Operator close flow

`close` gains optional `--operation-id`; `make close-arb` forwards optional
`operation_id=`.

Flow:

1. Construct the durable tracker.
2. If `--operation-id` was supplied, look up its receipt.
3. If a receipt exists:
   - require `operation_type == "close"` and `target_id == market_id`;
   - return the stored float result;
   - report `replayed: true`;
   - do not require an open position or invoke a close transition.
4. If no receipt exists, require the target position to be open.
5. Perform `close_position_with_fill(..., operation_id=supplied_id)`.
6. Return `operation_id`, `replayed: false`, per-close PnL, and cumulative PnL.

If no operation ID is supplied, the CLI generates a unique ID before calling
the tracker, passes it explicitly, and includes it in successful output. This
keeps existing calls functional, but output reports `retry_safe: false`: if the
response itself is lost, the caller never learns that generated ID and cannot
recover by replay. The CLI must not claim durable replay support for an
identity the caller never retained.

An existing receipt reused with another type or target is an identity conflict
and exits non-zero. An unknown supplied ID for a market with no open position
also exits non-zero; it must not create a zero-PnL “successful” receipt.

### Venue fill identity

`Fill` gains an optional immutable `fill_id` field so future venue adapters can
carry the exchange-confirmed trade/fill identity through the existing model.

For a `fill_provider` close:

```text
close:{signal_id}:{leg_id}:fill:{fill_id}
```

If `fill_id` is absent, the current timestamp fallback remains temporarily
available for legacy in-memory tests, but a warning states that durable retry
guarantees are unavailable. H-002 does not pretend a generated receipt is venue
truth. A future real-adapter phase must make `fill_id` mandatory at its adapter
boundary.

Paper close remains:

```text
close:{signal_id}:{leg_id}:paper-close
```

## Concurrency and Failure Semantics

Receipt lookup is an optimization for operator response recovery; repository
`apply()` remains the final idempotency authority.

If two processes race with the same operation ID:

- both may initially see no receipt;
- one obtains the SQLite write lock and commits;
- the second enters `apply()` afterward, finds the ledger row, and returns the
  original result without executing its transition.

Therefore correctness does not depend on a race-free read-before-write check.

Failures:

- missing DB: normal Phase 3 initialization;
- busy/corrupt DB: non-zero, no empty-account fallback;
- receipt type/target conflict: non-zero identity conflict;
- unknown receipt + missing position: non-zero “no open position”;
- transition/storage exception: rollback and propagate;
- first response lost after commit: retry with the same ID returns the stored
  result and unchanged cumulative PnL.

## CLI Response Contract

Successful first close:

```json
{
  "closed": "cond-0",
  "operation_id": "operator-close-20260717-001",
  "replayed": false,
  "retry_safe": true,
  "realized_pnl": 10.0,
  "total_realized_pnl": 10.0
}
```

Successful retry returns the same operation ID and PnL with
`"replayed": true`. Cumulative PnL remains 10.0.

Without a caller-supplied ID, `retry_safe` is false. This preserves the current
convenience path without overstating its recovery guarantee.

## Testing Strategy

All behavior changes follow RED → GREEN.

### Repository contract

- in-memory and SQLite receipt lookup returns `None` for unknown IDs;
- committed bool/float/None results round-trip with their original type;
- returned receipt is immutable/detached;
- receipt identity fields match the applied operation;
- storage errors never become `None`.

### Tracker/engine

- tracker delegates receipt lookup without private repository access;
- venue `fill_id` produces a stable close operation ID;
- repeating the same venue fill does not double-book PnL;
- missing legacy `fill_id` preserves compatibility and emits the documented
  warning;
- paper-close replay behavior remains unchanged.

### True subprocess recovery

Use one temporary DB and independent processes:

1. `run` opens `cond-0`;
2. first `close --operation-id close-001` succeeds, but the test intentionally
   discards its stdout to simulate a lost response;
3. second identical close command returns exit 0 with `replayed: true` and the
   original PnL;
4. `status` proves zero positions and PnL booked exactly once;
5. direct ledger inspection proves one close operation;
6. reusing `close-001` for another market exits non-zero;
7. a newly reopened position closes successfully with a different operation
   ID.

### Regression gates

- complete repository/tracker suite;
- execution and E2E suite;
- CLI/config/Makefile contracts;
- corrected non-overlapping full M2 suite;
- `make planning-status`, Ruff, and `git diff --check`.

## Makefile and Documentation

`make close-arb` help adds:

```bash
make close-arb db=... operation_id=close-001 market_id=cond-0 exit_price=0.50
```

The implementation updates chapter 13's FAQ/operation-identity section rather
than creating a new chapter: H-002 deepens the same persistence mental model.

## Acceptance Criteria

1. A close committed by one process can be replayed by another process using
   the same caller-supplied operation ID after the position has disappeared.
2. Retry returns the original PnL receipt, reports replay, and does not change
   balance, cumulative PnL, positions, or ledger cardinality.
3. Operation ID reuse for another target/type fails closed.
4. A later legitimate reopen/close succeeds with a new operation ID.
5. Future venue adapters have an explicit immutable `fill_id` seam; no claim is
   made that local timestamps are venue truth.
6. Existing close commands without explicit IDs remain functional and clearly
   report that they are not retry-safe.
7. Full M2 and planning gates remain green.
