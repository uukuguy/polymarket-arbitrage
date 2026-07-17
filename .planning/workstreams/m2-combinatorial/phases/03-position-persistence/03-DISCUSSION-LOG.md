# Phase 3: Position Persistence - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves alternatives considered.

**Date:** 2026-07-17
**Phase:** 03-position-persistence
**Areas discussed:** persistence architecture, transaction/idempotency boundary, operator scope, climb quality gates

---

## Persistence architecture

| Option | Description | Selected |
|---|---|---|
| Transactional SQLite repository | Normalized account/position/operation state; injected domain boundary | ✓ |
| Whole-state JSON snapshot | Simple serialization but weak concurrency, evolution, and audit behavior | |
| Supabase/Postgres primary | Multi-host capable but adds remote failure modes before venue integration | |

**User's choice:** Approved the recommended transactional SQLite repository design.

## Transaction and identity

| Option | Description | Selected |
|---|---|---|
| Stable operation ledger | Replay returns original result and never double-books state | ✓ |
| Market ID only | Cannot distinguish replay from legitimate reopen | |
| Best-effort duplicate check | Race-prone because validation is outside the write transaction | |

**User's choice:** Approved explicit atomic transitions and stable operation IDs.

## Phase scope

| Option | Description | Selected |
|---|---|---|
| Local paper lifecycle | Cross-process run/status/close with true subprocess proof | ✓ |
| Include real venue adapter | Requires account/wallet availability and broadens failure surface | |
| Include remote replication | Multi-host concern not required for current operator value | |

**User's choice:** Approved local paper persistence; live adapter and remote replication deferred.

## Climb execution

| Option | Description | Selected |
|---|---|---|
| Local GSD quality gates | Planning/unit/integration/CLI/restart score; no external actions | ✓ |
| External leaderboard semantics | Does not fit this development phase | |

**User's choice:** Requested climb continuous autonomous progress and approved the local-gate interpretation.

## the agent's Discretion

- Repository Protocol shape and row-mapping helpers.
- Compact transition history only when it simplifies implementation.
- Busy-timeout value and diagnostic wording.

## Deferred Ideas

- Real venue adapter/wallet signing.
- Partial fills and multiple lots.
- Remote persistence and multi-host locking.
