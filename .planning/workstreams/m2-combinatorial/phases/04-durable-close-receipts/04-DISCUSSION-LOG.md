# Phase 4: Durable Close Receipts - Discussion Log

> **Audit trail only.** Planning decisions live in `04-CONTEXT.md`.

**Date:** 2026-07-17
**Phase:** 04-durable-close-receipts
**Areas discussed:** recovery identity, receipt surface, CLI truthfulness, venue fill seam

## Recovery approach

| Option | Description | Selected |
|---|---|---|
| Explicit durable receipt | Caller retains immutable ID; ledger returns the committed result | ✓ |
| Derive from market/price/time | Collides across reopen or changes across retry | |
| Full reconciliation/outbox | Needs venue truth and materially expands H-002 | |

**User decision:** Approved explicit durable receipts and caller-owned identities.

## Compatibility and truthfulness

| Case | Contract |
|---|---|
| Caller supplies ID | Cross-process replay supported; `retry_safe: true` |
| CLI generates ID | Existing convenience retained; `retry_safe: false` |
| Existing ID targets another market/type | Non-zero identity conflict |
| Unknown ID and no open position | Non-zero; no invented zero-PnL receipt |

**User decision:** Approved the written response and failure semantics.

## Venue seam

- Selected: optional `Fill.fill_id` now, stable operation ID when present.
- Compatibility: timestamp fallback remains temporarily and emits a warning.
- Deferred: mandatory fill identity belongs at a future real venue adapter boundary.

## Verification

- The first close response is intentionally discarded.
- A new process replays the same ID and receives the stored PnL.
- Balance, cumulative PnL, position count, and ledger count prove no double-book.
- Conflict and legitimate reopen/new-ID paths are exercised.

**User decision:** Approved the complete written spec for autonomous implementation.
