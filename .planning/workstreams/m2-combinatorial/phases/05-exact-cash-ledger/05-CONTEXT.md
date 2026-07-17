# Phase 5: Exact Cash Ledger - Context

**Gathered:** 2026-07-17
**Status:** Ready for planning
**Source:** autonomous H-003 exploration and approved design

<domain>
## Phase Boundary

Replace binary-float authority for the M2 paper account with integer micro-pUSD across
in-memory state, SQLite state, realized PnL, stake, and close receipts. Preserve current
float-facing price, snapshot, CLI, and configuration contracts. Migrate Phase 4 SQLite
databases transactionally and prove exact restart/replay behavior.

</domain>

<decisions>
## Implementation Decisions

### Monetary representation
- Balance, snapshot balance, stake, fees, realized PnL, and new money receipts use a frozen `Money(micros: int)` value with `1 pUSD = 1_000_000 micros`.
- Inputs convert through `Decimal(str(value))` and `ROUND_HALF_EVEN`; boolean, non-finite, and SQLite-overflow values fail closed.
- Market prices, percentages, signals, and slippage estimates remain float-facing.

### Domain compatibility
- `PositionState` and `Position` hold exact money authority and expose legacy float-compatible properties.
- Tracker risk checks and mutations operate on exact money. Public close methods, snapshots, and CLI JSON keep their current float presentation.
- Paper realized PnL is centralized and rounded once to one micro-pUSD. Future venue-confirmed cash truth may replace the modeled amount but is outside this phase.

### Persistence and migration
- Add integer account and stake columns transactionally under `BEGIN IMMEDIATE`.
- Backfill legacy `REAL` through the same money conversion, validate all authoritative cells, and make restart idempotent.
- New writes dual-write integer authority plus derived legacy `REAL` compatibility projections.
- Invalid/corrupt data never triggers fallback reset or partial migration.

### Receipts
- New close results persist as tagged JSON `{\"kind\": \"money\", \"micros\": N}`.
- Legacy bool/float/None receipts remain readable with their original types.
- Unknown/malformed tags fail closed.

### Verification and delivery
- Use three RED→GREEN slices: money/domain, repository/migration, CLI/restart.
- Phase closure requires full corrected M2 tests, Makefile contracts, Ruff, diff check, zero planning drift, true subprocess smoke, SUMMARY, learnings, teaching, JOURNAL, and H-003 climb evaluation.

### the agent's Discretion
- Exact helper names and focused private migration helpers, provided the public compatibility and failure contracts remain unchanged.
- Test fixture organization and whether teaching extends chapter 13 or adds chapter 14; prefer a new chapter if the mental model is independently useful.

</decisions>

<canonical_refs>
## Canonical References

### Approved architecture
- `docs/superpowers/specs/2026-07-17-m2-exact-cash-ledger-design.md` — complete representation, migration, receipt, failure, and test contract.
- `.planning/notes/m2-cash-ledger-exactness.md` — exploration evidence, official venue constraints, and H-003 acceptance surface.

### Existing durable lifecycle
- `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-CONTEXT.md` — immutable operation and replay decisions that H-003 must preserve.
- `.planning/workstreams/m2-combinatorial/phases/04-durable-close-receipts/04-01-SUMMARY.md` — delivered code paths and verified response-loss smoke.
- `docs/learning/13-仓位持久化.md` — current user-facing repository/receipt mental model.

</canonical_refs>

<specifics>
## Specific Ideas

- The observed regression target is two logically exact `+10` closes persisting as `19.999999999999996` in a Phase 4 `REAL` ledger.
- Raw SQLite assertions must check integer storage (`typeof(...) = 'integer'`) and exact micro values, not only rounded CLI output.
- A response-loss retry must replay the exact tagged receipt without running the transition twice.

</specifics>

<deferred>
## Deferred Ideas

- Live venue order signing/encoding, tick-size normalization, SDK selection, fee truth, reconciliation/outbox, and partial-fill aggregation.
- Dropping legacy `REAL` compatibility columns.

</deferred>

---

*Phase: 05-exact-cash-ledger*
*Context gathered: 2026-07-17 from approved H-003 design*
