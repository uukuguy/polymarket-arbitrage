# Phase 7: Durable Partial-Fill Accounting - Context

**Gathered:** 2026-07-17  
**Status:** Ready for execution

<domain>
Close v3 positions through multiple exact fills with immutable fill-level idempotence,
remaining quantity/cost basis, atomic SQLite restart, and response-loss recovery.
</domain>

<decisions>

- Position fields represent remaining authority after every partial fill.
- Proportional cost basis uses HALF_EVEN; final fill takes all residual cash.
- Venue `fill_id` maps to canonical `venue-fill:{fill_id}` operation identity.
- Anonymous partial fills fail; anonymous full operator/paper closes remain compatible.
- Existing positions/operations tables suffice; no parallel fill ledger or new command.
- H-006 owns actual venue proceeds/fees and reconciliation.

</decisions>

<canonical_refs>

- `docs/superpowers/specs/2026-07-17-m2-durable-partial-fill-design.md`
- Phase 6 SUMMARY/LEARNINGS
- `.planning/threads/execution-accounting.md`

</canonical_refs>

