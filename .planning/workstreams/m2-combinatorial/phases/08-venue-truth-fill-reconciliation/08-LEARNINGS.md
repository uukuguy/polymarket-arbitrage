# Phase 8 Learnings — Venue-Truth Fill Reconciliation

## What changed our understanding

1. Terminal status and cash completeness are separate gates. A trade can expose useful
   intermediate data without being safe to book.
2. `fee_rate_bps` describes a rule, not the actual cash charged to one fill. Exact
   reconciliation must receive explicit gross and fee facts.
3. Idempotency needs payload identity as well as event identity. Returning an old receipt
   for a changed payload hides disagreement instead of reconciling it.
4. Fingerprint comparison must occur after acquiring the SQLite writer lock; a preflight
   read cannot serialize overlapping processes.
5. A real response-loss proof must reconstruct the original venue request in a new
   process. Receipt pre-read alone bypasses conflict detection.

## Patterns to reuse

- Separate modeled estimates from terminal external authority in the domain type.
- Store exact gross, fee, net, and PnL in a tagged receipt with integer units.
- Version deterministic fingerprints and compare strict equality inside the mutation
  transaction.
- Require all replay inputs needed to reconstruct the fingerprint at operator surfaces.
- Preserve the paper path explicitly when adding live facts; do not silently promote an
  estimate to confirmed truth.

## Adversarial decision questions

1. Why is a complete `MINED` trade still unsafe to book?
2. Which concrete cash field is missing when an API provides only `fee_rate_bps`?
3. Why must a retry with the same fill ID but changed source reference fail?
4. What race remains if fingerprint comparison occurs before `BEGIN IMMEDIATE`?
5. Why does CLI venue replay require the original size and exact cash fields even when a
   receipt already exists?

## Next boundary

A live adapter may now translate a venue's auditable terminal trade facts into
`VenueSettlement`. It must prove actual fee provenance, stable source identity, and
terminality before entering this boundary; live credentials and signing remain outside
Phase 8.
