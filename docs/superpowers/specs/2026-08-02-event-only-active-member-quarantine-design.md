# Event-only active member quarantine

## Problem and invariant

A complete Gamma `/events` catalogue can embed an active, open neg-risk market that is absent from the same window's complete `/markets` catalogue. Fresh snapshot 847 contains 62 such relations. Publishing the event membership unchanged would create a dangling active membership, so certification correctly fails with `membership-invalid`; treating the anti-join as harmless would weaken the active-open drift invariant.

The publication contract will instead recognize one third, mutually exclusive quarantine class: `active-open-neg-risk-event-member-absent-from-complete-market-catalogue`. It applies only when the pinned source window is complete, the event and its canonical event-market relation exist, that market has exactly one event parent, the embedded member is active/open neg-risk with a non-empty group identity, and no market staging row exists. Missing-market memberships outside that exact predicate remain fatal.

## Canonical projection

Normalization uses one shared event projection for `memberships`, `group_truth`, and `source_events` certification. It removes only authenticated event-only members. Group truth is then derived from the remaining members: expected count, active named count, and membership hash are recomputed. A group with no remaining members emits no truth row. This preserves the invariant that every published truth describes exactly its published memberships and that no published active membership points at an absent generation market.

The event and event-tag projections are unchanged. No generation market is synthesized from the embedded event member because that object is not an authenticated substitute for the complete market-catalogue payload. Opportunity readers therefore exclude the quarantined member naturally.

## Durable evidence and certification

The `issues` source keyset is the deterministic union of existing market-side candidates and event-only candidates, ordered by market ID. An event-only issue contains the fixed reason plus a SHA-256 receipt over a canonical envelope containing the full event payload hash, the embedded market payload hash, the event source ordinal, the embedded member ordinal, event ID, group ID, and market ID. IDs remain bounded by the existing issue columns while the receipt itself is fixed-size.

Certification independently reconstructs the envelope from the pinned window. It proves the market row is still absent, the event-market relation is unique and canonical, both ordinals select the recorded event/member, the embedded member satisfies the exact active/open neg-risk predicate, the generation membership and market are absent, and the generated issue exactly matches. `source_events` compares against the same filtered projection, so the only source-relation difference admitted is one backed by that exact issue. Forged issues, duplicate parents, identity changes, incomplete windows, ordinary markets, inactive/closed members, and any generated dangling member fail closed.

The normalization contract version is bumped so an in-flight publication created under the older projection is superseded rather than resumed with mixed semantics.

## Recovery and operations

Quarantine is generation-local. If a later complete `/markets` catalogue contains the market, the predicate is false: the membership, recomputed group truth, and generation market publish normally and the issue disappears. Existing health logic reports a non-fatal warning from the committed quarantine count; no long-lived degraded state is introduced after source convergence.

For failed subprocess diagnosis, `membership-invalid` may carry a fixed allowlisted subtype and a SHA-256 key fingerprint. The CLI emits only a bounded marker such as `structure-sync-failure failure_kind=membership-invalid membership_kind=<allowlisted> key_sha256=<64hex>`; scheduler parsing accepts only those fixed fields and never forwards raw market/event identifiers. Existing two-field JSON failure output remains compatible.

## Verification

Tests first reproduce the production shape and fail under the old implementation. They prove exact quarantine, recomputed partial and empty group truth, issue/source authentication, ordinary active-open drift rejection, duplicate/forged evidence rejection, bounded marker parsing, health warning/opportunity exclusion, next-generation recovery when `/markets` catches up, normalization-contract supersession, and bounded resumable keyset behavior. Focused suites, Ruff, planning-status, and the full repository suite must pass. This work does not deploy or mutate production.
