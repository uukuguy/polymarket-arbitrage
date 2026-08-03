# M1 Structure Drift Classifier Recovery

**Date:** 2026-08-02  
**Status:** Draft for written review — implementation not started  
**Scope:** Recover the production drift-safe switch for generation 848 after the
first `row-chain-sha256-v2` comparison correctly failed closed with 121
unclassified legacy-only members. This design does not authorize a pointer,
read-mode, or Quote change by itself.

## 1. Production Problem and Evidence

Release `c3f1d7cdf55272be4908f2736c61b844a7b50243` deployed the
`row-chain-sha256-v2` comparator. The production comparison completed naturally
as stale in minutes without an execution failure and produced no authorization
receipt:

- pointer: generation 848, publication `bb964...`, window `97b262...`;
- exact legacy identity: snapshot 845;
- generation/projection member counts: 36,762 / 36,762;
- legacy member scan count: 35,138;
- shared: 27,946;
- fresh additions: 8,816;
- fresh-source-absent: 6,982;
- current-nontradable: 87;
- event-only-quarantine: 2;
- overlap-conflict: 0;
- unclassified: 121.

The equality `shared + fresh-addition == generation` proves that all 121 are
legacy-only. A local two-member reproduction proved the missing semantic:

1. both members belong to one legacy `complete-supported` standard neg-risk
   group;
2. one fresh member becomes inactive;
3. fresh group truth becomes `complete-unsupported` with reason
   `standard-neg-risk-has-non-tradable-members`;
4. the inactive member is classified `current-nontradable`;
5. its still-active sibling disappears from the generation because the whole
   group is ineligible, but the existing member-only taxonomy assigns it no
   reason and marks it unclassified.

The run also exposed two recovery defects:

- `projection_member_count` is currently accumulated only while scanning
  generation rows. It proves that every emitted generation row can be
  independently reproduced, but it is not a complete fresh expected-universe
  proof and cannot detect an omitted generation row.
- a deterministic stale comparison ID is reused forever. With pointer 848
  unchanged, `INSERT OR IGNORE` finds the same stale row and the scheduler exits
  before initialization, so a corrected classifier contract cannot recover
  automatically.

The production data plane remains protected: legacy serving is active, Quote is
disabled, pointer 848 is unchanged, and the app health check is passing.

## 2. Decision

Introduce a versioned **drift classifier contract v2** with four inseparable
parts:

1. a complete independent fresh projection phase;
2. an authenticated `fresh-group-ineligible` removal class;
3. durable bounded diagnostics for every unresolved member;
4. deterministic contract supersession without retrying an unchanged failed
   contract.

The exact comparison path remains unchanged and independently authoritative.
The row-chain framing remains `row-chain-sha256-v2`; classifier semantics are
versioned separately because a semantic change is not a hash implementation
change.

Rejected alternatives:

- **Diagnostic-only first release.** It gives maximum epistemic separation but
  requires two more production releases and prolongs the Quote outage. The
  confirmed local reproduction plus fail-closed diagnostics in the combined
  release provide equivalent safety.
- **Blindly retry stale progress.** The result is deterministic for one frozen
  identity and contract, so retries waste capacity and create alert noise.
- **Fold the 121 rows into `current-nontradable`.** That loses group-level
  meaning and could authorize incomplete or conflicting source data.
- **Wait for another publication.** The current pointer cannot move until a
  switch gate authorizes it, so this can deadlock indefinitely.

## 3. Versioned Comparison Identity and Recovery

Define the exact constant:

```text
structure-drift-classifier-v2
```

Add `classifier_contract_version` to progress and receipt rows. Existing rows
are migrated as `structure-drift-classifier-v1`. Bind the field into:

- comparison identity JSON and `comparison_id`;
- active-progress uniqueness and lookup;
- every progress checkpoint;
- receipt payload and receipt digest;
- read-only status validation and health output.

The scheduler-owned child must call initialization for the current expected
contract before returning an existing terminal status. Initialization runs in
one `BEGIN IMMEDIATE` transaction:

1. revalidate pointer, publication, window, source, exact receipt, validation,
   and certification identities;
2. if an older-contract row is active, compare-and-set it to stale with exact
   terminal reason `drift-classifier-contract-superseded`;
3. leave older sealed or stale rows immutable as audit evidence;
4. insert the deterministic v2 progress row at the first phase and cursor zero;
5. commit all mutations together.

An existing stale v2 row for the same frozen identity is not retried. A new run
requires a source/pointer identity change or a newer classifier contract. This
prevents both permanent old-contract deadlock and same-contract retry storms.

Only the current classifier contract may produce `drift-safe-sealed`
authorization. Historical receipts remain queryable but cannot authorize the
current gate.

## 4. Complete Independent Fresh Projection

> **2026-08-03 amendment:** raw event JSON cannot provide a true database-side
> 500-member keyset because SQLite must re-expand the complete array. The
> approved durable sidecar design is specified in
> `2026-08-03-m1-durable-event-member-staging-design.md` and is binding for
> this section. The projection reader must consume a sealed, append-only
> per-ordinal member sidecar rather than `json_each()` over parent payloads.

Add a bounded `fresh-projection-members` phase before generation-member
comparison. It scans the pinned staging source by market-ID keyset, never by the
generation universe.

The projection keyspace is the ordered union of:

1. every row in market staging, ordered by `(0, market_id)`; and
2. every event-member anti-join candidate absent from market staging, ordered
   by `(1, market_id, event_id, source_ordinal, member_ordinal)`.

The second stream is mandatory: a market-driven scan alone cannot prove that an
uncertified event-only member is absent. An event-only candidate is excluded
only when its exact quarantine envelope recomputes. Any other active event-only
candidate records an unresolved diagnostic and makes the comparison stale.

For each candidate, one bounded bulk reader loads:

- the raw market payload;
- all pinned event relationships and raw event payloads;
- independently normalized group truth derived from the complete pinned event
  catalogue and global event/market relation graph;
- exact market-side and event-only quarantine evidence.

Global semantics are mandatory. The reader bulk-loads global relation
cardinality for every member in the candidate groups and applies the same
normalizer priority as a one-shot normalization of the full pinned catalogue.
A market related to another event makes the group `incomplete-source` even when
the local event also contains an inactive member. Per-event normalization may
be used only after this global conflict proof has been applied.

The phase emits a canonical structural member only when:

- raw market normalization succeeds;
- exactly one global event/member identity resolves;
- member and market identity agree;
- the independent group truth is `complete-supported` and standard;
- neither exact quarantine applies.

The emitted row is the complete 11-field `StructuralMemberIdentity` tuple:
event ID, group ID, market ID, member kind, active, closed, condition ID, Yes
token ID, No token ID, neg-risk flag, and incomplete flag. Market ID must be
globally unique in the eligible output. A duplicate or two different identities
for one market ID emits no projection row, records a deterministic conflict
diagnostic, and fails closed.

Rows are committed in `(market_id, event_id, group_id)` order with domain
`projection-member` and checkpointed by the union cursor. Count and root
therefore represent the complete eligible fresh expected universe, including
rows absent from generation. Unresolved event-only candidates are bound by the
diagnostic commitment and force stale; they are not fabricated into the
projection root. Generation scanning continues to produce:

- its domain-separated `generation-member` audit root; and
- a `projection-member` comparison mirror over the exact generation rows.

Authorization requires all three facts:

```text
fresh projection count == generation count == generation mirror count
fresh projection root  == generation mirror root
```

This replaces the misleading generation-driven projection count without
weakening the existing generation audit commitment.

The phase may read only frozen staging and immutable publication evidence. It
must not write serving tables, generation component tables, the pointer, exact
receipts, read mode, or Quote state.

## 5. Authenticated Group-Ineligible Removal

Extend `FreshMemberEvidence` with independently derived fields:

- resolved event-source count;
- normalized projected member before group eligibility filtering;
- exact fresh group-truth event ID, group ID, type, quality, reason, and
  membership hash derived with global catalogue conflict precedence;
- whether the exact legacy structural identity is present in the pinned source.

Add the removal class and row-chain domain:

```text
fresh-group-ineligible
class/fresh-group-ineligible
```

A legacy-only member enters this class only when every condition is true:

1. evidence comes from the pinned published window and generation
   certification is valid;
2. raw market and exactly one event source resolve;
3. the independently normalized source member plus normalized market fields
   equal the complete legacy structural identity, including the same event ID
   and group ID;
4. the member itself is active/open;
5. neither event-only nor market-side quarantine applies;
6. exact fresh group truth for that same `(event_id, group_id)` is standard and
   `complete-unsupported`, and its membership hash binds the globally resolved
   member set;
7. the reason is the closed allowlist value
   `standard-neg-risk-has-non-tradable-members`;
8. the member is absent from the certified generation.

`incomplete-source`, conflicting event membership, invalid membership, invalid
neg-risk classification, augmented groups, identity mismatch, missing evidence,
or an unknown reason remain unclassified and fail closed. This keeps the new
class narrow enough to prove the confirmed sibling transition without treating
source corruption as ordinary drift.

The class is added to legacy reconstruction commitments, progress/receipt class
counts and digests, status verification, and tamper tests. It is not added to
generation reconstruction.

## 6. Durable Diagnostic Evidence

Diagnostics are generated inline while the existing bounded member chunks are
already in memory; they must not trigger a second production scan.

Every unresolved row receives exactly one code by the following first-match
decision table. Later rows cannot override an earlier match.

| Priority | Applies to | Predicate | Diagnostic code |
|---:|---|---|---|
| 1 | either | duplicate market ID or divergent identity | `duplicate-market-identity` |
| 2 | either | evidence lookup missing or identity revalidation failed | `evidence-missing` |
| 3 | generation-only | generation certification is not current | `generation-addition-not-certified` |
| 4 | generation-only | absent from both pinned catalogues | `generation-addition-source-absent` |
| 5 | either | global relation cardinality exceeds one | `conflicting-event-membership` |
| 6 | either | event classification/neg-risk flags are invalid | `invalid-neg-risk-classification` |
| 7 | either | member structure or required booleans are invalid | `invalid-event-membership` |
| 8 | either | event-only candidate lacks an exact quarantine | `uncertified-event-only-member` |
| 9 | either | group truth is `incomplete-source` for another reason | `group-incomplete-source` |
| 10 | either | group type is augmented | `augmented-group` |
| 11 | either | group is complete-unsupported with an unapproved reason | `group-complete-unsupported-unknown-reason` |
| 12 | generation-only | exact event-only quarantine exists | `generation-addition-event-only-quarantine` |
| 13 | generation-only | exact market-side quarantine exists | `generation-addition-market-side-quarantine` |
| 14 | generation-only | current member is inactive or closed | `generation-addition-current-nontradable` |
| 15 | either | independent projection is absent | `active-open-projection-missing` |
| 16 | either | independent projection differs | `active-open-projection-mismatch` |
| 17 | legacy-only | more than one authorized removal reason survived invariant checks | `multiple-removal-reasons` |
| 18 | legacy-only | no authorized removal reason survived | `other-zero-removal-reason` |
| 19 | generation-only | no addition predicate matched | `generation-addition-other` |

Classified rows do not receive unresolved diagnostics. The implementation must
prove this table is total and exclusive for both one-sided paths with predicate
permutation tests.

Progress stores canonical JSON fields for:

- `diagnostic_counts_json` — exact count per code;
- `diagnostic_digest_state_json` / final root — one ordered
  `diagnostic/unclassified` row chain binding code, side, member tuple, group
  truth, and predicate bitset;
- `diagnostic_samples_json` — at most three lexicographically smallest public
  market/event/group identities per code plus the predicate bitset.

When invalid or event-only evidence cannot form a complete member tuple, the
diagnostic row uses a fixed candidate envelope with nullable 11 identity fields,
source ordinal/member ordinal, raw identity hashes, group truth, and predicate
bitset. This encoding is canonical and participates in chunk-invariance golden
tests; missing fields are explicit nulls, never omitted keys.

Samples are observability evidence only; authorization depends on the full count
and digest. All fields checkpoint atomically with class state and are included
in an authorization receipt when the comparison seals.

Diagnostics are most important when the comparison is stale, so the stale
finalizer also inserts an append-only
`structure_generation_drift_terminal_receipts` row in the same transaction that
changes progress to stale. The failure receipt binds the full comparison
identity, hash algorithm, classifier contract, terminal reason, class
counts/digests, diagnostic counts/root, canonical samples JSON and its digest,
and creation time. Update/delete triggers make the row immutable. A failure
inserting the receipt rolls back the stale transition. On restart, a terminal
row without one valid matching receipt is treated as corrupt terminal evidence,
not as an authenticated diagnostic.

The finalizer writes exact terminal reason `drift-overlap-conflict` when any
overlap conflict exists, otherwise `drift-unclassified`, and preserves both
histograms, the root, and samples.

Status validates the appropriate append-only authorization or terminal failure
receipt before exposing terminal evidence. Both `/health` and `/healthz` read
only that validated status and emit a failing Structure drift subcheck
containing terminal reason, histogram, bounded samples, diagnostic digest,
classifier contract, comparison ID, and checkpoint time.

The production delivery chain is the existing resident Polywatch process, not
a new ledger and not the Better Stack snapshot heartbeat:

```text
terminal receipt
→ validated drift status
→ /health + /healthz Structure drift subcheck
→ resident polywatch /healthz poll (every two minutes)
→ component notification state (dedupe/reminder/recovery)
→ Telegram alert or recovery message
```

`decide_l1` must inspect the Structure drift terminal subcheck before generic
snapshot age/outcome checks so the Telegram reason carries the diagnostic codes
and comparison ID. Existing component notification state remains the durable
delivery/deduplication authority. The L1 incident clears only after a later
current-contract comparison seals, the drift subcheck becomes healthy, and a
recovery notification is delivered. Better Stack continues its separate
availability/heartbeat role and is not treated as code-specific diagnostic
delivery.

Tests cover the complete chain: terminal write → status receipt validation →
health/healthz failure payload → Polywatch alert/dedupe/reminder → healthy clear
payload → recovery delivery. Production acceptance includes
`make polywatch-healthz-dry` and resident state/log evidence. No stale state may
be reported as a generic execution timeout or silently degrade forever.

## 7. State Machine and Failure Handling

The v2 phase order is:

```text
source-events
→ source-markets
→ fresh-projection-members
→ generation-members
→ legacy-members
→ fresh-group-truth
→ sealed | stale
```

Every phase remains cursor-keyset bounded, scheduler-owned, default-off by
configuration, and subordinate to Quote priority and the shared producer lock.
The existing 500-row, 100-chunk, 45-second cooperative slice and 75-second
parent timeout remain unchanged unless production evidence proves they are
unsafe.

Execution exceptions use the dedicated attempt ledger and existing automatic
retry/recovery policy. Deterministic semantic failure becomes terminal stale
with authenticated diagnostics and no same-contract retry. Neither path may
mutate pointer, legacy serving, exact receipt, publication identity, read mode,
or Quote state.

## 8. Migration and Compatibility

Schema migration is restart-safe and follows the existing small-table rebuild
contract:

- old progress/receipt rows receive classifier v1;
- active uniqueness includes hash algorithm and classifier contract;
- new diagnostic columns use canonical empty JSON/state defaults only for new
  v2 rows; historical v1 rows remain valid audit records;
- receipt immutability triggers are recreated;
- injected failure rolls back the authority-table rebuild and a later
  `init_schema()` completes it without business-row loss.

The domain registry is extended by exactly:

```text
class/fresh-group-ineligible
diagnostic/unclassified
```

This does not change row-chain framing. Decoder validation remains strict by
algorithm, domain, state length, count, and canonical JSON.

## 9. Verification Contract

Implementation is not eligible for deployment until all of the following are
proved with RED-before-GREEN tests:

1. the two-member sibling fixture reproduces the current v1 unclassified row;
2. classifier v2 assigns only the active sibling to
   `fresh-group-ineligible` and the inactive member to
   `current-nontradable`;
3. `incomplete-source`, conflict, augmented, invalid flags/membership, unknown
   reasons, missing evidence, and projection mismatch remain unclassified;
4. the full fresh projection detects a generation omission even when every
   emitted generation row projects correctly, and a non-quarantinable
   event-only anti-join candidate fails closed;
5. an inactive member plus a cross-event conflict yields global
   `incomplete-source` and remains unclassified; the local inactive-member
   reason must not win;
6. chunk sizes 1, 17, and 500 produce identical counts, roots, diagnostics,
   samples, terminal state, and receipt;
7. active classifier v1 is atomically superseded by v2 cursor zero, with an
   injected failure rolling back both changes;
8. an existing stale v2 identity is not retried, while a newer contract starts
   exactly once;
9. two concurrent scheduler ticks and the outer
   `_maybe_advance_structure_drift` terminal-status path create exactly one v2
   row before any stale short circuit;
10. classifier version, new class, complete projection, diagnostics, and
   terminal reason are receipt-bound and every field tamper fails closed;
11. stale finalization atomically writes an immutable terminal failure receipt;
    receipt insertion failure rolls back stale, and tamper/missing/mixed
    identity evidence is rejected after restart;
12. the diagnostic decision table is total and exclusive across both sides and
    predicate permutations;
13. historical v1 receipts remain queryable but cannot authorize v2;
14. no comparison path writes pointer, serving, publication, generation,
    source, read-mode, or Quote rows;
15. status, health, attempt ledger, and resident Polywatch alert lifecycle
    expose, deduplicate, remind, and clear exact diagnostic evidence;
16. bounded SQL uses covering/keyset plans without a temporary order sort or
    per-member source queries;
17. the complete local gate retains at least 2x measured improvement over the
    original v1 gate and every child stays within cooperative/parent deadlines;
18. Ruff, documentation checks, planning status, complete collection, full
    pytest, and diff checks pass;
19. independent review approves classification exclusivity, projection
    independence, migration rollback, receipt authentication, retry semantics,
    query plans, and data-plane write absence.

## 10. Production Acceptance Sequence

Deploy only one independently approved exact SHA:

1. keep `Structure=true`, `Quote=false`, `read_mode=legacy`, and drift enabled;
2. verify both machines run the same image and exact source SHA;
3. verify v1 stale evidence remains immutable and v2 starts naturally at cursor
   zero without changing pointer 848 or legacy serving;
4. observe the complete natural v2 run from initialization to a sealed receipt;
5. require `make structure-generation-drift-compare` to exit zero with
   `authorization_mode=drift-safe-sealed`, and require
   `make polywatch-healthz-dry` plus resident state/log evidence to show no
   unresolved drift alert;
6. switch the same exact image to `read_mode=generation` while keeping
   `Quote=false`; verify generation readers, authenticated status, and all
   non-Quote strict health gates;
7. enable `Quote=true` in a second config-only release of the same image;
8. verify strict health, Quote age below 300 seconds, and no opportunity 503;
9. observe one complete new natural Structure generation and Quote handoff from
   zero; throughout the entire generation, Quote age must remain below 300
   seconds and the opportunity endpoint must never return 503;
10. require no mixed identity, timeout, or observer-induced failure and require
    the Structure failure counter to clear through natural success;
11. continue candidate lifecycle qualification before declaring M1 complete.

Any unresolved diagnostic, receipt mismatch, identity drift, timeout, health
failure, Quote staleness, or mixed-generation read fails closed and blocks the
switch. Failed windows and observer-induced windows cannot be reused as
acceptance evidence.
