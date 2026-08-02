# M1 Structure Drift-Safe Read-Switch Gate

**Date:** 2026-08-02  
**Status:** Base gate implemented; row-chain v2 performance amendment approved
**Scope:** Authenticate a generation read switch when the pinned legacy snapshot
and the generation were built from different complete source windows. This
design does not deploy, change the production pointer, or change read mode.

## 1. Problem

Generation 848 is published and pointer-bound with valid authentication. Its
existing comparison receipt pins legacy snapshot 845 and is also authentic, but
the exact comparison fails:

- legacy market count: 115,839;
- generation market count: 121,887;
- mismatch reasons: market count, universe hash, and source-truth hash;
- legacy 845 was taken about 23 hours 56 minutes before generation 848;
- generation 848 owns a sealed published source window (16,314 events and
  122,101 raw markets), 121,887 generated markets, 227 issues, matching expected
  and committed counts, and a valid pointer/authentication digest.

The current comparator streams four immutable aggregates: legacy universe,
generation universe, legacy unsupported groups, and generation unsupported
groups. It proves that both compared identities and their hashes have not been
silently changed. It does not align shared members, independently reconstruct a
fresh expected projection, or explain additions and removals from pinned source
evidence. Exact equality between different temporal source identities is
therefore the wrong switch condition.

The fix must not accept a numeric or percentage tolerance. Every difference is
either authenticated and classified or the gate fails closed.

## 2. Decision

Add a second, explicit **drift-safe comparison**. It supplements rather than
replaces the existing exact comparison.

The drift-safe comparison has two independent proofs:

1. **Same-window compatibility proof.** Stream the sealed published generation
   source window through the legacy normalization functions and the explicit
   quarantine policy. Build a legacy-reader-compatible structural projection
   without reading generation rows as expected values and without writing any
   legacy serving table. Its eligible universe and fresh group truth must match
   generation exactly.
2. **Temporal-delta proof.** Partition the stale legacy universe and fresh
   generation universe into shared, generation-only, and legacy-only members.
   Shared structural identity must be exact. Every one-sided member must have
   one mutually exclusive explanation derived from the pinned source,
   freshness state, or exact quarantine evidence.

A read switch is authorized only by either:

- the unchanged exact comparison result; or
- a sealed drift-safe receipt for the same current legacy identity, generation
  pointer identity, publication, window, contract, and validation evidence.

## 3. Independent Same-Window Projection

### 3.1 Source-window lifecycle

The initial implementation supports the production post-publication case only.
The source window must have `status='published'` and
`published_snapshot_id=generation_snapshot_id`; publication, pointer, and window
must all resolve generation 848 as one identity. Production window 97b satisfies
this contract. Requiring `status='complete'` here would make the real 848 gate
impossible because successful pointer publication has already advanced the
window to `published`.

`published` proves lifecycle sealing, but comparison still revalidates the
exact frozen source counts and hashes on every resume and at receipt seal. Any
post-publication staging drift therefore invalidates progress and fails closed.
Supporting a pre-publication `complete` window would require a separate state
transition and receipt-binding design; it is explicitly out of scope.

### 3.2 Feasibility

The frozen staging tables retain the raw `/events` and `/markets` payloads and
their relationship rows. The existing pure `normalize_events` and
`normalize_market` functions can consume those payloads without network access.
The publication path already exposes deterministic quarantine functions for
market-side and event-only inconsistencies.

The projection therefore does not need a second snapshot or shadow serving
tables. A bounded projector streams staging rows, constructs canonical tuples,
and hashes them. It must not:

- call Gamma, CLOB, or any live endpoint;
- read generation component rows to derive expected values;
- insert into `events`, `event_market_memberships`, `neg_risk_group_truth`,
  `markets`, or alter the legacy latest-snapshot identity;
- move or repair `current_structure_generation`;
- apply undocumented cleanup or tolerance rules.

### 3.3 Compatibility semantics

The projection uses the legacy canonical field meanings and reader eligibility
predicate, but applies the generation contract's authenticated quarantine
policy before hashing. This distinction is required: raw legacy normalization
alone would retain event-only structural members for which the complete market
catalogue has no row. Generation intentionally quarantines those members and
recomputes group truth. Treating that intentional contract change as an
unexplained mismatch would recreate the current blocker.

For every fresh event, the projector:

1. runs `normalize_events` on the pinned raw payload;
2. identifies event-only members from the complete pinned relationship and
   market catalogues;
3. accepts removal only when the exact fixed-size event-only quarantine receipt
   recomputes;
4. recomputes membership and group truth from the remaining members;
5. marks cross-event identity conflicts with the same deterministic contract;
6. emits canonical member and group-truth tuples.

For every fresh market, the projector:

1. runs `normalize_market` with the pinned event relationship;
2. accepts omission only when the exact market-side quarantine receipt
   recomputes;
3. emits the canonical structural market tuple otherwise.

The final expected eligible-universe tuple uses the existing strategy-facing
identity fields: group identity, recomputed membership hash, market identity,
and YES token identity. The fresh group-truth stream includes the full
structural member identity described below. Expected projection hash and
generation hash must be byte-for-byte equal; there is no tolerance.

## 4. Temporal Delta Classification

### 4.1 Canonical shared identity

Comparison is keyed at member granularity, not only by group membership hash.
A shared eligible member must agree exactly on:

- event ID;
- neg-risk group ID;
- market ID;
- member kind;
- active and closed state;
- condition ID;
- YES and NO token IDs;
- neg-risk identity and incomplete flag.

Price, liquidity, volume, book values, fetch timestamps, and other mutable
market observations are not Structure switch identities and are excluded.

If a key exists on both eligible sides but any structural identity differs, it
is an `overlap-conflict`. Overlap conflicts are never freshness drift and always
fail the gate.

Membership hash is validated from exact member rows. A legitimate member
addition or removal changes the group hash, so stale and fresh group hashes are
not directly required to match. The independently reconstructed fresh group
hash must, however, exactly match generation 848.

### 4.2 Generation-only members

A generation-only member is classified `fresh-addition` only if all of the
following hold:

- its market and parent/group relationship are present in the complete pinned
  generation source window;
- the independent compatibility projector produces the same structural row;
- the generation source certification covers the row and its group truth;
- no quarantine issue targets the row.

Failure of any condition is `unclassified`, not an allowed addition.

### 4.3 Legacy-only members

Each legacy-only member must match exactly one reason, evaluated in a stable
priority order:

1. `current-nontradable`: current pinned event/market evidence identifies the
   member but proves it inactive or closed;
2. `event-only-quarantine`: the active event member is absent from the complete
   pinned market catalogue and its exact event-only quarantine receipt
   recomputes;
3. `market-side-quarantine`: the current market is omitted by an exact
   missing-group or parent-absent quarantine receipt;
4. `fresh-source-absent`: the market identity is absent from both complete
   pinned catalogues and no current relationship claims it.

The classifier records the first matching reason only and asserts that no
other reason matches. Present-active source rows missing from generation,
ambiguous relationships, conflicting quarantine evidence, source rows present
in only an unexplained location, and all other cases are `unclassified` and
fail closed.

### 4.4 Reconstruction invariants

The sealed result must prove:

- `legacy = shared + legacy-only`;
- `generation = shared + generation-only`;
- every row appears in exactly one partition;
- every legacy-only row appears in exactly one reason partition;
- expected fresh projection equals generation exactly;
- expected fresh group truth equals generation group truth exactly;
- `overlap-conflict = 0` and `unclassified = 0`.

Counts are accompanied by canonical tagged partition digests. The receipt also
stores union roots over the tagged rows, so verifiers can reconstruct and
authenticate both sides even though rows from different classes interleave in
market sort order. Counts alone never authorize the gate.

## 5. Durable State Machine

The comparison is resumable and bounded. One invocation reads at most the
configured row budget, with the existing production ceiling of 500 source or
universe rows. The phases are:

1. `source-events` — project event members and fresh group truth;
2. `source-markets` — project structural markets and eligible universe;
3. `generation-members` — authenticate generation rows, shared rows, and fresh
   additions;
4. `legacy-members` — authenticate stale-only rows and mutually exclusive
   removal reasons;
5. `fresh-group-truth` — seal exact source-projection/generation truth equality;
6. `sealed`.

Each phase persists cursor, serializable hash state, per-class count/hash state,
and checkpoint time with compare-and-swap semantics. A crash may repeat a
chunk; it cannot skip a chunk or combine two identities. Source lookups use
bounded bulk reads, not per-row SELECT loops.

### 5.1 Advancement ownership and trigger

Production advancement is owned only by the existing Structure scheduler. A
new explicit, default-off setting enables the drift-safe maintenance stage.
When enabled and the current generation lacks a drift-safe receipt, a normal or
operator-requested scheduler tick selects one bounded comparison slice before
collecting another source window.

The scheduler must use its existing `_tick_lock`, acquire the shared
`producer_lock`, and run the existing double Quote-priority check before it
creates attempt truth or advances comparison state. If Quote is active or due,
it records the existing Structure defer receipt and does no comparison work. A
comparison chunk uses the admitted deadline and releases the producer slot at
the same boundary as other Structure work. The durable progress row and CAS
cursor additionally prevent two admitted workers from owning the same chunk.

The Makefile command is a read-only receipt/progress verifier; it never advances
production comparison state outside scheduler admission. An operator who wants
immediate progress uses the existing scheduler request path, which still passes
through the same lock and Quote checks. Direct CLI chunk advancement is allowed
only in isolated tests or an explicitly offline database and is not a
production runbook path.

Structure publication and Quote admission remain higher priority. A delayed
comparison leaves read mode unchanged; it must not degrade or pause quote
collection. Scheduler integration is required in this implementation, not a
future optional follow-up.

Every resume and the sealing transaction revalidates:

- legacy snapshot ID, taken/finished timestamps, count, and existing exact
  receipt identity;
- generation snapshot and publication IDs;
- current pointer snapshot/publication/validation/receipt digest;
- frozen source window ID, `published` status, exact
  `published_snapshot_id`, counts, and source hashes;
- normalization contract version and generation validation/certification hash.

If the current legacy identity or generation pointer changes, existing progress
and any prior receipt are stale. They are retained as audit evidence but cannot
resume or authorize a switch; a new comparison identity is required.

### 5.2 Row-chain SHA-256 v2

The production-shaped v1 profile showed that the pure-Python serializable
SHA-256 compressor, not SQLite or commit latency, dominates comparison time.
The comparison therefore uses the exact algorithm identifier
`row-chain-sha256-v2` for every newly initialized drift comparison. This is an
algorithm-version change, not a transparent implementation substitution.

Let `H` be standard SHA-256 provided by Python's C-backed `hashlib`. Let `U16`
and `U64` encode unsigned integers in big-endian order. Let:

```text
PREFIX = UTF8("polyarb.structure-drift.row-chain-sha256-v2") || 0x00
FRAME(operation, domain) =
    PREFIX || U16(len(UTF8(operation))) || UTF8(operation)
           || U16(len(UTF8(domain))) || UTF8(domain)
CANONICAL(row) = UTF8(JSON(row, sort_keys=true, ensure_ascii=false,
                           allow_nan=false, separators=(",", ":")))
```

`operation` and `domain` are ASCII strings, so byte length and character count
are identical. No operation or domain may exceed 65,535 bytes. Each stream is
initialized, advanced, and finalized as follows:

```text
state_0 = H(FRAME("init", domain))
leaf_i  = H(FRAME("leaf", domain) || U64(len(CANONICAL(row_i)))
                                  || CANONICAL(row_i))
state_i = H(FRAME("chain", domain) || state_(i-1) || leaf_i)
root_n  = H(FRAME("root", domain) || U64(n) || state_n)
```

The empty root is therefore exactly
`H(FRAME("root", domain) || U64(0) || H(FRAME("init", domain)))`; it is never
an omitted field or the SHA-256 of an empty byte string. Durable state is the
strict JSON object:

```json
{"algorithm":"row-chain-sha256-v2","count":0,"domain":"source-market","state_hex":"<64 lowercase hex>"}
```

The decoder rejects missing or extra keys, a non-v2 algorithm, the wrong
domain, a non-integer or negative count, uppercase/non-hex state, and a state
whose decoded length is not 32 bytes. Resume restores exactly `state_hex` and
`count`; chunk boundaries never enter the transition, so a 500-row stream has
the same root under partitions `500`, `1+499`, or any other ordered partition.

The complete fixed domain registry is:

```text
source-event
source-market
source-group-truth
projection-member
generation-member
generation-group-truth
source-identity
legacy-reconstruction
generation-reconstruction
class/shared
class/fresh-addition
class/current-nontradable
class/event-only-quarantine
class/market-side-quarantine
class/fresh-source-absent
class/overlap-conflict
class/unclassified
```

Reconstruction commitments remain SHA-256 commitments over ordered class tag,
count, and the corresponding finalized v2 class root. Their framing is also
prefixed with `PREFIX` and domain-separated as `legacy-reconstruction` and
`generation-reconstruction`. `source_identity_hash` is recomputed with domain
`source-identity` over the authenticated source event count/root and source
market count/root. No v1 root may be copied into a v2 receipt.

`hash_algorithm` is a first-class column on progress and receipt rows and is
included in the comparison identity, `comparison_id`, source-identity
commitment, reconstruction commitments, and final receipt digest. The schema
migration labels existing rows `serializable-sha256-v1` and rebuilds the
progress and receipt identity uniqueness constraints to include
`hash_algorithm`. Progress also gains nullable `terminal_reason`; the migration
labels pre-existing terminal rows `legacy-terminal-reason-unspecified`, while
new algorithm supersession uses the exact reason below.

When v2 initialization finds an active v1 progress row for the same immutable
identities, one `BEGIN IMMEDIATE` transaction must:

1. revalidate the exact legacy, pointer, publication, and source-window
   identities;
2. CAS the v1 row to `phase='stale'` with exact reason
   `drift-hash-algorithm-superseded`;
3. insert a distinct v2 progress row at `source-events`, cursor `NULL`, all
   counts zero, and every v2 stream at its domain-specific empty state;
4. commit both changes together.

A crash cannot leave the v1 row stale without the v2 restart row, or create a
v2 row while v1 still owns active progress. Existing sealed v1 receipts and
progress remain immutable audit evidence, but `drift-safe-sealed`
authorization accepts only `row-chain-sha256-v2`. The unchanged exact receipt
path remains independently authorized. This migration does not mutate the
generation pointer, exact receipt, publication, source rows, legacy serving
tables, read mode, or any data-plane row.

### 5.3 Member scan indexes

The generation and legacy member keyset queries add covering scan indexes with
the common ordered prefix:

```text
(snapshot_id, market_id, event_id, neg_risk_market_id,
 member_kind, active, closed)
```

The `after_market_id IS NULL` and non-null cases use separate SQL statements.
The resumed form uses `snapshot_id=? AND market_id>?`, never
`(? IS NULL OR market_id>?)`, so SQLite can perform an ordered range scan and
stop after the row limit without a full-snapshot scan or a temporary sort.
Market and group-truth joins continue to authenticate the complete structural
predicate; the index changes access only, not eligibility.

Indexes and their targeted `ANALYZE` statistics are created idempotently by
`init_schema()` before the scheduler and Quote producers start. SQLite
`executescript()` may commit an index before the later authority-table rebuild
savepoint, so cross-index/table atomicity is neither claimed nor required. The
small progress/receipt table rebuild itself is one rollback-safe savepoint and
never mutates business data. If index creation, table migration, or `ANALYZE`
fails, startup fails closed; a harmless completed index may remain, and the
next `init_schema()` deterministically completes the missing steps without row
loss. The migration never drops an existing production index and does not run
online inside a drift slice.

## 6. Receipt Contract

Add an append-only drift-safe receipt, versioned independently from the existing
exact receipt. Its authenticated payload includes at least:

- receipt version, `hash_algorithm`, and creation time;
- legacy snapshot ID, timestamps, count, universe/source-truth hashes;
- generation snapshot ID and publication ID;
- source window ID, `published` lifecycle identity, exact
  `published_snapshot_id`, source counts, and source hashes;
- normalization contract version;
- generation validation hash, certification hash, and exact comparison receipt
  digest;
- current pointer validation and comparison digest observed at seal time;
- expected same-window projection universe/group-truth hashes;
- generation universe/group-truth hashes;
- shared count/digest;
- fresh-addition count/digest;
- each mutually exclusive legacy-removal reason count/digest;
- overlap-conflict and unclassified counts/digests;
- legacy and generation tagged reconstruction roots;
- final receipt digest over the canonical encoding of every field above.

The table is append-only: update and delete are rejected. Receipt lookup is by
the full comparison identity, not merely generation snapshot ID, so a later
legacy identity cannot accidentally reuse an earlier authorization.

## 7. Gate and CLI Semantics

The current `structure-generation-compare` command and its exact-match exit
semantics remain unchanged for compatibility and audit clarity.

The separate read-only Makefile-backed command
`make structure-generation-drift-compare`:

- reads only the bounded drift-safe progress/receipt and current authorization;
- prints exact identities, class counts, reason counts, projection equality,
  overlap-conflict/unclassified counts, receipt digest, and authorization mode;
- exits zero only for `exact` or `drift-safe-sealed` authorization on the
  current identities;
- exits non-zero for incomplete progress, stale identity, missing evidence,
  projection mismatch, conflict, unclassified drift, or receipt failure.

The operational read-switch preflight accepts a sealed authorization result for
the exact current identities only. It does not infer permission from a count
delta, snapshot age, authenticated-but-failing exact receipt, or a previously
sealed receipt for a stale legacy/pointer identity.

The generation reader continues to resolve one immutable pointer identity in a
single transaction. It does not scan source or comparison rows on hot paths.

## 8. Failure and Recovery

All anomalies fail closed while preserving production operation in legacy read
mode:

- incomplete or mutable source window;
- stale legacy or generation identity;
- normalizer/contract mismatch;
- source-projection versus generation mismatch;
- fresh group-truth mismatch;
- shared structural mutation;
- missing, ambiguous, or multiply classified delta evidence;
- non-zero overlap-conflict or unclassified counts;
- malformed progress, CAS loss, receipt digest mismatch, or receipt mutation.

Failure records a bounded reason and checkpoint for alerting and retry. It does
not repeatedly downgrade a healthy data plane, move the pointer, delete
evidence, or rewrite either snapshot. Recovery starts a fresh comparison only
after the responsible immutable identity or code contract changes.

## 9. Verification Requirements

Implementation is not complete until tests prove:

1. exact comparison behavior and CLI output are unchanged;
2. the expected projector uses frozen staging and legacy normalizers, never
   generation rows as expected values or live network calls;
3. same-window source projection equals an independently constructed generation
   fixture byte-for-byte;
4. every shared structural field mutation fails, including token, event, group,
   member kind, state, condition, and incomplete changes;
5. fresh additions require complete source plus generation certification;
6. every legacy-only reason succeeds only with exact evidence and reason
   partitions are mutually exclusive;
7. source-present active generation omissions and all ambiguous cases remain
   unclassified and fail;
8. event-only quarantine recomputes membership and fresh group truth exactly;
9. count and tagged-digest reconstruction covers both universes with no gaps or
   duplicates;
10. progress resumes across every phase/chunk boundary and rejects cursor,
    digest, identity, source-window, pointer, and contract drift;
11. receipt authentication is append-only and rejects field substitution,
    update, delete, and stale-identity reuse;
12. each chunk respects the 500-row bound, uses constant-count bulk queries,
    is advanced only under scheduler producer ownership, and yields to Quote
    admission/deadline pressure;
13. the real 845/848 evidence shape can seal only when every row in the full
    symmetric difference—all additions plus all removals—is individually
    covered by the exact partitions; the 6,048 net count difference is not the
    number of rows that require proof and is never used as a tolerance;
14. no test or command changes the production pointer, legacy serving tables,
    deployment state, or configured read mode.
15. the v2 row-chain matches the exact framing above, has the specified empty
    root for every domain, and produces one root for every tested partition of
    the same ordered rows;
16. changing any canonical field, row order, row count, duplicate, domain,
    algorithm, state byte, or persisted count changes the root or fails decode;
17. v1 active progress is atomically marked stale with
    `drift-hash-algorithm-superseded` and restarted at v2 cursor zero, while a
    sealed v1 receipt remains queryable but cannot authorize the v2 gate;
18. progress, comparison ID, source identity, reconstruction roots, and receipt
    digest all bind `hash_algorithm`, and cross-version field substitution
    fails closed;
19. `EXPLAIN QUERY PLAN` for both resumed member scans uses the new
    `(snapshot_id,market_id,...)` index without `USE TEMP B-TREE FOR ORDER BY`,
    and a 120,000-row production-shaped regression proves at least 2x median
    improvement for both generation and legacy scans;
20. a production-shaped benchmark records source-event, source-market, and
    member-root v1/v2 timings and must retain at least 2x estimated complete
    gate speedup before deployment is considered.

## 9.1 Performance evidence for the v2 amendment

The read-only local profile used 120,000 markets, 5,000 events, 24 members per
event, production-shaped raw payloads, the production schema, and 500-row
member/market chunks. It found:

- source-market SQL: 3-10 ms per 500 rows; complete warm v1 chunk about 350 ms;
- source-market root work: 336.2 ms v1 versus 0.477 ms v2 (705x);
- source-event root work for twenty approximately 10 KiB events: 329.8 ms v1
  versus 0.100 ms v2 (3,298x);
- three 500-member roots: 234.2 ms v1 versus 1.246 ms v2 (188x);
- generation member complete chunk: about 454 ms v1, of which fresh evidence
  and SQL remain about 220 ms, yielding an estimated 2.05x v2 chunk speedup;
- legacy member scan: 66-117 ms before the index and 5-15 ms after index build
  plus `ANALYZE`, with the temporary order sort removed.

Production attempt evidence at 17,000 of 122,101 source markets showed 2,500-
3,000 rows per 50-60 second slice and approximately 60 seconds between slices.
At that measured cadence, v1 had approximately 73 minutes of source-market wall
time and a conservative 2.4-2.7 hours for the complete remaining gate. The v2
row-chain plus member indexes estimates 55-65 minutes for the same remaining
work, or approximately 2.3-2.7x wall-clock speedup. These estimates are
capacity planning evidence, not an authorization shortcut; full correctness
and production verification gates remain mandatory.

## 10. Rejected Alternatives

### Refresh the legacy serving snapshot and run exact comparison

This would reuse the old gate but risks changing the latest legacy identity and
data plane. The current architecture has no isolated unpublished legacy serving
namespace. It also obscures intentional quarantine contract changes. Rejected.

### Materialize a projection by copying generation rows

This is tautological and cannot detect normalization defects. Rejected. The
expected projection must originate from pinned raw payloads through independent
normalization.

### Age, count, percentage, or hash-mismatch tolerance

These rules cannot detect an unexpected mutation hidden among legitimate fresh
rows and cannot causally explain removals. Rejected as a bypass rather than a
production-safe gate.
