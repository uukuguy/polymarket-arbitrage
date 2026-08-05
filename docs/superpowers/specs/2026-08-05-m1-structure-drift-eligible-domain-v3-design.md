# M1 Structure Drift Eligible-Domain Classifier v3

**Date:** 2026-08-05

**Status:** Approved design — implementation not started

**Scope:** Correct the classifier-v2 fresh-projection candidate domain without
weakening fail-closed authorization. This design does not switch Structure read
mode, enable Quote, mutate a published generation, or retry a stale v2 identity.

## 1. Production Problem and Evidence

Release `13e29c2dddf3a1665f9f5f294dee1446dd7f81b4` deployed as Fly release 238.
The bounded checkpoint scheduler recovered correctly: attempt 306 completed and
attempt 307 began 199 ms later. The current classifier-v2 comparison then
finished deterministically as `drift-unclassified`, so generation read and Quote
remain safely disabled.

The failure is a candidate-domain defect, not 125,158 corrupt market records.
The fresh-projection phase deliberately scans the complete frozen union of raw
market rows and event-only sidecar members, but v2 gives every row only two
outcomes: eligible member or unresolved diagnostic. It therefore treats records
that are provably outside the supported standard neg-risk universe as errors.

Read-only production queries against generation 874, publication
`e10d2b544e9b415fab3a71c2a53e8c53`, established the complete partition:

| Outcome | Reason | Count |
|---|---|---:|
| eligible | standard, complete-supported structural member | 41,768 |
| expected exclusion | ordinary `negRisk=false` market | 82,346 |
| expected exclusion | exact market-side quarantine | 193 |
| expected exclusion | augmented group member | 11,069 |
| expected exclusion | standard group ineligible because a member is non-tradable | 312 |
| expected exclusion | ordinary non-neg-risk event-only member | 13,655 |
| expected exclusion | closed or inactive event-only member | 17,515 |
| expected exclusion | exact event-only quarantine | 68 |
| **total** | complete market + event-only candidate union | **166,926** |

The counts are independently cross-checked:

- `invalid-event-membership=82,539` equals 82,346 ordinary markets plus all 193
  exact market quarantine rows; diagnostic priority currently masks quarantine.
- `augmented-group=11,069` equals every member joined to augmented group truth.
- the 31,238 event-only rows split exactly into 13,655 ordinary-event members,
  17,515 non-tradable members, and 68 exact quarantine receipts.
- the 312 approved standard group-ineligible rows split into 168 legacy rows
  already classified as `fresh-group-ineligible` and 144 generation-only rows
  currently reported as `generation-addition-other`.
- all 261 generation issues are authenticated exclusions: 149 orphan markets,
  44 neg-risk markets missing group identity, and 68 event-only members.

Thus:

```text
166,926 candidates
= 41,768 eligible
+ 125,158 authenticated exclusions
+ 0 unresolved
```

This equality is evidence for the design, not an authorization shortcut. The
implementation must recompute the partition from the pinned source and fail if
even one candidate has no unique outcome.

## 2. Decision and Rejected Alternatives

Introduce `structure-drift-classifier-v3`. Every fresh candidate receives
exactly one of three authenticated outcomes:

1. `eligible` — contributes to the independent projection member count/root;
2. `expected-exclusion` — contributes to a reason-specific count/root;
3. `unresolved` — contributes to the existing diagnostic count/root and makes
   the comparison stale.

The candidate total must equal the sum of those three outcome counts. Eligible
projection must still exactly equal the generation eligible universe. Expected
exclusion is not authorization to serve a row; it is proof that a scanned source
row is intentionally outside the supported universe for one frozen reason.

Rejected alternatives:

- **Patch v2 predicates in place.** A stale v2 identity will not retry, and the
  unchanged contract name would make historical receipts semantically
  ambiguous. Silent filtering also loses proof of what was excluded.
- **Filter staging before comparison.** This destroys the independent complete
  source scan and can hide event-only or cross-catalogue defects.
- **Treat every exclusion as a diagnostic warning.** Authorization currently
  and correctly requires zero unresolved diagnostics. Allowing non-zero
  diagnostics would weaken fail-closed semantics globally.

## 3. Canonical Outcome Model

Add a frozen `FreshProjectionExclusion` value containing:

- one reason from the closed taxonomy below;
- the candidate stream (`market` or `event-only`);
- the nullable canonical candidate envelope already used by diagnostics;
- source ordinal/member ordinal when present;
- raw market/event hashes;
- normalized group-truth identity, quality, reason, and membership hash when
  group truth participates in the decision.

The v3 exclusion reasons are:

```text
non-neg-risk-market
market-side-quarantine
non-neg-risk-event-member
current-nontradable-event-member
augmented-group
fresh-group-ineligible
event-only-quarantine
```

Names are exact wire values. No catch-all exclusion exists. Unknown group type,
unknown unsupported reason, malformed booleans, mismatched identity, duplicate
identity, global conflict, missing required quarantine, and incomplete source
continue through the existing ordered diagnostic table.

`FreshProjectionChunk` carries `members`, `exclusions`, `diagnostics`, and
`candidates_processed`. Its constructor/advance path enforces:

```text
len(members) + len(exclusions) + len(diagnostics) == candidates_processed
```

Each source candidate is processed once, and the implementation must reject a
chunk that places the same canonical candidate in more than one outcome.

## 4. Ordered Classification Rules

Ordering is part of the contract. A later expected exclusion cannot mask an
earlier structural error unless an exact quarantine receipt explicitly proves
that exception.

### 4.1 Market stream

For each raw market candidate:

1. A non-boolean `negRisk` is `invalid-neg-risk-classification`.
2. `negRisk is False` is `non-neg-risk-market`. It does not require an event or
   neg-risk group identity because those fields are outside this strategy's
   domain.
3. For `negRisk is True`, recompute exact market quarantine before requiring
   event membership. A matching immutable issue becomes
   `market-side-quarantine`; a forged/missing issue remains unresolved.
4. Revalidate unique market/event/member/group identity and global relation
   cardinality. Any mismatch, duplicate, or conflict remains diagnostic.
5. A structurally valid augmented group becomes `augmented-group`.
6. A structurally valid standard `complete-unsupported` group is
   `fresh-group-ineligible` only for exact reason
   `standard-neg-risk-has-non-tradable-members`.
7. A structurally valid standard `complete-supported` active named member with
   a complete 11-field market identity becomes `eligible`.
8. Every other state is unresolved under the existing diagnostic precedence.

### 4.2 Event-only stream

For each sealed event-member sidecar candidate absent from market staging:

1. Revalidate the raw event classification. A non-boolean or contradictory
   neg-risk classification remains diagnostic.
2. A member of an explicitly ordinary event (`negRisk is False` and
   `enableNegRisk is False`) becomes `non-neg-risk-event-member`; absence of a
   neg-risk group ID is expected in this branch.
3. Required member identity fields and `active`/`closed` booleans are validated
   before any tradability exclusion.
4. An inactive or closed member becomes
   `current-nontradable-event-member`.
5. A structurally valid augmented member becomes `augmented-group`; a standard
   member in the one approved non-tradable group state becomes
   `fresh-group-ineligible`.
6. An active/open standard member absent from market staging becomes
   `event-only-quarantine` only when the exact immutable quarantine envelope
   recomputes and the generation contains neither its market nor membership.
7. Missing, forged, duplicate, or conflicting quarantine evidence remains
   unresolved. No event-only row is fabricated into the eligible projection.

These rules intentionally classify ordinary and non-tradable source rows before
asking for a quarantine that is meaningful only for active/open neg-risk rows.

## 5. Commitments, Persistence, and Receipts

Keep the existing domain-separated member and diagnostic chains. Add one
row-chain per exclusion reason:

```text
projection-exclusion/<reason>
```

Progress persists:

- `projection_candidate_count`;
- `projection_member_count` and member digest state/root;
- `projection_exclusion_count`;
- canonical exclusion counts JSON and digest-state/root JSON keyed by the
  closed reason taxonomy;
- `projection_diagnostic_count` and diagnostic digest state/root.

The finalizer independently recomputes the expected frozen candidate count:

```text
market staging row count
+ event-member sidecar anti-join row count
```

It authorizes only when all of the following hold:

```text
candidate_count == member_count + exclusion_count + diagnostic_count
candidate_count == frozen market count + frozen event-only anti-join count
diagnostic_count == 0
projection member count == generation member count == generation mirror count
projection member root == generation mirror root
all existing source, sidecar, group-truth, certification, and exact-receipt checks pass
```

Add dedicated exclusion-count and exclusion-root JSON fields to v3 progress,
sealed receipts, and terminal receipts. Receipt digests bind them. Do not reuse
the member comparison class fields: legacy/generation reconstruction and fresh
candidate exclusion are different proof domains.

Status validation recomputes all count relationships, verifies every row-chain
root and the v3 receipt digest, and rejects unknown exclusion keys. `/health` and
`/healthz` expose bounded exclusion counts and roots for operations, but an
exclusion is never emitted as an alert by itself. Any unresolved diagnostic or
receipt mismatch remains a failing Structure drift subcheck delivered through
the resident Polywatch path.

## 6. Contract Supersession and Recovery

`structure-drift-classifier-v3` participates in the existing deterministic
comparison identity. On first scheduler execution after deployment:

1. revalidate the current pointer/publication/window/source identity;
2. leave the terminal v2 row and receipt immutable as audit evidence;
3. create a v3 comparison for the same frozen identity;
4. checkpoint through the existing bounded follow-up mechanism;
5. seal or stale once, without same-contract retry storms.

Only a sealed v3 receipt may authorize the current drift gate. v1/v2 receipts
remain queryable but cannot authorize. Startup or continuation failure keeps the
current legacy read mode and Quote state unchanged and is handled by the
existing retry/alert path.

No migration may rewrite a historical receipt. New nullable storage columns are
added transactionally and are mandatory for v3 rows. Older rows validate under
their original contract schema.

## 7. Test and Verification Contract

Implementation follows red-green-refactor. Tests must first fail against v2 and
cover real production shapes rather than only one synthetic standard event.

Required unit/property coverage:

- every reason in the closed exclusion taxonomy;
- malformed lookalikes remain diagnostics;
- exact quarantine wins over the otherwise invalid parent/member relationship;
- forged, missing, or cross-snapshot quarantine never excludes;
- ordinary event members do not require neg-risk group identity;
- inactive/closed neg-risk event-only members do not require quarantine;
- augmented and the one approved standard group-ineligible reason exclude;
- unknown unsupported reasons remain diagnostics;
- every candidate has exactly one outcome;
- chunk limits 1, 17, and 500 produce identical counts and roots;
- market-to-event-only cursor restart preserves the exact partition;
- exclusion count/root tampering invalidates sealed and terminal status;
- v2 terminal evidence remains immutable and v3 supersedes without retrying v2.

Required end-to-end fixture:

```text
82,346 non-neg-risk markets
193 exact market quarantines
11,069 augmented members
312 approved group-ineligible members
13,655 ordinary event-only members
17,515 non-tradable event-only members
68 exact event-only quarantines
41,768 eligible members
```

The test may generate these rows compactly, but its asserted aggregate partition
must equal the production evidence exactly. A second fixture introduces one
unknown candidate and must terminate `drift-unclassified` with exactly one
diagnostic.

Verification before deployment includes targeted projection/classifier tests,
the full M1 test suite, `make planning-status`, and the existing lint/type gates.

## 8. Production Rollout and Acceptance

Deployment uses an exact approved Git SHA and preserves:

```text
POLYARB_STRUCTURE_GENERATION_DRIFT_COMPARE_ENABLED=true
POLYARB_STRUCTURE_GENERATION_READ_MODE=legacy
POLYARB_ENABLE_NEG_RISK_QUOTE=false
```

Production acceptance requires read-only evidence that:

1. the exact release SHA is running on healthy app and cron machines;
2. a v3 comparison starts for the current frozen identity without modifying the
   v2 terminal receipt;
3. bounded checkpoints continue immediately and do not busy-loop after a
   terminal result;
4. candidate conservation and all exclusion counts/roots authenticate;
5. diagnostics are zero and the v3 comparison seals `drift-safe-sealed`;
6. `/health` and `/healthz` authenticate the same v3 receipt;
7. legacy read mode and Quote remain unchanged until a separate explicit
   cutover approval.

Only after those gates pass may the project propose generation-read cutover and
then Quote enablement. This classifier deployment alone is not M1 production
completion.

## 9. Out of Scope

- support for augmented-group arbitrage;
- changing the eligible standard neg-risk strategy universe;
- deleting or rewriting v1/v2 audit evidence;
- weakening any exact quarantine, source identity, conflict, or receipt check;
- enabling generation reads or Quote in the classifier deployment;
- unrelated startup performance or database-retention changes.
