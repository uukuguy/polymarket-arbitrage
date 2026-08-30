# M1 Recurring Transactional Quote Runs Design

**Date:** 2026-08-31  
**Status:** approved by continuous autonomous M1 authorization  
**Scope:** Phase 05.6 production freshness closure

## Production contradiction

The transactional source traverses Gamma `/events/keyset` sequentially because the next
opaque cursor is known only after the current page. Production currently needs roughly
35 minutes for about 200–225 capped 100-event pages. Non-overlapping source windows are
therefore a correct recovery boundary but cannot be a 900-second executable-price clock.

The Quote generation key is currently `quote:<structure_digest>`. That gives one immutable
Quote and one Opportunity projection to each Structure generation. Once those products age
past 900 seconds, no new Quote can be admitted until the next full Structure window publishes.
The qualification failure is therefore deterministic architecture, not a timeout incident.

## Decision

Restore the already-established two-clock product model in the transactional runtime:

| Product | Authority | Clock |
|---|---|---|
| known-universe Structure | complete certified Gamma traversal | independent universe coverage clock |
| executable Quote | atomic complete CLOB run over one certified Structure | 300-second admission cadence, 900-second hard qualification bound |
| Opportunity | one immutable certified Quote run | follows every Quote publication |

One Structure may parent many immutable Quote runs. A Quote run never changes membership,
never mixes Structure generations, and never refreshes its timestamp in place.

## Identity and lineage

Quote generation keys retain the existing `quote:<64 lowercase hex>` external shape. The
64-byte digest becomes a quote-run digest computed from canonical JSON containing:

- policy version `transactional-quote-run-v1`;
- exact `structure_generation_key`;
- exact `universe_hash`;
- `cadence_seconds=300`; and
- the UTC cadence bucket.

The initial Quote created directly after Structure certification may retain its legacy digest
for rolling compatibility. A new `m1_quote_generation_inputs` relation is authoritative for
all lineage:

```text
quote generation -> structure generation -> universe hash -> cadence bucket/admitted_at
```

Migration 038 backfills legacy `quote:<structure_digest>` manifests and rejects any row that
cannot map to the exact certified Structure manifest. New readers never infer Structure by
substring after the migration gate.

## Durable admission

A lightweight cadence admitter runs in the coordinator, but admission itself is a normal
durable `quote-admit` job. Under one shared advisory transaction lock it:

1. reads `quote:current` and its authoritative lineage;
2. refuses admission while any quote-admit, quote-batch, quote-certify, or
   opportunity-certify successor is unfinished;
3. creates at most one deterministic quote-admit job for the current cadence bucket;
4. points that job at the same immutable Structure bundle; and
5. records the intended quote generation identity before a worker may claim it.

Structure certification takes the same advisory lock while creating its immediate quote-admit
successor. A refresh of old Structure and the first Quote of new Structure therefore cannot
race into avoidable pointer conflicts.

The existing quote-admit worker remains the lifecycle authority. It rereads certified R2
Structure truth, freezes exact batch artifacts, and creates run-scoped quote-batch jobs. Every
existing lease, heartbeat, retry, circuit, alert and fault-matrix rule continues to apply.

## Publication and freshness chain-truth

Quote certification performs the existing expected-predecessor CAS, writes one immutable
manifest and moves `quote:current`. It then enqueues one Opportunity projection whose identity
is that Quote run. Previous complete generations remain immutable history.

Structure freshness and Opportunity input lookup join through
`m1_quote_generation_inputs`; neither derives Structure by parsing the Quote digest. Quote
freshness remains its manifest publication age, and Opportunity freshness remains projection
certification age. A missing or conflicting lineage row fails closed.

Universe coverage is not relabeled as executable freshness. The qualification/operator model
must expose Structure traversal liveness and last complete universe age separately; selecting
the exact universe SLO is a measured product decision and is not part of this change.

## Interruption and recovery

- Cadence bucket plus advisory lock prevents duplicate admission across coordinator restart.
- Job keys and input identities make committed-but-unacknowledged admission idempotent.
- An interrupted quote-admit/batch/certifier resumes through the existing lease path.
- A newer Structure successor wins only through the same serialized admission boundary.
- No source window overlaps, no timestamp is rewritten, and no stale Quote is called fresh.

## Verification and production gate

The change is acceptable only when tests and exact-image production evidence prove:

1. migration/backfill and least-privilege lineage reads;
2. two different Quote generations from the same Structure;
3. one admission per cadence bucket and no quote-pipeline overlap;
4. Structure/refresh admission race serialization;
5. exact pointer lineage and one Opportunity per Quote run;
6. replay after interruption without duplicate durable effects;
7. same-Structure Quote and Opportunity ages repeatedly return within 900 seconds;
8. ordinary freshness pauses resume the same qualification epoch; and
9. the existing 66 node-level commissioning attack obligations remain valid.

No wallet, signing, order, balance, trade, Colima, or global Docker-context authority is added.
