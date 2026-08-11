# M1 Transactional Cloud Control Plane Design

## Purpose

M1 must continue discovering and tracking Polymarket opportunities while
individual API requests, workers, machines, deployments, databases, alerts,
and network links fail temporarily. Correctness is required at the transaction
boundary, not as an unrealistic requirement that every node and every run be
simultaneously healthy.

The current deployment does not meet that standard. Structure, Quote, SQLite
writes, health projection, incident evidence, and the operator console share
one stateful Fly machine and one volume. A slow publication query can consume
the producer lane, delay Quote, strand health reads, generate repeated P1
incidents, and make the diagnostic surface unavailable at the same time.

## Chosen architecture

Adopt a transactional control plane with gradual migration:

- Supabase Postgres is the durable authority for jobs, leases, attempts,
  checkpoints, publication pointers, incidents, notification outbox entries,
  and delivery receipts.
- Fly machines are replaceable workers. They claim bounded jobs, heartbeat,
  commit idempotent transaction units, and may disappear at any point.
- R2 stores immutable large snapshot artifacts and manifests. Postgres stores
  their authenticated identity and current pointer.
- Local SQLite is a disposable performance cache and temporary staging area.
  Losing or filling the Fly volume must not lose the production control plane.
- Dashboard and alerting read the durable control plane rather than requiring
  the data-plane worker and its volume to be responsive.

This is a strangler migration. The existing M1 pipeline remains active until
each new path has passed shadow comparison and a reversible pointer switch.
There is no stop-the-world rewrite.

## Transaction model

Every executable unit has the following durable identity:

```text
job_key             stable idempotency key for one logical unit
job_type            structure-fetch | normalize | certify | quote | project
input_identity      source cursor/generation/universe digest
lease_owner         current worker boot identity
lease_epoch         monotonically increasing fencing token
lease_expires_at    automatic takeover boundary
checkpoint          last committed cursor and authenticated digest
attempt_id          immutable execution attempt record
outcome             running | checkpointed | succeeded | retryable | quarantined
next_attempt_at     bounded retry schedule with jitter
```

A worker may commit only while its lease epoch is current. Each transaction
uses `(job_key, input_identity, checkpoint_cursor)` as its idempotency key.
Replaying a committed unit authenticates and returns its existing receipt;
replaying an uncommitted unit starts from the preceding checkpoint.

At-least-once execution plus idempotent commit is the delivery model. The
system does not claim exactly-once process execution; it provides exactly-once
durable effects at transaction boundaries.

## Data plane boundaries

### Structure fetch

One job fetches one bounded Gamma page. It commits raw source rows, the next
cursor, source response digest, request timing, and coverage evidence in one
transaction. A failed request commits no cursor. Cursor rejection rotates a
new source window without deleting the prior authenticated window.

### Normalization

One job normalizes a bounded raw key range. Its input is a frozen source-window
identity. Canonical rows and the next source cursor commit atomically. Invalid
individual records enter a typed quarantine with raw payload digest and reason;
they do not abort unrelated rows or components.

### Certification

One job certifies a bounded canonical key range and extends an authenticated
row-chain receipt. Contract violations quarantine the affected generation and
preserve the current published pointer. Temporary storage failures retry the
same cursor.

### Publication

Publication is the only global transaction. It verifies terminal source,
normalization, certification, and artifact receipts, then atomically changes
the current Structure pointer. An incomplete generation is never visible as
current truth. The prior generation remains readable and rollback-capable.

### Quote and opportunity feed

Quote runs consume an immutable Structure identity. Collection is split into
bounded token batches with independent receipts. A terminal Quote generation
publishes only when required coverage and freshness gates pass. Failed batches
retry independently; they cannot block Structure or health workers. Every
gross candidate and its lifecycle transitions are durable even when the final
count is zero.

## Failure classification

| Class | Examples | Durable action | Service effect |
|---|---|---|---|
| transient | timeout, DNS, 429, 5xx, writer contention | checkpoint, backoff+jitter, retry | old certified truth remains live |
| worker loss | crash, OOM, rolling deploy, machine stop | lease expiry, fenced takeover | no manual unlock |
| bad record | malformed payload, isolated identity mismatch | quarantine record and continue | affected shard excluded visibly |
| contract failure | generation count/hash mismatch | quarantine generation, retain pointer | P1 only if current truth breaches SLA |
| dependency outage | Gamma/CLOB/Supabase unavailable | circuit open, bounded probes, incident | unaffected workers continue |
| capacity pressure | disk/memory/connection saturation | shed cache, pause low priority, scale worker | control plane remains readable |

Retries are bounded per attempt but not permanently disabled. The scheduler
uses exponential backoff with jitter and a maximum probe interval. Repeated
failure opens a scoped circuit and creates/reminds an incident; a successful
probe closes the circuit and records recovery evidence.

## Control-plane records

The first implementation slice introduces additive tables:

- `m1_jobs`: current desired work and checkpoint pointer.
- `m1_job_attempts`: immutable start/finish/failure evidence.
- `m1_job_leases`: fenced ownership and heartbeat.
- `m1_checkpoint_receipts`: authenticated transaction outputs.
- `m1_generation_manifests`: Structure/Quote artifact identities and state.
- `m1_publication_pointers`: current and rollback identities.
- `m1_incidents` and `m1_incident_events`: complete lifecycle history.
- `m1_alert_outbox`: notification intent created in the same transaction as
  the incident event.
- `m1_alert_deliveries`: channel attempt, response classification, retry, and
  terminal delivery receipt.

Rows are append-only where evidence matters. Mutable singleton/pointer rows
carry a monotonic version and are updated with compare-and-swap semantics.

## Worker supervision

Workers are separated by process group and resource pool:

- `control-api`: health, dashboard read API, job administration.
- `structure-worker`: fetch, normalize, certify, publish jobs.
- `quote-worker`: CLOB batch collection and Quote publication.
- `alert-worker`: outbox delivery and reminder scheduling.

No worker shares an in-process lock with another class. Autoscaling or machine
replacement cannot transfer correctness; correctness lives in fenced leases
and durable commits. Each worker exposes only process liveness locally. Product
health is derived from control-plane receipts and SLAs.

## Dashboard and alerting

The operator surface remains available when data workers are down. It shows:

- current certified Structure and Quote identities and ages;
- every open incident with affected scope, first/last occurrence, retry count,
  current checkpoint, next retry, automatic action, and operator action;
- job backlog, oldest runnable age, leased/stalled/quarantined counts;
- alert outbox and per-channel delivery receipts;
- recovery history and the evidence that closed an incident;
- capacity and dependency circuit state.

Alert creation is transactional with incident mutation. Delivery is a separate
retryable job, so Telegram or Better Stack failure cannot erase the alert
intent. Dashboard visibility never depends on successful notification delivery.

## Availability and correctness rules

- Last-known certified truth remains serviceable during transient failures and
  is labeled with its exact age and source identity.
- Degraded means a specific capability is unavailable; it never silently turns
  missing or stale data into an empty successful result.
- A global P1 requires loss/staleness of the currently serviceable product or
  corruption of its authority. A retrying non-current shard is warning/P2.
- A worker or machine restart must be recoverable without operator input.
- A poisoned shard cannot starve unrelated runnable jobs.
- Health and Dashboard have independent control-plane read budgets and do not
  query the large data-plane tables synchronously.

## Migration plan

1. **Control-plane foundation:** add schema, lease/attempt/checkpoint library,
   outbox, strict read API, and shadow job writer. Existing workers continue.
2. **Structure publication:** shadow existing checkpoints into jobs, run new
   normalize/certify workers, compare manifests, then atomically switch the
   Structure pointer. Rollback is pointer-only.
3. **Quote isolation:** move Quote batches and terminal publication to their own
   worker pool, consuming the new Structure pointer.
4. **Operator plane:** switch Dashboard and alerts to durable incidents/outbox;
   retain old endpoints as comparison probes until parity passes.
5. **Cache demotion:** remove production authority from SQLite. Volume loss and
   recreation become a chaos-tested cache recovery path.

Every stage has a dual-read/shadow comparison period and an explicit rollback
pointer. No later stage is required to validate an earlier independent stage.

## Verification and production acceptance

Automated verification covers lease fencing, crash at every commit boundary,
duplicate delivery, stale lease writers, cursor replay, poison quarantine,
circuit recovery, outbox delivery retry, pointer rollback, and control-plane
read availability under worker loss.

Chaos verification kills workers during fetch, normalization, certification,
publication, Quote collection, and notification delivery. It also stops a Fly
machine, fills/removes the disposable cache volume, injects upstream timeout/
429/5xx, and temporarily blocks Supabase/R2/Telegram independently.

M1 is production-accepted only when live evidence proves:

- Structure and Quote continue publishing across worker replacement;
- no committed transaction is duplicated or lost;
- old certified truth remains available during injected failures;
- stalled leases are reclaimed within their SLA;
- incidents, automatic actions, retries, and delivery receipts remain visible;
- Dashboard remains readable while a data worker is stopped;
- opportunity candidates and lifecycle history feed M2 from current certified
  identities;
- a sustained soak has no silent stop, permanent degradation, or manual unlock.

## Non-goals

- This phase does not execute trades or change M2 strategy semantics.
- It does not require Kubernetes or a new workflow framework.
- It does not promise every upstream response is valid or every run succeeds.
- It does not delete the current SQLite path until shadow parity and rollback
  evidence are complete.
