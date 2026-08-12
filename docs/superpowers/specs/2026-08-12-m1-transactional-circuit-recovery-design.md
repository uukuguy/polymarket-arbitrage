# M1 Transactional Circuit and Recovery Design

## Purpose

The transactional M1 workers currently retry every failure after a fixed delay
and leave their incident open forever. This does not satisfy the control-plane
contract: repeated dependency failure must be bounded, operators must see the
automatic action, and a successful probe must write durable recovery evidence.

## Decision

Each `job_key` owns one scoped circuit. This is deliberately narrower than a
global Gamma/CLOB circuit: a poisoned page or batch cannot pause unrelated
work, while a real upstream outage still becomes visible through the bounded
set of affected open incidents.

The durable policy is fixed for this first production slice:

| Consecutive retryable attempts | Next probe delay | Circuit state |
|---:|---:|---|
| 1 | 15 seconds | closed |
| 2 | 30 seconds | closed |
| 3+ | 60 seconds, doubling to a 5-minute cap | open |

No random jitter is used in the first slice because retry scheduling must be
deterministically replayable from persisted facts. Independent job keys already
avoid a shared retry lock; production tuning may add deterministic seeded jitter
only with separate evidence.

## Durable model

Revision 014 adds `m1_job_circuits`, one mutable row per job key:

```text
job_key             primary key / circuit scope
consecutive_failures durable count since last successful terminal effect
state               closed | open
opened_at           first failure that crossed the threshold
next_probe_at       exact bounded retry time
updated_at          most recent state change
```

`finish_retryable_with_incident` becomes the sole retry failure transition. In
the same transaction it fences the job lease, closes the running attempt,
updates the circuit, writes the retry schedule, and writes an incident event.
Attempts one and two have `attempt-failed`; the threshold transition writes
`circuit-opened`; later failures write `circuit-probe-failed`. The incident is
deduplicated by job key, so one outage stays one operator card while every
lease epoch retains immutable history.

Every successful terminal worker effect calls `record_job_recovery` in its
already-owned transaction. It clears the circuit count, transitions the open
incident to `resolved`, appends `recovered` evidence, and emits dashboard plus
configured Telegram recovery intent. Stale lease writers cannot recover a
newer failure because the same lease fence applies.

## Service effect and severity

This slice records the automatic action (`retry-after-seconds` or
`circuit-open-bounded-probe`) in event detail. It does not globally pause
Structure or Quote. All automatic incident severity remains `warning`: current
truth SLA/P1 promotion requires separately measured pointer age, not the
failure count of an arbitrary non-current shard. The control API exposes the
open circuit count and bounded circuit samples beside incidents/outbox.

## Non-goals

- No global dependency circuit or process-local breaker.
- No automatic scaling, cache shedding, pointer switch, or manual circuit
  reset endpoint.
- No claim that a worker success proves a complete Structure/Quote publication;
  recovery proves only the exact job circuit recovered.
- No modification to legacy SQLite incident authority.

## Verification

Real PostgreSQL contracts must prove: 15/30/60-second transition, five-minute
cap, threshold event/idempotency, no duplicate incident, successful fenced
recovery resolves exactly its incident, and a stale writer cannot recover it.
Worker contracts must prove every successful source/range/certifier/Quote
boundary invokes recovery. The operator snapshot must expose circuit facts.
Live acceptance additionally injects Gamma/CLOB failures, kills workers during
an open circuit, observes lease takeover/probe/recovery/outbox receipts, and
keeps the control API readable.
