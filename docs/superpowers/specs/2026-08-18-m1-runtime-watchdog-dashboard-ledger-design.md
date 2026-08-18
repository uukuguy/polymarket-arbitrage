# M1 Runtime Watchdog Dashboard Ledger Design

## Purpose

The independent M1 runtime watchdog currently alerts Telegram on control API,
machine restart, and durable-progress failures, but it deliberately has no
database credentials.  Operators therefore cannot use the cloud dashboard to
inspect the exact alert and later recovery.  This design makes both transitions
durable without weakening that isolation.

## Chosen design

Deploy a small private `runtime-event-writer` Fly service.  The watchdog holds
only an ingest bearer secret and posts a bounded transition payload.  The writer
holds a dedicated Postgres role that can append only to the existing incident
ledger (`m1_incidents`, `m1_incident_events`); it has no R2, Gamma, CLOB,
scheduler, or public operator-read authority.  The existing read-only control
API remains the only dashboard read source.

The stable incident is `runtime-watchdog`; an unhealthy transition creates or
updates it with severity `critical`, and a healthy transition resolves it and
appends a `recovered` event.  Idempotency keys include a watchdog boot UUID and
transition sequence.  Details are limited to reason codes, monitored machine
states/restart counters, and bounded control-plane counters.  URLs, headers,
tokens, DSNs and arbitrary exception text are excluded.

The API exposes the latest bounded runtime timeline and current runtime
incident in its existing `/perception/control-plane` response.  The authenticated
Next dashboard adds `/control-plane`, with a prominent active red panel and
chronological incident/recovery evidence.  “unavailable” stays visible rather
than becoming an empty/healthy state.

## Failure handling

On every state transition the watchdog first persists the dashboard event,
then sends Telegram.  A failed write is retained for retry on subsequent ticks;
Telegram remains immediate and explicitly says the dashboard record is pending.
The writer's `/healthz` is monitored by the watchdog as an exact target, so loss
of the ledger is itself an alertable fault.

## Acceptance

1. A controlled Fly sampler process exit yields a Telegram incident and one
   matching dashboard event with machine/restart evidence.
2. Its automatic restart yields a linked recovery event with duration.
3. Duplicate retries cannot make duplicate incident transitions.
4. Dashboard or writer outage remains explicit and creates a Telegram warning.
5. The watchdog has no Postgres/R2 credentials; writer role cannot access jobs
   or artifacts, and control API stays read-only.
