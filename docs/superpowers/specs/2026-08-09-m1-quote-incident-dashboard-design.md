# M1 Quote Incident and Dashboard Design

## Purpose

Make a failed or repeatedly timed-out read-only Quote collection observable and
actionable without treating retry as recovery.  An operator must be able to
open the Dashboard and see the active incident, the exact evidence, the
automatic actions already attempted, the next automatic action, and the
condition for escalation.  Telegram remains the immediate delivery channel;
the durable Dashboard/API ledger is the source of diagnostic truth.

## Current gap

`QuoteWorkerRuntime.mark_failure()` records process-local runtime state and
strict health exposes freshness/failure fields.  The worker retries immediately
after a subprocess timeout, and Polywatch can send a Telegram alert.  However,
no durable `IncidentManager` record is created for this producer.  The existing
`/perception/incidents` and Dashboard “Open incidents” card therefore cannot
show Quote timeout evidence, diagnosis, remediation, or recovery history.

## Chosen design

### Durable Quote incident lifecycle

- The Quote worker owns a single incident scope: `quote-collection`.
- A `QuoteCollectionSubprocessError(reason="timeout")` creates or updates the
  canonical `quote-collection-timeout` incident. Other terminal collection
  failures create `quote-collection-failure`; source supersession is a normal
  rebind and does not open an outage incident.
- Detection evidence includes: run ID when assigned, requested token count,
  120-second hard deadline, consecutive failure count, prior successful run
  timestamp/age, last error kind, and the source/universe identity if known.
- Lifecycle transitions are explicit: `detected → classified → recovering` on
  the first auto-retry; another failure updates the same active incident and
  increments the retry count; the next certified fresh success transitions it
  to `verified` with recovery run ID, successful/required responses, elapsed
  time, and recovery timestamp.
- The producer never claims recovery merely because it started another child.
  Only `mark_success` after certified projection publication may close it.

### Diagnosis and operator disposition

The bounded public incident envelope adds a typed, redacted `diagnosis` field
for Quote incidents. It is deterministic rather than LLM-generated:

| Condition | Diagnosis | Automatic disposition | Operator next action |
|---|---|---|---|
| First timeout, fresh last good feed | collection child exceeded deadline | immediate retry | observe next attempt and CLOB latency |
| Repeated timeout, still within feed freshness SLA | retrying but feed at risk | bounded retry/backoff | inspect child I/O and upstream request timing |
| Feed stale or repeated failures exceed threshold | M2 feed unavailable | continue bounded retries and alert | investigate CLOB availability, process pipe pressure, and capacity; do not consume opportunities |
| Certified success | recovered | close incident | verify freshness progression |

No secret, URL credential, request body, or order/wallet data appears in this
read model.

### Dashboard and API

- Extend `/perception/incidents` only with validated optional diagnostic fields;
  historical non-Quote incident contract remains compatible.
- The existing `/perception` Open incidents card gains a prominent `Impact`,
  `Automatic action`, `Next action`, and Quote evidence block.  It renders
  `not recorded` rather than inventing a diagnosis for old records.
- The card distinguishes `feed at risk` from `feed unavailable`; it must never
  display zero opportunities as a successful market conclusion when the Quote
  feed is stale.
- The existing detail/history route remains the full lifecycle trace. The
  overview card links a Quote incident to its history endpoint.

### Alert and recovery chain

1. Quote child hard deadline expires.
2. Worker terminalizes the child/attempt, writes durable incident evidence,
   starts bounded auto-recovery, and marks strict Quote health failed when the
   freshness gate is exceeded.
3. Resident Polywatch and external watchdog deliver/guard the alert chain.
4. Dashboard reads the same durable incident record, independent of log
   retention or a Telegram delivery outcome.
5. A certified successful Quote projection closes the incident with evidence;
   the Dashboard then exposes the recovery transition.

## Acceptance criteria

1. A red/green test proves timeout creates exactly one durable Quote incident
   with run/deadline/retry evidence and deterministic disposition.
2. Repeated timeouts update the same open incident; they do not create an
   unbounded incident storm or silently reset the retry count.
3. Certified success alone produces the verified recovery transition and
   preserves its evidence in the history endpoint.
4. API validation rejects malformed diagnostic evidence and redacts unsupported
   fields.
5. Dashboard tests render active Quote incident impact, automatic disposition,
   next action, and recovery state; unavailable data remains unavailable.
6. A production timeout is visible via Dashboard/API, Telegram, and strict
   health with matching incident identity/timestamps.

## Non-goals

- No trading, wallet, signer, order, secret, or automatic infrastructure
  mutation is introduced.
- No increase to the Quote deadline or weakening of freshness/identity gates.
- Root-cause repair of the current CLOB/subprocess stall is a separate bounded
  investigation; this design makes that investigation observable.
