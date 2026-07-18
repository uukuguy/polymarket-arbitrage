# M1 Durable Data Chain Recovery Design

**Date:** 2026-07-18  
**Workstream:** `m1-perception`  
**Target:** focused Phase 05.1 gap closure  
**Status:** user-approved design

## 1. Problem and verified evidence

M1's L1 snapshot pipeline remained fresh while the L2 business-data chain was
stale for roughly 40 days. The process stayed reachable and `/healthz` returned
HTTP 200, but strict `/health` returned 503:

- candidate Supabase fetch age was about 3.49 million seconds;
- WebSocket event age was about 1.81 million seconds;
- L2 mirror age was about 1.83 million seconds;
- L3 remained `0/10` and `l2_book_levels` had never written.

Production evidence identified two distinct failures:

1. L1's `POLYARB_EVENT_BUS_ENABLED` flag was absent, so new snapshots did not
   publish `snapshot_complete` notifications despite L1 continuing to write
   fresh rows.
2. After enabling the flag, a valid direct PostgreSQL NOTIFY still did not reach
   candidate refresh. `listen_snapshot_complete()` had opened a connection and
   then waited only on `stop_event`; termination of the asyncpg connection could
   not wake the task or enter its documented reconnect loop. The health wrapper
   continued reporting `listening` from a cached boolean.

Restarting the single L2 Fly machine restored the chain operationally:

- durable cursor advanced from snapshot `482` to `516`;
- candidate fetch, WS event, and mirror ages returned to seconds;
- strict `/health` changed from HTTP 503 to HTTP 200;
- new `l2_top_of_book` rows appeared for two subscribed assets.

The restart also exposed projection drift: Supabase showed 117 distinct active
candidate assets while the real WS candidate set contained two assets. The
mirror had only reconciled diffs against process-local memory, so a cold start
could not close stale active history rows.

## 2. Scope and non-goals

This phase makes the L1 snapshot → candidate refresh → WS → mirror chain
self-healing and makes its candidate projection truthful.

In scope:

- durable reconciliation driven by cursor state;
- LISTEN termination detection and reconnect;
- successful-processing cursor commits;
- periodic recovery from missed NOTIFY events;
- candidate history reconciliation by `(asset_id, recipe_name)`;
- chain-truth health checks and end-to-end chaos evidence;
- Makefile entry points for every new operational command;
- focused teaching-document update.

Out of scope:

- changing candidate recipes or thresholds;
- weakening Phase 05's strict N=5 L3 gate;
- declaring Phase 05 complete;
- adding real-money execution;
- repairing the historical repository-wide Ruff baseline;
- redesigning unrelated L1 storage or R2 behavior.

## 3. Chosen architecture

### 3.1 Durable reconciliation pump

PostgreSQL NOTIFY becomes a low-latency wake-up hint, not the source of truth.
`l2_event_cursor` is the durable processing boundary.

One serialized pump owns candidate refresh:

1. Wake immediately on a valid `snapshot_complete` notification or at least
   every 60 seconds.
2. Read the latest snapshot ID and the consumer cursor.
3. If there is lag, process only the newest snapshot. Candidate refresh reads
   current `markets_latest`, so replaying every historical snapshot produces no
   additional truth and currently creates concurrent debounced tasks.
4. Await candidate refresh and its required reconciliation result.
5. Advance the cursor only after successful processing.
6. Leave the cursor unchanged on failure so the periodic loop retries.

The pump must serialize overlapping NOTIFY and polling wake-ups. Multiple wake
signals may coalesce, but two refreshes must never run concurrently.

### 3.2 LISTEN lifecycle

The listener must observe actual asyncpg connection termination. It waits for
either shutdown or a termination signal, closes the old connection, reports
`reconnecting`, applies bounded backoff, and creates a new connection.

The periodic cursor reconciliation remains active while LISTEN is unavailable.
This ensures a silent or lossy notification channel cannot freeze business data.

### 3.3 Cursor semantics

The cursor means “candidate state for this snapshot or a newer one was
successfully reconciled,” not “a task was scheduled.”

Cursor updates therefore occur after the awaited refresh result. Startup uses
the same pump path as steady state; it must not dispatch every missed row and
advance the cursor before those tasks finish.

Cursor updates should also update the existing `updated_at` field so raw SQL
provides an operator-visible success timestamp.

### 3.4 Candidate projection reconciliation

Candidate history identity is `(asset_id, recipe_name)`. The projection keeps
multiple inclusion/removal cycles and preserves multiple recipe matches for one
asset.

For each successful refresh:

1. Compute the desired active key set from all current `CandidateRow` values.
2. Read all currently active Supabase candidate keys.
3. Mark active keys missing from the desired set with `removed_at_ts`.
4. Insert desired keys not currently active as new history rows.
5. Leave retained keys unchanged.

WS subscription remains a unique `asset_id` set. Health and dashboard counts
must label recipe-row count separately from distinct active asset count.

Reconciliation failure is not allowed to advance the cursor. This is stricter
than the existing fail-soft mirror writes because this projection is now part of
the durable chain-truth contract.

## 4. Health and observability contract

The following surfaces must be backed by live state rather than cached startup
flags:

- `event_bus:connection_state`: connected, reconnecting, or disabled/error;
- `event_bus:last_notification_age_seconds`;
- `candidates:last_reconcile_age_seconds`;
- `candidates:cursor_lag`: latest snapshot ID minus consumer cursor;
- `event_bus:reconnect_count`;
- existing candidate fetch, WS event, and mirror freshness checks.

The listener connection may be warning while periodic reconciliation keeps
cursor lag at zero. A stale reconciliation age or positive cursor lag beyond
the defined grace window must fail strict `/health` even when LISTEN says
connected.

Health implementation must satisfy the project's five-item chain-truth
checklist: named health check, real mutated source, declared config gates,
success/failure mutations, and an end-to-end chaos trigger.

## 5. Failure behavior

- Malformed NOTIFY payloads are logged and ignored; periodic reconciliation
  still recovers the durable state.
- LISTEN connection loss triggers reconnect without stopping cursor polling.
- Supabase read or candidate computation failure leaves the cursor unchanged.
- Candidate reconciliation failure leaves the cursor unchanged and fails the
  reconciliation freshness/cursor-lag health surface.
- WS dynamic subscribe failure remains visible through WS freshness and retries
  on reconnect using the desired in-memory candidate set.
- Shutdown and `CancelledError` continue to propagate promptly.

## 6. Verification strategy

Implementation follows test-first RED → GREEN cycles.

Required automated proofs:

1. A terminated asyncpg connection wakes the listener and reconnects.
2. A missed NOTIFY is recovered by the periodic cursor poll within 60 seconds.
3. Concurrent wake signals produce one serialized refresh.
4. Refresh failure does not advance the cursor; a later success does.
5. Startup with a large cursor gap processes the newest snapshot once rather
   than dispatching every historical row.
6. Candidate reconciliation closes stale active keys, inserts new keys, retains
   unchanged keys, and preserves multi-recipe identity.
7. Health values read the live pump/listener state and fail on stale cursor lag.
8. Cancellation and shutdown remain bounded.

Required production/chaos proofs:

- disconnect or restart the LISTEN session and observe automatic reconnect;
- suppress one notification and observe cursor convergence through polling;
- compare Supabase active distinct assets with the WS candidate set;
- observe fresh candidate, WS, and mirror timestamps for a sustained window;
- verify strict `/health` truthfully reflects every injected failure;
- preserve Phase 05 L3 `0/10` or later result as a separate verdict.

## 7. Delivery and documentation

The work is delivered as a focused M1 Phase 05.1 gap phase with explicit plans,
SUMMARY artifacts, `make planning-status` gates, and Makefile targets for all
new executable diagnostics/chaos actions.

The learning documentation must explain the core mental model:

> NOTIFY is a doorbell; the durable cursor is the ledger. A broken doorbell
> cannot lose work when the consumer periodically reconciles the ledger.

Phase 05's 24-hour strict N=5 soak begins only after this phase proves the
upstream prerequisites remain fresh. This design does not authorize relaxing
candidate or L3 thresholds to obtain a green verdict.

## 8. Production amendment: quiet-market book refresh

### 8.1 Evidence and contradiction

The no-restart production proof passed for LISTEN reconnect, polling recovery,
cursor convergence, and candidate projection. A later strict health check still
failed after the five subscribed candidates produced no business frame for more
than 600 seconds:

- the WebSocket remained open and continued completing ping/pong keepalives;
- `ws:connection_state` was `WAITING_FOR_EVENT`;
- `ws:last_event_age_seconds` and `mirror:l2_tob_age_seconds` crossed their
  existing fail thresholds;
- the watchdog intentionally treated a live-but-quiet socket as benign and did
  not reconnect.

This is a contract contradiction, not proof that the socket transport failed:
the runtime accepts indefinite market silence while strict health requires a
recent business snapshot. The freshness thresholds remain unchanged.

### 8.2 Chosen behavior

Before a quiet subscription reaches the existing warning boundary, the
consumer sends one in-band dynamic subscription update for the complete active
asset set:

```json
{
  "operation": "subscribe",
  "assets_ids": ["..."],
  "initial_dump": true
}
```

Polymarket's official WebSocket contract permits subscription changes without
reconnecting, and its changelog defines `initial_dump` as the optional request
for initial order-book state. A `book` event is emitted when subscribing to a
market. The relevant primary references are:

- https://docs.polymarket.com/market-data/websocket/overview
- https://docs.polymarket.com/market-data/websocket/market-channel
- https://docs.polymarket.com/changelog

The refresh is single-flight and rate-limited. It uses the real current union of
candidate and L3 asset sets, does not mutate either set, and does not mark the
data path fresh merely because the send succeeded. Only a received business
frame may update `last_event_at_s`, touch the watchdog, and write the mirror.

### 8.3 Failure behavior

- No live socket or a failed send leaves all freshness timestamps unchanged;
  strict health therefore continues to expose the failure.
- Repeated refresh requests are bounded so an upstream outage cannot produce a
  send storm.
- Normal subscription diffing and transport reconnect behavior remain
  unchanged.
- This amendment does not alter WS, mirror, candidate, or L3 health thresholds.

### 8.4 Verification

Automated tests must prove:

1. quiet live sockets send one refresh payload containing the current active
   union and `initial_dump=true`;
2. refresh is suppressed before the trigger age and during its cooldown;
3. empty sets, missing sockets, and send failures do not forge freshness;
4. a returned `book` frame follows the normal dispatch path and refreshes the
   mirror;
5. cancellation remains prompt.

Production acceptance requires the same Fly machine identity throughout, a
logged quiet refresh followed by real `book` frames, cursor lag zero, converged
five-asset candidate projection, and strict health with the existing freshness
thresholds. Phase 05's independent L3 `0/10` gate remains a warning/verdict and
is not relaxed by this recovery amendment.
