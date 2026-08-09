# M1 Autonomous Capacity Governance Design

**Date:** 2026-08-09
**Status:** approved for implementation
**Scope:** keep the M1 market-perception service continuously useful under
SQLite-volume pressure, without making manual Fly volume expansion a runtime
precondition.

## Production requirement

Capacity is a recurring production condition, not an exceptional deployment
gate.  M1 must keep its authoritative Quote path available wherever safely
possible; it must record, diagnose, alert, and automatically work through
storage pressure rather than silently stopping or permanently degrading.

The 50 GB Fly volume currently has approximately 15.8% free space and
`state.db` is approximately 40.9 GB.  Recent Quote hard timeouts coincide with
this pressure.  Existing health already shows volume pressure, but archive has
never run and the resource controller is disabled.  This design makes those
facts an active control loop.

Manual volume expansion remains an **emergency headroom action** only.  It is
never required to start, resume, or prove the normal M1 pipeline.

## Decision

Implement a resident, quote-aware capacity controller with three data tiers:

| Tier | Contents | Retention / recovery rule |
|---|---|---|
| Hot SQLite | current market truth, fresh certified Quotes, operational cursors | bounded by policy; writer-friendly and immediately queryable |
| Cold archive | aged bulky replay payloads | immutable Parquet segments with manifest, checksum, source identity, and restore verification |
| Proof skeleton | publication / certification / incident / archive receipts | retained in SQLite even after bulky payload moves, preserving audit and recovery authority |

The controller is a normal resident owner, not a manual maintenance command.
It prioritizes Quote, cooperates with Structure, makes bounded transactions,
and writes durable runtime/incident truth.

## 1. Watermark state machine

Free-volume percentage drives the controller.  Defaults are configuration, not
hard-coded policy:

| State | Default threshold | Automatic behaviour | M1 serving meaning |
|---|---:|---|---|
| `normal` | >20% | normal bounded archival and retention | full normal operation |
| `pressure` | <=20% | begin/accelerate cold archival and safe SQLite reuse; defer nonessential Structure drift | Quote remains highest priority |
| `critical` | <=12% | quote-first admission, archive/reclaim continuously in small chunks, create incident and urgent alert | serve only fresh certified Quote truth; no stale success claim |
| `exhaustion-imminent` | <=6% or write-space failure | stop all optional producers, preserve the last certified feed, write a terminal capacity incident and alert | strict health fails until recovery; no trading input may be consumed |

Recovery uses hysteresis: a state exits only after the high watermark is held
for a configured stability window and the controller has a successful archive
or reclaim receipt.  This prevents oscillation near a threshold.

The controller must never lower freshness, identity, certification, source
truth, or M2 execution gates in response to pressure.

## 2. Safe automatic work

Each controller tick performs at most one bounded operation and yields to Quote
before and after acquiring the shared writer lock:

1. Measure free space and SQLite page/freelist facts using inexpensive
   filesystem and pragma reads.
2. If pressure exists, select the oldest policy-eligible heavy payload whose
   authority is represented by a retained proof skeleton.
3. Materialize a deterministic Parquet segment outside the transaction.
4. Verify row count, content digest, source/generation identity, and readable
   restore sample.  Persist an archive manifest/receipt first.
5. In one small SQLite transaction, mark the payload archived and reclaim only
   the payload rows.  Never delete current generation, current certified Quote
   data, incident history, source receipts, or archive receipt.
6. Record the resulting free-space measurement and controller checkpoint.

SQLite pages freed by deletion are immediately reusable by later writers.  The
normal controller therefore does **not** run online `VACUUM`.

If physical file compaction is necessary after sustained high freelist ratio,
a separate rolling compaction job builds and verifies a copy, waits for a
Quote-safe window, atomically switches on restart, and preserves the original
until the replacement passes health and restore checks.  It is restart-safe,
rate-limited, and never runs on the critical Quote path.

## 3. Scheduling and fairness

- Quote collecting or due always wins.  Capacity work records `quote-priority`
  and backs off; it does not wait while holding the writer lock.
- Structure publication remains authoritative but nonessential drift,
  historical comparison, and optional cleanup are deferred in `pressure` and
  suppressed in `critical`.
- Archive/reclaim chunks have row/byte/time budgets and capped exponential
  retry.  Writer-busy is a normal defer, not a terminal failure.
- A controller failure cannot cancel a Quote or Structure worker.  Repeated
  controller failure opens its own incident and keeps retrying at a bounded
  cadence.
- On startup, an orphaned `running` checkpoint becomes `retry-pending` with
  its prior receipt/cursor; no manual advance is required.

## 4. Durable operator truth and alert chain

Add a durable singleton `capacity_controller_runtime` with state, watermark,
free bytes/percent, last measurement, active operation, cursor, bytes/rows
archived and reclaimed, retry/failure count, next attempt, and safe error
classification.  Archive manifests and reclaim receipts are the destructive
operation authority; runtime is operational truth.

`/health` must expose at least:

- `storage:volume_free_percent` with watermark/state;
- `perception:capacity_controller` with runtime progress and next action;
- `archive:last_attempt` and `archive:last_success_age_seconds` from actual
  resident work;
- `perception:open_incidents` containing capacity incidents.

Dashboard/`/perception/incidents` must render impact, automatic actions already
attempted, last error, next action, space trend, and recovery receipt.  The
alert chain opens once per capacity episode, escalates at `critical` and
`exhaustion-imminent`, and closes only after hysteresis plus a verified receipt.
Delivery failure itself remains visible; durable incident truth does not rely
on Telegram or logs.

## 5. Explicit non-goals and safety boundaries

- No automatic Fly volume resize, destructive file deletion, online `VACUUM`,
  or migration to another database.
- No erasure of authority evidence merely to make a health check green.
- No static permanent degradation: each defer has a recorded reason, next
  attempt, and automatic recovery condition.
- No use of archived payload as current Quote truth without manifest and
  restore verification.
- No M2 execution eligibility from a stale or pressure-compromised Quote feed.

## 6. Chain-truth and acceptance criteria

Every transition must be tested end to end: writer mutation/receipt → durable
runtime → `/health` subcheck → Polywatch decision → Dashboard incident →
verified recovery.

Before production rollout, prove:

1. 20% pressure automatically creates archive/reclaim progress while fresh
   Quote completion continues within the existing SLA.
2. A failed archive, writer-busy conflict, restart mid-operation, bad manifest,
   and unavailable object store each leave payload authoritative and create a
   diagnosable bounded retry; none causes silent worker death.
3. Quote priority wins both before and after lock acquisition, including under
   continuous pressure work.
4. Archived payload restores from its manifest and matches digest/identity;
   proof skeleton remains queryable after reclaim.
5. Critical/exhaustion states alert and fail strict health appropriately, while
   recovery transitions are observable and require real free-space improvement.
6. A production-shaped long soak demonstrates bounded hot storage, recurring
   archive receipts, continued opportunity discovery, and no manual capacity
   action required for normal recovery.

## Delivery order

1. Implement durable runtime, watermarks, health/incident/dashboard contract,
   and quote-aware scheduler using existing safe SQLite retention primitives.
2. Add manifest-backed cold archive plus restore verification for the largest
   eligible payload families.
3. Add rolling compaction only after measured freelist/latency evidence shows
   that ordinary reuse and archival cannot maintain the high watermark.
