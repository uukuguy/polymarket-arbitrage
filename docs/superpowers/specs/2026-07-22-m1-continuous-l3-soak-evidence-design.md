# M1 Continuous L3 Soak Evidence Design

**Date:** 2026-07-22  
**Workstream:** `m1-perception`  
**Target:** replace Phase 05 six-hour spot-check claims with durable interval evidence  
**Status:** direction approved in conversation; written-spec review pending

## 1. Outcome

Make the Phase 05 statement “five L3 markets remained healthy throughout a
24-hour soak” mechanically provable.

The existing T+0/T+6/T+12/T+18/T+24 observations remain useful human review
points, but they stop being the primary evidence. The system will instead
persist:

1. every scheduled promoter run and its outcome;
2. intended, control-committed, and business-evidenced WS membership;
3. per-market book/OHLC freshness rather than one global last-write clock;
4. a 30-second health time series with detectable gaps;
5. watchdog, reconnect, process-boot, and identity events with retention longer
   than the soak.

After this instrumentation is deployed and proven locally, a new exact-identity
24-hour soak starts from a fresh T+0. The current
`2026-07-21T14:29:47.941Z` re-soak remains useful diagnostic traffic but cannot
close Phase 05 because its observation contract changed after T+0.

## 2. Verified problem

The present plan says `min(observed_l3_market_count) == 5 throughout 24h` while
sampling `/health` every six hours. Five observations cannot prove the state
between them.

The six-hour interval contains roughly 72 promoter runs plus an unbounded number
of WS frames, subscription controls, projections, mirror writes, candidate
reconciliations, and watchdog decisions. Current evidence loses important
transitions:

- `_l3_active_set`, `_last_promote_at_s`, and
  `_last_book_levels_write_at_s` retain only current/latest process state.
- `l3_promoted_at_ts` mirrors current membership; it is not an append-only
  membership history.
- `l3:active_count` is informational and does not affect overall health.
- `promote_run` does not consume Boolean failure results from WS add/remove
  operations before committing its intended active set.
- one global book-write timestamp can stay fresh because one hot market writes
  while four promoted markets are silent.
- Fly's rolling log buffer already lost the first soak's watchdog interval.

Durable `l2_book_levels` and OHLC rows prove that data existed for a market over
an interval. They cannot reconstruct a five-minute under-fill that recovered, a
failed subscription send, a skipped promoter tick, or a process restart whose
logs have rolled away.

## 3. Evidence model

Add Alembic revision `007`, chained from production head `006`, with five
append-oriented evidence tables. Exact SQL types and indexes may be refined in
the implementation plan, but the fields and semantics below are locked.

### 3.1 `l3_runtime_boots`

One row per daemon process incarnation:

- `boot_id` — UUID generated once at process start;
- `started_at`, optional `stopped_at`;
- Fly machine/instance identity available to the process;
- code/version identifier and recipe/config digest;
- termination classification when graceful shutdown is observed.

Missing `stopped_at` is not rewritten after a crash. A new `boot_id` inside a
strict soak invalidates the exact-incarnation gate rather than being silently
joined to the previous process.

### 3.2 `l3_promote_runs`

Exactly one durable terminal row for every scheduled 300-second promoter tick,
including initial execution. Required fields:

- `boot_id`, monotonic `run_seq`, `scheduled_at`, `started_at`, `finished_at`;
- terminal `status`: `success`, `frozen`, `underfilled`, or `failed`;
- bounded reason/error code;
- selected market count and intended token count;
- control-committed token count and business-evidenced token count;
- added/removed token counts;
- deterministic hashes of market mapping, intended tokens, and committed tokens;
- WS control generation and add/remove operation results;
- mirror write-through result and duration.

The uniqueness key `(boot_id, run_seq)` makes a missing tick detectable. A
promoter exception must still attempt a terminal `failed` row. If the evidence
write itself fails, the in-process evidence freshness anchor does not advance,
and `/health` exposes the failure.

### 3.3 `l3_health_samples` and `l3_market_samples`

A daemon task samples every 30 seconds.

`l3_health_samples` stores one process-level snapshot:

- `boot_id`, sample sequence, sampled timestamp;
- intended/control-committed/business-evidenced token counts;
- promote, global book, WS, mirror, candidate, listener, reconciliation, and
  cursor values/statuses;
- watchdog and reconnect monotonic counters;
- current mapping/config hashes.

`l3_market_samples` stores one row for each of the five intended Yes-market
identities at the same sample:

- market ID plus Yes/No token IDs;
- requested/committed membership for both tokens;
- same-generation business-frame evidence;
- last book timestamp and age per token/market;
- last Yes-side OHLC timestamp and age;
- per-market status and bounded reason.

One active market cannot refresh another market's clock. The existing global
anchors remain for compatibility but no longer satisfy the strict gate alone.

### 3.4 `l3_runtime_events`

Append low-volume operator-critical events:

- watchdog stale decision;
- reconnect reserved, deferred, started, succeeded, or failed;
- WS generation change;
- subscription-control failure or compensation;
- evidence-writer failure/recovery;
- process shutdown signal.

The table is the strict absence/presence source. Fly logs remain a diagnostic
copy, not acceptance evidence.

All five tables retain at least 30 days. Expected volume is small: about 1,440
process samples, 7,200 five-market samples, and 288 promoter rows per day.

## 4. Membership truth and promoter transaction

Replace the overloaded idea of “active” with three explicit states:

1. **Desired** — the five markets/ten tokens selected by the promoter recipe.
2. **Control-committed** — tokens for which the current WS generation accepted
   the serialized subscribe/unsubscribe control path, or which are explicitly
   queued as reconnect desired state.
3. **Business-evidenced** — tokens observed through the expected initial/book
   frame on that WS generation.

`promote_run` must not ignore `False` from `add_subscriptions` or
`remove_subscriptions`. It records the failed operation, leaves committed
membership truthful, and returns a non-success terminal run. Supabase
`l3_promoted_at_ts` mirrors control-committed market membership only after the
reconciliation result is known.

Because Polymarket does not provide a durable subscription ledger, socket send
success is not treated as business proof. Same-generation initial/book evidence
is tracked separately. Quiet-market refresh may produce that evidence, but it
cannot rewrite an earlier failed control result.

The strict cardinality invariant is:

```text
desired == control_committed == 10 distinct tokens
and mapping == 5 complete distinct Yes/No pairs
```

Business evidence is evaluated per token/market under the existing freshness
policy. Phase 05's strict gate has no quiet-market exception: the quiet-refresh
mechanism must create fresh same-generation evidence for every promoted market.
A market that exceeds the locked freshness threshold makes the interval
`NOT-CLOSED`, even when pong/control-generation liveness and the global
last-write clock remain healthy.

## 5. Sampling, aggregation, and verdict

### 5.1 Machine cadence

- Health and per-market samples: every 30 seconds.
- Maximum allowed consecutive sample gap: 75 seconds.
- Promoter run cadence: every 300 seconds, with no missing `run_seq` and no
  scheduled-to-start gap above 360 seconds.
- Evidence retention: at least 30 days.

The discrete 30-second sampler does not claim mathematical continuity; it gives
a bounded observation gap. Event-driven promoter and runtime rows cover the
state transitions that matter between samples.

### 5.2 Six-hour checkpoint

Keep T+6/T+12/T+18/T+24 as human-readable summaries generated from durable
evidence. A checkpoint queries the exact `[T+0, checkpoint)` interval and reports:

- expected versus recorded promoter ticks and every non-success row;
- maximum health-sample gap;
- minimum desired/committed/evidenced cardinality;
- per-market maximum book/OHLC ages and coverage;
- watchdog/reconnect/runtime-event counts;
- boot IDs and identity/config hashes observed;
- raw evidence bounds and row counts.

The checkpoint never substitutes a new current `/health` response for missing
history.

### 5.3 Strict 24-hour PASS

Phase 05 may close only when one immutable 24-hour window satisfies all of:

1. exactly one accepted boot/runtime identity for the whole window;
2. no health-sample gap greater than 75 seconds;
3. every expected promoter tick has one terminal durable row;
4. every promoter row is `success` with five complete markets/ten desired and
   control-committed tokens;
5. per-market evidence meets the locked book/OHLC freshness and coverage gates;
6. zero unapproved watchdog-stale, reconnect-failure, WS-control-failure, or
   evidence-writer-failure events;
7. exact-window book and Yes-side OHLC coverage includes all five bound markets;
8. T+0/T+6/T+12/T+18/T+24 summary artifacts are retained and hash-bound to the
   same window.

Any violation makes the window `NOT-CLOSED`. Repairing the system later does not
rewrite the failed interval; a new formal window starts after diagnosis/fix.

## 6. Operator surface and Makefile contract

Every new executable path has a Makefile entry:

- `make l3-evidence-status` — current sampler/promoter continuity and latest
  desired/committed/evidenced membership;
- `make l3-soak-checkpoint start=<ISO> end=<ISO>` — render an interval summary
  with non-zero exit on a strict violation;
- `make l3-soak-verify start=<ISO> end=<ISO>` — final 24-hour verdict;
- `make l3-evidence-retention-check` — prove oldest retained evidence exceeds
  the configured minimum.

Outputs must include UTC boundaries, boot ID, release/machine anchors, evidence
row counts, maximum gaps, and explicit `PASS`/`NOT-CLOSED`. Commands are
read-only except the daemon's normal append-only evidence writer.

## 7. Failure and chain-truth contract

- A failed evidence write leaves its success anchor unchanged; health ages into
  warn/fail and the external interval query sees a gap.
- Evidence rows are never synthesized after recovery.
- An under-filled promoter run is persisted even if the next run recovers.
- A WS control failure cannot be converted into active membership merely by
  updating the promoter's intended set.
- A process restart creates a new boot ID and invalidates the current strict
  window unless a future, separately approved acceptance contract says otherwise.
- A single hot market cannot satisfy another market's freshness.
- Unavailable historical logs are reported as unavailable, never zero.
- Evidence schema/config/version changes alter the bound digest and require a
  new T+0.

Add `/health` subchecks for evidence sample freshness, promoter-ledger freshness,
desired-versus-committed membership equality, and per-market worst freshness.
Each subcheck must read a field actually mutated by the corresponding success
path and have a chaos/integration test that breaks the complete chain.

## 8. Test-first verification

Implementation proceeds in RED/GREEN slices. Required automated and chaos proofs:

1. Alembic 007 is add-only, indexed for exact-window queries, and replays through
   upgrade/downgrade/re-upgrade locally.
2. Initial and periodic promoter ticks produce contiguous `(boot_id, run_seq)`
   rows for success, freeze, under-fill, exception, and evidence-write failure.
3. WS add/remove returning `False` leaves desired and committed states unequal,
   records the failure, and prevents a success run.
4. Same-generation business evidence cannot be borrowed from an older socket
   generation.
5. One hot market with four silent markets makes the per-market strict gate fail
   even while the compatibility global book anchor is fresh.
6. Killing the sampler creates a detectable gap greater than 75 seconds; restart
   does not backfill it.
7. A daemon restart creates a second boot ID and makes the active window fail.
8. Watchdog/reconnect events remain queryable after Fly CLI logs are unavailable.
9. Six-hour and final Make targets use exact interval boundaries and fail closed
   on missing rows, missing ticks, identity change, or evidence gaps.
10. Chain-truth chaos exercises every new health subcheck end to end.
11. Focused tests, full repository pytest, Ruff, compile, image-aware checks,
    `make planning-status`, and manual M1 contracts pass before completion.

## 9. Delivery sequence and hard gates

Create a focused M1 gap phase, recommended identifier
`05.4-continuous-l3-soak-evidence`, with separate plans for:

1. evidence schema and boot/promoter ledger;
2. truthful desired/committed/business membership reconciliation;
3. 30-second process/per-market sampler and durable runtime events;
4. Makefile aggregation/verdict tools plus chaos and teaching documentation;
5. production migration/deployment/readiness proof and a fresh strict 24-hour
   soak.

Local implementation and verification do not authorize production mutation.
Production Alembic 007 and deployment require a separate explicit gate. The
current release-37 re-soak is archived as diagnostic-only; no T+6 observation
can upgrade it back into a strict PASS.
