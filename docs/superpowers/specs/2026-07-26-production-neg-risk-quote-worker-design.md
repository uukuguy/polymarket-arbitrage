# Production Neg-Risk Quote Worker Design

**Date:** 2026-07-26
**Status:** Approved
**Approved option:** Scheme A — in-process L1 quote worker

## Problem

The production opportunity endpoint contains the H-009 atomic quote-run scanner,
but no production task creates quote runs. The truthful result is therefore:

```json
{"error":"quote run unavailable"}
```

This is not a zero-opportunity result. It means the M1→M2 opportunity feed has
no fresh executable quote source.

The existing Fly `cron` process is not a valid producer host. The HTTP `app`
machine owns `/data/state.db` on its attached volume; the running `cron` machine
has no volume mount. A cron-side SQLite write would be invisible to the route.
The 256 MB cron machine also exited 137 during its latest snapshot attempt.

## Capacity evidence

The separately authorized production capacity observation ran on the L1 app
machine against snapshot 711:

| Measure | Observed |
|---|---:|
| Eligible YES tokens | 1,278 |
| Neg-risk groups | 254 |
| CLOB batches at size 500 | 3 |
| Accepted responses | 1,278 / 1,278 |
| Collector elapsed | 1,013 ms |
| Wall elapsed | 1.035 s |

The public endpoint returned HTTP 200 immediately after the complete run. The
observed duration is below 1% of the chosen 120-second cadence and comfortably
inside the unchanged 300-second quote SLA.

## Decision

Run one dedicated, fail-soft quote worker inside the L1 `app` process. It reads
the same latest snapshot universe and writes the same `/data/state.db` consumed
by `/arbitrage/opportunities`.

The worker:

- is disabled by default in local/test settings and explicitly enabled in
  `fly.toml`;
- runs immediately after the HTTP server binds, then every 120 seconds measured
  from the end of the prior attempt;
- has exactly one in-process loop, while the existing durable SQLite lease
  rejects any accidental second collector;
- performs public, unauthenticated CLOB reads only;
- never signs, submits, cancels, or settles an order;
- catches collection failures, records bounded runtime state, logs the failure,
  and retries on the next interval;
- preserves the last complete run when a newer attempt fails;
- propagates cancellation so L1 shutdown remains bounded.

## Freshness and health contract

The opportunity route keeps the existing hard limits:

- quote age: at most 300 seconds;
- universe age: at most 50,400 seconds.

When the production worker is enabled, both `/health` and `/healthz` include:

1. `quote_feed:last_complete_age_seconds`
   - `pass`: a complete run exists and age is below 240 seconds;
   - `warn`: age is from 240 through 300 seconds;
   - `fail`: no complete run exists or age is greater than 300 seconds.
2. `quote_feed:collector_state`
   - bounded process-local state: cold-start, collecting, pass, error, stopped;
   - a current error warns while a still-fresh durable run remains usable;
   - the durable age check escalates to fail if failures continue.

The durable complete-run timestamp is the success anchor. A failed attempt does
not update it, so repeated failure mechanically ages into a strict health
failure and an HTTP 503 opportunity feed.

## Lifecycle

`polyarb.daemon.main` creates the worker and passes its runtime state into the
Starlette application. After Uvicorn reports that its socket is bound, the
daemon starts both the snapshot scheduler and quote worker.

On SIGINT/SIGTERM, it cancels both background tasks and gathers them with the
existing five-second shutdown bound. Worker cancellation is never converted
into a collection failure.

## Safety and scope

- No freshness threshold is relaxed.
- No partial or mixed quote run becomes visible.
- No current snapshot, L2, L3, wallet, or execution logic changes.
- Opportunity output remains `gross-before-fees` and `known-universe`.
- Positive output is a discovery lead, not an order instruction. Fees, fill
  probability, market-group semantic completeness, atomic execution, and
  real-money readiness remain outside this rollout.
- Existing R2 configuration and event-bus password warnings are recorded but
  are not silently folded into this focused change.

## Acceptance

Local acceptance requires RED/GREEN evidence for:

- immediate first collection and 120-second cadence;
- no overlapping attempt;
- fail-soft retry and preservation of the prior complete run;
- cancellation propagation;
- health cold-start, pass, warn, and fail boundaries;
- disabled-by-default behavior and explicit Fly enablement;
- lifecycle wiring and same-database construction;
- existing opportunity route stale/unavailable semantics.

Production acceptance requires:

1. exact deployed source identity recorded;
2. a fresh complete run within one interval;
3. at least three successive complete runs with full response coverage adjusted
   only for a newer universe;
4. strict health exposing fresh quote checks;
5. repeated HTTP 200 opportunity responses spanning more than one worker
   interval;
6. no increase in snapshot failure count or process memory alarm.
