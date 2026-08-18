# M1 Watchdog External Supervisor — Design

## Problem

`polyarb-control-alert` immediately pages Telegram when data-plane or support
targets fail, but its per-check heartbeat is stdout only. Fly restarts that
Machine, yet a stopped or restart-looping watchdog has no independent actor
that can page or create a Dashboard incident. This leaves one monitoring node
outside the otherwise explicit runtime boundary.

## Decision

Deploy one minimal Cloudflare Worker with a UTC `* * * * *` Cron trigger. It
reads exactly the Fly alert Machine `d891e941a41dd8` through the Fly Machines
API. A non-`started` state, a missing Machine, a malformed response, or a
restart-count increase is an unhealthy observation.

The Worker stores only the previous observation in a dedicated KV namespace.
It emits Telegram and an authenticated event-writer request only when health
changes. Its idempotency key is SHA-256 of the source, kind, scheduled UTC
minute and normalized failures: Cloudflare's at-least-once re-delivery of the
same scheduled event cannot duplicate either receipt, while a later incident
with the same failure code remains distinguishable.

The event writer accepts an optional bounded `source` string. It preserves the
existing default for the Fly watchdog and records
`cloudflare-watchdog-supervisor` for the external supervisor. Dashboard events
therefore explain which monitor observed the failure.

## Boundaries

- The Worker receives a read-only Fly token, Telegram token/chat identifier,
  writer bearer token, and no Postgres, R2, Gamma, CLOB, worker, or wallet
  credential.
- The Worker only checks the alert Machine. The alert Machine remains the
  database-independent owner of all data-plane checks.
- KV is a transition-memory cache, not authority. A KV read/write failure is
  itself an unhealthy observation and pages immediately; no silent healthy
  default is allowed.
- Cloudflare cron is at-least-once and can propagate slowly after deployment;
  evidence begins only after a real scheduled healthy observation is stored.
- The Worker blocks public fetch routes. Scheduled execution is the only
  production entry point.

## Acceptance

1. Unit tests prove `started` plus unchanged restart count is healthy;
   stopped/missing/malformed/restart-increased states are unhealthy.
2. Unit tests prove duplicate scheduled invocations produce the same
   idempotency key and only health transitions invoke the notifier.
3. Writer tests prove an optional source is validated, stored, and defaults to
   the existing source when omitted.
4. Production proof stops the alert Machine once, observes a Cloudflare-origin
   Telegram page plus Dashboard `detected`, starts it, and observes matching
   `recovered`. The final 24-hour cloud-soak run begins only after this topology
   and recovery proof are stable.
