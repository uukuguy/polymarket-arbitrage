# Structure writer-busy recovery retry

## Production finding

The Structure event-member sidecar persists a safe checkpoint when Quote owns
SQLite's writer.  Its `deferred=True` result was then returned as a completed
scheduler tick, causing the resident scheduler to wait its ordinary 300-second
cadence before trying again.  With Quote updates every minute, a large
membership recovery could therefore take hours despite durable forward progress.

## Change

Treat a `writer-busy` event-member checkpoint as a scheduler deferral rather
than a completed tick.  The existing `_tick` loop retries it after the existing
five-second `STRUCTURE_DEFER_RETRY_DELAY_S`; it remains bounded, preserves the
durable breadcrumb, and never holds or preempts Quote's write lease.

## Verification

- Regression: a deferred child followed by a sealed child is invoked twice in
  one `_tick`, separated by the five-second bounded defer delay.
- Existing event-member admission, lock-release, and sealing tests remain green.
