# Polymarket CLOB application-heartbeat repair

**Date:** 2026-07-24  
**Scope:** M1 Phase 05.4 Plan 05 recovery after rejected A5  
**Authorization:** the user delegated autonomous completion; this design does
not authorize trading, H-009, retention cleanup, or weaker soak criteria.

## Problem

The production CLOB market socket repeatedly changed generation. During those
windows the durable sampler truthfully captured only 2/10 or 7/10
current-generation book-evidenced tokens, permanently invalidating A5.

The client currently sets `websockets.connect(..., ping_interval=10)`. That
sends WebSocket protocol Ping control frames. Polymarket's market-channel
contract separately requires the application text message `PING` every ten
seconds and replies with the application text message `PONG`. The receive loop
currently tries to JSON-decode every text frame and has no application
heartbeat sender.

## Considered approaches

1. **Add the required application heartbeat at the connection boundary
   (selected).** Start one heartbeat task per live socket, send text `PING` on
   the documented cadence, ignore text `PONG` before JSON decoding, and cancel
   the task on every connection exit. Keep protocol Ping enabled as an
   independent transport-liveness probe.
2. **Delay or relax the sampler.** Rejected. A5 had incomplete generations for
   longer than one sample slot, so a short wait would not solve the transport
   defect. Ignoring reconnect-adjacent samples would weaken immutable strict
   evidence.
3. **Only add logging and retry A6.** Rejected. It improves diagnosis but leaves
   the documented heartbeat contract violated and risks wasting another
   24-hour window.

## Design

`stream_market_events` owns both the socket and its heartbeat lifecycle. After
the initial subscription succeeds it creates one task that sleeps for the
configured application-heartbeat interval and sends exactly `PING`. The
receive loop treats exactly `PONG` as transport metadata and yields no business
event. Every exit path cancels and awaits the task; cancellation of the parent
continues to propagate.

The existing `ping_interval_s` remains the WebSocket protocol Ping cadence.
A separate named application-heartbeat interval prevents the two protocols
from being conflated again.

Close diagnostics remain bounded and secret-free. The existing warning keeps
the received close code/reason in ephemeral logs. Durable reconnect events may
later carry only a normalized code/category if a separate failure proves it is
needed; this repair does not widen the schema or event allowlist before A6.

## Tests and acceptance

Tests must fail first and then prove:

- a live connection sends text `PING` on the application cadence;
- text `PONG` is consumed and never JSON-decoded or yielded;
- the heartbeat task is cancelled on normal close, abnormal close, and parent
  cancellation without swallowing `CancelledError`;
- existing reconnect initialization and market-frame behavior remain intact.

After focused and full local gates, deployment must use a newly approved exact
SHA, new Fly/DB boot identity, fresh readiness evidence, and an attempt-unique
A6 manifest/T0. A5 and its missing later checkpoint files remain permanently
NOT-CLOSED. No strict threshold changes.
