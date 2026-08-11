# M1 high-write health projection budget

## Production finding

During active Structure member recovery, `/healthz` took 4.93 seconds and the
four-second internal health deadline returned `runtime:health_read_lane` P1.
The full projection performs several bounded (250ms) durable authority reads;
under writer pressure their serialized waits accumulate.  The timed-out worker
then finishes its remaining reads and temporarily occupies the single lane.

## Change

Set the full health projection budget to eight seconds.  Fly's service check
and resident Polywatch both use ten-second external deadlines, leaving a
bounded serialization/network margin while preserving explicit failure before
the external observer gives up.

## Verification

- Regression pins the health lane budget to 8.0 seconds.
- Existing saturated-lane, timeout-envelope, and registered-connection
  interruption tests remain green.
