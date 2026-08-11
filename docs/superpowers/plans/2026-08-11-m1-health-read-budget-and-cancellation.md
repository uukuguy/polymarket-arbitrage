# M1 health read budget and cancellation

## Observed production failure

During Structure bootstrap writes, a full `/healthz` projection took about
three seconds. The old 0.8-second budget emitted `read-model-unavailable` P1
even though the same bounded read completed shortly after. Because the sole
read lane remained occupied, subsequent requests also emitted saturation P1s.

The original cancellation class maintained a connection registry, but the
largest Structure generation status query never registered its SQLite handle.
Its deadline could therefore not interrupt that query.

## Repair

- Use a four-second full-projection budget: enough for the measured normal
  write-pressure path, still below the watcher HTTP timeout.
- Register the Structure generation status connection with the request
  deadline and give that health-only read a 250ms SQLite busy timeout.
- Preserve fail-closed `runtime:health_read_lane` P1 output for an actual
  timeout or saturated lane.

## Production acceptance

1. Current release health returns a complete check map rather than a
   self-generated read-lane P1 during normal Structure checkpointing.
2. A true blocked generation read is interrupted and releases the lane.
3. Polywatch reports the real Structure incident, not a missing-health-model
   surrogate.
