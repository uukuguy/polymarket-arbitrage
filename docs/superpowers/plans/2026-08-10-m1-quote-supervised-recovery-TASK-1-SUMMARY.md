# M1 Quote Supervised Recovery — Task 1 Summary

Date: 2026-08-10

## Delivered

`QuoteWorker` now has an opt-in `stop_after_consecutive_timeouts` boundary.
After recording the normal durable timeout evidence and cleaning failed payloads,
it stops cleanly when that threshold is reached. The default remains unchanged:
un-supervised workers retain immediate retry behavior.

## Why

The production Quote worker previously retried hard timeouts indefinitely in
one process. This boundary allows the upcoming outer `ProducerSupervisor` to
own bounded restart, backoff, receipts and escalation without creating
overlapping CLOB collectors.

## Verification

- RED: the new supervisor-boundary test failed because the constructor lacked
  the option.
- GREEN: three forced timeouts stop exactly after attempt three; existing
  immediate retry and failed-attempt identity tests also pass.
- Ruff passes for the changed implementation and test.

## Next

Add the `quote` worker CLI component and connect it to the supervisor; only
then enable the threshold in the isolated production topology.
