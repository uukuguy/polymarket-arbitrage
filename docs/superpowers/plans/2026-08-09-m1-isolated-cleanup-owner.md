# M1 Isolated Cleanup Owner Repair

## Goal

Ensure enabled Structure-generation cleanup remains resident when snapshot
production is isolated into supervised subprocesses.

## Root cause

The parent daemon disabled the only cleanup worker whenever
`isolated_producers=true`, while health still required its durable heartbeat.
The child scheduler does not create that worker, leaving reclaimable generation
evidence unprocessed indefinitely.

## Repair and verification

The parent now owns cleanup whenever Structure sync and cleanup are enabled,
regardless of producer topology. A RED/GREEN wiring test proves isolated mode
creates the worker, while disabled sync still does not. Worker and health
regressions pass.
