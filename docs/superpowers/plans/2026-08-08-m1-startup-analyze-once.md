# M1 Startup Statistics Repair

## Goal

Prevent every M1 daemon restart from rescanning the full drift indexes before
the health server and scheduler can start.

## Change

`SQLiteStore.init_schema()` checks for the planner-statistics row of each drift
index. It runs `ANALYZE` only when that row is absent (new or rebuilt index),
not on ordinary restarts. Indexes and query plans are unchanged.

## Verification

- RED: a second schema initialization reissued both `ANALYZE` statements.
- GREEN: with persisted index statistics, the restart emits no `ANALYZE`.
- Run schema-lockstep and health regression suites, Ruff, and planning audit.
