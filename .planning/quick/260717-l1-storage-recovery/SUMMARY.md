# L1 Storage Recovery Summary

## Outcome

- Production root cause: `/data` was 100% full; `/data/state.db` was 4.69GB with
  497 retained snapshots and no free SQLite pages. Snapshot fetch/validation passed,
  then persistence failed with `database or disk is full`.
- Operational recovery: extended Fly volume `vol_40olm80dgol2xqn4` from 5GB to
  15GB. The next deployment completed successfully and snapshot freshness recovered
  from roughly 39 days to under 3 minutes; Supabase mirror returned to pass.
- Durable correction: every successful scheduler tick now runs the existing 7-day /
  keep-last-5 purge on the app-owned mounted store via `asyncio.to_thread`.
- Diagnostic correction: a failed SQLite rollback no longer masks the original disk
  error with `cannot rollback - no transaction is active`.
- Storage inspection is bounded to snapshots/markets/scheduler_state; it no longer
  scans the multi-gigabyte event-tags table and times out over Fly SSH.

## Evidence

- Fly after extension: `/dev/vdc 15G`, 4.6G used, 9.5G available (33%).
- Production `/healthz`: top status `warn`, snapshot age 164s, last status `OK`,
  Supabase `pass`; only R2 archival remains `warn`.
- Focused regression: 38 scheduler/store/control/chaos tests passed.
- Targeted Ruff and `git diff --check`: passed.

## Follow-up

- Observe the first deployed app-owned retention run and confirm expired snapshot
  count drops while fresh snapshots continue.
- Repair R2 archival separately; it no longer blocks the usable M1→M2 paper path.
