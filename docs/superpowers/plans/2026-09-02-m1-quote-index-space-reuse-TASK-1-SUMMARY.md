# M1 Quote index space reuse — Task 1 Summary

## Outcome

Each successful Quote publication now performs ordinary PostgreSQL maintenance
after its pointer-gated retirement, so pages from the previous dashboard index
become reusable by the next Quote generation. This prevents Free-tier database
growth from accumulating one full Quote index per refresh.

## Implementation

- Added `PostgresControlPlane.reuse_quote_research_space()`, which runs
  `VACUUM (ANALYZE)` for `m1_business_quote_rows` on a temporary autocommit
  connection and restores the connection's pool setting afterwards.
- Quote certification invokes that maintenance only after its atomic
  publication transaction has succeeded.
- A maintenance failure cannot revoke the newly published generation or retry
  its terminal certification; the worker emits the explicit
  `certified:space-reuse-pending` outcome for the scheduler and operations
  surface.
- This intentionally uses neither `VACUUM FULL` nor a table rewrite. Emergency
  full compaction remains a separately controlled, write-lane-paused operation.

## Verification

- Focused TDD coverage proves publication precedes space reuse and that a
  reuse failure leaves the published generation available.
- A control-plane contract test proves the vacuum executes in autocommit mode
  and restores the connection mode before returning it to the pool.
- Full Quote worker regression suite and targeted PostgreSQL contract test pass.

## Production follow-up

Deploy the coordinator image before resuming recurring Quote generations. On
the next publication, verify the coordinator outcome is `certified`, capacity
does not accumulate another historical Quote index, and an open maintenance
outcome is visible if the provider rejects vacuum maintenance.
