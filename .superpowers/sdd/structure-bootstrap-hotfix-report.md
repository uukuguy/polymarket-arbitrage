# Structure Bootstrap Hotfix Report

## Production root cause

Read-only production evidence on `polyarb-l1` showed a 22,174,773,248-byte
SQLite database, 63,919 staged events, 471,401 staged markets, a complete
Structure window, zero event-market relation rows, and no publication. Attempts
480–482 all timed out at the exact 75-second child limit with `last_stage=NULL`.

`init_structure_sync_schema()` unconditionally updated every legacy staging row
and expanded every event JSON through `json_each()` in one transaction. The hard
kill rolled that transaction back, so every retry restarted the same scan from
zero and incremented the failure counter.

## Fix

- Removed the unbounded staging scan from child schema initialization.
- Added a per-window durable `after_rowid` checkpoint. Each invocation reads and
  parses at most `max_events` immutable event rows without a writer lock, then
  revalidates the cursor and commits relationship rows plus the next checkpoint
  in one bounded writer transaction.
- A newly collected window marks this migration complete in the same transaction
  as its final event page. Legacy complete windows advance one chunk and return a
  clean scheduler checkpoint before publication begins.
- `structure-generation-backfill` prioritizes this prerequisite and emits stable
  JSON. Default `max_rows=500` means at most 128 calls for 63,919 production
  events; the hard maximum 5,000 means at most 13 operator calls.
- Invalid JSON or member shape records a durable blocked reason, exits nonzero,
  and never advances the cursor or silently omits parent truth.
- The parent now strictly parses normalization/certification publication
  checkpoint JSON instead of reclassifying a committed child chunk as
  `snapshot-subprocess-invalid-json`.

Quote admission, the 45-second cooperative target, the 75-second child hard
limit, and the 15-second pointer deadline are unchanged. No deployment, config
switch, database mutation, or production restart was performed by this task.

## TDD and verification

Initial RED: 3 failures for the absent durable migration API/schema and absent
publication-checkpoint result type. Focused GREEN expanded to 249 tests.

- Full M1: `3019 passed, 1 skipped, 1 xfailed in 512.12s`.
- Changed-file Ruff: PASS.
- `git diff --check`: PASS.
- Documentation, planning status, and pre-commit gate are recorded after the
  exact staged revision.
