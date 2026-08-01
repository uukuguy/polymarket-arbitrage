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
- Added a per-window durable indexed `event_cursor + member_offset` checkpoint.
  Each invocation reads and parses at most `max_events` immutable event rows
  without a writer lock, then revalidates the window identity/cursor and commits
  at most `max_relationships` relation rows plus the next checkpoint in one
  bounded writer transaction.
- A newly collected window marks this migration complete in the same transaction
  as its final event page. Legacy complete windows advance one chunk and return a
  clean scheduler checkpoint before publication begins.
- `structure-generation-backfill` prioritizes this prerequisite and emits stable
  JSON. Event and relationship rows have independent equal hard budgets;
  `event_cursor + member_offset` resumes even inside one oversized event.
  Default `max_rows=500` requires at least 128 calls for 63,919 production
  events; the hard maximum 5,000 requires at least 13, with additional calls
  when nested relationships are more numerous.
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
publication-checkpoint result type. The final focused authority/bootstrap gate
covered 262 tests.

- Focused authority/bootstrap: `262 passed in 46.40s`.
- Full M1: `3032 passed, 1 skipped, 1 xfailed in 518.16s`.
- Changed-file Ruff: PASS.
- `git diff --check`: PASS.
- Documentation, planning status, and pre-commit gate are recorded after the
  exact staged revision.

## Legacy ordinal follow-up

Post-commit self-review found that the rolled-back legacy migration also left
event and market `source_ordinal` values NULL. The bounded certification cursor
previously serialized NULL, making the next tuple comparison match no rows and
silently skip the remainder. A precise two-event RED reproduced that false
completion. Source certification now keysets canonically by indexed `event_id`
or `market_id`; EXPLAIN proves index SEARCH without a temporary sort. The shared
checkpoint vocabulary includes source and comparison phases, complete staging
is database-frozen, progress binds the exact window checkpoint, and corrupt
source automatically rotates to a clean successor while preserving blocked
evidence. Status/strict health expose cursor, offset, age, and blocked reason.
