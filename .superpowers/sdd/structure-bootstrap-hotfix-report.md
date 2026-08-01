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
- Fresh event pages only stage immutable raw events; they never expand nested
  relationships inside the page transaction. The final market-page transaction
  creates an incomplete bootstrap checkpoint bound to the exact complete-window
  identity. Fresh and legacy windows then use the same bounded parser before
  publication begins.
- `structure-generation-backfill` prioritizes this prerequisite and emits stable
  JSON. Event and relationship rows have independent equal hard budgets;
  `event_cursor + member_offset` resumes even inside one oversized event.
  Default `max_rows=500` requires at least 128 calls for 63,919 production
  events; the hard maximum 5,000 requires at least 13, with additional calls
  when nested relationships are more numerous.
- Invalid JSON/member shape or a raw event above the 1,000,000-byte hard limit
  records a durable blocked reason before bulk materialization, exits nonzero,
  and never advances the cursor or silently omits parent truth.
- Metadata-only streaming plus a 16,000,000-byte invocation budget prevents a
  high `max_rows` value from prefetching hundreds of megabytes of raw JSON.
- Recovery roots survive cursor-restart successors. Digest-authenticated
  rotation observations and same-transaction append-only recovery receipts are
  independent of purgeable staging windows, so retention cannot resurrect a
  resolved historical failure.
- The operator backfill command uses a 250ms writer timeout across schema,
  bootstrap, copy, certification, and comparison phases. Writer contention
  returns retryable exit-0 JSON (`deferred=true`, `writer-busy`, zero copied
  rows) instead of waiting behind the production writer and consuming Quote SLA.
  Admission recognizes only SQLite BUSY/LOCKED primary result codes. If a
  blocked-progress commit already happened and successor rotation then contends,
  the command exits 1 with `mutated=true` and `rotation_pending=true`; it never
  misreports that post-mutation state as a zero-write defer.
- The parent now strictly parses normalization/certification publication
  checkpoint JSON instead of reclassifying a committed child chunk as
  `snapshot-subprocess-invalid-json`.

Quote admission, the 45-second cooperative target, the 75-second child hard
limit, and the 15-second pointer deadline are unchanged. No deployment, config
switch, database mutation, or production restart was performed by this task.

## TDD and verification

Initial RED: 3 failures for the absent durable migration API/schema and absent
publication-checkpoint result type. The final operator-admission remediation
covered 293 expanded tests; the independent reviewer gate covered 97 tests.

- Expanded authority/bootstrap/operator admission: `293 passed`.
- Independent reviewer focused gate: `97 passed` plus Ruff.
- Full M1: `3063 passed, 1 skipped, 1 xfailed in 519.03s`.
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
evidence in an append-only digest-sealed observation. Status/strict health keep
that recovery fail visible until the successor finishes bootstrap, then retain
history without poisoning current health. The old `after_rowid` migration now
rewinds to the lexical beginning for idempotent bounded replay, preventing
scrambled rowid/event-id order from skipping truth.
