# M1 Structure Market Ordinal Index

**Goal:** Remove the O(active-window) query from each transactional Gamma market
page commit so a growing recovery window cannot exhaust the child watchdog.

**Root cause evidence:** Production window `bf8cb511…` had 104,600 staged
markets. `SELECT MAX(source_ordinal) ... WHERE window_id=?` used only the
primary key `(window_id, market_id)` and took 15.625 seconds. The query runs
after `BEGIN IMMEDIATE`, so it monopolized the writer long enough for a normal
multi-page child to reach its 75-second watchdog.

**Design:** Add `idx_structure_sync_market_ordinal(window_id, source_ordinal
DESC)` through the idempotent schema DDL. The existing transactional ordinal
assignment and cursor contract are unchanged; only the query access path gains
a covering suffix ordered for `MAX`.

**Verification:** The schema test asserts `EXPLAIN QUERY PLAN` names the new
ordinal index for the exact `MAX(source_ordinal)` query.
