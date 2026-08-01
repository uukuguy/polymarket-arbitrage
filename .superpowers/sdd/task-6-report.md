# Structure Task 6 Report — Rollout Health and Bounded Evidence Cleanup

## Status

Implemented and locally verified with TDD. This task did not deploy, change the
production read mode, touch credentials, or introduce wallet/order authority.

## Delivered

- Added strict generation health for publication stage/cursor/checkpoint age,
  pointer identity and authenticated count/hash agreement, comparison
  receipt/progress, Quote-priority defer visibility, and retained evidence
  pressure/cleanup blockage.
- Added stable JSON operator commands and Make targets:
  `structure-generation-status`, `structure-generation-backfill`,
  `structure-generation-compare`, and `structure-generation-cleanup`.
  Status/compare are read-only; backfill advances one `max_rows` chunk; no
  command changes `structure_generation_read_mode`.
- Added durable bounded cleanup progress. Candidate ownership is fixed under
  `BEGIN IMMEDIATE` only after it is outside the current+rollback floor and its
  immutable publication/comparison proof authenticates. Each invocation
  deletes at most `max_rows` from exactly one component phase and can resume
  after store/process reopen.
- Frozen generation DELETE triggers permit only a matching active cleanup
  progress row. INSERT/UPDATE remain frozen. After all six bulk components are
  empty, cleanup atomically seals an append-only digest-bound receipt and
  removes progress. It never deletes the publication, comparison receipt,
  snapshot, sync window, or legacy proof skeleton.
- Exact generation reads reject both active cleanup and reclaimed generations,
  preventing an old reclaimed identity from being treated as a rollback target.
  Generic snapshot purge remains separate and never performs generation-chain
  reclamation.
- Updated the living M1 manual with the exact schema deploy → bounded backfill
  → compare PASS → generation mode → natural publication rollout, pointer
  switch semantics, explicit legacy rollback, health interpretation, and the
  bounded cleanup command.

## TDD evidence

Observed RED before implementation for missing store cleanup/status APIs, all
four Make/CLI surfaces, generation health projection, stable unavailable compare
JSON, and manual rollout/cleanup contracts.

GREEN evidence:

- Required health/Make/manual suites: all passed.
- Expanded generation publication/readers/store migration/schema/operator
  regression: 281 tests passed.
- Full M1 gate: **2984 passed, 1 skipped, 1 xfailed** in 668.83 seconds.
- `uv run ruff check src tests/m1-perception`: passed.
- `make docs-m1-check`: passed.
- `make planning-status`: no drift.
- `git diff --check`: passed.

## Remaining risk / Task 7 handoff

- No production schema migration, backfill, compare, read-mode switch, natural
  generation, cleanup, or rollback was run here. Task 7 owns exact-SHA production
  qualification.
- Cleanup intentionally preserves immutable proof skeleton metadata. Capacity
  health measures unreclaimed bulk generations; SQLite file shrink/VACUUM is a
  separate operator concern and is not performed automatically.

## Review hardening

The first independent review found no Critical issues and requested six related
root-cause corrections. They are now implemented as one coherent authority and
boundedness refinement:

- Status no longer shares the schema-initializing writer helper. It opens
  SQLite with `mode=ro` plus `query_only`; compare was already read-only. Trace
  contracts reject DDL/DML/repair, and a missing DB parent is not created.
- Health fails Quote-priority defer or active comparison progress beyond the
  Structure SLA. Status prefers next-generation active comparison progress over
  the sealed current receipt, while still fully authenticating the current
  pointer-bound receipt digest and identity.
- Initial cleanup authentication failures append an immutable digest-bound
  blocked observation without creating deletion authority. A later authenticated
  start appends an authorized observation, so health sees one stable latest
  state rather than a transient return value.
- Cleanup progress is single-slot, composite-bound to
  `(snapshot_id, publication_id)`, and receipt-digest authorized. Component
  DELETE triggers require that same pair, the exact component phase, no blocked
  reason, sealed receipt digest, complete count contract, validation hash, and
  accepted certification marker. Forged cross-publication, wrong-phase, and
  blocked rows cannot delete bulk evidence even through direct SQL.
- Published history uses partial composite indexes. Pressure is a bounded
  `fail_threshold + 1` probe with explicit lower-bound/exact semantics; retention
  floor and oldest candidate queries are index searches. EXPLAIN contracts reject
  table scans and temporary sort B-trees.
- First backfill no longer performs six source `COUNT(*)` scans or generation
  recounts after each chunk. Durable committed counts increment by each keyset
  chunk; each component seals its exact expected count only at cursor exhaustion;
  all six exhausted components then enter the existing bounded certification and
  comparison chain. Trace tests run the complete `max_rows=1` backfill and find
  no COUNT statement.
- The manual now classifies status/compare under daily read-only and
  backfill/cleanup under local mutation.

Review-fix verification:

- Reviewer reproducer and focused/expanded suites: all passed.
- Full M1 gate: **2997 passed, 1 skipped, 1 xfailed** in 522.59 seconds.
- Ruff, docs contract, planning no-drift, and diff checks passed.

## Second review hardening

The second review identified four Important authority/recovery gaps. They are
closed without deploying or changing production read mode:

- A missing comparison receipt is recoverable only for the exact pre-Task5
  state: the current pointer receipt digest is NULL, its validation hash/counts/
  certification facts still agree with the publication, and that same
  generation has active bounded comparison progress. Health warns only while
  that repair checkpoint is inside the Structure SLA and fails after it; digest
  or identity mismatches remain fail-closed.
- Cleanup retention is now a database invariant, not only an API selection
  rule. A progress INSERT rejects the current generation and either member of
  the fixed current+rollback floor. Every component DELETE independently
  rechecks the same invariant, including two newer complete, certified,
  unreclaimed publications, so authorization cannot outlive a later floor
  change.
- Active comparison lookup has a partial checkpoint/publication index and a
  bounded non-negative checkpoint predicate. Its EXPLAIN contract is an index
  SEARCH with no table scan or temporary B-tree.
- The pre-composite cleanup migration no longer silently drops diagnostics.
  Existing blocked progress becomes an append-only blocked observation with
  its original reason; invalid snapshot/publication binding becomes an explicit
  `cleanup-progress-migration-invalid-binding` observation. Only the newest
  valid unblocked row can acquire the single progress slot.

Second-review verification:

- Focused four-gap regression: 7 tests passed.
- Expanded publication/readers/health/migration/schema regression: passed.
- Full M1 gate: **3003 passed, 1 skipped, 1 xfailed** in 508.59 seconds.

## Final identity-binding review

The final narrow review found that the pre-Task5 missing-receipt exception
looked up repair progress by generation alone. It is now fail-closed unless one
indexed lookup matches both the current pointer's `generation_snapshot_id` and
`publication_id`. The same O(1) check also proves that the serialized SHA-256
state and phase cursor are parseable and structurally resumable, including all
prior phase hashes required by the active phase. A valid publication belonging
to another generation cannot authorize a warning.

Final-review verification:

- Exact wrong-publication tamper reproduced RED, then passed GREEN.
- Malformed digest-state and cursor-state cases fail closed; the valid same-pair
  case remains warn inside SLA and fail after SLA.
- Pointer-repair EXPLAIN uses an index SEARCH with no scan or temporary B-tree.
- Focused and expanded generation/health/migration/schema suites passed.
- Full M1 gate: **3006 passed, 1 skipped, 1 xfailed** in 507.89 seconds.
- Ruff, docs contract, planning no-drift, and diff checks passed.

## Final resumability-field review

The remaining two persisted field families are now part of the same fail-closed
repair authority. `phase_row_count` must be an actual non-negative Python
integer as returned by SQLite integer storage; booleans, floats, strings, and
negative integers are rejected. The indexed pointer-repair query also reads its
pinned legacy snapshot id, taken/finished timestamps, and market count. Inside
one explicit read-only transaction, that tuple must exactly equal the current
eligible legacy identity. Parse failure or drift disables the recoverable
missing-receipt exception, so health fails while the next backfill still exposes
the underlying error instead of being masked by a warning.

Resumability-field verification:

- Exact `phase_row_count='oops'` and `legacy_taken_at_ms=9999` tampers reproduced
  RED, then passed GREEN while their next backfill attempts still raised.
- Bool/float/string/negative count variants fail the resumability predicate.
- Valid repair, SLA warn/fail behavior, and indexed SEARCH/no-temp-sort query
  plan remain green.
- Focused and expanded generation/health/migration/schema suites passed.
- Full M1 gate: **3013 passed, 1 skipped, 1 xfailed** in 510.64 seconds.
