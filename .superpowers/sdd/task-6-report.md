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
