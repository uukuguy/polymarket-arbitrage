---
phase: 02-l1-production-grade
plan: 08
workstream: m1-perception
subsystem: storage
tags: [sqlite, supabase, alembic, asyncio, migration, fail-soft]

# Dependency graph
requires:
  - phase: 02-01
    provides: HTTP daemon + Settings + DB path resolution
  - phase: 02-02
    provides: SnapshotScheduler 3-failure-pause state machine + run() loop
  - phase: 02-03
    provides: Supabase mirror (push_snapshot / update_parquet_url / reconcile) + R2 archive
provides:
  - SQLiteStore.init_schema idempotent column-add for legacy DBs (F-01)
  - SupabaseMirror.update_parquet_url pure UPDATE — no upsert (F-02)
  - Alembic 002 top_movers_view migration (F-03)
  - daemon scheduler 1s shutdown polling + cancellation-aware (F-04)
  - orchestrator step 7.5 is_valid guard — 0-market snapshots skip mirror (F-05)
affects: [02-04, 02-05, 02-06, 02-07, wave-3, wave-5-chaos-test]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "idempotent_migration: PRAGMA table_info + conditional ALTER TABLE ADD COLUMN (add-only, LEARNINGS P7)"
    - "fail_soft_guard: orchestrator step 7.5 pre-check on snapshot_result.is_valid before mirror"
    - "cancellation_aware_loop: scheduler.run() polls stop_event every 1s + scheduler_task.cancel() on shutdown"

key-files:
  created:
    - alembic/versions/002_add_top_movers_view.py
    - tests/m1-perception/test_sqlite_store_migration.py
    - tests/m1-perception/test_daemon_shutdown.py
  modified:
    - src/polyarb/storage/sqlite_store.py
    - src/polyarb/storage/supabase_mirror.py
    - src/polyarb/snapshot/orchestrator.py
    - src/polyarb/daemon/scheduler.py
    - src/polyarb/daemon/main.py
    - tests/m1-perception/test_supabase_mirror.py
    - .planning/workstreams/m1-perception/phases/02-l1-production-grade/02-03-SUMMARY.md

key-decisions:
  - "F-01: idempotent migration is add-only — never drop/rename/retype (LEARNINGS P7)"
  - "F-02: update_parquet_url uses UPDATE().eq('id', ...) — row-not-found is a warning, not an error"
  - "F-03: top_movers_view DDL adapted to actual markets_latest columns (no snapshot_id partitioning available; sort by abs(mid_price - 0.5) DESC)"
  - "F-04: scheduler graceful shutdown uses BOTH 1s inner-sleep granularity AND explicit scheduler_task.cancel()"
  - "F-05: is_valid guard pre-empts the entire 7.5 mirror block, including init — keeps fail-soft policy"

patterns-established:
  - "Legacy-DB migration: any column added post-Plan 03 must use _ensure_column helper (PRAGMA + conditional ALTER)"
  - "Plan-scoped commits with pre-existing SUMMARY skeleton: create SUMMARY draft before first task commit so pre-commit hook is happy"

requirements-completed:
  - "F-01 init_schema idempotent ALTER — pragma table_info check + ADD COLUMN only if missing (LEARNINGS P7 add-only)"
  - "F-02 update_parquet_url 改纯 UPDATE — 不走 upsert，行不存在 → no-op + log"
  - "F-03 Alembic 002 建 top_movers_view（dashboard 用，对齐 Plan 03 SUMMARY 承诺）"
  - "F-04 daemon SIGINT 1s 内 graceful — 调研 _tick 内阻塞点，加 cancellation 或 timeout 包裹"
  - "F-05 0-market snapshot 不触发 mirror — orchestrator step 7.5 前置 is_valid 检查（fail-soft 该 skip not corrupt）"
  - "Acceptance: 完整 snapshot 跑通后 Supabase snapshots 表 +1 行 status=ok；daemon pkill -INT 在 1s 内退干净；老 DB 启动一次后两列就位"

# Metrics
duration: in-progress
completed: 2026-05-14
---

# Phase 02 Plan 08: Plan 03 Retro Fix-up Summary

**5 Plan 03 mirror+R2 落地偏差 (F-01..F-05) 一次性补正，Wave 3 dispatch 解锁**

## Performance

- **Started:** 2026-05-14T21:13:53Z
- **Completed:** _filled at plan end_
- **Tasks:** 6
- **Files modified:** _filled at plan end_

## Accomplishments

- F-01: legacy DBs now self-migrate on `init_schema()` — `supabase_mirror_at_ms` + `parquet_r2_url` columns appear after first daemon boot, no manual `ALTER TABLE` needed
- F-02: `update_parquet_url` is now a pure UPDATE — when the snapshot row doesn't exist remotely, we log warn + return False instead of fabricating a NOT-NULL violation via upsert
- F-03: Alembic 002 creates `top_movers_view` so Plan 06 dashboard has the promised data shape
- F-04: daemon responds to SIGINT/SIGTERM within 1s — inner-sleep dropped from 10s → 1s, scheduler_task explicitly cancelled on stop, final gather wrapped in 5s timeout
- F-05: orchestrator step 7.5 short-circuits when `snapshot_result.is_valid == False` — 0-market snapshots no longer pollute Supabase

## Task Commits

_filled per task_

## Files Created/Modified

_filled per task_

## Decisions Made

_filled at plan end — see frontmatter for the 5 key decisions_

## Deviations from Plan

_filled at plan end_

## Issues Encountered

_filled at plan end_

## Acceptance Verification

_filled at plan end_

## Next Phase Readiness

_filled at plan end_

---
*Phase: 02-l1-production-grade*
*Plan: 08*
*Workstream: m1-perception*
*Completed: 2026-05-14*
