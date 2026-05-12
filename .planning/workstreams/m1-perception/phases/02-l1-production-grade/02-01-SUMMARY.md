---
phase: 02-l1-production-grade
plan: "01"
subsystem: database
tags: [pyarrow, sqlite, parquet, schema-evolution, makefile]

# Dependency graph
requires:
  - phase: 01.1-observation-toolkit
    provides: "4-point lockstep schema invariant (DDL/MARKETS_COLUMN_ORDER/MARKETS_INSERT_SQL/SNAPSHOT_SCHEMA), GammaClient._paginate pagination loop, normalizer.normalize_market, sqlite_store._row_to_tuple"
provides:
  - "page_fetched_at_ms nullable INTEGER column in markets + events DDL (修 L2 fetched_at_ms 语义误导)"
  - "4-point lockstep schema invariant extended to 23 columns for markets (was 22)"
  - "3-point lockstep for events extended to 12 columns (was 11)"
  - "GammaClient._paginate 每页注入 _page_fetched_at_ms 时间戳"
  - "normalizer.normalize_market + normalize_events 透传 _page_fetched_at_ms → page_fetched_at_ms"
  - "make triple-check 全链路三重契约门 (exit 0 ↔ SQLite +1 ↔ parquet 落地 ↔ 行数一致)"
  - "test_parquet_sqlite_consistency.py D-12 双源一致性测试"
  - "Wave 0 tests for page_fetched_at_ms (RED→GREEN TDD cycle)"
affects:
  - 02-02: "http server / scheduler 会读 markets.page_fetched_at_ms"
  - 02-03: "Supabase mirror 会同步 page_fetched_at_ms 列"
  - 02-07: "Chaos 测试会用到 page_fetched_at_ms 不为 NULL 的断言"

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "schema-evolution: add-only column (per LEARNINGS P7) — page_fetched_at_ms 加列而非重命名 fetched_at_ms"
    - "per-page stamp 注入: _paginate 完成一页后立即注入 _page_fetched_at_ms 私有键"
    - "triple-check gate: bash 脚本 4 个门 + exit 77 graceful skip (autotools 惯例)"
    - "4-point lockstep: DDL / COLUMN_ORDER / INSERT_SQL / SNAPSHOT_SCHEMA 四点同步"

key-files:
  created:
    - tests/m1-perception/test_parquet_sqlite_consistency.py
    - tests/m1-perception/test_makefile_triple_check.sh
  modified:
    - src/polyarb/storage/schemas.py
    - src/polyarb/snapshot/normalizer.py
    - src/polyarb/clients/gamma_client.py
    - tests/m1-perception/test_schema_lockstep.py
    - tests/m1-perception/test_normalizer.py
    - tests/m1-perception/test_makefile_contract.py
    - Makefile

key-decisions:
  - "page_fetched_at_ms 加列而非重命名 fetched_at_ms (P7 schema evolution add-only constraint)"
  - "SNAPSHOT_SCHEMA 加 pa.int64() nullable=True (backward compat — 旧 parquet via union_by_name=true NULL 填充)"
  - "triple-check 在 DB 不存在时 exit 77 skip (合理 — Plan 04 会用 fixture 目录硬化)"
  - "test_parquet_sqlite_consistency.py 在 Task 1 时已经 GREEN — D-12 dual-source 本来已经正确，该测试是防回归 sentinel"

patterns-established:
  - "per-page stamp: GammaClient._paginate 完成一页 → 立即 stamp _page_fetched_at_ms → normalizer 透传"
  - "semantic comment: fetched_at_ms 旁边加注释说明 stage stamp 语义避免 L2 类误解"

requirements-completed:
  - "framework abstraction A — minimal page_fetched_at_ms add-only column (RESEARCH §5)"
  - "L11 silent failure prevention — make snapshot-markets triple check (LEARNINGS L11, S5)"
  - "D-12 parquet/SQLite dual validation"

# Metrics
duration: 45min
completed: 2026-05-12
---

# Phase 02 Plan 01: page_fetched_at_ms 4-point lockstep + Makefile triple-check gate

**为 markets/events 表加 page_fetched_at_ms nullable 列（修 L2 fetched_at_ms 语义误导），4-point schema lockstep + GammaClient 每页时间戳注入 + make triple-check L11/S5 沉默失败防护门**

## Performance

- **Duration:** ~45 min
- **Started:** 2026-05-12T (session start)
- **Completed:** 2026-05-12
- **Tasks:** 3 (TDD RED → GREEN → triple-check gate)
- **Files modified:** 9

## Accomplishments

- Phase 01.1 L2 bug 修复：`fetched_at_ms` 是 stage 5 完成时戳（同一快照所有行相同），新列 `page_fetched_at_ms` 记录真正的每页抓取时间（nullable，向后兼容旧 parquet）
- 4-point schema lockstep 完整落地（markets DDL / MARKETS_COLUMN_ORDER / MARKETS_INSERT_SQL / SNAPSHOT_SCHEMA），events 3-point lockstep（无 parquet schema）
- `GammaClient._paginate` 每完成一页后注入 `_page_fetched_at_ms` 私有键，`normalize_market` / `normalize_events` 透传为 `page_fetched_at_ms`
- `make triple-check` 全链路三重契约门：exit 0 ↔ SQLite 快照行 +1 ↔ parquet 文件 +1 ↔ 两侧行数一致
- 404 测试通过（Phase 01.1 基线 402 + 2 新测试），1 个预存在的不相关失败（`test_make_snapshot_markets_full_dry_run_recipe` 期望 `python -m polyarb.snapshot --full` 但 Makefile 用 `uv run python -m polyarb.snapshot snapshot --full`）

## Task Commits

1. **Task 1: Wave 0 RED state tests** - `cecb66b` (test)
2. **Task 2: page_fetched_at_ms implementation GREEN** - `5da55dc` (feat)
3. **Task 3: Makefile triple-check gate + SUMMARY** - (feat)

## Files Created/Modified

- `src/polyarb/storage/schemas.py` — 4-point lockstep markets + 3-point events；Phase 02 语义注释；SNAPSHOT_SCHEMA 加 pa.int64() nullable=True field
- `src/polyarb/clients/gamma_client.py` — `import time`；`_paginate` 注入 `_page_fetched_at_ms` 私有键
- `src/polyarb/snapshot/normalizer.py` — `normalize_market` + `normalize_events` 透传 `page_fetched_at_ms`
- `tests/m1-perception/test_schema_lockstep.py` — 新增 `test_page_fetched_at_ms_in_all_four_sync_points`；更新列计数（22→23）；修复 EVENTS_INSERT_SQL 参数（11→12）
- `tests/m1-perception/test_normalizer.py` — `EXPECTED_KEYS` 加 `page_fetched_at_ms`；新增 `test_page_fetched_at_ms_carried_from_raw`
- `tests/m1-perception/test_parquet_sqlite_consistency.py` — 新文件：D-12 双源一致性测试
- `tests/m1-perception/test_makefile_triple_check.sh` — 新文件：可执行 bash 三重契约门（exit 0/1/77）
- `tests/m1-perception/test_makefile_contract.py` — 新增 `test_make_triple_check_dry_run_recipe`
- `Makefile` — 新增 `triple-check` target（.PHONY + ## 注释 + echo 头 + recipe）

## Decisions Made

1. **page_fetched_at_ms 加列而非重命名 fetched_at_ms**：严格遵守 LEARNINGS P7 schema evolution add-only 约束。旧 parquet 通过 DuckDB `union_by_name=true` 自动 NULL 填充新列。
2. **SNAPSHOT_SCHEMA nullable=True**：backward compat 关键。旧快照没有 page_fetched_at_ms，读取时 NULL 填充不报错。
3. **triple-check exit 77 graceful skip**：工作树（worktree）内没有 live `data/state.db`，这是预期行为。Plan 04 会用 fixture 目录硬化 shell 级测试路径。
4. **test_parquet_sqlite_consistency.py 在 Task 1 就是 GREEN**：这是正确的——D-12 dual-source consistency 在 Phase 01.1 pipeline 已经满足。该测试是防回归 sentinel，不是 RED 驱动测试。

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 修复 test_lockstep 中 DDL regex 匹配到注释行而非列定义行**
- **Found during:** Task 2 (running Wave 0 tests after implementation)
- **Issue:** `test_page_fetched_at_ms_in_all_four_sync_points` 用 regex 匹配 `page_fetched_at_ms\s+(\w+)` 时命中了 DDL 注释行（`-- Semantic note... for page_fetched_at_ms`），导致 `group(1)` 返回 `"for"` 而非 `"INTEGER"`
- **Fix:** 改进正则匹配逻辑：先过滤掉以 `--` 开头的注释行，再在 non-comment body 上用 `re.MULTILINE` 匹配列定义
- **Files modified:** `tests/m1-perception/test_schema_lockstep.py`
- **Verification:** Test passes GREEN
- **Committed in:** 5da55dc (Task 2 commit)

**2. [Rule 1 - Bug] 更新 test_schema_lockstep.py 中 EVENTS_INSERT_SQL hardcoded 参数数量**
- **Found during:** Task 2 (running full lockstep test suite after schema changes)
- **Issue:** `test_events_composite_primary_key` 用 hardcoded 11-tuple 调用 EVENTS_INSERT_SQL，但添加 `page_fetched_at_ms` 后 SQL 现在需要 12 个参数
- **Fix:** 更新测试中所有三个 `con.execute(EVENTS_INSERT_SQL, (...))` 调用，在 `fetched_at_ms` 后插入 `None` 作为 `page_fetched_at_ms`
- **Files modified:** `tests/m1-perception/test_schema_lockstep.py`
- **Verification:** All 17 lockstep tests pass
- **Committed in:** 5da55dc (Task 2 commit)

**3. [Rule 1 - Bug] 更新 test_normalizer.py EXPECTED_KEYS 和列计数测试**
- **Found during:** Task 2 (running full normalizer tests)
- **Issue:** `EXPECTED_KEYS` 不包含 `page_fetched_at_ms`，导致 `test_normalize_happy_path` 失败；`test_markets_column_count_is_22_after_amendment_01` 列计数过时（22 → 23）
- **Fix:** 在 `EXPECTED_KEYS` 加 `page_fetched_at_ms`；更新列计数测试名称和断言值
- **Files modified:** `tests/m1-perception/test_normalizer.py`, `tests/m1-perception/test_schema_lockstep.py`
- **Verification:** All 28 normalizer tests, all 17+ lockstep tests pass
- **Committed in:** 5da55dc (Task 2 commit)

---

**Total deviations:** 3 auto-fixed (all Rule 1 — bugs in test assertions exposed by schema change)
**Impact on plan:** 所有 auto-fix 都是因为加列必然触发的连锁测试更新，无 scope creep。

## Known Stubs

无 — 所有 page_fetched_at_ms 数据路径（GammaClient → normalizer → SQLite / Parquet）均已完整打通。没有 hardcoded NULL 或 placeholder 值流向 UI 渲染层。

## Issues Encountered

**预存在的测试失败（非本 plan 引入）**：`test_make_snapshot_markets_full_dry_run_recipe` 在本 plan 之前就已失败（已通过 git stash 验证）。该测试期望 `python -m polyarb.snapshot --full`，但 Makefile 实际使用 `uv run python -m polyarb.snapshot snapshot --full`。属于 Phase 01.1 的遗留问题，建议在 Plan 03 或独立修复。

## Follow-up TODOs

1. **triple-check fixture 硬化（Plan 04）**：`test_makefile_triple_check.sh` 在没有 live `data/state.db` 时 exit 77 skip。Plan 04（Dockerfile + 生产环境部署）时需用专用 fixture 目录让 shell 级测试可以真正执行（exit 0 全通）
2. **pre-existing test fix**：`test_make_snapshot_markets_full_dry_run_recipe` 字符串匹配需更新为 `uv run python -m polyarb.snapshot snapshot --full`
3. **markets.page_fetched_at_ms backward migration**：现有 `data/state.db` 如果需要使用，需要 `ALTER TABLE markets ADD COLUMN page_fetched_at_ms INTEGER;` + 同样的 events 迁移。Plan 03 Alembic migration 会覆盖这一需求。

## Next Phase Readiness

Plan 02（HTTP server + scheduler）可以直接基于 `page_fetched_at_ms` 列构建 `/health` endpoint 的多源状态判断。schema 变更已经向后兼容，旧快照不会破坏读取。

---
*Phase: 02-l1-production-grade*
*Completed: 2026-05-12*

## Self-Check: PASSED

All created files exist on disk. All 3 task commits (cecb66b, 5da55dc, 65730a3) confirmed in git log.
