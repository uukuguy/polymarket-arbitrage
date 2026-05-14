---
phase: 02-l1-production-grade
plan: "03"
subsystem: database
tags: [supabase, postgresql, r2, s3, boto3, alembic, parquet, fail-soft, cloud-native]

requires:
  - phase: 02-l1-production-grade
    plan: "02"
    provides: Starlette daemon shell, SQLiteStore, /health endpoint, orchestrator step 7/7

provides:
  - SupabaseMirror: 镜像写入（upsert snapshots + insert markets_latest）fail-soft 模式
  - R2Sync: Parquet 上传 Cloudflare R2 fail-soft 模式
  - Alembic 001 初始 schema + RLS anon SELECT 策略
  - scripts/supabase_seed.py: reconcile + init_check CLI
  - Makefile targets: supabase-migrate, supabase-reconcile, r2-list
  - /health 新增 Check 3 (supabase mirror age) + Check 4 (r2 upload recency)
  - SQLiteStore 新增 update_snapshot_mirror_fields / get_snapshot / get_markets_for_snapshot

affects:
  - 02-l1-production-grade plan 04+ (orchestrator 完整 7+2 步骤基线)
  - m5-industrialize (Supabase 仪表盘依赖 markets_latest 表)
  - 运维 runbook (Alembic 迁移 + R2 bucket 创建流程)

tech-stack:
  added:
    - supabase-py (REST SDK, service_role 写入)
    - alembic 1.16 (Postgres schema 迁移)
    - boto3 + botocore (R2 S3-compatible 上传)
    - typer (scripts/supabase_seed.py CLI)
  patterns:
    - post-write fail-soft adapter (D-12 amendment): SQLite+Parquet 先写，mirror/upload 失败 → DEGRADED Issue，不中断快照
    - W6 双 URL 约定: POLYARB_SUPABASE_URL (REST SDK) vs POLYARB_SUPABASE_DB_DSN (Alembic Postgres DSN)
    - 3-point lockstep: snapshots 表 DDL / COLUMN_ORDER / INSERT_SQL 三常量同步维护
    - Stubber 测试模式: botocore.stub.Stubber 代替 mock，真实参数验证 put_object
    - pydantic frozen model auto-enable: object.__setattr__ 绕过 frozen 约束，凭证在场自动开启 mirror/r2_enabled

key-files:
  created:
    - src/polyarb/storage/supabase_mirror.py
    - src/polyarb/storage/r2_sync.py
    - alembic.ini
    - alembic/env.py
    - alembic/script.py.mako
    - alembic/versions/001_initial_dashboard_schema.py
    - scripts/supabase_seed.py
    - tests/m1-perception/test_supabase_mirror.py
    - tests/m1-perception/test_r2_sync.py
  modified:
    - src/polyarb/config.py (Supabase + R2 字段)
    - src/polyarb/storage/schemas.py (supabase_mirror_at_ms + parquet_r2_url 列)
    - src/polyarb/storage/sqlite_store.py (update_snapshot_mirror_fields 等)
    - src/polyarb/snapshot/orchestrator.py (step 7.5 + 7.6)
    - src/polyarb/http/health.py (Check 3 + Check 4)
    - src/polyarb/storage/__init__.py (新导出)
    - tests/m1-perception/conftest.py (mocked_supabase + mocked_r2_stubber fixtures)
    - tests/m1-perception/test_schema_lockstep.py (2 个新锁步断言)
    - tests/m1-perception/test_makefile_contract.py (3 个 dry-run 测试 + 1 个预存失败修复)
    - Makefile (3 个新 target)
    - .env.example (Supabase + R2 环境变量)

key-decisions:
  - "D-02/D-19 fail-soft: SupabaseMirror 和 R2Sync 失败不中断快照，返回 bool/抛 R2UploadError，orchestrator catch 降级为 DEGRADED Issue"
  - "W6 双 URL: supabase-py 用 POLYARB_SUPABASE_URL (HTTPS REST)，Alembic 用 POLYARB_SUPABASE_DB_DSN (PostgreSQL DSN)，两者不可互换"
  - "T-02-12 路径注入防御: compute_r2_key 仅接受 taken_at_ms: int，key 由 UTC datetime 生成，不含任何外部字符串拼接"
  - "NullPool for Alembic: pgbouncer 不支持 prepared statements，env.py 强制 NullPool 避免缓存冲突"
  - "put_object 代替 upload_file: upload_file 走 s3transfer 内部管道，botocore Stubber 无法拦截 —— 改用 put_object 直接调用"
  - "supabase_mirror_enabled / r2_enabled 自动推断: pydantic frozen model 用 object.__setattr__ 绕过，凭证在场则自动设为 True"

patterns-established:
  - "post-write fail-soft: 云端同步永远在 SQLite 写入成功后异步执行，失败只升级 health status 不回滚本地数据"
  - "project-typed exception (R2UploadError): 隔离 botocore 内部异常，orchestrator 无需感知 boto3 细节"
  - "3-point lockstep: 任何列变更必须同步修改 DDL + COLUMN_ORDER + INSERT_SQL，test_schema_lockstep.py 强制执行"
  - "Alembic offline-only migration: env.py run_migrations_offline 加载 DSN，不依赖 SQLAlchemy MetaData autodetect"

requirements-completed: []

duration: ~90min
completed: 2026-05-13
---

# Phase 02 Plan 03: Supabase Mirror + R2 Archive Summary

**Supabase Postgres 镜像（D-02/D-19 fail-soft upsert）+ Cloudflare R2 Parquet 归档（D-03 fail-soft put_object）接入 L1 快照 orchestrator，采用 TDD 全程驱动，测试套件从 429 扩展至 447 个用例全部通过**

## Performance

- **Duration:** ~90 min（含 RED → GREEN → Task 3 三轮 TDD 循环）
- **Started:** 2026-05-12T (context window 切换前)
- **Completed:** 2026-05-13T
- **Tasks:** 3（TDD RED / GREEN / Alembic+scripts+Makefile）
- **Files modified:** 22（9 新建 + 13 修改）

## Accomplishments

- `SupabaseMirror` 类实现 fail-soft push_snapshot（upsert snapshots + 批量 insert markets_latest）+ reconcile 对账接口，失败仅记录日志不抛出
- `R2Sync` 模块实现 fail-soft upload_parquet_to_r2（put_object + retry 3次）+ T-02-12 路径注入防御（compute_r2_key 纯 int → UTC 路径）
- Orchestrator step 7.5/7.6 接入两个适配器，post-write 顺序执行，任意失败升级为 DEGRADED Issue，cache.cleanup() 保持无条件执行
- Alembic 001 迁移文件创建 snapshots/markets_latest/recipe_runs 三张表 + anon SELECT RLS 策略
  - **Plan 02-08 amend (2026-05-14)**：top_movers_view 由 Plan 02-08 的 Alembic 002 补建（本 plan 实际只交付了 3 张表，未含 view；SUMMARY 原列 view 是文档偏差，F-03 retro fix-up 已补）
- scripts/supabase_seed.py typer CLI 提供 `reconcile` 和 `init_check` 两条命令
- /health 扩展 Check 3（supabase mirror age，componentType: datastore）和 Check 4（r2 upload recency，componentType: system）
- 修复一个预存失败：`test_make_snapshot_markets_full_dry_run_recipe` 期望值与实际 Makefile 命令不符

## Task Commits

1. **Task 1: TDD RED — 12 个失败测试** - `12faeea` (test)
2. **Task 2: GREEN — SupabaseMirror + R2Sync + orchestrator step 7.5/7.6** - `9977d57` (feat)
3. **Task 3: Alembic 001 + supabase_seed + Makefile targets** - `3e378dc` (feat)

**Plan metadata:** (本条 commit)

_TDD RED commit: 12 tests fail with ModuleNotFoundError before implementation. GREEN commit: 447 passed, 0 failures._

## Files Created/Modified

**新建：**
- `src/polyarb/storage/supabase_mirror.py` — SupabaseMirror 类：push_snapshot（upsert + 批量 insert）、reconcile 对账、_chunk 分片
- `src/polyarb/storage/r2_sync.py` — R2UploadError、compute_r2_key、upload_parquet_to_r2、_R2_RETRY_CONFIG
- `alembic.ini` — Alembic 配置，sqlalchemy.url 留空（env.py 动态加载 DSN）
- `alembic/env.py` — 加载 POLYARB_SUPABASE_DB_DSN，NullPool，offline-only 迁移
- `alembic/script.py.mako` — 标准 Alembic 模板
- `alembic/versions/001_initial_dashboard_schema.py` — snapshots/markets_latest/recipe_runs + anon SELECT RLS
- `scripts/supabase_seed.py` — typer CLI: reconcile + init_check
- `tests/m1-perception/test_supabase_mirror.py` — 6 个测试：写入/幂等/fail-soft/分片/reconcile/单例
- `tests/m1-perception/test_r2_sync.py` — 6 个测试：上传/key确定性/UTC-only/known-exception/retry/注入防御

**修改：**
- `src/polyarb/config.py` — Supabase 4 字段 + R2 5 字段 + auto-enable model_validator
- `src/polyarb/storage/schemas.py` — supabase_mirror_at_ms/parquet_r2_url 列 + 3-point lockstep 常量
- `src/polyarb/storage/sqlite_store.py` — update_snapshot_mirror_fields / get_snapshot / get_markets_for_snapshot
- `src/polyarb/snapshot/orchestrator.py` — step 7.5 (Supabase fail-soft) + step 7.6 (R2 fail-soft)
- `src/polyarb/http/health.py` — Check 3 (supabase mirror age) + Check 4 (r2 upload recency)
- `src/polyarb/storage/__init__.py` — 导出 SupabaseMirror/narrow_market_row/R2UploadError/compute_r2_key/upload_parquet_to_r2
- `tests/m1-perception/conftest.py` — mocked_supabase / mocked_r2_stubber / settings_with_supabase / settings_with_r2 fixtures
- `tests/m1-perception/test_schema_lockstep.py` — 2 个新锁步断言（supabase_mirror_at_ms + parquet_r2_url）
- `tests/m1-perception/test_makefile_contract.py` — 3 个新 dry-run 测试 + 修复预存失败
- `Makefile` — supabase-migrate / supabase-reconcile / r2-list targets
- `.env.example` — Supabase + R2 环境变量示例

## Decisions Made

1. **D-12 amendment 严格遵守** — Supabase mirror 和 R2 upload 均为 post-write，SQLite 写入成功是两者的前提。失败不回滚 SQLite，仅 Issue.DEGRADED 记录。
2. **W6 双 URL 约定** — POLYARB_SUPABASE_URL 供 supabase-py REST SDK 使用（以 `https://` 开头），POLYARB_SUPABASE_DB_DSN 供 Alembic 使用（以 `postgresql://` 开头）。两者字符串格式完全不同，不可复用。
3. **put_object 代替 upload_file** — botocore Stubber 在单元测试中无法拦截 upload_file（走 s3transfer 多线程管道），改为 put_object 直接调用后 Stubber 参数验证可以正常工作。
4. **compute_r2_key 纯 int 接口** — 函数签名仅接受 `taken_at_ms: int`，不接受任何字符串路径组件，根治 T-02-12 路径注入威胁（函数注解断言测试锁定）。
5. **NullPool for Alembic** — Supabase 默认走 pgbouncer transaction mode，不支持 prepared statements。env.py 显式传 NullPool 避免缓存冲突。

## Deviations from Plan

### Auto-fixed Issues

**1. [Rule 1 - Bug] 修复预存失败 test_make_snapshot_markets_full_dry_run_recipe**
- **Found during:** Task 1（TDD RED 阶段运行完整测试套件时发现）
- **Issue:** 测试期望 `python -m polyarb.snapshot --full`，但 Makefile 实际命令为 `uv run python -m polyarb.snapshot snapshot --full`（Plan 02 已修改 Makefile，测试未跟进）
- **Fix:** 更新测试断言匹配实际 Makefile 命令
- **Files modified:** `tests/m1-perception/test_makefile_contract.py`
- **Verification:** 429 → 430 基线（排除该预存失败），后续 GREEN 完成后 447 passed
- **Committed in:** `12faeea`（Task 1 RED commit 中随修复一起提交）

**2. [Rule 2 - Missing Critical] `from __future__ import annotations` 导致类型注解字符串化**
- **Found during:** Task 2（test_r2_key_rejects_user_input 断言 `ann.get("taken_at_ms") == int` 失败）
- **Issue:** Python 3.10+ PEP 563 行为：`from __future__ import annotations` 使 `int` 在注解字典中存储为字符串 `"int"` 而非 `int` 类型对象，导致 `== int` 比较失败
- **Fix:** 断言改为 `ann_val in (int, "int")` 兼容两种情况
- **Files modified:** `tests/m1-perception/test_r2_sync.py`
- **Verification:** test_r2_key_rejects_user_input 通过
- **Committed in:** `9977d57`（Task 2 GREEN commit）

---

**Total deviations:** 2 auto-fixed（1 bug, 1 missing critical）  
**Impact on plan:** 两个修复均属于正确性要求，无范围蔓延。

## Issues Encountered

1. **botocore Stubber 与 upload_file 不兼容** — upload_file 内部通过 s3transfer 的 TransferManager 发出请求，请求链路在 Stubber 之外，导致 Stubber 期望的请求未被触发而超时。解法：改用 put_object 直接调用（无需 s3transfer）。
2. **worktree 基准 commit 不一致** — 会话启动时 worktree 在旧 commit，需要 `git reset --hard aaa8d3e` 对齐到 Plan 02 结束点，之后 RED/GREEN 均基于正确基线执行。

### Post-deploy issues surfaced by Phase 1 调试期 verification (2026-05-13/14)

5 个 landing-time deviations were caught during real-environment verification
and addressed by **Plan 02-08 retro fix-up** (commits a055670 / 818e1df /
daabe30 / c327af6 / 46208b4):

- **F-01 (HIGH)**: `init_schema()` did not add the two new snapshots columns
  (supabase_mirror_at_ms / parquet_r2_url) to legacy DBs because
  CREATE TABLE IF NOT EXISTS is a no-op on existing tables. Plan 02-08 added
  a PRAGMA-driven idempotent ALTER TABLE ADD COLUMN pass.
- **F-02 (MEDIUM)**: `update_parquet_url` used upsert, which INSERTed a
  degenerate row when the snapshot didn't exist remotely (NOT NULL violations).
  Plan 02-08 changed it to a pure UPDATE().eq().
- **F-03 (LOW)**: `top_movers_view` was claimed as a deliverable but never
  built — Alembic 001 only created 3 tables. Plan 02-08 added Alembic 002.
- **F-04 (MEDIUM)**: daemon SIGINT shutdown took up to 10s (scheduler inner
  sleep granularity). Plan 02-08 dropped to 1s + added explicit
  scheduler_task.cancel() + bounded final gather timeout.
- **F-05 (MEDIUM)**: 0-market is_valid=False snapshots still triggered mirror
  push, polluting Supabase. Plan 02-08 added an is_valid guard at step 7.5.

See `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-08-SUMMARY.md`
for full retro fix-up details.

## User Setup Required

以下环境变量需在真实环境中配置（本地/CI 均无需，仅在实盘部署时配置）：

```bash
# Supabase（两个变量，用途不同）
POLYARB_SUPABASE_URL=https://<project-ref>.supabase.co
POLYARB_SUPABASE_DB_DSN=postgresql://postgres:<password>@db.<project-ref>.supabase.co:5432/postgres
POLYARB_SUPABASE_SERVICE_KEY=<service_role JWT>

# Cloudflare R2
POLYARB_R2_ENDPOINT=https://<account-id>.r2.cloudflarestorage.com
POLYARB_R2_ACCESS_KEY_ID=<r2-access-key>
POLYARB_R2_SECRET_ACCESS_KEY=<r2-secret-key>
POLYARB_R2_BUCKET=polyarb-snapshots
```

**首次部署步骤：**
1. 在 Supabase 控制台创建项目，获取上述三个值
2. 在 Cloudflare R2 创建 bucket `polyarb-snapshots`，生成 API token
3. 设置环境变量后运行 `make supabase-migrate` 初始化 schema
4. 运行 `make supabase-reconcile` 将本地 SQLite 历史数据同步至 Supabase
5. 运行 `uv run python scripts/supabase_seed.py init-check` 验证三张表可访问

## Next Phase Readiness

- orchestrator 7 + 2 步骤基线完整（SQLite → Parquet → Supabase mirror → R2 upload）
- /health 4-check 体系就位，cloud-native 健康监控可运行
- Plan 04+ 可接入实盘 API key 直接测试完整快照管道
- 待用户提供 Supabase + R2 凭证后 `make supabase-migrate` 一键初始化即可完成云端配置

---
*Phase: 02-l1-production-grade*
*Completed: 2026-05-13*
