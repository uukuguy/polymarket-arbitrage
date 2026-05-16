---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
current_phase: 02
status: executing
stopped_at: "SESSION 19 EOD 2026-05-16: Plan 02-09 streaming-paginator landed (5 commits via worktree merge 9901bf9) + 1GB Fly VM scale (1f324f4) + T7 docs/learning/08 + thread §2.8 + 02-09-SUMMARY (f041cc4) + Wave 4 SaaS prep guide (9fd0306). prod /health=pass, snapshot id=6 OK 6753 markets. OOM resolved, worktrees cleaned. Wave 4 gated on user finishing docs/setup/04-wave4-observability-saas-prep.md (Sentry/Axiom/Better Stack/Telegram, all free tier, ~30-40 min)."
last_updated: "2026-05-16T11:45:00.000Z"
last_activity: 2026-05-16
progress:
  total_phases: 4
  completed_phases: 2
  total_plans: 20
  completed_plans: 17
  percent: 85
---

# Project State

## Current Position

Phase: 02 (l1-production-grade) — EXECUTING
Plan: 6 of 9 ✅ (Wave 1+2+2.5+3+3.5 complete; Wave 4/5 pending)
**Status:** Wave 3.5 complete — prod stable on 1GB Fly VM, OOM resolved
**Current Phase:** 02
**Last Activity:** 2026-05-16
**Last Activity Description:** SESSION 19 — Plan 02-09 (streaming paginator + 1GB scale) merged. Empirical: Linux daemon peak anon-rss = 402MB; 256MB/512MB OOM, 1GB stable. SQLite snapshots id reached 5 (6729 markets, is_valid=1), /health overall=pass with all 3 component checks pass (snapshot:last_status / supabase:mirror / r2:upload). Plan 02-09 architecture (streaming) + 1GB scaling jointly required — neither alone sufficient.

## Progress

**Phases Complete:** 2 (Phase 01 + Phase 01.1)
**Phase 01.1 status:** ✅ COMPLETE — LEARNINGS extracted 2026-05-12 (14 decisions / 12 lessons / 10 patterns / 8 surprises); deployment thread locked; 6 plans + 4 acceptance amendments shipped
**Phase 02 status:** 🟡 EXECUTING — Wave 1 ✅; Wave 2 ✅; Wave 3+ pending (user SaaS prep gate)
**Phase 1.5 status:** ❌ REVERTED (历史) — `filterDate` API 参数不存在；方向重定为 WebSocket（已并入 Phase 3）
**Test count:** 447 m1-perception tests green (Plan 02 baseline 429 + Plan 03 net +18; pre-existing Phase 01.1 makefile path failure FIXED in Plan 03)

## Phase 01.1 Deliverables (2026-05-10 全部 committed)

- ✅ Schema 升级（events / event_tags / question_translations + Amendment 01：删 markets.category/tags 加 event_id FK） — plan 01
- ✅ 翻译 vertical slice（OpenAI 兼容 SDK + 缓存表 + tqdm 进度 + Makefile + .env.example）— plan 02 + 5-10 amendment
- ✅ Scanner 引擎 + 6 内置配方 + YAML 自定义配方 + 4 层 SQL 注入防御 — plan 03
- ✅ 跨 snapshot diff（DuckDB FULL OUTER JOIN）+ 单市场 tracker（union_by_name）— plan 04
- ✅ show-market 多源详情 + watchlist YAML + 受限 AST 表达式求值（无 eval/exec）— plan 05
- ✅ 教学文档 `docs/learning/07-观察市场.md`（347 行）+ Makefile 工作流 quick-ref — plan 06 Task 1+2
- ✅ **Acceptance-driven amendments**（5-10）：解耦翻译 sidecar / 三态 OK-DEGRADED-FAILED 状态 / `make overview` 总览 dashboard / `make snapshots-purge` 数据保留 / E2E 验收手册
- ⏸️ Plan 06 Task 3（human-verify checkpoint，5 对手题）→ 升级为架构方向纠偏（见 thread）

## Phase 01.1 后续 (✅ 全部完成 2026-05-12)

- ✅ **架构 thread**: `.planning/threads/market-observation-architecture.md` — Phase 01.1 SESSION 15 实证完整（§0.3 #10 + §2.7 subset/full 决策 + §2.1.a #4 2h 漂移）
- ✅ **部署 thread**: `.planning/threads/deployment-architecture.md` — drafting → **locked** 2026-05-11，用户 §7 四锚点决策（PaaS 混合 / CN 不约束 / DB 合并 / KMS 延 M3）
- ✅ `/gsd-extract_learnings 01.1` — `01.1-LEARNINGS.md` 落库（14D / 12L / 10P / 8S，327 行）
- ✅ `/gsd-discuss-phase 02 --ws m1-perception` — `02-CONTEXT.md` (22 决策 + 7 the agent discretion) + `02-DISCUSSION-LOG.md`
- ✅ `/gsd-plan-phase 02 --ws m1-perception` — `02-RESEARCH.md` (1914 行) + `02-PATTERNS.md` (36 files) + `02-VALIDATION.md` (22+ Wave 0 tests) + 7 `02-{NN}-PLAN.md` (4150 行)
- ✅ Plan-checker 2 轮 iteration — 5 BLOCKERs + 7 WARNINGs + 2 NEW BLOCKERs 全 resolved

## Phase 02 状态 (executing — Wave 1 done)

- **Goal**: L1 production-grade long-running — 云上 7×24 + 一键部署 + dashboard 雏形
- **完成判定** (thread §1 生产级判定标准): 7-day soak + Better Stack uptime ≥ 99% + ≥1 次自然失败自愈或正确告警
- **Plans / Waves**:
  - Wave 1: Plan 01 (page_fetched_at_ms + L11 silent-failure triple-check) — autonomous ✅ **2026-05-13** (cecb66b/5da55dc/65730a3/b0610e4, ~45 min, 4 commits, 02-01-SUMMARY landed)
  - Wave 2: Plan 02 (HTTP+scheduler) ✅ **2026-05-13** (593f986/8bd22b6/91a9701/f475512, ~90 min, 4 commits) + Plan 03 (Supabase mirror+R2) ✅ **2026-05-13** (12faeea/9977d57/3e378dc/d4753f0, ~90 min, 4 commits) — forced sequential due to 7-file overlap; total Wave 2 = ~3h, 8 commits, 2 SUMMARYs landed
  - Wave 3: Plan 04 (Dockerfile+fly.toml+GHA+first deploy) — **user checkpoint** (Fly + R2 + Supabase 注册)
  - Wave 4: Plan 05 (Sentry+Axiom+Better Stack+Telegram) + Plan 06 (Vercel dashboard) — **user checkpoint** ×2
  - Wave 5: Plan 07 (chaos + 7-day soak + 教学文档 08) — **user checkpoint** + 7 天云上自动跑

## Plan 03 deliverables (2026-05-13, Wave 2)

- ✅ **SupabaseMirror** (`src/polyarb/storage/supabase_mirror.py`) — supabase-py REST SDK service_role 写入；upsert `snapshots` + insert `markets_latest`；**fail-soft**：失败 → log warning + DEGRADED + 继续，不中断 snapshot（D-12 amendment + LEARNINGS P5 严格遵守）
- ✅ **R2Sync** (`src/polyarb/storage/r2_sync.py`) — boto3 + botocore S3-compat client to `<account-id>.r2.cloudflarestorage.com`；parquet 上传 key `parquet/YYYY/MM/DD/HH-MM-SS.parquet`；**fail-soft** 同款
- ✅ **Alembic 001** (`alembic/versions/001_initial_dashboard_schema.py`) — Postgres schema: `snapshots` + `markets_latest` + `top_movers_view` + RLS anon-SELECT 策略；`alembic.ini` + `alembic/env.py` async 配置
- ✅ **W6 双 URL 约定**：`POLYARB_SUPABASE_URL` (REST SDK 用于 mirror 写入) vs `POLYARB_SUPABASE_DB_DSN` (Alembic Postgres DSN 用于 migration)；避免单一 URL 混淆
- ✅ **3-point lockstep extended**：`snapshots` 表加入 SNAPSHOTS_DDL / SNAPSHOTS_COLUMN_ORDER / SNAPSHOTS_INSERT_SQL（test_schema_lockstep.py 加 assertion 防回归）
- ✅ **Orchestrator step 7.5/7.6** (`src/polyarb/snapshot/orchestrator.py`) — local atomic write (steps 1-7) 完成后 fan-out mirror + R2 upload；SQLite 仍为 source of truth
- ✅ **/health checks 3+4** (`src/polyarb/http/health.py`) — Check 3: supabase mirror age (degraded if last mirror > N min)；Check 4: r2 upload recency
- ✅ **SQLiteStore 新增**：`update_snapshot_mirror_fields` / `get_snapshot` / `get_markets_for_snapshot`
- ✅ **CLI** (`scripts/supabase_seed.py`) — typer-based `reconcile` + `init_check` commands
- ✅ **Makefile targets**：`supabase-migrate` / `supabase-reconcile` / `r2-list`
- ✅ **Pre-existing fix**：`test_make_snapshot_markets_full_dry_run_recipe` 现在 green（Plan 01 carry-over 已清）
- ✅ **pydantic frozen-model 自动开关**：凭证在场自动开启 mirror/r2_enabled（W6 写入触发条件）

## Plan 02 deliverables (2026-05-13, Wave 2)

- ✅ Starlette HTTP app (`src/polyarb/http/{app,health,scan}.py`) — `/health` IETF三态 (pass / warn / fail per draft-inadarei-api-health-check, HTTP 200 for pass+warn, 503 for fail), `/scan` HMAC X-Signature gate (constant-time compare via `hmac.compare_digest`, 401 on missing/invalid)
- ✅ **D-22 amendment confirmed**: BOTH `/health` AND `/scan` are PUBLIC (researcher verified Flycast is org-internal and Vercel Edge is cross-org → unreachable). Auth = HMAC middleware over request body, keyed by `SCAN_SHARED_SECRET` env. Pattern matches Stripe/GitHub/Shopify webhook auth.
- ✅ `SnapshotScheduler` (`src/polyarb/daemon/scheduler.py`) — 3-consecutive-failure-pause state machine; pause sets health → fail (503); manual `/scan` resumes + clears counter; persisted via new `scheduler_state` table (sync points: SCHEDULER_STATE_DDL in schemas.py + get/upsert methods in sqlite_store.py)
- ✅ Daemon entry (`src/polyarb/daemon/main.py`) — Starlette + uvicorn host, embedded scheduler via AsyncIOScheduler-pattern
- ✅ `loguru` JSON sink (`src/polyarb/observability/logging.py`) — one JSON line per record to stderr (timestamp ISO 8601 UTC / level / message / module / function / line / extra.\*); `correlation_id` middleware binds per-request UUID via `logger.bind`
- ✅ Settings extended (`src/polyarb/config.py`) — `SCAN_SHARED_SECRET`, scheduler cron, log level/format via pydantic-settings
- ✅ Makefile: `make daemon-run-local` + `make smoke-health-local` for dev verification
- ✅ W11 SQL-injection defense test (`scan_recipes_tampered.yaml` fixture) — proves Phase 01.1 4-layer SQL defense engaged via HTTP path

## Plan 01 deliverables (2026-05-13, Wave 1)

- ✅ `page_fetched_at_ms` nullable column 加到 markets + events 表（DDL/COLUMN_ORDER/INSERT_SQL/SNAPSHOT_SCHEMA 四点 lockstep markets，三点 events）
- ✅ GammaClient `_paginate` 每页注入 `_page_fetched_at_ms`；normalizer 透传到 `page_fetched_at_ms` 列；SNAPSHOT_SCHEMA nullable=True 保证旧 parquet 经 `union_by_name=true` NULL 填充
- ✅ `make triple-check` 全链路三重契约门：exit 0 ↔ SQLite +1 ↔ parquet 文件 +1 ↔ 行数一致；live DB 缺席时 exit 77 优雅 skip（Plan 04 fixture 硬化）
- ✅ Phase 01.1 P7 schema add-only 纪律全程遵守 — 不重命名 `fetched_at_ms`，加注释说明 stage stamp 语义
- ✅ Wave 0 tests RED → GREEN: `test_page_fetched_at_ms_carried_from_raw`、`test_page_fetched_at_ms_in_all_four_sync_points`、`test_parquet_sqlite_consistency.py`
- ⏸️ Pre-existing `test_make_snapshot_markets_full_dry_run_recipe` 失败（与本 plan 无关，Phase 01.1 遗留：Makefile 用 `uv run python -m polyarb.snapshot snapshot --full` 但测试期望 `python -m polyarb.snapshot --full`） — Plan 03 顺手修

## 下次会话该做的（2026-05-14 EOD 更新）

1. **`make planning-status` 验证零 DRIFT**（应该 OK — Plan 01/02/03 全 SUMMARY ✓）
2. **`git log --oneline -5`** 看最新两个 commit (`14de7c6` 工具链 fix + `f568041` 文档/state)
3. **决策路径**（二选一）：

   **路 A**（推荐，符合 thread §1 "L1 未到生产级禁开下一层"纪律）：
   ```
   开 Plan 03 retro fix-up PR — 修 F-01..F-05（详见 project_plan-03-retro-issues-2026-05 memory）
   ↓
   F-01 (HIGH) + F-04 (MEDIUM) 至少必修；F-02/F-03/F-05 可合修
   ↓
   merged 后 Wave 3 dispatch
   ```

   **路 B**（如果想切换主题）：
   ```
   m2-combinatorial 推 T2 Slippage Model（避开 m1 mirror bug）
   ↓
   注意：用户原则上禁开 L2，但 m2 是横向能力线不冲突
   ↓
   等 Plan 03 retro 时机成熟回 m1
   ```

4. **Wave 3 dispatch 条件**（不要提前跑）：
   - ✅ Plan 03 retro PR merged
   - ⏳ 用户准备 8 个 Fly secrets + GHA `FLY_API_TOKEN`（详见 `docs/setup/03-wave3-saas-prep.md` Phase 2 章节）
   - 命令：`/gsd-execute-phase 02 --wave 3 --ws m1-perception`

**关键提醒**：

- Phase 1 调试期端到端 verification ✅ PASSED（详见 `project_phase-1-verification-2026-05` memory）
- Plan 03 5 个 fix-up issue 落 `.planning/threads/deployment-architecture.md §10.2`
- Polymarket Gamma offset≤10000 新约束（详见 thread §10.3）— 短期不阻塞 Phase 02，Phase 02.x 修分页
- 7 天 soak gate 在 Wave 5 末，不可跳；调试期 Supabase Free 够，soak 前升 Pro
- daemon SIGINT 不响应（F-04），停机用 `pkill -9 -f polyarb.daemon.main`
- 关键 memory 入口：
  - [Phase 02 locked stack](memory/project_phase-02-locked-stack.md) — 22 决策栈 + Plan 状态
  - [Phase 1 verification](memory/project_phase-1-verification-2026-05.md) — 验收结果 + 链路状态
  - [Plan 03 retro issues](memory/project_plan-03-retro-issues-2026-05.md) — 5 个 fix-up issue 详细
  - [Secrets hygiene](memory/feedback_secrets-hygiene-2026-05.md) — 4 个泄漏面纪律
  - [Port numbers](memory/feedback_port-numbers-2026-05.md) — 19080 约定

---

## Phase 1 历史快照（已完成，保留作参考）

## Phase 1 Deliverables (verified)

- ✅ `make snapshot-markets` (subset, default) → `python -m polyarb.snapshot`
- ✅ `make snapshot-markets-full` (--full flag) → `python -m polyarb.snapshot --full`
- ✅ `src/polyarb/{clients,storage,snapshot,validator}/` — 5 sub-packages
- ✅ Atomic SQLite (BEGIN IMMEDIATE + DELETE + executemany INSERT) + WAL mode
- ✅ Atomic Parquet writes (`data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet` + tmp + os.replace)
- ✅ Validator Layer 1 (count) + Layer 2 (fields) + Layer 4 (cross-source incl. ghost-book detection per Issue #180)
- ✅ Per-row `fetched_at_ms` (best-effort consistency, NOT transactional)
- ✅ Security invariants: F-1 _safe_float, F-2 follow_redirects=False + MAX_PAGES=1000, F-3 path validator, F-4 fixture sanitization, F-5 truncation caps
- ⏸️ Live API smoke test: untested (manual gate — user runs `make snapshot-markets` against real Polymarket when ready)

## Open Items (carried to subsequent phases)

- **Live API verification** — paper run untested; user's manual step
- **F-7 lockfile** — deferred to m5-industrialize or any phase introducing wallet/auth (per SECURITY-REVIEW.md)
- **`fetched_at_ms` semantic gap** — currently stamped on ALL normalized markets including subset-filtered-out ones. Phase 2 (WebSocket increment) should clean up.
- **Top-of-book single-side** — only `yes_token_id` populated. Phase 3 strategies can't assume symmetric YES/NO; explicitly fetch NO when needed.
- **`record_fixtures.py`** at project root is a working artifact (not committed, not gitignored). User can delete or commit as a tool.

## Session Continuity

**Stopped At:** Phase 02 plan complete, awaiting execute Wave 1
**Last Activity:** 2026-05-12 — SESSION 17 resumed; planning-status clean (0 drift), all 7 Phase 02 plans NOT-STARTED as expected
**Last Resume:** 2026-05-12 — proceeding to `/gsd-execute-phase 02 --wave 1 --ws m1-perception`

**SESSION 08 deliverables (4 unpushed commits)**:

1. `8bbdc47 docs(learning):` — Phase 1 教学文档 6 章 (`docs/learning/`) + CLAUDE.md "教学文档持续产出" 纪律
2. `ccedb5a feat(01):` — `ChunkCache` class + `make snapshot-status` + 5 个新 make target
3. `50c4299 fix(01):` — Gamma 翻页进度 + Phase N/7 banner + macOS `ps -o etime` 兼容
4. `63797ad feat(01):` — 时间戳 prefix + 每 phase elapsed timing (`► Phase X — done in Ys`)
- Tests: 119/119 green (97 → 119, +22 new tests across cache / progress / phase timing)

**SESSION 09 results** (2026-05-01):

- LIVE-RUN-005: 20353 markets, 32916 issues, 72% ghost_book stable
- 6m12s total (vs 26m25s RUN-001) — API idle period effect
- All observability features confirmed: timestamp/phase-elapsed/progress/cache cleanup
- 4 commits already on origin/main (previous push succeeded)
- Snapshot ID 3 in SQLite

**Recommended Next Action** (下次会话首选项)：

**A. 启动 Phase 1.1 plan（observation-toolkit + 中文化）**（推荐 — discuss 已完成）

   - `/gsd-plan-phase 1.1 --ws m1-perception`
   - 如果 SDK 兼容问题再次出现，降级为 Claude 手工 plan
   - CONTEXT.md 已锁定 T1-T7 全部决策，plan 阶段只需排执行顺序
   - 第一目标：T1 (schema+category) + T2 (翻译) + T3 第一个配方 走通

**B. 切到 m2-combinatorial 推 T2 Slippage Model**（避开 m1 完成 m2）

   - T1 已 commit 落地，T2 缺 PolymarketDepthCurve

**推荐 A**：方向已重定 — 用户明确反对 demo 路线，要"为进入市场做准备"的成熟观察体系。Phase 1.1 是真正的下一步，Phase 2 WebSocket 推迟到 1.1 完成

**Carry-over open items**:

- 220 个市场无 endDate（Layer 2 UNKNOWN）— 需要分类调查（是 perpetual market？）
- clob_missing 在 4 小时内 +33%（CLOB 可达性漂移）— 需要时序观察
- Polymarket Gamma API 在 CST 22-24 时段明显慢（page 速度从 1.7/s 降到 0.3/s）— 暂未触发任何代码改动，但记录为环境事实
- **新观察**: Gamma 翻页速度在北京时间 10 点（美东 22 点）极快（6m12s 总耗时），但在 22-24 点（美东 10-12 点）慢（26m25s）— 推测 API 有北美白天高峰

## Phase 1 Artifacts

- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-CONTEXT.md` — locked decisions
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-RESEARCH.md` — 970 lines tech research
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-PATTERNS.md` — 32 file analogs
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-SECURITY-REVIEW.md` — 1 HIGH + 3 MED + 3 LOW (resolved)
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-PLAN.md` — 5 executable plans
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-SUMMARY.md` — per-plan executor output
