---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
current_phase: 02
status: gate-passed-ready-for-extract-learnings
stopped_at: "SESSION 21 EOD 2026-05-20 04:30 CST: Phase 02 ✅ HARD GATE PASSED — alert chain end-to-end verified live in prod chaos. Inj 2-v2 (21:06:22Z) 在 ~75s 内完整跑出 3× FAILED → PAUSED → send_paused_alert → Sentry email (PYTHON-C/D + PYTHON-B digest) + Telegram '3 consecutive FAILED snapshots' + Sentry dashboard. 全部 5 个 chaos injection 完成。5 个新 P0/P1 bug 4 个在本会话修了 (alerts.py Telegram unconditional / Makefile init_sentry / Sentry alert rule / scheduler_interval_s 可配 / GHA setup-flyctl@1.6)。Phase 02 final 02-07-SUMMARY.md landed. planning-status zero drift. **NEXT 下次会话**：(1) `/gsd-extract_learnings 02 --ws m1-perception` 关 Phase 02。(2) Phase 03 (L2) 启动前必须先消化 Phase 02.1 backlog (2 个 P1 + 1 个 trade-off 待修)。(3) M2 T2 三选一决策。"
last_updated: "2026-05-19T21:30:00.000Z"
last_activity: 2026-05-19
progress:
  total_phases: 4
  completed_phases: 2
  total_plans: 20
  completed_plans: 20
  percent: 100
---

# Project State

## Current Position

Phase: 02 (l1-production-grade) — ✅ **HARD GATE PASSED**, ready for /gsd-extract_learnings
Plan: 9 of 9 ✅ (all SUMMARY landed, planning-status zero drift)
**Status:** Phase 02 alert chain end-to-end verified live in prod chaos (Inj 2-v2 2026-05-20 21:06Z). Phase 03 unblocked, 但启动前必须先消化 Phase 02.1 backlog (2 P1 + 1 trade-off) + 回补 7-day uptime soak (thread §1 要求).
**Current Phase:** 02 (gate passed, awaiting extract_learnings)
**Last Activity:** 2026-05-20
**Last Activity Description:** SESSION 21 EOD pt 2 — Phase 02 final close. 5 个 prod chaos injection 完成 (Inj 1/2-v1/2-v2/3/5; Inj 4 SSH+SQL 实操). Inj 2-v2 真触发 send_paused_alert → 三路独立 alert verified (Sentry email PYTHON-C/D + PYTHON-B digest, Telegram, Sentry dashboard). 暴露 8 个新 bug,本会话修了 5 个 P0 (alerts.py TG unconditional / Makefile init_sentry / Sentry alert rule / scheduler_interval_s 可配 / GHA setup-flyctl@1.6). Phase 02.1 backlog deferred 3 (P1×2 + trade-off×1). 17 commits 全 push. Phase 02 final 02-07-SUMMARY.md + 配套 thread (soak-gate-deviation / learnings-meta updates) 落地.

## Progress

**Phases Complete:** 2 (Phase 01 + Phase 01.1) + Phase 02 hard gate passed (awaiting LEARNINGS)
**Phase 01.1 status:** ✅ COMPLETE — LEARNINGS extracted 2026-05-12 (14 decisions / 12 lessons / 10 patterns / 8 surprises); deployment thread locked; 6 plans + 4 acceptance amendments shipped
**Phase 02 status:** ✅ HARD GATE PASSED — Wave 1+2+2.5+3+3.5+4+5 全 ✅;5 个 prod chaos injection 完成;alert chain end-to-end verified live in prod (Sentry + Telegram + Sentry dashboard 三路). 待 `/gsd-extract_learnings 02 --ws m1-perception`
**Phase 02.1 backlog (deferred bugs, 启动 Phase 03 前必修)**:
  - #6 trade-off: `/health` 503 触发 Fly proxy 切流量 (IETF strict vs Fly proxy 行为冲突)
  - #7 P1: fail-soft 互相抵消 (撤 secret 场景 `mirror_enabled=False` → 静默)
  - #8 P1: daemon PAUSED 无 prod-friendly unpause endpoint (现需 SSH+sqlite3+restart 三步)
**Phase 1.5 status:** ❌ REVERTED (历史) — `filterDate` API 参数不存在；方向重定为 WebSocket
**Test count:** 459+ m1-perception tests green (Plan 02-07 +22 chaos tests + 2 scheduler_interval tests; 3 pre-existing failures still deferred)

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

## 下次会话该做的（2026-05-19 SESSION 20 EOD 更新）

### 第一步（恢复 + 健康）

```
/gsd-resume-work --ws m1-perception
make planning-status                                      # 应该 zero drift
curl -sS https://polyarb-l1.fly.dev/health                # 应该 overall=pass, 4 checks all pass
```

### 第二步（决策：Plan 02-07 起 vs 其它工作）

Phase 02 只剩 Plan 02-07 (Wave 5 = chaos + 7-day soak)，是 Phase 02 完结的最后一个 plan。

**路 A — 启动 Wave 5（推荐时机：你有 7 天观察 budget 时）**：
```
/gsd-execute-phase 02 --wave 5
```
两段：(a) chaos test 验证 alert paths 在真实失败下都触发 → (b) 7 天 soak gate（daemon 不重启持续运行 7 天，prod uptime ≥ 99%，至少 1 次自然失败被正确告警）。chaos 期间会真实触发 Sentry 邮件 / Better Stack 邮件 / Telegram 推送，是好事。

**路 B — 跨线工作**（不触发 Wave 5，避免 chaos 噪声）：
- m2-combinatorial T2 Slippage Model（不依赖 Phase 02 完成，可并行）
- 清 3 个 pre-existing test failures（test_pass_when_fresh / make_smoke_health_local / test_r2_retry — 不阻塞但拖测试套件干净度）
- 173 个旧 commit author 重写（**不建议**，会破坏 SUMMARY cross-ref，详见 [git-identity-anomaly](memory/feedback_git-identity-anomaly-2026-05.md)）

**路 C — 文档 / 教学补遗**：
- `docs/learning/` 加 Wave 4 教学文档（Sentry/Better Stack/Telegram alert path 三轨设计、Edge Runtime HMAC 模式、lib supabase server-client split for Next.js 15 App Router）

### 第三步（关键事实记忆）

- prod daemon **持续运行中**，不需要手动重启
- Vercel dashboard URL: `polymarket-arbitrage-ppf6exo78-jiangwen-su-s-projects.vercel.app`（产品 production deployment alias）
- 14 个 Fly secrets 全 deployed（8 base + 6 Wave 4 observability）
- 5 个 Vercel env vars 全 set（Supabase URL + anon key + EMAIL_WHITELIST + SCAN_SHARED_SECRET + SCAN_ENDPOINT_URL）
- Supabase Auth URL Configuration done (Site URL + /auth/callback Redirect URL)
- 本地 `.env` 含 14 个 secrets（Wave 4 backfilled，gitignored），`.env.bak-pre-wave4` 是 backup
- **`.git/config [user]` section 已删** — 新 commit 用 global `Jiangwen Su <uukuguy@gmail.com>` → Vercel auto-deploys

### 关键 memory 入口

- [Phase 02 Wave 4 完成 2026-05](memory/project_phase-02-wave-4-2026-05.md) — Wave 4 全栈状态 + 3 alert path + dashboard + 2 process 事故
- [Phase 02 locked stack](memory/project_phase-02-locked-stack.md) — 22+1 决策 + Wave 1-4 ✅ 状态
- [Git identity anomaly 2026-05](memory/feedback_git-identity-anomaly-2026-05.md) — 不要再凭空构造 author identity，禁止 set git config user.*

### 关键提醒

- Wave 5 7-day soak gate 不可跳 — 这是 Phase 02 "生产级" 判定标准（uptime ≥ 99% + ≥1 次自然失败正确告警）
- chaos test 期间会真实触发告警邮件 + Telegram 推送，**用户邮箱会收一波**（计划好心理预期）
- Supabase Free tier 7 天无活动 auto-pause 会让 soak 中断 → soak 启动前升 Pro $25/月（thread §5）
- daemon SIGINT 不响应（F-04 deferred），停机用 `pkill -9 -f polyarb.daemon.main`
- Polymarket Gamma offset≤10000 新约束（thread §10.3）— 短期不阻塞 Phase 02，Phase 02.x 修分页

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

**Stopped At:** SESSION 20 EOD (2026-05-19) — Wave 4 完整收尾 + git tree/MEMORY/STATE 全部干净
**Next Resume:** 见上方 "下次会话该做的（2026-05-19 SESSION 20 EOD 更新）" 段
**Authoritative state:** [Phase 02 Wave 4 完成 memory](memory/project_phase-02-wave-4-2026-05.md)

**Carry-over observational facts**（值得跨 session 保留的市场观察）:
- 220 个市场无 endDate（Layer 2 UNKNOWN）— 需要分类调查（是 perpetual market？）
- clob_missing 在 4 小时内 +33%（CLOB 可达性漂移）— 需要时序观察
- Polymarket Gamma API 在 CST 22-24 时段明显慢（page 速度 1.7/s → 0.3/s）— 北美白天高峰；记录为环境事实
- Gamma offset≤10000 是 2026-05 新约束（详见 deployment-architecture.md §10.3）— Phase 02.x 修分页

**历史 session 详细 carry-over** — 见 git log + 各 plan SUMMARY，不在 STATE.md 重复维护。

## Phase 1 Artifacts

- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-CONTEXT.md` — locked decisions
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-RESEARCH.md` — 970 lines tech research
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-PATTERNS.md` — 32 file analogs
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-SECURITY-REVIEW.md` — 1 HIGH + 3 MED + 3 LOW (resolved)
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-PLAN.md` — 5 executable plans
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-SUMMARY.md` — per-plan executor output
