# Roadmap: M1 Market Perception

> 能力线，不是里程碑。持续运行 + 持续增强。
> Phase 由 `gsd-tools phase add "..."` 动态长出，不预先列。

## Overview

构建对 Polymarket 整个市场的实时、完整、准确的视图。是 M2/M3/M4 所有策略的底座。

不"完成"，只"够用了"。每当有新策略需要新数据维度，本 workstream 长一个新 phase。

## Phases

(待 `gsd-tools phase add` 长出第一个)

### Phase 1: 完整市场快照工具 ✅ COMPLETE

**Goal:** Polymarket 全市场快照工具（subset/full 双模式），SQLite + Parquet 落库
**Status:** Verified 2026-05-01 (LIVE-RUN-005, 20353 markets, 119 tests green)
**Plans:** 5 plans complete

### Phase 1.1: 市场观察工具与中文化（Observation Toolkit + Translation）

**Goal:** 把 Phase 1 的原始 snapshot 数据转成"可观察的市场"——配方化扫描命令、跨快照对比、单市场详情、中英对照、类别统计、watchlist。让初学者能 30 秒判断一个市场该不该碰。
**Status:** ⏳ Plan complete (2026-05-01 SESSION 12) — 待 execute
**Depends on:** Phase 1
**Plans:** 6 plans (4 waves)

Plans:
- [x] 01.1-01-PLAN.md — Wave 1: schema 升级（Amendment 01: events + event_tags + question_translations + markets.event_id；删除 markets.category/tags）+ normalizer 双源融合 + deps + 新基线 snapshot
- [x] 01.1-02-PLAN.md — Wave 2: T2 翻译完整 vertical slice（OpenAI 兼容 SDK + 缓存表 + 批量 orchestrator + CLI + Makefile + .env.example）
- [x] 01.1-03-PLAN.md — Wave 3: T3 scanner 引擎 + 6 内置配方 Batch 1（amendment: scan-by-tag 替代 scan-by-category）+ 自定义 yaml 加载（4 层防御 + trust split）
- [ ] 01.1-04-PLAN.md — Wave 4: T4 跨 snapshot diff（duckdb FULL OUTER JOIN）+ 单市场时序 tracker（read_parquet glob + union_by_name）
- [ ] 01.1-05-PLAN.md — Wave 4: T5 show-market 多源拼装 + T7 watchlist（yaml.safe_load + 受限 AST 求值，禁内置任意求值器）
- [ ] 01.1-06-PLAN.md — Wave 5: 教学文档 07-观察市场.md + Makefile help 整理 + 端到端 9 步验收 + 对手测试 5 题（含 human-verify checkpoint）

Goals:
- [x] T1 markets 表补 event 关联（Amendment 01: event_id FK + events + event_tags 表）— covered by plan 01
- [x] T2 question 中文翻译（独立 question_translations 表，OpenAI 兼容 API）— covered by plan 02
- [x] T3 配方化扫描命令（thick-but-slippery / near-end / ghost-suspicious / coin-flip / neg-risk-incomplete / by-tag）— covered by plan 03
- [ ] T4 跨 snapshot 对比命令（duckdb 跨 parquet）— covered by plan 04
- [ ] T5 单市场详情命令（中英对照 + 完整字段 + 时间维度）— covered by plan 05
- [x] T6 类别统计命令 — covered by plan 03 (scan-by-tag, amendment 01 replacement for scan-by-category)
- [ ] T7 watchlist（YAML 或 SQLite 表）— covered by plan 05

### Phase 2: L1 production-grade long-running

**Goal:** 把 Phase 01.1 的 L1 观察工具从"研发期单次跑通"升级到"云上 7×24 自主跑 + 健康监控 + 一键部署"。L1 达到生产级判定标准（thread §1）后才能开 L2 工作。
**Status:** 🟢 Ready for discuss — Phase 01.1 LEARNINGS 已映射 6 条 must-haves
**Depends on:** Phase 1.1（observation toolkit + acceptance amendments + deployment thread locked）
**Refs:**
- `.planning/threads/market-observation-architecture.md` §1 (L1 生产级判定标准) + §1.5 (框架抽象 A/B/C)
- `.planning/threads/deployment-architecture.md` §0.1 (locked 4 decisions) + §2.6 (云栈调研)
- `.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/01.1-LEARNINGS.md` (D11/D14/L2/L11/L12/S4)

Scope (从 LEARNINGS carry-over):
- Makefile CLI 入口 smoke test（修 L11/S5 silent failure 根因）
- Snapshot 健康判定升级到三态（D14/L12 已在 amendment 落地，phase 02 补 parquet/SQLite 双校验）
- 框架抽象 A 落地（统一市场状态模型 + 显式分离 stamp 时间 vs 抓取时间，L2/D5/P3）
- 一键部署链路（Dockerfile + fly.toml + GHA workflow，D11/S4 region eu，PaaS 单 DB）
- L1 云上 7×24 长跑 + 健康监控（thread §1 生产级判定标准）
- Dashboard 雏形（Vercel + Supabase 单库，CN 友好非约束）

不在 scope：
- KMS / 私钥栈（D11 决策延到 M3 实盘前）
- Tiger Cloud 双库（D11 单库先撞墙）
- L2/L3 定向跟踪与 WebSocket（thread §1 纪律：L1 未到生产级禁开下一层）

Plans:
- [x] 02-01-PLAN.md — Wave 1: page_fetched_at_ms 列 + L11 silent-failure triple-check（make triple-check 三重契约门）
- [x] 02-02-PLAN.md — Wave 2: Starlette HTTP daemon (/health + /scan HMAC) + AsyncIOScheduler + loguru JSON sink
- [x] 02-03-PLAN.md — Wave 2: Supabase mirror + Cloudflare R2 sync + Alembic 001 schema + 双 URL 约定
- [x] 02-08-PLAN.md — Wave 2.5 retro: Plan 03 F-01..F-05 fix-up（Supabase upsert / Polymarket offset cap / 工具链）
- [x] 02-04-PLAN.md — Wave 3: Dockerfile multi-stage + fly.toml + GHA deploy workflow + 首次 prod deploy（256MB）
- [ ] 02-09-PLAN.md — **Wave 3.5 emergent**: 流式分页根因修复（D-23 amendment） — `_paginate` → `AsyncIterator[dict]` + orchestrator streaming consumer + PyArrow ParquetWriter chunked write + 内存回归测试。**触发**: 2026-05-15 Plan 02-04 首次 prod deploy 后 daemon OOM-killed（256MB Fly VM）。**Blocks**: Wave 4-5（daemon 不稳态下装观测/跑 soak 无意义）。
- [ ] 02-05-PLAN.md — Wave 4: Sentry + Axiom + Better Stack + Telegram alert 集成
- [ ] 02-06-PLAN.md — Wave 4: Vercel Next.js dashboard 雏形 + scan trigger
- [ ] 02-07-PLAN.md — Wave 5: chaos test + 7-day soak + 教学文档 08

### Phase 02.1: L1 Production-Grade Fix-up (INSERTED 2026-05-20)

**Goal:** 消化 Phase 02 chaos injection 暴露的 3 个 deferred bug, 让 L1 daemon 从"生产级带 caveats"升级到"真生产级", 解锁 Phase 03 (L2 orderbook) discuss。严格 fix-up scope — 不蔓延。
**Requirements**: 3 bug 全修 + chaos-grade verification (per Phase 02 L6/L7 alert chain discipline)
**Depends on:** Phase 02 (LEARNINGS extracted, hard gate passed)
**Refs:**
- `.planning/workstreams/m1-perception/phases/02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-CONTEXT.md` (7 decisions locked)
- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-LEARNINGS.md` §§ L6/L7/P14 (verification 设计依据)
- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-07-SUMMARY.md` § "8 个新发现 bug" 表 (#6/#7/#8 命名定义)
**Plans:** 3/4 plans executed

Plans:
- [x] 02.1-01-PLAN.md — Wave 1: Bug #7 fail-soft visibility (orchestrator step 7.5 audit log + Sentry breadcrumb + chaos Inj 3-v2 verification, D-01/D-02)
- [x] 02.1-02-PLAN.md — Wave 1: Bug #8 prod unpause endpoint (src/polyarb/http/control.py + ControlAuthMiddleware + make unpause-prod + chaos Inj 4 verification, D-03/D-04/D-22)
- [x] 02.1-03-PLAN.md — Wave 2: Bug #6 /healthz endpoint + fly.toml probe path 切换 (health.py refactor + _build_health_checks helper + Fly checks smoke, D-05/D-06)
- [ ] 02.1-04-PLAN.md — Wave 3: docs/learning/09-生产化运维.md + 00-INDEX 更新 + Phase 02.1 closure verification (per CLAUDE.md 教学纪律, D-07)

Scope (3 bug):
- #7 P1: fail-soft visibility — 撤 Supabase secret → mirror_enabled=False 路径静默跳过, 加 orchestrator audit log + Sentry breadcrumb (D-01/D-02)
- #8 P1: daemon unpause endpoint — PAUSED 后只能 SSH+sqlite3+restart 三步, 新起 /control/unpause endpoint (HMAC), counter 清零 (D-03/D-04)
- #6 trade-off: /health 503 vs Fly proxy — 拆两 endpoint /health (IETF strict) + /healthz (Fly-friendly 200), fly.toml probe 切到 /healthz (D-05/D-06)

不在 scope (D-07 严格 fix-up):
- 3 pre-existing test failure (独立 chore commit)
- Axiom log-shipping (P2 backlog)
- 7-day uptime soak (Phase 03 启动期)
- Vercel dashboard "Unpause Daemon" 按钮 UI (推独立 plan)
- /control/pause + /control/status (本 phase 只建 unpause, planner 可选择是否搭 router 框架)

### Phase 03: L2 Orderbook Tracking (分钟级 daemon)

**Goal:** L2 中间层 — 候选子集 (10-100 个) 分钟级 (1-5 min) tracking, top-of-book + 成交流, 信号识别 / 进场触发. thread §1 三层金字塔的中段，桥接 L1 全市场观察与 L3 策略执行。
**Status:** ✅ COMPLETE (closed 2026-05-25, 8/8 plans + 3 chaos live PASS + 2 deferred to Phase 03.1)
**Plans:** 8 / 8 complete
**Depends on:** Phase 02.1 (3 bug 修完 + chaos 闭环) ✅
**Refs:**
- `.planning/threads/market-observation-architecture.md` § 1 三层金字塔 + § 2.2 Polymarket WS 真实能力调研
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-CONTEXT.md` (9 D-XX decisions locked)
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-RESEARCH.md` (1513 lines, 7 focus areas)
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-PATTERNS.md` (33 files mapped, 8 SP)
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-VALIDATION.md` (5 chaos Inj + Wave 0 RED tests)
- `.planning/workstreams/m1-perception/phases/02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-LEARNINGS.md` (Phase 02.1 9D/8L/7P/5S)
- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-LEARNINGS.md` § L6/L7/P14

**Plans:** 8 plans across 7 stages (Wave 1 // Wave 2 // Wave 3 // Wave 4 // Wave 5 // Wave 6 (checkpoint) // Wave 7 (checkpoint)) — Plans 04/05 serialized per B2 fix to avoid pyproject.toml + l2_main.py + config.py overlap

Plans:
- [x] 03-01-PLAN.md — Wave 1 (autonomous, parallel w/ 02): GHA Supabase keepalive + Better Stack heartbeat (D-01)
- [x] 03-02-PLAN.md — Wave 1 (autonomous, parallel w/ 01): polyarb-l2 Fly bootstrap (D-06)
- [x] 03-03-PLAN.md — Wave 2 (checkpoint, depends on 01+02): L2 daemon entry + /health + /healthz + P9 server-started gate (D-06)
- [x] 03-04-PLAN.md — Wave 3 (autonomous, depends on 03): WS client + WsWatchdog 30s + storm cap + WsConsumer (D-02/D-03)
- [x] 03-05-PLAN.md — Wave 4 (autonomous, depends on 03+04 — B2 serialization to avoid pyproject/l2_main/config overlap): asyncpg LISTEN/NOTIFY event bus + candidate refresh, `POLYARB_EVENT_BUS_ENABLED` defaults FALSE (B1) (D-04/D-05)
- [x] 03-06-PLAN.md — Wave 5 (autonomous, depends on 04+05): Alembic 003 + L2SupabaseMirror + Data API trades backfill (D-07/D-08)
- [x] 03-07-PLAN.md — Wave 6 (checkpoint, depends on 06): chaos verification (3 Inj L2-1/L2-2/L2-3a live PASS + L2-3b/L2-4/L2-5 deferred to Phase 03.1 with substitute evidence) + 03-SOAK-LOG.md
- [x] 03-08-PLAN.md — Wave 7 (checkpoint, autonomous-final, depends on 07): docs/learning/10-L2-跟踪.md + 4 Vercel dashboard pages + VALIDATION flip + Phase 03 hard-gate closed (D-07/D-09)

Scope (核心问题, discuss-phase 决):
- 候选集选择: L1 snapshot 哪些字段 → L2 跟踪集 (流动性 / volume / tag / 用户 watchlist?)
- WS vs REST polling: top-of-book 用 Polymarket /book channel 还是 REST? thread §2.2 未答
- 时序后端: 继续 SQLite + Parquet 还是 TimescaleDB / DuckDB? L2 频率高于 L1
- 成交流采集: trades 历史用 Subgraph 还是 WS 流自己累积?
- 信号识别: surface "候选→tracker→signal" 流程? 是否进 dashboard?
- L1 7-day soak deviation 回补: paid Supabase Pro $25/mo? 切 Neon? 新 deviation?
- DB 选型重审: thread §0.2.1 deployment 决策 + L2 写入频率 → 是否升 Supabase Pro / 切 Neon / 自管 Postgres

不在 scope (delegated to L3 / Phase 04+):
- 完整 orderbook 深度 (top-of-book only)
- 策略执行层 (信号识别 only, 不下单)
- WebSocket 全市场流 (L3 候选)
- M4 LLM 价值判断 (m4-smart-strategies 独立 workstream)

### Phase 03.1: L2 Observability Gaps Fix-up

> INSERTED 2026-05-26 — addresses Phase 03 chaos Inj L2-2 carry-over + SESSION 27 Sentry RCA new findings.


**Goal:** 修 Phase 03 chaos Inj L2-2 暴露的 5 个 observability GAP + 跑 3 个 deferred chaos Inj + 整合 SESSION 27 L1 PAUSE 3.5 天 RCA 的 4 项新发现 (Fly DNS chronic / failure_threshold 调优 / Sentry env=dev tag audit / snapshots.notes 写 fail reason)。让 L1+L2 alert chain 真正能在分钟级被发现并修复。
**Status:** 🟢 Plans ready (7 plans across 5 waves; planned 2026-05-26 SESSION 28; revised post-checker — Wave 1 [01,03] parallel, Wave 3 [04,06] parallel)
**Depends on:** Phase 03 (closed 2026-05-25, carry-over filed in 03-LEARNINGS)
**Plans:** 7 plans
**Refs:**
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-LEARNINGS.md` (carry-over: 5 GAPs + 3 deferred Inj)
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-SOAK-LOG.md` (Inj L2-2 fail-soft chain-truth gap)
- `.planning/JOURNAL.md` SESSION 27 (2026-05-26 L1 PAUSE RCA + Polywatch 立项)
- memory `project_fly-dns-chronic-failure-2026-05.md` (Sentry issue 121111789, 6 天 3 次 occurrence)
- memory `feedback_code-vs-chain-truth-2026-05.md` (fail-soft 必须 surface /health)
- memory `feedback_container-image-aware-chaos-2026-05.md` (pkill/ps 必先验目标 image)
- memory `feedback_fly-api-token-shadowing-2026-05.md` (.env FLY_API_TOKEN 覆盖 keychain)

Scope (12 项, discuss-phase 决):

**5 carry-over GAPs from Inj L2-2 (P0):**
- GAP-1: `l2_mirror_enabled` flag 接入 `/health` + Sentry breadcrumb (fail-soft surface to chain truth)
- GAP-2: `SqliteStore.get_l2_tob_last_mirror_at_s()` getter 实现
- GAP-3: L2SupabaseMirror.push_* success path 持久化 `last_mirror_at_s` 到 SQLite
- GAP-4: chaos Makefile + secrets sync — flyctl 前 drop FLY_API_TOKEN env (修 token shadowing)
- GAP-5: re-run Inj L2-2 with Sentry API breadcrumb query (验证 GAP-1..3 真闭环)

**3 deferred chaos Inj (P1):**
- Inj L2-3b: opt-in L1 NOTIFY happy-path (POLYARB_EVENT_BUS_ENABLED=true 真启 + 低流量窗口跑)
- Inj L2-4: cross-bug WS storm + Supabase paused (需 POLYARB_WS_TEST_KILL flag ~10 LoC)
- Inj L2-5: Data API 429 backfill (ad-hoc 路径, 实际用时再验)

**4 SESSION 27 Sentry RCA 新发现 (P0/P1):**
- GAP-100: Fly machine 容器 DNS chronic 故障 — 6 天 3 次 EAI_NODATA 触发 SCHEDULER_PAUSED (Sentry issue 121111789). 调研 IPv6 fallback / 缓存 TTL / 是否 Fly 平台问题, 给修复方案 (P0)
- GAP-101: `failure_threshold` 调优 — 当前 1 次 fail 就 PAUSE, DNS jitter 不该 trip; 考虑 N-of-M 阈值或 transient error 分类 (P0)
- GAP-102: Sentry alert routing audit — `environment=dev` tag 是否让 alert routing 静音/降级? 6 天 3 次 alert 用户都没主动响应需查 alert rule (P0)
- GAP-103: `snapshots.notes` 列写 fail reason — 让 dashboard / SQL 能直接看到 "DNS fail" 等具体原因, 不止 is_valid=false (P1)

**2 process upgrades (P1):**
- PROCESS-1: plan-checker 新规则 — "fail-soft envelope MUST surface to /health" (encode chain-truth not just code-truth) 进 .planning/threads/
- PROCESS-2: CLAUDE.md context — "container-image-aware chaos design" (pkill 不存在的 image 教训)

不在 scope:
- Polywatch healthz-watcher MVP 进一步扩展 (走 m5-industrialize phase 01 polywatch-mvp)
- 7-day uptime soak (单独触发, 等 P0 修完)
- M2/M3/M4 workstream 推进 (workstream 独立)

Plans (revised 2026-05-26 SESSION 28 post-checker — 5 waves, Wave 1 & Wave 3 parallel):
- [ ] 03.1-01-PLAN.md — Wave 1 (autonomous): SqliteStore l2_mirror_state singleton + L2SupabaseMirror success-path freshness persist (GAP-2 + GAP-3)
- [ ] 03.1-02-PLAN.md — Wave 2 (autonomous, depends 01): Settings.l2_mirror_enabled + l2_tob_age_warn/fail_s thresholds + /health mirror sub-check live wiring + orchestrator snapshots.notes derive from issues (GAP-1 + GAP-103)
- [ ] 03.1-03-PLAN.md — Wave 1 (autonomous, parallel w/ 01): Makefile chaos FLY_API_TOKEN= prefix + thread chain-truth discipline + CLAUDE.md container-image-aware chaos + docs/dev/chaos-toolkit.md (GAP-4 + PROCESS-1 + PROCESS-2)
- [ ] 03.1-04-PLAN.md — Wave 3 (autonomous, depends 02): tenacity DNS-class retry on Gamma fetch + FAILURE_THRESHOLD 3→5 + dns_baseline_probe.py script + Polywatch trial backlog registration (GAP-100 + GAP-101)
- [ ] 03.1-05-PLAN.md — Wave 4 (checkpoint, depends 06): Sentry alert routing audit (playwright-cli) + sentry_environment Settings field + W-6 typo guard + user-applied prod env rollout (GAP-102)
- [ ] 03.1-06-PLAN.md — Wave 3 (autonomous, depends 01+02+03; parallel w/ 04): POLYARB_WS_TEST_KILL primitive + /health chaos:test_kill_flag sub-check (W-5 chain-truth) + chaos-l2-inj4 Makefile orchestrator + Inj L2-5 429 fixture+dry-run (Inj L2-4 + Inj L2-5 fixture)
- [ ] 03.1-07-PLAN.md — Wave 5 (checkpoint, depends 02+03+04+05+06): chaos run batch — Inj L2-2 re-run with lowered-threshold chain-truth proof (GAP-5, B-3) + Inj L2-3b + Inj L2-4 + SOAK-LOG + VALIDATION + phase closure



### Phase 04: Candidate Set 扩容 + L2 Throughput 验证 + 投影 Gap 收尾

> ADDED 2026-05-28 (SESSION 30) — Phase 03.1 chaos 多处"低负载只验逻辑不验 throughput"的明确欠账 + Phase 02/03 投影 gap + GAP-200。

**Goal:** 把 polyarb-l2 candidate set 从 3 个 bootstrap asset_ids 扩到真实规模（compute_candidates 改读 Supabase `markets_latest` 而非本地 SQLite，解决跨容器读不到的限制），在真实负载下验证 WS storm / watchdog throughput（补 Phase 03.1 Inj L2-4 只验逻辑的欠账），并收尾两个投影 gap（`markets_latest.yes_token_id` 补列 + GAP-200 config-disable 也 surface 成 chain-truth）。
**Status:** ✅ CLOSED 2026-05-29 (SESSION 31) — 4 plans shipped + LEARNINGS (11D/9L/8P/5S) + G-01 cold-start fix prod-verified (subs 3→60). D-06 throughput verdict **DEFERRED** (G-02/G-03/G-04 阻塞) → carried to Phase 04.1. origin/main `7bb4fa1`.
**Depends on:** Phase 03.1 (closed 2026-05-27) + Supabase `markets_latest` schema（Phase 02 mirror 投影）
**Plans:** 4 plans

Scope (候选, discuss-phase 决):
- **candidate set 扩容**: compute_candidates 改 Supabase `markets_latest` 查询（当前需 L1 SQLite 本地访问，跨容器读不到 — memory CURRENT-CALL）
- **markets_latest.yes_token_id 补列**: Phase 02 mirror 投影 gap（markets_latest 缺 yes_token_id 列）
- **L2 throughput 验证**: 真实 candidate scale 下重跑 WS storm / watchdog（Phase 03.1 Inj L2-4 只在 3-asset 低负载验了逻辑，没验 throughput）
- **GAP-200**: config-disable mirror 也 surface 成 chain-truth（service_key empty 时 /health 仍注册 mirror sub-check status=fail）

Plans (4 plans, 3 waves — D-01/02/03 是数据源切换前提, D-07/D-08 独立可并行):
- [ ] 04-01-PLAN.md — Wave 1 (autonomous: false — [BLOCKING] alembic push needs live DSN): D-07 markets_latest.yes_token_id nullable column (Alembic 004 add-only + push) + supabase_mirror narrow projection 补列
- [ ] 04-03-PLAN.md — Wave 1 (autonomous, parallel w/ 01): D-08 GAP-200 /health three-branch mirror gate (config-disable surface as chain-truth)
- [x] 04-02-PLAN.md — Wave 2 ✅ EXECUTED 2026-05-28 (autonomous, deps 01+03 ✅): D-01/D-02/D-03/D-04 数据源切换核心 — Supabase markets_latest 分页拉取 + 命名临时 SQLite 适配层(fail-loud) + compute_candidates fail-soft + candidates fetch-age /health 链. Commits 52858c1 + de54785 + SUMMARY commit; 18 new tests pass; +1 Rule 1 bug fix (BUILTINS dropped when scanner_yaml=None — pre-existing latent)
- [ ] 04-04-PLAN.md — Wave 3 (checkpoint: human-verify, depends 02): D-05/D-06 真实 candidate scale prod throughput chaos — WsConsumer dropped-frame counter + chaos-l2-inj4-throughput Makefile target + baseline/storm/recovery verdict (补 Inj L2-4 throughput 欠账)

Makefile targets delivered: `chaos-l2-inj4-throughput` (Plan 04). `supabase-migrate` (existing, reused by Plan 01 D-07 push).

### Phase 04.1: D-01 跨 restart 健壮性 + chaos primitive 重设计 + throughput verdict 收口

> ADDED 2026-05-29 (SESSION 31) — Phase 04 Plan 04 Task 3 prod chaos run 暴露的 3 个结构性 gap (G-02/G-03/G-04)，阻塞 D-06 throughput verdict。fix-up phase（同 02.1 / 03.1 体例）。

**Goal:** 修掉 Phase 04 prod chaos 暴露的三个结构性问题，让 D-06 throughput verdict 能在真实 candidate scale 下真正算出来：(1) D-01 fetch 跨 L2 restart 健壮 — restart 落在 NOTIFY 静默窗口也能拿到真实 candidate set（G-02）；(2) chaos primitive 从 "flyctl secrets set = rolling restart" 重设计成 in-flight 注入，让 storm 真测 in-flight kill-recovery 而非 startup（G-03）；(3) RSS 测对 Python L2 进程而非 Fly hallpass（G-04）。修完后 re-run chaos 拿真 D-06 verdict + 补 Pitfall 4 watchdog false-trip 观察（被 G-03 阻塞未观察到）。
**Status:** 📋 PLANNED (2026-05-29 SESSION 31) — 4 plans, 3 waves
**Depends on:** Phase 04 (closed 2026-05-29) — G-02/G-03/G-04 findings in 04-SOAK-LOG.md
**Plans:** 4/4 plans complete

Scope (locked in 04.1-CONTEXT.md):
- **G-02 D-01 跨 restart 健壮**: eager startup-prime (catchup 完无条件 dispatch 一次 synthetic prime → markets_latest fetch) — D-01.1 locked
- **G-03 chaos primitive 重设计**: in-band `POST /control/chaos/ws-test-kill` (HMAC-gated 复用 ControlAuthMiddleware, 翻进程内 atomic flag, 不重启) — D-03 locked
- **G-04 RSS 测对进程**: /health `process:rss_kb` 子检查 (psutil 当前进程) + psutil dev→runtime + Makefile RSS 改读 /health — D-04 locked
- **D-06 verdict re-run**: 修完三个后重跑 chaos-l2-inj4-throughput 拿真 verdict + 观察 Pitfall 4 watchdog false-trip — D-06 locked

Plans:
- [x] 04.1-01-PLAN.md — Wave 1 — G-02 eager startup-prime in l2_main.py (cross-restart candidate-set robustness)
- [x] 04.1-02-PLAN.md — Wave 1 — G-04 /health process:rss_kb sub-check + psutil dev→runtime + chaos Makefile RSS via /health
- [x] 04.1-03-PLAN.md — Wave 2 — G-03 in-band HMAC /control/chaos/ws-test-kill + process-local flag + make chaos-ws-kill (THREAT MODEL)
- [x] 04.1-04-PLAN.md — Wave 3 — deploy + prod chaos re-run for real D-06 verdict + Pitfall 4 observation (checkpoint:human-verify)

### Phase 05: WS /book + /prices 增量推送 (L2 → L3 升级)

> PLANNED 2026-06-01 (SESSION 34) — 16 D-XX decisions locked in CONTEXT, 1402-line RESEARCH, 13-file pattern map. Scope re-defined per D-01: 字面 "WS /book+/prices" 已在 Phase 03 落地; 本 phase 是 L2→L3 跨层升级 — full depth 持久化 + OHLC K 线 + 自动 promote + dashboard `/l3/[asset_id]`。

**Goal:** 把 Phase 03 已落地的 L2 通路升级到 L3 单市场层：自动 promote (5-min cron, top-5 markets) + full book depth (`l2_book_levels` top-10 levels/边) + OHLC views (`l2_ohlc_1m/5m/1h` via Postgres `date_trunc`, NOT `time_bucket` — TimescaleDB deprecated on Supabase PG17) + WS 动态 subscribe/unsubscribe API + 3 /health L3 sub-checks (chain-truth) + dashboard /l3/[asset_id] K-line via lightweight-charts v5.
**Status:** 📋 PLANNED (2026-06-01 SESSION 34) — 6 plans, 5 waves
**Depends on:** Phase 03 (L2 daemon ready) + Phase 04 (Supabase markets_latest read path) + Phase 04.1 (GAP-401 watchdog liveness gate — must NOT regress)
**Plans:** 2/6 plans executed
**Refs:**
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-CONTEXT.md` (16 D-XX decisions locked)
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-RESEARCH.md` (1402 lines — Pitfall 1 date_trunc; Pitfall 2 ws send-after-connect API gap; Pitfall 5 candidate-refresh race)
- `.planning/workstreams/m1-perception/phases/05-ws-book-prices/05-PATTERNS.md` (13 files mapped + 5 cross-pattern decisions)
- `.planning/quick/260531-gap-401-watchdog-false-trip/SUMMARY.md` (GAP-401 liveness gate — DO NOT REGRESS)
- memory `feedback_cold-start-debounce-trap-2026-05.md` (sentinel-None pattern for new `_last_X_at_s` anchors)

Scope (16 locked D-XX):
- **D-01 Scope**: L2 → L3 升级 (NOT WS plumbing rewrite — Phase 03 已 ship)
- **D-02/05/09/13/14 L3 Promote**: SQL rule via scanner recipe `l3-promote.yaml` (D-13 thresholds yaml-only, NOT env per CLAUDE.md "experiment values never touch baseline defaults"); N=5; 5-min cron
- **D-03/06 OHLC**: Postgres `date_trunc` (TimescaleDB deprecated on Supabase PG17); regular view (not MV); 1m/5m/1h granularity
- **D-04/07/10 Full Book Depth**: `l2_book_levels` top-10/side; unified `l2_*` namespace
- **D-08 Dashboard**: `/l3/[asset_id]` + L3 badge on candidates page
- **D-11 WS Sub**: dynamic add/remove subscribe (NEW API on WsConsumer — current client only subscribes on (re)connect)
- **D-12 Done**: 5 L3 markets 24h soak + OHLC view returns data + book depth in DB + dashboard K-line renders
- **D-15 Deploy**: same process as polyarb-l2 fly app (asyncio task, no new Fly app)
- **D-16 Carry**: ship 04.1 code-review fixes + GAP-401 liveness gate with this deploy (happy-path equivalent)

不在 scope (deferred to Phase 06+):
- Multi-granularity OHLC (15m/4h/1d) — start with 1m/5m/1h
- REST `/prices-history` cold-start backfill
- Materialized view + pg_cron (regular view first)
- L3 signal strategies (M4 workstream)
- Yes/No 双 token L3 单边 promote (v2 optimization)

Plans (6 plans across 5 waves; Wave 1 parallel; Wave 5 has 2 checkpoints — deploy + 24h soak):
- [x] 05-01-PLAN.md — Wave 1 (autonomous, parallel w/ 02): Alembic 005 — `l2_book_levels` table + 3 OHLC views (date_trunc) + `l2_candidates.l3_promoted_at_ts` column + Wave 0 schema-contract tests + `make supabase-migrate-test` target
- [x] 05-02-PLAN.md — Wave 1 (autonomous, parallel w/ 01): WsConsumer `add_subscriptions`/`remove_subscriptions` API + split `_candidate_set`/`_l3_active_set` (Pitfall 5 race fix) + l2_candidate_refresh `update_candidate_set` helper (no more raw `_subscribed_assets` clobber) + GAP-401 liveness gate preserved
- [ ] 05-03-PLAN.md — Wave 2 (autonomous, depends 01+02): `_book_levels_rows_from_frame` projector + `L2SupabaseMirror.push_book_levels` (fail-soft envelope + chain-truth anchor mutation) + `l2_main._on_event` dispatcher gate on `_l3_active_set` + `l3_promote.py` module scaffold (state + getters + stub `promote_run`)
- [ ] 05-04-PLAN.md — Wave 3 (autonomous, depends 03): `l3-promote.yaml` (D-13 thresholds) + full `promote_run` (Supabase tob fetch + scanner temp-DB + diff + ws_consumer add/remove + last-known-good freeze on outage) + raw asyncio `run_periodic` (5-min cron, NO apscheduler dep) + 3 /health L3 sub-checks (chain-truth getters)
- [ ] 05-05-PLAN.md — Wave 4 (autonomous: false — [BLOCKING] alembic push to prod; depends 01+02+03+04): Task 1 [BLOCKING] `make supabase-migrate` push 005 to prod Supabase + dashboard `/l3/[asset_id]` page + `KlineChart.tsx` (lightweight-charts v5 dynamic import) + `DepthLadder.tsx` + candidates page L3 badge column + 3 new Makefile targets (`l3-promote-dry-run` / `ohlc-spot-check` / `smoke-l3-dashboard`)
- [ ] 05-06-PLAN.md — Wave 5 (autonomous: false — 2 checkpoints; depends 01-05): flyctl deploy polyarb-l2 + 24h prod soak + 3-sub-indicator STRICT N=5 verdict (Blocker #5 — all 3 must == 5 for GREEN; no YELLOW fallback) + GAP-401 prod re-verification (carry-over) + docs/learning/11-L3-K线.md teaching doc (Chinese, 5 自检题) + VALIDATION nyquist_compliant flip + STATE/ROADMAP phase closure

Makefile targets to deliver (per Plan 05-01 + 05-05):
- `make supabase-migrate-test` — Alembic 005 forward+reverse roundtrip validation (Plan 05-01)
- `make l3-promote-dry-run` — single promote tick locally, prints candidate set (Plan 05-05)
- `make ohlc-spot-check` — query /health for current L3 active set + freshness anchors (Plan 05-05)
- `make smoke-l3-dashboard asset_id=<id>` — HTTP smoke against `/l3/[asset_id]` route (Plan 05-05)

### Phase 6: 04.1 d01-restart-robustness-chaos-redesign

**Goal:** [To be planned]
**Requirements**: TBD
**Depends on:** Phase 5
**Plans:** 0 plans

Plans:
- [ ] TBD (run /gsd-plan-phase 6 to break down)

---

*Workstream: m1-perception*
