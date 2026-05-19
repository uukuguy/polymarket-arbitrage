---
phase: 02
phase_name: "l1-production-grade"
workstream: "m1-perception"
project: "polymarket-arbitrage"
generated: "2026-05-20"
counts:
  decisions: 18
  lessons: 15
  patterns: 14
  surprises: 9
missing_artifacts:
  - "02-VERIFICATION.md (Phase 02 用 02-SOAK-LOG.md + chaos injection trail 代替 — 已用 SUMMARY/SOAK-LOG 抽取)"
  - "02-UAT.md (无独立 UAT 文档 — 验收通过 Inj 2-v2 hard gate 凭证)"
plans_covered: 9
plans:
  - "02-01-page_fetched_at_ms + triple-check"
  - "02-02-Starlette daemon + scheduler"
  - "02-03-Supabase mirror + R2 archive"
  - "02-04-Dockerfile + Fly deploy"
  - "02-05-Sentry + Better Stack + Telegram"
  - "02-06-Vercel dashboard"
  - "02-07-Chaos injection (Phase 02 hard gate)"
  - "02-08-Plan 03 retro fix-up (F-01..F-05)"
  - "02-09-Streaming paginator (D-23, OOM resolution)"
---

# Phase 02 Learnings: l1-production-grade

> **Phase outcome**: L1 daemon `polyarb-l1.fly.dev` 真 LIVE — Gamma fetch / SQLite / Parquet / Supabase mirror / R2 archive / Sentry+Telegram+Better Stack alert chain 全链路云上跑通。Hard gate (alert chain end-to-end verified live in prod chaos) ✅ PASSED 2026-05-20 via Inj 2-v2.
> **Gate variant**: 原 thread §1 "7-day soak + uptime ≥99%" 被替换为 "5 prod chaos injection 真验证 alert chain"。Phase 03 必须回补真 soak。
> **Phase 02.1 backlog**: 8 个 bug 中 5 个 P0 本会话已修,3 个 deferred (2 P1 + 1 trade-off)。

## Decisions

### D-01..D-04: Deployment stack 锁定 — Fly AMS + Supabase Dublin + R2 + all-eu region
四锚点 PaaS 栈在 discuss 阶段已固化。Fly AMS 离 Polymarket London ~10ms; Supabase Dublin 同 region; R2 Free tier cover parquet 归档; 整栈 EU 内部避免跨洋。

**Rationale:** thread `deployment-architecture.md` §0.1 用户 4 锚点决策 + thread §2.1.7 Fly 评 "首选" + Trading-readiness ★★★★★ (M3 实盘不用换栈)
**Source:** 02-CONTEXT.md (D-01..D-04)

---

### D-05/D-06: Single prod environment + GHA build + flyctl deploy
不上 staging,先单 prod 跑;CI 跑 lint+test+pyright 全绿后 flyctl deploy。拒绝 PaaS 原生 Git 直推 (没 test gate)。

**Rationale:** L1 只读 daemon 无数据破坏风险;test gate 是工程纪律下限。clawfirm + polymarket-kalshi-weather-bot 业内同行都用同模式。
**Source:** 02-CONTEXT.md (D-05/D-06)

---

### D-12: 三态健康判定 + post-write fail-soft (mirror/R2 失败不中断 snapshot)
OK/DEGRADED/FAILED 三档 (jitter <1%/1-5%/≥5%)。Supabase mirror + R2 upload 是 post-write,失败仅升级 health status 不回滚 SQLite。

**Rationale:** D-12 amendment + LEARNINGS P5 (fail-soft)。SQLite 是 source of truth,远端 mirror 必须 fail-soft 否则任何 SaaS 抖动都拖垮 L1。Inj 3 (撤 Supabase secret) 验证主契约 ✅。
**Source:** 02-CONTEXT.md (D-12) + 02-03-SUMMARY.md key-decisions

---

### D-13: 连续 3 次 FAILED → daemon PAUSED + 持久化 failure counter
RUNNING → PAUSED 状态机,counter 存 SQLite `scheduler_state` singleton (CHECK(id=1));重启不丢。DEGRADED **不计** failure。

**Rationale:** 避免 cron 狂重试贬值 API quota;PAUSED 是 explicit ops state 不是 crash;Inj 2-v2 真触发并产生 alert ✅。
**Source:** 02-CONTEXT.md (D-13) + 02-02-SUMMARY.md key-decisions

---

### D-14..D-17: 4-leg observability stack (Sentry + Axiom + Better Stack + Telegram)
Sentry (5k errors/月 Free) + Axiom (500GB/月 ingest, deferred 实际接入) + Better Stack (10 monitor × 30s Free) + Telegram bot (无 quota)。Telegram 走 daemon-direct 不依赖 BS 中转。

**Rationale:** Free tier 全覆盖;每条路径独立保证冗余;CN/港开发者 Telegram 主流推送通道。Inj 2-v2 三路真触发 ✅。
**Source:** 02-CONTEXT.md (D-14..D-17) + 02-05-SUMMARY.md key-decisions

---

### D-18..D-22: Vercel + Next.js dashboard + magic-link + EMAIL_WHITELIST 单用户
read-only 状态面板 + scan trigger 按钮 (Edge Function HMAC-forward to Fly)。Vercel + Supabase JS SDK + magic-link auth + 中间件白名单。

**Rationale:** scan trigger 是观察不是策略,不违反 thread §1 层级纪律。Vercel Edge cross-org 无法访问 Fly Flycast 内部网络 → D-22 amendment 改为 public + HMAC。
**Source:** 02-CONTEXT.md (D-18..D-22) + 02-06-SUMMARY.md key-decisions

---

### D-22 amendment: /scan + /health 全 PUBLIC + HMAC 中间件 (Flycast cross-org 不可行)
原 plan D-22 想用 Fly internal network only,researcher 实证 Vercel Edge 跨组织无法 route through `fly-local-6pn` → 改 public + `hmac.compare_digest` constant-time。

**Rationale:** Stripe/GitHub/Shopify webhook 同模式;HMAC over body 与 internal-only 等效安全。
**Source:** 02-02-SUMMARY.md key-decisions + 02-04-SUMMARY.md

---

### D-23: 流式分页是 L1 生产稳定的硬约束 (SESSION 18 OOM 后 amendment)
2026-05-15 Plan 02-04 首次 prod deploy (256MB Fly VM) 观测到 daemon OOM-killed。Root cause: paginator 累积 20k stripped dicts ≈ 160MB 常驻 + normalize 中间结构。约束: 任何后续 L1/L2 数据源接入必须 streaming-by-default,不准累积全量 list。

**Rationale:** thread `market-observation-architecture.md` §2.8 OOM 事故根因;`fix-code-not-config` + `profile-with-real-data` 两条 feedback 纪律。
**Source:** 02-CONTEXT.md (D-23 amendment 01) + 02-09-SUMMARY.md

---

### W6: 双 Supabase URL 约定 (REST SDK vs Postgres DSN)
`POLYARB_SUPABASE_URL` (HTTPS REST,supabase-py 用) vs `POLYARB_SUPABASE_DB_DSN` (postgresql://,Alembic 用)。两者格式完全不同不可复用。

**Rationale:** supabase-py 走 PostgREST,Alembic 走 raw Postgres。统一 URL 会导致 SDK 报错且诊断难。
**Source:** 02-03-SUMMARY.md key-decisions

---

### F-01..F-05: Plan 03 retro fix-up (Plan 02-08)
5 个 landing-time 偏差一次性补正: F-01 (init_schema 不 ALTER 老表) / F-02 (update_parquet_url upsert 制造 NOT NULL 违反) / F-03 (top_movers_view 承诺未交付) / F-04 (daemon SIGINT 10s) / F-05 (0-market snapshot 触发 mirror)。

**Rationale:** 用 retro fix-up plan 而非污染 02-03-SUMMARY 是 plan-末纪律的体现 — 错了不改 SUMMARY,补 plan。
**Source:** 02-08-SUMMARY.md

---

### Inj 2-v2 fast scheduler_interval (30s) 替代 1h 默认让 chaos 真触发
原 1h interval 跑 3 次 FAILED 要 3 小时,人工 chaos 不现实。修 P0 (`scheduler_interval_s` 可配) 后 30s × 3 = 90s 完整跑出 PAUSED state machine。

**Rationale:** chaos test 设计目标 = 让代码路径真实触发,interval 是手动 override 的合理参数化。
**Source:** 02-07-SUMMARY.md key-decisions

---

### alerts.py Telegram direct 升为无条件主路径 (Inj 1 后)
原 fallback only (`if not bs_ok: telegram_direct(...)`)。Inj 1 暴露 BS /fail 200 OK 时 TG 永远不发 → 用户实际未通知。改为无条件主路径,BS 失败保留兜底。

**Rationale:** "HTTP 200 ≠ user notified" 教训。TG 是用户实际收到通知的最可靠路径,不能依赖 BS routing。
**Source:** 02-07-SUMMARY.md (Bug #1) + alerts.py b4de60c

---

### 关闭凭证从 "7-day soak" 改为 "5 prod chaos injection"
Supabase Free tier 7 天 idle auto-pause → soak 必断;用户拒升 Pro $25/月。改用 chaos injection 真触发 alert chain 作为 hard gate。Phase 03 必须回补。

**Rationale:** SaaS 经济约束 + thread §1 生产级判定的本质是 "alert chain end-to-end works",chaos 直接验证而非 soak 等自然失败。decision trace: `threads/soak-gate-deviation-2026-05.md`。
**Source:** 02-07-SUMMARY.md + 02-SOAK-LOG.md

---

### /movers 用 top_movers_view 不等真 cross-snapshot delta (Plan 02-08 Alembic 002)
markets_latest 是 full-overwrite 无历史,真 delta 需 markets_history 表。F-03 补 `top_movers_view` ORDER BY abs(mid_price-0.5) 作为 uncertainty proxy。

**Rationale:** schema add-only 纪律 (LEARNINGS P7) — 不为 UI 加 markets_history 表;真 delta 推 Phase 02.1+。Plan 06 dashboard 消费 view。
**Source:** 02-06-SUMMARY.md key-decisions + 02-08-SUMMARY.md (F-03)

---

### Fly VM 1024MB scale-up after streaming (Plan 02-09)
256 → 512 → 1024 MB 三次升 → 真 fix code (streaming) + profile with real data 后,1024MB 是合理终态 (~500MB headroom for 2× growth)。$7.12/mo vs 多进程架构 trade-off 明显。

**Rationale:** "scaling after code is optimized AND profiled with real data is NOT avoidance" — feedback `fix-code-not-config-2026-05` 加 caveat 更新。
**Source:** 02-09-SUMMARY.md (Decision rationale section)

---

### Vercel 删除 stale `.git/config [user]` (不重写 173 commits)
Vercel Hobby tier author verification 拒部署 `firmwwwee@fastmail.com` 作者 commit。三选一: (A) rewrite history (B) Vercel paid (C) **删 project-level [user] section** → 新 commit 继承 global `uukuguy@gmail.com`。选 C。

**Rationale:** Rewriting 173 commits 会让所有 SUMMARY 里的 SHA 引用失效。Vercel 只 verify 部署的 commit,不 verify history。memory `git-identity-anomaly-2026-05` 同步落地。
**Source:** 02-06-SUMMARY.md key-decisions

---

### Edge Runtime HMAC 用 Web Crypto API 不用 Node crypto
`crypto.subtle.importKey('raw', SECRET, {name:'HMAC',hash:'SHA-256'}, ...) + sign + Buffer.toString('hex')`。不用 `crypto.createHmac` (Edge Runtime 没 Node `crypto` 模块)。

**Rationale:** `runtime = 'edge'` 是性能选择 (global low-latency),但 Node 模块不可用;Web Crypto 跨 runtime 通用。
**Source:** 02-06-SUMMARY.md patterns-established Pattern B

---

### Axiom log-shipping 推 P2 backlog (5-day Fly buffer 暂代)
Plan 02-05 Task 4 Step B 想用 "Fly Monitoring → Configure Axiom",flyctl 无此命令 + Free tier 不让走 Axiom external sources wizard。realistic: 独立 `superfly/fly-log-shipper` Fly app (~1 free machine)。punted。

**Rationale:** Sentry 90 天 + Better Stack 30 天 retention 短期够;Wave 5 soak 数据真显示需要 Axiom 再上。先 trade off 复杂度。
**Source:** 02-05-SUMMARY.md key-decisions + Known Limitations

---

## Lessons

### L1: Plan budget table 是 sketch 不是 contract (OOM 教训)
Plan 02-09 设计时 "30MB delta / 105MB peak" 是设计估算 — 实际 macOS pytest ~80-90MB delta / ~285MB abs, Linux Fly daemon ~402MB peak。

**Context:** Plan-check 两轮都没把 6700×3.5KB target_markets ≈25MB 真值代回 budget table;pyarrow C-side 分配 tracemalloc 看不到;macOS vs Linux glibc 差 ~80MB。
**Source:** 02-09-SUMMARY.md (Deviation section)

---

### L2: 分页 ≠ 流式 (HTTP-level vs application-level)
HTTP pagination 防止单页响应过大;但代码 `for page: results.extend(page)` 仍累积全量 → 一样 OOM。streaming = `async for raw in iter(): yield ...` 不是 `extend`。

**Context:** Plan 02-04 OOM 真根因 = `_paginate` 返回 `list[dict]`。Plan 02-09 改 `AsyncIterator[dict]` + orchestrator streaming consumer 才真解决。
**Source:** 02-09-SUMMARY.md (Lessons learned)

---

### L3: macOS pytest peak ≠ Linux daemon peak
glibc C-allocator vs darwin 差 ~80MB (long-running arena retention)。pyarrow C 端分配 tracemalloc 不可见。本地 profiling 数字必须 Fly logs `anon-rss` 验证。

**Context:** Plan 02-09 OOM 在 512MB Fly 才暴露 — macOS profiling 显示 ~285MB peak,Linux 真打到 402MB。
**Source:** 02-09-SUMMARY.md (Empirical numbers section)

---

### L4: 修代码不是加内存 (但 profile 后 scale-up 不是绕过)
Plan 02-04 三次升内存 (256→512→1024→2048) 都是错方向 — root cause 是 paginator 保留 50+ 字段完整响应。strip 到 15 字段后 256MB 够用。但 Plan 02-09 streaming 之后 1024MB scale 是合理 (data-resident working set 真的需要)。

**Context:** feedback `fix-code-not-config-2026-05` 加 caveat: "scaling after code is optimized AND profiled with real data is not avoidance"。
**Source:** 02-04-SUMMARY.md (关键教训) + 02-09-SUMMARY.md

---

### L5: asyncio 协作调度不是免费的
`await httpx.get()` 返回太快 (HTTP/2 ~40ms) 时其他 coroutine 拿不到 cycle → uvicorn 永远没 bind socket → health check timeout。必须显式 `asyncio.sleep(0)` yield。

**Context:** Plan 02-04 部署期发现 health check 失败 — scheduler._tick() monopolize event loop。
**Source:** 02-04-SUMMARY.md (关键教训 + ecf38b3 commit)

---

### L6: HTTP 200 ≠ user notified (alert chain tri-layer)
监控 / 告警 / 通知是三层。Better Stack `/fail` 返回 200 OK 只证明 BS 接收 signal;是否 routing 到邮箱/TG 取决于 on-call/escalation 配置。我们 BS primary responder 邮箱没确认 = 黑洞;Sentry "Notify Suggested Assignees" issue unassigned = 黑洞。

**Context:** Inj 1 五层根因分析 — alert 全空但所有 HTTP 都 200。必须 chaos 真触发 + 用户邮箱/手机真收到才叫 verified。
**Source:** 02-07-SUMMARY.md + 02-SOAK-LOG.md (Inj 1 5 层根因) + memory `alert-chain-discipline-2026-05`

---

### L7: SESSION 20 "E2E verified" 是自我欺骗
Plan 02-05 SUMMARY 写 "4 redundant alert paths verified live in prod",实际是 `make sentry-test` 走简化路径绕过 alert rule;Better Stack 手动 POST /fail;Telegram 直 API curl。3 路全是测试按钮触发,**不是** scheduler PAUSED 真路径触发。Inj 2-v1 才暴露真 alert rule + alerts.py 路径多个 P0。

**Context:** Plan 02-07 chaos injection 才识别这是自我欺骗反模式。SUMMARY 写 "X verified" 必须附 "via [具体动作] [具体证据]"。
**Source:** 02-07-SUMMARY.md (Pattern B/C) + memory `alert-chain-discipline-2026-05` + thread learnings-meta

---

### L8: GHA setup-flyctl@v1.5 tag 不存在 (5-16 起 silent deploy fail)
所有 push-to-main 从 5-16 起 GHA 都 silently fail,但 Inj 2-v1 才暴露。Phase 02 Wave 4+5 期间所有 "deploy" 其实是用户手动 `flyctl deploy` 在本地跑。v1.5 → 1.6 修后 2m13s deploy success。

**Context:** CI/CD 失败如果没 alert 就是黑洞。GHA failure email 默认走 GitHub primary,可能没及时看。
**Source:** 02-07-SUMMARY.md (Bug #5)

---

### L9: Mock patch target = handler 的 import site 不是 definition site
`patch("polyarb.observation.scanner.run_recipe")` 不工作;必须 `patch("polyarb.http.scan.run_recipe")` (handler 的 import 处)。Python mock 标准规则但容易踩。

**Context:** Plan 02-02 test_invokes_run_recipe 调试。
**Source:** 02-02-SUMMARY.md (Decisions Made / Deviations)

---

### L10: pydantic frozen model auto-enable 用 `object.__setattr__` 绕约束
凭证在 .env 出现时自动开 `mirror_enabled=True`,但 frozen model 禁止赋值。`object.__setattr__(self, 'mirror_enabled', True)` 在 `model_validator(mode='after')` 内合法绕过。

**Context:** Plan 02-03 SupabaseMirror / R2Sync 自动开关。
**Source:** 02-03-SUMMARY.md key-decisions

---

### L11: botocore Stubber 与 upload_file 不兼容 → 用 put_object
upload_file 走 s3transfer 内部 TransferManager,Stubber 拦截不到 → 用 put_object 直接调用。

**Context:** Plan 02-03 test_r2_sync 调试。
**Source:** 02-03-SUMMARY.md (Issues Encountered)

---

### L12: `from __future__ import annotations` 让 `ann.get('x') == int` 失败
PEP 563 让注解字典存字符串 `"int"` 而非 type object。断言要 `ann_val in (int, "int")` 兼容。

**Context:** Plan 02-03 test_r2_key_rejects_user_input。
**Source:** 02-03-SUMMARY.md (Deviations)

---

### L13: bash printf %02d "08" octal trap (plan ≥ 08 silent block)
`.githooks/pre-commit` 用 `printf '%02d' "$PLAN"` 在 `set -e` 下对 "08"/"09" 报 invalid number。`$((10#$PLAN))` 强制 base-10。Plan 1-7 silently work,Plan 02-08 才暴露。

**Context:** Plan 02-08 Task 2 commit 时发现 — Task 1 巧因 stale COMMIT_EDITMSG 短路过。
**Source:** 02-08-SUMMARY.md (Auto-fixed Issues)

---

### L14: Next.js 15 pnpm typecheck OK 不等于 pnpm build OK
`pnpm typecheck` 全绿,`pnpm build` 报 "next/headers in Client Component"。原因: 单文件混 browser+server factory,Server Component 与 Client Component 共享 import 树违反 App Router boundary。

**Context:** Plan 02-06 部署时构建失败。fix: 拆 supabase-browser.ts + supabase-server.ts。
**Source:** 02-06-SUMMARY.md key-decisions + Lib Split (04cfe3b fix)

---

### L15: Schema mismatch debug protocol — adapt UI, don't grow schema
Plan 02-06 executor draft 选 3 个 Supabase 不存在的列 (parquet_r2_url / supabase_mirror_at_ms / is_valid)。正确做法: 读 Alembic migration 是 source of truth,改 UI 匹配 schema,不为 UI 加 Alembic 003。

**Context:** Schema add-only 纪律 (LEARNINGS P7 from Phase 01.1) 在 dashboard 工作再次生效。
**Source:** 02-06-SUMMARY.md key-decisions + Pattern D + /status Schema Adaptation section

---

## Patterns

### P1: 4-point schema lockstep (DDL / COLUMN_ORDER / INSERT_SQL / SNAPSHOT_SCHEMA)
任何列变更必须同步修改 4 个常量;test_schema_lockstep.py 强制执行回归。markets 表 4-point,events 表 3-point (无 parquet schema)。

**When to use:** 任何加列 / 改类型;`page_fetched_at_ms` (Plan 02-01)、`supabase_mirror_at_ms` + `parquet_r2_url` (Plan 02-03)、`scheduler_state` (Plan 02-02) 全走此模式。
**Source:** 02-01-SUMMARY.md + 02-03-SUMMARY.md

---

### P2: Post-write fail-soft adapter (orchestrator step 7.5/7.6)
SQLite + parquet 先写;mirror / R2 upload 是 post-write fan-out。失败 → log warn + Sentry breadcrumb + DEGRADED Issue,**不中断快照**,**不回滚 SQLite**。

**When to use:** 任何 "本地是 source of truth, 远端是镜像" 的 sync;D-12 amendment 严格契约。
**Source:** 02-03-SUMMARY.md (post-write fail-soft) + 02-08-SUMMARY.md F-05

---

### P3: Singleton state table (CHECK(id=1))
`scheduler_state` 单行 + CHECK 约束。`INSERT OR REPLACE` upsert pattern。survives daemon restart。

**When to use:** 任何 daemon-level singleton state (failure counter / pause state / heartbeat tick);避免 app-level guard。
**Source:** 02-02-SUMMARY.md (key-decisions)

---

### P4: Module-attribute monkeypatch pattern
`from polyarb.daemon import alerts as _alerts; await _alerts.send_paused_alert(...)` 不写 `from .alerts import send_paused_alert`。tests 可以 `patch('polyarb.daemon.alerts.send_paused_alert', new=AsyncMock())` 因为 lookup 在 call time 不是 import time。

**When to use:** 任何需要 mock 的异步副作用调用;比 dependency injection 轻量。
**Source:** 02-05-SUMMARY.md (Pattern D)

---

### P5: Shared redact source-of-truth (single file owns regex + key-name list)
`polyarb.observability.redact` 单文件 `redact_secrets(text)` + `_KEY_PATTERNS`。logging.py (loguru filter) + sentry.py (before_send hook) 各 import 一次。加新 secret = 一处编辑 + 两处测试。

**When to use:** 任何跨模块的字符串过滤/重写;避免循环依赖。
**Source:** 02-05-SUMMARY.md (Pattern A)

---

### P6: Dedup-by-situation (not by message)
`alerts.py _recent_alerts: dict[str, float]` keyed by alert name (`scheduler-paused`)。300s 内第二次 = no-op。防止 3 次 FAILED 连发 3 个 PAUSED alert。

**When to use:** 任何 user-facing 通知,situation 是 dedup key 而不是 message hash。
**Source:** 02-05-SUMMARY.md (Pattern C)

---

### P7: Sentry breadcrumb for fail-soft events
orchestrator step 7.5/7.6 fail 调 `sentry_sdk.add_breadcrumb(level='warning', category='mirror', message=...)`。breadcrumb 附在下次 captured event,不自己 fire alert。

**When to use:** 任何 fail-soft 路径,想留 context 给下个真正 critical event 但不想吵闹 user。
**Source:** 02-05-SUMMARY.md (Pattern B)

---

### P8: Idempotent migration (PRAGMA + conditional ALTER ADD COLUMN)
`_ensure_column(table, col, type)` helper: `PRAGMA table_info(table)` → if col missing → `ALTER TABLE ADD COLUMN`。Never DROP/RENAME/RETYPE (LEARNINGS P7)。

**When to use:** 任何 legacy DB schema 升级 (含本地 dev DB + 已部署 prod);取代 Alembic SQLite-side 缺位。
**Source:** 02-08-SUMMARY.md (F-01 + patterns-established)

---

### P9: Server-started gate (uvicorn 先 bind 再启 scheduler)
`main.py asyncio.gather` 之前 await `server.started`,确保 scheduler `_tick()` 第一次执行前 uvicorn 已 bind socket → /health 立刻 responsive。

**When to use:** 任何 HTTP + 长跑 task 共存 daemon。
**Source:** 02-04-SUMMARY.md (patterns-established)

---

### P10: Field-stripping paginator (memory budget control)
`_paginate(keep_fields=[...])` 每页 fetch 后只保留 ~15 字段 (从 50+ 缩),del raw dict。streaming + stripping 联合控制 working set。

**When to use:** 任何分页 API 返回 fat dict 但实际只用少量字段。
**Source:** 02-04-SUMMARY.md (patterns-established + OOM fix 1a97200)

---

### P11: HMAC X-Signature middleware (Stripe/GitHub webhook pattern)
`scan_auth_middleware`: read body → `hmac.new(secret, body, sha256).hexdigest()` → `hmac.compare_digest(sig, expected)` constant-time。401 on missing/invalid。

**When to use:** 任何 public endpoint 想要 secret-based auth (no OAuth/JWT overhead);跨 platform/runtime 边界 (Vercel Edge ↔ Fly)。
**Source:** 02-02-SUMMARY.md (HMAC-SHA256 webhook pattern) + 02-06-SUMMARY.md (Pattern B Edge Runtime)

---

### P12: Server/Client Supabase split (Next.js 15 App Router)
`lib/supabase-browser.ts` (createBrowserClient, Client Component 用) vs `lib/supabase-server.ts` (createServerClient + cookies(), Server Component / Route Handler 用)。两者**严禁**互相 import。

**When to use:** 任何 Next.js 15 App Router + Supabase Auth;`pnpm typecheck` 不抓但 `pnpm build` 会拒的 boundary 违反。
**Source:** 02-06-SUMMARY.md (Pattern + Lib Split section)

---

### P13: Whitelist gate in middleware.ts (server-side, not page.tsx)
`middleware.ts` matcher 覆盖 protected paths → 检 Supabase session cookie → 比 EMAIL_WHITELIST env → 不匹配 redirect /login。防止 UI 短暂 flash protected content during hydration。

**When to use:** 单用户 / 小白名单 dashboard;比 RBAC 轻量但有效。
**Source:** 02-06-SUMMARY.md (Pattern C)

---

### P14: Chaos injection 反推设计 (不是测哪些 chaos,是反推哪些代码路径触发 alert)
不问 "测哪 5 个 chaos";问 "alert chain 哪几条独立路径,逆向构造场景让那条路径真触发"。Inj 2 = 反推 PAUSED → send_paused_alert,Inj 3 = 反推 fail-soft 主契约。

**When to use:** 任何 alert/告警工程的验证设计;比 "广撒网测试" 更系统。
**Source:** 02-07-SUMMARY.md (Pattern A) + thread learnings-meta

---

## Surprises

### S1: 256MB Fly VM 可用 RSS ~150MB (kernel/init 占 ~100MB)
Fly 分配 256MB ≠ 可用 256MB。Linux microVM 基线 kernel/init/sshd 已占 ~100MB,实际用户进程只剩 ~150MB。Plan 02-04 第一次 deploy 没意识到。

**Impact:** OOM 时间表混淆 — "我才用 200MB 怎么爆了" 答案是 VM ceiling 已经在 150MB 附近。FLY VM table 加入 02-09-SUMMARY.md。
**Source:** 02-04-SUMMARY.md (Fly microVM 可用内存 ≠ 分配内存) + 02-09-SUMMARY.md (margin table)

---

### S2: macOS profiling vs Linux Fly 内存差 ~80MB (glibc allocator)
Plan 02-09 macOS pytest peak ~285MB,Linux Fly daemon 402MB,差 117MB。glibc long-running arena retention + macOS Mach memory 分配策略不同。

**Impact:** 任何 memory budget 必须 deploy 后从 Fly logs anon-rss 验证,不能信任本地 profiler 数字。
**Source:** 02-09-SUMMARY.md (Empirical numbers + Why plan was wrong)

---

### S3: Polymarket Gamma API offset>10000 强制 422 (新约束 2026-05)
`_paginate` 在 offset=10000 撞 422 → `_NonRetryableHTTPError`。Plan 02-04 deploy 期间 prod 真撞到,daemon crash → 加 422 catch 返回 partial data。

**Impact:** Phase 02.x 必须修分页架构 (cursor/since 模式) 否则 markets 总数破 10k 后 silent truncation。memory 同步落地。
**Source:** 02-04-SUMMARY.md + thread deployment-architecture.md §10.3

---

### S4: Vercel Hobby tier author verification 强匹配 GitHub account
deploy 的 commit author email 必须 match 部署 Vercel 项目的 GitHub account。`firmwwwee@fastmail.com` (Claude 凭空构造的 identity) 被拒部署。

**Impact:** memory `git-identity-anomaly-2026-05` 落地 + 173 历史 commit 不可修复 (会让所有 SUMMARY SHA 引用失效) — deferred indefinitely。
**Source:** 02-06-SUMMARY.md (Vercel Deploy Author Verification Block)

---

### S5: pnpm typecheck 不抓 Next.js 15 boundary 违反
`pnpm typecheck` 完全 clean。`pnpm build` 才报 `next/headers in Client Component`。原因: TS 不感知 Next.js "use server" / "use client" runtime split。

**Impact:** dashboard/M5 CI 必须跑 `pnpm build` 不能只跑 typecheck。教训入 P12 (server/client split)。
**Source:** 02-06-SUMMARY.md (Lib Split section)

---

### S6: Better Stack `/fail` POST 返回 200 ≠ 用户被通知
BS heartbeat 产品的 /fail endpoint 只是 "signal accepted",routing 到邮箱/TG 取决于 on-call/escalation policy。Free tier 没 Escalation Policies,只有 per-heartbeat default "Notify primary responder + E-mail"。primary responder 邮箱没显式 verify → 黑洞。

**Impact:** alerts.py Telegram 升为无条件主路径 (Bug #1 fix)。memory `alert-chain-discipline-2026-05` L6 教训化。
**Source:** 02-07-SUMMARY.md + 02-SOAK-LOG.md (Inj 1 5 层根因 #4)

---

### S7: send_paused_alert 在 Inj 1 短停 (2 min) 期间从未被调用
Inj 1 设计假设 = Fly stop → 探针 miss → alert。**真相**: 2 min 内根本没到 cron tick (0/12h UTC),0 次 FAILED → counter 不增 → PAUSED 不进 → send_paused_alert 永不调用。Inj 1 全空 alert 的根因 #2。

**Impact:** chaos 设计必须 trigger 真实代码路径 (cron tick 真发生),不能只 trigger infra 层 (machine stop)。Inj 2-v2 用 fast 30s interval 才补上。
**Source:** 02-SOAK-LOG.md (Inj 1 根因 #2) + 02-07-SUMMARY.md

---

### S8: GHA setup-flyctl@v1.5 tag 不存在 (silent silent failure 5-16 → 5-20)
不存在的 tag 让 setup action 直接 fail。Plan 02-04 commit 5-15 → 5-16 起所有 push-to-main GHA failed。但 GitHub email notification 用户没及时注意 → 4 天 silent。所有 Wave 4-5 期间 "deploy" 实际都是用户本地 flyctl。

**Impact:** silent CI failure 比真 bug 更危险;CI 失败必须有独立 alert path。Bug #5 fix v1.5 → 1.6 后 2m13s success。
**Source:** 02-07-SUMMARY.md (Bug #5)

---

### S9: 6700 × 3.5KB target_markets 真值 = 25MB (plan 估错 10×)
Plan-check round 1 用 "few hundred × 2KB" 估 target_markets,实际 6700 × 3.5KB (stamped + book attached) ≈25MB。round 2 修了 test threshold ($10k→$1k) 但没回代 budget table。10× 低估直接导致 OOM。

**Impact:** plan budget 不是 sketch,必须用真实分布数据 (fixture log-normal μ=ln(500) σ=2) 代回 budget table。L1 lesson 同步。
**Source:** 02-09-SUMMARY.md (Why plan was wrong)

---

*Phase 02 closed: 2026-05-20*
*Workstream: m1-perception*
*Total: 9 plans + 1 retro fix-up plan, ~12h chaos verification, 5 P0 bug fixed live, alert chain hard gate ✅ PASSED via Inj 2-v2*
*Next: Phase 02.1 backlog (2 P1 + 1 trade-off) 消化 → Phase 03 (L2 orderbook) 启动前必须先回补真 soak*
