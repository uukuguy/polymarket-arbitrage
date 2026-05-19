---
phase: 02-l1-production-grade
plan: 07
workstream: m1-perception
subsystem: chaos-engineering + production-verification
tags: [chaos-engineering, alert-chain, sentry, telegram, fail-soft, scheduler, state-machine, production-gate]

requires:
  - phase: 02-04
    provides: Fly deploy + /scan HMAC + /health endpoint live in prod
  - phase: 02-05
    provides: send_paused_alert + send_heartbeat_ok + Sentry + Better Stack + Telegram channels wired
  - phase: 02-06
    provides: Vercel dashboard (read-only, not in Inj scope)

provides:
  - "Chaos test suite (mocked CI) — 22 tests covering Gamma 5xx / CLOB malformed / Supabase 500 / R2 503 / 3× FAILED → PAUSED / /scan flood / SQLite WAL concurrency"
  - "scripts/soak_monitor.py — Better Stack API status/export, Phase 02 soak gate audit tool"
  - "docs/learning/08-生产化部署.md — 251 行 Phase 02 教学文档"
  - "5 个 prod chaos injection 完成,Phase 02 alert chain hard gate ✅ verified live"
  - "scheduler_interval_s 修复 (P0 bug,从 1h 写死改可配)"
  - "alerts.py Telegram direct unconditional (P0 bug,Inj 1 暴露 BS 200 ≠ user notified)"
  - "Makefile alerts-test 加 init_sentry (P0 bug,SDK 静默 no-op)"
  - "GHA deploy.yml setup-flyctl 版本修复 (P0 bug,自 5-16 起所有 deploy 都没真生效)"

affects:
  - phase: 03 (L2 orderbook) — 必须先消化剩余 4 个 Phase 02.1 backlog
  - all m1-perception phases — chaos injection 方法论 + SOAK-LOG 体例确立
  - all observability work — "HTTP 200 ≠ user notified" + "chaos injection 是对抗 SUMMARY 自我欺骗" 教训入 thread learnings-meta

tech-stack:
  added:
    - "respx>=0.21 (dev only, chaos test HTTP mocking)"
    - "botocore stubber (dev only, R2 chaos test)"
  patterns:
    - "Chaos injection 体例: 真触发 ≠ 测试按钮触发,Send Test Notification 走简化路径绕过 alert rule"
    - "Pre-injection backup: ORIG_<SECRET>=$(flyctl ssh ... -C 'printenv ...') 必备份"
    - "Hard gate / soft gate 分层: PAUSED→alert 是硬门,其它是 caveats"
    - "Tri-layer monitoring: 监控/告警/通知是三层,HTTP 200 只能证前两层,必须 end-to-end chaos 验证最后一层"

key-files:
  created:
    - "tests/m1-perception/test_chaos_*.py (7 files, 22 tests)"
    - "tests/m1-perception/test_sqlite_concurrency.py"
    - "scripts/soak_monitor.py (168 lines)"
    - "docs/learning/08-生产化部署.md"
    - ".planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md (chaos audit trail)"
    - ".planning/threads/soak-gate-deviation-2026-05.md (thread §1 偏离决策 trace)"
  modified:
    - "src/polyarb/config.py (+5 lines, scheduler_interval_s field)"
    - "src/polyarb/daemon/scheduler.py (1 line, getattr → 直接属性访问)"
    - "src/polyarb/daemon/alerts.py (-3 +5 lines, Telegram unconditional)"
    - "src/polyarb/validator/layers.py + orchestrator.py (KeyError escape fix, Rule 1 trigger)"
    - "tests/m1-perception/test_alerts.py (新加 BS 200 case + 重命名 503 case)"
    - "tests/m1-perception/test_scheduler.py (+2 tests for scheduler_interval_s)"
    - "Makefile (alerts-test target 加 init_sentry; 3 个 soak-* target)"
    - ".github/workflows/deploy.yml (setup-flyctl@v1.5 → @1.6)"
    - ".planning/threads/learnings-meta.md (3 个新 sub-lesson 沉淀)"

key-decisions:
  - "Phase 02 关闭凭证改为 'chaos injection 真验证 alert chain' 而非 thread §1 原定的 '7-day soak'. 详见 threads/soak-gate-deviation-2026-05.md. Phase 03 必须回补真 soak."
  - "Inj 2-v2 走 fast scheduler_interval_s=30 方式让 3 次累积在 90s 跑完. 这是 P0 修复后才有的能力."
  - "alerts.py Telegram direct 升为无条件主路径. BS /fail 仍 best-effort 但不再决定 TG 是否发. 兜底保证 alert 链可达."
  - "Sentry alert rule 配置最终用 'Notify Suggested Assignees + Recently Active Members fallback', 因 'Recently Active' 兜底真 work, 不强制 'Notify Member' 显式化."
  - "/health 在 FAILED 时返 503 (IETF strict) → Fly proxy 切外部流量. 设计 trade-off, Phase 03 可考虑改'overall 200 + status in body'模式让外部探针仍能拿到状态."

patterns-established:
  - "Pattern A — Chaos injection 设计反推: 不是'测哪些 chaos',而是'反推哪些代码路径触发 alert,逆向构造场景让那个路径真触发'"
  - "Pattern B — Verdict 三态: full verified / partial verified / failed-by-design-succeeded-by-discovery. SESSION 20 的 'E2E verified' 自我欺骗反模式禁止重演."
  - "Pattern C — SUMMARY 'verified' 必须附验证手段: 不可写 'X verified', 必须写 'X verified via [具体动作] [具体证据]'."
  - "Pattern D — Pre-injection secret backup: 任何撤 secret 操作前 ORIG_X=$(flyctl ssh ... -C 'printenv X'). 本地 .env 同步备份."
  - "Pattern E — SDK init contract for ad-hoc scripts: 任何 ad-hoc Makefile target / scripts/ 走 SDK 的命令必须显式 init SDK (alerts-test 漏了 init_sentry 教训)."

requirements-completed:
  - "Plan 02-07 Task 1 — chaos test suite 8 scenarios (RESEARCH §11)"
  - "Plan 02-07 Task 2 — scripts/soak_monitor.py + Makefile soak targets"
  - "Plan 02-07 Task 3 — docs/learning/08-生产化部署.md"
  - "Plan 02-07 Task 4 (REVISED) — 5 chaos injections in prod (变体替代原 7-day soak)"
  - "Phase 02 thread §1 生产级判定标准 (变体): alert chain end-to-end verified ✅"

requirements-deferred:
  - "Original thread §1 '7-day soak + uptime ≥99%' — 因 Supabase Free tier 不能扛 7 天 auto-pause, 用户不升 Pro $25/mo. Phase 03 必须回补."
  - "Phase 02.1 backlog (5 个新发现 bug):"
  - "  1. P0 ✅ 本会话修了 — scheduler_interval_s 不可配"
  - "  2. trade-off — /health 503 触发 Fly proxy 切流量 (IETF strict vs Fly proxy 行为冲突)"
  - "  3. P1 — fail-soft 互相抵消 (撤 secret 场景 mirror_enabled=False, 0 log/0 breadcrumb)"
  - "  4. P1 — daemon PAUSED 没 prod-friendly unpause endpoint (现在需 SSH + SQL + restart 三步)"
  - "  5. P0 ✅ 本会话修了 — alerts.py Telegram fallback only → 无条件主路径"
  - "  6. P0 ✅ 本会话修了 — Makefile alerts-test 漏 init_sentry"
  - "  7. P0 ✅ 本会话修了 — GHA setup-flyctl@v1.5 tag 不存在 (5-16 起所有 deploy fail)"

duration: ~12h (SESSION 20 Task 1-3 ~3h + SESSION 21 chaos injection + bug fix + verification ~9h)
completed: 2026-05-20

---

# Plan 02-07: Phase 02 Chaos Injection Verification + Alert Chain Verified Live

**Phase 02 hard gate ✅ verified live in prod: `send_paused_alert` 真触发 → Sentry email + Telegram + Sentry dashboard 三路独立确认 — 用 fast scheduler interval (30s) 让 3 次 FAILED 累积在 ~75s 触发完整 PAUSED 状态机链路。**

## TL;DR

Plan 02-07 由两半组成 + 一个 second revision:

**Half 1 (SESSION 20 完成)** — Mocked chaos test suite + soak infra + teaching doc 落地:
- 22 个 chaos test (respx + botocore stubber, CI-friendly)
- scripts/soak_monitor.py + Makefile soak targets
- docs/learning/08-生产化部署.md (251 行)

**Half 2 (SESSION 21 完成)** — Prod chaos injection 5 个 (REVISED — 原 7-day soak 改为 4+1 chaos):
- Inj 1: Fly machine stop → failed-by-design / succeeded-by-discovery (修 4 个 alert chain bug)
- Inj 2-v1: Gamma URL invalid + 默认 1h interval → 暴露 P0 scheduler_interval_s 不可配
- Inj 2-v2: 修后 fast 30s interval → **Phase 02 关闭硬门 ✅** (PAUSED → alert end-to-end verified live)
- Inj 3: Supabase secret unset → D-12 fail-soft 主契约 ✅ + 暴露 P1 fail-soft 抵消
- Inj 4: SSH + SQL unpause + restart → 操作手册凭证 + 暴露 P1 缺 prod unpause endpoint
- Inj 5: HMAC flood → daemon stability boundary ✅

**Verdict**: Phase 02 production-grade L1 daemon 真 LIVE,alert chain end-to-end verified in prod chaos. Phase 03 (L2 orderbook) 现在 unblocked,但启动前必须先消化 4 个 Phase 02.1 backlog item (其中 4 个本会话已修)。

## Hard Gate Evidence (Inj 2-v2)

**Timeline (UTC 2026-05-20)**:
- 21:04:25 `flyctl secrets set POLYARB_SCHEDULER_INTERVAL_S=30 POLYARB_GAMMA_URL=https://gamma-invalid.example.com`
- 21:05:08 第 1 次 tick FAILED, `failure_counter=1/3`
- 21:05:45 第 2 次 tick FAILED, `failure_counter=2/3`
- 21:06:22 第 3 次 tick FAILED, `failure_counter=3/3` → **PAUSED**
- 21:06:22 `ALERT: scheduler paused: 3 consecutive FAILED snapshots` (send_paused_alert 真触发)
- 21:06:22 Sentry PYTHON-C + PYTHON-D issue 创建 (capture_message + Loguru auto-capture)
- 5:06 AM CST Gmail 收到 PYTHON-C subject email
- 5:12 AM CST Gmail 收到 PYTHON-B digest "2 new alerts since 5:06 a.m."
- 21:06:22 用户 Telegram 真收到 "polyarb-l1 scheduler PAUSED: 3 consecutive FAILED snapshots"
- 21:11:19 `flyctl secrets unset POLYARB_GAMMA_URL POLYARB_SCHEDULER_INTERVAL_S` 恢复

**三路独立 alert path verified**:
- Sentry capture_message (alerts.py) → DSN → alert rule → Gmail ✅
- Loguru auto-capture (scheduler.py:_on_paused logger.error) → DSN → alert rule → Gmail ✅
- Telegram direct (alerts.py _telegram_direct unconditional) → bot API → 用户手机 ✅

## 关键 Bug 发现 + 修复表

| # | Bug | Severity | 修法 | Verified |
|---|---|---|---|---|
| 1 | alerts.py Telegram fallback only (BS 200 → TG 不发) | P0 | unconditional 主路径 + 测试守门 | ✅ Inj 2-v2 |
| 2 | Makefile alerts-test 漏 init_sentry → SDK silent no-op | P0 | target 加 init_sentry(s) | ✅ Inj 2-v2 |
| 3 | Sentry alert rule "Notify Suggested Assignees" + "high priority" filter | P0 | 用户 dashboard 改 When + 用 Recently Active fallback | ✅ Inj 2-v2 |
| 4 | scheduler_interval_s 不可配 (getattr 读未声明 field) | P0 | Settings 显式声明 + env var override + 2 测试 | ✅ Inj 2-v2 |
| 5 | GHA setup-flyctl@v1.5 tag 不存在 → 5-16 起所有 deploy fail | P0 | @v1.5 → @1.6 | ✅ deploy success 2m13s |
| 6 | /health 503 触发 Fly proxy 切流量 | trade-off | (Phase 03 重定) | 入 Phase 02.1 |
| 7 | fail-soft 互相抵消 (撤 secret 场景静默) | P1 | (Phase 02.1) | 入 backlog |
| 8 | daemon PAUSED 没 prod unpause endpoint | P1 | (Phase 02.1) | 入 backlog,实测 SSH+SQL+restart |

## 测试

- `uv run pytest tests/m1-perception/ -k chaos --tb=short` — 22 passed
- `uv run pytest tests/m1-perception/test_scheduler.py -xvs` — 10 passed (含 2 新加 scheduler_interval test)
- `uv run pytest tests/m1-perception/test_alerts.py -xvs` — 7 passed (含新 BS 200 case)

## Commits (SESSION 21, 5-19 ~ 5-20)

- `8ccd604 test(02-07)`: chaos engineering test suite (8 scenarios, 22 tests)
- `2fbfd32 feat(02-07)`: scripts/soak_monitor.py + Makefile soak targets
- `522ea56 docs(02-07)`: Phase 02 teaching doc — 生产化部署 (08)
- `3f70781 docs(02-07)`: interim SUMMARY (deprecated by this file)
- `69cb9c1 chore(02-07)`: pyright unused-import cleanup
- `63cc8ea docs(state)`: SESSION 21 Wave 5 chaos+soak infra landed
- `ce5f5ed docs(02-07)`: 7-day soak → 4 chaos injections + thread 偏离
- `4a333ca docs(threads)`: m2 slippage.py 18-day plan-code drift 考古
- `05786e6 docs(state)`: SESSION 21 EOD pt 1
- `b4de60c fix(alerts)`: Telegram direct unconditional + chaos Inj 1 bug 发现
- `24a8e87 fix(02-07)`: chaos Inj 1 全 bug 修复 + verdict 改写
- `1f118f7 docs(02-07)`: Inj 2-5 设计 second revision (核心 alert chain 反推)
- `7ed3a6a docs(02-07)`: Inj 2/3/5 verdict + 4 个新 bug 发现
- `d271e52 fix(02-07)`: scheduler_interval_s 可配 (Inj 2 P0)
- `5a5c475 fix(ci)`: setup-flyctl@v1.5 → @1.6 (P0,自 5-16 起所有 deploy fail)
- `7a39b89 docs(02-07)`: Inj 2-v2 hard gate PASSED — alert chain 真验证
- (this commit): Phase 02 final SUMMARY + close

## Next

1. `/gsd-extract_learnings 02 --ws m1-perception` — 关 Phase 02,extract learnings
2. **跨 workstream 决策点**:
   - M1 Phase 03 (L2 orderbook) — 启动前必须先消化剩余 backlog (2 个 P1 + 1 个 trade-off)
   - M2 T2 (Slippage Model) — plan-code 沉默分叉 18 天考古结论 (threads/learnings-meta.md): 三选一决策待做
3. Phase 02.1 backlog 内容由 LEARNINGS.md 落地后再决定优先级
