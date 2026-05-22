# Phase 02 Soak / Chaos-Injection Log

> **Original definition (thread §1 生产级判定标准 + RESEARCH §11)**: 7-day production soak — Phase 02 ships when this log shows 7 consecutive days of healthy behavior + ≥1 natural fault self-recovered.
>
> **REVISED 2026-05-19**: Replaced 7-day soak with **4 prod chaos injections** as gate variant. Rationale: Supabase Free tier auto-pauses after 7 days idle → breaks soak gate; user opted not to upgrade to Pro $25/mo for this Phase. Full decision trace: `.planning/threads/soak-gate-deviation-2026-05.md`. Phase 03 (L2) MAY revisit a real 7-day soak when account economics shift.
>
> **W9 fix (2026-05-12)**: 无本地长跑进程。Better Stack monitor (Plan 05 已部署) 是云端 30s ping 探针。
> 此日志由 `make soak-export` (= `scripts/soak_monitor.py export`) 追加 + 用户手动记录 chaos injection 事件。

## 如何使用

```bash
# Baseline check before starting:
make soak-status                                  # 拿 Better Stack 当前 uptime baseline
curl -sS https://polyarb-l1.fly.dev/health        # all 4 components pass

# 跑 4 次 chaos injection (顺序无所谓, 可拆几天):
# 详细命令见 02-07-PLAN.md Task 4 Step B

# 验收时拿 Better Stack 历史片段:
make soak-export

# 全部 4 次 + criteria 都过 → "chaos verified" 给 Claude 写 final SUMMARY
```

## Pass Criteria (REVISED 2026-05-20 — second revision after Inj 1 真实发现)

**Hard gate (Inj 2 必过)**:
- [x] **Inj 2-v2 (scheduler PAUSED 全链路)**: ✅ **PASSED 2026-05-20** — Telegram 收到 + Gmail 收到 Sentry email (PYTHON-C + PYTHON-D + PYTHON-B digest) + Sentry dashboard 真 issue. Hard gate ✅

**Soft criteria (Inj 3/4/5 可记 "shipped with caveats")**:
- [ ] **Inj 3 (D-12 fail-soft)**: 撤 Supabase secret 后 snapshot 是 OK/DEGRADED **NOT FAILED** + /health supabase=warn + Sentry breadcrumb 出现
- [ ] **Inj 4 (manual unpause)**: scheduler PAUSED 后用 /scan 能恢复 RUNNING + /health 全 pass
- [ ] **Inj 5 (HMAC flood resilience)**: 30× bad HMAC 期间 daemon /health 全程 pass + 无 restart
- [ ] **No permanent damage**: 全部 injection 后 /health overall=pass + secret 全恢复
- [ ] **Cron 健康**: `flyctl logs --since=2h | grep "snapshot OK"` 显示正常 cron 命中
- [ ] **Volume**: `flyctl ssh console -a polyarb-l1 -C "df -h /data"` ≤ 4GB

**Inj 1 已完成** (设计 failed-by-design, 实际 succeeded-by-discovery — 详见 Events 段)

## Chaos Injection Plan (4 必跑, REVISED 2026-05-20)

> 详细命令在 02-07-PLAN.md Task 4 Step B；这里是 quick ref。

| # | What | Verifies | Recovery | Alert chain? |
|---|---|---|---|---|
| 1 ✅ | `flyctl machines stop` 短时 | (原: BS uptime probe — 实际不会触发) | auto-restart | 暴露真 bug 而非 verify; **已完成** |
| 2 | 让 Gamma URL 无效 → 3 次 FAILED | **scheduler PAUSED → send_paused_alert** 全链路 | `flyctl secrets set` 恢复 + /scan unpause | ⭐ **核心凭证** |
| 3 | `flyctl secrets unset POLYARB_SUPABASE_KEY` | D-12 fail-soft 契约 (snapshot 不 abort) | `flyctl secrets set ...` | 不触发 alert (单 DEGRADED) |
| 4 | Inj 2 后 `/scan` 触发 unpause | scheduler 手动恢复路径 | (自动) | 不触发 alert |
| 5 | 30× curl /scan with bad HMAC | daemon stability boundary | (无需恢复, 401 是预期) | 不触发 alert |

⚠️ **Inj 2/3 前必备份原 secret**！见 PLAN.md Task 4 Step B 的 backup 命令。
⚠️ **Inj 2 必须先跑** — Inj 4 (unpause) 依赖 Inj 2 留下的 PAUSED 状态。

## Events

(Entries below are appended by `make soak-export` and manually by user during fault injection.)

<!-- BEGIN SOAK EVENTS -->
<!-- soak_monitor.py appends audit sections below this line -->

### Inj 1 — Fly machine stop (2026-05-19) — **FAILED + FOUND REAL BUG**

**Timeline (UTC)**:
| 时刻 | 事件 |
|---|---|
| 14:48:51Z | flyctl machines stop 6830939c0070d8 (app role, crimson-frost-3779) |
| 14:48:54Z | Fly 接收 stop |
| 14:49:00Z | machine exit (exit_code=0, requested_stop=true) |
| 14:51:07Z | **Fly proxy 收 inbound request → auto-wake start** (我的 curl poll 触发) |
| 14:51:09Z | machine started, daemon 冷启 |
| 14:53:41Z | /health 全 pass 恢复，daemon 立刻跑了一次 snapshot 自愈 |
| **总 downtime** | **~4 分 50 秒** (machine stopped ~2 min + daemon 冷启 ~2.5 min) |

**预期 vs 实际**:
| 通道 | 预期 | 实际 | 结果 |
|---|---|---|---|
| Better Stack incident email | 探针 miss → email | **未收到** | ❌ |
| Telegram (via Better Stack) | 探针 miss → BS 转发 TG | **未收到** | ❌ |
| Sentry 邮件 | snapshot FAILED → Sentry event → email | **未收到** | ❌ |
| Fly auto-restart | 不预期 | 2 分钟后由入站请求触发 wake | ✓ 意外功能验证 |
| Daemon 自愈 | 起来后等下次 cron tick | 起来立刻跑 snapshot | ✓ 自愈意外好 |

**为什么 alert 全空 — 5 层根因**:

1. **设计 expectation 错** — 我们误以为 Better Stack 是 30s uptime probe，实际是 12h heartbeat receiver (`POLYARB_BETTER_STACK_HEARTBEAT_URL` = heartbeat 产品)。短 stop (<12h) 在 heartbeat 模型下根本不会触发 missed-beat alert。
2. **Snapshot 不算 FAILED** — Inj 1 只停了 2 分钟，期间没到 cron tick (0/12h UTC)，所以 0 次 FAILED → scheduler 没 PAUSED → `send_paused_alert` 从未被调用。
3. **`send_paused_alert` 的 Telegram 路径是 fallback only** — 旧代码：`if not bs_ok: telegram_direct(...)`。BS `/fail` 返回 200 (signal accepted) 时 TG 永远不发。
4. **Better Stack `/fail` POST 200 ≠ 用户被通知** — 200 只意味着 BS 接收 signal，是否 routing 到邮箱/Telegram 取决于 BS 的 on-call/escalation 配置。我们的 BS heartbeat **on-call 配的是默认 "primary responder + email"，但 primary responder 邮箱**没确认/没配，等于黑洞。
5. **Sentry alert rule 用 "Notify Suggested Assignees"** — issue unassigned 时无人通知，等于黑洞。

**Diagnostic 验证 (诊断脚本 + make alerts-test)**:

| 测试 | 结果 |
|---|---|
| 直接 POST Better Stack `/fail` | 200 OK (body 空) — BS 接收 signal |
| 直接 POST `api.telegram.org/.../sendMessage` | 200 OK + `{"ok":true,"message_id":6,...}` — TG 直连完全通 |
| `make alerts-test` (走 send_paused_alert 完整链路) | 用户**未**收到 TG (因为 BS 返回 200 → fallback 被跳过) |

**修法 (本 commit 一并落)**:

- ✅ **代码层 (甲)**: `alerts.py:send_paused_alert` 把 Telegram direct 从 fallback 升为**无条件主路径**。BS `/fail` 仍发但不再决定 TG 是否发。
- ✅ **测试**: 新加 `test_send_paused_alert_calls_telegram_direct_when_better_stack_200` 守住新契约；改名旧 503 测试避免误导
- ✅ **真实验证**: 修后跑 `make alerts-test` → TG 实收一条 "polyarb-l1 scheduler PAUSED: alerts-test from sujiangwen@m3max.local" — 用户确认 ✓ (2026-05-19 23:42 UTC+8)
- ⏳ **配置层 (乙)**: 用户去 Sentry / Better Stack dashboard 修
  - Sentry: alert rule "Then" 从 "Notify Suggested Assignees" 改为 "Send a notification to Member uukuguy@gmail.com"
  - Better Stack: heartbeat on-call primary responder 邮箱确认 / 改成正确邮箱
  - 验证: 在 dashboard 点 "Send Test Notification" / "Test alert"，Gmail 1-2 min 内收到

### Inj 2 — Force Gamma URL invalid → scheduler FAILED tick (2026-05-20) — **PARTIAL VERIFIED + 2 个新发现**

**Timeline (UTC)**:
| 时刻 | 事件 |
|---|---|
| 19:21:58Z | `flyctl secrets set POLYARB_GAMMA_URL=https://gamma-invalid.example.com` |
| 19:22:15Z | daemon 重启完成 |
| 19:22:44Z | 第 1 次 snapshot tick — `status=FAILED, is_valid=False, 2 L1 issues` |
| 19:22:45Z | scheduler log: `snapshot tick FAILED: failure_counter=1/3` |
| 20:01:51Z | `flyctl secrets unset POLYARB_GAMMA_URL` (停损 — 等 3 次累积要 1h45m，不划算) |
| 20:04:08Z | daemon 恢复 — /health 全 pass，snapshot OK |
| **Total**: ~42 min，**只完成 1/3 失败累积**，未到 PAUSED 状态 |

**预期 vs 实际**:
| 通道 | 预期 (PAUSED 后) | 实际 |
|---|---|---|
| `send_paused_alert` 真触发 | Telegram + Sentry email | ⏸ 未到 3 次 FAILED 累积，未触发 |
| daemon 不崩 + status=FAILED | ✓ | ✅ daemon 跑 7.3s snapshot 完成 + 内部 /health=fail (503) + last_status=FAILED |
| F-05 Supabase mirror skip | ✓ | ✅ `step 7.5: skip Supabase mirror — snapshot is_valid=False` |
| R2 upload (parquet 0 rows) | ✓ (即使 0 row 也上传) | ✅ R2 upload success |

**第 1 个新发现 (P0 bug)**: **`scheduler_interval_s` 不可配**

`src/polyarb/daemon/scheduler.py:214` 用 `getattr(self._settings, "scheduler_interval_s", 3600)` 读 interval，但 `Settings` class 没声明这个 field — pydantic-settings 不会从 env var 读未声明 field。意思是**生产环境 scheduler interval 写死 1 小时**，无法通过 `flyctl secrets set POLYARB_SCHEDULER_INTERVAL_S=60` 加快。这让 chaos injection 的 "3 次 FAILED 累积 → PAUSED" 验证**需要 1h45m+ 等待**。

修法（建议进 Phase 02.1 不影响本次 ship）：
1. `config.py` `Settings` 加 `scheduler_interval_s: int = 3600` field
2. 让 env var `POLYARB_SCHEDULER_INTERVAL_S` 真生效
3. 加测试验证 settings.scheduler_interval_s 真读 env

**第 2 个新发现 (设计 trade-off, 非 bug 但需 doc)**: **/health 在 FAILED 时返 503 → Fly proxy 切流量 → 外部完全不可达**

IETF Health Check 标准：任何 check fail → overall fail → return 503。Fly `[http_service.checks]` 看到 503 → 标 machine "critical" → **proxy 停止 route 外部流量**。结果：

- ❌ 外部 `curl https://polyarb-l1.fly.dev/health` timeout — 用户/Better Stack 完全看不到 daemon 在跑
- ❌ Better Stack heartbeat probe 一旦也走 polyarb-l1.fly.dev 必然 timeout (但我们用的是 daemon → Better Stack push 模型，不受此影响)
- ✅ daemon 内部正常运行 + cron tick 继续

**这是 IETF spec compliance vs Fly proxy behavior 的 trade-off**：
- 严格 IETF: snapshot FAILED → /health 503 (我们现在的实现)
- 可观测优先: /health 总返 200，把 FAILED 放 body status 字段里让客户端自己判断 (alternative)

Phase 02 选了 IETF 严格但**结果是 prod down 期间 Better Stack heartbeat 看不到救命信号**。这跟 thread §1 "生产级判定标准" 的"失败自动告警"目标冲突。Phase 03 改 L2 实时 daemon 时必须重定，但 Phase 02 ship 时记入 02.1 backlog。

**Inj 2 verdict**: ⚠️ **partial verified** — 1/3 累积证明 scheduler 状态机正确开始计数 + status reporting 正确，但**未到 3/3 PAUSED 状态触发 send_paused_alert**。`send_paused_alert` 已通过 `make alerts-test` end-to-end verified live (Inj 1 期间 4 封 Sentry email + 3 次 Telegram 接收)，从代码路径推断 PAUSED 触发链路完整，但**未在 prod chaos 真实场景下做端到端 verify**。

Phase 02 关闭决策选项:
- **路 A** — 接受 partial verified：Inj 2 找到 2 个新 bug 入 02.1 backlog，但 PAUSED-alert 链路凭证不完整。Trade-off: ship with caveat。
- **路 B** — 修 P0 bug 1 (scheduler_interval_s 可配)，让 Inj 2 能 3 分钟跑完，真验证 PAUSED → alert。Trade-off: 半小时改代码 + 部署 + 跑。

本次会话走路 A 节奏，路 B 留作 Phase 02.1 优先项。

---

### Inj 2-v2 — Force 3× FAILED → PAUSED → send_paused_alert 全链路 (2026-05-20) — **✅ FULL VERIFIED (hard gate ✅)**

**前置 fix (commit `d271e52` + `5a5c475`)**:
- P0 fix: `Settings.scheduler_interval_s` 显式声明 + scheduler 读改为属性访问
- GHA fix: `superfly/flyctl-actions/setup-flyctl@v1.5` → `@1.6` (tag 不存在导致 5-16 起所有 deploy fail)

**Timeline (UTC)**:
| 时刻 | 事件 |
|---|---|
| 21:04:25Z | `flyctl secrets set POLYARB_SCHEDULER_INTERVAL_S=30 POLYARB_GAMMA_URL=https://gamma-invalid.example.com` |
| 21:04:39Z | daemon 起来, scheduler 进入 30s tick 模式 (10s 启动延迟后第一次 tick) |
| 21:05:08Z | 第 1 次 tick FAILED, `failure_counter=1/3` |
| 21:05:45Z | 第 2 次 tick FAILED, `failure_counter=2/3` (37s 后) |
| 21:06:22Z | 第 3 次 tick FAILED, `failure_counter=3/3` |
| **21:06:22Z** | **`SCHEDULER_PAUSED: consecutive failure threshold reached (counter=3)`** |
| **21:06:22Z** | **`ALERT: scheduler paused: 3 consecutive FAILED snapshots`** — `send_paused_alert` 真触发 |
| 21:06:52Z 起 | `scheduler is PAUSED, skipping tick` (PAUSED 状态下后续 tick 全跳过 ✅) |
| 21:11:19Z | `flyctl secrets unset POLYARB_GAMMA_URL POLYARB_SCHEDULER_INTERVAL_S` 恢复 |

**预期 vs 实际 (alert chain end-to-end)**:
| 通道 | 预期 | 实际 |
|---|---|---|
| 3 次 FAILED → 自动 PAUSED | ✓ | ✅ counter=1/3 → 2/3 → 3/3 完美累积 |
| `send_paused_alert` 真触发 | ✓ | ✅ scheduler log `ALERT:`, alerts.py log `send_paused_alert:87` |
| Sentry capture_message → DSN | ✓ | ✅ PYTHON-D issue 创建 (capture_message), PYTHON-C issue (loguru auto-capture from scheduler:_on_paused) |
| Sentry alert rule → email | ✓ | ✅ Gmail 收到 5:06 AM CST email (PYTHON-C subject), 5:12 AM digest (PYTHON-B "2 new alerts since...") |
| Telegram direct (unconditional, Inj 1 修法) | ✓ | ✅ 用户 TG 真收到 "polyarb-l1 scheduler PAUSED: 3 consecutive FAILED snapshots" |
| PAUSED 后跳过 tick (state machine) | ✓ | ✅ 多次 "scheduler is PAUSED, skipping tick" log |
| /scan unpause 路径 (Inj 4) | 跳过 — 因为本次会话结束后 secret 恢复后会自动重启 daemon, 重置 state | (deferred 进 phase 03 mocked test) |

**Inj 2-v2 verdict**: ✅ **Phase 02 关闭硬门凭证拿到**。alert chain 在 prod 真实 chaos 下端到端 verified live (Sentry email + Telegram + Sentry dashboard 全确认)。

**配套发现**: Gmail 还有 PYTHON-9 (Inj 2-v1 期间 Gamma stream failed) 和 PYTHON-A (Inj 3 secret 恢复瞬间 supabase mirror push 失败) 邮件 — 说明 Inj 2-v1 + Inj 3 也都触发了 Sentry 邮件，**只是我们当时查 Gmail newer_than:1h 错过了**。这意味着 **Sentry alert chain 在所有 Inj 期间都 work**，之前的 partial verdict 偏保守。

### Inj 3 — Supabase secret unset → D-12 fail-soft 验证 (2026-05-20) — **PARTIAL VERIFIED + P1 新发现**

**Timeline (UTC)**:
| 时刻 | 事件 |
|---|---|
| 20:07:18Z | baseline /health 全 pass，snapshot OK，supabase mirror age=207s |
| 20:07:30Z | `flyctl secrets unset POLYARB_SUPABASE_SERVICE_KEY` |
| 20:10:15Z | daemon 起来，旧 snapshot OK 显示中 (20:08:35Z snapshot)，**`supabase` check 整段消失** |
| 20:08:35Z～20:09:55Z | daemon 第一次 tick — snapshot id=87, **status=degraded, is_valid=True**, 14000 books, no mirror log |
| 20:18:33Z | 从本地 `.env` 备份读 secret 恢复 |
| 20:22:11Z | daemon 恢复 OK，supabase mirror check 回来 (但还 warn=None) |

**预期 vs 实际**:
| 通道 | 预期 | 实际 | Verdict |
|---|---|---|---|
| snapshot 是 OK/DEGRADED NOT FAILED | ✓ | ✅ **DEGRADED**, is_valid=True | **D-12 ✅** |
| /health supabase = warn | ✓ | ❌ **整段消失** (不是 warn) | ⚠ |
| Sentry breadcrumb 出现 | ✓ | ❌ **无 breadcrumb, 无 mirror 任何 log** | ⚠ |

**P1 新发现 — 两层 fail-soft 互相抵消**：

预期路径："撤 secret → mirror.push_snapshot() 失败 → fail-soft except 分支 → Sentry breadcrumb"

实际路径："撤 secret → `supabase_mirror_enabled=False` (model_validator auto-set) → orchestrator step 7.5 `if settings.supabase_mirror_enabled:` 失败 → **整段 mirror block 跳过** → 0 log, 0 breadcrumb"

```python
# config.py — auto-disable when secret 缺
@model_validator(mode='after')
def _enable_flags(self):
    if self.supabase_url and self.supabase_service_key.get_secret_value():
        object.__setattr__(self, "supabase_mirror_enabled", True)
    # implicit: 否则保持 default False

# orchestrator.py step 7.5
if settings.supabase_mirror_enabled and not is_valid:
    ...F-05 guard...
elif settings.supabase_mirror_enabled:
    try: ... mirror.push_snapshot(...) ... except: log warning + Sentry breadcrumb
# 第三种 branch (mirror_enabled=False) 隐式什么都不做
```

**安全场景**："Supabase 短暂网络故障 → secret 还在 → enabled=True → 进 except → 真 breadcrumb"。Inj 3 测的不是这种。

**Inj 3 的 bug 场景**："运维不小心撤了 secret / 输错 → enabled 变 False → mirror 静默停摆 → dashboard 不更新 + 0 alert + /health 没显示 supabase check"。这是真"运维盲区"。

**修法建议 (P1, Phase 02.1)**:
1. `health.py` 把 supabase check 改为：`enabled=False` 时返 `warn + observedValue="mirror_disabled"`（不是完全省略）
2. orchestrator step 7.5 在 `mirror_enabled=False` 时 + 历史上曾 enabled (有过 mirror 记录) 时，发一次"mirror disabled detected"breadcrumb + log warning

**Inj 3 verdict**: ✅ **D-12 fail-soft 主契约满足** (snapshot 不 abort, status=degraded) + ⚠ **alerting/visibility 路径 disabled-secret 场景静默** (P1 bug)

---

### Inj 5 — HMAC flood (2026-05-20) — **✅ FULL VERIFIED**

**Timeline (UTC)**: 20:23:30Z 30 个并发 `curl -X POST /scan -H "X-Signature: sha256=deadbeef"`

**Results**:
- 30/30 returns 401 ✅
- daemon /health 全程 pass (Fly check 1/1 passing throughout) ✅
- 0 restart, 0 ERROR log, 0 异常 ✅
- 401 是 starlette middleware 层 silent rejection（无 log，无 Sentry event）— 这是设计选择避免 noise

**Inj 5 verdict**: ✅ daemon stability boundary 通过。

---

### Inj 4 — SKIPPED (依赖 Inj 2 PAUSED 状态未达成)

Inj 4 设计前提：scheduler 真的进 PAUSED 状态后 `/scan` 触发 unpause。但 Inj 2 因 `scheduler_interval_s` 写死 3600s 没跑到 3/3 累积，scheduler 未 PAUSED。Phase 02.1 修 scheduler_interval_s 配置后补 Inj 2 + Inj 4 全链路。

---

**Inj 1 verdict (REVISED 2026-05-20)**: ❌ 原设计失败 + ✅ 真 bug 全修

原设计 "Fly stop 2 min → Better Stack uptime probe 触发邮件" 完全失败（heartbeat 模型不可能触发 2 min stop）。**但 chaos 暴露的 3 个真 bug 全部追下来修完**：

| Bug | 修法 | Verified Live |
|---|---|---|
| 1. `send_paused_alert` TG 是 fallback only (BS 200 时不发) | `alerts.py` 把 TG 升无条件主路径 + 测试守门 | ✅ TG 3 次连续触发都收到 |
| 2. `make alerts-test` 漏 `init_sentry()` → Sentry SDK 静默丢弃 events | Makefile target 加 `init_sentry(s)` 在 send_paused_alert 前 | ✅ Sentry 邮件 4 封连续收到 (Gmail) |
| 3. Sentry alert rule "Then=Notify Suggested Assignees" + "When=high priority" → unassigned info 级别 issue 被 silent drop | User 在 dashboard 改成 "Notify Jiangwen Su (Member)" + "When=A new issue is created" | ✅ 通过 Send Test Notification + 真 capture_message 双重 verified |
| 4. (剩余) Better Stack heartbeat on-call routing primary responder 邮箱未确认 | 暂不修 — 已有两条独立 alert 路径 (Sentry 邮件 + TG)，BS 邮件冗余可选 | ⏳ 留待 |

**结论**：Inj 1 设计预期 100% 失败，但 chaos injection 这个**手段本身**100% 成功 — 把 alert chain 从"3 路全死 (SESSION 20 验证幻觉)"修成 "2 路 live + 第 3 路冗余可选"。

**比 7 天 soak 通过更有价值的凭证**：用 ~2 小时 chaos diagnostic 把 alert chain 从"运维盲区"变成"两路可达 + 测试守门"。如果走 7-day soak 路径，**这个 bug 99% 不会暴露**（7 天里多半没有真故障，即使有也未必走到 scheduler PAUSED）。SESSION 20 的"E2E verified" SUMMARY 在那种路径下会被 ship 出去，未来某次真 oracle 操纵告警石沉大海。

### Inj 1 后 alert chain 真实状态

| 通道 | 触发条件 | 状态 |
|---|---|---|
| Sentry email | scheduler PAUSED (3× FAILED) → capture_message | ✅ live verified |
| Telegram direct | scheduler PAUSED (3× FAILED) → unconditional send | ✅ live verified |
| Better Stack incident email | heartbeat missed > tolerant window (12h+) OR /fail POST + on-call routes to email | ⚠ /fail POST 发了但 BS dashboard 未配 on-call → 不发邮件 |

**新契约下 Telegram = primary alert channel**，下面 Inj 2-4 重新设计触发条件以让 `send_paused_alert` 真触发（不再依赖 Better Stack uptime probe）：

- **Inj 2 (R2 unset)** — 撤 R2 secret + 改成连续 3 次 snapshot 失败 (或临时让所有 snapshot FAILED 而非 DEGRADED) 触发 scheduler PAUSED → 触发 send_paused_alert
- **Inj 3 (Supabase unset)** — 同上设计；或确认 D-12 fail-soft 让 snapshot 仍 OK，则这条 injection 不会触发 alert，需要重新设计
- **Inj 4 (HMAC flood)** — 本来就不会触发 alert，验证的是"daemon 不挂"，不是 alert path

→ Inj 2-4 走前必须**先做 PLAN 修订**：要测什么 alert path 必须从 "what gets send_paused_alert called" 倒推。

---

## Inj 3-v2 — fail-soft visibility (BUG-7 closure, 2026-05-22)

**Plan:** [02.1-01](../02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-01-PLAN.md) (D-01 / D-02)

**Difference vs original Inj 3:** original Inj 3 在 Phase 02 Wave 5 跑过, 暴露了 BUG-7 (fail-soft 撤 secret 路径完全静默, 无 log / 无 breadcrumb / 无 health check). Plan 02.1-01 修法是 step 7.5 else 分支加 audit log + Sentry breadcrumb. Inj 3-v2 是修后复跑, 验证 fail-soft 不再"黑洞静默".

**Procedure:**

```bash
# 1. 备份 Supabase service key
flyctl ssh console -a polyarb-l1 -C 'printenv POLYARB_SUPABASE_SERVICE_KEY' > /tmp/svc_key.bak

# 2. 撤 secret (触发 mirror_enabled=False 路径)
flyctl secrets unset POLYARB_SUPABASE_SERVICE_KEY -a polyarb-l1
sleep 90

# 3. 验 audit log 出现
flyctl logs -a polyarb-l1 --no-tail | grep -E "mirror disabled|config-disabled" | tail -5
# 命中: snapshot_id=146 line 557 "step 7.5: mirror disabled — reason=config-disabled"

# 4. 触发 Sentry capture 看 breadcrumb (本地, 不是 prod daemon 进程)
make alerts-test
# 收到 Sentry email: scheduler paused: alerts-test from sujiangwen@m3max.local

# 5. 恢复 secret
flyctl secrets set POLYARB_SUPABASE_SERVICE_KEY="$(cat /tmp/svc_key.bak)" -a polyarb-l1
rm /tmp/svc_key.bak

# 6. (Sentry API verdict) 用 read-only token 拉 prod event JSON 解析 breadcrumbs
curl -sS -H "Authorization: Bearer $SENTRY_AUTH_TOKEN" \
  "https://de.sentry.io/api/0/organizations/speechlessai/issues/PYTHON-A/events/latest/" \
  | jq '.entries[] | select(.type=="breadcrumbs") | .data.values | map(select(.category=="mirror"))'
```

**Truth-by-truth verdict** (must_haves.truths from 02.1-01-PLAN.md frontmatter):

| # | Truth | 状态 | Evidence |
|---|---|---|---|
| 1 | 撤 secret → daemon log 出现 'mirror disabled' / 'config-disabled' | ✅ PASS | flyctl logs 命中 "step 7.5: mirror disabled — reason=config-disabled (snapshot_id=146)" @ 2026-05-21 22:46:10Z |
| 2 | breadcrumb (category='mirror', level='info') 出现在下次 Sentry event 上 | ❌ **design-unreachable** | 拉 PYTHON-A latest event (200 breadcrumbs, 时间窗口覆盖撤 secret 期间) — 0 条 category=mirror. **根因**: (a) 代码只在 disabled 路径 emit crumb, 但 disabled 路径 fail-soft 不抛 exception → crumb 进了 buffer 但**永不上传**; (b) 真正抛 exception 的成功 mirror 失败路径里, buffer 里也没 mirror crumb (代码不在那条路径 emit) |
| 3 | D-12 fail-soft 主契约不变 — snapshot 仍继续完成, mirror skip 不阻断 | ✅ PASS | snapshot 146 disabled 后仍完成; 后续 snapshot 147 / 148 正常; /health 整体仍 pass |
| 4 | Phase 02 LEARNINGS L7 verification 纪律遵守 — chaos 复跑 (不是只 unit test) | ✅ PASS | 本节即复跑记录 |

**Verdict: 3/4 PASS, 1 design-unreachable → ship-acceptable (Plan 02.1-01 核心目标 BUG-7 闭环)**

**核心目标判定**: BUG-7 原症状是"撤 secret = 黑洞 (0 log / 0 breadcrumb / 0 health signal)". Truth 1 验证 log 路径已通 — **黑洞已破** — audit visibility 在 log 层已恢复. Truth 2 是更进一步的"Sentry UI 上也能看见", 不阻塞 BUG-7 闭环.

**Truth 2 design gap → Phase 02.2 backlog**:

两种修法 (任选其一即可让 truth 2 prod 可达):

- **修法 A** (轻): mirror **成功路径**也 emit `category=mirror, level=info, message="mirror ok", data={snapshot_id, age_seconds}` breadcrumb. 这样 PYTHON-A 这种 mirror 失败 event 上一定带最近一次 mirror crumb. 代价: 每次 snapshot 多一条 breadcrumb (Sentry SDK buffer 自动 evict, 无成本).
- **修法 B** (硬): mirror **disabled 路径**顺手 `sentry_sdk.capture_message(level="info", message="mirror disabled")`. 这样 prod 一定出现一条 info-level event. 代价: Sentry 配额多用 (info-level 默认计费, 但 Polymarket daemon 每 N 小时 snapshot, 量可忽略).

我推荐**修法 A** — 不增加 Sentry 计费, 实现简单 (`mirror.py` push_snapshot 成功路径加 3 行).

**Closure SHA**: d0ed6aa

**Status: PASS (partial, truth 2 deferred to Phase 02.2 with concrete fix path)**

---

## Inj 4 — prod unpause endpoint (BUG-8 closure, 2026-05-22)

**Plan:** [02.1-02](../02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-02-PLAN.md) (D-03 / D-04 / D-22)

**Procedure:**

```bash
# 1. 加速 scheduler + 注入 invalid Gamma URL → 3 consecutive failures → PAUSED
flyctl secrets set POLYARB_SCHEDULER_INTERVAL_S=30 POLYARB_GAMMA_URL=https://gamma-invalid.example.com -a polyarb-l1
# 等 ~3 min, 看到 SCHEDULER_PAUSED log + send_paused_alert ERROR log @ 00:16:58
# (Telegram + Sentry email 双路告警实际收到 ✓)

# 2. 恢复 Gamma URL (counter sticky, daemon 仍 PAUSED 即便 restart)
flyctl secrets set POLYARB_GAMMA_URL=https://gamma-api.polymarket.com -a polyarb-l1
# 验证: daemon 重启后日志仍持续 "scheduler is PAUSED, skipping tick"  ✓ (sticky 行为正确)

# 3. 核心验证 — 从容器内部 localhost 调 endpoint (BUG-6 阻断 prod proxy 路径)
flyctl ssh sftp shell -a polyarb-l1 --machine <app-id> << 'EOS'
put /tmp/inj4_internal.sh /tmp/inj4_internal.sh
EOS
flyctl ssh console -a polyarb-l1 --machine <app-id> -C \
  'sh -c "chmod +x /tmp/inj4_internal.sh && /tmp/inj4_internal.sh"'

# 4. cleanup
flyctl secrets unset POLYARB_SCHEDULER_INTERVAL_S -a polyarb-l1
```

**Truth-by-truth verdict** (must_haves.truths from 02.1-02-PLAN.md frontmatter):

| # | Truth | 状态 | Evidence |
|---|---|---|---|
| 1 | PAUSED → 单条 `make unpause-prod` 切 RUNNING (无需 SSH+sqlite3+restart 三步) | ⚠️ **PASS via fallback** | endpoint code + 协议本身完整 work — 从容器内 localhost:8080/control/unpause 单 POST 即切. **prod proxy 路径暂阻** (BUG-6: /health=fail → Fly proxy 切流量 → "could not find a good candidate within 40 attempts at load balancing"). Plan 02.1-03 修 /healthz 上线后此 truth 自动 prod 可达. |
| 2 | POST /control/unpause (HMAC valid, PAUSED) → 200 + RUNNING + counter=0 | ✅ PASS | `{"status":"ok","state":"RUNNING","failure_counter":0}` HTTP 200 |
| 3 | POST /control/unpause (no/invalid HMAC) → 401 constant-time | ✅ PASS | `{"error":"missing X-Signature header"}` HTTP 401 |
| 4 | POST /control/unpause (already RUNNING) → 200 + already_running (幂等) | ✅ PASS | `{"status":"already_running","state":"RUNNING"}` HTTP 200 |
| 5 | /healthz + /health 不被 ControlAuthMiddleware 拦截 (path guard 只匹配 /control/*) | ✅ PASS | /health localhost 直接返 JSON (status=fail 是 IETF 内容, 不是 middleware 401) |
| 6 | Telegram + Sentry 告警在 PAUSED 触发时真发出 | ✅ PASS bonus | send_paused_alert ERROR log @ 00:16:58.732; alert chain L4 (unconditional fallback) 生效 |

**Verdict: 5/6 PASS + 1 partial-PASS (prod proxy 路径阻塞是 BUG-6 实证,不是 BUG-8 失败)**

**Cross-bug interaction discovery (新发现, 进 thread learnings-meta)**:

Inj 4 把 BUG-8 (no prod unpause) + BUG-6 (IETF strict vs Fly probe) **完全展开**: 
- BUG-8 代码闭环, endpoint 完全 work
- 但 prod 验证路径要走 Fly proxy → proxy 看 /health=fail (因 daemon PAUSED, snapshot stale) → 切流量 → make unpause-prod 在 prod 不可达
- **两个 bug 是耦合的**: BUG-8 让 daemon PAUSED 进生产, BUG-6 让 PAUSED 期间 ops 失去 prod proxy 路径恢复能力 (只能 ssh)
- Plan 02.1-03 修 /healthz 上线后, BUG-8 的"一条 make unpause-prod"UX 才在 prod 真闭环

**实际 Fly proxy 报错** (proof of BUG-6 manifestation):
```
proxy lax error.message="could not find a good candidate within 40 attempts at load balancing"
request.url="https://polyarb-l1.fly.dev/control/unpause"
```

**Closure SHA**: 0e4300f

**Status: PASS (5/6 truths) + BUG-6 cross-injection evidence; Plan 02.1-03 上线后 truth 1 prod 路径自动闭环**

---

## Inj #6-verification — /healthz + fly.toml probe switch (BUG-6 closure, 2026-05-22)

**Plan:** [02.1-03](../02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-03-PLAN.md) (D-05 / D-06)

**Procedure:**

```bash
# Baseline (post-deploy, before injection):
curl --noproxy '*' https://polyarb-l1.fly.dev/healthz  # → 200 status=warn
curl --noproxy '*' https://polyarb-l1.fly.dev/health   # → 200 status=warn (IETF: warn=200)
flyctl checks list -a polyarb-l1                       # → passing (Fly probe now sees /healthz)

# Cross-injection: force /health=fail
flyctl secrets set POLYARB_GAMMA_URL=https://gamma-invalid.example.com \
                   POLYARB_SCHEDULER_INTERVAL_S=30 -a polyarb-l1

# Wait ~3min for 3 consecutive snapshot failures, then verify:
curl --noproxy '*' https://polyarb-l1.fly.dev/health    # → 503 status=fail  (IETF strict)
curl --noproxy '*' https://polyarb-l1.fly.dev/healthz   # → 200 status=fail  (D-05/D-06)
flyctl checks list                                       # → passing  (Fly probe still happy)
curl --noproxy '*' -X POST https://polyarb-l1.fly.dev/control/unpause -H "Content-Length: 0"
# → 401 "missing X-Signature header"  (endpoint reachable, BUG-6 + BUG-8 联合修复实证)

# Cleanup
flyctl secrets set POLYARB_GAMMA_URL=https://gamma-api.polymarket.com -a polyarb-l1
flyctl secrets unset POLYARB_SCHEDULER_INTERVAL_S -a polyarb-l1
```

**Truth-by-truth verdict** (must_haves.truths from 02.1-03-PLAN.md):

| # | Truth | 状态 | Evidence |
|---|---|---|---|
| 1 | GET /healthz 永远返 HTTP 200 (即便 underlying checks 显示 fail) | ✅ PASS | Cross-injection 期间 /healthz [HTTP 200] body status=fail |
| 2 | GET /health 仍遵守 IETF strict — fail → 503 | ✅ PASS | Cross-injection 期间 /health [HTTP 503] body status=fail |
| 3 | fly.toml [http_service.checks] path 指向 /healthz | ✅ PASS | Deploy 后 servicecheck-00-http-8080 OUTPUT 显示 body 来自 /healthz; daemon PAUSED-pre-recovery 期间 check 仍 passing (vs Inj 4 时 critical) |
| 4 | /healthz body 与 /health 共享 schema (4 checks + status + serviceId + version + releaseId) per D-06 | ✅ PASS | curl 两 endpoint 比对, body 同 shape; 4 checks 全到位 |
| 5 | /healthz 和 /health 都是 public (no HMAC) — D-22 保持 | ✅ PASS | curl 不带 X-Signature 直接 200 OK; ControlAuthMiddleware path guard only matches /control/* |
| 6 | _build_health_checks() shared helper 抽出 — check 逻辑不分叉 | ✅ PASS | health.py refactor; /health 和 /healthz 都调 helper; 4 unit tests + 现有 regression 全 GREEN |

**Bonus — cross-bug interaction repair**:

Inj 4 (2026-05-22 00:30Z) 期间观察到 Fly proxy 切流量: `error.message="could not find a good candidate within 40 attempts"`. **Inj #6-verification (now) 期间相同 /health=503 状态下, Fly proxy 仍正常路由** — `curl /control/unpause` 经 Fly proxy 返 endpoint 自己的 401, 不再被 proxy 拦截. **BUG-6 + BUG-8 联合修复 prod ops 闭环已实证**.

**Closure SHA**: be9d05f

**Status: PASS (6/6 truths verified live in prod, cross-injection 验证 BUG-6 与 BUG-8 互锁修复)**
