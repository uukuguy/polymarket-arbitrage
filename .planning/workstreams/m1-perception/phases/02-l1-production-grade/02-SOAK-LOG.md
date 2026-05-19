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
- [ ] **Inj 2 (scheduler PAUSED 全链路)**: Telegram 收到 PAUSED msg + Gmail 收到 Sentry email — 这是 Phase 02 关闭的核心凭证

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
