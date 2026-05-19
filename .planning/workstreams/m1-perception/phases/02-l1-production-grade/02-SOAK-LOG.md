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

## Pass Criteria (REVISED 2026-05-19 — chaos-injection variant)

- [ ] **Inj 1 (Fly stop)**: Better Stack incident email received + Telegram downtime msg + Telegram recovery msg + /health 恢复 pass
- [ ] **Inj 2 (R2 unset)**: Sentry breadcrumb for R2UploadError + /health r2 warn + 恢复后 /health pass + 期间 snapshot 是 DEGRADED 不是 FAILED (D-12 fail-soft)
- [ ] **Inj 3 (Supabase unset)**: Sentry breadcrumb for mirror failure + /health supabase warn + 恢复后 pass + 期间 snapshot 仍 OK/DEGRADED 不是 FAILED (D-12)
- [ ] **Inj 4 (HMAC flood)**: 30× 401 in Axiom logs + daemon /health 全程 pass + 无 OOM/restart
- [ ] **No permanent damage**: 全部 4 个 injection 后 /health overall=pass, Fly machine 仍存活, secret 已恢复
- [ ] **Cron 健康**: 测试窗口内 subset cron 命中率 100% (`flyctl logs --since=Nh | grep "snapshot OK"`)
- [ ] **Sentry 噪音**: 测试窗口内 Sentry errors < 5/day 除掉 chaos injection 期间的预期错误
- [ ] **Volume**: `flyctl ssh console -a polyarb-l1 -C "df -h /data"` ≤ 4GB
- [ ] **Audit trail**: 本文件 "Events" 段记录每次 injection 的 timestamp + observed alerts + recovery time

## Chaos Injection Plan (4 必跑)

> 4 个都要跑完才能关 Phase 02。详细命令在 02-07-PLAN.md Task 4 Step B；这里是 quick ref。

| # | What | Verifies | Recovery |
|---|---|---|---|
| 1 | `flyctl machines stop` 3-5 min | Better Stack uptime probe + Telegram alert path | `flyctl machines start` |
| 2 | `flyctl secrets unset POLYARB_R2_SECRET_ACCESS_KEY` | R2 fail-soft + Sentry path + /health r2 warn | `flyctl secrets set ...` |
| 3 | `flyctl secrets unset POLYARB_SUPABASE_KEY` | Supabase fail-soft + Sentry path + /health supabase warn | `flyctl secrets set ...` |
| 4 | 30× curl /scan with bad HMAC | 401 batch + daemon stays alive | (无需恢复, 401 是预期) |

⚠️ **Inj 2/3 前必备份原 secret**！见 PLAN.md Task 4 Step B 的 `ORIG_R2` / `ORIG_SB` 备份命令。

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

**Inj 1 verdict**: ❌ alert chain 不通过原设计的 "Better Stack uptime probe" 路径触发 — **但发现了 3 个真 bug + 修复其中代码层的 1 个**。这比 7 天 soak 无事跑发现的多得多 — 印证 thread `soak-gate-deviation-2026-05.md` "chaos injection 凭证比 7 天无事更强"的判断。

**新契约下 Telegram = primary alert channel**，下面 Inj 2-4 重新设计触发条件以让 `send_paused_alert` 真触发（不再依赖 Better Stack uptime probe）：

- **Inj 2 (R2 unset)** — 撤 R2 secret + 改成连续 3 次 snapshot 失败 (或临时让所有 snapshot FAILED 而非 DEGRADED) 触发 scheduler PAUSED → 触发 send_paused_alert
- **Inj 3 (Supabase unset)** — 同上设计；或确认 D-12 fail-soft 让 snapshot 仍 OK，则这条 injection 不会触发 alert，需要重新设计
- **Inj 4 (HMAC flood)** — 本来就不会触发 alert，验证的是"daemon 不挂"，不是 alert path

→ Inj 2-4 走前必须**先做 PLAN 修订**：要测什么 alert path 必须从 "what gets send_paused_alert called" 倒推。
