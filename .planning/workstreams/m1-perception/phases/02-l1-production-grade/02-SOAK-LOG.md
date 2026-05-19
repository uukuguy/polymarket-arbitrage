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
