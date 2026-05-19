# Phase 02 Soak Log

> 7-day production soak per RESEARCH §11 + thread §1 生产级判定标准.
> Phase 02 ships when this log shows 7 consecutive days of healthy behavior + ≥1 natural fault self-recovered.
>
> **W9 fix (2026-05-12)**: 无本地长跑进程。Better Stack monitor (Plan 05 已部署) 是真正的 7×24 云端探针。
> 此日志由 `make soak-export` (= `scripts/soak_monitor.py export`) 追加写入，以及用户手动记录故障注入事件。

## 如何使用

```bash
# Day 0+: Better Stack 自动 30s ping /health (云端，不需要本地操作)

# 随时查看当前 uptime (需要 BETTERSTACK_API_TOKEN + BETTERSTACK_MONITOR_ID):
make soak-status

# Day 7: 导出 7 天历史到本文件作为 audit trail:
make soak-export

# 故障注入（Day 3-4，可选）:
make soak-fault-inject  # 打印故障注入操作说明
```

## Pass Criteria

- [ ] Uptime ≥ 99% (Better Stack 7-day SLA)
- [ ] Cron 14/14 subset fires (7 days × 2/day, 12:00 UTC + 00:00 UTC)
- [ ] Cron 1/1 full fires (Sunday 04:00 UTC)
- [ ] OK + DEGRADED ≥ 95% of snapshot attempts (not FAILED)
- [ ] ≥ 1 natural failure → Telegram alert received (or fault injection verified alert)
- [ ] Self-healing after failure OR correct PAUSED (3x consecutive)
- [ ] SQLite volume ≤ 4GB at day 7 (`flyctl ssh console -a polyarb-l1 -C "df -h /data"`)
- [ ] Sentry errors < 5/day (excluding transients like retry breadcrumbs)

## Fault Injection Plan (Step B — Day 3-4)

Choose one of the following to verify alert chain end-to-end:

1. **Scale to 0 briefly** (simplest):
   ```bash
   flyctl machines stop <machine_id> -a polyarb-l1
   # Wait 3-5 minutes → expect Telegram alert + Better Stack downtime event
   flyctl machines start <machine_id> -a polyarb-l1
   # Verify: machine back up, /health returns pass, Telegram recovery notice
   ```

2. **Break R2 credentials temporarily**:
   ```bash
   flyctl secrets unset POLYARB_R2_SECRET_ACCESS_KEY -a polyarb-l1
   # Wait for next snapshot → R2 upload fails → /health r2 check warn
   # Restore: flyctl secrets set POLYARB_R2_SECRET_ACCESS_KEY=<original> -a polyarb-l1
   ```

3. **HMAC flood** (validates scan endpoint resilience):
   ```bash
   for i in $(seq 1 30); do
     curl -s -X POST https://polyarb-l1.fly.dev/scan \
       -H "Content-Type: application/json" \
       -H "X-Signature: deadbeef" \
       -d '{"recipe_name":"thick-but-slippery"}' &
   done
   wait
   # Expect: all 401, daemon stays alive, /health still pass
   ```

## Events

(Entries below are appended by `make soak-export` and manually by user during fault injection.)

<!-- BEGIN SOAK EVENTS -->
<!-- soak_monitor.py appends audit sections below this line -->
