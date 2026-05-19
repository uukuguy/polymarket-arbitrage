# Soak Gate Deviation — 2026-05-19 (Phase 02 only)

> Cross-workstream thread. Records a deliberate deviation from `market-observation-architecture.md` §1 生产级判定标准 ("7-day soak + uptime ≥99% + ≥1 natural fault self-recovered or correctly alerted") for Phase 02 specifically. Does NOT amend the thread definition — future phases default back to the original.

## 决策

Phase 02 closes with **4 prod chaos injections** instead of a **7-calendar-day soak**.

The chaos test suite (mocked) already proved code-level fail-soft and alert wiring in CI (22/22 tests, Plan 02-07 Task 1). The chaos injections in prod prove the deployed alert chain is end-to-end real (Better Stack + Sentry + Telegram + Axiom). The 7-day uptime number is NOT collected this Phase.

## 偏离 thread §1 的具体点

| Thread §1 原定义 | Phase 02 变体 |
|---|---|
| 7 天连续 prod 运行 | ~1-3 天的 chaos injection 窗口 (按用户节奏分散) |
| Uptime ≥99% 由 Better Stack 7 天 SLA 给 | Uptime 数字不收集；改为 4 次注入的每次"故障→告警→恢复"链路凭证 |
| ≥1 次自然故障正确告警 | 4 次人为故障各自验证一条 alert path（Better Stack/Sentry/Telegram/Axiom 各打一次） |
| Cron 14×subset + 1×full 全部命中 | 测试窗口内 subset cron 命中率 100%；full cron 不要求（窗口短于一周） |

## 决策理由

1. **Supabase Free tier 不能扛 7 天 idle** — 7 天无活动 auto-pause project，supabase-mirror 健康检查会变 fail，会让 soak gate 误报 fail。
2. **不升 Supabase Pro $25/mo** — 用户判断 Phase 02 阶段 Supabase 实际负载（dashboard 偶尔查 + mirror 每天 ~17 次写）远低于 Pro tier 的价值。M3 跨平台时跳入实盘量级再升。
3. **chaos injection 凭证比"7 天无事"更强** — 7 天无事可能只是"7 天没遇到故障"；4 次注入证明"故障真的能被察觉 + 真的能告警 + 真的能恢复"，覆盖度更直接。
4. **L1 阶段不该卡 7 天日历** — thread §1 的 7-day 是为 L2/L3 实盘前的硬门设的，L1 只读观察层风险面比实盘窄太多。

## 补偿 — 4 次 injection 覆盖的 alert path

| Injection | 触发的 alert path | thread §1 对应条款 |
|---|---|---|
| Fly machine stop | Better Stack uptime probe → email + Telegram (downtime + recovery) | "uptime probe 自动告警" |
| R2 secret unset | Sentry breadcrumb (R2UploadError) + /health r2 warn → 恢复后 pass | "R2 失败 fail-soft，告警链 reachable" |
| Supabase secret unset | Sentry breadcrumb (mirror fail) + /health supabase warn → 恢复后 pass + snapshot 仍 OK/DEGRADED 不是 FAILED | "D-12 fail-soft 不 abort snapshot" + "mirror 失败告警 reachable" |
| HMAC flood | Axiom 30× 401 log + daemon 全程 health pass | "scan endpoint 抗 flood, 资源不耗尽" |

## 风险面 / open questions

- **没有跑过 7 天，所以下面这些 Phase 02 没验证**：
  - SQLite WAL 长跑会不会无限增长 → 计划 Phase 03 (L2) 开工前主动跑一周纯 L1 soak 补此凭证（即使那时 L2 在写也行，L1 SQLite 写少）
  - Fly Volume 慢漏 → 同上
  - Supabase mirror 在长周期内会不会有 silent corruption → Phase 03 引入跨日 diff 自动校验时一并补
- **chaos in prod 本身的副作用**：
  - 跑 injection 期间会真发邮件给用户、真发 Telegram、真打 Sentry quota → 用户已预期；不算 alert 噪音
  - Inj 2/3 必须备份 secret，备份失败 = phase 卡住 → PLAN.md 已写明 `ORIG_R2` / `ORIG_SB` 备份步骤

## 未来回补点

Phase 03 (L2 orderbook tracking) discuss-phase 时 must-haves 必须包含 "real 7-day soak (with paid Supabase Pro or alternative DB)" 作为门槛，把这次跳过的 7-day 凭证补上。M2 Combinatorial 不需要等这个补。

## Trace

- Plan: `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-07-PLAN.md` Task 4 (REVISED 2026-05-19 marker)
- Log: `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md` (REVISED 2026-05-19 marker)
- Thread §1 原定义: `.planning/threads/market-observation-architecture.md`
- 用户决策时间: 2026-05-19 SESSION 21 (AskUserQuestion answer recorded)
