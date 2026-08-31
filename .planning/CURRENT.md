# 当前项目状态

> M1 的唯一动态状态入口。最后对齐：2026-08-31。
> `JOURNAL.md` 是追加式历史；旧的 `[NEXT]`、revision-026 授权包和 Plan 05.6-207
> 叙述均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)。

## 一句话结论

M1 self-healing 的最新完成闭环是 Plan 05.6-267/268：生产运行的精确 release 为
`3a70cd9f5a52294fba5709f0d390421600baa5de`。它从 durable receipts 自动恢复了
被阻塞的 Quote certifier；随后真实 Structure → Quote → Opportunity successor
lineage 让既有 qualification epoch 恢复 `eligible`，没有人工 SQL、epoch reset 或
freshness-SLO 放宽。Plan 268 关闭了 merge-review 提出的 scoped connection typing
与 daemon pool 测试边界问题，全部工作已整合到 `main`。

当前没有进行中的 M1 实现、部署、迁移或合并任务。日常 M1 业务证据固定为 strict
readiness 与默认的一屏 business brief；后者可以按需下钻到 durable business status 和
认证机会投影。下一步只能根据**新的、
只读的生产证据**定义一个有界目标；不得因为本文曾包含的旧授权包而重走 revision-026
路径。

认证机会入口的参数边界已在 `6640b330` 与 `05ff19a9` 完成修复：不再全局导出
小写 Make 参数；`control-plane-opportunities` 仅以 target-scoped
`CONTROL_PLANE_OPPORTUNITIES_*` 原始值传递给 URL 编码的只读 curl 请求。每日基线为
北京时间 `08:30`，活跃会话为 `09:00–23:00` 每 `15` 分钟；异常证据应读取
`.runtime_incidents`、`.recovery_actions` 和 `.runtime_watchdog`，而不是将失败写成零机会。

## 最后验证的生产边界

| 范围 | 已验证结论 | 当前约束 |
|---|---|---|
| Quote durable recovery | Q1 的 140/140 receipt 由 coordinator 自动从 waiting 推进到 succeeded | 不使用 operator SQL 修复同类阻塞 |
| Opportunity freshness | 过期 Q1 被正确隔离；新 Q2 在 29.768514 秒后发布 Opportunity | 不放宽 900 秒 SLA 来掩盖旧数据 |
| Rolling qualification | `epoch-f66adc…` 从 `paused(freshness.structure)` 恢复为 `eligible` | 不重置 epoch 或重启整个窗口 |
| Runtime health | 最后确认时零 expired leases、零 open circuits，API/controller 健康 | 新故障须以新证据处理 |
| Delivery boundary | 8 个既有 Machines 保持身份和配置；release `3a70cd9f` 经 OrbStack 构建验证 | 不切换全局 Docker context，不配置 Colima |

## 当前权威证据

- [M1 STATE](workstreams/m1-perception/STATE.md) — 当前 phase、生产事实和硬约束。
- [JOURNAL Session 387–390](JOURNAL.md) — Quote recovery、successor proof、merge
  closure 与本次状态对齐。
- [Plan 05.6-267 summary](workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-267-SUMMARY.md)
  和 [Plan 05.6-268 summary](workstreams/m1-perception/phases/05.6-self-healing-structure-production/05.6-268-SUMMARY.md)
  — 完成证据。
- [双时钟教学章节](../docs/learning/105-市场全集与可执行报价必须使用两个时钟.md)
  — Structure/Quote 时钟与 lost-wakeup 的操作模型。

## 当前下一步

1. 依次运行 `make smoke-control-plane-prod` 和 `make control-plane-business-brief`；每日
   默认 brief 的 text 可用 `format=json` 作为自动日报输入。只有需要核验原始事实时，才
   下钻到 `make control-plane-status limit=20` 和
   `make control-plane-opportunities limit=50`（后者只读访问
   `https://polyarb-control-api.fly.dev/perception/opportunities`）。
2. brief 最多显示 5 个候选；它们不代表成交、收益或 P&L。仅当机会投影成功返回
   `status=available` 且 `current_opportunity_count=0` 时，才可记录“暂无认证机会”；brief
   或审计命令非零、HTTP/解析失败或 `status` 不可用时，记录“业务数据不可用”，不得归零。
3. 将新发现路由为：有明确验证产出的 M1 phase、跨 workstream 的 thread 更新，或暂不做的 backlog。
4. 在没有新的证据与授权前，不进行 deploy、migration、secret、recovery 或 qualification mutation；已退役 L1/L2 smoke 命令会明确失败并给出替代入口。
