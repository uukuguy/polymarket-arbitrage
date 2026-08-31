# M1 业务数据产品 Dashboard 设计

> 本文取代同名的早期简化规格。它采纳了架构评审结论：现有 `business-brief`
> 把本地 Postgres 状态和独立 HTTP 机会读取组合，不能作为 Dashboard 的业务事实源。

## 目标

把 Dashboard 变为 M1 的业务研究工作台：先回答“当前能否可信判断”，再让用户研究
机会形成前的 Structure、Quote、分析漏斗与最终认证机会。运行任务和恢复细节保留在
`/control-plane`，不要求业务用户从 task 日志推导数据产品状态。

## 决策与边界

- 新的唯一业务 authority 是 Postgres 中一次只读、`REPEATABLE READ` 事务生成的
  `BusinessOverviewV1`；CLI 与 Dashboard 消费同一序列化结果。
- 不在浏览器或 CLI 中拼接多个响应，不把重型研究查询塞入
  `/perception/control-plane`，不新增写、恢复、交易、订单或 P&L 权限。
- 资格（Qualification）是业务产品的信任/消费门，不是与 Structure、Quote、Analysis
  并列的第四个数据产品；运行任务仅在 `/control-plane` 展示。
- 不伪造趋势、覆盖率、价格统计或拒绝原因。读模型尚未提供时明确显示“未提供”；
  `available + 0`、`not-published`、`stale/lagging` 与 `unavailable` 永不混同。

## 原子业务契约

`GET /perception/business-overview` 返回 versioned `BusinessOverviewV1`，并通过一个
bounded read-only transaction 获得 `observed_at`、指针、资格、机会和阻塞项。响应顶层为：

```json
{
  "schema_version": "m1.business-overview.v1",
  "status": "available",
  "observed_at": "2026-08-31T00:00:00Z",
  "eligibility": {"state": "eligible", "reason_code": null},
  "structure": {}, "quote": {}, "analysis": {}, "opportunities": {},
  "blockers": []
}
```

每个产品读取（`structure`、`quote`、`analysis`、`opportunities`）遵循同一语义：

| 状态 | 含义 | 数值规则 |
| --- | --- | --- |
| `available` | 当前产品可用于其声明用途 | `count=0` 是真实零 |
| `stale` / `lagging` | 最后已知产品仍可研究，但不能作为当前判断 | 保留 generation 与 as-of |
| `not-published` | 所需生产物尚不存在 | 不显示为零 |
| `unavailable` | authority/read 失败 | 隐藏依赖数值，给出稳定 reason code |

`ProductRead` 包含 generation、parent generation、published_at、age、状态与本产品明确
声明的 metrics。Opportunity 必须携带其 Quote generation；Quote 必须携带其 Structure
generation。不匹配由服务端标为 `lagging`，而不是由浏览器时钟猜测。

资格 `paused` 的含义是“当前不可可信判断”，而不是“没有机会”；此时仍展示最后已知的
各产品及其 as-of。顶层 unavailable 只表示整个快照无法读取。

## 产品指标与阻塞项

- Structure：冻结 generation 的 events、tags、memberships、group truth、markets、issues
  组件计数及 support/incomplete 计数。现有 `record_count` 是多组件总行数，不能标为市场数。
- Quote：expected、executable、unavailable、invalid、group completeness、batches 与时间范围。
- Analysis：`known → structurally supported → quote complete → evaluated → positive edge /
  no edge / rejected` 的漏斗。每一层都注明分母，且必须通过守恒测试；没有持久化的拒绝原因
  不作声称。
- Opportunities：认证正边机会、legs、edge/size、projection generation 与 parent Quote。
- Blockers：服务端按影响排序并严格限量，字段包括 scope、code、impact、since、关联
  incident 与证据下钻链接。

## 信息架构

根路径重定向到 `/business`；一级导航只有 **Business Research** 与 **Runtime**。

- `/business`：信任条（资格、as-of、generation）→ Structure → Quote → 分析漏斗 →
  Opportunities → Blockers。机会在末尾，避免把罕见结果误作系统全貌。
- `/business/structure`、`/business/quotes`、`/business/analysis`、`/business/opportunities`：
  对应产品的全局研究页。
- `/business/events/[event_id]`、`/business/groups/[group_id]`、
  `/business/markets/[market_id]`：实体详情必须绑定 query 参数中的精确 generation，
  当前指针已经淘汰时返回可解释的 `410`。
- `/control-plane`：按“产品指针 → incidents/recovery → tasks → qualification → cloud/evidence”
  排序，承担运维证据与恢复解释。
- 旧 `/perception` 保持 legacy，直到与新事务契约做 shadow parity 后才可从新导航移除。

所有卡片均展示状态、generation、发布时间、as-of/age、单位和分母；不用颜色或
`Date.now()` 承担业务事实。页面不可用与单产品不可用须视觉和文案分开。

## 分阶段交付

1. **事实边界**：实现单事务 `BusinessOverviewV1`、API、严格 Python/TypeScript decoder；
   CLI 改为读同一 API/contract。首屏仅呈现已有严格事实与“未提供”。
2. **产品研究**：补齐 Structure/Quote 发布摘要与可证实的 analysis funnel，交付
   `/business` 与四个全局产品页。
3. **实体研究**：增加 generation-bound indexes、实体详情、旧 generation 语义和独立
   低优先级读池/截止时间，确保重查询不挤占 operator reader。
4. **收敛**：从 Runtime 证据页建立 blocker 下钻，做 legacy shadow parity，更新 Make
   入口和业务操作指南。

## 验收与验证

- 后端：指针切换压力下不混合 generation；漏斗守恒；zero、paused、stale、lagging、
  not-published、unavailable 的 API 契约；实体 pagination/404/410。
- 客户端：decoder 拒绝非法状态、数字和 lineage；fixture 覆盖所有状态分支、正边机会、
  长 ID 与分页；Vitest/RTL 和 Playwright（desktop、375px、键盘、错误态）。
- 生产：`make control-plane-business-brief` 与 `/perception/business-overview` 的同一
  快照语义一致；业务读取不可用时 `/control-plane` 仍可用；业务 API 无任何写副作用。

## 非目标

本轮不增加交易、资金、收益、P&L、手工恢复按钮、同步 R2 扫描，亦不以浏览器聚合或
历史趋势图替代数据库权威事实。
