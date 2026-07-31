# Neg-risk 自动机会盯盘设计

**日期：** 2026-07-27
**状态：** 已完成设计讨论；等待书面规格审阅
**目标：** 把云端 M1 从“持续采集与故障告警”变成一条用户能直接使用的自动盯盘闭环：发现合格 neg-risk 报价机会、持续重点跟踪、发送可复核的 Telegram 观察卡，并向 M2 提供相同的机器可读事实。

## 1. 问题与边界

现有云端已经能发布完整 Structure、采集 Quote、计算 gross-before-fees
neg-risk 候选，并报告自身健康。但用户只看到了服务故障告警，未得到“哪个事件、
买哪些腿、成本多少、为什么值得看”的策略信息。

本设计只交付一条策略闭环：**standard neg-risk buy-all 观察信号**。它不是自动
下单系统，且不声称已扣除手续费、滑点或多腿成交风险。

### 非目标

- 不签名、不下单、不持仓、不触发资金动作。
- 不把 gross edge 标为净利润或可执行套利。
- 不要求 L2/L3 通过才发现观察信号；L2/L3 将在 paper/实盘阶段用于执行前确认。
- 不建设全市场浏览器、新闻系统或大而全历史仓库。

## 2. 云端职责与运行层次

本机或浏览器仅发起请求、阅读结果；采集、校验、保存、告警和供 M2 消费都在
云端完成。

| 层次 | 云端工作 | 当前是否常驻 |
|---|---|---|
| 市场地图 | 完整 Structure、结算关系与策略资格的可复现版本 | 默认每 30 分钟刷新；不按 5 分钟机械全量抓取 |
| 全局发现 | 对地图中合法 standard neg-risk 组收集同轮 Quote，计算 gross edge | 是，作为发现传感器，默认每 2 分钟 |
| 重点跟踪 | 对刚跨阈值的组短周期复核成本、容量和状态 | 仅对 watchlist 中机会运行，默认每 15 秒 |
| 通知与事实 | 保存机会生命周期、提供 M2 API、推 Telegram | 轻量常驻 |
| L2/L3 | 实时盘口与执行证据 | 未来 paper/实盘阶段启用，不阻塞本设计 |

市场地图可保存并按需读取；全局 Quote 循环有明确用途：发现报价是否跨过机会阈值。
这不是为了持续收集所有原始数据。

## 3. 最小市场地图

每次建图产生一个不可变 Structure revision。核心地图只保存能改变后续策略判断的
四类事实：

| 地图层 | 保存事实 | 策略用途 |
|---|---|---|
| 市场身份 | 市场问题、事件、condition/token、active/closed | 定位可交易腿 |
| 结算关系 | event、neg-risk 组、完整成员、membership hash | 证明“买全”覆盖所有结算路径 |
| 策略资格 | standard/augmented、缺腿、不可交易成员、拒绝原因 | 决定是否允许该组进入 buy-all 策略 |
| 版本证明 | revision、生成时间、完整性、来源 hash | 防止 Structure/Quote 混批 |

地图向用户和 M2 提供三类视图：可扫描组、拒绝组及原因、指定事件详情。它不要求
用户浏览原始全市场行。

## 4. 发现与扫描合同

1. 使用当前完整且未过期的地图（默认最大年龄 30 分钟）；地图过期时先重建。
2. 仅对地图中 standard、complete-supported 且每条必要腿可交易的组取同轮 Quote。
3. 任何 Quote run 缺腿、结构版本不匹配或过期，整组不可评估，不输出伪零机会。
4. 计算：`bundle_cost = sum(best_ask)`；`gross_edge_bps = (1 - bundle_cost) * 10_000`；
   `max_bundle_size = min(leg.ask_size)`。
5. 首次达到 `gross_edge_bps >= 100` 才创建观察机会。100 bps 是过滤噪声的默认值，
   不是净利润阈值。

结果只能是：`observe`（达到阈值）、`no-edge`（结构和报价合法但未达阈值）、
`ineligible`（结构不支持）或 `unavailable`（事实不完整/过期）。只有 `observe`
进入机会生命周期；M2 读取它后仍必须加费用、滑点、资金和执行规则。

## 5. 机会生命周期与重点跟踪

```text
eligible quote group
  ├─ gross edge < 100 bps → no-edge
  └─ gross edge >= 100 bps → observe / 首次 Telegram
                                  │
                                  ▼
                           focused watchlist
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
        edge/容量实质变化      结构或报价失效        edge 跌破阈值
              │                   │                   │
              ▼                   ▼                   ▼
          更新机会卡             invalidated          closed
```

全局两分钟发现循环不能假设机会会存活到下一轮。首次 `observe` 后，组进入重点
watchlist，由短周期定向 CLOB 复核。重点跟踪持久化每次观察，至少能回答：机会
持续多久、edge 方向、最低可成交规模、哪一腿变化导致失效，以及是否发生
Structure revision 变化。

重点跟踪保存两层追加事实：

1. **机会主记录：** 首次发现时间、事件/全部腿、初始 Structure revision 与
   Quote run、初始成本/edge/规模、当前状态。
2. **观察序列：** 每次定向复核的时间、成本、gross edge、最低可成交规模、变化
   最大的腿、Structure/Quote 身份和状态转换原因。

第一版只保存顶档报价和关键变化，不无限保存每次完整深度盘口。这样能重放机会从
发现到消失的过程；L2/L3 原始深度和成交证据留给 paper/实盘阶段。机会主记录长期
保留，观察序列默认保留 30 天后按日摘要压缩，避免无限增长。

通知去重规则：首次进入、gross edge 相对上次通知变化至少 25 bps、最低可成交规模
相对上次通知变化至少 20%、失效/关闭时才推送；稳定机会不重复刷屏。通知失效不
改变机会事实，失败待重试并保持审计记录。这些数值均为可配置的生产默认值。

## 6. 用户卡片与 M2 数据合同

Telegram 是人类摘要；云端持久化并通过 M2 接口提供同一事实。示例：

```json
{
  "status": "observe",
  "strategy": "neg-risk-buy-all",
  "event_id": "event-id",
  "event_title": "event title",
  "legs": [{"market_id": "market-id", "token_id": "token-id", "ask": 0.31}],
  "bundle_cost": 0.97,
  "gross_edge_bps": 300,
  "max_bundle_size": 42,
  "structure_revision": 764,
  "quote_run_id": 804,
  "quote_age_seconds": 18,
  "execution_status": "not-verified",
  "alert_reason": "entered-gross-edge-threshold"
}
```

每条事实必须保留 event、所有腿、Structure revision、membership/universe identity、
Quote run、时效、阈值和状态转换原因。Telegram 明确显示“仅观察，未扣手续费、
滑点和多腿成交风险”。

## 7. 操作方式

最终操作面必须区分本机控制台与云端任务：

- `make build-market-map`：请求云端创建并保存一次完整地图任务；不在本机抓市场。
- `make inspect-market-map`：读取云端地图的可扫描组、拒绝组或事件详情。
- `make scan-neg-risk-map`：请求云端以指定地图完成一次 Quote 扫描。
- `make watch-opportunities-status`：读取云端发现、重点跟踪和最近通知状态。

这些名称是接口目标；实现时必须在 Makefile 暴露统一入口，并说明其是否产生云端
采集任务。未来 paper/实盘模式才开放连续 L2/L3 和执行前确认开关。

## 8. 失败、时效与监控

- Structure 失败：保留最后完整地图；达到地图时效上限后停止新发现，明确显示
  `unavailable`。
- 全局 Quote 失败/过期：停止新 `observe`，不把无法判断当作零机会。
- 重点跟踪失败：机会标为 `unavailable` 或 `invalidated`，保留最后观察和原因。
- L2/L3 未通过：不阻塞观察信号，但所有卡片保持 `execution_status=not-verified`。
- 健康告警与机会告警分离：服务故障不能掩盖机会状态，机会存在也不能压制故障告警。

## 9. 验收

1. 给定完整地图和完整 Quote，系统只为 standard complete-supported 组创建候选。
2. gross edge 首次跨过 100 bps 时，恰好创建一个持久 `observe` 事实并发送一张卡。
3. 稳定处于阈值上方时不重复告警；edge/容量实质变化与关闭各产生一次可追溯转换。
4. 重点跟踪能定位导致机会消失的腿或 Structure revision 变化。
5. M2 API 与 Telegram 对同一机会具有相同 revision、Quote run、腿和成本。
6. Structure/Quote 不完整、过期或版本漂移时不产生 `observe`。
7. 重点机会可从主记录和观察序列重放持续时间、edge/容量变化与失效原因。
8. 所有云端任务、通知重试和状态转换均能从持久记录和健康查询复核。

## 10. 实施顺序

1. 固化市场地图 revision 与云端读取合同，移除“本机直接采集”假设。
2. 将现有 verified neg-risk Quote 结果转换为持久机会事实和阈值状态机。
3. 实现 Telegram 机会卡、去重、变化/关闭通知与 M2 读取接口。
4. 实现重点 watchlist 的定向复核与生命周期证据。
5. 将常驻 Structure 调度改为地图刷新策略；L2/L3 留待 paper/实盘执行确认。
