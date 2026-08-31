# M1 业务数据产品 Dashboard 设计

## 目标

让 Dashboard 从“机会结果 + 运维明细”升级为 M1 的业务研究工作台：既能回答今天
是否有认证机会，也能持续研究机会形成前的 Structure、Quote、覆盖、新鲜度、分类与
恢复事实。

## 信息架构

### `/business` — 业务总览

首屏固定回答：今天是否具备判断条件、资格原因、Structure/Quote 当前产品、覆盖规模、
认证机会、业务阻塞与下一步。它消费 canonical business brief；不可用必须明确展示，
不以零或前端推断替代。

### `/business/data-products` — 数据产品研究

按数据产品而非任务类型展示：

| 产品 | 全局研究指标 | 下钻内容 |
| --- | --- | --- |
| Structure | generation、市场/事件记录数、发布时间、完整性 | manifest、成员关系、source/materialize/range/certify 进度与失败 |
| Quote | generation、父 Structure、报价记录数、发布时间、cadence | admission/batch/certify 状态、freshness、retry/缺口 |
| Opportunity | 认证 count、group/event、edge/size | group timeline、legs、机会转换与分页证据 |
| Qualification | state/reason、epoch、freshness、eligible progress | breaker、certificate、事实时间线 |

默认提供全局摘要；点击 generation、group、incident 或 blocker 进入已有 `/perception`
与 `/control-plane` 证据页。首版不制造不存在的历史趋势、价格指标、收益或执行结论。

### `/control-plane` — 运行状态与恢复

保留现有任务、controller、lease、incident、recovery、watchdog、资源面板；从业务页
带上下文下钻，解释“为什么这个数据产品不可判断/暂停”，而不是让操作者先读所有任务。

## 数据与可信边界

- Dashboard 读取已有严格验证的 control-plane / perception read models，业务总览复用
  `business-brief` canonical shape；不以浏览器时钟或多个异步响应拼造业务事实。
- 每张卡显式显示 authority 状态、observed/published time、generation 与 unavailable
  reason。`available + 0`、`paused`、`unavailable` 是三种不同业务结论。
- API/read model 未提供的指标显示 `未提供`，并明确下钻入口；不得以任务数推断市场覆盖，
  不得以 Quote count 推断机会或收益。

## 交互与实现原则

业务总览采用结论优先、数据产品卡片、异常优先级、证据下钻四层；运行页继续承担技术
细节。共享 TypeScript strict validator 和 server-side read，提供 loading/unavailable
states 与小屏可读布局。全部 observer-only。

## 验收

- `/business` 一屏显示业务结论、四个数据产品、top blockers 与下钻链接；
- `/business/data-products` 可全局比较 Structure/Quote/Opportunity/Qualification，并下钻
  到现有证据；
- 每个 unavailable/zero/paused 分支可区分且经过 fixture contract 测试；
- Dashboard 不增加写操作、交易动作、P&L 推断或新的数据库 authority。
