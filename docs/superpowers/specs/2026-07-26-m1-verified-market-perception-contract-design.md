# M1 已验证市场感知契约设计

> 日期：2026-07-26  
> 状态：已批准方案 A，等待书面规格复核  
> 范围：M1 市场事实、人工查询入口、M1→M2 消费契约  
> 不含：真实资金下单、钱包授权、M2 执行适配器

## 1. 产品结论

M1 的核心价值不是证明进程活着，而是持续回答三个问题：

1. 市场现在发生了什么；
2. 人和程序怎样取得可解释、可追溯的新鲜市场事实；
3. 哪些事实已经达到 M2 可以继续评估的质量，哪些必须拒绝。

运行维护检查是底线，不是产品主体。M1 收口必须同时交付：

- **事实面**：市场、事件、结果集合、盘口、深度、成交、变化和异常；
- **查询面**：一个日常简报入口、单事件/单市场深入查询、Dashboard；
- **策略面**：只向 M2 输出通过完整性与新鲜度证明的候选，不输出下单许可。

## 2. 触发本设计的生产证据

2026-07-26 的只读生产检查显示：

- L1 strict health 为 PASS；
- 报价 run 年龄约 38 秒，collector 为 PASS；
- L2 订阅 107 个 token，L3 membership 为 10/10；
- `/arbitrage/opportunities` 正常返回 20 条候选。

但第一条候选将 Michigan Republican Senate Primary 识别成两腿组合：

- M1 只持有 Kent Benham 与 Fred Heurtebise；
- 两腿 ask 合计 0.008；
- feed 因而报告 9,920 bps、约 99.2% 的 gross edge。

Gamma 事件 `111080` 的权威事件对象实际包含 33 个结构成员，其中 6 个已经启用：
Kent Benham、Fred Heurtebise、Mike Rogers、Genevieve Scott、Bernadette
Smith、Andrew Kamal；另有 Other 和未命名占位结果。这个事件还是
`negRiskAugmented=true`。

根因链已经复现：

1. M1 使用 Gamma `/markets` 的 offset 分页；
2. 当前生产式请求在 offset 2100 收到 HTTP 422；
3. `GammaClient._paginate` 将这个 422 当作正常分页结束；
4. snapshot 和 quote run 对“已取得的子集”保持内部原子一致，却没有证明它是市场全集；
5. opportunity scanner 将同一 `negRiskMarketID` 下仅有的两条本地腿误当成完整集合。

Gamma keyset 分页可取得被旧 offset 路径遗漏的另外四个已启用结果。官方
Negative Risk 文档还明确区分标准和 augmented neg-risk：augmented 事件有占位
结果与 Other，不能套用普通“买齐当前可见 YES 即保证 $1”假设。

因此，当前机会 feed 的进程可用性为真，但其完整组合语义不成立。在本设计落地前，
它只能作为异常发现输入，不能指导 M2 paper 或 live 决策。

## 3. 已选方案与边界

### 3.1 采用：已验证市场感知契约

M1 保存原始市场事实，同时生成带证明的读模型。M2 只能消费通过质量门的读模型。

优点：

- 完整性、新鲜度和数据身份只实现一次；
- 人工查询与 M2 使用同一份事实；
- 不会把 Gamma、CLOB 和事件结构陷阱复制到每个策略；
- M1 不需要知道 M2 的资金、持仓或下单实现。

### 3.2 不采用：只暴露原始数据

这会迫使每个 M2 策略独立判断事件是否完整、报价是否同批、placeholder/Other
如何处理。不同策略最终会产生互相矛盾的“市场事实”。

### 3.3 不采用：M1 直接输出下单指令

M1 不掌握账户余额、已有仓位、实际手续费、订单原子性和风控授权。让感知层直接
生成订单会混淆事实、策略和执行责任。

## 4. 责任分界

### 4.1 M1 负责

- 发现完整事件及其全部结构成员；
- 标记标准或 augmented neg-risk、Other、placeholder、active/closed；
- 维护市场和事件 membership 身份；
- 采集同一报价 run 的 ask、size、时间与终态；
- 提供 top-of-book、深度、成交、OHLC 和变化事实；
- 证明数据覆盖、新鲜度和组完整性；
- 生成 gross-before-fees 候选；
- 对缺页、缺腿、混批、陈旧或语义不支持的组合 fail-closed。

### 4.2 M2 负责

- 将候选转换成策略信号；
- 按目标仓位计算费用、滑点和市场冲击；
- 决定 FOK/FAK/GTC、腿顺序和失败补偿；
- 处理部分成交、仓位、现金账和 oracle 风险；
- 应用资金限额、kill switch 和人工批准；
- 决定 paper 或 live；M1 不授予真实交易权限。

## 5. 市场事实模型

### 5.1 事件是组合完整性的根

广覆盖市场发现改用 Gamma keyset 分页。对于 neg-risk，Gamma event 对象是组
结构的权威输入，不能仅凭 `/markets` 中碰巧出现的相同
`negRiskMarketID` 分组。

每个事件快照至少保存：

- `event_id`、`neg_risk_market_id`；
- `neg_risk_type = standard | augmented`；
- 全部结构成员的 market ID；
- active named、inactive placeholder、Other 各自身份；
- `expected_member_count`、`active_named_count`；
- 规范化 membership hash；
- event/source 更新时间和本次取得时间；
- 本次分页完成证明。

inactive placeholder 和 Other 可以不进入普通行情轮询，但必须进入结构事实。否则
“没有报价”会被误写成“这个结果不存在”。

### 5.2 市场事实

每个可交易市场至少提供：

- event/group/market/condition/token 身份；
- question、slug、结算时间和状态；
- bid/ask、对应 size、spread；
- 深度档位及其 venue timestamp；
- 最近成交、价格变化和流动性；
- snapshot、quote run、WS generation 和持久化时间；
- data-quality flags。

### 5.3 四种完整性状态

- `complete-supported`：结构完整且当前策略语义受支持；
- `complete-unsupported`：结构已知，但 augmented/Other 等语义不属于当前策略；
- `incomplete-source`：分页、事件或 membership 取得不完整；
- `incomplete-quotes`：结构完整，但至少一条必需腿缺报价或已陈旧。

只有 `complete-supported` 可以进入 M2 候选。

## 6. Neg-risk 候选契约

### 6.1 当前支持范围

第一版只支持 **standard neg-risk buy-all**：

- 完整结果集合在事件创建时已知；
- 每条结果都是当前可交易的 named outcome；
- 所有必需 YES 腿来自同一 complete quote run；
- 每条腿都有合法 ask 和正 size；
- `sum_asks < 1` 才生成候选。

### 6.2 Augmented neg-risk

`negRiskAugmented=true` 的事件不进入第一版 buy-all feed。它们以
`complete-unsupported` 出现在市场简报和事件查询中，并明确给出
`augmented-neg-risk-not-supported` 原因。

不能只买当前 UI 可见或 active named outcomes：未命名结果可能以后澄清，未被
命名的获胜者由 Other 承接。针对 augmented neg-risk 的转换或交易策略必须作为
独立的 M2 设计，不得通过放宽 M1 完整性门实现。

### 6.3 候选输出

每条候选至少包含：

- `event_id`、`group_id`、`membership_hash`；
- `quote_run_id`、`quoted_at`、`quote_age_seconds`；
- `strategy = neg-risk-buy-all`；
- `profit_basis = gross-before-fees`；
- 全部 legs 的 market/condition/token/ask/ask_size；
- `sum_asks`、`gross_edge_bps`；
- `max_top_level_quantity`、`gross_profit_at_max_top_level`；
- `quality = complete-supported`；
- 明确的非授权声明。

top-of-book 候选只证明在每腿当前第一档中存在相同数量，不保证多腿原子成交。
M2 若评估更大仓位，必须使用 M1 深度数据按目标 quantity 重算 VWAP 和容量，不能
线性放大第一档。

### 6.4 拒绝而不是静默丢弃

接口除 `opportunities` 外同时返回 bounded rejection summary：

- incomplete source groups；
- augmented groups；
- missing/stale quote groups；
- invalid identity groups。

这样“0 条候选”表示经过完整扫描后没有正 edge，而不是数据缺失。source
不完整时整个 feed 返回 503；单个已证明结构完整但报价缺失的组可以被拒绝并计数。

## 7. 三类使用入口

### 7.1 日常市场简报

新增统一 Makefile 入口：

```bash
make market-brief-prod
```

输出面向操作者的一页摘要：

- 市场、事件和结果覆盖数量；
- 新增、关闭、临近结算和主要流动性变化；
- spread/depth/freshness 异常；
- standard/augmented neg-risk 数量；
- 通过与拒绝的候选数量及原因；
- L1/L2 数据年龄和是否允许 M2 继续消费。

它是“今天值得看什么”的入口，不要求用户先读 health JSON。

### 7.2 单事件和单市场

新增：

```bash
make show-event-prod event_id=<id>
make show-market-prod market_id=<id>
```

`show-event-prod` 展示完整结果树、标准/augmented、Other/placeholder、每腿当前
状态、membership hash 和拒绝原因。`show-market-prod` 展示盘口、深度、成交、
变化、新鲜度和所属事件。

### 7.3 Dashboard

保留 `/status`、`/candidates`、`/signals`、`/l3/<asset_id>`，新增或重构为：

- `/events/<event_id>`：完整事件结果集合；
- `/markets/<market_id>`：单市场实时与历史事实；
- `/opportunities`：通过的候选和被拒绝的分组摘要。

Dashboard 使用与 API/CLI 相同的读模型，不在前端重新实现完整性判断。

## 8. M1→M2 数据流

```text
Gamma keyset markets ─┐
                      ├─▶ event + market truth snapshot
Gamma event members ──┘          │
                                 ├─▶ human brief / event / market queries
                                 │
CLOB quote run ──────────────────┼─▶ completeness + freshness gate
L2 depth/trades/OHLC ────────────┘              │
                                                ▼
                                  verified opportunity candidate
                                                │
                                                ▼
                              M2 cost/risk/routing/paper evaluation
```

M2 必须保存并回显 `membership_hash + quote_run_id`。如果执行前这两个身份任一
改变或 quote 超龄，M2 必须重新评估，而不是沿用旧候选。

## 9. 故障与运行策略

### 9.1 两种 24 小时窗口

- **故障发现窗口**：可恢复异常只记录，不立即修复重启；跑满原定时间以收集故障
  种类、频率、影响和恢复时长。
- **最终验收窗口**：已知缺陷批量修复后运行；应用严格连续性标准。

当前 release-75 窗口继续作为故障发现窗口。只有持续不可恢复、事实错误继续扩大、
监控失明、数据库损坏或资源失控才提前中断。

### 9.2 数据质量健康

运行健康与市场事实健康分开：

- process/transport health：进程、WS、数据库、调度是否工作；
- coverage health：keyset 是否正常终止、事件是否完整、membership 是否一致；
- freshness health：snapshot、quote、depth、trade 是否在 SLA；
- product health：brief/query/feed 是否基于同一已验证读模型。

进程绿色不能覆盖 coverage failure。旧 offset 分页的“收到 422 后正常结束”改为
fail-closed；最后一个成功完整 snapshot 可以保留供诊断，但不能被刷新成新事实。

## 10. 测试与生产验收

### 10.1 自动测试

- 超过 2,100/10,000 行的 keyset 多页遍历；
- cursor 重复、缺页、HTTP 错误和中途终止；
- event membership 与 market stream 不一致；
- Michigan augmented fixture：33 个结构成员、6 active named，不产生 buy-all；
- standard neg-risk 完整组产生候选；
- 缺一腿、混 quote run、旧 quote、重复 token 全部拒绝；
- `available-zero` 只在 source 完整且所有组均已分类时成立；
- API、Makefile、Dashboard 和 M2 consumer 的契约测试。

### 10.2 生产只读验收

- keyset 抓取完成并留下终止/cursor/count 证据；
- 对抽样和所有候选事件调用 Gamma event endpoint，membership count/hash 一致；
- 每条 opportunity 都是 standard neg-risk；
- 每条 opportunity 的 legs 数等于事件必需成员数；
- 所有 legs 属于同一 membership 和 quote run；
- augmented 事件只出现在 unsupported/rejection summary；
- `market-brief-prod`、event/market 查询和 Dashboard 展示同一身份；
- M2 paper evaluator 能消费真实候选，并拒绝篡改或陈旧身份。

## 11. 实施切片

1. **停止假事实**：keyset + event membership + coverage fail-closed，撤回当前 feed
   的策略可用声明。
2. **建立查询产品**：market brief、event/market API 与 Makefile 入口。
3. **建立 M2 契约**：verified candidate schema、rejection summary、paper consumer。
4. **Dashboard 与运行门**：共用读模型、coverage health、故障发现/最终验收报告。

切片 1 完成前，当前 `/arbitrage/opportunities` 不得作为 M2 输入。当前 24 小时
故障发现窗口继续运行，不因本设计和本地开发重启生产。

## 12. 完成定义

M1 只有同时满足下列条件才算“实战持续可用”：

1. 能证明市场和事件覆盖完整，而不是只证明请求成功；
2. 用户能从一个简报进入事件和市场细节，不需要拼 health JSON；
3. 每个事实带来源、身份、时间和质量状态；
4. M2 只收到完整、同批、新鲜且语义受支持的候选；
5. augmented、缺腿、陈旧和分页失败均 fail-closed；
6. 人工查询、Dashboard 与 M2 使用同一读模型；
7. 运维告警能说明故障，故障发现窗口能持续收集，最终验收窗口能严格签字；
8. 不存在任何由 M1 health、candidate 或 opportunity 自动升级出的真实资金授权。

## 参考

- Polymarket Gamma keyset pagination:
  <https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination>
- Polymarket Negative Risk Markets:
  <https://docs.polymarket.com/advanced/neg-risk>
- 当前 M1 手册：`docs/M1-市场感知平台使用手册.md`
- 当前生产机会流教学：`docs/learning/23-生产机会流.md`
- 原 M1→M2 设计：
  `docs/superpowers/specs/2026-07-17-m1-m2-neg-risk-discovery-design.md`
