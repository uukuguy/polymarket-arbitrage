---
slug: market-microstructure
title: Polymarket Market Microstructure
status: open
created: 2026-04-28
updated: 2026-04-28
---

# Thread: Market Microstructure

> Polymarket（以及一般 prediction market）市场微观结构的认知累积。
> 一切策略（M2 / M3 / M4）的执行可行性判断都基于这里的原理。
> 跨能力线永久存活，会话开头如做策略相关讨论应预读。

---

## 1. Polymarket 数据宇宙的 4 层结构

| 层 | 内容 | 套利角色 | 数据特征 |
|---|---|---|---|
| **Gamma API** | 市场 metadata + mid 价 + 流动性聚合 | 广覆盖扫描（找候选） | 12k+ 全量、限流宽松、无鉴权 |
| **CLOB API** | 订单簿（多档）+ 成交流 | 决定能否真实变现 | 限流较严（~1-2 req/s public）、REST + WS |
| **Subgraph (TheGraph)** | 链上事件历史、所有 trade、LP 操作 | 对手分析、IMDEA Type 2 数据来源 | GraphQL、历史数据完整 |
| **Polygon RPC** | CTF 余额、UMA 仲裁状态、链上真相 | 风控/终极仲裁 | 慢、需要 Web3.py + ABI |

**第一性原理**：前三层都是衍生品，链上是源头。设计风控时 RPC 必须接（参考 Paris 吹风机 oracle 错判事件）。

---

## 2. 订单簿（CLOB = Central Limit Order Book）

每个 Polymarket 市场有**两个独立订单簿**：YES 簿、NO 簿。

```
NO 簿示例（价格 0-1 美元）:
  ASK（卖单，从低到高）
  0.95     50 份
  0.96     200 份
  0.97     500 份
  ─────────────────────────  ← 价差 (spread)
  BID（买单，从高到低）
  0.92     30 份
  0.90     100 份
```

- `best_ask` = 卖单最低价 = 你**买入**的真实成本
- `best_bid` = 买单最高价 = 你**卖出**的真实回收
- `mid_price` = (best_bid + best_ask) / 2 = **展示价**（前端 / Gamma 给的就是它）
- `spread` = best_ask - best_bid

---

## 3. mid 价 vs ask 价（套利从业者第一红线）

> **永远不要用 mid 价计算套利空间。**

Gamma 显示的"价"是 mid。看起来漂亮，**下不到单**。
你按 mid 下市价单，会从 best_ask 开始**往上吃订单簿**，每多一档价更差。

### 反例（Phase 1 discuss SESSION 04 实战题）

某市场 mid: YES @ 0.30 / NO @ 0.65 → 看似 5% 套利空间（YES + NO = 0.95，缺口 0.05）。
查 CLOB：YES 簿 best_ask **0.32** 量 100 份 / NO 簿 best_ask **0.68** 量 50 份。

按 ask 算：

- 真实成本：0.32 + 0.68 = **1.00**
- 真实利润率：**0%**（手续费一算就是亏）

那 5% 是 mid 之差，**幽灵价格**。

### 正确公式

```
real_arb_space(YES_NO_market) = best_ask(YES) + best_ask(NO) - 1
                              （正值才是真套利空间）
```

任何用 mid 价算出的"机会"都是候选，**必须用 ask 价复算才能下单**。

---

## 4. 流动性瓶颈 = min(best_ask 量) × 价差

做组合套利（同时买 YES 和 NO）必须**两边数量相等**——少了一边就不是套利，是单边敞口。

### 算法

```
max_positions = min(best_ask(YES).size, best_ask(NO).size)
max_capital   = max_positions × (best_ask(YES) + best_ask(NO))
real_profit   = max_positions × (1 - (best_ask(YES) + best_ask(NO)))
                                  ↑ 仅当 ask 之和 < 1 时为正
```

### 反例

YES @ 0.32 量 100 / NO @ 0.68 量 50：

- 瓶颈是 NO（50 份）
- 最大资金：50 × 1.00 = $50
- 实际利润：$0

> **真实套利者口头禅**："价差是利润，深度是上限。两个都得有。"

价差再诱人，吃不完那一档量就只是看起来诱人。

---

## 5. 冲击成本（Market Impact）— 隐形税

你越激进、单子越大，越往订单簿深处吃，每多一档价格更差。

### 反例（同样 SESSION 04 实战题）

如果硬塞 $5000 进上面那个市场：

```
YES 簿被扫穿，平均成交价从 0.32 → ~0.42
NO 簿被扫穿，平均成交价从 0.68 → ~0.78
合计 1.20 → 每份组合亏 $0.20
$5000 投入到期变 ~$4170，亏 17%
```

### 实战守则

- 大资金套利者**从不下市价单**——用 limit + 分单（slicing）
- 单笔执行量必须用 best_ask 那一档的量做硬上限
- 多档深度可以让你预估"如果分 N 笔下，平均成交价多少"

### Phase 1 设计含义

- snapshot 至少要拉 best_ask **价 + 量**（识别"机会大小天花板"）
- 不需要本 phase 拉多档深度——多档是 Phase 3 异常检测时按需二次拉取的工作

---

## 6. Polymarket 特定属性（区别于股票交易所）

### 6.1 没有传统"做空"

- "做空 NO" = "买 YES"（YES + NO = 1，互为对冲）
- 不需要券，不需要保证金交易
- 但**资金占用**和股票做空类似——你必须实际买入对手腿

### 6.2 结算逻辑

- 到期：YES 和 NO 必有一边为 1、一边为 0
- 不存在"中间结算"——非黑即白（除少数事件 unresolvable 走 invalid 50/50 结算）

### 6.3 2026-02 移除 ~500ms taker 延迟

- 之前 taker 单要等 500ms 才执行（保护 maker）
- 现在移除 → MM 风险升、HFT 门槛降
- **战略含义**：高频策略（M4 子方向）变得更有可能；MM rebate 策略风险提高

### 6.4 negrisk group（多合约同事件）

- 同一事件可能有多个互斥结果（"谁赢 2024 大选" → Trump / Harris / Kennedy / ... 各为独立合约）
- 同 group 内所有合约**必有且仅有一个**最终为 1
- 套利公式：`sum(best_ask) over group < 1` → 全买保证赚 1 - sum
- 这是 IMDEA 论文 Type 2 套利的主要数学根

详情待 m2-combinatorial 启动后展开累积。

---

## 7. 流动性分布的常识

- Polymarket 12k+ 市场中大部分**没有真实流动性**（liquidity_usd < $100，所谓 "zombie" 市场）
- 真正可吃的市场可能只有几百到一两千个（liquidity > $1k）
- "看起来便宜"的低流动性市场往往**永远没有 ask 量**——价格只是某人挂了 1 份的小单

### Phase 1 设计含义

- 默认 snapshot 子集模式：仅拉 liquidity > $1k 的市场（覆盖真实可吃部分）
- `--full` 全量模式留给周末或调研用

---

## 待累积（后续 phase 填充）

- [ ] CLOB 多档深度的"虚假深度"识别（big quote pulled instantly）
- [ ] Maker rebate 经济学（M4 MM 策略相关）
- [ ] negrisk group 内部相关性建模
- [ ] 跨市场的"事件相关性"（如 ETH 价格相关市场之间的 implied correlation）
- [ ] 时段流动性规律（美东工作日 vs 周末，重大新闻前后）
- [ ] CLOB WebSocket 增量更新的"陈旧"问题（Phase 2 将遇到）
- [ ] 限流的真实表现（不同 endpoint 实测 RPS）

---

## 历史关键事实

- **2026-02**：Polymarket 移除 ~500ms taker 延迟
- **IMDEA 论文**：86M 笔交易、$40M 套利历史、Top 3 钱包合计 $4.2M
- **Paris 吹风机事件**：oracle 单点错判，提示 RPC/UMA 状态必须接入风控
