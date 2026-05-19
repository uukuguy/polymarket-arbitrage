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

---

## IMDEA Type-2 套利经济学（m2 T2 fee-differential 模型的论文根据，2026-05-20）

> Trigger: m2-combinatorial Phase 2 T2 走向锁定为 Option B (fee-differential 模型) 后,需要把论文经济学量级与代码模型量级对照。

### Type-2 定义（论文术语）

- **Type-1** = AMM ↔ AMM 跨池价差（早期 Polymarket 主要套利形式,2024 前）
- **Type-2** = **cross-venue 同结果不同场所的 fee/spread 差**（Polymarket Public CLOB 推出后出现，2024 至今主流）
- **Type-3** = negrisk group `sum(best_ask) < 1` 多合约组合（见 §6.4，这是另一种 combinatorial 套利，与 Type-2 fee differential 互补）

### Type-2 的经济学量级（论文数据 + 代码模型对照）

| 维度 | 数值 |
|---|---|
| 总交易笔数 | 86M |
| 套利总利润 | $40M |
| Top 3 钱包合计 | $4.2M (≈10.5% of total) |
| 单笔平均利润 (粗估) | $40M / 86M ≈ $0.47/笔 (低,因大部分是小单) |
| Top 3 钱包平均单笔 (估) | ~$1-4/笔 (依单均规模) |

**模型量级对照** (用 `SlippageParams` 默认值):
- BUY 场景: CLOB maker (-10bps) vs PM taker (-50bps) → `fee_diff_bps = 40bps`
- 单笔 $1k size × 40bps = **$4 fee differential / 笔**
- 这与论文 Top 3 钱包平均单笔 ~$1-4 实证量级一致 → 模型在 $1k-$10k size 区间是合理的

### m2 T2 validation 测试设计

T2 IMDEA validation 测试至少要 cover:
1. **fee_diff_bps BUY clob_maker_avail** — 断言 40bps (CLOB maker rebate -10 vs PM taker -50 实际拿到 maker side 时的差)
2. **fee_diff_bps SELL no_clob_maker** — 断言 20bps (PM rebate +30 vs PM taker +50 退而求次的差)
3. **estimate_cross_execution_savings @ $1k size** — 断言 savings_bps × notional 在 $1-10 量级,对齐论文 Top 3 钱包平均单笔利润

### 引用

- 论文出处: `docs/research/polymarket-oss-landscape-2026-04.md` 引用 (IMDEA 86M / $40M / $4.2M 三组数字)
- 代码出处: `src/polyarb/models/slippage.py` SlippageParams.fee_diff_bps 方法 + estimate_cross_execution_savings 方法
- Plan 锁定: `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/02-1-PLAN.md` Revision 4 (2026-05-20)

### Open Questions (未来 m2 推进时考虑)

- **Top 3 钱包 $4.2M 的 size 分布**: 论文是否给单笔 size 中位数 / 90 分位?这会决定 SlippageParams.small/mid_notional 阈值的真实校准
- **PM rebate 现状**: pm_rebate_bps 默认 30bps 是设计假设,实际 Polymarket 当前 maker rebate 是否还是这个数?需要 2026-Q2 重新 verify (Polymarket 2026-02 移除 taker delay 时可能调过 fee)
- **CLOB maker availability rate**: clob_maker_avail 取决于实时 BBO 是否有 maker 挂单,这是 m1 Phase 03 (L2 orderbook) 才能给数据。在 L2 出来前 m2 T3 只能用占位/概率分布

---

## SESSION 06 — Polymarket Issue #180 实测：72% liquid 市场 `/book` 给假价（2026-04-29）

第一次 live API run（17,259 markets, liquidity > $1k）真实数字：

| 类目 | 计数 | 占 L4 issues |
|---|---|---|
| ghost_book | 24,949 | **89.1%** |
| clob_missing | 3,063 | 10.9% |

24,949 ghost_book ÷ 34,518 subset tokens ≈ **72% 的 active liquid 市场** `/book` 端点返回的是幽灵 0.01/0.99 而不是真价。

**这不是"边缘 bug"，这是 Polymarket CLOB 的当前默认行为。**

### 对策略层的硬约束（每个下游 phase 都必须遵守）

1. **mid 价 vs ask 价 vs `/book` 价 vs `/prices` 价 — 优先级**
   - 套利计算的"真价"：`get_prices(BUY)` 和 `get_prices(SELL)`，**不是** `/book.bids[0].price` 或 `/book.asks[0].price`
   - 流动性瓶颈 / 冲击成本：仍用 `/book.bids[0].size` 和 `/book.asks[0].size`（**size 是真的，price 是假的**）
   - mid 价（Gamma `bestBid` + `bestAsk`）：只用作 sanity check，不进套利公式

2. **跨源对比是默认操作，不是可选项**
   - Phase 1 layer4_cross_source 已经做了：`abs(book_mid - prices_mid) > 0.05` 触发 `ghost_book`
   - Phase 3 异常检测：所有"真实可成交价差"必须先经过这个 cross-check，否则全是噪声

3. **WebSocket 增量（Phase 2）**：
   - 订 `book` 频道做 size 跟踪
   - 订 `prices` 频道（如果有）做价格信号
   - 千万别让 `book` 价更新触发 trade decision

4. **真实 tradeable universe ≈ 17k market**（subset >$1k），不是 49k 全集

### 实际数字 vs RESEARCH.md 早期估计

| 指标 | RESEARCH.md 推测 | Live 实测 |
|---|---|---|
| Active markets total | ~12,000 | **48,985** |
| Subset (>$1k) | "几百到一两千" | **17,259** |
| `/book` 异常率 | "Issue #180 偶发" | **72% 持续异常** |

→ 早期估计严重低估市场规模和数据质量噪声。后续所有 phase 用这份 LIVE-RUN 数字。

### 套利者的工程公理（更新）

旧版（discuss SESSION 04）：
> mid 价 vs ask 价 — 套利计算必须用 ask 价（来自订单簿）

更新版（live SESSION 06）：
> mid 价 vs `/prices` 价 — 套利计算必须用 `/prices`，**不能信** `/book.asks[0].price`
> 真实可成交价 = `get_prices(SELL)` (你买入时对手 ask)
> 流动性瓶颈 = `/book.asks[0].size` × 价差（size 仍可信）
