# 05 — Issue #180 ghost_book 实战

## 核心心智模型

> **Polymarket 的 `/book` 接口对大多数 liquid 市场返回幽灵簿（ghost book）：bids[0]=0.01 / asks[0]=0.99**，
> 但同时 `get_prices` 接口给出真实可成交价（如 0.55）。
>
> 这不是 bug，是默认行为。72% 的 liquid (>$1k) 市场都是这样。

GitHub Issue: [Polymarket/clob-client#180](https://github.com/Polymarket/clob-client/issues/180)（2024 年至今未修）

→ 所有下游 phase 都必须遵守的硬约束：

1. **取价用 `get_prices`，不用 `book.bids[0].price` / `book.asks[0].price`**
2. **取流动性 / 簿深度用 `book.bids[].size` / `book.asks[].size` 仍可信**
3. **best_bid_price / best_ask_price 字段在 SQLite/Parquet 里是"原始记录"，不是策略输入**

## 数字震惊我们的瞬间

live run #001（2026-04-29）跑完后：

```
subset markets fetched: 17,259
tokens to fetch:        34,518   (yes + no)
ghost_book issues:      24,949   ← 72.27%
clob_missing issues:     3,180
unparseable book:           0
api_jitter (Layer 1):       1    ← 因 dedupe 4% 几乎必触发
```

下游每写一个策略，都必须先读这份数据。详见 `phases/01-/01-LIVE-RUN-001.md`。

## 为什么会发生？

我们的理解（基于 GitHub issue 讨论 + 经验）：

1. Polymarket 的 CLOB 是真订单簿，但**大量 market maker 不挂单子**或者用了链下签名机制
2. UI 显示的"价格"（你打开 polymarket.com 看到的）来自 `get_prices`，是**taker quote**，不是 book top
3. `/book` 接口返回的 `bids/asks` 是**链上挂单的真实状态**，可能确实只有一些极端价位的订单
4. 所以 book 显示的 0.99 / 0.01 是真的（确实有人挂这些极端价位的小单），但**不是市场真实成交价**

## 这意味着什么（对于策略）

### ❌ 错误的代码

```python
# 想做"YES 价格 + NO 价格 = 1 ± epsilon" 套利
yes_price = market["best_ask_price"]   # 来自 SQLite 的 best_ask_price 字段
no_price = ...
if yes_price + no_price > 1.02:
    place_order(...)
```

→ 用 `best_ask_price` 字段做策略 **错**。这个字段是 `book.asks[0].price`，72% 概率是 0.99。
你会以为找到了"NO 卖 0.99 的便宜货"，下单立刻被打 → 真实价格是 0.55，谁卖你 0.99？

### ✅ 正确的代码（Phase 2 之后）

```python
# 用 get_prices 的 SELL quote 作为"我能卖出的价格"
# 用 get_prices 的 BUY quote 作为"我能买入的价格"
prices = await clob.get_prices_buy_sell([yes_token_id, no_token_id])
yes_buy = float(prices["buy"][yes_token_id]["BUY"])
yes_sell = float(prices["sell"][yes_token_id]["SELL"])
# 这才是 taker quote，可以下单
```

或者：用 `book.bids[].size` 看深度，但**价格不信 book**：
```python
# 簿深度（YES 一边能卖出多少）
total_bid_size = sum(float(b["size"]) for b in book["bids"][:5])  # 前 5 档
# 但价格信号取自 get_prices
yes_actual_quote = ...
```

## 在我们代码里现在的处理方式

Phase 1 选择：**仍然把 best_bid/best_ask_price 灌进 SQLite/Parquet**。

为什么？因为：
- 它是**原始记录**。下游如果要研究 ghost_book 的形状变化（例如"这个 market 的 ghost_book 持续多久了"），需要这个字段
- Layer 4 validator 已经把"它是不是 ghost"标记到了 `validation_issues` 表 —— 下游策略**联表查询**就能避免误用

策略代码的正确读法：

```sql
-- 找最近一次 snapshot 里所有"非 ghost_book"的市场
SELECT m.*
FROM markets m
WHERE m.market_id NOT IN (
    SELECT market_id FROM validation_issues
    WHERE snapshot_id = (SELECT MAX(id) FROM snapshots)
      AND category = 'ghost_book'
)
```

或者最简单的：**`best_bid_price` 字段直接当作"参考"，真的下单前再调一次 `get_prices`**。

## 在 normalizer / orchestrator 里的具体痕迹

`orchestrator.py:31-34`（Phase 1 简化注释）：
```
top-of-book attached only for ``yes_token_id`` (NO side is symmetric
on Polymarket; Layer 4 validator still checks both tokens for ghost-book).
```

`validator/layers.py:8-12`（Layer 4 用意）：
```
Layer 4 ghost-book defense addresses Polymarket issue #180 (RESEARCH.md Pitfall 1):
when /book reports a top-of-book like ask=0.99/bid=0.01 (no real liquidity) but
/price (taker quote) returns ~0.55, the order book is "ghost" / stale and a naïve
arbitrage signal would mis-fire. We surface this divergence as a categorized Issue
so downstream code can skip the market instead of trading a phantom edge.
```

`Category.GHOST_BOOK` 在 `category.py:22`：
```
GHOST_BOOK = "ghost_book"  # ⚠️ issue #180 defense (RESEARCH.md Pitfall 1)
```

## 触发逻辑回顾（来自第 04 章）

```
对每个 (market, token):
  if book 在 books_by_token 里:
    top_ask, top_bid = float(asks[0].price), float(bids[0].price)
    if top_ask > 0.98 and top_bid < 0.02:                    ← 看起来死了
      ref = prices_by_token[token].buy                       ← 真实参考
      if abs(ref - top_ask) > 0.05:                          ← 但参考价反对
        emit Issue(GHOST_BOOK)
```

## 跨 phase 沉淀

这个发现已经写入：
- `.planning/threads/market-microstructure.md` SESSION 06（永久参考）
- `.planning/workstreams/m1-perception/phases/01-/01-LIVE-RUN-001.md`（实战报告）
- `.planning/JOURNAL.md` SESSION 07 [LEARNING] 段（决策时间线）

任何未来的 phase（M2 / M3 / M4 / M5）做策略前，**都要遵守这条硬约束**。CLAUDE.md 第 3 项原则"代码是主线，paper 是验证手段"在这里具象化：我们的代码里已经把这个事实编码进去了，不是"以后注意"，是"基础设施级别就处理了"。

## 自检题

1. 我现在打开 SQLite 看 `markets.best_ask_price`，看到一个 market 是 `0.99`。这意味着什么？我能下单按这个价吗？
2. `book.bids[0].size` 显示 100。这个数字可信吗？为什么？
3. Phase 2 要在 WebSocket 层订阅价格 —— 我应该订 `/book` 频道还是 `/prices` 频道？为什么？
4. 我写了一个套利策略，用 SQL 从 markets 表读价。怎么改 SQL 才能避开 ghost_book 市场？
5. 如果某个 market 在 Layer 4 没有 ghost_book issue，它的 `best_ask_price` 一定可信吗？(hint: clob_missing 和正常状态的差别)

## FAQ 增量

_暂无_

---

← [04-validator-layers.md](04-validator-layers.md) | 下一节 → [06-security-invariants.md](06-security-invariants.md)
