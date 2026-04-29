# 01 — Polymarket 的数据双源

## 核心心智模型

Polymarket 的数据**不在一个 API 里**。我们要打两个不同的服务，它们各自只懂一半的事：

```
┌─────────────────────────────────────────────────────────┐
│  Gamma API (HTTP REST)                                   │
│  → 市场元数据：题目、token IDs、流动性、成交量、结束时间   │
│  → 一次性拉所有"活的"市场（pagination）                   │
│  → 不知道"现在多少钱"                                     │
│                                                          │
│  Endpoint: /markets?active=true&closed=false             │
│  我们的代码: src/polyarb/clients/gamma_client.py         │
└─────────────────────────────────────────────────────────┘
                         +
┌─────────────────────────────────────────────────────────┐
│  CLOB API (官方 SDK: py-clob-client)                     │
│  → 订单簿（bids/asks 和大小）+ 现价（buy/sell quote）     │
│  → 必须按 token_id 一笔一笔问                            │
│  → 不知道"这个 token 是哪个市场的、市场题目是什么"        │
│                                                          │
│  Methods: get_order_books([token_id]) + get_prices(...)  │
│  我们的代码: src/polyarb/clients/clob_client.py          │
└─────────────────────────────────────────────────────────┘
```

**所以 snapshot 流程一定是**："先拿 Gamma 拿到所有市场和它们的 token_id，再拿这些 token_id 去问 CLOB 现在的价格和簿"。

## 一些关键术语先定义清楚

| 术语 | 是什么 | 例子 |
|---|---|---|
| **market** | 一个预测题目 | "Trump wins 2026 election?" |
| **market_id** | Gamma 给 market 的整数 ID（字符串形式） | `"516542"` |
| **condition_id** | 链上的市场标识符（hex 字符串） | `"0xabc...123"` |
| **token** | 市场里的一种结果（YES 或 NO 各是一个 token） | YES token / NO token |
| **token_id** | 链上的 ERC1155 token ID（uint256，70+ 位十进制） | `"71321045679..."` |
| **outcome price** | YES token 现价（0 到 1 之间，可解读为概率） | `0.46` |
| **liquidity** | Polymarket 给这个市场打的"流动性分"，单位 USD | `$1,234` |
| **clobTokenIds** | Gamma 返回字段，**JSON 字符串编码的 list of token_id** | `'["7132...", "8924..."]'` |

⚠️ **`clobTokenIds` 是字符串不是 list** —— Gamma 偷懒用 JSON 编码塞进 JSON。我们的代码必须 `json.loads` 一次。这是我们叫 "Pitfall 2"（见 RESEARCH.md）。

⚠️ **token_id 必须当字符串处理** —— Polymarket 用 ERC1155，token_id 是 uint256（70+ 位十进制），Python `int` 能装下但 SQLite/Parquet 的 `int64` 装不下，会溢出。所以全程 `str`。这是 "Pitfall 3"。

## Gamma 给的字段（normalize 之前，原始 dict）

我们关心的部分（完整字段更多，无关的我们不存）：

```python
{
    "id": "516542",                    # → market_id
    "conditionId": "0xabc...",         # → condition_id
    "slug": "trump-wins-2026",         # 人类可读 URL slug
    "question": "Will Trump win...?",
    "clobTokenIds": '["7132...", "8924..."]',   # ⚠️ JSON 字符串
    "outcomePrices": '["0.46", "0.54"]',        # ⚠️ JSON 字符串，[0]=YES [1]=NO
    "liquidityNum": 1234.56,           # ← 优先用 *Num 字段
    "liquidity": "1234.56",            # 字符串 fallback
    "volumeNum": 99999.0,
    "volume": "99999.0",
    "endDate": "2026-11-05T00:00:00Z", # ISO-8601, UTC
    "active": true,
    "closed": false,
    "negRisk": false,                  # 是否属于 neg-risk multi-outcome 市场
    "negRiskMarketID": null,           # 父市场 ID（如果是 neg-risk 的子市场）
}
```

**normalize 干的事**：把这堆原始字段→我们自己的标准字段名，做类型转换，解 JSON 字符串。
代码：`src/polyarb/snapshot/normalizer.py:75` `normalize_market(raw)`。

## CLOB 给的字段（两种调用）

### 调用 1：`get_order_books([token_id, ...])` → 订单簿

返回一个 list，每个元素是 `OrderBookSummary`（py-clob-client 的 dataclass）：

```python
OrderBookSummary(
    market="0xabc...",        # ⚠️ 这里 "market" 字段值是 conditionId，不是 market_id
    asset_id="71321045...",   # ⭐ 这是 token_id（我们 indexing 用这个）
    bids=[
        {"price": "0.45", "size": "100.0"},
        {"price": "0.44", "size": "50.0"},
        # ...
    ],
    asks=[
        {"price": "0.47", "size": "200.0"},
        # ...
    ],
    timestamp="...",
    hash="...",
)
```

⚠️ `bids[0]` 和 `asks[0]` 是 **best bid / best ask**（top of book）。后面靠的就是它。
⚠️ price 和 size 都是**字符串**，要 `float()`。

### 调用 2：`get_prices([BookParams(token_id, side="BUY"|"SELL"), ...])` → 现价

返回一个 dict：

```python
# 一次只能问一个 side，所以我们各调用一次再合并
{
    "7132104567...": {"BUY": "0.46"},
    "8924...":       {"BUY": "0.54"},
    # ...
}
```

合并 BUY+SELL 之后我们的代码里长这样（`clob_client.py:102`）：

```python
{
    "buy":  {token_id: {"BUY":  "<price>"}},
    "sell": {token_id: {"SELL": "<price>"}},
}
```

## 为什么需要两个不同的价格源？

这是 Phase 1 Layer 4 validator 存在的根本原因，下面这个是关键：

> **Polymarket Issue #180**：order book 里 `bids[0].price=0.01 / asks[0].price=0.99`（看起来死了），但 `get_prices` 返回 `0.55`（taker quote 是真价格）。
>
> 这说明 order book 是**幽灵簿** —— 没人挂真单，但市场实际有定价。
>
> live run #001 实测：**72% 的 liquid (>$1k) market 都是这种状态**。

→ 详细见 [05-ghost-book-issue-180.md](05-ghost-book-issue-180.md)

## 代码地图

| 文件 | 作用 | 关键函数 |
|---|---|---|
| `src/polyarb/clients/gamma_client.py` | Gamma HTTP client | `fetch_all_active_markets()` |
| `src/polyarb/clients/clob_client.py` | CLOB SDK 异步包装 | `get_books([tids])` / `get_prices_buy_sell([tids])` |
| `src/polyarb/snapshot/normalizer.py` | Gamma raw → 我们的 row dict | `normalize_market(raw)` |

## 设计取舍（你需要知道的）

1. **Gamma 用 httpx + tenacity，CLOB 用 py-clob-client + asyncio.to_thread**
   - Gamma 是裸 REST，自己控制 retry / rate limit / timeout 更直接
   - CLOB SDK 是同步代码，没有 async 接口，只能丢线程池跑
   - 见 `gamma_client.py:1-30` 和 `clob_client.py:1-30` 的 module docstring

2. **CLOB client 没有 retry**
   - 因为 SDK 是同步的，套 tenacity 要么阻塞 event loop 要么搞复杂的 thread pool 舞蹈
   - 让异常往外抛，由 orchestrator 分类
   - 实测如果 CLOB 频繁失败，再回来加（CONTEXT.md 决议过）

3. **Gamma 的 4xx (非 429) 不重试**
   - 4xx 是请求方错误，重试解决不了。`gamma_client.py:47` `_NonRetryableHTTPError` 专门把这种异常包成 tenacity 不会重试的类型

## 自检题

读完这一节，你应该能答（不要急着看答案，看代码自己想）：

1. 我手里有一个 `market_id="516542"`，要拿到它现在 YES 的 buy quote，需要打几次 API？哪些？
2. `OrderBookSummary.market` 这个字段值是什么？是 `market_id` 吗？
3. `clobTokenIds` 字段我直接 `raw["clobTokenIds"][0]` 行不行？为什么？
4. 我把 `token_id` 存成 SQLite `INTEGER` 类型行不行？为什么？

## FAQ 增量

（每次会话你提问后，我把答疑追加在这里，不动正文）

_暂无_

---

下一节 → [02-snapshot-pipeline.md](02-snapshot-pipeline.md)
