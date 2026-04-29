# 04 — Validator 三层防御

## 核心心智模型

数据来源是不可信的（外部 API、可能 jitter、可能有 bug、可能被攻击）。
我们用**分层校验**来把不同种类的错误分开看：

```
Layer 1  ─┐  Gamma 报了 N 个市场，我真的拿到 N 个吗？
          │   → API jitter / 拉取漏行
          │   → 严格：不一致 → is_valid=False
          │
Layer 2  ─┤  这一行市场的字段全吗？liquidity 高的市场反而没 token_id 是不正常的
          │   → 字段完整性
          │   → 仅记录，不影响 is_valid
          │
(Layer 3 ─┤  时间一致性 —— Phase 1 不做，Phase 3 时序场景再开)
          │
Layer 4  ─┘  Gamma 说 mid=0.46，CLOB 订单簿说 ask=0.99 / bid=0.01，
              get_prices 又说 0.55 —— 这三个对不齐 → ghost_book
              → 跨数据源对账
              → 仅记录，不影响 is_valid
```

代码：`src/polyarb/validator/layers.py`（280 行）+ `validator/category.py`（41 行）

## 为什么没有 Layer 3？

CONTEXT.md 的 D-D2 决策：Phase 1 不做时序校验（"上一个 snapshot 这个市场 ask=0.46，这次 ask=0.85，10 分钟内涨这么多合理吗？"）。
理由：Phase 1 没有"上次"概念 —— 它是单次快照工具。Phase 2 (WebSocket 增量) 才有时序流，Layer 3 推到那时候。

我们保留了"Layer 3"这个槽位（编号不重排）—— 未来加进来不会和现有 issue 表里的 `layer` 列冲突。

## Layer 1 — 计数校验（is_valid 的唯一驱动）

代码：`validator/layers.py:79`

```python
def layer1_count(reported_total: int, fetched_count: int) -> list[Issue]:
    if reported_total != fetched_count:
        return [Issue(
            layer=1,
            category=API_JITTER,
            market_id=None,
            detail=f"Gamma reported {reported_total} active markets, fetched {fetched_count}",
        )]
    return []
```

逻辑超简单：拉之前 Gamma 告诉我们"有 N 个 active market"，拉完 normalize 完应该还是 N 个（去重之后可能少一点 —— Phase 1 决议 strict equality，dedupe 后的 markets 数和 reported 不一致就 fire）。

⚠️ Phase 1 实际行为：**dedupe 之后 markets 数 ≠ Gamma reported**（dedupe 干掉了 ~4% 重复行），所以**几乎每次 live run Layer 1 都会 fire**。这个不是 bug，是预期 —— is_valid=False 的 categorized success（D-D3：仍然落库，但 exit 1）。

`is_valid_overall`（`layers.py:275`）就一行：
```python
return not any(i.layer == 1 for i in issues)
```
**只看 Layer 1**。Layer 2 / Layer 4 不影响这个标志。

## Layer 2 — 字段完整性 + 分类

代码：`validator/layers.py:99`

要求字段（`REQUIRED_FIELDS`）：
```python
("market_id", "condition_id", "yes_token_id", "no_token_id",
 "mid_price", "liquidity_usd", "end_time_ms")
```

逻辑：每个市场都过一遍 → 哪些 required 字段是空的 → 如果有空 → 启发式分类。

```
end_time_ms 在未来 24h 内?
├─ 是 → category = RESOLVING（市场快结束了，字段缺失能理解）
└─ 否 → liquidity_usd < $10?
        ├─ 是 → category = ZOMBIE_MARKET（垃圾市场）
        └─ 否 → category = UNKNOWN（系统债 —— 我们应该把它细分类掉）
```

副作用：把这一行 market 的 `incomplete` 字段改成 `True`（"标记，不删除"）。
下游策略可以选择跳过 incomplete=True 的市场，但 Phase 1 阶段我们不做这个判断 —— 数据仍然落库。

⚠️ **`UNKNOWN` 是系统债**。CONTEXT.md D-D4 的硬约束："steady state 下不能有持续 UNKNOWN issue"。如果 live run 看到大量 UNKNOWN，就要把它们模式分析后做出新的 Category。

`Category` 定义在 `validator/category.py:16`：
```python
class Category(str, Enum):
    ZOMBIE_MARKET = "zombie_market"      # 流动性极低、明显死了的市场
    RESOLVING = "resolving"              # 接近结束的市场，字段缺正常
    API_JITTER = "api_jitter"            # 计数对不上等 API 不稳定症状
    API_UNREACHABLE = "api_unreachable"  # 整个端点不通
    CLOB_MISSING = "clob_missing"        # 这个 token 在 CLOB 没数据
    GHOST_BOOK = "ghost_book"            # ⭐ Issue #180 重点防御
    UNKNOWN = "unknown"                  # 系统债，绝不能稳态有
```

## Layer 4 — 跨源对账（ghost_book 防御主场）

代码：`validator/layers.py:159`

逻辑（每个市场的 yes_token_id 和 no_token_id 各跑一遍）：

```
对每个 (market, token_id):

  step 1: token 在 books_by_token 里吗？
          ├─ 不在 → Issue(CLOB_MISSING)，跳过这个 token
          └─ 在 → 继续

  step 2: top_ask_price 和 top_bid_price 能 _safe_float 出来吗？
          ├─ 不能（字段缺失或非法 string） → Issue(UNKNOWN, 带 raw_payload 截断)
          └─ 能 → 继续

  step 3: top_ask > 0.98 AND top_bid < 0.02 吗？  （book "看起来死了"）
          ├─ 不是 → 跳过（不是 ghost_book 候选）
          └─ 是 → 继续

  step 4: 拿 prices_by_token[tid].buy 作为参考价
          这个值和 top_ask 差距 > 0.05 吗？
          ├─ 是 → Issue(GHOST_BOOK)：order book 说 0.99 但 taker quote 说 0.55，对不齐
          └─ 否 → book 真的死了（ask=0.99 / quote=0.99 一致）
```

**为什么阈值 0.98 / 0.02 / 0.05？**

- `0.98` ask 和 `0.02` bid：当一个市场是真活的，best ask 通常远低于 0.98（除非那个 outcome 真的是"几乎肯定不发生"）。**两边同时极端**才是"book 看起来死了"
- `0.05` divergence：getprices 的 quote 和 book ask 的合理误差范围。超过 5 分钱的差距就是一个明确信号"book 在说谎"

这些阈值是 RESEARCH.md 的初始猜测，live run 数据可以校准。

实战数据（live run #001）：subset 17,259 markets × 2 tokens = 34,518 token，里面 24,949 个被标 ghost_book —— **72%**。
这告诉我们 Issue #180 不是边缘情况，是默认行为。详见 [05-ghost-book-issue-180.md](05-ghost-book-issue-180.md)。

## Issue dataclass

代码：`validator/category.py:26`

```python
@dataclass(frozen=True)
class Issue:
    layer: int                          # 1, 2, 4
    category: Category
    market_id: str | None               # None for Layer 1（不针对单个市场）
    detail: str                         # ≤ 200 字符（F-5 截断）
    raw_payload: str | None = None      # ≤ 1024 字节（F-5 截断）
```

`detail` 和 `raw_payload` 都有截断。理由 F-5：单条 issue 不能撑爆数据库。

## 有效性策略对照（Phase 1）

| 层 | 触发是否影响 is_valid | 是否落库 | 严格度 |
|---|---|---|---|
| Layer 1 count mismatch | ✅ 影响 | ✅ 落库 | strict equality |
| Layer 2 字段缺失 | ❌ 不影响 | ✅ 落库（issue + market 都落） | record-only |
| Layer 4 ghost_book / clob_missing | ❌ 不影响 | ✅ 落库 | record-only |
| API_UNREACHABLE | 间接（如果 Layer 1 因此触发） | ✅ 落库 | record-only |

CONTEXT.md Q5 决议：Phase 1 优先不阻塞数据收集，让所有信息进库可分析。"是否要让 ghost_book 也阻塞 valid" 等 Phase 3 收集到证据后再说。

## 为什么 Layer 4 这种宽松策略是对的

如果 Layer 4 也阻塞 is_valid，live run 看到 72% ghost_book → 几乎每次 snapshot 都 invalid → make exit 1 → 用户疲于"为什么又失败了"。

但**真相是数据本身就是 72% ghost_book**，这不是我们的 bug，是 Polymarket 的现实。我们的工作不是把 invalid 当成"不该发生的事"，而是**把它分类后让下游决定怎么处理**。

→ 下游策略代码会读 `validation_issues` 表，看到一个 market 在最近 N 次 snapshot 里都是 ghost_book → 列入"不可信定价"黑名单 → 不在它上面下单。

这是项目章程"看清市场再下手"的工程化体现。

## 代码地图

| 文件 | 行 | 关键函数 |
|---|---|---|
| `src/polyarb/validator/category.py` | 41 | `Category` enum + `Issue` dataclass |
| `src/polyarb/validator/layers.py` | 280 | `layer1_count` / `layer2_fields` / `layer4_cross_source` / `is_valid_overall` |

## 设计取舍

1. **`layer2_fields` 有副作用** —— 改了入参 markets 的 `incomplete` 字段。这违反"validator 应该是纯函数"的直觉，但 Phase 1 选择 pragmatic：在同一遍循环里既产 issue 又打标记。代码里有显式注释（`layers.py:103`）。
2. **阈值是常量不是 Settings** —— 因为这些是"市场结构事实"（ghost_book 的形状特征），不是部署参数。用户不会有理由调它们。如果未来要 A/B 测，再升级成 Settings。
3. **Layer 4 跑 yes 和 no 两个 token** —— 即使 Phase 1 我们只把 yes 的 best_bid/ask 灌回 row（步骤 5），Layer 4 仍然检查 NO 那一边的 ghost_book。所以哪怕只持久化半边，**校验记录是完整的**。

## 自检题

1. 我看到一次 snapshot 报 `28,229 issues`，里面 `ghost_book: 24,949`、`clob_missing: 3,180`、`api_jitter: 1`。这次 `is_valid` 是 True 还是 False？为什么？
2. 一个 market 字段不全，被 Layer 2 打了 `incomplete=True`。它会进 SQLite markets 表吗？
3. 为什么 Layer 1 用 `==` 严格相等，而不是允许 ±10 行的 jitter？（看代码注释和 CONTEXT.md）
4. Category.UNKNOWN 出现 → 我应该做什么？（不是看代码，是看 CLAUDE.md / CONTEXT.md 的工作纪律）
5. Phase 2 加 Layer 3 时序校验，加在 `validator/layers.py` 还是另起文件？为什么？

## FAQ 增量

_暂无_

---

← [03-market-snapshot-shape.md](03-market-snapshot-shape.md) | 下一节 → [05-ghost-book-issue-180.md](05-ghost-book-issue-180.md)
