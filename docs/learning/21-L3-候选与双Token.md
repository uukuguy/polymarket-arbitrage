# L3 候选与双 Token：先看见代表性盘口，再做严格升级

## 30 秒心智模型

L3 promoter 不是“从全市场里直接挑 5 个市场”，它只能从 L2 已经观察到的
盘口里挑。Phase 05.3 把这条链补成两步：

1. `l3-seed` 让 L2 最多观察 100 个流动性足够、价格不极端的 Yes 盘口；
2. 原来的 L3 recipe 仍只挑 `spread < 0.02`、Yes depth > $500 的前 5 个，
   然后把每个 Yes 资产还原成完整的 Yes/No token pair。

一句话：**seed 决定“看谁”，promoter 决定“谁合格”；两个门不能混。**

## 数据流

```text
Gamma clobTokenIds = [Yes, No]
  → normalizer / SQLite / Parquet
  → markets_latest.yes_token_id + no_token_id
  → l3-seed 选 <=100 个 Yes token
  → L2 WS 写 Yes-side top_of_book / depth
  → l3-promote 严格选 5 个 Yes asset
  → 按 yes_token_id 查回 (Yes, No)
  → 5 个完整 pair = 10 个 distinct token
  → WS full book → l2_book_levels / OHLC
```

这里最容易误解的是 `asset_id`。在 `l2_candidates` 和
`l2_top_of_book` 这条链里，它不是 market ID，而是 Polymarket 的 **Yes token
ID**。所以 promoter 不能去 `markets_latest.asset_id` 查询——生产表根本没有这个列。

## 关键代码

### 1. 两个 token 都进入 durable projection

`src/polyarb/storage/supabase_mirror.py:43-55`：

```python
_NARROW_MARKET_COLUMNS = (
    # ...
    "yes_token_id",
    "no_token_id",
)
```

Alembic 006 只新增 nullable `no_token_id`。nullable 是诚实语义：上游缺 token
时保存 NULL，不伪造身份；后续 promoter 会 fail closed。

### 2. seed 只负责观察覆盖

`src/polyarb/observation/recipes.py:80-91`：

```python
"l3-seed": Recipe.from_builtin(
    name="l3-seed",
    where=(
        "yes_token_id IS NOT NULL "
        "AND mid_price BETWEEN 0.1 AND 0.9 "
        "AND liquidity_usd >= 500"
    ),
    order_by="liquidity_usd DESC, market_id ASC",
    limit=100,
)
```

100 不是拍脑袋：一次只读样本中，100/100 个 book 完整，86/100 在不改 L3
阈值时已经合格。seed 仍受全局 `MAX_CANDIDATES=500` 和 watchlist precedence
约束。

### 3. token lookup 必须按 Yes ID 对齐

`src/polyarb/observation/l3_promote.py:162-187`：

```python
client.table("markets_latest") \
    .select("yes_token_id, no_token_id") \
    .in_("yes_token_id", yes_asset_ids)
```

返回值也以 Yes token 为 key。这样 TOB 选出的 `asset_id` 能直接查到自己的 No
兄弟，不需要 Gamma 运行时请求，也不需要不存在的 market-level asset ID。

### 4. 5 个市场不一定等于 10 个 token

`src/polyarb/observation/l3_promote.py:585-605` 会拒绝以下 pair：

- Yes 或 No 为空；
- 查询返回的 Yes 与所选 Yes asset 不相等；
- Yes == No；
- 任一 token 已被另一个 pair 占用。

被拒绝的 pair 是整对丢弃，不保留单独 Yes，也不再用 `asset_id` fallback。
因此 `l3:active_count < 10` 仍会如实暴露 under-fill。

### 5. dry-run 是 plan，不是 apply

`promote_run(..., apply_mutations=False)` 仍可读取 TOB、跑 recipe、查 token pair
并计算 `added/removed/proposed_active`，但不会：

- 调用 WS add/remove；
- 更新 `l2_candidates.l3_promoted_at_ts`；
- 修改 `_l3_active_set`、LKG map 或 freshness timestamp。

所以 `make l3-promote-dry-run` 可以用于诊断，但它不是生产证据，也不会把
`l3:active_count` 变绿。

## 设计取舍

### 为什么不直接降低 spread/depth 阈值？

因为旧输入只有三个临近结算、价格接近 0/1 的市场。此时降低 promotion 阈值，
等于同时改变“看什么”和“什么算好”，无法知道哪个变量起作用。先补观察覆盖，
再保持 promotion gate 不动，实验才能解释。

### 为什么把 No token 存进 Supabase，而不是运行时请求 Gamma？

L1 已经拿到了 `[Yes, No]`，SQLite/Parquet 也已保存。继续把它投影到 Supabase
避免了 promoter 的额外网络依赖、时间错位和源不一致；代价只是一个 add-only
nullable column。

### 为什么 invalid pair 是 under-fill，而不是冻结旧 10 tokens？

Supabase 请求异常仍走 LKG freeze；但一次成功查询返回不完整身份是数据合同问题。
冻结旧集合会掩盖新输入的身份缺陷。under-fill 让 `/health` 暴露真实状态。

## 对手测试

1. 某市场 spread=0.005、depth=$10k，但 `no_token_id=NULL`。它能进入 L3 吗？为什么？
2. `l3-seed` 选了 100 个市场，是否意味着 L3 会订阅 200 个 token？指出第二道门。
3. 两个市场意外共享同一个 No token。若仍报告 10 tokens，会破坏什么身份不变量？
4. dry-run 输出 10 个 `proposed_active`，能否据此开始 24h soak？还缺哪类证据？
5. 如果生产迁移了 006 但没有新的 L1 mirror snapshot，旧行的 No token 是什么？

## FAQ 增量

### Q：`asset_id` 为什么不统一改名成 `yes_token_id`？

WS 和 L2 表的既有协议广泛使用 `asset_id`，一次全局改名会扩大迁移范围。Phase
05.3 在边界函数和教学文档中锁定“这条链里的 asset_id = Yes token ID”，先修正
语义错误；全局 schema rename 不属于本次必要范围。

### Q：seed 会不会永远占掉 100 个 subscription？

它进入现有 candidate reconciliation，不是独立永久集合。每次 snapshot 都重新计算，
不再满足价格/流动性条件的市场会被移除，watchlist 和专用 recipe 仍按原规则参与。
