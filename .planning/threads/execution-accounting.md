# Execution Accounting Thread

> 跨 phase/workstream 累积：现金单位、fee/PnL authority、venue truth、迁移与对账边界。

## 2026-07-17 — M2 Phase 5 exact cash ledger

### 已验证事实

- SQLite REAL 的两次逻辑 `+10` 可累计成 `19.999999999999996`；CLI rounding 不能替代账本精度。
- pUSD 使用六位 amount；paper account 采用 `1 pUSD = 1_000_000 micros`。
- `Decimal(str(value)) + ROUND_HALF_EVEN` 是 paper modeled cash 的集中量化规则。
- SQLite INTEGER affinity 不是类型不变式：外部可写入 REAL，因此每次 load 必须验证 `typeof(...) = 'integer'`。
- Phase 4 REAL schema 可在 `BEGIN IMMEDIATE` 内 additive ALTER、backfill、validate、commit；非法金额会连同新增列一起回滚。
- exact receipt 需要带 scale/type：`{"kind":"money","micros":N}`；裸 JSON integer 会和 bool/单位语义冲突。

### 跨能力线边界

- **M1/M4 perception/valuation**：价格、概率、score 可继续使用 float；它们是观察/排序，不是 cash authority。
- **M2 execution**：balance、stake、fees、realized PnL、settlement receipt 必须进入 Money 边界。
- **M3 live venue**：实际 fill cash/fee 是 venue truth，应覆盖 paper formula；order signing/tick/amount wire precision 必须独立验证，不能假设 float 可签名。
- **M5 industrialization**：reconciliation 应比较 canonical micros/venue amount，而不是格式化后的 CLI float。

### 触发条件

当 live adapter、fee accounting、partial fills 或 reconciliation 任一启动时，先回答：

1. venue 返回的是 price/size 还是明确 cash amount？
2. 谁负责 rounding，venue 还是本地 model？
3. receipt/outbox 的 canonical unit 是什么？
4. legacy float 输入在何处只量化一次？
5. 哪个 raw storage/query 证明 authority 没降级？

## 2026-07-17 — H-004 Knowledge Layer: quantity is not cash

### 新发现

- 当前 M2 的 `size/stake/filled_size` 同时承担 shares 和 pUSD，导致余额与 PnL 不可能同时正确。
- `ArbitrageLeg.pm_size` 由 `price * size` 计算成本，证明它是 shares；`ExecutionLeg.size`
  文档却写 dollars，而路由仍把它乘价格。
- 官方 V2 market order 的 `amount` 是 side-dependent：BUY 表示花费的 pUSD，SELL 表示卖出的
  shares。不能把 SDK 字段名当作领域单位。
- FAK、`size_matched` 和 trade `size` 证明 partial-fill 真相首先是 token quantity；现金 proceeds
  与 fee 是另一条维度。

### 决策

先做 H-004，把 exact `Quantity`、position quantity、cash cost basis、fill quantity 分开；之后
H-005 才允许实现 partial aggregation。H-006 再引入 venue-confirmed cash/fee truth。价格仍保留
decimal-facing estimate，不扩张 H-003 的 Money 范围。
