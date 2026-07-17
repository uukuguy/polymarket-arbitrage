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

## 2026-07-17 — H-004 verified unit-safe execution ledger

### 已验证事实

- BUY 100 @ .50 的开放余额是 950、cash exposure 是 50；@ .60 full close 后余额 1010、PnL 10。
- fully-collateralized paper SELL 100 @ .60 锁 40；@ .50 close 后同样余额 1010、PnL 10。
- full-fill equality 现在比较 exact Quantity；`0.1 + 0.2` 在 micro-share 边界量化后可确定等于 0.3。
- Phase 5 legacy BUY 100 @ .50、balance 900 迁移后 balance 950；restart 不重复 refund。
- partial v3 column、非法 side、非 INTEGER authority 与 cost-basis inconsistency 全部 fail closed。

### 给 H-005/H-006 的约束

- H-005 residual/aggregation authority 必须是 Quantity；每个 fill ID 只能减少一次 remaining quantity。
- 部分关闭需要按 exact quantity 分配 cost basis，不能把原仓位 Money 与 fill shares 直接相减。
- H-006 的 venue proceeds/fee 是 Money truth；SDK side-dependent amount 只在 adapter boundary 翻译。

## 2026-07-17 — H-005 verified durable partial fills

### 已验证事实

- position quantity/cost basis 直接表示 remaining authority；partial mutation、cash、PnL 与 receipt
  在同一 repository transaction 内提交。
- venue fill identity 固定为 `venue-fill:{fill_id}`；caller operation ID 不能制造第二条事实。
- proportional allocation 可 HALF_EVEN，但 final fill 必须消费全部 residual Money，才能保证守恒。
- true subprocess response-loss：首次 partial close 已提交但 stdout 丢失，重启 retry 只返回原 receipt；
  q/cost/balance/PnL 不重复变化。
- anonymous partial、zero、overfill 和跨 market fill-ID conflict 全部 rollback；anonymous full
  operator/paper close 保留兼容。

### 给 H-006 的约束

- H-006 可替换 modeled proceeds/PnL/fee，但不得改变 fill identity 和 remaining Quantity。
- venue cash/fee 必须进入 exact Money 边界并与同一 fill receipt 原子提交。
- adapter 必须显式翻译 BUY cash request、SELL share request、matched shares 与 settlement cash，
  不能复用一个 side-dependent `amount` 字段做领域 authority。

## 2026-07-17 — H-006 verified venue-truth reconciliation

### 已验证事实

- `VenueSettlement` 只接受完整 `CONFIRMED` gross/fee/source facts；MATCHED/MINED、空
  source、负值与 fee>gross 在 repository mutation 前失败。
- venue path 的 cash authority 是 `net=gross-fee`，realized PnL 是
  `net-allocated_cost`；故意错误的 exit price 不影响账本。
- exact `SettlementReceipt` 用 integer micros 保存 gross/fee/net/PnL；legacy Money/float
  receipts 与 paper modeled path 保持兼容。
- request fingerprint 包含 market/quantity/gross/fee/status/source/version，并在
  `BEGIN IMMEDIATE` 内严格比较；同 ID 的任何 payload 变化都原子冲突。
- true subprocess response-loss 后，完整原请求可恢复 structured receipt；changed fee
  返回 exit 2，SQLite 文件与 remaining authority 均不变。

### 给 live adapter 的约束

- 只有 fee rate 而没有 actual fee 的普通 trade 不能进入 venue-truth path。
- adapter 必须提供可审计 terminal status、gross cash、actual fee、matched Quantity 与
  immutable source reference；不能在 tracker 内补公式或猜测。
- live signing/network/allowance 不属于 H-006，本阶段没有扩大执行权限。
