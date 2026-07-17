# Venue truth 对账：价格是模型，结算现金才是账本事实

> Phase 8 / H-006。读完后你应该能解释：为什么 `fee_rate_bps` 不能直接记成
> 实际手续费，以及为什么同一个 fill ID 的金额变了必须报冲突而不是覆盖。

## 30 秒心智模型

H-005 回答“成交了多少、还能卖多少”；H-006 回答“交易所最终到底返了多少钱”。

```text
immutable fill identity + terminal venue facts
      quantity=30, gross=13.80, fee=.30, CONFIRMED
                         │
                         ▼
allocated cost = 12.00
net cash       = 13.50   (gross - fee)
realized PnL   =  1.50   (net - allocated cost)
```

只要 venue facts 完整且终态，它们就覆盖 `exit_price` 模型。即使调用者传入故意错误的
`.99`，账本仍只记 gross=13.80、fee=.30、net=13.50、PnL=1.50。

## 代码地图

| 文件 | 责任 |
|---|---|
| `src/polyarb/routing/position_tracker.py:142` | `VenueSettlement` 只接收完整 `CONFIRMED` 事实 |
| `src/polyarb/routing/position_tracker.py:474` | 构造 fingerprint，按实际净现金迁移仓位 |
| `src/polyarb/routing/position_repository.py:162` | `SettlementReceipt` 保存 exact Money 结果 |
| `src/polyarb/routing/position_repository.py:240` | tagged JSON codec，不把 cash 降级成 float |
| `src/polyarb/cli_arbitrage.py:388` | all-or-none venue 参数与跨进程重放入口 |
| `Makefile:1193` | `make close-arb` 的统一 operator 入口 |

## finality 与 fee-rate 是两种不同的事实

`MATCHED` / `MINED` 表示流程仍可能变化；只有 `CONFIRMED` 才能成为这版账本的终态输入。
同样，`fee_rate_bps` 是费率规则，不是这笔交易实际扣掉的现金。数量、价格、费率公式可能
用于估算，但不能被标成 venue-confirmed fee。

因此边界要求四个字段同时存在：gross cash、actual fee、`CONFIRMED`、source reference。
缺任何一个都 fail closed，不创建 receipt，也不改变仓位。

## 为什么 exit price 不再决定现金

核心迁移在 `position_tracker.py:530-555`：

```python
allocated_cost = Money.allocate(...)
if fill.settlement is None:
    pnl_money = Money.pnl_for(...)       # paper/model path
    cash_returned = allocated_cost + pnl_money
else:
    cash_returned = fill.settlement.net_cash
    pnl_money = cash_returned - allocated_cost
state.balance_money += cash_returned
state.realized_pnl_money += pnl_money
```

两条路径故意并存：paper path 继续用价格模型；venue path 只用确切现金。这样 live truth
不会污染 paper simulation，paper 估算也不会伪装成实盘结算。

## fingerprint 为什么比“ID 存在就返回”更严格

单有 fill ID 只能阻止第二次插入，不能证明重试内容相同。第一次提交 fee=.30，第二次若带
fee=.31，而系统直接返回旧 receipt，调用者会误以为新事实被接受。

fingerprint 固定包含 market、quantity micros、gross/fee micros、status、source_ref 和版本。
repository 在 SQLite `BEGIN IMMEDIATE` 内比较：

- ID 与 fingerprint 都相同：重放旧 receipt；
- 同 ID 但任何字段不同：`operation identity conflict`；
- legacy empty 与新 non-empty 也冲突，绝不静默升级旧事实。

比较发生在写锁内，所以两个并发进程不能同时“都没看见 receipt”后各写一次。

## CLI 为什么重放仍要求完整原始参数

venue retry 不能只预读 receipt 后直接打印。它必须重新构造同一 `Fill + VenueSettlement`，
再次调用 tracker，让 repository 比较 fingerprint。于是 response loss 后相同请求可恢复，而
改过 fee/quantity/source 的请求会明确失败。

统一入口：

```bash
make close-arb market_id=cond-0 exit_price=.99 size=30 \
  fill_id=fill-001 venue_cash=13.80 venue_fee=.30 \
  venue_status=CONFIRMED venue_ref=trade-001
```

## 设计取舍

1. **不推导 actual fee**：普通 trade 只有 fee rate 时继续停在 modeled path，等待 adapter 提供
   可审计的最终现金事实。
2. **receipt 同时保存 gross/fee/net/PnL**：虽然后两者可计算，持久化完整结果让恢复与审计不必
   重新依赖后来可能变化的公式。
3. **不接 live signing**：本 phase 只锁定对账合同和本地恢复证据；订单认证、网络与 allowance
   不在 H-006 权限范围。
4. **exit_price 仍保留在 Fill**：兼容 paper path 和展示，但它不是 venue cash fingerprint 的权威。

## 对手测试

1. BUY 100 @ .40，venue 回 gross=13.80、fee=.30、quantity=30，但 exit_price=.99。余额与
   PnL 应是多少？为什么不能使用 `.99-.40`？
2. trade 状态是 `MINED` 且现金字段完整，能否入账？如果之后 reorg，当前选择保护了什么？
3. fill-1 已提交 fee=.30，响应丢失；重试 fee=.31。为什么不能返回旧 receipt 当作成功？
4. 两个 SQLite writer 同时提交同一 fill ID、不同 gross，比较必须发生在事务的哪个位置？
5. 普通 authenticated trade 只给 `fee_rate_bps`。还缺哪类事实才能进入 venue-truth path？

## FAQ 增量

暂无。后续答疑只追加在这里；同一问题出现三次后再提升进正文。
