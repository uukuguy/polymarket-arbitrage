# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-17 16:38 CST。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

## 一句话结论

这是一个已经具备较完整市场感知代码和 M2 paper 执行/精确账本基础的研发系统，
**还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | 停在 `5b90e6c` | 没有包含本轮 M2 Phase 3–8 成果 |
| `feat/m2-position-persistence` | M2 foundation 位于此 worktree；状态审计开始时相对 main 有 87 个提交，当前值以 `make status` 动态输出为准 | 可在该 worktree 本地运行和验证 |
| M1 L1 Fly 服务 | 进程可达，但 2026-07-17 实测 `/health` 返回 503，snapshot 严重过期 | 当前不可作为可信实时数据源 |
| M1 L2 Fly 服务 | 进程可达，但实测 `/health` 返回 503，WS event 严重过期 | 当前不可作为可信实时 orderbook 数据源 |
| M2 paper execution/accounting | Phase 2–8、H-001～H-006 已通过本地质量门 | 可用于本地模拟、账本和恢复测试 |
| M2 真实组合套利 | 没有真实信号发现、M1→M2 输入或真实 venue adapter | 不可实盘 |
| M3 | 未开始 | 不可用 |
| M4 | 未开始 | 不可用 |
| M5 | 有一个计划，但依赖 M1 未完成工作 | 尚不可用 |

## 已验证可用的内容

以下命令已于本次状态审计在 `feat/m2-position-persistence` worktree 实际运行：

```bash
# 合成信号的滑点/路由决策；不下单
make eval-arb mid=0.40 stake=100 legs=1

# no-op executor 的 paper 执行；状态写入独立 SQLite
make run-arb db=/tmp/m2-paper.db mid=0.40 stake=100 legs=1 \
  signal_id=my-paper-run

# 查看 paper 账户
make status-arb db=/tmp/m2-paper.db

# 用完整 venue-like 结算事实测试 exact reconciliation；仍不连接交易所
make close-arb db=/tmp/m2-paper.db market_id=cond-0 exit_price=0.99 \
  size=100 fill_id=fill-001 venue_cash=46.00 venue_fee=1.00 \
  venue_status=CONFIRMED venue_ref=trade-001
```

审计向量结果：BUY 100 @ .40 锁定 40 pUSD；gross=46、fee=1 后 net=45、
realized PnL=5、最终 balance=1005。SQLite 状态和 structured receipt 均正常。

## 不要误用

- `make run-arb` 的默认 executor 总是模拟成功，**不是 Polymarket 下单**。
- `evaluate/run` 的输入是 synthetic signal，**不是从真实市场自动发现的组合套利机会**。
- `VenueSettlement` 是已验证的对账合同；当前没有 live adapter 自动提供这些事实。
- Fly `/healthz` 返回 200 只表示平台探针可达；业务 `/health` 当前为 503，不能据此称生产健康。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. 将 feature branch 审核并集成到 `main`，否则当前交付仍只在 worktree。
2. 修复 M1 L1/L2 生产数据新鲜度，让 snapshot/orderbook 重新成为可信输入。
3. 实现真实 combinatorial opportunity discovery，并定义 M1→M2 market-state contract。
4. 用真实市场数据进行持续 paper run，记录机会、成交可行性、滑点和 drawdown。
5. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
6. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：代码成熟度较高，但生产当前 unhealthy；Phase 05 最终 soak 未完成。**
- **M2：execution/accounting foundation complete；产品能力仍 open。未来 Phase 尚未规划。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

先处理交付分叉：审查 `feat/m2-position-persistence` 相对 `main` 的提交差异（当前数量由
`make status` 动态显示），决定如何安全集成。集成之后，最高优先级是恢复 M1 L1/L2
数据健康，而不是直接接真实资金。
