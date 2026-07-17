# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-17 17:40 CST。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

## 一句话结论

这是一个已经恢复新鲜 M1 生产快照、具备 M2 可成交 neg-risk 机会发现和
paper 精确账本的研发系统；**可用于真实数据监控/paper 决策，但还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 3–8 已部署；Phase 9 本地实现正等待发布验证 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2 Fly 服务 | 进程可达，但实测 `/health` 返回 503，WS event 严重过期 | 当前不可作为可信实时 orderbook 数据源 |
| M2 paper execution/accounting | Phase 2–8、H-001～H-006 已通过本地质量门 | 可用于本地模拟、账本和恢复测试 |
| M2 真实组合套利 | Phase 9 本地提供 fresh executable-ask neg-risk buy-all feed | 测试已过；线上待发布验证 |
| M3 | 未开始 | 不可用 |
| M4 | 未开始 | 不可用 |
| M5 | 有一个计划，但依赖 M1 未完成工作 | 尚不可用 |

## 已验证可用的内容

以下本地命令已在 `main` 实际验证；最后一个生产命令将在本次发布后现场验证：

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

# 查询生产 M1 的真实 neg-risk 可成交 ask 机会（gross-before-fees）
make scan-arb-live min_edge_bps=0
```

审计向量结果：BUY 100 @ .40 锁定 40 pUSD；gross=46、fee=1 后 net=45、
realized PnL=5、最终 balance=1005。SQLite 状态和 structured receipt 均正常。

## 不要误用

- `make run-arb` 的默认 executor 总是模拟成功，**不是 Polymarket 下单**。
- `evaluate/run` 仍是 synthetic executor；真实市场发现请用 `scan-arb-live`。
- `VenueSettlement` 是已验证的对账合同；当前没有 live adapter 自动提供这些事实。
- L1 业务健康已恢复；R2 archive 仍为 warn，不影响本地 SQLite→M2 feed。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. 修复 L2 WS/orderbook 生产新鲜度，把候选的盘口持续性证据接入 paper 评估。
2. 用真实机会 feed 持续记录机会频率、存活时间、费用后 edge 与失败腿风险。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：L1 fresh snapshot 已恢复；L2 仍 stale，Phase 05 最终 soak 未完成。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

部署并现场验证 Phase 9 `make scan-arb-live`；随后恢复 M1 L2 新鲜度并积累真实 paper evidence。
