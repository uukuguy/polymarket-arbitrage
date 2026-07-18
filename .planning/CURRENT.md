# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-19（H-009 本地实现、租约恢复与文档合同）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

这是一个已经恢复新鲜 M1 生产快照、具备 paper 精确账本的研发系统；
M2 neg-risk 机会 endpoint 最近只读实测返回 HTTP 503；H-009 只新增了本地 known-universe quote collector/complete-run scanner 与离线合同，故**快照/健康监控与 paper 账本可用，但机会 feed 生产仍是有条件/未 ready，还不是可以投入真实资金运行的套利产品**。H-008 的只读诊断只会区分响应状态，H-009 的本地代码也**不会修复或证明 production producer cadence/SLA，更不会把 feed 宣布为 ready**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 2–9 已集成并部署 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2 Fly 服务 | 40 天 stale 主链已恢复；candidate=5、WS=5、真实 book→Supabase mirror 与同实例 ≥180s 基线已通过；quiet-refresh 自然 60s 边沿尚未触发，L3 仍为 0/10 | 主链已恢复，但 Phase 05.1/05 最终生产门未关闭 |
| M2 paper execution/accounting | Phase 2–8、H-001～H-006 已通过本地质量门 | 可用于本地模拟、账本和恢复测试 |
| M2 真实组合套利 | 本地已实现 known-universe quote complete-run collector/scanner；最近生产请求 HTTP 503 | 生产当前有条件/未 ready；H-009 仍 pending，需单独授权部署/调度和时间戳化只读容量观察 |
| M3 | 未开始 | 不可用 |
| M4 | 未开始 | 不可用 |
| M5 | 有一个计划，但依赖 M1 未完成工作 | 尚不可用 |

## 已验证可用的内容

以下命令已在 `main` 实际验证：

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

# 诊断性查询；只有 available-zero/available-opportunities 的 exit=0 才能解读机会数，HTTP 503 不是零机会
make diagnose-arb-feed-prod min_edge_bps=0

# 仅离线 fixture/Make dry-run 的 H-009 合同，不访问生产
make eval-local profile=opportunity-feed-cadence-sla
```

审计向量结果：BUY 100 @ .40 锁定 40 pUSD；gross=46、fee=1 后 net=45、
realized PnL=5、最终 balance=1005。SQLite 状态和 structured receipt 均正常。

## 不要误用

- `make run-arb` 的默认 executor 总是模拟成功，**不是 Polymarket 下单**。
- `evaluate/run` 仍是 synthetic executor；真实市场发现请用 `scan-arb-live`。
- `VenueSettlement` 是已验证的对账合同；当前没有 live adapter 自动提供这些事实。
- L1 业务健康已恢复；R2 archive 仍为 warn，不影响本地 SQLite→M2 feed。
- L2 `889fab4` 的主链恢复证据已通过，但不能把持续活跃流量当作 quiet-refresh 证据；必须等自然 60 秒静默后的 receive→mirror 链。
- 全仓 CI 仍被历史 repo-wide Ruff 基线阻断；本次相关 tests/targeted Ruff 与 Fly deploy 通过，不等于全仓 CI 绿色。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。
- `make diagnose-arb-feed-prod min_edge_bps=0` 最近对应 HTTP 503 的不可用语义；在 H-008 打通 chain-truth 前，不得把它说成“当前无正 edge”。只有 `available-zero`/exit=0 且 payload 可解析的 0 条才是合法零机会；诊断本身不修复 producer cadence/SLA mismatch。
- `make collect-neg-risk-quotes` 只读 CLOB、但写本地 SQLite；`make scan-arb-quotes` 只读完整 quote run。它们未部署、未调度，且 local contract 通过不等于生产 capacity、freshness 或 readiness。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. 在同一 L2 instance 上完成自然 quiet-refresh 的 receive→mirror 证据；若 instance 变化，先重建 ≥180 秒主链基线。
2. H-009 本地实现保持 pending：先取得**生产部署/调度的单独授权**，再进行**时间戳化只读容量观察**；之后仍须积累重复 complete run、可解析 exit=0 契约和不可变证据，才可评估 producer cadence/SLA。没有这些证据，HTTP 503 绝不等于零机会。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：L1 fresh；L2 主链已恢复，Phase 05.1 quiet-edge 与 Phase 05 最终 soak 尚未完成。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

继续监控已核实的 L2 instance `01KXSMS80B5AX2FGT5EPRC6V82` 的自然 60 秒 quiet edge，要求 `sending → real book/mirror write → evidenced` 完整证据。最新 authenticated 11m03s 窗口最大 `ws_age=2.6s`，quiet 日志 `0/0/0`，仍未触发。Phase 05 Plan 06 的 24h soak 在 Phase 05.1 关闭前不得启动。

```bash
/gsd-resume-work --ws m1-perception
```
