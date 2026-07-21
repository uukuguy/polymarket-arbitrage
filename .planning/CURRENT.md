# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-21（首个 L3 24h soak 证据不完整）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

这是一个 L1 快照新鲜、L2 durable chain 严格健康、L3 已在生产达到 5 市场/10 token
并产生真实深度行、paper 账本可用的研发系统。Alembic 006、L1 release 127 和 L2
release 37 已上线；启动与下一次 5 分钟 tick 均为 10/10。Phase 05 的首个 24 小时
soak 已结束，但 T+6/T+12/T+18 缺失且 T+24 未观测，因此证据门 NOT-CLOSED，必须
late-diagnose 后重跑。机会 feed 生产最近仍为 HTTP 503。因此**市场感知平台已达到
soak-ready，而不是完整 production-qualified；整套系统还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 2–9 已集成并部署 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2/L3 Fly 服务 | release 37 durable chain 健康，L3 连续两 tick `10/10`，真实 book depth 已写入 | soak-ready；需 24h strict verdict 才能关闭 Phase 05 |
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
- L2 `889fab4` 的 quiet-refresh 机制产生了真实 book→mirror 证据；后来空 projection 回归已定位为本地测试继承生产 `.env`，当前链已自然恢复并重验。
- WS age 和 mirror age 仍是两条独立 clock；一次 send 或任意 WS frame 不能替代真实 book→mirror 成功。
- `markets_latest` 的 HTTP 200 空响应不能代表“真实市场为零”；L1 空 rows 必须拒绝远程覆盖，L2 空投影必须冻结 candidate/LKG/游标并重试。
- 最终全仓 pytest 1433 passed、1 skipped、1 xfailed；touched-file Ruff 通过。没有据此声称所有外部 CI/job 均已运行。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。
- `make diagnose-arb-feed-prod min_edge_bps=0` 最近对应 HTTP 503 的不可用语义；在 H-008 打通 chain-truth 前，不得把它说成“当前无正 edge”。只有 `available-zero`/exit=0 且 payload 可解析的 0 条才是合法零机会；诊断本身不修复 producer cadence/SLA mismatch。
- `make collect-neg-risk-quotes` 只读 CLOB、但写本地 SQLite；`make scan-arb-quotes` 只读完整 quote run。它们未部署、未调度，且 local contract 通过不等于生产 capacity、freshness 或 readiness。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. Phase 05 的生产 prerequisite 已通过：Alembic 006、L1/L2 deploy、严格 5 市场/10 token 和真实 book-level 写入均有 exact-instance 证据。首个 24h soak 因缺少中间 checkpoint 而 NOT-CLOSED；下一步先做 labelled late diagnostic，再启动新窗口，并每约 6 小时验证 active count、五市场 book/OHLC coverage 与 watchdog；无需放宽 recipe。
2. H-009 本地实现保持 pending：先取得**生产部署/调度的单独授权**，再进行**时间戳化只读容量观察**；之后仍须积累重复 complete run、可解析 exit=0 契约和不可变证据，才可评估 producer cadence/SLA。没有这些证据，HTTP 503 绝不等于零机会。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：L1/L2/L3 的 T+0 生产链为绿；首个正式 24h 窗口已结束，但 T+6/T+12/T+18 缺失且 T+24 未采，证据门 NOT-CLOSED，必须 late-diagnose 后重跑 soak。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

Production Alembic 已是 006，L1 release 127 与 L2 release 37 已部署。exact L2 instance
`01KXZJKY9SKEJAY2DD8MMPNB2E` 的第二次真实 promoter tick 在 `11:00:20Z` 保持
`+0 -0 markets=5 tokens=10`；`11:00:50Z` health 仍为 10/10，book-write age 59.3s，
数据库已有 280 条 release 后真实 book rows。生产发现的 book 数组方向、时序重复行和
No-side 反馈污染已在 `7ccd2da` / `57d3fc0` / `9451b4b` 修复，recipe 阈值未改变。
`2026-07-20T12:49:25Z` 的候选 T+0 在 exact identity 与 active `10/10` 均通过时，
仍因 book-level write age `207.2s` 超过严格 `<120s` gate 被拒绝；没有拼接更早的绿样本。
随后在 promoter `+2/-2` churn 后重新解析权威五市场/十 token 映射，并于
`2026-07-20T13:30:55Z` 从同一 exact instance 的新样本建立正式 T+0：HTTP 200，
active `10/10`、promote age `20.0s`、book-write age `19.9s`，其余主链检查全绿。
T+24 `2026-07-21T13:30:55Z` 已过去，但 T+6/T+12/T+18 均未采集，handoff 时
T+24 也尚未观测。缺失点不允许回填，因此本窗口不能 PASS。下一步先采一份明确标为
late 的 exact-instance/完整区间 SQL/watchdog 诊断，随后建立新的 24h re-soak；本次没有
真实交易或生产变更。

```bash
/gsd-execute-phase 05 --ws m1-perception
```
