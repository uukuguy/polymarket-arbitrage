# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-20（Phase 05.1 当前链恢复并关闭）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

这是一个 L1 快照新鲜、L2 durable chain 当前严格健康、paper 账本可用的研发系统。
Phase 05.1 已用自然 quiet-refresh、空投影根因修复、自然恢复和 258 秒重验关闭；当前
snapshot 574 有 1942 个市场，L2 有 3 个候选且 strict `/health` 为 HTTP 200。但 L3 仍为
`0/10`，机会 feed 生产仍为 HTTP 503，所以**感知底座部分可用，L3/机会生产未 ready，
还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 2–9 已集成并部署 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2 Fly 服务 | snapshot 574 后自然恢复 3 个候选；258s 窗口 10/10 HTTP 200，游标 0、listener listening，同实例未变 | L2 durable chain 当前可用；L3 `0/10` 已定位为 candidate seed + token-map schema 双断点 |
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
- 全仓 pytest 1413 collected tests 已通过；touched-file Ruff 通过。没有据此声称所有外部 CI/job 均已运行。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。
- `make diagnose-arb-feed-prod min_edge_bps=0` 最近对应 HTTP 503 的不可用语义；在 H-008 打通 chain-truth 前，不得把它说成“当前无正 edge”。只有 `available-zero`/exit=0 且 payload 可解析的 0 条才是合法零机会；诊断本身不修复 producer cadence/SLA mismatch。
- `make collect-neg-risk-quotes` 只读 CLOB、但写本地 SQLite；`make scan-arb-quotes` 只读完整 quote run。它们未部署、未调度，且 local contract 通过不等于生产 capacity、freshness 或 readiness。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. 审阅并实施已批准的 L3 prerequisite 设计：补齐 `markets_latest.no_token_id` / 正确 yes-token lookup，并给 L2 增加上限 100 的中间价 seed 候选。只读 CLOB 样本中 100/100 book 完整、86/100 在原阈值下合格；无需放宽 L3 recipe。只有达到锁定的 5 市场/10 token 后才能开始 24h soak。生产迁移/部署仍需单独授权。
2. H-009 本地实现保持 pending：先取得**生产部署/调度的单独授权**，再进行**时间戳化只读容量观察**；之后仍须积累重复 complete run、可解析 exit=0 契约和不可变证据，才可评估 producer cadence/SLA。没有这些证据，HTTP 503 绝不等于零机会。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：L1 fresh；L2 durable chain 当前 strict HTTP 200，Phase 05.1 complete；L3 `0/10` 阻塞 Phase 05 closure。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

Phase 05 Plan 06 的只读诊断已完成：最近 1h 只有 3 个 near-end TOB asset，两个
spread≈0.998、一个不完整，L3 recipe 命中 0；同时 production `markets_latest` 只有
`yes_token_id`，而 promoter 错查不存在的 `asset_id/no_token_id`。推荐设计已获继续执行授权并写入
`docs/superpowers/specs/2026-07-20-m1-l3-prerequisite-repair-design.md`，且书面规格已获批准。
Phase 05.3 四个本地 TDD plan 已注册，当前从 05.3-01 token projection 开始。24h soak 必须从
`l3:active_count=10` 开始，未经单独授权不迁移/部署，不改变 N=5 阈值。

```bash
/gsd-execute-phase 05.3 --ws m1-perception
```
