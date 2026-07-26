# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-26（Phase 05.5 production opportunity feed PASS）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

这是一个 L1 快照新鲜、L2 durable chain 严格健康、L3 已在生产达到 5 市场/10 token
并完成连续 24 小时原始证据验证、paper 账本可用的研发系统。生产已从 Alembic 006
精确迁移到 007；runtime 与 retention 使用两个隔离的最小权限 LOGIN。A1–A6 和旧
release-37 均保留为 diagnostic/rejected evidence。最终 exact SHA `6471d41…`
运行于 Fly release 72 / boot `9eeab4d5…`；A7 的 T0/T6/T12/T18/T24 五份累计报告
全部 PASS。最终 `[T0,T24)` 有 2,880 个 health row、14,400 个 market row、
288/288 promoter tick，minimum 5/10/10，最大 gap 38.653337 秒、最大 freshness
111.039 秒；独立 verifier 重算出同一 raw/report hash。Phase 05.4 已完成。
Phase 05.5 已把 H-009 生产化：L1 内置只读 quote worker 每 120 秒采集一次，
生产 run 2→3→4 均为 1,278/1,278，机会入口跨周期重复 HTTP 200，quote/snapshot
health 均为 pass；release 131 的 `/health.releaseId` 已绑定精确 SHA `cb0ba9c…`。
这表示 known-universe gross-before-fees 机会发现已可实战巡检，但**整套系统还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 2–9 已集成并部署 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2/L3 Fly 服务 | A1–A6 保留为拒绝证据；release 72/A7 strict 24h + final raw verify PASS | 可作为 Phase 05 Plan 06 closure 的严格证据输入 |
| M2 paper execution/accounting | Phase 2–8、H-001～H-006 已通过本地质量门 | 可用于本地模拟、账本和恢复测试 |
| M2 真实组合套利 | known-universe quote complete-run collector/scanner 已在 L1 生产持续运行 | 机会发现输入已验证可用；只输出 gross-before-fees 线索，真实下单仍不可用 |
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
- Plan 04 合并后全仓 pytest 1811 passed、1 skipped、1 xfailed；Plan-scope Ruff 通过。没有据此声称所有外部 CI/job 均已运行。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。
- `make diagnose-arb-feed-prod min_edge_bps=0` 当前已在 run 2→3→4 后重复返回
  `available-opportunities`/HTTP 200；这只证明 feed 可解析且新鲜，不证明候选扣费后
  盈利或允许下单。将来若返回 HTTP 503，仍必须解释为 feed 不可用而不是零机会。
- `make collect-neg-risk-quotes` 只读 CLOB、但写本地 SQLite；生产由 L1 worker
  自动执行同一完整 run 合同。本地命令仍只代表本地数据，不能替代生产
  `quote_feed:*` 与 `make diagnose-arb-feed-prod`。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. Phase 05.4 已闭环：production 007、隔离 credentials、release72/A7 identity、
   readiness、binding、五个 checkpoint 与 final verifier 全部 PASS。下一步把这组
   更严格的连续证据映射到 legacy Phase 05 Plan 06，并完成 dashboard smoke 与
   Phase 05 validation/summary。不能补写、覆盖或复用 A1–A6。
2. Phase 05.5/H-009 已闭环：时间戳化容量 run 1、自动 run 2→3→4、
   重复 HTTP 200、quote/snapshot health 与进程内存均通过。下一步是把机会存活时间、
   费用、滑点和多腿成交风险纳入 M2 paper 策略评估，不把 gross edge 直接当收益。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：生产 007；release72/A7 strict 24h PASS；L1 opportunity feed run 2→3→4 PASS。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper；不可真实下单。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

Phase 05.4 的 selected A7 manifest 为
`.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-MANIFEST-20260724T164301Z.json`。
五个报告与 final raw verifier 已 PASS；A1–A6 继续保持不可变拒绝状态。

Phase 05.5 已完成。下一步读取 legacy `05-ws-book-prices/05-06-PLAN.md`，将
Phase 05.4 的更严格连续证据映射到其 N=5 / book / OHLC / watchdog 条款，并完成
Dashboard 已认证浏览器验收、validation、SUMMARY 与 Phase 05 状态收口。当前浏览器
运行时没有可用浏览器，因此 Dashboard 已认证验收仍是外部阻塞；不以匿名 HTTP 200
冒充通过。除非发现实际合同不一致，不重新跑 24 小时；不执行 retention cleanup、
production chaos 或真实交易。

```bash
/gsd-resume-work --ws m1-perception
```
