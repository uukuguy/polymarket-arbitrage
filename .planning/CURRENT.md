# 当前项目状态

> 唯一当前状态入口。最后核验：2026-07-24（Phase 05.4 Plan 05 A5 T0 PASS，24 小时窗口进行中）。
> `JOURNAL.md` 是追加式历史；其中旧 `[NEXT]` 均不代表当前任务。

稳定的使用流程、健康语义和命令安全分级见
[M1 市场感知平台使用手册](../docs/M1-市场感知平台使用手册.md)；本文继续只维护动态状态。

## 一句话结论

这是一个 L1 快照新鲜、L2 durable chain 严格健康、L3 已在生产达到 5 市场/10 token
并持续产生原始证据、paper 账本可用的研发系统。生产已从 Alembic 006 精确迁移到
007；runtime 与 retention 使用两个隔离的最小权限 LOGIN。当前选中 L2 为 source
`9f2c935…` / release 66 / boot `be240060…`，GitHub、Fly、DB 身份一致。旧 release-37
窗口和 A1–A4 均为 diagnostic/rejected evidence，不能升级为 PASS。A5 的不可变
manifest 已在 T0 前单次绑定，T0 `2026-07-24T12:43:29.274117Z` 及其 exact-grid
样本/报告 PASS；T+6/T+12/T+18/T+24 仍受真实墙钟 not-before 约束，最终 verifier
尚未运行。机会 feed 生产最近仍为 HTTP 503。因此**市场感知平台正处于严格 24 小时
soak，而不是完整 production-qualified；整套系统还不是可以投入真实资金运行的套利产品**。

## 交付状态

| 范围 | 当前结论 | 现在能否使用 |
|---|---|---|
| 主分支 `main` | M2 Phase 2–9 已集成并部署 | 当前交付主线 |
| M1 L1 Fly 服务 | 39 天 stale 根因已修复；snapshot 恢复到分钟级，Supabase pass | 可作为 M2 机会发现输入 |
| M1 L2/L3 Fly 服务 | release 66 exact identity，A5 T0 与早期审计 PASS，连续 append-only ledger 正在积累 | 严格 24h soak 进行中；必须等待四个 not-before checkpoint 与 final verify |
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
- Plan 04 合并后全仓 pytest 1811 passed、1 skipped、1 xfailed；Plan-scope Ruff 通过。没有据此声称所有外部 CI/job 均已运行。
- Phase 2–8 complete 表示 foundation plans 完成，不表示 M2 产品使命完成。
- `make diagnose-arb-feed-prod min_edge_bps=0` 最近对应 HTTP 503 的不可用语义；在 H-008 打通 chain-truth 前，不得把它说成“当前无正 edge”。只有 `available-zero`/exit=0 且 payload 可解析的 0 条才是合法零机会；诊断本身不修复 producer cadence/SLA mismatch。
- `make collect-neg-risk-quotes` 只读 CLOB、但写本地 SQLite；`make scan-arb-quotes` 只读完整 quote run。它们未部署、未调度，且 local contract 通过不等于生产 capacity、freshness 或 readiness。

## 距离可实际 paper/live 使用还缺什么

按依赖顺序：

1. Phase 05.4 Plan 05 的 migration、隔离 credentials、exact-SHA deploy、
   readiness、A5 manifest binding 和 T0 已完成。现在必须在 manifest 声明的
   T+6/T+12/T+18/T+24 边界分别生成一次累计报告，再由 final verifier 重查 DB
   binding、五文件、raw-row digest、共同 soak hash 和全部严格 invariant。不能提前、
   补写、覆盖或复用旧窗口。
2. H-009 本地实现保持 pending：先取得**生产部署/调度的单独授权**，再进行**时间戳化只读容量观察**；之后仍须积累重复 complete run、可解析 exit=0 契约和不可变证据，才可评估 producer cadence/SLA。没有这些证据，HTTP 503 绝不等于零机会。
3. 实现经过明确授权的 Polymarket order/fill adapter、认证、allowance、限额与 kill switch。
4. 通过 paper→小额 live 质量门后，才讨论真实资金运行。

## Workstream 摘要

- **M1：生产 007 / release 66 / A5 T0 为绿；旧窗口均非 strict evidence；当前等待 A5 四个 not-before checkpoint 与 final verify。**
- **M2：execution/accounting + neg-risk buy-all discovery 可用于真实数据监控/paper。**
- **M3/M4：未开始。**
- **M5：计划存在，但当前不应先于 M1 恢复。**

## 当前下一步

Selected A5 manifest:
`.planning/workstreams/m1-perception/phases/05.4-continuous-l3-soak-evidence/05.4-SOAK-MANIFEST-20260724T124329Z.json`

- T0: `2026-07-24T12:43:29.274117Z` — PASS
- T+6: `2026-07-24T18:43:29.274117Z` — pending not-before
- T+12: `2026-07-25T00:43:29.274117Z` — pending
- T+18: `2026-07-25T06:43:29.274117Z` — pending
- T+24: `2026-07-25T12:43:29.274117Z` — pending

manifest-derived T+6 runner 已在持久会话中等待。它会先验证 manifest/T0 文件 SHA
和 release 66 的 Fly machine/instance/digest/source identity，再使用直连的最小权限
runtime credential 生成 manifest 声明的唯一 `...-T6.json`。任何不匹配都 fail
closed。T+6 PASS 后按同一模式排 T+12；T+24 后运行 `make l3-soak-verify`，仅在五
文件与 raw rows 全部重验 PASS 时创建 SUMMARY、签署 validation、勾选 ROADMAP。
不执行 retention cleanup、production chaos、H-009 或真实交易。

```bash
/gsd-resume-work --ws m1-perception
```
