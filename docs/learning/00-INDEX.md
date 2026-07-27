# Phase 1 教学文档索引

> **目的**：Phase 1 的代码是 agent 并行执行造的，我（用户）需要补回知识曲线。
> 这套文档由 Claude 写成，我读 → 提问 → Claude 修订大纲 → 我再读，循环到我能独立打开任何 `src/polyarb/` 文件不慌。
>
> **不是 API 文档**。文档目标是建立心智模型，而不是穷举字段。

要运行或巡检平台，请先用 [M1 市场感知平台使用手册](../M1-市场感知平台使用手册.md)。
本索引负责建立代码心智模型，不承担实时生产状态或操作手册职责。

## 阅读顺序

| # | 文档 | 你读完之后能答 |
|---|---|---|
| 01 | [Polymarket 数据双源](01-polymarket-data-sources.md) | Gamma vs CLOB 各自给什么、为什么需要两个、什么时候打哪个 |
| 02 | [一次 snapshot 的完整旅程](02-snapshot-pipeline.md) | 一行 `make snapshot-markets` 内部走了哪 7 步、每步产出什么数据形状 |
| 03 | [MarketSnapshot 数据形状](03-market-snapshot-shape.md) | SQLite `markets` 表 / Parquet 行 / 内存里 dict 三处的字段对应、为什么要严格对齐 |
| 04 | [Validator 三层防御](04-validator-layers.md) | Layer 1/2/4 各防什么、为什么没有 Layer 3、ghost_book 在第几层、为什么 is_valid 只看 Layer 1 |
| 05 | [Issue #180 ghost_book 实战](05-ghost-book-issue-180.md) | 这个问题的根源、为什么影响 72%、下游策略写代码时的硬约束 |
| 06 | [代码安全约束（F-1 ~ F-8）](06-security-invariants.md) | 为什么每个 `float()` 都包 try、为什么 `MAX_PAGES=1000`、F 编号代表什么 |
| 07 | [观察市场（Observation Toolkit + Translation）](07-观察市场.md) | 6 个配方分别看什么、workflow 怎么走、翻译/AST/diff 三个设计取舍、5 道自检题 |
| 08 | [生产化部署（Phase 02 L1 Production Grade）](08-生产化部署.md) | asyncio daemon 为什么要等 server.started、DEGRADED vs FAILED 的区别、PAUSED 跨重启保持的意义、/scan HMAC trust-split、soak gate 判定标准 |
| 09 | [生产化运维（Phase 02.1 fix-up）](09-生产化运维.md) | fail-soft 不等于 silent / `/control/unpause` HMAC 设计 / `/health` IETF strict vs `/healthz` Fly-friendly 的语义分离 / BUG-8 与 BUG-6 的互锁验证（Inj 4 实证） |
| 10 | [L2 跟踪（Phase 03：候选集 WS 流 → 实时信号源）](10-L2-跟踪.md) | 独立 polyarb-l2 daemon 与 polyarb-l1 的分工 / WsWatchdog 30s 业务层心跳为什么不依赖 TCP PING / POLYARB_EVENT_BUS_ENABLED B1 安全门 / fail-soft 双锚点为什么成功路径也 emit breadcrumb / hybrid catchup+bootstrap 启动期实战修法 |
| 11 | [L3 K 线（Phase 05：深度 book → OHLC 视图 → 仪表盘）](11-L3-K线.md) | L1/L2/L3 三层金字塔心智模型 / promoter 5-min cron 的 9 步 promote_run 流水线 / book_levels top-10 投影 / OHLC 1m/5m/1h 视图为什么不用 TimescaleDB / chicken-and-egg 冷启动种子集 / pitfall 5 候选集与 L3 集互不覆盖 / 5 道自检题 |
| 12-M1 | [M1 持续运行（生产巡检、告警与恢复）](12-M1-持续运行.md) | 四个生产面如何一起判定 / Polywatch 15 分钟巡检 / WAITING 与空机会的正确语义 / 五条日常命令 / 最小恢复流程 |
| 12 | [套利引擎（M2 Combinatorial Arbitrage）](12-套利引擎.md) | ArbitrageSignal / ExecutionLeg / RoutingDecision 数据契约 / SlippageCalculator 三笔成本 / _select_venue 滑点感知选场 / abort-vs-partial 原子不变式 / paper-mode vs real venue 安全面 / 五个 Makefile target 对照 |
| 13 | [仓位持久化：让每个进程看见同一本账](13-仓位持久化.md) | PositionState / repository transition / BEGIN IMMEDIATE / 三表原子投影 / operation ID 幂等 / fail-closed DB / 跨进程 run→status→close |
| 14 | [精确现金账本：价格可以近似，钱必须有唯一答案](14-精确现金账本.md) | price float 与 cash authority 的边界 / micro-pUSD / HALF_EVEN / additive SQLite migration / tagged Money receipt / 五道对手测试 |
| 15 | [成交数量与现金不是一回事：别让一个 size 同时戴两顶帽子](15-成交数量与现金不是一回事.md) | Quantity shares vs Money pUSD / BUY 与 SELL collateral / full-fill quantity equality / Phase 5 余额修复 / SDK side-dependent amount |
| 16 | [部分成交如何不重不漏](16-部分成交如何不重不漏.md) | remaining authority / residual cost basis / immutable fill identity / response-loss replay / partial-fill fail-closed 边界 |
| 17 | [Venue truth 对账](17-venue-truth-reconciliation.md) | terminal finality / actual fee vs fee rate / exact settlement receipt / fingerprint conflict / response-loss reconciliation |
| 18 | [Neg-risk 买齐套利](18-neg-risk买齐套利.md) | complete sibling set / executable asks / gross edge / fail-closed opportunity feed |
| 19 | [独立报价运行与已知市场覆盖](19-独立报价运行与已知市场覆盖.md) | known-universe snapshot / atomic quote run / dual freshness clocks / local-only operator boundary |
| 20 | [NOTIFY 门铃与游标账本](20-NOTIFY门铃与游标账本.md) | NOTIFY wake hint / durable cursor / candidate→WS→mirror 收敛 / quiet refresh 为什么必须等 book→mirror evidence |
| 21 | [L3 候选与双 Token](21-L3-候选与双Token.md) | observation seed vs promotion gate / L2 asset_id=Yes token / durable Yes+No identity / fail-closed 5→10 expansion / mutation-free dry-run |
| 22 | [L3 连续浸泡证据](22-L3连续浸泡证据.md) | truthful membership / server append-only evidence / AcceptanceConfig / manifest+五报告+raw-row hashes / event-kind fail-closed / retention privilege boundary |
| 23 | [生产机会流](23-生产机会流.md) | app 内 quote worker / 120-240-300 秒三层时钟 / durable success anchor / 503 为什么不是零机会 / 日常实战诊断 |
| 24 | [L3 连续性事务](24-L3-连续性事务.md) | prepare evidence → atomic commit → strict sample / generation-scoped evidence / 10/10/10 成功门 / 超时补偿而非 grace period |
| 25 | [市场全集不是请求成功](25-市场全集不是请求成功.md) | 2,100 行截断为何制造假套利 / keyset completion proof / exact event membership / standard 与 augmented / M1→M2 provenance 门 |
| 26 | [M1 数据层与失败事实](26-M1数据层与失败事实.md) | 为什么“旧快照仍新鲜”不能掩盖新 OOM / published truth 与 scheduler attempt 的边界 / 为什么 L2 不应阻塞 L1 recovery |
| 27 | [Structure 与 Archive：把市场成员关系从历史归档中救出来](27-Structure与Archive分层.md) | 为什么 Gamma 结构、可交易报价和历史文件不能绑成同一项作业 / Archive 失败为何不能拖垮 M2 |

## Phase 02.1 教学增量（2026-05）

Phase 02.1 (fix-up) 在 [09-生产化运维](09-生产化运维.md) 加入了三个生产化运维核心概念：
- **fail-soft 可见性**（D-01）— 不抛 exception ≠ 不留 audit trail，撤 secret 路径必须 emit log + Sentry breadcrumb
- **prod control endpoints**（D-03）— HMAC-protected `/control/unpause`，独立 ControlAuthMiddleware 与 scan.py 解耦
- **IETF strict 监控与 platform probe 在路由层面的语义分离**（D-05/D-06）— `/health` 503 是告警信号正确，`/healthz` 永远 200 让 Fly proxy 不切流量

## 每篇文档的体例

- **核心心智模型**（30 秒能讲清楚的版本）
- **代码地图**（src/polyarb/ 哪几个文件实现）
- **关键代码片段**（贴最核心的 5-15 行，配 file:line 引用）
- **会让你卡住的细节**（基于 Phase 1 实际遇到的坑）
- **自检题**（答得上 = 这一节过；答不上 = 提问）

## 如何提问

1. 读完一节，把"看不懂的句子"或"看了但不知道为什么这样设计"贴出来
2. 我会把答疑内容**追加进对应文档的"FAQ 增量"区**，而不是重写正文
3. 当某个 FAQ 出现 3 次以上 → 我会把它提升成正文的一节
4. 我们持续迭代到你能不依赖我地阅读 `src/polyarb/` 任何文件

## 实物先于理论的可选路径

如果某节读着抽象，跳到 `tests/m1-perception/` 去找对应的测试 —— 测试是带具体输入输出的代码示例。不必从头读测试，只读你正在学的那一节对应的几个 test。

## 不在本系列里的内容

- 项目章程 / 角色契约 → `CLAUDE.md`
- 决策时间线 → `.planning/JOURNAL.md`
- Phase 1 决策记录 → `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-CONTEXT.md`
- live run 实战发现 → `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-LIVE-RUN-001.md`
- 跨 phase 经验沉淀 → `.planning/threads/market-microstructure.md`

教学文档只负责**让你看懂代码**。"为什么我们当时选 A 不选 B" 看 CONTEXT.md / JOURNAL.md。
