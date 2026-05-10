---
slug: market-observation-architecture
title: Production-Grade Market Observation Architecture (3-Layer Pyramid)
status: drafting
created: 2026-05-10
updated: 2026-05-10
---

# Thread: Production-Grade Market Observation Architecture

> 实盘级市场观察工作流的架构设计。**这是 m1-perception 后续所有工作的底座**，
> 直接决定 Phase 01.2+ 长什么样、要不要 WebSocket、K 线从哪来、定向跟踪怎么定。
>
> 跨能力线永久存活。任何观察 / 抓取 / 数据落库相关讨论应预读本文。

---

## 0. 起点 — 用户洞察（2026-05-10）

### 0.1 全量快照的本质（早些时候的洞察）

> "全量快照是跨越下载时长（8 分钟以上）的模糊影像。如果全量市场是一个拖开
> 时间轴的模糊影像，应该只能参考作用，并非主角。定向快照的设计应该是生产
> 上线工作流的级别，应该有前因后果，不是随机选这种的。然后锁定单市场，
> 又是一套快照追踪设计，然后 K 线。"

### 0.2 项目定位（同次会话补充）

> "目前是框架启动的初期，不求大而全，而是求稳定推进保证生产级水准，工程
> 可落地。我们目前的开发目标是具备生产级实盘可观测能力，为接下来的分析
> 能力打下坚实的基础。我们需要建立一个稳定高效反应迅速的市场观察分析
> 平台框架，为实盘进入市场做好准备。"

### 0.3 直接结论

1. 全量快照只能用于"日级市场画像"，不能做策略主角
2. 真正的实盘观察需要**三层金字塔**：日级全量（候选池） → 定向（主题/分组） → 单市场（K 线）
3. 之前的 6 个 scan recipes 是"切面"不是"全景"，定位需要重新归类
4. 三层之间必须有**触发关系**（前因后果），不是随机选市场
5. **平台框架，不是工具集合** — 三层之间共享统一的数据抽象、时序模型、事件机制
6. **生产级 = 可长跑 7×24** — 单次跑通不算数，挂了能恢复、状态可观测才算
7. **稳定推进 = 每层做到生产级再进下一层**，不一次性把三层都铺开

---

## 1. 三层金字塔总览

```
                  Layer 1: 日级全量快照
              （慢、宽、模糊 — 8 分钟拖尾）
              Polymarket Gamma + CLOB 全量
                       │
        圈定候选池（按 tag / liquidity / event / 漂移）
                       ↓
                  Layer 2: 定向跟踪
            （中频、定向 — 分钟级、有明确 scope）
              候选池子集，每 N 分钟刷新
                       │
            筛出"该锁定的单市场"（待定义触发条件）
                       ↓
                  Layer 3: 单市场 K 线
            （高频、单点 — WebSocket 实时 + OHLC 聚合）
                  策略 / 套利信号生产
```

| 层 | 频率 | 数据宽度 | 数据深度 | 用途 | **生产级判定标准** |
|---|---|---|---|---|---|
| L1 | 日级 ~1-2 次 | 12k+ 全市场 | 当下 mid + 流动性 + tag | 候选池圈定 / 市场画像 | 7 天连跑无人值守，失败自动告警，2 次成功 snapshot 可对比，磁盘不爆 |
| L2 | 分钟级 ~1-5min | 候选子集 ~10-100 个 | 含订单簿 top-of-book + 成交流 | 信号识别 / 进场触发 | 7×24 daemon，断网自愈，时序数据查询响应 < 1s |
| L3 | 实时 / 秒级 | 锁定 1-5 个 | 完整深度 + tick 级历史 + OHLC | 策略执行 / 风险监控 | WS 断连重连 + 历史回填，OHLC 与官方 / 链上交叉一致 |

**重要纪律**：**当前层不达到生产级判定标准，禁止开下一层的工作**。
否则会出现"L1 还在断断续续，L2 daemon 已经在跑残缺数据"这种灾难。

---

## 1.5 平台框架抽象层（"框架 ≠ 工具集合"）

三层金字塔不是三套独立工具，是**同一个观察平台的三种采集 cadence**。
为了避免"造一堆孤岛"，三层必须对齐三个共享抽象：

### A. 统一的市场状态模型（Market State）

任意时刻、任意层级，对一个市场的描述都使用同一套字段（schema 单一来源）：

- 标识：`market_id` / `slug` / `event_id`
- 价格：`mid` / `yes_ask` / `yes_bid` / `no_ask` / `no_bid` / `spread`
- 流动性：`liquidity_usd` / `top_of_book_size` / `book_depth_USD@1c`
- 业务：`question` / `end_time` / `status` / `tag_labels`
- 元：`source` (gamma|clob_rest|clob_ws) / `fetched_at_ms` / `quality_flags`

**意义**：L1 写入和 L2 写入用同一个 dataclass，L3 实时流也聚合到同一个 dataclass。
查询代码不需要知道数据来自哪层。

### B. 时序模型（Time-Series）

任意层级写入的市场状态都进入**统一的时序后端**（候选：DuckDB / TimescaleDB / 自建 parquet+SQLite hybrid，待 §2 调研）：

- 时间维度：`fetched_at_ms`（采集时刻）
- 主键：`(market_id, fetched_at_ms, source)`
- 写入是**append-only**（绝不覆盖），保留历史完整
- 查询接口：`get_market_history(market_id, from_ts, to_ts, sources=[...])` 一行调用穿透三层

**意义**：L1 / L2 / L3 数据天然可拼接看一个市场的完整时序，不用胶水代码。

### C. 事件驱动（Event Bus）

层级之间的"触发"关系通过**事件**而非"硬编码调用链"：

```
L1 完成 → emit("snapshot.complete", snapshot_id)
        ↓
        L2 daemon 订阅事件，决定是否更新自己的跟踪集（candidate refresh）

L2 检测异动 → emit("market.anomaly", market_id, severity)
              ↓
              L3 daemon 订阅事件，决定是否升级到 WS 实时跟踪

L3 信号 → emit("signal.detected", market_id, strategy_hint)
        ↓
        策略层（M2/M4）订阅事件
```

**意义**：层级解耦，新增层级 / 策略不修改已有代码，只订阅新事件。

**实现取舍**（待 §2 调研定）：
- 简单方案：SQLite 表 + LISTEN/NOTIFY 模拟（够用、零依赖）
- 中等方案：Redis pub/sub（够轻、社区验证）
- 重方案：Kafka / NATS（过度设计，框架启动期不上）

### 框架启动期的最小可行版本（MVP 抽象）

不需要一开始就把 A/B/C 全造出来。MVP 推进顺序：

1. **先做 A**（统一市场状态 dataclass）— 是 B / C 的前提
2. **B 的最小版本**（DuckDB over parquet glob，read-only） — 不用换存储后端，先把查询接口封起来
3. **C 暂用文件标记**（L1 完成写 `.state/last-snapshot-complete.json`，L2 daemon 轮询） — 等 L2 真起来再决定要不要上事件总线

骨架阶段不锁死实现，只锁死"这三个抽象必须存在"。

下面这些是骨架阶段**还不知道答案**的，需要做实质调研后才能给出架构方案。
每条带【调研方式】和【为什么必须先答这个】。

### 2.1 当前快照的"时间一致性"实情

**问题**：8 分钟翻页期间，第一页的 mid 价 vs 最后一页的 mid 价，能拖出多大的真实信号失真？

- 第 1 页（北美高峰开始时）的 markets 价格 vs 第 N 页（8 分钟后）的价格
- 同一个市场如果在第 1 页和第 N 页都被抓到（因为 Gamma 翻页可能 overlap），两次价差是多少？
- 我们 stamp 的 `fetched_at_ms` 是单一时刻还是分页时刻？看代码确认。

**调研方式**：
- 读 `src/polyarb/snapshot/orchestrator.py` + `clients/gamma_client.py` 看 `fetched_at_ms` 实际怎么 stamp
- 拉一次真实 snapshot，导出 parquet，按市场 group_by + 看 fetched_at_ms 分布
- 跑一次重复 snapshot（10 分钟内连跑 2 次），对同一个市场比 mid 价差

**为什么先答**：决定 L1 数据**能不能拿来做"即时套利信号"**还是**只能做"画像 + 候选池"**。如果同市场 8 分钟漂移 > 0.005，做套利是骗自己。

### 2.2 Polymarket WebSocket 的真实能力边界

**问题**：
- `/book` 通道是 per-token 订阅还是支持批量？一个连接最多订几个？
- `/prices` 通道粒度是什么？事件级还是 token 级？
- 是否存在"某个 event 下所有 markets 一次订完"的快捷方式？
- 速率限制 / 连接数 / 重连策略？
- 历史 trades 有 REST 接口吗（深度多深）？还是只能从 WS 流自己累积？

**调研方式**：
- Context7 查 `polymarket clob websocket` 官方 docs
- jina/web 查现成 OSS 项目的 WS 代码（`3th-party/polymarket-kalshi-weather-bot/` 优先）
- 读 `3th-party/clawfirm/` 的相关模块（虽然套利层是空的，连接层可能有）

**为什么先答**：决定 L2 / L3 的实现路径。如果 WS 必须 per-token 订阅且连接数有限，"动态切换跟踪集"就是核心架构问题；如果有 event 级订阅，整个工作流大幅简化。

### 2.3 K 线数据源

**问题**：
- Polymarket 有原生 OHLC API 吗？还是只能从 trades 历史聚合？
- 历史 trades 能回溯多远？（小时？天？月？）
- 从 WS 流实时聚合 OHLC，需要保证什么数据完整性（断连怎么办？）？
- Subgraph (TheGraph) 上的 trades 历史能不能补全？延迟多少？

**调研方式**：
- DeepWiki 查 polymarket subgraph schema
- jina 搜 "polymarket trades history api" / "polymarket ohlc"
- IMDEA 论文 86M 笔交易是怎么采的（这个量级肯定不是从 WS 流自己存的）

**为什么先答**：L3 直接依赖。没有可靠 K 线源就没有策略 backtest。

### 2.4 业内成熟做法借鉴

**问题**：实盘做 Polymarket 套利的人怎么解决全量 vs 定向取舍？

**调研方式**：
- `3th-party/polymarket-kalshi-weather-bot/` — 完整 Python 实现，看它怎么做 cadence
- `docs/research/polymarket-oss-landscape-2026-04.md` — 35+ OSS 项目调研报告，挑 top star 看架构
- jina 搜 "polymarket production trading architecture" / "kalshi market making"
- IMDEA 论文方法论部分（86M 交易、$40M 套利）

**为什么先答**：避免重复造轮子。已有的 production 实现一定踩过我们将要踩的坑。

### 2.5 生产级长跑要求（"7×24 跑得起来"）

**问题**：当前 `make snapshot-markets` 是 ad-hoc 命令，离生产级长跑差什么？

具体子问题：
- **调度**：cron / systemd timer / supervisord / Python 应用内调度器（apscheduler）哪个匹配本项目规模？
- **失败告警**：snapshot 失败、translate 失败、disk full、API quota exhausted —— 怎么知道？email / 桌面通知 / 文件落标记？
- **日志**：当前 loguru 输出到 stderr，长跑下日志去哪？要不要按天 rotate？要不要分级别（INFO / ERROR 分文件）？
- **状态健康看板**：`make snapshot-status` 是手动跑的，长跑下需要"自动健康检查 + 异常立即可见"。当前缺什么？
- **磁盘管理**：`make snapshots-purge` 已有，但没人定时调用。是否要 cron 进调度？
- **重启恢复**：daemon 崩了重启，怎么知道上次跑到哪？是否要 checkpoint？
- **配置热更新**：watchlist.yaml 改了不重启 daemon 是否生效？

**调研方式**：
- 读 `3th-party/polymarket-kalshi-weather-bot/` 部署部分（必看 — 已经在生产跑过的项目）
- 评估当前代码距 daemon 化还差什么（asyncio loop 是否能直接长跑？资源回收？）

**为什么先答**：生产级是本阶段的核心目标。如果 L1 都不能 7×24 跑，做 L2/L3 是空中楼阁。

---

## 3. 现有工具栈在三层架构里的位置（重新归类）

> 这是骨架阶段的初步映射，调研后会调整。

| 现有工具 | 当前定位 | 三层归类 | 生产级缺口 |
|---|---|---|---|
| `make snapshot-markets` (subset / full) | 全市场快照 | **L1** | 缺：调度（无 cron）/ 失败告警 / 日志 rotate / 磁盘配额。明确定位为日级 |
| `make overview` | 一屏总览 | **L1** | 缺：自动健康检查（当前要人手跑）；可考虑 web dashboard |
| `make scan-*` 6 个 recipes | 切面查询 | **L1** 候选筛选 | 缺：scan 结果未持久化为"候选池"对象，下游消费不到 |
| `make compare-snapshots` | 跨快照漂移 | **L1** 演变追踪 | 基本可用，缺 N×M 多对漂移分析 |
| `make track-market` | 单市场时序 | **L2 雏形（错位）** | 现在从 parquet glob 读，本质是 L1 数据，不是真时序。L2 起来后整个重做 |
| `make show-market` | 单市场详情 | **L1/L2 都有用** | 保留，框架抽象 A 落地后扩展支持多源 |
| `make watchlist` | YAML 自选 | **L1 → L2 升级入口** | 保留，未来 daemon 模式下需要支持热更新 |

**还缺的（三层架构需要但当前没有）**：

- 框架抽象 A/B/C（统一市场状态、时序、事件 — 见 §1.5）
- L1 调度自动化（cron / systemd timer + 失败告警）
- L1 健康监控（持续可见的状态，不是手动跑）
- L2 定向跟踪 daemon（按 watchlist + 候选池定期刷新）
- L2 时序后端选型（DuckDB / TimescaleDB / hybrid）
- L3 WebSocket 客户端 + OHLC 聚合器
- 三层间触发条件（候选池规则 / 异动阈值 / 解锁条件）

---

## 4. 待决策的架构 trade-offs

骨架阶段先列出，调研后逐条对答案：

1. **L1 频率**：1 天 1 次 vs 1 天 2 次（早中各一）vs 按需触发？（Gamma 北美高峰慢，凌晨快 — 有现成证据）
2. **L1 调度方式**：cron / systemd timer / Python 应用内 daemon —— 框架启动期选哪个最快落地且可靠？
3. **时序后端选型**：DuckDB over parquet glob（零依赖、查询慢） vs SQLite + WAL（已有、写快查中等） vs TimescaleDB（生产标准、运维成本）？
4. **L2 实现形态**：常驻 daemon（asyncio loop） vs cron job 串成 pipeline？
5. **L3 触发条件**：人工 watchlist vs 自动信号（漂移阈值 / 流动性变化 / 即将结算）？
6. **数据保留**：L1 快照保多久？L2 时序保多久？L3 tick 流保多久？
7. **故障恢复**：daemon 崩了 / 网络断了 / API 超限了，分别什么策略？
8. **失败告警机制**：邮件 / 桌面 / 文件标记 / 第三方（PagerDuty 等）—— 框架启动期选最轻的

---

## 5. 决策后影响的下游 phase（保守预测）

按"稳定推进、每层做到生产级再进下一层"的纪律，**只预测最近的 2 个 phase**：

- **m1-perception Phase 02 — L1 生产级长跑 + 框架抽象 A 落地**
  - L1 调度自动化（cron / systemd）+ 失败告警 + 日志 rotate + 磁盘自动 purge
  - 统一市场状态 dataclass（抽象 A）+ 现有代码迁移
  - 健康监控（自动健康检查 + 异常立即可见的入口）
  - 完成判定：连续 7 天无人值守正确产出 snapshot，所有失败有告警

- **m1-perception Phase 03 — L2 定向跟踪 + 框架抽象 B 落地**
  - 时序后端选型 + 实施（基于 §2 调研结果）
  - L2 daemon（订阅候选池 + watchlist，分钟级刷新）
  - 候选池模型（L1 → L2 升级规则）
  - 完成判定：daemon 7×24 跑通，时序查询 <1s 响应，断网自愈

L3（WebSocket / OHLC）和事件总线（抽象 C）暂不预测，等 L1+L2 跑稳后再回头评估。

---

## 6. FAQ 增量区

（用户提问随时追加）

---

## Status / Next

**Status**: drafting (骨架完成 + 项目定位锁定 + 抽象层补齐，待调研填充 §2.1-2.5)

**Next actions**:
1. ✅ 骨架先给用户看，对方向（2026-05-10）
2. ✅ 加入"框架启动期 + 生产级 + 工程可落地"约束（2026-05-10）
3. 进入 §2 调研循环（建议串行 + 实证优先）：
   - **2.1**（读自己代码 + 跑实测，1 小时内出结果）
   - **2.5**（评估 L1 生产级缺口 — 与 2.1 同窗口可一起做）
   - **2.4**（看 `3th-party/polymarket-kalshi-weather-bot` 现成实现）
   - **2.2 + 2.3**（外部调研，Context7 + jina）
4. 调研结果回写本文档对应章节
5. 调研完成后产出"三层架构方案 v1"（§3 重新归类 + §4 决策对答案）
6. 基于方案决定 m1-perception Phase 02（L1 生产级长跑）怎么开

**Related**:
- `.planning/threads/market-microstructure.md` — Polymarket 微观结构基础（先决知识）
- `.planning/threads/market-structure.md` — 数据宇宙 4 层结构（API 层级）
- `.planning/threads/data-quality.md` — 已知数据质量问题
- `docs/research/polymarket-oss-landscape-2026-04.md` — OSS 项目调研报告
