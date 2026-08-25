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
| 29 | [Structure Snapshot 阶段诊断：超时先定位，再决定实验](29-structure-snapshot-stage-diagnostics.md) | 如何把 `gamma-markets` timeout 与健康完成区分开 / stage、elapsed 的证据链 / 为什么诊断不能自动触发调参 |
| 30 | [Structure 动态时钟](30-structure-adaptive-schedule.md) | 成功时长 p95 如何控制 timeout/cadence / timeout 立即退避与普通冷却 / 重启幂等 / health chain-truth |
| 31 | [Opportunity-first 按组盯盘](31-opportunity-first-group-watch.md) | 为什么在线正确性边界是一个完整组 / before→books→after 如何阻止混腿 / 候选失败为何不降级优先级 / durable due time 如何跨重启 |
| 32 | [Bounded Discovery](32-bounded-discovery.md) | opaque cursor 与页面事务 / promotion 前的真实组认证 / Decimal priority 与 age anti-starvation / 15-30-60 分钟统计覆盖 |
| 33 | [Checkpointed Full Reconciliation](33-checkpointed-reconciliation.md) | 慢地图如何分页续跑 / terminal completion proof / 原子 diff / 并发 Discovery 与 closure authority |
| 34 | [M1 异常恢复](34-M1异常恢复.md) | append-only incident / writer-side recovery proof / resource shedding 与 cooldown / shell-free producer isolation |
| 35 | [认证当前机会读模型](35-认证当前机会读模型.md) | current-authority 行绑定 / O(1) candidate 状态计数 / bounded keyset 机会页 / v2→v3 原子迁移 |
| 36 | [有界 Incident 权威](36-有界Incident权威.md) | prefix checkpoint / open authority / replay anchor / failure breadcrumb |
| 37 | [有界资源决策权威](37-有界资源决策权威.md) | deterministic policy replay / 512→256 compaction / suffix tail binding / resource evidence health |
| 38 | [四类证据时间线](38-四类证据时间线.md) | 单事务四源合并 / canonical group 游标 / opportunity 转换 / per-group checkpoint seed / 保守与精确 history floor |
| 39 | [机会运维读模型](39-opportunity-operations-read-models.md) | 认证机会入口 / zero 与 unavailable / server-time TTL / 真实 fixture / incident 分层证据 |
| 40 | [生产资格证据](40-production-qualification-evidence.md) | local vs production scope / release-machine-boot 窗口 / missing 不等于零 / immutable verdict |
| 41 | [故障资格矩阵](41-fault-qualification-matrix.md) | plan-only / fault-specific 授权 / durable incident / recovery writer / cleanup 串行门 |
| 42 | [生产故障控制边界](42-生产故障控制边界.md) | 四角色 SOURCE/VERDICT 能力矩阵 / fail-open control 与 fail-closed evidence / immutable source facts 与独立 stale 门 / group ID 与 outbox ID / cleanup→recovery→VERIFIED |
| 43 | [Structure 分页恢复](43-Structure分页恢复.md) | opaque cursor 页面事务 / 超时续跑 / RECOVERING 不停产 / complete-window 原子发布 / point reconciliation |
| 44 | [M1 生产恢复边界](44-M1生产恢复边界.md) | Quote 成功与 housekeeping 分界 / PostgreSQL→SQLite 类型适配 / L2 depth 继承 / 60-240-300 时钟 / 双缓冲政策门 |
| 45 | [Quote 投影与阶段收据](45-Quote投影与阶段收据.md) | 目标索引投影 / Structure revision fence / run-bound source receipt / 采集阶段 checkpoint / 原子发布边界 |
| 46 | [Structure 漂移安全切换](46-Structure漂移安全切换.md) | 同窗 raw/gen 双 root / scheduler child slice / 75s child + 15s transaction + 5s writer lock / sealed receipt |
| 47 | [Classifier-v2 诊断收据](47-Classifier-v2诊断收据.md) | 完整 sidecar projection / fresh-group-ineligible / atomic terminal receipt / 认证 status |
| 48 | [Fresh 投影的独立真值](48-Fresh投影的独立真值.md) | source-only group truth / bounded SHA checkpoint / member receipt root / sealed-stale 三方交叉绑定 |
| 49 | [常驻保留维护](49-常驻保留维护.md) | authority/payload/runtime 三层 / Quote 优先 / 500 行事务 / backoff、告警与恢复 / 30 万行验收 |
| 50 | [Classifier-v3 候选守恒](50-Classifier-v3候选守恒.md) | complete scan 与策略域的区别 / candidate 三出口守恒 / 七类 expected exclusion / 166,926 行 chunk-invariant 证明 |
| 51 | [Quote 分块 Staging](51-Quote分块Staging.md) | 为什么 500 行 staging 不会泄露半个 feed / current pointer 认证边界 / 持久化块进度 |
| 52 | [Supervisor 故障收据](52-Supervisor故障收据.md) | Quote child/worker/supervisor 边界 / 无秘密诊断 / 有界 Dashboard 读取 |
| 53 | [Structure 切片截止时间](53-Structure切片截止时间.md) | 45 秒正常 checkpoint 与 75 秒 watchdog 的区别 / 末页动态预算 / cursor 不丢失 |
| 54 | [Structure 写入索引](54-Structure写入索引.md) | 事务内 `MAX` 为什么会阻塞恢复 / ordinal 覆盖索引 / query-plan 回归 |
| 55 | [Structure 关系回填切片](55-Structure关系回填切片.md) | 为什么 cursor 不等于可持续恢复 / 50 行 checkpoint / SQLite writer 预算 / watchdog 只作兜底 |
| 56 | [Structure 回填查询截止](56-Structure回填查询截止.md) | outer slice 为什么拦不住卡住 SQL / progress handler rollback / `bootstrap` 诊断边界 |
| 57 | [Quote 重启租约回收](57-Quote重启租约回收.md) | 滚动发布为什么会留下 210 秒采集空窗 / Quote-only 回收如何保持 Structure 互斥 / `released` 收据如何审计 |
| 58 | [健康读取预算与取消](58-健康读取预算与取消.md) | 为什么完整健康检查不能套 0.8 秒 / SQLite connection 如何随 deadline 中断 / 真 P1 与自报 P1 的边界 |
| 59 | [Structure 写忙恢复重试](59-Structure写忙恢复重试.md) | checkpoint 不等于完成 / 五秒有界 defer / Quote 优先而不把 Structure 饿死 |
| 60 | [健康投影总预算](60-健康投影总预算.md) | 多个短 SQLite 读的总时限 / 8s 内部预算 / 10s 外部告警边界 |
| 61 | [Structure 查询截止](61-Structure查询截止.md) | slice 时钟进入 SQLite / deadline checkpoint / 避免 75 秒 watchdog 强杀 |
| 62 | [Sealed 后立即认证](62-Sealed后立即认证.md) | sealed 不是 P1 closure / 两秒 continuation / Quote 仲裁不被绕过 |
| 63 | [事务型 Quote 云端控制面](63-事务型Quote云端控制面.md) | batch lease / 冻结身份 / R2 receipt / terminal pointer / 默认关闭 operator 边界 |
| 64 | [事务型云端控制面](64-事务型云端控制面.md) | at-least-once 执行与 exactly-once durable effect / Structure+Quote lease 接管 / 独立 control-api / 云端上线门 |
| 65 | [三次影子一致性门](65-三次影子一致性门.md) | 三次 shadow evidence / source+bundle+counts+universe 对账 / pointer 零变更 / 可逆切换前 gate |
| 66 | [事务型 Structure 源窗口](66-事务型Structure源窗口.md) | Gamma 单页事务 / opaque cursor / R2 证据与 Postgres 回执 / lease 接管 / events→markets 交接 |
| 67 | [事务失败告警闭环](67-事务失败告警闭环.md) | retryable 为什么必须同时产生 incident/outbox / lease epoch 幂等 / 不制造告警风暴 |
| 68 | [事务型告警投递](68-事务型告警投递.md) | outbox lease / append-only delivery receipt / 外部通知不阻塞采集 |
| 69 | [事务型熔断与恢复](69-事务型熔断与恢复.md) | job-scoped circuit / deterministic probe / fenced recovery / operator projection |
| 70 | [Structure 源并发租约池](70-Structure源并发租约池.md) | event cursor 为什么串行 / market batch 如何八 lane 并发 / `succeeded:1/8` 如何判读 / 为什么并发不削弱 lease fencing |
| 71 | [事件内嵌 Structure 源](71-事件内嵌Structure源.md) | 为什么二次 market 拉取无法在线收敛 / event 页如何同源展开 market / terminal event 如何直接释放物化 |
| 72 | [长事务任务的恢复与吞吐预算](72-长事务任务的恢复与吞吐预算.md) | checkpoint 为什么是恢复证据 / 健康与发布为何分离 / 串行 lease budget 为什么不是无约束并发 |
| 73 | [R2 回执边界与租约接管](73-R2回执边界与租约接管.md) | R2 上传不等于完成 / fenced receipt 才是发布 / 真实进程丢失后的 epoch 接管 |
| 74 | [受控重试熔断与范围告警](74-受控重试熔断与范围告警.md) | retry circuit 与进程丢失的区别 / SQL 范围领取 / 不回放历史 Telegram outbox |
| 75 | [事务型云端采集工作池](75-事务型云端采集工作池.md) | role-local lease worker / coordinator 与独占池 / 高水位背压 / queue health hint |
| 76 | [真实 R2 故障接管与持续证据](76-真实R2故障接管与持续证据.md) | R2 crash boundary / lease epoch 接管 / 唯一 receipt / 24 小时 fail-closed soak |
| 77 | [长认证租约心跳](77-长认证租约心跳.md) | 短 lease + same-epoch heartbeat / R2 parity / stale fence / 最终提交 |
| 78 | [云端持续验收证据](78-云端持续验收证据.md) | 独立 sampler / Postgres append-only ledger / 900 秒缺口 / 24 小时 fail-closed 验收 |
| 79 | [独立运行看门狗](79-独立运行看门狗.md) | 为什么验收器不能替代告警器 / API+五机 fail-closed 状态 / 30 秒心跳与 transition-only Telegram |
| 80 | [多应用运行时看门狗](80-多应用运行时看门狗.md) | 为什么 sampler 必须独立、怎样在不授予告警器数据库权限的前提下同时监控两应用 |
| 81 | [运行异常 Dashboard 账本](81-运行异常Dashboard账本.md) | 为什么 Telegram 不是审计记录、异常如何进入云端账本、为何“不可用”不能伪装成健康 |
| 82 | [外部告警监督器](82-外部告警监督器.md) | 谁来监控监控器、Cloudflare Cron 如何留下 Telegram 与 Dashboard 的同源异常闭环 |
| 83 | [可诊断运行异常 Dashboard](83-可诊断运行异常Dashboard.md) | 为什么“有一条报警”还不够、检测/恢复如何构成同一事件生命周期、怎样从页面直接定位受影响对象 |
| 84 | [任务局部事实与事件驱动自愈](84-任务局部事实与事件驱动自愈.md) | heartbeat 与 progress 为什么不能混为一谈 / terminal fact 如何和业务结果原子提交 / timer 与 watchdog 为什么只能做兜底 / 八任务真实终态门如何防止假阳性 |
| 85 | [有围栏的截止时间协调器](85-有围栏的截止时间协调器.md) | 任务事实如何进入纯 deadline 判断 / controller、job、action 三层租约 / durable budget 与 active-target 仲裁 / stale-noop 与 fail-loud 错误的区别 / 只读 status 与 job-level 权限边界 |
| 86 | [滚动资格证书与自动重开](86-滚动资格证书与自动重开.md) | contained 与 breaking 如何决定 epoch 命运 / ingest_seq cursor 为什么不丢迟到事实 / freshness 双视角 / certificate read+reverify trust 边界 / Make 入口权限 |
| 87 | [任务自愈与滚动验收](87-任务自愈与滚动验收.md) | 任务本身事件触发与 watchdog 兜底如何汇合 / control-plane Dashboard 四面板如何读 / Telegram 与 outbox 边界 / smoke 为什么必须用已认证正文 |
| 88 | [确定性故障矩阵与 Observe-Only 上线门](88-确定性故障矩阵与Observe-Only上线门.md) | 12 类真实 PostgreSQL 故障如何压缩验证反馈 / decision 与 idle 证据 / observe-only 零 mutation 窗口 / 精确 Fly recovery capability 边界 |

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
