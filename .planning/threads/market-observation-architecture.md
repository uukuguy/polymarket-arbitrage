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

## 0.5 进程身份不能等同于数据库能力（2026-08-26）

Plan 05.6-207 adds a fourth production-readiness boundary before self-healing
can be enabled: the runtime process identity must be narrower than the schema
owner and must be proven at startup. The model is:

- LOGIN role = connection identity, password rotation, and emergency `NOLOGIN`.
- Capability role = reviewed database authority bundle.
- Startup contract = read-only proof that effective authority and namespace
  resolution are neither missing nor broader than expected across the complete
  catalog envelope.

For M1 self-healing this creates two scoped pairs:
`m1_runtime_controller_login` inherits only
`m1_runtime_controller_capability`, and `m1_qualification_worker_login`
inherits only `m1_qualification_worker_capability`. The runtime controller can
read runtime/job state and write observe decisions, but it cannot mutate
recovery actions in observe-only mode and cannot access public sequences. The
qualification worker can read qualification/publication truth and call bounded
freshness/certificate functions, but it cannot directly insert the qualification
ingress ledger or use its identity sequence.

“Exact” means catalog enumeration plus deterministic object resolution, not a
handful of negative probes. Both daemon SQL paths use schema-qualified
application objects. Migration and daemon/operator checks compare every
non-system namespace/object authority and ownership, database CREATE,
active/role/database search path, every SECURITY DEFINER routine, and complete
PG16 membership option tuples against closed allowlists. Thus shadow schemas,
non-public grants, path overrides or role delegation options cannot hide outside
the named profile. `TEMPORARY` remains an explicit compatibility allowance only
under this controlled namespace proof.

Production boundary after local closure: production database `postgres` has
revisions 022/023/024/025 applied, revision 026 is not applied, the original
four apps are running, and the new runtime-controller/qualification-worker apps
plus scoped production logins/secrets do not exist. The next action is an exact
authorization package for corrected application release
`d050c8290c52e07acb72c8db7fe3fb02072d126c`, revision 026, the two login roles,
two new private apps, observe-only mode, empty recovery allowlist, rollback, and
the 05.6 evidence directory. It is not direct migration or deployment.

Planning hygiene boundary: external Plan 05.6-207 is audited through the
`plan-source` frontmatter in `05.6-207-SUMMARY.md`; a missing plan-side anchor
or stale reviewed-template SHA256 is drift. `.githooks/pre-commit` remains a
staged SUMMARY safety layer, while `.githooks/commit-msg` enforces plan-scoped
subjects from Git's actual message file. Fresh local H-018 run
`20260826-135855-h-018` binds the corrected executable state to
`d050c8290c52e07acb72c8db7fe3fb02072d126c` with all four scoped nodes passing.

See `docs/learning/89-数据库能力角色与进程身份.md` for the teaching version.

## 0.6 Provider 角色创建者也是 catalog 事实（2026-08-27）

Supabase 生产连接中的 `postgres` 不是 superuser，而是由
`supabase_admin` 授权的 delegated `CREATEROLE` 身份。PostgreSQL 16
用它执行 `CREATE ROLE capability` 或 `CREATE ROLE login` 时，都会自动
留下一条创建者成员关系：`postgres -> created_role`，选项精确为 `ADMIN=true, INHERIT=false,
SET=false`，grantor 为 `supabase_admin`。这条边是 provider 的角色生命周期
真值，但它不让 `postgres` 继承或 `SET ROLE` 进入 capability。

闭合权限包因此对 capability 和 scoped LOGIN 都只允许这一条 exact
ambient creator tuple；应用 login 指向 capability 的唯一 effective tuple 必须为
`ADMIN=false, INHERIT=true, SET=true`。任何其他 member、grantor 或选项变化
继续 fail-closed。这是“承认 provider 现实”，不是把运行时权限放宽给
admin identity。

## 0.3 失败事实与已发布事实必须分离（2026-07-27）

`snapshots` / `market_view_published` 说明最近一版可读市场事实；`snapshot_attempts` 说明
调度器刚刚做了什么。两者不能互相覆盖：旧完整 revision 可能仍适合解释 M2 的已知结构，
同时新的采集已因 OOM 失败。严格健康检查必须同时暴露 published truth age、latest attempt
和连续失败计数；Polywatch 也必须按 L1、机会 feed、L2、Dashboard 各自管理 incident/recovery。
这一层只解决可观测性，不能被误写成 all-in-one snapshot 已满足 Structure production SLO，
也不等同启动下一轮 24 小时资格验证。

---

## 0.4 Structure、Quote、Archive 是三个产品（2026-07-27）

生产上的 **Structure** 是 Gamma 的完整市场/事件/member truth，必须可以原子发布给 Quote 和
M2；它不读取 CLOB、不写 Parquet/R2。**Quote** 才是可交易价格事实，绑定一个 Structure
revision 并有独立时钟。**Archive** 是按需的 CLOB/Parquet/R2 研究证据：可以慢、可以失败、
必须有自己的状态，但 `market_view_published=0`，不得替换 Structure。

这不是“给 all-in-one snapshot 加更多内存”的变体。第一阶段只在线调度 Structure；Archive
要等独立容量、成本预算和结果通道后才可进入生产日程。strict health 的 `archive:*` 项只作
非阻断证据，Archive warning 不能把 Structure→Quote→M2 健康误判为中断。

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

### 0.2.1 部署形态约束（同次会话再补充）

> "现在就要设计可直接实施的部署架构，我认为完全可以使用面向创业公司的云
> 基础设施，部署服务器、数据库、监控网站等。现在的日级全量快照、定向快照、
> 单市场 K 线等的采集服务和监测管理服务等，完全可以本地研发完成直接一键
> 部署云端开始工作。具体选型可以深度研究一下，这个的市场可选很久，选主流
> 稳定价格合适的。"

**部署范式锁定**：本地研发 → 一键部署 → 云上 7×24 自主跑。**不是"先本地跑后迁移"** —
L1/L2/L3 一开始就按云原生形态设计（数据库选型、长跑调度、日志聚合、监控告警都
按云上现成服务来选）。这个约束直接影响 §1.5 抽象 B 的时序后端选型、§2 调研
问题的范围、§5 Phase 02 的边界。

#### 0.2.1.a 用户侧硬约束（2026-05-11 会话开头补问，§2.6 调研可行域）

| 维度 | 用户答复 | §2.6 调研含义 |
|---|---|---|
| **支付能力** | "CN + 美区 + PayPal 都可以；启动阶段先用免费额度" | 可行域全开 — Fly/Render/Railway/DO/Hetzner/Supabase/Neon/Better Stack/Grafana Cloud 全部可候选；**启动期优先免费/试用层组合**，把硬付费推迟到产生明确价值 |
| **预算档** | "启动阶段先不定" | 不锁预算，但调研产出必须分档列（< $25 / $50-100 / $150-300）让用户后续按价值决策 |
| **未来云上交易执行（6-12 月）** | "**是，要预留方案**" | **§2.6 必须把"私钥管理 + 低延迟出网 + 合规模型"列为一类一级维度**，不能只按观察栈选；选型时排除"未来加交易要彻底重选"的栈（如纯 serverless / 无 VPC / 无 secrets manager 的栈）；推荐候选必须能演进出 KMS/Vault/TEE 路径 + 私网出站 + 固定 IP 白名单 |
| **数据出境 / 部署地区** | "具体分析，都有倾向" | §2.6 region 选择不预设结论，而是按**延迟（数据源在美东）/ 本人监控延迟（CN 操控 dashboard）/ 合规出境**三向量做分析，给出"美东 / 新加坡 / 日本东京 / 香港"对比表，让用户基于实际优先级选 |

**对 §2.6 调研的直接影响**：
- 候选栈必须**预留交易执行演进路径**（私钥安全 / 私网 / 固定 IP）— 这会过滤掉一些纯 PaaS 选项
- 启动期方案优先**免费/试用层堆叠**（Fly free tier + Supabase free + Better Stack free + Grafana Cloud free），把"开始花钱"作为价值产生后的决策
- 地区选型不预判，列对比表让用户决策
- 不锁预算 = 调研产出必须给"按预算分档的推荐组合"，不是单一推荐

#### 0.2.1.b §2.6 调研产出 — 关键事实修正（2026-05-11 窗口 B）

完整对比矩阵 + 4 档预算推荐 + 决策树见 `.planning/threads/deployment-architecture.md`（872 行）。

**最大方向纠偏 — Polymarket 服务器在 AWS eu-west-2 London**：
- 早期 §0.2.1.a "数据源在美东"是错误预设
- 实测来源：[NYCServers 2026-04-07](https://newyorkcityservers.com/blog/polymarket-server-location-latency-guide)
- 含义：**所有数据抓取层必须部署在 Dublin / Amsterdam / Helsinki**（低延迟 + 非封锁）
- Polymarket IP 黑名单 33 国（US / UK / SG / HK / CN 全在内）→ 直接影响候选栈：**Render 全区废、Railway us-only 废、Fly.io AMS 命中、Supabase Dublin 命中**

**4 档推荐**（详见 deployment thread）：
- $0 免费堆叠：Fly trial + Supabase Free + Axiom/Sentry/Better Stack Free + Cloudflare Pages
- $30：Fly AMS + dedicated egress + Supabase Pro Dublin
- $100：上面 + Sentry Team + Supabase compute add-on
- $300：多 region + Tiger Cloud Dublin + AWS KMS（trading）

**Polymarket Auth 关键事实**（影响交易执行预留）：
- Auth 必须是 EOA secp256k1（EIP-712 L1），AWS KMS / Vault Cloud BYOK 可托管
- 真正卡 trading-readiness 的不是 KMS，而是**出口固定 IP**（Cloudflare 限流白名单）→ Fly.io $3.60/月 dedicated egress IPv4 是刚需

### 0.3 直接结论

1. 全量快照只能用于"日级市场画像"，不能做策略主角
2. 真正的实盘观察需要**三层金字塔**：日级全量（候选池） → 定向（主题/分组） → 单市场（K 线）
3. 之前的 6 个 scan recipes 是"切面"不是"全景"，定位需要重新归类
4. 三层之间必须有**触发关系**（前因后果），不是随机选市场
5. **平台框架，不是工具集合** — 三层之间共享统一的数据抽象、时序模型、事件机制
6. **生产级 = 可长跑 7×24** — 单次跑通不算数，挂了能恢复、状态可观测才算
7. **稳定推进 = 每层做到生产级再进下一层**，不一次性把三层都铺开
8. **云原生优先 = 选型时优先 PaaS / managed services**（compute + DB + log + metrics 一站式），不自己造运维基础设施
9. **一键部署是工程纪律** — 部署成本低 = 迭代成本低；任何"在本地跑得起来但部署上去要改一堆"的设计都不合格
10. **L1 "日级全量" = subset（~23k 高流动性市场），不是 full**（§2.7 实证锁定，2026-05-11）— full 保留为周/月审计工具，不作策略数据源

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

> 判定"生产级"不能只看 code-level OK —— 必须按 §1.6 chain-truth discipline 把
> `failure → /health → human signal` 这条链端到端走通。Phase 03 Inj L2-2 是
> 这条纪律的诞生原因（mirror fail-soft envelope 完美但 /health 子检查 gate 在
> 不存在的 config flag 上，5 天没人发现）。

> **L1 Phase 02 偏离备注 (2026-05-19)**：Phase 02 关闭时**没有**收集 "7 天连跑" 这个凭证 — 用 4 次 prod chaos injection 替代了 7 天日历门。完整决策 + 风险面 + 未来回补点见 [`soak-gate-deviation-2026-05.md`](./soak-gate-deviation-2026-05.md)。**Phase 03 (L2) discuss-phase 时必须把这个凭证补上才能视为 L1 真生产级。** 本表上 L1 行的原定义不变。

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

任意层级写入的市场状态都进入**统一的时序后端**（候选见 §2.6.B 云原生选型 — 大概率是
managed Postgres + TimescaleDB extension，因为 §0.2.1 的部署约束要求选 PaaS 友好的方案）：

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

#### 2.1.a 实证结果（2026-05-11 窗口 A）

**1. fetched_at_ms stamp 机制 — schema-level 拖尾不可见**

代码证据：`src/polyarb/snapshot/orchestrator.py:340-343`

```python
clob_done_ms = int(time.time() * 1000)
with _phase("5/7: Stamp + attach top-of-book"):
    for m in target_markets:
        m["fetched_at_ms"] = clob_done_ms
```

- `fetched_at_ms` 是 **stage 5（CLOB fetch 完成后）一次性 stamp**，所有 target_markets 共用同一毫秒值
- 不是逐市场 / 逐 page 抓取时的真实时间戳
- events 表用更晚的 `finished_at_ms`（stage 7 时戳），但同样**所有 row 同值**
- DuckDB 验证（4 个历史 snapshot）：`COUNT(DISTINCT fetched_at_ms) = 1` per snapshot

**含义**：下游消费者**无法**从 schema 知道某条 row 是在 8 分钟里哪一秒抓的。
所有时序分析、漂移检测、套利信号都建立在"该 snapshot 内时间一致"的虚假前提上。

**2. 真实 elapsed（snapshot_taken_at_ms → finished_at_ms）— "8 分钟"是实测平均**

```
sid | mode   | mkts   | elapsed | 备注
----+--------+--------+---------+------
  1 | subset |  20589 |  496.6s | 2026-05-02 早期跑
  2 | subset |  17486 |  418.8s | 2026-05-04
  3 | subset |     ?  |   96.0s | 2026-05-10 cache 热跑（fixture / quick）
  4 | subset |  24032 |  527.3s | 2026-05-10
  5 | subset |  21023 |  519.6s | 2026-05-11 20:18 RUN1
  6 | subset |  21169 |  510.5s | 2026-05-11 20:26 RUN2
  7 | full   |  54424 |  917.6s | 2026-05-11 22:00 — full 实测 15m19s（远低于 CLI 注释的 1-2h）
  8 | subset |  23448 |  581.7s | 2026-05-11 22:29 — Makefile 修复后
```

- subset 平均 ~8-10 分钟（7 次跑：均值 506s，最大 581s）
- full 单次实测 15m19s — **CLI 注释 "~1-2 hours" 是 worst case，typical case 15-20 分钟**
- cache 热可压到 ~1.5 分钟（sid=3），但不能依赖
- 与用户洞察"8 分钟以上"完全吻合

**3. 跨 snapshot mid 价漂移（A-3 双跑实证，2026-05-11）**

测试方法：subset snapshot 连跑两次（RUN1 20:18→20:26、RUN2 20:26→20:35，间隔约 9 分钟），
对同时存在于两次 snapshot 且都有 best_bid/best_ask 的市场（n=19,081，~80% subset 覆盖），
计算 `mid = (bid+ask)/2`，统计 `|mid_run2 - mid_run1|`。

**结果分布**：

| 漂移区间 | 占比 | 含义 |
|---|---|---|
| 恰好 0 | **99.15%** | 9 分钟内 mid 价完全不变 |
| 0 - 0.1¢ | 0.04% | 微噪声 |
| 0.1¢ - 0.5¢ | 0.07% | 一般噪声 |
| 0.5¢ - 1¢ | 0.29% | 临界 |
| 1¢ - 5¢ | 0.34% | 实质性移动 |
| > 5¢ | 0.10% | 显著移动（含新开市场从 50¢ 默认价跳到真实价） |

汇总统计：mean=0.02¢，p50/p75/p90/p95/p99=0，max=30¢。

**Top 10 movers 模式**：基本都是 `mid_r1 = 0.50` → `mid_r2 ≠ 0.50`，
即"新开市场默认 50¢ 锚 → 流动性进来后跳到真实价"。不是已有市场的内生波动。

**修正先前假设**：
- 原假设："L1 是 8 分钟模糊影像，价格漂移影响所有市场"
- 实证：**99% 市场内部一致性极强（漂移 = 0），1% 长尾市场才显著漂移**
- 这是 Polymarket 薄市场结构的体现 — 多数市场全天无 trade，价格挂着不动

**含义重组**：
- ✅ **L1 全量快照内部一致性不差** — 用于"画像 / 候选池" 完全够
- ⚠️ **不能假定 1% 长尾市场"无关紧要"** — 它们恰好是流动性 + 价格变化双高的市场，套利信号最可能来自这类
- ⚠️ **9 分钟拖尾 vs 9 分钟实测漂移 — 同量级**，但**漂移集中在少数市场**，全样本看上去"一致"
- ❌ **不能基于 L1 做分钟级套利** — 即使 99% 市场稳定，1% 出错就是亏损来源
- ✅ **L2 定向跟踪的价值证据强** — 把这 1% 长尾市场切出来给分钟级 cadence 完全合理

**架构含义 — 三层金字塔的实证基础**：

| 层 | 时间窗口 | 实证支持 |
|---|---|---|
| L1（日级全量） | 9 分钟拖尾内 99% 市场零漂移 | ✅ 做画像、候选池筛选 |
| L2（定向分钟级） | 1% 长尾市场漂移 > 0.5¢ | ✅ 必须切出来高频跟踪 |
| L3（单市场 K 线） | top movers 30¢/9min = 200¢/h 量级 | ✅ 真正活跃的市场需要 tick 流 |

**4. 2 小时窗口漂移分布（SESSION 15 续，2026-05-11）**

测试方法：sid=6 (20:35) vs sid=8 (22:39) — 间隔 **124.3 分钟**，joined n=18,509。

**结果分布**：

| 漂移区间 | 9min 占比 | **2h 占比** | 倍数 |
|---|---|---|---|
| 完全不变 | 99.15% | **97.77%** | 不变区间缩小 1.4 pp |
| 0.5¢ - 1¢ | 0.29% | **0.72%** | **2.5×** |
| 1¢ - 5¢ | 0.34% | **0.93%** | **2.7×** |
| > 5¢ | 0.10% | **0.30%** | **3.0×** |

汇总：mean=0.056¢ (vs 9min 的 0.02¢)，p50/p90/p95=0，p99=1¢，max=35¢。

**关键观察 — Top movers 锚价模式**：

```
701800   0.1500 →  0.5000   drift=0.3500
2088568  0.5000 →  0.2505   drift=0.2495
2071546  0.2550 →  0.5000   drift=0.2450
1494695  0.4950 →  0.7250   drift=0.2300
2074081  0.2850 →  0.5050   drift=0.2200
```

`0.5000` 是新开市场无人挂单时盘口默认锚价。Top movers 主要不是"市场剧烈波动"，而是**新流动性进入/退出导致锚价跳出**。真正的内生价格波动隐藏在 1-5¢ 那 0.93% 区间里。

**含义**：
- 2 小时窗口里 95%+ 市场仍然完全静止 — Polymarket 薄市场结构在更长时间尺度上依然主导
- 但 "活跃市场"（有内生波动那部分）漂移幅度**随时间线性放大** — 9min 0.83% 市场 > 0.5¢，2h 翻 2.5-3 倍到 ~2% 市场 > 0.5¢
- **L1 日级 cadence（每天 1-2 次）→ 1% 市场可能错过 5-10¢ 移动** — 这正是 L2 定向跟踪要切出来的目标人群

**结论（§2.1 完整锁定）**：

- ✅ **fetched_at_ms schema-level 拖尾不可见** — 框架抽象 A 必须显式标注"stamp 时间 vs 抓取时间"
- ✅ **8-15 分钟拖尾物理事实** — subset 8-10min（7 次实测），full 15-20min（1 次实测）
- ✅ **L1 可用于日级 / 不可用于分钟级** — 99% 一致 但 1-2% 长尾恰是策略目标
- ✅ **漂移随时间近似线性放大** — 9min/2h 实测倍数 2.5-3×
- ✅ **三层金字塔架构在数据特征上有实证支持** — 不是工程美学，是数据驱动的必要拆分

**结论（2.1 部分锁定）**：

- ✅ **schema 层无法表达"snapshot 内时间不一致"** — 这是**框架抽象 A**（统一市场状态模型）的关键设计点：要么 stamp 真实 page-level 时间（改 normalizer），要么明确标注"这是 stamp 时间不是抓取时间"
- ✅ **8 分钟拖尾窗口是物理事实** — 全量 L1 只能日级用途；任何分钟级以下决策必须走 L2/L3
- ⏳ 单市场漂移分布待 A-3 数据出来后定 v1

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

### 2.6 云原生部署架构选型（最深度的调研项）

**问题**：从"本地 Python + SQLite + Makefile"到"云上 7×24 跑 + 监控 + 一键部署"，
最适合本项目（个人开发 / 创业公司预算 / Polymarket 数据采集 / Python 主力）的
**完整云栈组合**是什么？

具体子问题（按调研维度）：

**A. Compute（长跑 daemon 跑哪）**
- Fly.io（边缘部署 + 全球 region + 应用容器） vs
- Render（Heroku 体验 + 北美主导） vs
- Railway（开发者体验最好 + 单 region） vs
- DigitalOcean App Platform（老牌稳定） vs
- Hetzner Cloud + 自管 docker-compose（最便宜但运维成本高）
- 关键约束：
  - Polymarket Gamma API 的友好 IP 段（北美 / 欧盟优先）
  - 长跑 daemon 模式（不是 serverless 冷启动）
  - 容器化（Dockerfile + 一键部署）
  - 价格 < ~$25/月 单服务

**B. 数据库（时序 + 关系数据混合）**
- managed Postgres + TimescaleDB extension（Supabase / Neon / Render Postgres / Crunchy Bridge）
- DuckDB on attached volume（Fly volumes / Render disk）
- ClickHouse Cloud（时序专门、定价对小用量友好）
- 决策点：
  - 我们当前 SQLite + parquet 怎么映射？
  - L2 时序数据预计量级（每天一个 watchlist 50 个市场 × 1 分钟 × 24h = 72k 行/天 × 365 天 = 26M/年 — 单表完全够用，不要过度选型）
  - 价格 < ~$20/月 managed 实例

**C. Observability（日志 + 指标 + 告警）**
- Better Stack（日志 + uptime + status page + 价格友好）
- Grafana Cloud Free（10k metrics / 50GB log / 14 天保留 — 慷慨）
- Axiom（日志专门 + 慷慨 free tier）
- Sentry（错误聚合，已经成熟）
- 决策点：
  - 异常立即可见 → 邮件 / Slack / 桌面通知
  - 健康看板（uptime + last successful snapshot）
  - 价格：起步阶段 free tier 即可

**D. Deployment（一键部署链路）**
- GitHub Actions → Fly Deploy / Render Deploy / Railway Deploy
- 单一 `fly deploy` / `git push` 触发
- Secrets 管理（API key / DB password 不进 git）
- 决策点：
  - PR-based preview env？还是单一 prod？（启动期单一 prod 即可）
  - 蓝绿 / canary 还是直接替换？（启动期直接替换）

**E. Frontend / Dashboard（监控网站）**
- 当前 `make overview` 是 CLI rich.Table。云上需要 web 形态：
  - Streamlit Cloud（最快搭起来 + Python 原生）
  - 自己写 FastAPI + HTMX + Tailwind（控制权强但工作量大）
  - Grafana dashboard over Postgres（如果 B 选 Postgres，零额外代码）
- 决策点：
  - 框架启动期，"零代码 dashboard"（Grafana over Postgres）可能最经济
  - 但 Streamlit 对自定义业务逻辑（候选池审阅 + watchlist 编辑）更灵活

**F. 跨方向约束（影响 ABCDE 同时）**
- 部署地区 → Polymarket 限制 / 中国大陆访问 / 用户操作面板时延
- 信用卡支付 — 哪些云收 CN 信用卡 / 哪些只收美区
- 数据出境合规（如果未来要做 KYC 类合规，敏感）
- "未来会上交易"的扩展性：是否需要私钥安全（Vault / KMS）和低延迟到 Polygon RPC

**调研方式**：
- jina/web 搜每家最新定价 + 用户评测（注意时效，市场变化快）
- 已部署 polymarket-bot / quant 类项目的 OSS 工程文件（Dockerfile / fly.toml / render.yaml）
- `3th-party/polymarket-kalshi-weather-bot/` 是否包含部署配置
- Hacker News / r/SaaS 最近 6 个月相关讨论
- 最关键：**做一个对比矩阵**（5 个候选 × 6 个维度 × 价格分档），不靠单点意见

**预期产出**：
- 一份 `.planning/threads/deployment-architecture.md`（独立 thread）— 选型矩阵 + 推荐栈 + 部署蓝图（Dockerfile 草稿 + 一键部署脚本骨架）
- 更新本 thread §1.5 抽象 B（时序后端选型基于 §2.6.B 的结论锁定）
- 更新本 thread §5（Phase 02 范围扩到"L1 云上 7×24 跑通 + 一键部署链路打通"）

**为什么必须先答（且要深度调研）**：
- 一旦本地写了 SQLite-only 代码再迁移 Postgres，是可避免的工程债
- 一旦选型错误（某家厂商 6 个月后涨价 / 倒闭 / 限制 CN 用户），切换成本巨大
- 这是**框架启动期最重要的一次单点决策** — 影响后续所有 phase 的形态



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

#### 2.5.a 实证缺口清单（2026-05-11 窗口 A）

代码扫描 `src/polyarb/` + 根目录部署物，对照 7 维度：

| 维度 | 现状 | 缺口 | 严重度 | 触发场景 |
|---|---|---|---|---|
| **调度** | 仅 Makefile target 人工触发 | 无 cron / APScheduler / systemd timer / k8s CronJob | 🔴 阻断 | 云上 7×24 必须自动跑 |
| **健康检查** | `grep -r /health src/polyarb/` 零结果 | 无 HTTP `/health` endpoint | 🔴 阻断 | Fly/Render/Railway readiness probe 接不上 |
| **日志聚合** | loguru → stderr 文本 | 未 structlog 化；无远程 sink 配置 | 🟡 必须补 | Better Stack / Axiom / Grafana Loki 需 JSON 格式 |
| **告警** | `watchlist-alerts` 子命令仅 stdout 输出 | 无 webhook / Sentry / Slack / 邮件 | 🟡 必须补 | snapshot 连续失败 / API quota 用尽 / 磁盘满 无人知晓 |
| **部署物** | 项目根无 Dockerfile / fly.toml / render.yaml / `.github/workflows/` | 整个一键部署链路缺失 | 🔴 阻断 | 用户约束 §0.2.1 明文要求 |
| **重试** | ✅ `clients/gamma_client.py` + `clob_client.py` 用 tenacity exponential backoff | 已经做好 | ✅ | — |
| **数据保留** | ✅ `snapshots-purge` 子命令存在 | 仍需被调度（依赖 #1） | 🟡 部分 | 长跑 30 天后磁盘满 |

**额外发现 — CLI 入口契约破裂（本会话触发）**：

5-11 跑 `make snapshot-markets` 两次，每次 1 秒退出 `exit 0`。**没有任何 snapshot 落地**。

根因：
- `polyarb/snapshot/cli.py` 已升级为 typer 多 subcommand 结构（`snapshot` / `snapshots-purge`）
- 但 `Makefile:56-60` 仍调 `uv run python -m polyarb.snapshot`（无 subcommand）
- typer 显示帮助页 + 正常退出 — **cron 看到 exit 0 以为成功**

含义：
- 这是"生产级长跑"最危险的一类故障 — **silent failure with success exit code**
- 任何只看 exit code 的健康检查（systemd `Restart=on-failure` / k8s livenessProbe / 监控告警）都会被骗
- 必须在 Phase 02 加入：**snapshot 成功 = "exit 0 + parquet 文件落盘 + SQLite snapshots 行 +1"** 三联校验，不能只看 exit
- 同步：**修 Makefile target 调 `polyarb.snapshot snapshot`**（这次会话顺手补，见 commit）

**结论（2.5 部分锁定）**：

- 🔴 当前 L1 距云上 7×24 还差 **5 个核心维度**（调度 / 健康 / 日志 / 告警 / 部署物）
- ✅ 重试 + 数据清理已经做好（不再是缺口）
- 🔴 **健康判定语义必须比 "exit 0" 更强** — 否则 silent failure 没人发现
- ✅ 这 5 个缺口正好对应 §2.6 调研报告（`threads/deployment-architecture.md`）的 5 个候选栈维度，**两套调研产出可以一对一映射到 Phase 02 plan**

---

### 2.7 subset vs full 决策实证（2026-05-11 续）

**触发**：会话中跑了 1 次 full（sid=7）+ 修复 Makefile 入口断裂后跑了 1 次 subset
（sid=8），首次有跨模式实测对比。先前 §0.1 假设"L1 全量"语义模糊（subset 还是
full 都叫全量？），本节用实测锁定。

#### 2.7.a 字面差别（来自代码 + 实测）

| 项目 | subset | full |
|---|---|---|
| Gamma 元数据抓取 | 全市场（同） | 全市场（同） |
| 过滤逻辑 | `liquidity_usd > $1000` 过滤后才进 normalize 持久化 | 不过滤 |
| 持久化范围 | ~21-24k 行（全 liquidity > $1k 市场）| ~54k 行（全市场）|
| CLOB 调用数 | ~23k token | ~54k token (~2.3×) |
| 实测 elapsed | 8-10 分钟（7 次跑均值 506s）| 15m19s（1 次实测） |
| 时间错位窗口 | 8-10 分钟 | 15-20 分钟（拖尾翻倍）|

代码证据：`orchestrator.py:264-268` — `target_markets` 在 stage 3 已按 mode 分流，
**stage 7 只持久化 `target_markets`**（filtered-out 在 SQLite/Parquet 都不存在）。

#### 2.7.b 数据特征差别（sid=7 full vs sid=8 subset 实测）

```
subset sid=8 持久化:    23,448 行
  liquidity > $1k:       23,448 (100%)
  双侧有 bid/ask:        21,645 (92.3%)

full sid=7 持久化:      54,424 行
  liquidity > $1k:       ~23k (与 subset 重合)
  liquidity ≤ $1k/NULL:  ~31k (subset 跳过的部分)
    └ 有挂单价格:        ~27k (小池子，但有人挂单)
    └ 无挂单 / 死市场:    ~6k (CLOB 取了但盘口空)
  双侧有 bid/ask:        46,730 (85.9%)
```

**关键事实**：subset 不是"截断了元数据"，是"截断了 Polymarket 上所有**有交易意义**
的市场之外的长尾"。多抓的 31k 个市场，27k 是"小到无法套利的池子"，6k 是死市场。

#### 2.7.c 策略相关性判断

| 用途 | subset 是否够 | 理由 |
|---|---|---|
| 套利信号源 | ✅ 完全够 | 流动性 ≤ $1k 的市场吃 $200 单子就打穿盘口，slippage > 价差，无利可图 |
| 候选池筛选 | ✅ 完全够 | 同上 |
| 跨平台对照（Polymarket vs Kalshi）| ✅ 够 | 跨平台对照只看头部市场 |
| 死市场分布观察（oracle 风险源）| ⚠️ 不够 | full 多抓的 6k 死市场是研究目标 |
| LLM 训练样本库（含长尾 question）| ⚠️ 部分 | 元数据可以另写脚本只抓 Gamma 不调 CLOB |
| 全市场画像 / 学术研究 | ⚠️ 不够 | 但通常用 Subgraph 历史 trades 更合适 |

**核心论点**：**当前阶段策略目标 100% 覆盖在 subset 范围内**。IMDEA 论文 $40M
套利集中在头部市场也证实了这点 — 套利在长尾市场无现实可行性。

#### 2.7.d 时间错位影响差别

| 模式 | 9min 漂移分布外推 | 实测 / 估计 |
|---|---|---|
| subset 10min 拖尾 | 99.15% 一致（实测） | 信 |
| full 15min 拖尾（高流动性段） | ~98.7% 一致（外推）| 大致信 |
| full 多抓的 31k 低流动性市场 | **未实测** | **不可信** — 流动性低 = 单笔成交大幅拉价 |

#### 2.7.e 决策锁定（Phase 02 预设）

**L1 日常 cadence = subset**（`make snapshot-markets-v`，每天 1-2 次，10min）

**L1 周/月审计 = full**（`make snapshot-markets-full-v`，可选，15min）
  - 用途限定：死市场分布、新市场上市追踪、LLM 长尾样本库
  - **不**作为策略数据源使用

**全量"语义"统一**：今后 thread / SUMMARY 提"L1 日级全量"时默认指 subset。
需要语义精确时写明 "subset (high-liquidity)" 或 "full (54k)"。

**反模式（禁止）**：
- ❌ 用 full 当日常 L1 主力 — 拖尾窗口翻倍，没有相应价值
- ❌ "先跑一次 full 当基线再 subset" — 两次跑互不依赖，无此机制
- ❌ 拿 full 的长尾低流动性市场 mid 价当套利信号 — 数据不可信

#### 2.7.f 既往判断订正

- ❌ 我（Claude，5-11 早段）说过 "subset 元数据不全" — 字面看是对的（确实少 31k 行），但**对策略目标 100% 完整**，所以这个表述误导
- ❌ 我说过 "full 模式接近死代码" — 用户立场对：功能完整 + 维护成本 ≈ 0 就该留，**保留但限定用途**
- ❌ CLI 注释里 "~1-2 hours" — 实测 typical 15-20min，注释 outdated（Phase 02 顺手改）

---

### 2.8 OOM 实证与内存预算约束（2026-05-16 SESSION 19）

Phase 02 首次 prod deploy 在 Fly 256MB VM 上 OOM，复测 512MB 仍 OOM。Plan 02-09 streaming 改造 + 升 1GB 双管解决。这一节锁定经验与未来约束。

**实证数字**（Linux Fly daemon, $1k production threshold, ~6700 target_markets）:

```
[OOM kill log, 512MB VM, 2026-05-16 11:23:32 UTC]
  process: python (pid 647)
  total-vm:  871344 kB
  anon-rss:  402364 kB   ← 数据采集 daemon peak Linux RSS
```

**Fly VM 边际表**:

| Fly VM | User RSS 上限 (扣 kernel/container overhead) | 结果 |
|---|---|---|
| 256MB | ~150MB | OOM (SESSION 18) |
| 512MB | ~400MB | OOM (SESSION 19，撞 402MB 顶) |
| **1024MB** | **~900MB** | **稳态** (SESSION 19 验证) |
| 2048MB | ~1.9GB | 浪费 |

**Working set 拆解（estimate from code inspection）**:

- Python + pyarrow + httpx + sqlite + uvicorn + sentry + loguru baseline: ~120-150MB
- target_markets (6700-7000 × ~3.5KB stamped+book attached): ~25MB
- books_by_token + prices buy/sell/combined (~14k tokens): ~10MB
- market_to_event_map + seen_ids set: ~10MB
- pyarrow ParquetWriter C-allocator + 500-row batch buffer: ~10-15MB
- SQLite executemany batch + tx state: ~10-15MB
- **Linux glibc / C-allocator slack vs macOS**: ~80MB (意外项；macOS pytest 看不到)
- httpx HTTP/2 connection state + asyncio: ~10MB

**架构教训**:

1. **"HTTP 分页 ≠ 应用流式"** — Plan 02-04 误以为 paginator 已经是流式，实际 `_paginate` 内部 `out: list[dict]` 累积 20k dicts。真正流式要 `AsyncIterator[dict]` + 调用方 `async for` 逐项消费。
2. **streaming 单独不够** — raw 不累积省 ~160MB，但**数据本身的 working set (~240MB) 是不可压的**。streaming + 1GB VM 是双管必需。
3. **macOS pytest peak ≠ Linux Fly peak** — 差 ~80-120MB，Linux glibc 长跑 arena 保留是主因。本地 pytest 测过不代表 prod 够。
4. **"修代码不加内存"纪律有 caveat** — 代码已优化 + 真实数据 profile + 剩下 RSS 是数据本身 → 升一档是合理工程选择，不是逃避。详见 `memory/feedback_fix-code-not-config-2026-05.md` 更新版本。

**未来 phase 的硬约束（D-23 amendment in 02-CONTEXT.md）**:

- L1 daemon 任何新数据源接入 **必须 streaming-by-default**，不准累积全量 list
- `fly.toml memory = "1024mb"` 是 m1-perception phase 的**稳态决策**，不准回退
- 如果未来 RSS 超 600MB → 不要再升 2GB，而是**架构层面**改造（多进程拆分 / lazy CLOB fetch / events streaming 等）

**触发未来 plan 02-10 的条件**（写在 R5 风险里）:

- T6 docker smoke 观测到 target_markets > 10000 → CLOB phase 内存攻击成 next priority
- 1GB VM 仍 OOM → events streaming（Decision A defer 的部分）必做
- 数据采集成本敏感 → 多进程拆分 snapshot vs HTTP server

**相关 artifact**:

- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-09-PLAN.md`
- `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-09-SUMMARY.md`
- `docs/learning/08-streaming-snapshot.md`
- `memory/project_phase-02-OOM-resolution-2026-05.md`

---

## 1.6 chain-truth discipline (added 2026-05-26 from Phase 03 Inj L2-2 lesson)

> 与 §1（三层生产级判定标准）配套的纪律。判定标准告诉你"什么算生产级"，
> chain-truth 告诉你"怎么验证它真的是生产级，而不是 code-level OK 的幻觉"。
> 这条纪律在 plan-checker 会被引用。

**Rule:** Every fail-soft envelope MUST surface to `/health`（或同等可观测端点）。
代码层的 unit test 通过是**必要不充分**的 —— 必须把 `failure → telemetry surface → human-visible signal` 这条链在 plan 时端到端走通，不能等 chaos 时才发现链断了。

### 1.6.1 "chain-truth" 具体什么意思

1. **fail-soft 必有 /health 子检查对应**。任何形如 `try: ... except: log + breadcrumb + return False` 的代码路径，**必须**存在一个 `/health` 子检查（`mirror:l2_tob_age_seconds`、`ws:last_event_age_seconds` 等），它能把**持续的** False 状态转化为 HTTP 级别的 503/warn。
2. **子检查必须读真实数据源**。子检查读的字段（age timestamp / counter / flag）必须是 fail-soft 路径**真在 mutate** 的东西，不能 gate 在一个永远不会被翻转的 config flag（dead-code gate）上。
3. **plan-checker 必须在 plan 时走链**：问 "which check reads this?" 和 "which secret/flag gates the check?" —— 任一环节缺失 = plan incomplete，不许 lock。

### 1.6.2 Lived example — Phase 03, Inj L2-2

`L2SupabaseMirror.push_top_of_book` 有完美的 fail-soft envelope（try/except + Sentry breadcrumb + `return False`）。`/health` 也有 `mirror:l2_tob_age_seconds` 子检查。**但是**子检查 gate 在 `settings.l2_mirror_enabled` 上 —— 而这个字段从来没有在 `config.py` 里声明过。

结果：revoke 掉 prod 的 Supabase service key → daemon 继续报 healthy → operator 永远收不到告警 → 这条 5-layer dead code 直到 chaos 故意 inject 才暴露。

完整 5-layer RCA 与命中链：`.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-SOAK-LOG.md` Inj L2-2 段。

### 1.6.3 plan-template checklist（fail-soft 路径锁 plan 前必勾）

写新 plan、planner 在 `<must_haves>` 或 `<verify>` 里**必须**显式回答下面 5 项；plan-checker 在 review 时会逐项核查：

- [ ] 哪个 `/health` 子检查观察这条 fail-soft 路径？（file:line）
- [ ] 子检查读什么数据源？（file:line —— 必须是写入侧真在 mutate 的字段）
- [ ] 什么 config flag 门控子检查？该 flag 是否已在 `config.py` 声明？
- [ ] 写入侧成功 / 失败路径如何更新数据源？（file:line）
- [ ] 哪个 chaos test 端到端触发（不是 unit-level）？

### 1.6.4 与 §1 判定标准的关系

§1 表中"7×24 daemon，断网自愈，时序数据查询响应 < 1s"是**外部观察者视角**的判定。
§1.6 是**让这种外部判定真的成立**的代码侧纪律 —— 没有 chain-truth，§1 的判定只是 wishful thinking。

> Cross-ref: `feedback_code-vs-chain-truth-2026-05.md`（用户偏好原文）；
> Phase 03 LEARNINGS L1（meta-discovery 的 narrative 版）。

---

## 3. 现有工具栈在三层架构里的位置（重新归类）

> 这是骨架阶段的初步映射，调研后会调整。

| 现有工具 | 当前定位 | 三层归类 | 生产级缺口 |
|---|---|---|---|
| `make snapshot-markets-v` (subset, ~10min) | 高流动性活跃市场快照（~23k 行） | **L1 日常主力**（每天 1-2 次）| 缺：调度（无 cron）/ 失败告警 / 日志 rotate。详见 §2.7 |
| `make snapshot-markets-full-v` (full, ~15min) | 全市场快照（~54k 行，含死市场 + 小池子）| **L1 周/月审计**（可选） | 用途限定：死市场分布 / 新市场追踪 / LLM 样本。**不作策略数据源**。详见 §2.7 |
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

1. ~~**L1 频率**：1 天 1 次 vs 1 天 2 次~~ → 5-11 §2.7 锁定 subset 每天 1-2 次（实测 10min 可承受）+ full 周/月一次审计
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

- **m1-perception Phase 02 — 云原生 L1 长跑 + 一键部署链路 + 框架抽象 A 落地**
  - 云栈选型决策（基于 §2.6 调研产出）+ 部署蓝图落地
  - L1 调度迁移到云调度（不再是 macOS cron）+ 失败告警（Better Stack / Slack webhook）+ 日志聚合
  - 统一市场状态 dataclass（抽象 A）+ 现有代码迁移
  - SQLite → managed Postgres + TimescaleDB（如果 §2.6 决策为此） / 或继续 SQLite-on-volume（如果决策为此）
  - 监控网站雏形（最低限度：snapshot 状态 + 健康度 + 最近 N 次跑结果）
  - 一键部署脚本（`fly deploy` / `git push` / Makefile target）
  - 完成判定：从本地一键 deploy 到云上，连续 7 天无人值守正确产出 snapshot，所有失败 Slack 可见，dashboard 一打开就知道当前健康度

- **m1-perception Phase 03 — L2 定向跟踪 + 框架抽象 B 落地（云上）**
  - L2 daemon（同一云栈）订阅候选池 + watchlist，分钟级刷新
  - 候选池模型（L1 → L2 升级规则）
  - 监控网站扩展：候选池可视化 + 时序图
  - 完成判定：daemon 7×24 云上跑通，时序查询 <1s 响应，断网自愈

L3（WebSocket / OHLC）和事件总线（抽象 C）暂不预测，等 L1+L2 跑稳后再回头评估。

---

## 6. FAQ 增量区

（用户提问随时追加）

---

## Status / Next

**Status**: drafting (骨架完成 + 三大约束锁定：项目定位 / 抽象层 / 部署形态。待调研填充 §2.1-2.6)

**Next actions** (按"实证优先 / 架构决策次之 / 实现细节最后" 排序)：
1. ✅ 骨架先给用户看，对方向（2026-05-10）
2. ✅ 加入"框架启动期 + 生产级 + 工程可落地"约束（2026-05-10）
3. ✅ 加入"云原生部署 + 一键部署"约束（2026-05-10）
4. 进入 §2 调研循环：
   - **第一窗口（实证，1 小时）**：§2.1 时间一致性实情（读自己代码 + 跑实测）+ §2.5 生产级长跑缺口评估
   - **第二窗口（深度选型，2-3 小时）**：§2.6 云原生部署架构选型 — **本架构最重决策点**，要做矩阵对比，产出独立 thread `deployment-architecture.md`
   - **第三窗口（参考已有，1 小时）**：§2.4 看 `3th-party/polymarket-kalshi-weather-bot` 现成实现
   - **暂缓**：§2.2 + 2.3（WebSocket / K 线源 — 影响 L2/L3，框架启动期不需要）
5. 调研结果回写本文档对应章节 + 部署 thread
6. 产出"三层架构方案 v1 + 云栈选型 v1"双成果
7. 基于双成果开 Phase 02：云原生 L1 + 一键部署链路 + 抽象 A

**Related threads created from this one**:
- `.planning/threads/deployment-architecture.md` (待 §2.6 调研产出)

**Related**:
- `.planning/threads/market-microstructure.md` — Polymarket 微观结构基础（先决知识）
- `.planning/threads/market-structure.md` — 数据宇宙 4 层结构（API 层级）
- `.planning/threads/data-quality.md` — 已知数据质量问题
- `docs/research/polymarket-oss-landscape-2026-04.md` — OSS 项目调研报告

---

## RESEARCH UPDATE 2026-05-23 (Phase 03 pre-discuss research)

> 触发：Phase 03 (L2 Orderbook Tracking) discuss-phase 启动前，要求把 §2.2（WebSocket 能力）+ §2.6（DB 量级）两块从"待调研"变为"决策可拍"。来源以官方 docs.polymarket.com + 厂商 pricing 页 + 3th-party OSS 为准；研发期不臆测、不无源。

### §2.2 Polymarket WebSocket capability research

**调研路径备注**：
- `3th-party/polymarket-kalshi-weather-bot/` 调研后**不含**实际 Polymarket WS client 代码 — 该项目对 Polymarket 仅通过 Gamma REST 拉 markets（见 `backend/data/markets.py L37`，平台标 `polymarket`，但实际 fetch 是 `https://gamma-api.polymarket.com/events`），WS 仅出现在 RESEARCH.md L65/L79 的目录性提及。**不是可参考的 WS 实现样本。**
- `3th-party/clawfirm/` 套利模块为空壳（早已记录），其中的 `useWebSocket.ts` 是产品内部 dashboard 用 WS，与 Polymarket 无关。
- 因此本节 5 个问题全部以 **docs.polymarket.com (Mintlify 文档站，2026-05-23 取)** + Polymarket org GitHub issues 实证为权威源。

#### Q1: `/book` 通道是 per-token 订阅还是支持批量？一个连接最多订几个？

**Answer**: **支持批量**，且**一个连接订阅数量无硬上限（2025-05-28 起取消 100 token 限制）**。
- 订阅 payload 是 `assets_ids: string[]`（数组），单个 connection 一次性可订多个 token；channel type 是 `"market"`。
- 字段名注意：是 `assets_ids`（带 `s`），不是 `asset_ids`。
- Subscribe 后还能用 `{operation: "subscribe", assets_ids: [...]}` 动态加订，无需重连；`operation: "unsubscribe"` 同理可减。
- 2025-05-28 changelog 明确："The 100 token subscription limit has been removed for the Markets channel. You can now subscribe to as many token IDs as needed for your use case."
- 同期新增 optional 字段 `initial_dump`（default `true`）：subscribe 时是否要求服务器先 push 一个 full orderbook snapshot。

**Source**:
- https://docs.polymarket.com/market-data/websocket/overview（fetched 2026-05-23）— Subscribing 章节 + 字段表
- https://docs.polymarket.com/changelog（fetched 2026-05-23）— 2025-05-28 条目 "Websocket Changes"
- https://docs.polymarket.com/api-reference/wss/market（fetched 2026-05-23）— Subscription Request 示例

**含义**：L2 实现**不需要**做"多连接分片"。一条 WS 连接订满整个 watchlist（candidate-set ≤ 几百 token）是预期的官方使用模式。这把整个 §2.2"动态切换跟踪集"问题从"架构问题"降级为"应用层 subscribe/unsubscribe 一行代码"。

#### Q2: `/prices` 通道粒度是什么？事件级还是 token 级？

**Answer**: Polymarket WS **没有独立的 `/prices` 通道** — 这是 §2.2 问题里的概念误导。所有"price 变化"事件都走 `market` 通道的 event 流，**粒度按 asset_id（token）**：
- `price_change`（orderbook level delta，含 `asset_id` + `price` + `size` + `side`）— 粒度 = token level
- `last_trade_price`（trade execution，含 `asset_id` + `price` + `size`）— 粒度 = token level
- `best_bid_ask`（custom feature，含 `asset_id` + `best_bid` + `best_ask` + `spread`）— 粒度 = token level
- `book`（full snapshot）— 粒度 = token level

REST 侧的 `/prices` / `/prices-history` 是分开的 HTTP endpoint，不在 WS 范围。

**Source**: https://docs.polymarket.com/api-reference/wss/market — Messages 表（event_type 列）+ 各 event schema 都带 `asset_id` 字段。

**含义**：L2 价格事件全部 keyed by `asset_id` (= token_id = clob_token_id)；策略层做映射 `asset_id → (market, outcome=Yes/No)` 是必做工作（市场状态表必须缓存这层映射）。

#### Q3: 是否存在"某个 event 下所有 markets 一次订完"的快捷方式？

**Answer**: **没有** event-level 快捷订阅。WS 订阅参数只接受 `assets_ids: string[]`（market 通道）或 `markets: string[]`（user 通道，per condition_id）。
- 没有 `event_id` / `event_slug` / `tag` 作为订阅维度的 endpoint。
- 但**可一次订很多 token**（Q1 — 无上限），所以实际操作是：先用 Gamma REST `GET /events/{id}` 拿到该 event 下所有 markets 的 `clob_token_ids`（每个 binary market 2 个：Yes/No），然后把 token id list 一次性 push 进 `assets_ids` 订阅。
- 应用层封装一个 `subscribe_event(event_id)` helper 即可，3-5 行 code。

**Source**:
- https://docs.polymarket.com/market-data/websocket/overview — Subscribing payload schema（仅 `assets_ids` 一个维度）
- https://docs.polymarket.com/api-reference/wss/market — Subscription Request 示例只支持 `assets_ids`

**含义**：没有原生 "event subscribe"，但**完全不阻塞**架构。watchlist 数据模型仍以 token 为单位（与 L1 已有的 token-centric 设计一致）。

#### Q4: 速率限制 / 连接数限制 / 重连策略？

**A. WS 连接侧**（与 REST rate limit 独立）：
- Heartbeat 强制：market/user 通道 client 必须每 10 秒发 `PING`（plaintext "PING"），server 回 `PONG`。**不发心跳 ≈10s 后服务器断连**（docs 显式列为 troubleshooting 项 "Connection drops after about 10 seconds"）。
- 连接 timeout：subscribe 必须在连接建立后立即发送，否则服务器关掉连接（"Connection closes immediately after opening"）。
- 已知问题（生产侧 bug，2026-03 仍 open）：偶发 "WSS server accepts connection + subscription but sends no messages" 状态——TCP 没断、PONG 还在回，但 event 流冻结。需要 client 侧做"业务层 staleness 检测"（>N 秒无业务消息则强制重连），不能依赖 TCP-level keepalive 单一信号。
- **没有公开的"单 IP 最大并发 WS 连接数"上限文档**，但 OSS 实现（如 `Polymarket/real-time-data-client`、`GoPolymarket/polymarket-go-sdk`）的常规做法是单进程一条 WS 连接复用所有订阅，符合 Q1 取消 100-token 限制后的官方推荐路径。

**B. REST 侧（用于 fallback / discovery / history）**：Cloudflare throttle，按 endpoint 分（全部 sliding window，限速时 delay 不 reject）：
| Endpoint | Limit |
|---|---|
| 全局 general | 15,000 req / 10s |
| Gamma `/events` | 500 req / 10s |
| Gamma `/markets` | 300 req / 10s |
| CLOB `/book` | 1,500 req / 10s |
| CLOB `/books`（批量） | 500 req / 10s |
| CLOB `/prices-history` | 1,000 req / 10s |
| CLOB `/trades`（ledger） | 900 req / 10s（合订 `/orders` `/notifications` `/order` 共享） |
| Data API `/trades` | 200 req / 10s |

**C. 重连策略**（基于 docs + GH issues + OSS 客户端模式）：
- 推荐：业务层 staleness watchdog（举例 30s 无任何 event → close + reconnect）+ 指数退避（1s, 2s, 4s, capped 30s）+ 重连成功后**重新 subscribe**（服务器不持久化订阅状态）。
- `initial_dump: true` 在重连后非常关键 — 否则只能等下一笔 `price_change`，期间 orderbook 是错的。

**Source**:
- https://docs.polymarket.com/market-data/websocket/overview — Heartbeats / Troubleshooting
- https://docs.polymarket.com/api-reference/rate-limits（fetched 2026-05-23）— 完整 rate limit 表
- https://github.com/Polymarket/py-clob-client/issues/292（2026-03-05 open）— "Server accepts connection + subscription but sends no messages" 实证：业务层心跳必须独立于 TCP keepalive
- https://docs.polymarket.com/changelog — 2025-05-28 `initial_dump` 字段添加

#### Q5: 历史 trades 有 REST 接口吗（深度多深）？还是只能从 WS 流自累积？

**Answer**: **有，三条独立 REST 路径，各有侧重**：

| 来源 | Endpoint | 深度 / 限制 | 用途 |
|---|---|---|---|
| Data API（公开，无 auth） | `GET https://data-api.polymarket.com/trades` | 全市场聚合 trades；`limit` max **500**，`offset` max **1,000**（2025-08-26 收紧）；rate 200 req/10s；支持 `takerOnly=true` 等过滤；按 market/user 都能查 | 全市场历史 trades 回溯（offset 上限是硬伤，深历史要靠时间窗 + 多次请求拼） |
| CLOB Ledger（auth） | `GET https://clob.polymarket.com/trades` | 用户自己的成交；900 req/10s | 自己交易后的 reconciliation |
| CLOB Market Data | `GET /prices-history` | 单 market `interval` ∈ {`1h`, `6h`, `1d`, `1w`, `max`, `all`, `1m`}, `fidelity` 默认 1 分钟；返回 `{t, p}` 时序点 | OHLC-like 历史价格（**不是 trades**，是 price ticks 聚合） |

**已知陷阱**（项目侧风险点，要写进 Phase 03 CONTEXT）：
- `prices-history` 对**已结算 (resolved/closed) 市场只返回 12+ 小时颗粒度**的数据，即使该市场曾是高交易量（py-clob-client issue #216，2025-12-22 仍 open）— 意味着事后回测 closed 市场的细粒度价格历史**不可靠**，必须 Phase 03 一开始就开 WS 实盘累积自己的 trades 流。
- Data API `/trades` offset 1000 上限意味着深回溯（>1000 笔以前的 trade）必须按时间窗倒序滑动，不能纯 offset paginate。

**WS 自累积建议**：Phase 03 L2 应**两条腿走**：
1. REST 一次性 backfill（启动 / 重连）— 用 `/prices-history` 拉过去 7-30 天分钟级 price points 作 baseline
2. WS 实时累积 `last_trade_price` 事件持久化到 L2 时序表 — 这是事后回测和策略 backtest 的真数据源

**Source**:
- https://docs.polymarket.com/api-reference/rate-limits — Data API + CLOB 分表
- https://docs.polymarket.com/changelog — 2025-08-26 "Updated /trades and /activity endpoints" 条目（limit 500 / offset 1000）
- https://docs.polymarket.com/api-reference/markets/get-prices-history — query params
- https://github.com/Polymarket/py-clob-client/issues/216（2025-12-22） — closed markets 12h 颗粒度退化

---

### §2.6 DB tier selection research (L2 量级)

**项目侧基线事实**：
- 当前用 Supabase Free（`.env.example L21-25` 已含 `POLYARB_SUPABASE_URL` / `_DB_DSN` / `_SERVICE_KEY`，Phase 02 实际用的就是 Free 项目）
- Phase 02 已踩坑：7 天无活动 auto-pause 是 soak-gate-deviation 的根本原因（详见 `threads/soak-gate-deviation-2026-05.md`）
- L2 工作负载：~72k 行/天 × 365 天 = **~26M 行/年**，单表存量到年底约 **2-4 GB**（含索引），属于"小型时序数据"区间

#### A. 各 DB option 对照表（all prices USD，fetched 2026-05-23）

| 维度 | Supabase Pro | Supabase Free（现状） | Neon Launch | Fly MPG Basic | Fly Volume 自管 PG |
|---|---|---|---|---|---|
| **月费起步** | **$25** + $10 compute credit 内含 | $0 | ~**$5-15**（usage-based: 100-300 CU-hr × $0.106 + storage $0.35/GB-mo） | **$38** | **~$10**（VM $5 + 10GB volume $1.50 + 自管 PG image） |
| **Idle 自动暂停** | **❌ 不暂停**（Pro 显式取消 pause 行为） | ✅ **7 天无活动暂停**（root cause of soak-gate-deviation） | ⚠️ Auto-suspend 到 zero compute（可配置；Launch 默认 idle 后 scale-to-zero，再访问 350ms 冷启动） | ❌ 不暂停（一直常驻 VM） | ❌ 不暂停 |
| **DB 存储（L2 量级足够）** | 内含 8 GB；overage $0.125/GB-mo | 500 MB（年底 2-4GB 撑不到） | 内含 10 GB；overage $0.35/GB-mo | 自购 storage $0.28/GB-mo（10GB = $2.80） | Fly Volume $0.15/GB-mo（10GB = $1.50，便宜很多） |
| **Compute** | 2-core ARM Micro (1 GB RAM)；含 $10 compute credit | Shared CPU / 500 MB RAM | 0.25-2 CU autoscale (1-8 GB RAM per CU)，按 CU-hr 计 | Shared-2x / 1 GB RAM（HA cluster 内含） | 1× shared CPU / 256 MB-1 GB RAM | 
| **HA / backup** | PITR add-on $100/mo（不含）；自动每日 backup 内含 | 每日 backup 7 天保留 | PITR via instant-restore 内含（1-day history default） | HA cluster + 自动 backup + failover 内含 | 自己写 cron `pg_dump` |
| **Auth/SDK 集成成本（项目当前已用 Supabase Auth）** | 零（in-place 升级） | — | 中（要重接 auth；Neon Auth ≤60k MAU 免费但不是 Supabase Auth 兼容） | 高（无 hosted auth；要么继续用 Supabase Auth 跨厂商 + 数据在 Fly PG，要么自建） | 高（同上） |
| **同 region 部署（polyarb-l1 在 fra）延迟** | Supabase region 可选；当前 Dublin/eu-west-1，跨 region 几十 ms | 同 | Neon 全球 region，可选近 polyarb-l1.fly.dev 的 region | ✅ Fly 同 organization 私网；几 ms | ✅ 同 |
| **迁移成本**（从 Supabase Free） | ✅ **零停机 in-place upgrade**（Pro 升级在 dashboard 一键，DB 不动） | — | 中（pg_dump + restore；改 DSN + 改 supabase-py → asyncpg/psycopg；auth 体系迁移） | 高（DB + auth 都改） | 高 |
| **TimescaleDB 扩展** | ❌ 不在 Supabase 默认扩展列表（要 self-hosted；可 fallback 用 BRIN/partial index + 分区表） | ❌ 同 | ❌ 不在 Neon 扩展列表 | ❌ Fly MPG 仅 default PG16 trusted ext + `pgvector` + `PostGIS`（明文列；no Timescale） | ✅ 可装 |
| **L2 量级 fit-for-purpose** | ✅ 充分（8 GB 存量足够 2 年；compute 微负载） | ❌ 7 天暂停硬伤 + 500 MB 存量不够 | ✅ 充分；scale-to-zero 反成本（小负载 ~$5-10/mo） | ⚠️ 充分但**贵**（$38 起 + storage 单价 2× Fly Volume） | ✅ 充分**且最便宜**，但运维成本付现 |

**Source**:
- Supabase Pro: https://supabase.com/pricing（fetched 2026-05-23）; https://aiagencyplus.com/keep-your-supabase-free-tier-project-live-past-the-limit（"The Pro plan currently $25/month removes the project pause behaviour entirely"）
- Supabase Free pause 政策: https://supabase.com/pricing "Free projects are paused after 1 week of inactivity"; https://supabase.com/docs/guides/deployment/going-into-prod "Upgrade to Pro to guarantee that we won't pause your project for inactivity"
- Neon pricing: https://neon.com/pricing（fetched 2026-05-23）— Launch plan $0.106/CU-hr, scale-to-zero default, 10 GB storage included on Launch; storage $0.35/GB-mo paid plans
- Fly MPG pricing: https://fly.io/docs/mpg/（fetched 2026-05-23）— Basic plan $38/mo (Shared-2x/1GB), Starter $72; storage $0.28/GB-mo; only `pgvector` + `PostGIS` 3rd-party ext
- Fly Volume pricing: https://kuberns.com/blogs/flyio-pricing（cited Fly docs）— Volume $0.15/GB-mo

#### B. TimescaleDB 必要性评估

**结论**：26M 行/年（72k 行/天）**不需要 TimescaleDB**。
- 单表 26M 行在原生 PG 16 上配合 BRIN 索引（时间列）+ 普通 btree（asset_id）就是 sub-100ms 查询的工作负载（参考 `medium.com/@vbahadircan/what-10-million-rows-taught-me-about-tuning-postgresql` — 10M 行在调优 PG 上 sub-second）。
- 真要 partitioning：原生 PG declarative partitioning（按月分区，12 个 child table/年）覆盖 99% 时序需求，不引入 Timescale 依赖。
- 触发 Timescale 升级的真实阈值通常在 **>1B 行 或 >100GB** 时序数据 — L2 单 watchlist 模型至少 30+ 年才到，远超本项目时间尺度。

**Source**: Neon/Supabase/Fly MPG 全部不在默认扩展集合中包含 Timescale（厂商文档已 cite），意味着选 managed 时 Timescale 不是免费选项；这同时也确认了"对小型时序工作负载，Timescale 不是必需"是业内共识（厂商不在 default 集合 = 不是主流小项目刚需）。

#### C. 推荐排序（按 Phase 03 实际诉求权重：稳定性 > 迁移成本 > 价格）

1. **Supabase Pro $25/mo** — **首选**。理由：(a) 一键 in-place 升级零迁移成本，(b) 立即根除 7-day pause 这个 Phase 02 已证的坑，(c) 8 GB 存量 + Micro compute 覆盖 L2 2 年以上量级，(d) Supabase Auth 投资保留（dashboard、RLS、service_role key 流程全继承）。$25/mo 的"价格不便宜"在这个项目阶段不是 blocker（用户 §0.2.1.a 明确接受 ~$25/月 单服务预算）。
2. **Neon Launch ~$5-15/mo** — **次优**。理由：实际 L2 工作负载（每分钟一次 query + 写）在 Neon scale-to-zero 模式下账单极低；但迁移要重接 auth，且 L1 dashboard 已是 Supabase 生态，**迁移成本不抵省下的钱**。除非未来要做"按需冷启动 + 偶发查询"型负载（不是 L2 形态），否则不推荐切换。
3. **Fly Postgres + 自管 Volume ~$10/mo** — **不推荐**。理由：和 polyarb-l1 同区私网延迟优势对 1 分钟一次的 L2 写入零意义；自管 PG 的运维成本（backup / 版本升级 / 故障处理）只有时段套利者愿意付，研发期付这个税亏本。
4. **Fly MPG Basic $38/mo** — **不推荐**。比 Supabase Pro 贵 50%，又失去 Supabase Auth 集成，无任何对应优势。

---

### Summary recommendation for Phase 03 discuss

> **Recommendation: WS vs REST — hybrid (WS 主 + REST backfill)** because Polymarket 官方 docs 已明示 WS market 通道支持单连接订阅无上限 token、`initial_dump` 字段、动态 subscribe/unsubscribe，但同时存在"silent freeze"已知 bug（py-clob-client issue #292）— 业务层 staleness watchdog 必须做，REST `/prices-history` + Data API `/trades` 兼任"启动 backfill"和"WS 冻结时 fallback"双角色。**DB — Supabase Pro $25/mo** because in-place upgrade 零迁移成本 + 直接根除 Phase 02 已证的 7-day idle-pause 风险 + 保留现有 Supabase Auth 投资，是唯一"工程纪律"侧零风险的路径；Neon 便宜但迁移税不划算，Fly Postgres 贵且无对应优势。**These two together make candidate-set max ~200 markets × 1-min interval feasible** within $25/mo total（compute 余量大），且**有清晰的扩容路径**（订阅数翻倍仍是单 WS connection；DB 存量到 8GB 上限触发再加 compute add-on 或迁 Neon Scale）。

**残留不确定**（写进 Phase 03 CONTEXT 的"未决/已知风险"）：
1. WS "silent freeze" 已知 bug 的发生频率 — docs 没量化，社区 issue 个案性；Phase 03 必须把 watchdog 阈值（建议 30s 无业务消息触发重连）和 reconnect 后的 idempotent re-subscribe 写为 plan 一级 acceptance criterion。
2. `prices-history` 对 closed markets 12h 颗粒度退化 — 意味着事后回测能力不能依赖 REST history，必须 Phase 03 起就 **WS-accumulate own trades** 到 L2 表。这反过来强化"WS 主 + REST 补"的混合架构。
3. Supabase Pro Micro compute (1 GB RAM) 在 L2 负载下绰绰有余，但 Phase 05+ 若加 L3 OHLC 聚合 + dashboard 复杂查询，可能需要 compute add-on（$10-30/mo 升 Small）。不是 Phase 03 立即决策点。

---

### §2.7 M1→M2 neg-risk completeness contract (2026-07-17)

- subset snapshot 不能只按 liquidity 过滤；任何带 `neg_risk_market_id` 的 active sibling
  都必须保留，否则漏掉低流动性腿会制造假的 `sum(asks) < 1`。
- scanner 只对同一个 fresh snapshot 运算；任一 sibling inactive/closed/incomplete、缺 ask
  或缺 ask size 时整组 fail closed。
- 第一条产品路径只发布 buy-all YES，capacity=`min(best_ask_size)`，收益明确标记
  `gross-before-fees`；费用后 edge 和持续性证据属于下一质量门。
- 真实资金 adapter 不在这个 read-only contract 内。

---

### §2.8 Quiet refresh protocol and evidence contract (2026-07-18)

Plan 04 production established a sharper distinction between subscription
transport activity and an orderbook resnapshot:

- `subscribe(existing_asset, initial_dump=true)` is not a resnapshot operation.
  Polymarket documents `initial_dump` on subscription, while its official client
  multiplexes duplicate asset subscriptions without sending another wire request.
  Production agreed: duplicate subscribe logged send/request but produced no book.
- A real in-band resnapshot therefore needs an absent→present edge:
  `unsubscribe(asset_ids)` followed by `subscribe(asset_ids, initial_dump=true)`.
  The pair must preserve candidate/L3 desired state; it changes wire membership,
  not business ownership.
- Send completion is only transport truth. Refresh success requires receive-side
  `book` evidence and a successful mirror write. Neither the quiet-refresh sender
  nor an optimistic local subscription set may advance freshness.
- Quiet refresh, candidate reconciliation, L3 promotion/demotion, and reconnect
  must share one subscription-control/reconnect gate. WebSocket concurrent-send
  safety does not protect semantic ordering. Partial send, cancellation, timeout,
  or connection-generation change makes wire membership uncertain and must force
  convergence from the latest desired union.
- Production validation is trigger-sensitive. Instance
  `01KXSMS80B5AX2FGT5EPRC6V82` stayed naturally active for an extra ten minutes,
  never reaching even 45 seconds of WS age; therefore no natural 60-second quiet
  trigger occurred. An active market cannot validate the quiet edge, but this is
  not evidence that the edge failed.
- Evidence is instance-bound. If the deployed instance changes before the natural
  quiet proof, first rebuild the >=180-second healthy startup/candidate/WS/mirror
  baseline, then observe quiet trigger→book receive→mirror success on that instance.
- 2026-07-20 follow-up exposed a new open chain-truth question on the same instance:
  strict health showed WS age `0.2s` while mirror age was `6306.2s`, and rolling
  logs had no received-book debug or TOB/trade mirror success. Before another
  quiet-window monitor, identify which frame types advance the coarse WS freshness
  clock without producing mirror-eligible data; do not treat a fresh aggregate WS
  age as proof that the book→mirror business chain is live.
- The same-instance follow-up later supplied the missing natural edge without a
  code or cloud change. At `05:32:41.823Z` the daemon logged quiet `sending`, at
  `05:32:42.567Z` Supabase accepted `l2_top_of_book` with HTTP 201, and at
  `05:32:42.684Z` the same-generation waiter logged `evidenced`. The following
  198-second window stayed below the unchanged WS/mirror fail thresholds with
  cursor lag zero and identical machine/instance/image anchors. This proves the
  quiet-refresh mechanism without claiming Phase closure; the two clocks remain
  intentionally separate.
- A later fresh probe exposed a different upstream failure: `markets_latest`
  returned HTTP-success with zero rows even though snapshot 573 remained valid
  with 1939 markets. Candidate refresh accepted the empty response, replaced the
  3-asset desired set with zero, and thereby prevented quiet refresh from asking
  for new books. New chain discipline: **zero remote projection is not zero real
  universe**. Writers reject empty rows before mutation; readers freeze LKG,
  freshness, membership, and cursor until a non-empty projection returns.
- The writer was not Fly: mocked M1 orchestrator tests inherited production
  credentials through `SettingsConfigDict(env_file=".env")`. A unit test can be
  deterministic locally yet externally destructive unless every cloud adapter
  is explicitly disabled. The repository-wide test fixture now defaults
  Supabase, R2, event bus, Sentry, Better Stack, and Telegram to off; opt-in
  tests use localhost/dummy settings or a Testcontainer DSN.
- Snapshot 574 then naturally restored 1942 projection rows. The unchanged L2
  instance recovered three candidates and passed a new 258-second strict window
  10/10 with cursor lag zero. Because mirror age reached 565.4s near its 600s
  failure boundary, validation continued until a real TOB 201 reset the clock;
  this prevents a threshold-adjacent false pass.
- Phase 05.1 can close without deploying the new guards because the production
  mutation source was the local test process, now contained, and the unchanged
  deployed chain naturally re-proved recovery. The guard remains explicitly
  "locally verified, undeployed defense-in-depth," not production evidence.

### §2.9 L3 prerequisite contract: seed coverage before strict promotion (2026-07-20)

- A strict L3 recipe cannot create its own observation input. If L2 candidate
  recipes select only near-expiry extreme-price markets, L3 sees only those
  books and `spread < 0.02` can remain unreachable even when the global market
  universe contains hundreds of mid-band liquid markets.
- Current evidence: three active L2 candidates all came from `near-end` in one
  event; recent spreads were about `0.998` or incomplete. The current universe
  had 598 mid-band rows and 583 mid-band rows with liquidity >=500. This is seed
  starvation, not a zero-opportunity universe.
- The five-market→ten-token contract also requires both outcome token IDs in the
  durable projection. Production `markets_latest` exposes `market_id` and
  `yes_token_id` only; querying `asset_id/no_token_id` is a schema-contract bug.
- Repair ordering: first make token-pair identity durable and testable; then add
  a bounded L2 seed selected from snapshot facts; only then calibrate/validate
  the unchanged L3 spread/depth recipe on observed TOB data. Lowering L3
  thresholds before supplying representative input confounds two variables.
- Read-only feasibility evidence supports a seed cap of 100: the 100 most liquid
  rows with `mid_price BETWEEN 0.1 AND 0.9` and `liquidity_usd >= 500` yielded
  100 complete public-CLOB books, 86 of which already satisfied the unchanged
  `spread < 0.02` and Yes top-10 depth > $500 recipe. Two later smaller probes
  timed out during TLS handshake, so bounded diagnostics stopped without using
  transport failure to overturn the successful sample.
- The seed is an observation-coverage mechanism, not a promotion shortcut. It
  must remain under the existing candidate cap and the promoter must fail closed
  on incomplete Yes/No identity; only ten distinct tokens may satisfy N=5.
- Phase 05.3 implemented this contract locally under TDD. Its dry-run now guards
  every WS, Supabase, LKG, active-set, and freshness mutation; a proposal is not
  post-run state. Full local gates passing does not update the production chain:
  Alembic 006, deployment, `10/10`, real book-level writes, and soak evidence are
  still distinct authorization/evidence steps.
- The 2026-07-20 rollout supplied those first four production steps and exposed
  two further ingestion truths. Polymarket book arrays can be farthest-first, so
  TOB/depth/full levels must rank BUY descending and SELL ascending rather than
  trust index zero. Recent TOB is a time series, so a recipe `limit: 5` must run
  after newest-row-per-asset collapse or duplicate snapshots under-fill distinct
  markets. Release 36 reached five markets/ten tokens at startup but fell to
  four/eight on its next tick because its newly subscribed No-side TOB row fed
  back into the Yes-market recipe limit. The third boundary is therefore outcome
  identity: resolve `markets_latest.yes_token_id` before LIMIT, not after. Exact
  release 37 then stayed five/ten across the second real tick and wrote 280
  release-local depth rows. This proves feedback-loop reachability, not 24-hour
  stability.
- Soak coverage is also an interval-query contract. At handoff, the newest 1000
  PostgREST book rows contained only four hottest assets, while direct SQL over
  the identical release interval counted 3840 rows across all ten token assets.
  Never infer distinct-asset coverage from a capped time-series page; aggregate
  in SQL over the exact T+0/T+24 window, then map the ten token IDs back to the
  five promoted Yes-market identities.
- A later formal-start attempt proved that `active_count=10/10` plus root HTTP
  200 is still not a sufficient T+0 predicate. On the unchanged release 37
  identity, WS, mirror, cursor, candidate, and promoter checks passed while
  `l3:last_book_levels_write_at_s=207.2s` remained warn against the strict 120s
  limit. The candidate start was rejected rather than stitched to an earlier
  green sample. T+0 is therefore a same-sample conjunction across identity,
  membership, and the dedicated book-level freshness clock.
- The accepted retry also showed why identity must be resolved after the
  promoter tick, not merely near it. A pre-baseline read included market
  `908713`; the `13:30:35Z` tick then applied `+2/-2`, and the authoritative
  post-tick five-market mapping changed before the strict `13:30:55Z` sample.
  The formal soak therefore binds its T+0 health and SQL coverage to that
  post-tick mapping. Initial one-market coverage is retained as an honest
  boundary reading, not treated as an early D-12 failure or backfilled to 5/5.
- The first formal soak then exposed an operational evidence failure rather than
  a proven daemon failure: T+0 was valid, but T+6/T+12/T+18 were never captured
  and T+24 was overdue at handoff. Raw SQL can reconstruct interval coverage,
  but a later health probe cannot reconstruct `min(observed active_count)` at
  missing times. Chain-truth therefore includes observation scheduling itself;
  this run is NOT-CLOSED and must be re-soaked after a labelled late diagnostic.
- The late diagnostic showed a second retention boundary. Direct SQL could
  reconstruct 48,940 book rows across all ten tokens/five markets and 732
  Yes-side OHLC rows across all five markets, but Fly's 100-row rolling log
  buffer began after the formal window. A missing historical watchdog interval
  is `unavailable`, never an inferred zero. The first re-soak candidate then
  returned 10/10 and fresh WS/mirror/candidate/cursor links in one body while
  dedicated book freshness was 253.8s. It was rejected without stitching an
  earlier green book sample. Observation retention and same-response conjunction
  are both parts of the production truth contract.
- The accepted re-soak added an operator-level identity distinction: Fly's
  `Fly-Force-Instance-Id` routing header takes the hexadecimal machine ID, while
  the ULID incarnation remains an independent before/after identity anchor. An
  HTTP 400 with no health JSON is transport rejection, not a gate sample. After
  a natural book write, the corrected request passed every locked gate in one
  body and established a separate immutable T+0; the old incomplete window was
  not merged into it.
- That second T+0 exposed a deeper acceptance-contract flaw before T+6:
  **review cadence is not observation cadence**. Six-hour spot checks cannot
  prove roughly 72 intervening promoter ticks, WS add/remove outcomes,
  watchdog/reconnect decisions, or five independent market freshness chains.
  Latest-state process anchors and a global book clock are diagnostic surfaces,
  not interval history. With user approval, the release-37 window is therefore
  diagnostic-only. Strict Phase 05 validation must first add durable per-tick
  promoter/runtime ledgers, truthful desired/control-committed/business-evidenced
  membership, 30-second process/per-market samples, and exact-window aggregate
  verdicts. Six-hour checkpoints remain human summaries. Design contract:
  `docs/superpowers/specs/2026-07-22-m1-continuous-l3-soak-evidence-design.md`.

### §2.10 Continuous-evidence implementation closure (2026-07-23)

- A sample sequence is a **boot-grid slot identity**, not an attempt counter.
  If `boot+30s` is skipped and the loop resumes at `boot+60s`, the persisted
  identity is `(sample_seq=2, scheduled_at=boot+60s)`. This preserves the gap
  without permanently offsetting a later formally chosen T0.
- `max_sample_gap <=75s` does not imply 30-second completeness. Verdicts must
  derive every expected `(sample_seq, scheduled_at)` in `[start,end)` from the
  boot and compare exact schedule equality; actual `sampled_at` remains the
  independent gap/freshness clock.
- A promoter row is not trustworthy merely because its business timestamps look
  historical. PostgreSQL and offline verdicts now require
  `scheduled_at <= started_at <= finished_at <= recorded_at < finished_at+30s`,
  so a missing tick cannot be healed hours later.
- Deploy path coverage is reachability, not authorization. Eligible pushes may
  expose a workflow, but the production deploy job itself is gated to explicit
  `workflow_dispatch`; Plan 05 still owns migration/credentials/deploy approval.

### §2.11 Production continuous-evidence chain corrections (2026-07-24)

- Configuration identity is part of chain truth. The L2 daemon's direct
  PostgreSQL paths use `l2_runtime_db_dsn`; a health subcheck that instead reads
  the migration-owner field can report `not_configured` while reconciliation
  succeeds. Health now reads the same runtime credential source, and a poisoned
  owner-only regression test preserves the boundary.
- An image label does not prove the boot ledger. GitHub/Fly `GH_SHA` and the
  database `release_id` are separate consumers; the workflow must inject the
  dispatched SHA into `POLYARB_RELEASE_ID`, then production must compare all
  three identities before readiness.
- Event-loop timeout is not wall-clock authorization. A timer can wake
  fractionally before an exact UTC boot-grid boundary; emitting immediately can
  violate `scheduled_at <= started_at` and lose the append-only terminal row.
  The scheduler now rechecks wall time after every timeout until the boundary is
  actually reached.
- Global activity cannot prove per-market freshness. With 100 candidate tokens,
  unrelated frames kept the global WS clock hot while four committed L3 markets
  aged beyond 120 seconds. Quiet refresh now keys off every current-generation
  committed-L3 evidence timestamp; candidate traffic cannot mask a stale L3
  member, and fresh L3 evidence avoids unnecessary refresh.
- All three defects were discovered before manifest creation. Their production
  boots remain diagnostic-only; the immutable 24-hour clock starts only from a
  later boot that passes the two-promoter/12-sample readiness gate.
- A view's grouping key is not automatically a freshness clock.
  `l2_ohlc_1m.bucket_ts=date_trunc('minute', source.ts)` labels the bucket and
  can be almost 60 seconds older than its latest member. Strict OHLC freshness
  now reads the latest non-null base observation from `l2_top_of_book`; exact
  interval coverage still reads the OHLC view. This separates “is the source
  current?” from “did every required aggregate bucket exist?”
- Startup absence is not writer failure. Promoter and sampler are sibling
  tasks, so sampler seq 0 can run before the first ten-token desired mapping
  exists. That precondition now skips the grid slot without evidence; after
  desired reaches exactly ten, membership non-convergence remains a failed
  sample and every collection/write exception remains a disallowed event.
  Readiness plus a future exact-grid T0 prevents skipped startup slots from
  weakening the soak.
- WebSocket protocol Ping and venue application heartbeat are different chain
  links. `websockets.connect(ping_interval=10)` emits RFC control frames, while
  Polymarket CLOB requires text `PING` every ten seconds and returns text
  `PONG`. A5's durable seq 201/216/217 captured only 7/2/7 current-generation
  evidenced tokens during repeated 20–30-second generation churn, permanently
  rejecting the interval before T+6. The repair belongs to the socket owner:
  one cancellable text-heartbeat task per connection and PONG filtering before
  JSON decode. Sampling thresholds and reconnect-adjacent failure semantics
  stay unchanged; recovery requires a new exact deployment/boot/A6 clock.
- Sibling task creation is not startup ordering. Release 68 started promoter
  run 0 against generation 0 before the WS initializer durably published
  generation 1, so the terminal `generation_changed` row correctly made that
  boot ineligible. The structural repair is a one-time, cancellable promoter
  gate over `WsConsumer.has_active_connection`, whose source of truth is the
  successfully initialized `_current_ws`. The gate preserves run 0's
  boot-anchored `scheduled_at`, emits no row if shutdown wins, and does not
  relax any post-start generation-change failure. A failed release is never
  reinterpreted or restarted into eligibility; readiness begins again on a new
  exact release and boot.

### §2.12 Control ambiguity is not business-evidence absence (2026-07-25)

- A6 seq 35 durably failed at desired/committed/evidenced `10/10/8`; both
  tokens for market `562802` lacked current-generation depth-write evidence.
  Its T0 remains an immutable PASS, but the interval is permanently
  NOT-CLOSED and no T6/T12/T18/T24 artifact may be created.
- The old quiet-refresh exception path classified a final missing book exactly
  like an unsubscribe/subscribe send or identity failure and compensated the
  whole generation. The new generation then reached the quiet gate before
  initial evidence convergence, creating self-sustaining refresh/reconnect
  churn. This is a feedback-loop defect, not permission to ignore the failed
  sample.
- Chain-truth now distinguishes two failure domains. Control-send failure,
  generation drift, and cancellation still make wire intent ambiguous and
  force compensation. A same-generation successful final subscribe followed
  by missing business evidence retains the socket, records the exact missing
  identities in process state, and returns failure without advancing any
  freshness clock.
- Refresh uses one full first control cycle, an 8-second evidence interval,
  then one missing-only control cycle inside the unchanged 25-second total
  budget. A later organic successful depth write removes its identity from the
  generation-scoped retry set; the next due attempt prefers only the remaining
  set.
- A newly initialized connection gets the existing 60-second quiet interval to
  converge before this mechanism runs. The sampler, AcceptanceConfig, strict
  `10/10/10`, and 30-second ledger cadence are unchanged.
- Candidate `3be6ef6a8ceed8517020506291d474c13a6f6bc0` passed 40 focused
  control/evidence tests, 209 full L2/L3 focused tests, and the full repository
  suite. Changed-file Ruff, compileall, docs, planning, exact release-70 image
  required-`python`, and diff gates passed. This is local qualification only;
  A7 requires a clean exact-SHA deploy, a new boot, and repeated successful
  quiet cycles before manifest binding.

### §2.13 Evidence monitors must share the verifier's exact interval (2026-07-26)

- A7's canonical scope is `[T0,T24)`. It passed with 2,880 health samples,
  14,400 market samples, 288/288 promoter ticks, one mapping/config/boot, and
  an independently reproduced raw-row hash.
- A convenience monitor queried every row since T0 and saw a second mapping
  after the formal boundary. Exact SQL placed the first different promoter
  mapping at `T24+30s`; it was not part of the soak.
- Operational monitors that judge an immutable interval must copy both bounds,
  timestamp column, and inclusivity from the canonical verifier. A broader
  live query may be useful for current health, but it cannot reject a sealed
  historical interval.
- Availability failures are separate from verdict failures. A timed-out
  read that creates no O_EXCL report may be retried exactly; a persisted
  canonical NOT-CLOSED artifact remains immutable.

### §2.14 A durable consumer needs a producer on the same truth path (2026-07-26)

- Deploying a quote store, scanner, and HTTP route did not make the opportunity
  feed live. With no complete quote-run producer, HTTP 503 `quote run
  unavailable` was the only truthful answer; it was never a zero-opportunity
  result.
- Process placement is part of chain-truth. The L1 app reads
  `/data/state.db`; the Fly cron machine has no `/data` volume. A cron-side
  collector could succeed while remaining permanently invisible to the HTTP
  consumer. The producer therefore belongs in the volume-owning L1 app process
  unless storage is deliberately moved to a shared service.
- Producer and consumer clocks stay independent. The worker attempts every
  120 seconds, health warns at 240, and the public scanner rejects quotes after
  300. A scheduling fix must not weaken the consumer SLA to hide missed runs.
- Production capacity must precede cadence commitment. The 1,278-token universe
  required three configured CLOB batches and completed in 1.013 seconds; runs
  2→3→4 then proved 1,278/1,278 continuity at about 121-second start intervals.
- Durable complete-run age is the success truth; process-local collector state
  explains the current attempt. A recent complete run plus a transient worker
  error is warn, while no complete run, unreadable storage, or age over 300
  seconds is fail-closed.
- Release identity is also a health-chain link. The older L1 workflow deployed
  exact source but left `/health.releaseId=dev`; release 131 now injects the
  workflow `GITHUB_SHA`, so the public health body can be matched to the
  deployed commit without relying only on external release history.

### §2.15 Durable replay must count as evidence, not failure (2026-07-26)

- A WebSocket initial dump is at-least-once input. Quiet refresh can replay the
  same asset, venue timestamp, side, and level after that row is already
  durable.
- A uniqueness violation is therefore ambiguous: an exact same-key/same-value
  replay is successful idempotence, while a same-key/different-value row is a
  data-integrity conflict. Treating both as generic write failure converts
  durable truth into a false missing-evidence timeout.
- The evidence acknowledgement must describe the postcondition—this exact
  observation is durably present—not merely whether the current INSERT created
  a new row. Replay equivalence must be proven before advancing the chain-truth
  anchor; conflicts remain fail-closed and observable.
- Alert/recovery state also needs durable reason context. Persisting only an
  active component key supports deduplication but cannot explain a recovered
  incident after short runtime logs expire.

### §2.16 1 GB OOM is a capacity-model problem, not a one-off defect (2026-07-27)

- [PRODUCTION EVIDENCE] In the expanded production universe, the L1 HTTP
  parent, isolated snapshot child, and quote child can overlap in the same
  1 GB cgroup. One observed overlap reached about 306 MB + 403 MB + 134 MB
  RSS before the kernel OOM-killed the snapshot process. The exact values are
  samples rather than allocation ceilings, but they prove that 1 GB has no
  defensible production safety margin for the current co-located topology.
- [HISTORICAL CONTINUITY] This is the same capacity class first exposed on
  256/512 MB during Phase 02, now amplified by a much larger market universe,
  an always-on HTTP process, and the two-minute M2 quote producer. Individual
  retention, streaming, subprocess, compact-projection, GC, and allocator-trim
  fixes reduce waste; they do not turn the remaining aggregate working set into
  a transient bug.
- [STAGED DESIGN ITEM] Resource sizing and snapshot policy must be designed as
  one staged production contract:
  1. M1 initial operation separates slow structural-market discovery from the
     fast quote truth path and gives both measured memory headroom.
  2. M1→M2 integration isolates snapshot work from the online feed, defines an
     atomic publication boundary, and removes shared SQLite concurrency from
     the cross-machine design before adding workers.
  3. Formal arbitrage isolates perception, strategy, and execution failure
     domains; live decisions never wait for a full-market snapshot and fail
     closed when their source identity/freshness contract is not met.
- [DEFERRED DECISION] Do not independently resize the VM or redesign the
  pipeline from this note. During the consolidated repair, use the accumulated
  long-run evidence to decide exact memory/CPU, process placement, snapshot
  cadence, storage migration, retry policy, and promotion gates for each stage.
  The user has accepted reliability-first staged separation as the design
  direction, while asking that the final choice be considered and implemented
  together with the holistic repair.
## 2026-07-29 — Task 8 fault chain closure

Production qualification now separates runtime detection from mutation
capability. Gamma, Candidate CLOB/SQLite, Resource disk/load, notification
delivery, producer exit/stall, and HTTP faults all have durable expected
Incident or coverage facts plus component-specific recovery writers. This does
not make every injection executable: upstream API faults still require a
scoped proxy with exact release/fault authorization and finally-protected
cleanup; host/store/runtime faults retain distinct primitives.

Resource history remains backward-readable: additive `disk_free_bytes` and
`load_per_cpu` fields default to unknown for older v1 sample JSON, while each
new decision persists the thresholds used for deterministic replay. Telegram
recovery is keyed to the exact durable opportunity outbox ID; API delivery
success is not user receipt/read evidence.

### §2.17 Control-plane projection cannot substitute for write-side truth (2026-07-30)

- A virtual read-side `EXPIRED` state is not enough when the write-side
  one-active query still sees the original `AUTHORIZED` intent. Time-based
  terminal facts that release admission capacity must be materialized inside
  the same authority transaction used by claim or admission.
- Cleanup is a command until the owning runtime consumes it. Before claim, the
  authority must make the request terminal and non-claimable; after claim, the
  owner must clear memory before recording the terminal receipt. An API action
  row alone is not cleanup evidence.
- Unknown cleanup truth is not equivalent to “no cleanup requested.” A store
  read error or invalid history while a fault is active must make injection
  pass-through and freeze/degrade local control, without inventing a durable
  terminal fact from an untrusted source.
- Accepted and rejected envelopes share an audit table but not capabilities.
  Every claim, active-chain, or business-evidence writer must prove
  `status=accepted` plus the complete immutable hash/auth/runtime binding.
  Shape-only validation lets rejected control input contaminate source
  evidence.
- Authority timestamps must be monotonic against the verified event/action
  tail. Reconciliation uses `max(boundary_now, tail_time)` so clock regression
  cannot corrupt an otherwise append-only history.

### §2.18 Recovery work needs bounded retention and its own alert chain (2026-07-31)

- A resumable Structure producer fixed timeout/restart continuity but exposed a
  different capacity leak: about 1,370 full-universe Quote runs retained roughly
  70,000 child rows each and grew `state.db` to 18 GB on a 20 GB volume.
- Producer recovery and storage retention are one availability contract.
  Structure staging, snapshots, and Quote history each need bounded cleanup;
  fixing only the currently failing table merely moves the next outage.
- Quote cleanup runs only after a new feed is certified and published. It
  protects the newest complete and failed history independently, never selects
  collecting rows, deletes a bounded batch in foreign-key order, and retries
  on the next successful Quote cycle.
- SQLite capacity truth is reusable pages, not immediate file shrinkage.
  Online `VACUUM` would require dangerous extra space and a long exclusive
  rewrite; steady-state writers instead reuse freelist pages produced by
  bounded deletion.
- Cleanup fail-soft must not mean cleanup silent. Consecutive retention
  failures have a dedicated health check (warn at 1–2, fail at 3), Polywatch
  creates a component alert, and the next successful cleanup resets the chain
  so recovery is delivered once.
- A durable notification outbox also needs bounded service. Production drains
  at most 20 cards per pass with 1.1-second spacing while preserving per-card
  exponential retry facts; this converts a 91-card burst from repeated HTTP
  failure into a finite backlog without discarding observations.
- Two independently bounded producers can still starve one another. When a
  full-universe Quote run exceeds its nominal 120-second cadence, start-to-start
  scheduling immediately launches another run; an overlapping lower-priority
  Structure child then repeatedly times out despite durable cursor progress.
  A process-local producer slot serializes the heavy child processes, while
  Structure cooperatively checkpoints after 80 durable pages and immediately
  requeues. This bounds the time Quote waits for the slot without losing
  Structure progress. A cooperative checkpoint is healthy progress, not a
  failure-counter increment; real timeout/exit still uses the recovery and
  alert chain. Lock wait is outside the child's timeout and never blocks the
  HTTP parent. A certified Structure publication directly wakes the Quote
  single-worker loop; health treats the tiny publish-visible/task-wake race as
  the same bounded refresh warning, while the opportunity endpoint remains
  fail-closed until the matching Quote commits.

### §2.19 Runtime type boundaries and publication handoff are production contracts (2026-07-31)

- PostgreSQL `NUMERIC` and `TIMESTAMPTZ` arrive as `Decimal` and timezone-aware
  `datetime`. A temporary SQLite projection that only normalizes numbers or
  ISO strings can silently turn valid rows into `NULL` timestamps and make a
  healthy L3 universe look underfilled. Adapter tests must assert every field
  used by the recipe, not merely that insertion succeeds.
- A price-only WebSocket frame must not erase the last real book depth. The
  latest-row projection is authoritative for promotion, so coalescing needs
  per-asset depth memory and price updates must inherit that evidence until a
  new book replaces it.
- A successful public feed and its housekeeping have different critical
  paths. Publish certified immutable truth and mark runtime success before
  retention, observer reconciliation, or notification work; those paths retain
  their own bounded failure counters and alerts.
- Production calibration kept the hard Quote SLA at 300 seconds and shortened
  the attempt trigger from 120 to 60 seconds. Over 32 live samples strict L1
  health stayed HTTP 200 while steady runs advanced, showing that recovery
  should improve cadence rather than rename old data as fresh.
- A complete new Structure creates a second atomicity question: whether the
  previous complete Quote remains readable during recomputation. The measured
  fail-closed handoff was 93 seconds. The proposed contract is a one-version
  double buffer bounded by the unchanged 300-second Quote SLA; partial or
  mixed revisions remain forbidden. This is an explicit product policy gate,
  not an implementation detail to change silently.

### §2.20 One-version handoff needs one policy across HTTP, health, and alerts (2026-08-01)

- The approved double buffer is not a second mutable projection. Runtime keeps
  one immutable certified feed pointer until a complete successor atomically
  replaces it; endpoint-side rescans and partial publication remain forbidden.
- Availability is one shared pure decision over source/latest revision order,
  Quote age, Universe age, and Structure completion age. HTTP and strict health
  consume the same result, preventing a health-warn/API-503 policy split.
- Existing market-truth freshness is anchored to `taken_at_ms`. The handoff SLA
  is anchored to `finished_at_ms`, so completion age is a separate fact; changing
  the old metric in place would silently alter the Structure health contract.
- A valid previous feed reports `refreshing=true`, the newer
  `latest_structure_snapshot_id`, and its own older `source_snapshot_id`.
  Polywatch validates that relationship. It suppresses a healthy bounded Quote
  transition, but an unreachable opportunity endpoint now alerts even during
  refresh because continuous serving is the feature's core promise.

### §2.21 A migration wait must yield every upstream priority gate (2026-08-04)

- Production classifier-v2 exposed a scheduler deadlock hidden by isolated
  tests: event-member derivation correctly treated a pre-contract window as
  `waiting-natural-window`, but the next-priority drift child still consumed
  every scheduler tick. It reached the sidecar-dependent projection phase,
  deferred as `identity-stale`, and prevented the snapshot producer that alone
  could create a new authoritative window.
- A wait-state contract must be checked across the whole admission chain, not
  only in the component that first detects it. The safe pass-through predicate
  is the exact authenticated triple `waiting-natural-window` +
  `structure-event-source-receipt-unavailable` + `authenticated=true`.
  Anything weaker remains fail-closed.
- Yielding means scheduling a natural successor, not repairing history. The
  old window, pointer, receipts, and stale classifier evidence remain
  immutable; durable defer evidence explains why drift temporarily ceded the
  producer slot. The eventual pointer switch atomically terminalizes the old
  active comparison without creating an authorization receipt. A failed
  pointer transaction rolls both changes back; the next tick creates exactly
  one v2 comparison for the new current identity.

### §2.22 Cooperative progress must break failure streaks and retention must have a resident owner (2026-08-04)

- Production release 233 retained `snapshot:failure_counter=4` across many
  authenticated, durable sidecar and generation checkpoints. The code resets
  the counter only after a complete `OK/DEGRADED` snapshot. That contradicts
  the documented consecutive-failure meaning: non-deferred forward progress
  breaks a streak even though a scheduler already in `RECOVERING` must remain
  there until complete certified truth is published. Supersession, writer-busy
  defer, and zero-progress results do not prove recovery and must not reset it.
- Retention table ownership must evolve with schemas. Both failed and published
  Structure-window purge lists omitted
  `structure_sync_event_group_truth_staging`; a minimal current-schema fixture
  reproduces the production `FOREIGN KEY constraint failed`. Snapshot purge
  likewise selects rows still referenced by `snapshot_attempts`; it must retire
  same-retention operational attempts transactionally and leave snapshots
  referenced by independently retained Quote runs alone.
- A safe cleanup primitive without a resident caller is not self-healing.
  Generation evidence currently has a bounded authenticated cleanup API and a
  Makefile target, but no daemon owner, so production pressure remains at nine
  retained generations and seven reclaimable indefinitely. The production
  contract needs a low-priority, Quote-aware resident maintenance loop with
  bounded transactions, durable cursor/health truth, retry, and alert recovery.

### §2.23 Maintenance chain-truth needs three independent facts (2026-08-04)

- Authority evidence, replayable payload, and operational runtime have different retention
  lifecycles. Reclaiming bulk generation/staging rows must preserve the publication/comparison/
  cleanup proof skeleton; retaining that skeleton does not require retaining every replay row.
- Pressure alone cannot prove that maintenance is alive. A singleton runtime row must be mutated
  by the resident owner and read in the same bounded generation-status transaction used by health.
  Health combines runtime state/failure/checkpoint age with reclaimable pressure; Polywatch reads
  that exact sub-check for alert and recovery.
- Quote fairness is an admission contract, not a sleep interval. Check priority before and after
  acquiring the shared producer lock, bound one cleanup transaction to 500 rows, then release the
  lock. Cancellation waits for the SQLite thread before release so a background transaction never
  outlives ownership.
- Production-shaped acceptance must fail immediately on authenticated `blocked` results. A test
  fixture that changed generation payload without resealing both expected and committed counts
  reproduced `generation-count-contract-mismatch`; treating only unchanged pressure as a loop
  condition turned a correct fail-closed response into a false performance timeout.

### §2.24 Bounded slices also need an explicit continuation signal (2026-08-04)

- A bounded child can be perfectly restart-safe and still make production
  recovery unacceptably slow. Release 237 advanced classifier-v2 by thousands of
  rows per slice, but each successful non-terminal slice slept the ordinary
  300-second Structure cadence because its path omitted the resident loop's
  `_checkpoint_pending` signal.
- The chain-truth test for cooperative work is therefore not only “did the
  cursor move?” It is “did durable non-terminal progress select the documented
  continuation cadence?” Event-member and Structure checkpoints already chose
  100 ms; classifier drift must use the same signal. For drift, that means an
  active phase stopped by `max-chunks` or `max-elapsed-seconds` after at least
  one committed chunk and `ready=false`; no single signal is sufficient by
  itself. A committed chunk may legitimately process zero rows while advancing
  a phase boundary, so continuation must key on chunk commitment rather than
  row count.
- Immediate follow-up does not weaken Quote priority. Every slice releases the
  producer lock, and the next admission repeats both Quote active/due checks
  before and after acquiring it. `stale`, `not-pending`, zero-progress,
  deferred, and failed paths keep their existing scheduling and incident
  semantics.

### §2.25 Timeout recovery and nullable ordinary events require immutable evidence (2026-08-08)

- A process timeout is not evidence of progress. A child can commit before the
  parent kills it, so recovery may use the 100 ms continuation only after a
  fresh status read proves that the same comparison ID advanced its durable
  `checkpoint_at_ms`. The terminal failed attempt remains the audit record;
  unchanged, replaced, terminal, or unavailable status stays on normal cadence.
- Production exposed 11 event-only members with `negRisk=null`,
  `enableNegRisk=false`, `negRiskMarketID=null`, null member group, and
  `negRiskOther=false`. This is safe ordinary-event evidence, but changing v3
  would rewrite its terminal receipt semantics. v4 binds the exact predicate to
  a new comparison identity and leaves every v3 receipt immutable.

### §2.26 Task-local evidence is primary; watchdogs are a backstop (2026-08-24)

- A transactional task already knows whether heartbeat, progress, receipt and
  terminal commit succeeded. Those results must atomically update runtime,
  job/attempt, incident and alert-outbox truth instead of waiting for sampling.
- Cancellation does not stop `asyncio.to_thread`; blocking work must be
  shielded and drained while preserving cancellation. A bounded terminal
  commit's durable result wins over a scheduler timeout.
- A heartbeat renewal racing stop must update the in-memory lease before the
  stop check, or the worker can reject its own commit with an obsolete lease.
- Historical receipts missing terminal evidence require narrow durable proof,
  append-only idempotency and full rollback; business truth alone is not a
  complete runtime lifecycle.

### §2.27 A cross-job lifecycle gate must traverse real terminal boundaries (2026-08-25)

- A fixture that claims a real job but directly calls the private runtime-event
  helper proves only that the event table accepts rows. It can stay green while
  a Quote receipt, Structure manifest, or opportunity pointer stops committing
  its terminal fact atomically.
- The coverage gate must therefore build each job type's minimum durable
  prerequisites and invoke its public specialized completion method. Assertions
  then bind exactly one start, its registered progress chain, and exactly one
  terminal event to the same job key.
- Test infrastructure is part of the fail-closed contract. If the Postgres
  authority needed for transactional proof is unavailable, skipping converts an
  unevaluated invariant into a false pass; the operator must receive an
  actionable gate failure instead.

### §2.28 Recovery authority needs three fences and two kinds of no-op (2026-08-25)

- A pure deadline decision is not recovery authority. Multiple reconcilers can
  correctly derive the same action; controller identity+epoch, exact
  attempt/job lease, active-target uniqueness and durable budget must arbitrate
  scheduling in one Postgres transaction.
- Job ownership and recovery-action ownership are distinct. A job lease may be
  expired precisely because it needs reclaim, while an expired action-worker
  lease means that executor must not mutate anything. Business mutation and
  action terminal completion therefore share a transaction with a final
  DB-clock action-lease check.
- `stale-noop` is a durable expected outcome for an obsolete fence or a real
  active-target race. Budget changes, conflicting idempotency payloads,
  runtime-state changes and incident identity conflicts are store failures;
  converting them to CLI `status=ok` recreates silent permanent failure.
- Read-only status and mutation control are separate capabilities. Status may
  expose controller, incident, budget/cooldown and action outcome but cannot
  claim/renew a controller or consume recovery budget. Once/serve require an
  explicit operator enable gate, and serve exits nonzero on unhandled
  store/fencing errors so an outer supervisor can alert or restart it.
- Automatic job recovery is intentionally narrower than topology recovery.
  Heartbeat, cancel, retry, expired reclaim and one circuit probe are enabled;
  process/Machine actions remain `disabled-action` until a separate
  least-privilege production-enablement plan proves that boundary.

### §2.29 Rolling qualification needs two histories and two detection clocks (2026-08-25)

- Task-local facts and periodic observations answer different questions. A
  terminal task event can invalidate immediately on the next bounded tick;
  per-tick Structure, Quote and opportunity freshness observations detect the
  absence of events. Neither can replace the other.
- Business `occurred_at` is not a safe durable cursor because an old-timestamp
  row may commit late. Migration 024 projects source changes into an append-only
  ingress ledger with a database monotonic `ingest_seq`; the original time stays
  in the payload while detection order cannot skip a late commit.
- Epoch evidence and recovery-period evidence have different certificate
  meaning. A breaking fact terminates the old epoch. Facts seen while recovering
  belong to an append-only recovery-observation ledger, so they remain visible
  without violating the policy invariant that a recovering epoch has no
  qualifying facts. Confirmation opens a new clean epoch automatically.
- An immutable row is not automatically trustworthy. Certificate canonical
  bytes, digest, content-derived IDs, epoch bounds, counts, SLO results and
  evidence digest are checked at the Python API, the PostgreSQL insert trigger
  and the read verifier. Ordinary API roles can read but cannot invoke the
  privileged writer.
- Qualification status must follow the history chain. When the active row is
  recovering, `last_breaker` comes from the latest breaking recovery observation
  or the previous invalidated epoch; returning `None` would recreate the silent
  failure the operator surface is meant to eliminate.

### §2.30 Operator truth needs task events, absence detection, and monotonic presentation (2026-08-25)

- A timer cannot know a task's exact business result. Task-local terminal and
  retry paths must atomically create their incident/event/outbox intent; the
  watchdog observes missing progress, deadline and infrastructure absence.
  These are complementary clocks, not competing authorities.
- Notification rendering is downstream of durable intent. The writer decides
  incident transitions and reminder cadence from persisted rows, then writes
  event plus Dashboard/Telegram outbox in one transaction. Normal Telegram
  delivery belongs to the outbox worker; direct watchdog delivery is only a
  generic one-shot break-glass when the writer itself is unavailable.
- Arrival order cannot be mistaken for event order. The incident row lock and
  latest relevant transition form a durable watermark; stale or equal-time
  detected/recovered observations are no-ops. Reminder timing has a separate
  detected/escalated cursor so a recovery-started event orders the history
  without restarting the 15-minute/hourly schedule.
- Operator surfaces are projections, not controllers. The API validates and
  bounds facts, the Dashboard decoder fails closed, and authenticated smoke
  checks real panel content rather than HTTP reachability. Recovery mutation
  remains behind controller/job/action/attempt/lease fences.

### §2.31 Deterministic fault proof and production authority are separate gates (2026-08-25)

- Repeated 24-hour restarts are a poor debugging loop. A local matrix now runs
  12 fault classes twice against a disposable database upgraded through the
  real migration head, checking incidents, actions, fences, Dashboard and
  qualification projection. It exposed real decoder/ingress defects; the gate
  fixed the production chain instead of excluding failing cases.
- Observe-only must be durable, not a log level. Each bounded candidate becomes
  an immutable decision bound to controller owner+epoch and runtime-state
  digest; an empty turn writes idle. A read-only repeatable snapshot rejects
  tick gaps, stale identity, replay mismatch, candidate mismatch and any
  recovery action overlapping the window.
- Concurrent visibility and bounded mutation have different cardinality. When
  three sampling points fail together, observe-only records all three; execute
  mode may still select only the first actionable target per turn to preserve
  budget and active-target arbitration.
- `--enable` starts a guarded service but is not recovery authority. Execute
  requires a separate closed mode. Process/Machine recovery additionally needs
  action-class enablement, an exact immutable `(app, machine_id)` allowlist,
  current controller/action leases, database preflight and independent health.
- Local proof cannot silently become production mutation authority. Deployment,
  job fault and process/Machine fault each need an exact release/target/fault/
  maximum-effect/rollback/evidence authorization. Until then the evidence must
  say NOT RUN, not pending-pass or implied approval.

### §2.32 Least privilege includes namespace resolution and credential lifecycle (2026-08-26)

- A table-level ACL can be exact while an unqualified query resolves to an
  attacker-owned same-name table. Runtime authority therefore needs both
  schema-qualified application SQL and a startup proof over active, role and
  database search paths plus every non-system schema/object owner and grant.
- `CREATE ON DATABASE` is a namespace-creation capability, not harmless
  metadata. It must be denied to scoped roles. `TEMPORARY` is intentionally
  retained for original-app compatibility, but only with controlled
  `pg_catalog,public` resolution and schema-qualified persistent objects.
- PostgreSQL 16 membership is a tuple, not just two role names. `admin_option`,
  `inherit_option` and `set_option` belong in the exact allowlist; otherwise a
  role can delegate or switch authority while a name-only comparison stays
  green.
- Migration/provision/disable authority must not share an environment variable
  with a daemon login. The operator path uses
  `POLYARB_CONTROL_PLANE_DB_ADMIN_DSN`, then verifies the two app-specific DSNs,
  then reconnects with the admin DSN for disable. Neither app receives the
  admin credential or its peer's DSN.
- Durable evidence gates must fail on missing structure before checking bytes.
  A missing/invalid evidence file or empty artifact list is drift, and repeated
  numeric phase names in Git hooks require an unambiguous staged workstream
  path rather than first-match selection.

### §2.33 Secret inventory is not effective secret provenance (2026-08-27)

- A Fly app can list a deployed hidden secret while an older detached Machine
  command overrides it from a password-bearing ordinary environment variable.
  Secret inventory therefore proves presence, not effective runtime provenance.
- Read-only topology inspection must avoid unfiltered Machine JSON because
  ordinary env values may contain credentials. Prefer bounded app/Machine
  identity projections; if raw config is unavoidable, capture it only through
  a redactor and never persist or quote secret-like values.
- Remediation rotates the affected database login, replaces the hidden secret,
  removes the ordinary env override and updates exactly one Machine. Rollback
  must never restore the compromised password. This is a separate exact
  authorization from revision 026 rollout and from all recovery/fault gates.

### §2.34 Delegated role lifecycle must write only the verified delta (2026-08-27)

- A delegated `CREATEROLE` identity can create a constrained LOGIN and rotate
  its password, yet PostgreSQL may reject a later full attribute restatement
  with `42501`. Idempotence cannot mean replaying every original DDL clause.
- Existing roles must first pass the complete attribute, membership,
  ownership, direct-ACL and namespace snapshot. After that proof, provision
  writes only the necessary delta: restore `LOGIN` if disabled, rotate the
  password, and restate the exact application-to-capability membership.
- First creation remains explicit and fail-closed. Separating validation from
  minimal mutation preserves least privilege while respecting provider role
  ownership boundaries; it does not trust ambient state.

### §2.35 Connection startup intent is not active namespace truth (2026-08-27)

- A libpq `options=-csearch_path=...` argument can be accepted locally while a
  managed Session Pooler does not forward it to PostgreSQL. The only authority
  is the connected session's `current_setting` plus `current_schemas` result.
- Relaxing the startup verifier would create a false green: later daemon
  connections would still use `"$user", public`. The factory must establish
  the invariant, not merely the verifier.
- Each scoped connection therefore applies schema-qualified
  `pg_catalog.set_config` at session scope, commits that bootstrap so callers
  receive a clean connection, and closes on bootstrap failure. Startup options
  remain defense-in-depth for direct PostgreSQL paths.

### §2.36 Extension namespace is deployment state, not a source-code constant (2026-08-27)

- Schema-qualifying a SECURITY DEFINER dependency is necessary but the schema
  must be the one recorded by `pg_extension.extnamespace`. Standard PostgreSQL
  commonly installs pgcrypto in `public`; Supabase installs it in
  `extensions`. Hard-coding either environment turns namespace hardening into
  a deterministic production outage.
- The safe repair resolves the installed extension schema from system catalogs,
  verifies the exact `digest(bytea,text)` routine, and rewrites only the known
  token count in the three existing function definitions. Missing functions,
  unexpected counts, or an invalid schema name fail the migration closed.
- `CREATE OR REPLACE FUNCTION` repair evidence must compare owner, ACL,
  SECURITY mode and configured search path before/after. A working freshness
  call is insufficient if the repair silently broadens EXECUTE authority.
- Production-like migration tests must preinstall pgcrypto into an
  `extensions` schema, prove revision 026 fails for the scoped qualification
  login, then prove 027 succeeds and survives a 027→026→027 round trip. A
  default local `public` install cannot expose this provider-specific defect.

### §2.37 Failure reporting can become the process-killing secondary failure (2026-08-28)

- The first business failure and the final process exception need not be the
  same defect. Here an old worker's upstream task failed first; recording its
  incident/recovery facts fired qualification projection, whose missing
  `public.digest` dependency raised SQLSTATE 42883 and terminated the process.
  Fly then repeated that deterministic secondary failure until ten retries were
  exhausted. Diagnosis must preserve both links instead of naming only the last
  stack trace.
- A database repair does not repair an old running image. Revision 027 made the
  trigger chain safe, but the three 2026-08-18 worker images still predated
  task-runtime instrumentation. They could run without crashing while leaving
  runtime reconciliation and qualification's runtime source blind.
- Deployment identity is therefore part of chain-truth: bind the reviewed
  source release to an immutable image digest, verify that exact digest on each
  Machine, and hash the config projection with only `image` removed before and
  after. A healthy process on an unreviewed digest is not evidence that the
  intended observation chain exists.
- Runtime-event proof must follow real workload semantics. An idle quote pool
  correctly emits no job lifecycle event; coordinator and Structure jobs proved
  the shared append→trigger→qualification path. Injecting an artificial
  production job merely to make a counter non-zero would expand the effect and
  weaken the evidence boundary.
- Current-candidate parity is intentionally fail-closed and can sample the
  small interval after a new job becomes visible but before the controller's
  next durable decision. Do not waive the gate. Require an active lease, the
  next scheduled decision, a bounded max gap, deterministic replay and zero
  recovery actions; a genuine stalled controller will not converge on the next
  tick.

### §2.38 Liveness deadlines must scale with deterministic work, not only leases (2026-08-28)

- Lease, heartbeat, progress and attempt deadlines protect different failure
  modes. A job can renew its lease and emit real progress while still being
  deterministically cancelled by an absolute attempt ceiling shorter than its
  admitted workload.
- The production Structure certifier exposed this exact live-lock: 1,117
  immutable ranges advanced at roughly one per second, but the common
  300-second ceiling cancelled every attempt around range 290. Because parity
  has no checkpoint, each reclaim restarted at range one. Short liveness gates
  were healthy; the total-work budget was wrong.
- Keep heartbeat/progress detection short and job-specific absolute ceilings
  bounded. Plan 05.6-208 changes only `structure-certify` to 3,600 seconds for
  a 30-second lease; every other job retains 300 seconds. If the workload later
  approaches the new ceiling, optimize parity or introduce authenticated
  checkpointing rather than removing the ceiling.
- Qualification freshness must follow consumed publication authority. The
  transactional chain intentionally never writes legacy `structure:current`;
  `quote:current` canonically embeds the Structure bundle digest it consumes.
  Mapping that identity to the certified Structure manifest avoids a second
  drifting pointer and stays inside the existing qualification capability.
- Query design is part of least-privilege chain-truth. An initially plausible
  join through `m1_quote_admission_inputs` was rejected because the production
  qualification role cannot read that table. The final query uses only its
  already-reviewed pointer and manifest reads, while missing identity still
  fails closed as `evidence.gap`.
- Identity derivation must validate grammar before transforming strings.
  Substring mapping alone lets matching malformed `quote:bad` and
  `structure:bad` rows look internally consistent. Both freshness queries now
  require the exact lowercase 64-hex Quote generation grammar; a real
  PostgreSQL regression proves malformed pairs become gaps.

### §2.39 A task may have many clocks but only one lifecycle authority (2026-08-28)

- Plan 05.6-208 increased one deadline and exposed the deeper defect: durable
  attempt, worker-local profile, scheduler turn timeout, I/O timeout and service
  shutdown were independently allowed to terminate the same work. The repair is
  not a larger number. `runtime-v2` is a closed eight-job registry that derives
  heartbeat, progress, absolute attempt, I/O, terminal grace, retry budget and
  checkpoint cadence from one lease-bound policy persisted with the attempt.
- Scheduler order is not dependency truth. A validated acyclic successor graph
  supplies lane construction, while downstream eligibility still comes only
  from durable predecessor receipts/pointers. Independent lanes repeat at their
  own cadence, so a long certifier cannot starve source admission or Quote.
- Service stop has a three-step contract: stop claims, request/cancel work, then
  drain only through policy terminal grace. A non-cooperative sync call runs
  behind a non-joining daemon bridge; after grace, the still-current lease and
  checkpoint are the recoverable fact. A late terminal write loses the epoch
  fence after takeover. Both coordinator and role-local loops emit
  `service-stop-grace-expired` rather than disappearing silently.
- Open circuit is a parent lifecycle state, not another property of the last
  attempt. Its cooldown/probe decision must precede the expired heartbeat/lease
  clocks stored in that historical runtime row. The controller releases exactly
  one probe and moves `next_probe_at` forward; ordinary claim cannot self-probe.
- Migration is part of recovery semantics. Revision 028 closes superseded
  running attempts, releases a current leased job that has no matching runtime
  row, persists exact policy snapshots without server defaults, and introduces
  per-job monotonic checkpoint sequence. A real 027→028→027→028 test proves the
  orphan is claimable rather than permanently leased.
- Qualification cursor handoff is identity-sensitive. The first identity in a
  database consumes existing breaker history; a later release detects a
  predecessor cursor identity and starts at current ledger high-water. This
  prevents both missed startup failures and offset-zero history being counted as
  new-release live coverage.

### §2.40 Read timeouts do not bound result transfer (2026-08-28)

- A production pre-rollout `qualification-status` read exposed a different
  unbounded lifecycle: PostgreSQL had completed the query and waited in
  `ClientRead`, while psycopg was still receiving the latest epoch's full
  `fact_records` and `fact_digests` selected by `SELECT *`. A server-side
  statement timeout cannot interrupt time spent transferring an already
  completed, monotonically growing result.
- Operator projections and evidence projections are different contracts.
  An initial network-bounded query still used JSONB dereference/expansion and
  was rejected in review because database work remained growth-bound. Revision
  029 instead stores generated final-fact, recovery-count and last-20-recovery
  columns at write time. Status reads those fixed columns; predecessor breaker
  fallback selects only `invalidated_at` and `invalidation_reason`. Certificate
  and state-transition paths retain full evidence and independent verification.
- Bounded interruptibility therefore requires all three: a connection bound,
  server execution/lock bounds, and a bounded result shape. Wrapping an
  unbounded read in another arbitrary wall-clock timeout only hides which of
  those contracts failed.

### §2.41 Active evidence must be normalized, not rewritten as state (2026-08-28)

- Fixing one status query did not close the chain. Coordinator canary exposed
  `operational_snapshot()` selecting the same growth columns, while the active
  qualification writer still reread and rewrote the complete epoch JSON each
  tick. That is O(n²) lifecycle work and another delayed single-point failure.
- Revision 030 separates state from history. Each fact is an immutable ordered
  row; the epoch mutates only fixed-size lifecycle fields and counters. Restart
  reconstructs policy state in 500-row pages and verifies the replay against
  persisted scalars. Cursor CAS rejects source divergence; epoch version CAS
  rejects a stale in-memory replay even when the cursor happens to match.
- Terminal history materializes once in memory for invalidated/qualified
  evidence. Certificate insert/reverify compares only scalar epoch projections;
  the append-only fact relation remains the independent replay source. A canary failure must roll the Machine back to
  its prior digest/state before implementation continues; mixed release state
  is not acceptable evidence.

### §2.42 Timeout authority and DAG eligibility must be durable (2026-08-28)

- The runtime-v2 audit separated four authorities: attempt/progress watchdogs
  own a claimed attempt, DB deadlines own one I/O, terminal grace owns service
  exit, and cadence waits own only the next scheduling time. An outer wrapper
  may not terminate a lower-level lifecycle merely because its number is
  shorter.
- `asyncio.to_thread` is unsafe as a last-resort isolation boundary because
  `asyncio.run()` joins the default executor during shutdown. Formal blocking
  calls now use one daemon bridge: first cancellation drains within central
  grace, second cancellation detaches and prevents cleanup from starting new
  terminal I/O. Quote's async reader obeys the same second-cancel authority.
- A topological scheduler order cannot prevent sibling processes from claiming
  a successor. Structure/Quote certifiers now start `waiting`; the final
  terminal receipt atomically wakes the barrier only when durable receipt and
  input counts match. Real PostgreSQL connections prove the before/after claim
  boundary; the certifier still validates every identity as defense in depth.
- Database connect/statement/lock limits are one ordered policy applied at
  session entry. Fenced transactions may tighten the policy to remaining lease
  time, while API request envelopes must cover connect plus statement rather
  than contradict them.

### §2.43 Schema compaction must fence every writer and migration wait (2026-08-28)

- Revision 030 normalized qualification facts but intentionally left legacy
  arrays populated during backfill. Production preflight found the active
  2,500-fact epoch still carried both canonical rows and the old growth-bound
  copy. Revision 031 validates count, ordinal, payload and recovery-index parity
  before clearing all legacy arrays; CHECK constraints make fixed-size epoch
  rows a database invariant rather than an application convention.
- A compatibility writer outside the new qualification service still rewrote
  `fact_digests` and `contained_recoveries`. The 031 fence exposed it in the
  full real-Postgres suite. It now appends/verifies normalized rows and updates
  only scalars; concurrent CAS losers roll back their entire transaction.
- Migration execution belongs to the same timeout audit. Alembic previously
  had a private statement timeout but no connection or lock-acquisition bound.
  Its connect/statement/lock settings now come from the central ordered policy;
  a migration fails quickly on lock contention instead of becoming a silent
  deployment singleton.

### §2.44 A bounded statement does not bound an unbounded operation (2026-08-28)

- Every formal operation must declare both its per-I/O deadline and its number
  of I/O rounds. A loop of individually bounded queries is still an unbounded
  business operation when object/fact count controls the round count.
- Role authority verification now batches schema, table and sequence privilege
  matrices into fixed catalog rounds. Qualification's ordinary accumulating
  path uses one bulk append, one range verification and fixed scalar updates;
  restart history is the explicit paged/checkpointed exception.
- Session policy must be active when a connection is returned. Libpq startup
  options provide the first layer, but the production Session Pooler is known
  to drop them. The scoped factory therefore performs one post-connect
  `set_config`/readback round under a cancellation deadline derived from the
  central statement policy; it closes on timeout or mismatch. This preserves
  §2.35 active-session truth without restoring an unbounded bootstrap gap.
- Orchestration tools cannot add a universal outer timeout to heterogeneous
  gates. Climb delegates deadlines to each gate and checkpoints successful
  gates against exact git head plus argv, so interruption resumes the unfinished
  suffix without treating orchestration death as domain failure.
- Cooperative stop is a chain property: stop new work, signal blocking code,
  finish at most the current server-bounded statement, rollback, then refuse
  subsequent I/O. A second cancellation still detaches under lease fencing if
  the client does not cooperate.
- Observe-only is not exempt from operation-round limits. The runtime controller
  previously wrote up to 100 decision records through 100 connections on the
  event loop. One turn now uses a single lease-fenced bulk transaction and a
  stop-aware daemon bridge; controller epoch and idempotency digest fence any
  late result after grace detach.
- A provider timeout does not make a mixed async/sync service interruptible.
  Alert delivery previously ran synchronous claim/finish calls on the signal
  loop. Its whole turn now runs in a stop-aware daemon bridge; cooperative stop
  prevents a late provider response from starting finish SQL, while the durable
  outbox lease recovers abandoned claims.

### §2.45 Inner retries and proof loops are lifecycle controllers (2026-08-28)

- An SDK retry policy is a second recovery controller. R2's three inner attempts
  multiplied with three durable attempts, while Gamma retry/backoff could outlive
  the worker I/O envelope that was supposed to contain it. Formal Gamma, CLOB
  and R2 clients now perform one explicit provider attempt below the worker I/O
  deadline; PostgreSQL attempt/circuit state exclusively owns retry count,
  backoff and probe release.
- Outcome labels do not define scheduling semantics. Quote and opportunity
  certifiers called incomplete input `waiting` but persisted a fixed five-second
  retry outside the central circuit. Incomplete barriers now consume the normal
  retryable incident/backoff/circuit path. Durable `waiting` remains only an
  eligibility state changed by predecessor receipts.
- A proof loop with bounded leaf calls can still block shutdown or exceed its
  cadence. Cloud-soak samples run off the signal loop, receive cooperative stop,
  and recheck it before the append boundary. Local `flyctl` reads have their own
  bounded subprocess lifetime.
- Watchdog observation is a fixed parallel operation round, not an app/Machine
  serial loop: at most eight apps, sixteen Machines per app, control and app
  snapshots in parallel, then one list plus one parallel exact-Machine detail
  round per app. Detail reads remain necessary because list events omit
  authoritative `request.restart_count`.
- Alert delivery declares one ordered clock contract: one provider attempt,
  provider timeout below DB-derived stop grace, stop grace below the durable
  outbox lease, and retry cadence owning only future scheduling. This ordering
  is executable policy, not a comment beside independent constants.

### §2.46 Read-only database intent does not imply zero runtime effect (2026-08-28)

- A role verifier launched over SSH inside the 256MB live controller performed
  no writes, but the extra Python process exhausted the Machine cgroup and
  produced exit 137. Fly restarted the Machine and the controller correctly
  claimed a new lease epoch. The incident invalidated the prior 1,800-second
  continuity window even though durable data remained correct.
- Publication preflight therefore runs fixed-round catalog proof from the
  operator host and binds deployed identities through required secret-name
  presence. It may not execute extra interpreters inside constrained production
  Machines. Resource footprint is part of an operation's effect class.
- Continuity evidence is boot/lease-epoch scoped. An automatic restart is a
  successful recovery fact, not permission to splice evidence across the old
  and new processes. Qualification and observe-only clocks restart from the new
  durable anchor.

### §2.47 Transport intent and active session truth require two bounded layers (2026-08-28)

- The operation-round repair accidentally removed §2.35's post-connect session
  bootstrap and trusted only libpq startup options. The controller canary then
  failed `database-role.namespace-unsafe: active-search-path`, confirming again
  that the production Supabase Session Pooler accepts but drops those options.
  The Machine was rolled back before any sibling rollout.
- This was a design-regression failure, not a reason to increase a timeout.
  Startup options remain defense-in-depth for direct PostgreSQL. A single
  autocommit `set_config`/readback statement establishes active truth for pooled
  sessions and is cancelled at the central statement-policy boundary. Timeout,
  mismatch or provider exception closes the connection; no partially scoped
  session reaches role verification or business SQL.
- Release gates must preserve previously established provider-specific facts.
  A locally valid simplification cannot delete a production workaround unless
  the new path re-proves the same provider boundary end to end.

### §2.48 Retry authority includes probe-release holdoff (2026-08-28)

- Centralizing ordinary retry transitions is insufficient when recovery
  execution can write its own next-probe clock. The formal `probe-circuit`
  transaction still used a literal five-minute holdoff, so observe-only replay
  and enabled recovery would have consumed different circuit timing authority.
- Retry policy is now lease-independent and resolved directly by job type.
  Both retryable-failure transactions and probe release use the same budget,
  exponential backoff and cap with the current consecutive-failure count. No
  caller invents a minimum lease merely to obtain retry settings.
- Observe-only success cannot certify code that diverges only after mutation is
  enabled. Reverse execution-path audits must include the write side of every
  proposed recovery action before the image can advance to sibling rollout.

### §2.49 A fail-closed gate still needs actionable failure truth (2026-08-28)

- Collapsing every expected policy rejection to an exception class is not safe
  observability. The operator cannot distinguish insufficient evidence from a
  stale controller, decision gap, replay mismatch or parity break, so a
  deterministic gate degenerates into wall-clock guessing and repeated runs.
- Expected observe-gate errors are a closed, safe vocabulary. Insufficient
  windows now include measured and required seconds; the CLI preserves those
  predefined reasons while unknown exceptions remain type-only. This adds
  actionability without exposing DSNs or provider bodies.
- Evidence duration is a qualification lower bound, not an execution timeout.
  The gate remains nonzero and never waits, relaxes policy or kills the observed
  controller; interruption only discards the operator read and the durable
  lease-epoch evidence continues accumulating.

### §2.50 A read model needs one snapshot clock, not an early caller clock (2026-08-28)

- Production coordinator canary exposed negative controller lease/progress ages.
  `operational_snapshot()` captured client wall time before connection/bootstrap,
  then executed roughly 45 statements under `READ COMMITTED`. Later statements
  could observe a heartbeat committed after the captured time, making a healthy
  future fact look malformed.
- Production operator reads now use `REPEATABLE READ READ ONLY` and obtain
  `clock_timestamp()` inside the same PostgreSQL data statement as every compared
  row. Explicit caller time remains only for deterministic fault tests. Time
  consistency is a database snapshot property, not an NTP or tolerance setting.

### §2.51 A one-statement request envelope requires one data statement (2026-08-28)

- `CONTROL_PLANE_DB_POLICY.request_timeout_seconds` intentionally covers one
  connection plus one bounded statement. Applying it to roughly 45 sequential
  snapshot queries caused 15.5–17 second operator reads to fail at 10.5 seconds
  even though no individual SQL timed out. The same latent mismatch existed in
  the two-query opportunity page.
- Both public reads now send transaction setup and one aggregate data statement
  in one client execute round. Structural tests fence the number of commands and
  execute calls. Production-equivalent snapshot reads complete in 2.85–4.56
  seconds and the opportunity page in 5.15 seconds; the envelope now describes
  the operation it actually wraps.
- The general rule from §2.44 is stricter for request paths: if the request policy
  says one statement, composition must occur in SQL or the operation must declare
  a different explicit policy. Increasing a generic timeout is not an option.

### §2.52 Parent cancellation must not preempt child recovery ownership (2026-08-28)

- Two Structure subprocess paths sent SIGKILL and then waited forever for pipe
  drain. The daemon parent simultaneously imposed a five-second `wait_for` on a
  child cleanup contract that allowed 15 seconds for TERM and 15 for KILL. This
  created two lifecycle authorities and could interrupt receipt persistence or
  leave a subprocess lane wedged.
- Structure subprocesses now share one bounded TERM/KILL/pipe-drain helper.
  Producer supervisor process wait and stdout/stderr drain are bounded as well.
  The daemon no longer installs a competing outer cancellation; each task owns
  its cleanup, Uvicorn uses the same 30-second maximum child budget, and Fly's
  40-second `kill_timeout` is the sole process-level backstop.
- Platform kill time is best-effort, so restart correctness still depends on
  durable checkpoints, lease expiry and terminal-receipt reconciliation. Grace
  improves clean shutdown; it never becomes the recovery source of truth.

### §2.53 Process exit is a valid TERM/KILL race outcome (2026-08-29)

- `returncode is None` is only a prior observation. A child may exit before the
  following `terminate()` or `kill()`, causing `ProcessLookupError`. Treating
  that race as supervisor-control failure corrupts the terminal receipt and
  incident narrative even though the intended stopped state already exists.
- Both supervisor signal stages now accept only `ProcessLookupError` as
  already-exited success; all other control failures remain visible. Behavioral
  tests inject the race independently at TERM and KILL and prove bounded return.
- Read/recovery paths apply the same boundary discipline to database JSON:
  numeric, list and text fields are validated before domain conversion. A
  malformed observation must fail as a bounded read error, not cascade into a
  second ambiguous recovery failure.

### §2.54 Platform shutdown configuration is part of lifecycle authority (2026-08-29)

- Internal shutdown correctness is insufficient if the deployment layer can
  preempt it earlier. Read-only Fly preflight found the live controller with
  `kill_signal=null` and `kill_timeout=null`, so formal runtime-v2 still
  inherited the platform's historical five-second termination default while
  the child cleanup owner required up to 30 seconds.
- All seven formal long-running templates now declare `SIGTERM` and a uniform
  40-second platform backstop. The value is derived from the maximum 15-second
  TERM plus 15-second KILL/reap owner and a 10-second terminal-evidence /
  interpreter-exit margin. It is not an operation timeout and does not delay a
  process that exits normally.
- Template and rendered-config tests fence the field at both source and rollout
  boundaries. A controller update may change only exact image plus these two
  lifecycle fields; Machine identity, region, env, guest, restart policy,
  observe-only mode and empty allowlist remain invariant. Discovery of the gap
  superseded the pushed candidate before any Machine mutation.

### §2.55 Release builds need bounded and authenticated input acquisition (2026-08-29)

- A runtime can have correct shutdown semantics while its release pipeline can
  still block forever. The exact-image build exposed an unbounded Supercronic
  `curl`; 138.8 seconds of silence had no declared way to distinguish a slow
  valid transfer from a wedged release lane.
- Immutable input acquisition now has one aggregate owner and smaller leaf
  bounds derived from the measured 12,432,517-byte artifact: 15-second connect,
  240-second transfer at a 64 KiB/s throughput floor plus margin, one transient
  retry, and a 500-second aggregate owner. The downloaded bytes must match the
  pinned v0.2.30 amd64 SHA256 before execution permission is added.
- Build-download policy is not runtime policy. Its retry cannot alter a job,
  lease, circuit, Machine, or production evidence window; a failed build simply
  produces no release artifact and can resume from immutable Docker layers.

### §2.56 Deployment schemas require an explicit translation boundary (2026-08-29)

- A valid Fly TOML lifecycle contract does not have the same JSON shape as a
  direct Machines API update. Top-level `kill_signal` / `kill_timeout` translate
  to `config.stop_config.signal` / `config.stop_config.timeout`; posting the TOML
  names in Machine JSON returned success but silently persisted neither field.
- Direct rollout now starts from a fresh Machine GET, includes its
  `instance_id` as optimistic `current_version`, copies the complete config and
  changes only image plus `stop_config`. The local renderer removes unknown
  legacy names and emits only a preserved-config digest, never env values.
- Command success is not deployment truth. A second fresh GET must show a new
  version, exact Machine ID/region, exact non-release config and the intended
  stop contract; only Fly's image digest resolution may differ. Template tests
  and remote-object verification guard distinct links in the chain.
