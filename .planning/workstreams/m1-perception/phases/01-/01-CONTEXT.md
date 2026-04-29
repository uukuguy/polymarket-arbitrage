# Phase 1: 完整市场快照工具 - Context

**Workstream:** m1-perception
**Gathered:** 2026-04-28
**Status:** Ready for planning

<domain>
## Phase Boundary

**交付**：一个一次性"拍快照"工具——给定时刻 T，拉取 Polymarket 整个市场宇宙的状态切片，落到本地能查询。

**核心能力**：
- 一次 `make snapshot-markets` 调用 → 拉取全量 Polymarket 市场（Gamma 12k+ + CLOB 顶档）
- 落入 SQLite (热) + Parquet (冷) 双存储
- 完整性校验，明确告知哪里不全 + 根因归类

**不属于本 phase**：
- 实时流（WebSocket 增量）→ Phase 2
- 异常检测（YES+NO≠1 等业务规则）→ Phase 3
- Dashboard / 可视化 → Phase 4
- Subgraph / 链上 RPC 数据 → 后续 phase 按需加
- 定时调度（cron / systemd timer）→ 不在 m1-perception 范畴，进 m5-industrialize

**第一性目的**：观察能力。"你不能管理你看不见的东西。"snapshot 是后续所有能力（异常检测、回测、对手分析）的训练数据 + 对照基线。

</domain>

<decisions>
## Implementation Decisions

### A. 数据源覆盖度

- **D-A1**：本 phase 拉取 **Gamma API 全量 + CLOB 顶档**
  - Gamma：所有 active markets 的 metadata + mid_price + liquidity_usd
  - CLOB：best_bid / best_ask 的**价格 + 量**（仅顶档，不拉多档深度）
- **D-A2**：CLOB 拉取支持双模式
  - 默认：仅 `liquidity_usd > $1000` 的市场子集（预计数百到一两千个，10-20 分钟跑完）
  - `--full` 标志：全量 12k+ 市场（1-2 小时，受 CLOB 限流约束）
- **D-A3**：本 phase **不**接入 Subgraph、Polygon RPC
  - 但 schema 设计需为后续接入预留扩展点（如 market_id 跨源 join 字段）

**为什么**：
- 仅 Gamma 不够——Phase 3 异常检测必须用 ask 价计算"真实可执行套利空间"，否则全是 mid 价的幽灵机会
- 全量 CLOB 多档深度本 phase 不需要——能识别"机会大小天花板"（best_ask 量）就够了，多档是 Phase 3 自己的事
- 子集模式覆盖了真实可吃的市场（< $1k 流动性的市场即便价格再奇怪也下不进多大单子）

### C. 存储模型

- **D-C1**：**方案 1** — SQLite 放最新一份 snapshot（覆盖式更新）+ Parquet 是历史归档
  - SQLite 角色：高频实时查询（"现在某个市场状态"），服务 Phase 3 异常检测、Phase 4 dashboard
  - Parquet 角色：批量分析（时间序列回放、回测、训练数据），用 DuckDB 查询
- **D-C2**：Parquet 文件粒度 — 单文件 per snapshot
  - 路径：`data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet`
  - 写入 append-only，失败回滚直接删文件
  - 后期可做 compaction 合并大文件，但本 phase 不需要
- **D-C3**：SQLite 主表至少包含
  - `markets`：每条记录是一个市场的最新状态（市场元数据 + Gamma 价 + CLOB 顶档 + 流动性）
  - `snapshots`：snapshot 元数据表（id, timestamp, mode (subset/full), market_count, is_valid, parquet_path）
  - `validation_issues`：校验失败明细（snapshot_id, layer, category, market_id, raw_payload）
  - 具体字段在 plan 阶段定型

**为什么**：
- 冷热分离的本质是**让每种查询用对的工具**，不是按"新/旧"切——单点 SQLite，扫描 Parquet
- 方案 2（所有 snapshot 进 SQLite）会让表迅速膨胀到上亿行，列式聚合性能崩塌
- 方案 3（SQLite 只做 catalog）让 Phase 3 的实时检测每次都先开 parquet，对高频访问太重
- 单文件 per snapshot 写入最简单、失败回滚最容易，DuckDB 跨文件查询毫无性能负担

### D. 完整性校验

- **D-D1**：本 phase 实现的校验层
  - **Layer 1 数量校验**：API 报告的 active markets 数 vs 落库数，必须严格相等
  - **Layer 2 字段完整性**：每条市场必有核心字段（market_id, question, yes_token_id, no_token_id, mid_price, liquidity_usd, end_time）；缺字段标记 `incomplete=true` 不丢弃
  - **Layer 4 跨源一致性**：Gamma 有但 CLOB 找不到订单簿 → 标记 `clob_missing`
- **D-D2**：本 phase **不**实现的校验
  - **Layer 3 业务一致性**（YES+NO≠1 等）→ 推到 Phase 3（异常检测的核心工作）
  - 历史漂移检测（"今天市场数比昨天少 N 个"）→ 推到 Phase 3
- **D-D3**：失败处理策略 — **严格 + 标记**
  - 校验失败的 snapshot 仍然落库（带 `is_valid=false` 标志），不丢数据
  - `validation_issues` 表记录每次失败的细节
  - 退出码非零 + stderr 打印失败摘要（cron / make 能 catch）
  - 日志醒目，避免麻木症
- **D-D4**：`validation_issues` 表必须有 `category` 字段做**根因归类**
  - 已知类目示例：`zombie_market`（流动性极低）、`resolving`（即将结算）、`api_jitter`（API 抖动）、`api_unreachable`、`clob_missing`、`unknown`
  - 新发现的根因模式立即手工补到归类，让 `unknown` 不断收敛到具体类目
  - 这是把"调查根因"机制化进系统的最小动作 — 不容忍持续 unknown

**为什么**：
- 完整性的本质不是"数据齐全"，是"知道哪里不齐全以及为什么"
- 阈值是反模式（"缺 < 10 个就忽略"会演化成温水煮青蛙的告警麻木）
- 规则是正模式（"缺市场必须能归类，否则 fail"强制理解）
- IMDEA 论文里跑路的 bot 大多败给自己的告警麻木，不是市场

### B. 调度（默认值，未深度讨论）

- **D-B1**：手动触发为主，`make snapshot-markets` 跑一次落一次
  - 不引入 cron / systemd timer / 定时器
  - "持续在线"是 Phase 2 WebSocket 的工作
  - 工业化定时（M5）才考虑

### E. 失败恢复（默认值，未深度讨论）

- **D-E1**：API 调用失败重试 3 次（指数退避 1s / 2s / 4s）
- **D-E2**：3 次仍失败 → `validation_issues` 标 `api_unreachable`，整体 snapshot `is_valid=false` 落库
- **D-E3**：**不**做"部分成功补拉"
  - 原因：snapshot 必须代表同一时间点 T，事后补拉的数据是 T+N 的状态，混入会污染时间一致性
  - 一致性 > 完整性 — 缺数据可补，时间漂移不可逆

### F. CLI 输出形态（默认值，未深度讨论）

- **D-F1**：默认静默成功（单行总结）
  ```
  OK | 12345 markets | mode=subset | 0 issues | 1.2GB → data/snapshots/2026/04/28/10-00-00.parquet
  ```
- **D-F2**：`--verbose` 显示进度条 + 分阶段耗时
- **D-F3**：`is_valid=false` 时 stderr 打印失败摘要 + 受影响 market 数 + 主要 category

### Makefile 入口（必产出）

- **D-MK1**：`make snapshot-markets` — 默认子集模式
- **D-MK2**：`make snapshot-markets-full` — 全量模式（等价 `python -m polyarb snapshot --full`）
- **D-MK3**：每个 target 上方注释说明用途和典型场景

### Claude's Discretion（planner 可决定）

- 模块拆分（gamma client / clob client / storage / validator 各自的边界）
- 配置文件格式（YAML 是 PROJECT.md 锁定栈，但具体 schema 待定）
- 限流实现（sleep-based / aiohttp-throttle / 自实现 token bucket）
- 进度条库选择（tqdm / rich.progress）
- 测试策略（fixture 选择、是否打 API mock）
- 日志框架（loguru 是用户偏好，但是否引入待 plan 决定）

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### 项目宪章
- `.planning/PROJECT.md` — Mission、约束（技术栈 Python 3.12+ / SQLite / Parquet / YAML）、能力线总览
- `CLAUDE.md` — Claude 角色契约、Makefile 入口约定、gsd 工作模式

### 跨能力线累积
- `.planning/threads/market-structure.md` — 当前对 Polymarket 市场结构的理解（本 phase 实现会反哺这里）
- `.planning/threads/data-quality.md` — 数据质量踩坑记录（本 phase 必读，本 phase 也会贡献）
- `.planning/threads/learnings-meta.md` — 元教训（套利从业者思维方式）

### 第三方参考资源
- `3th-party/polymarket-kalshi-weather-bot/` — 完整 Python 实现参考（已 clone）
- `3th-party/clawfirm/` — 仅 .whip 编排层，下单实现缺失（不直接依赖）
- `docs/research/polymarket-oss-landscape-2026-04.md` — 35+ 开源项目调研报告
- 推荐 clone（plan 阶段决定）：`py-clob-client`（官方 Python SDK）、`polyclaw`、`pmxt`

### 关键事实备忘
- 2026-02 Polymarket 移除 ~500ms taker 延迟 → MM 风险升、HFT 门槛降
- IMDEA 论文：86M 笔交易、$40M 套利、Top 3 钱包合计 $4.2M（Subgraph 派生）
- Paris 吹风机事件：oracle 单点风险真实存在 → 链上数据未来必须接入

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

本 phase 是项目的第一份生产代码，目前 codebase 主要是 .planning/ + Makefile 骨架。

- `Makefile`（项目根）— 已有 `help` / `status` / `journal` target，本 phase 需要新增 `snapshot-markets` / `snapshot-markets-full`
- `3th-party/polymarket-kalshi-weather-bot/` — 已 clone 的 Python 套利参考实现，可借鉴其 Polymarket 客户端代码（仅借鉴，不直接 import）

### Established Patterns

无（本 phase 立基线）。这个 phase 的代码组织方式会成为后续所有 m1-perception phase 的样板，需要质量优先。

### Integration Points

- 项目根 `pyproject.toml` 不存在（待 plan 阶段创建）
- 入口模块路径建议（plan 阶段定型）：`src/polyarb/snapshot/`（snapshot 工具）、`src/polyarb/clients/`（Gamma / CLOB 客户端）、`src/polyarb/storage/`（SQLite + Parquet 抽象）
- 配置目录建议：`config/snapshot.yaml`（liquidity 阈值、API endpoint、限流参数）

### 技术栈约束（PROJECT.md 锁定，不可调整）

- Python 3.12+
- 不引入 LangChain/LangGraph 等中间层框架
- LLM 调用 = Anthropic SDK 直调（本 phase 暂不需要 LLM）
- SQLite (热) + Parquet (冷) + YAML (配置)
- Rust 升级仅当实盘数据证明需要时

</code_context>

<specifics>
## Specific Ideas

### 教练角色固化的套利常识（本 phase 设计基础，必读必用）

**1. ask 价不是 mid 价，套利计算必须用 ask 价**
- Gamma 给 mid 价，看起来漂亮但下不到单
- 真实可成交价是订单簿 best_ask，可能比 mid 高 2-5%
- Phase 3 异常检测公式：`real_arb_space = best_ask(YES) + best_ask(NO) - 1`，不是 mid 之和

**2. 流动性瓶颈 = min(best_ask 量) × 价差**
- best_ask 那一档的量是"机会大小天花板"
- 瓶颈在窄边（YES 簿和 NO 簿哪边量小），不能取大边
- 真实套利者口头禅：**"价差是利润，深度是上限。两个都得有。"**

**3. 冲击成本是隐形税**
- 大单往订单簿深处吃，每多一档价就更差
- 大资金从来不下市价单，用 limit + 分单
- Phase 1 至少要给出 best_ask 量，让 Phase 3 能识别"机会天花板"

**4. 一致性 > 完整性**
- snapshot = 时刻 T 的市场切片
- 部分失败补拉 = 时间漂移（T 与 T+N 混合），比缺数据更危险
- 缺数据可标记可调查，时间不一致不可逆

### 测试用市场样本（plan 阶段参考）

- 高流动性主流市场（成交量大、订单簿厚）：用作"主流路径"测试
- 低流动性 zombie 市场：用作 `zombie_market` 归类测试
- 即将结算的市场（end_time < 24h）：用作 `resolving` 归类测试
- negrisk group 市场（多合约同事件）：用作跨市场关系测试预留

</specifics>

<deferred>
## Deferred Ideas

跨 phase / 跨能力线被识别但不在 Phase 1 范围的想法。

### 推到 m1-perception 后续 phase
- **CLOB 多档深度抓取**：本 phase 顶档够用，多档进 Phase 3（异常检测时按需二次拉取）
- **Subgraph 历史 trade 接入**：对手钱包分析能力，独立 phase 启动（可能 m1-perception Phase 3+ 或独立子 phase）
- **链上 RPC 接入**：CTF 余额、UMA 仲裁状态——风控必须接，但不是观察层第一优先级
- **历史漂移检测**：跨 snapshot 对比（"今天比昨天少 100 个市场"）— 推到 Phase 3
- **negrisk group 关系图**：当前先写进 `threads/market-structure.md` 持续累积理解，到必要时再开 phase

### 推到其它能力线
- **定时调度**（cron / systemd timer）→ m5-industrialize
- **24h 不间断守护进程化** → Phase 2 (WS) 启动时考虑，定时部署进 m5
- **告警 / on-call 通知**（Slack / 邮件 / PagerDuty）→ m5-industrialize
- **Dashboard / 可视化 / TUI** → m1-perception Phase 4

### 跨 phase 的元学习（已记入 threads/）
- gsd 工作机制（路径解析、phase 编号身份、workstream 隔离）→ `threads/learnings-meta.md` 已记
- 套利系统数据完整性哲学（规则 vs 阈值）→ 本 CONTEXT 的 `<specifics>` 段已固化，后续 phase 也会用

</deferred>

---

*Phase: 01-完整市场快照工具*
*Workstream: m1-perception*
*Context gathered: 2026-04-28*
