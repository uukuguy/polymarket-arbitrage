# Project Charter: Polymarket Arbitrage

> 项目章程 — 锁定核心目标、约束、技术决策。重大调整需要明确决议。
> Last updated: 2026-04-28

## Mission

通过研发驱动（development-driven learning）的方式，构建一套智能体辅助的 Polymarket 套利系统，最终实现 **+5-15%/月** 的稳定风险调整后回报，并使开发者本人成为系统化的预测市场套利从业者。

## Core Principles

1. **研发即学习** — 知识在解决具体工程问题中被吸收，反对纯学院派阶段
2. **看清市场再下手** — 市场感知层是一切策略的底座，不在感知不完整时盲目实现策略
3. **代码是主线，paper 是验证** — paper/live 是同一份代码的运行模式，不是项目阶段
4. **质量门控制节奏** — 不按日历推进，按"代码达到何种状态"推进
5. **进展持续可追** — 任何会话都能从 .planning/ 恢复完整上下文
6. **知识缺陷主动暴露** — Claude 教练角色，每个 phase 末用对手测试发现盲点

## Capability Lines（M1-M5 = 5 条并行能力线，不是时序里程碑）

不是"做完 M1 才做 M2"。M1-M5 是同时演进的 5 条能力线，最终合在一起服务于稳定回报这一整体目标。
每条线对应一个 gsd workstream（`.planning/workstreams/m{N}-{slug}/`），各自独立长 phase。

| 能力线 | Workstream | 职责 | 当前重点 |
|---|---|---|---|
| M1 市场感知 | `m1-perception` | Gamma/CLOB/异常检测/Dashboard，所有策略的底座 | 主战场，正在长 Phase 1（snapshot tool） |
| M2 Combinatorial | `m2-combinatorial` | IMDEA Type 2 跨市场组合套利 | 等 m1 接口可用 |
| M3 跨平台 | `m3-cross-platform` | Kalshi 数据源 + Polymarket × Kalshi 套利 | 等账户开通 + 合规判断 |
| M4 Smart Strategies | `m4-smart-strategies` | LLM 驱动的价值判断（weather/news/MM rebate） | 等 m1 数据沉淀 + 评估基线 |
| M5 工业化 | `m5-industrialize` | 部署/监控/风控/可观测性 | 任何线证明需要时启动 |

**演进规律**：
- 每条线"够用了"就允许下游线启动，不要求"完成"——M1 永远在演进
- 能力线之间的依赖是软依赖（接口可用即可），不是硬时序
- 跨能力线的知识/观点 → 进 `threads/*.md`，不进任何 workstream

## Constraints (Hard)

- **平台优先级**：Polymarket 数据/执行先彻底打通；Kalshi（m3）等账户和合规判断
- **资金**：m1-perception 阶段全程 paper；实盘门槛由各能力线自身的质量门触发，不按时间
- **风险接受**：oracle 操纵、周期性 drawdown、监管风险均已知并接受
- **技术栈**：
  - 主线：Python 3.12+
  - LLM：Claude (Anthropic SDK，直接调，不引中间框架)
  - 存储：SQLite (热) + Parquet (冷) + YAML (配置)
  - 数据流：内存 + Redis（仅当跨进程时引入）
  - Rust 升级：仅当实盘数据证明 latency edge 真实存在时

## Success Definition (Long-term)

不是"一年内赚到 $X"，而是：
- ✅ 拥有一套**可解释、可调参、可扩展**的套利系统
- ✅ 每个策略有**完整回测 + paper + 实盘三段证据**
- ✅ 风控经过故意触发验证，从未被动失血
- ✅ 知识体系完整：能向第三方解释任何决策的依据
- ✅ 实盘运行 6+ 个月，月度 PnL 中位数为正

## Roles

- **User**：套利从业者，AI 工程师，决策者，资金提供者
- **Claude**：项目经理 + 架构师 + 教练。主动驱动进展、提醒命令、评估进度、暴露盲点。

## Meta-Protocol（Claude 工作模式）

每次会话：
- **开头**：从 `.planning/JOURNAL.md` 恢复上下文，明确告知"上次到哪、本次该做什么、用什么命令"
- **进行中**：决策点主动给出权衡，不让用户在不知情时选错；教学优先于复制粘贴
- **结尾**：更新 JOURNAL，给出"下次会话第一条命令"

每个 Phase 末：
- 强制 `/gsd-extract_learnings`
- 提 3-5 个对手测试问题，发现知识盲点立即补
- 评估完成度，决定是否进入下一 phase

## Decision Log（重大决策固化在此，时间倒序）

### 2026-04-28: M1 定位 = 市场感知层（非"扫描器"）

- **决策**：M1 不是写"找机会的扫描器"，而是构建对 Polymarket 整个市场的实时、完整、准确视图
- **依据**：用户指出"建立简捷高效准确的市场信息收集和分析能力，应该是首先要做的"
- **影响**：所有后续策略（M2+）都建立在 M1 产出之上，是基础设施而非业务

### 2026-04-28: 技术栈锁定 Python 主线

- **决策**：M1-M3 全部用 Python 实现，包括决策层与执行层
- **依据**：py-clob-client 最成熟、研究迭代速度优先、用户为资深 AI 工程师
- **后门**：保留接口抽象，M4+ 实盘数据若证明 latency edge 存在，hot loop 单独 Rust

### 2026-04-28: 进展跟踪体系

- **决策**：充分用 gsd 全套（thread / workstreams / learnings / intel / journal）
- **强制**：JOURNAL.md 是"活跃时间线"，每次会话开头恢复、结尾更新
