# 项目日志（活跃时间线）

> 每次会话开头：Claude 读取此文件恢复上下文
> 每次会话结尾：Claude 主动追加本次进展
> 格式：`[TYPE] 内容`，TYPE ∈ {SESSION, DECISION, LEARNING, BLOCKER, NEXT, NOTE}

---

## 2026-04-28

- [SESSION 01] 项目启动会话
- [DECISION] 项目章程固化（PROJECT.md）：
    - Mission：稳定 5-15%/月，研发驱动，研发即学习
    - 平台优先级：Polymarket → 跨平台延后
    - 技术栈：Python 主线，Claude SDK，SQLite+Parquet
- [DECISION] M1 重定位为"市场感知层"，非"扫描器"
    - 基础设施先行，所有策略基于此
- [DECISION] gsd 进展跟踪体系：
    - JOURNAL.md = 活跃时间线
    - threads/ = 主题性长期累积
    - learnings/ = phase 内复盘
    - notes/ = 即兴想法
- [DECISION] Claude 工作模式升级为"主动驱动"：
    - 主动提醒命令、评估进度、预判方向、暴露知识盲点
    - 每个 phase 末用 3-5 个对手测试题
- [LEARNING] 用户学习方式 = 项目开发驱动，反对学院派
    - → 每个 phase 必须产出可运行代码
- [DECISION] Makefile 作为统一命令入口
    - 所有可执行命令暴露 `make <verb>-<noun>` target
    - phase plan 必须显式列出 Makefile target 作为产出
    - 已建 Makefile 骨架 + `make help` / `make status` / `make journal`
- [DECISION] gsd 使用模式 = 复合模式（"研发即研究"模式）
    - 反对默认走 phase 流水线
    - 主干：Workstreams（并行工作流）+ Threads（主题累积）
    - Phase 仅在有明确"完成定义 + 可验证产出"时使用
    - 配套：/gsd-explore（探索）/gsd-plant-seed（前瞻）/gsd-note（即兴）/gsd-add-backlog（积压）
    - 决策树和命令参考已固化到 CLAUDE.md
- [NEXT] 下次会话第一条命令：`/gsd-workstreams`
    - 先建 5 条 workstream（data-collection / market-research / strategy-rd / infrastructure / learning）
    - 再启动 M1-P01 discuss（归属 data-collection workstream）

---

## 2026-04-28 续

- [SESSION 02] Workstreams 建立
- [DECISION] 创建 5 条 workstream：
    - `data-collection` (active) / `market-research` / `strategy-rd` / `infrastructure` / `learning`
    - `.planning/workstreams/<name>/{STATE.md, phases/}` 各自独立
- [LEARNING] gsd-tools `workstream create` 副作用：
    - 首次创建时会把 `.planning/ROADMAP.md` 自动迁移到 `.planning/workstreams/<某>/ROADMAP.md`
    - 每次 create 都会把新建的 workstream 设为 active
    - → 已手动恢复 ROADMAP 至项目根，删除 phantom `milestone` workstream
    - → 已 `workstream set data-collection` 重新激活主战场
- [NEXT] 启动 M1-P01 discuss：`/gsd-discuss-phase`
    - 主题：完整市场快照工具
    - 产出：Makefile target `make snapshot-markets`
    - 归属：data-collection workstream

---

## 2026-04-28 续

- [SESSION 03] gsd 认知重塑 + workstream 重整
- [LEARNING] M1-M5 是**并行能力线**，不是时序里程碑（详见 threads/learnings-meta.md SESSION 03）
- [LEARNING] gsd 在 workstream 模式下不读项目根 ROADMAP；workstream 是并发隔离边界，必须有自己的 ROADMAP 骨架；phase 是动态长出的，按编号身份不按 slug
- [DECISION] Workstream 重命名 / 重组（按能力线切）：
    - 删除：`data-collection` / `strategy-rd` / `infrastructure`（旧命名）+ `market-research` / `learning`（thread 性质，不该是 workstream）
    - 重建为：`m1-perception` (active) / `m2-combinatorial` / `m3-cross-platform` / `m4-smart-strategies` / `m5-industrialize`
    - 每条线一个最小 ROADMAP.md 骨架，让 gsd-tools 能跑
- [DECISION] 删除 `.planning/ROADMAP.md`（项目根） — gsd 在 workstream 模式不读
    - M1-M5 能力线总览迁移到 `PROJECT.md` 的"Capability Lines"小节
- [DECISION] 长出 m1-perception 的 Phase 1：`gsd-tools phase add "完整市场快照工具"`
    - 状态：`disk_status: empty`，目录 `phases/01-/`（slug 为空因中文，不重要）
- [DECISION] CLAUDE.md 同步更新：删除"M1-P01"伪命名，更新文件结构图，更新当前状态区
- [NEXT] 下次会话第一条命令：`/gsd-discuss-phase 1`
    - 主题：完整市场快照工具
    - 产出：Makefile target `make snapshot-markets`
    - Active workstream: `m1-perception`

---

## 2026-04-28 续

- [SESSION 04] Phase 1 discuss 完成 + 套利常识首次系统教学 + threads frontmatter 标准化
- [LEARNING] 教学内容沉淀去向（这次把"教学怎么归档"也建机制了）：
    - **CONTEXT.md** = 给 planner 用的实现要点（精简）
    - **DISCUSSION-LOG.md** = 审计追踪（不被未来 phase 主动读）
    - **`threads/*.md`** = 跨 phase 元知识的真正归档（持续生效）
- [DECISION] Phase 1 - 完整市场快照工具，实现决策固化在 `01-CONTEXT.md`：
    - **A 数据源**: Gamma 全量 + CLOB 顶档（best_bid/ask 价 + 量），双模式（默认 liquidity > $1k 子集 + `--full`）
    - **C 存储**: SQLite (最新覆盖式) + Parquet (单文件 per snapshot, `data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet`)
    - **D 校验**: Phase 1 做 Layer 1+2+4，严格模式 `is_valid=false` 落库 + `validation_issues` 表（带 `category` 根因归类字段）
    - B/E/F 用合理默认值（手动触发、重试 3 次、默认静默 + `--verbose`）
- [LEARNING] 套利常识系统教学（4 道对手测试题）：
    - mid 价 vs ask 价（套利第一红线）
    - 流动性瓶颈 = min(best_ask 量) × 价差
    - 冲击成本 / 为什么不能下市价单
    - 完整性的本质：不是"齐全"，是"能解释每一处缺失"
    - 阈值反模式 vs 规则正模式（告警麻木症 = bot 跑路第一原因）
    - 详见 `threads/market-microstructure.md`（新建）+ `threads/data-quality.md`（追加）+ `threads/learnings-meta.md`（追加 SESSION 04 段）
- [DECISION] thread frontmatter 标准化：
    - 5 个原有 thread（market-structure / oracle-risk / data-quality / learnings-meta / market-microstructure）全部加 gsd-thread 标准 frontmatter
    - 从此 `/gsd-thread list` 能完整显示所有 thread
    - 工作模式锁定：thread 创建用 `/gsd-thread <desc>`，追加用 Edit，关闭用 `/gsd-thread close`
- [LEARNING] gsd-thread 命令不管 append（详见 `threads/learnings-meta.md` SESSION 04 段）
- [NEXT] 下次会话恢复方式：

    **第 1 步（恢复上下文）**：
    ```
    /gsd-resume-work --ws m1-perception
    ```
    `--ws m1-perception` 绕开 session-local 指针失效问题（tmp 文件过期或新 session 没继承），强制路由到 m1-perception 的 STATE.md。

    **第 2 步（执行下一动作）**：
    ```
    /gsd-plan-phase 1 --ws m1-perception
    ```
    Phase 1 准备进入 plan 阶段。
    - planner 读：`01-CONTEXT.md` / `PROJECT.md` / `threads/market-microstructure.md` / `threads/data-quality.md`
    - 产出：`01-{N}-PLAN.md` 多个，分波次（gamma client / clob client / storage / validator / cli）
    - Active workstream: `m1-perception`

---

## 2026-04-29

- [SESSION 05] Phase 1 plan 完成（research + patterns + 5 PLAN.md + checker PASS）
- [DECISION] 因 `gsd-sdk` v0.1.0 接口已变（不再支持 `query init.plan-phase`），手动驱动子智能体编排：phase-researcher → pattern-mapper → planner → plan-checker
- [LEARNING] 研究阶段三大产出（落 phase 目录）：
    - `01-RESEARCH.md`（970 行）— py-clob-client v1 sync + asyncio.to_thread / Gamma `/markets` 限流 300/10s 是真瓶颈 / CLOB `/book` 幽灵 0.01-0.99 价（Issue #180 OPEN）必须用 `get_order_books` + `get_prices` 双源对比 / SQLite WAL + BEGIN IMMEDIATE + DELETE+executemany 替 `INSERT OR REPLACE` / Parquet 显式 schema + tmp+os.replace 原子替换
    - `01-PATTERNS.md` — 32 个文件全部分类，9 个有 `3th-party/polymarket-kalshi-weather-bot/` 结构性参考，23 个 greenfield 直接锚 RESEARCH.md。**反模式被钉死**：SQLAlchemy ORM / 每调用新建 AsyncClient / 异常吞咽 / JSON 字符串当 list 读 — 全部不抄
    - 7 个 open question 全部由 pattern-mapper 给出推荐答案，植入 planner 输入
- [LEARNING] **CLOB Issue #180（幽灵价）是 Phase 1 的最大暗礁**
    - 现象：流动性强的活跃市场 `/book` 返回 0.01/0.99 假价，`/prices` 返回真价
    - 防御：Layer 4 校验里加 `ghost_book` 类目，`abs(book_mid - prices_mid) > 0.05` 触发
    - 套利第一红线（mid vs ask）在生产数据层就已经踩到工程坑，符合"看清市场再下手"原则
- [DECISION] 5 个 PLAN.md 切分（已验证依赖图无重叠）：
    - `01-1` skeleton（wave 1）— pyproject.toml + 5 个包 + config/snapshot.yaml + Makefile target
    - `01-2` clients（wave 2，依赖 01-1）— gamma_client + clob_client（长生命 httpx + aiolimiter + asyncio.to_thread 包同步 SDK）
    - `01-3` storage+validator（wave 2，并行 01-2，依赖 01-1）— SQLiteStore + ParquetWriter + 三层校验 + Category/Issue
    - `01-4` orchestrator+cli（wave 3，依赖 01-2, 01-3）— normalizer + 编排 + typer CLI + Makefile recipe
    - `01-5` tests+integration（wave 4，依赖 01-4）— conftest fixtures + 端到端 mock 测试 + Makefile contract 测试
    - 共 32 个 task，每个 task 都有 `<read_first>` `<action>` `<acceptance_criteria>`
- [DECISION] 7 个 open question 解决方案植入 plan：
    - rate_limiter 不抽出独立文件（aiolimiter 单行内联到每个 client）
    - normalizer 在 Plan 4（orchestrator 用，client 返回原始 dict）
    - config.py 顶级位置 `src/polyarb/config.py`（Plan 1 owns）
    - schemas.py 单文件（Phase 1 一个就够）
    - Layer 2/4 不设阈值（Phase 1 只 Layer 1 strict 翻 is_valid，先观察）
    - Issue + Category 同住 `validator/category.py`
    - `__main__.py` 和 `[project.scripts] polyarb` 双入口都给（Makefile 用 `python -m polyarb.snapshot`）
- [DECISION] checker PASS — 12 个维度全过。5 个 minor issue 不阻塞执行（fetched_at_ms 对 filter 掉的市场语义不准 / yes_token_id 单边顶档 / load_settings 显式 missing 路径行为未定 / 测试 mock side_effect 一次性等），全部 in-flight 解决并写进 SUMMARY
- [NEXT] 下次会话恢复方式：

    **第 1 步（恢复上下文）**：
    ```
    /gsd-resume-work --ws m1-perception
    ```

    **第 2 步（执行 Phase 1）**：
    ```
    /gsd-execute-phase 1 --ws m1-perception
    ```
    - 32 个 task，4 个 wave，自治执行（autonomous=true）
    - 关键投递：`make snapshot-markets` 端到端跑通（mock）+ ghost-book 检测 + SQLite/Parquet 双存储原子写
    - 执行前注意：先确认 venv（`python3 -m venv .venv && source .venv/bin/activate`），Plan 1 T1 跑 `pip install -e '.[dev]'` 会装 py-clob-client / pyarrow / typer / loguru 等

- [SESSION 05 续] Plan 安全 audit + 7 处 surgical fix
- [DECISION] 走 security-auditor 单 agent 审计（不跑全 /review-plan，因为 phase 1 是数据层，安全面有限）
- [LEARNING] audit 结果 = **0 CRITICAL / 1 HIGH / 3 MEDIUM / 3 LOW / 1 INFO**
    - 真值发现：F-1（HIGH）— planner 自己写了 D-D3"校验失败仍落库"原则，但具体代码 `float(asks[0]["price"])` 没套同原则。这种"plan 内部不一致"比"找新威胁"更值钱
- [DECISION] 5 个有效 finding（F-1 至 F-5）直接 surgical edit 进 4 个 PLAN.md：
    - **F-1**：01-4-PLAN.md T2 + 01-3-PLAN.md T5 — try/except 包 float() 异常 → Issue(UNKNOWN)
    - **F-2**：01-2-PLAN.md T2 — `follow_redirects=False` 显式 + `MAX_PAGES=1000` 上限
    - **F-3**：01-1-PLAN.md T3 — Settings `db_path/parquet_root` 加 field_validator 限制项目根目录内
    - **F-4**：01-2-PLAN.md T1 + 01-5-PLAN.md T1 — fixture 序列化用白名单（不用 `o.__dict__`）+ conftest 启动期凭据扫描
    - **F-5**：01-3-PLAN.md T5 + 01-4-PLAN.md T2 — `raw_payload[:1024]` + `detail[:200]` 截断
- [LEARNING] **F-3 与 pytest 的冲突 → env var 逃生口范式**
    - validator 严格不让 `tmp_path`（在 /tmp 不在项目根）→ 测试全炸
    - 解决：`POLYARB_ALLOW_EXTERNAL_PATHS=1` opt-in 绕过，conftest module top 设置，生产代码绝不设
    - 这范式以后多次复用（DB 白名单、URL 白名单、shell 注入防御）→ 已记 threads/learnings-meta.md SESSION 05
- [DECISION] F-6 文档化（json 解析非 retry，docstring 说明）；F-7（lockfile）deferred 到 m5-industrialize 或引入钱包时；F-8 已被 plan 5 T2 test 9 覆盖
- [DECISION] checker 12/12 仍有效 — 没新增 task，只改 task 内部的 action/verify。无需重跑 plan-phase
- [NEXT] 下次会话恢复方式不变（同上 SESSION 05 第 1 步 + 第 2 步），plan 此时是"PASS for execution"

---
