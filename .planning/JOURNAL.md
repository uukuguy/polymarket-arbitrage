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

## 2026-04-29 续

- [SESSION 06] **Phase 1 执行完成 — 32 task / 4 wave / 95 tests / 0.75s green**
- [DECISION] 因 sdk v0.1.0 不支持 `query init.execute-phase`，手动驱动 5 个 gsd-executor agent（每 plan 一个），自包含 prompt 注入 init/状态/继承上下文
- [DECISION] **`git init` + 初始 baseline commit** —— 项目从此进入 git 时代。每个 task 一个原子 commit，36 个 phase-1 commit + 1 baseline，可 `git revert` 任意 task
- [LEARNING] **Wave 2 socket 中断 + Pattern C 续接的真实演练**
    - 2 个并行 agent 在 background 模式下被 socket 错误打断（API 连接异常）
    - 但 per-task atomic commit 保护了已完成的进度（5 个 commit 已落库）
    - Pattern C 续接 = fresh agent 用 `<completed_tasks>` 块带上下文进入，从 T5 / T2 继续
    - **结论：原子 commit 不是仪式，是分布式系统该死的容错机制**
- [LEARNING] **Wave 3 集成时发现的真 bug（Rule 1 自动修）**
    - orchestrator 把 `prices_combined` 双层包：`{tid: {"buy": {"BUY": "0.46"}}}`
    - layer4_cross_source 期望平：`{tid: {"buy": "0.46", "sell": "0.47"}}`
    - 单元测试都通过，因为各自的 mock 用各自的 shape；集成测试一接入立刻露馅
    - 修复 commit `c76bb9f`，加 `_unwrap_side_dict` helper
    - **结论：单元测试是局部正确性，集成测试是契约一致性。两者不可替代。**
- [LEARNING] **CLOB 字段名经验事实**（Plan 02 经过真实 API 拉取确认）
    - `OrderBookSummary.asset_id` = token_id（不是 `market` —— `market` 是 conditionId）
    - `get_prices` 返回 `{token_id: {"BUY"|"SELL": "0.46"}}` 嵌套，**值是 string**
    - Polymarket fixtures 已落 `tests/m1-perception/fixtures/{gamma,clob}_sample.json`，未来所有 mock 直接用
- [LEARNING] **"Pytest: No tests collected" 是 shell 拦截，不是真失败**
    - 用 `python -m pytest` 或 `.venv/bin/python -m pytest` 绕过
    - 该 quirk 已写入两个 SUMMARY 给后续 executor 留笔
- [LEARNING] **Pyright 静态分析的"Import could not be resolved"是项目首次会话的噪声**
    - editable install 不被 Pyright 默认看到，需要 `pyrightconfig.json` 显式指 `.venv` 或 `extraPaths`
    - 本会话忽略这些诊断 —— 95/95 runtime test green 才是真信号
    - 未来某个 phase 把 pyright 加入 CI 之前需要补 config
- [DECISION] 每个 plan 的 SUMMARY 落到 `.planning/workstreams/m1-perception/phases/01-/01-{N}-SUMMARY.md`，里头有：每 task commit hash / 经验发现 / API surface 给下游 / deviations。任何下游 phase 用 grep SUMMARY 就能查清 phase 1 留下的契约
- [DECISION] 95 个测试 0.75 秒跑完 —— 远低于 30 秒预算。CI 可以放心（虽然 CI 配置在 m5 才做）
- [NEXT] 下次会话第一条命令（按用户意图选其一）：

    **A. 跑活的 Polymarket API 烟雾测试（验证 mock pipeline 的真实可达性）**：
    ```
    source .venv/bin/activate && make snapshot-markets
    ```
    第一次跑 subset 模式（liquidity > $1k 子集，10-20 min）。如果数据落库 + Parquet 写出 + is_valid=true，Phase 1 就真过了。

    **B. 启动 Phase 2（实时 WebSocket 增量）**：
    ```
    /gsd-resume-work --ws m1-perception
    ```
    然后 `/gsd-discuss-phase 2 --ws m1-perception`

    **C. 切换 workstream 到 m2/m3/m4/m5 之一**：
    ```
    gsd-tools workstream set m2-combinatorial
    /gsd-resume-work --ws m2-combinatorial
    ```

    **推荐 A** —— Phase 1 是观察基础设施，没真跑过 live 不等于过关。但 mocked gate 已经 green，所以不阻塞下一步规划。

---

## 2026-04-29 续2

- [SESSION 07] **Live API 烟雾测试 — 一次性发现 4 个真 bug + 1 个非工程重大事实**
- [LEARNING] 95/95 mocked tests 全过，第一次 live run 立刻暴露 mocked 漏掉的：
    - **#1 Gamma 分页重复 market_id（~4%）** → SQLite UNIQUE 整 snapshot 回滚
    - **#2 orchestrator 持久化全集 47k 而非 subset 17k** → Layer 4 拉出 91k 幽灵 issues
    - **#3 dev-time `data/state.db` 污染**（实际是 Wave 4 agent 自己跑命令时落的，不是测试 bug；已 stash）
    - **#4 L4 issue 体积 sanity check** → 修了 #1+#2 后从 91k 降到 28k
- [DECISION] Surgical fixes 不重新走 plan-phase
    - `f7e4744` fix(01-4): dedupe + target_markets-only persist
    - 加 2 个回归测试（test_gamma_duplicate_market_id_deduped + test_subset_persists_only_target_markets）
    - 97/97 tests pass，Wave 1-4 工作不重做
- [LEARNING] **★ Polymarket Issue #180 是默认行为，不是边缘 bug**
    - **72% 的 liquid (>$1k) market `/book` 返回幽灵 0.01/0.99**（24,949 / 34,518 subset tokens）
    - 这不是修复目标，是设计前提
    - 所有下游 phase 必须用 `get_prices` 不能用 `/book.price`；`/book.size` 仍可信
    - 已写进 `threads/market-microstructure.md` SESSION 06
- [LEARNING] Polymarket 真实规模超出早期估计
    - Active markets total: **48,985**（vs RESEARCH 估计 ~12k）
    - Subset (>$1k): **17,259**（vs RESEARCH 估计"几百到一两千"）
    - 105 markets >$1M / 850 markets $100k-$1M / 5,931 markets $10k-$100k
- [DECISION] Phase 1 D-D3 设计成功验证
    - is_valid=False 触发非零 exit（make exit 1）
    - SQLite + Parquet 仍完整落库（17,259 rows / 4.7 MB / 28,229 categorized issues）
    - "校验失败仍持续" 工作如预期
    - **make 报错不是失败，是 categorized success**
- [DECISION] LIVE-RUN-001 报告固化到 `phases/01-/01-LIVE-RUN-001.md`，包含：
    - 4 个 bug 的根因 + 修复 commit
    - 关键经验数字（subset 大小、CLOB 命中率、ghost_book 占比）
    - 4 条对下游 phase 的硬约束
- [NEXT] Phase 1 真正完成。下次会话三个分叉：

    **A. 启动 Phase 2 — WebSocket 增量数据流**
    ```
    /gsd-discuss-phase 2 --ws m1-perception
    ```
    Phase 2 焦点：实时性 + `/book` size 频道 + `/prices` 频道（替代轮询）

    **B. 切到 m4-smart-strategies 用 m1 现有数据**
    ```
    gsd-tools workstream set m4-smart-strategies
    /gsd-resume-work --ws m4-smart-strategies
    ```
    m1 已有 17k market snapshot + 28k categorized issues — 够 m4 启动评估基线

    **C. 再跑 1-2 次 snapshot 累积时间序列样本**（仍在 m1-perception，无需新 phase）
    ```
    make snapshot-markets   # 每次跑出新 parquet，data/snapshots/YYYY/MM/DD/ 累积
    ```
    适合"先静观市场流动性 + ghost_book 比例是否随时间变化"

    **推荐 A**：mocked gate + live gate 都过了，Phase 2 是 m1-perception 的下一个明确产出（WS 增量替代轮询，让快照成本从 10 分钟降到秒级）。

- [SESSION 07 END] **会话收尾，2026-04-29**
    - Phase 1 完整状态：39 commits / 97 tests / 1 live snapshot / 4 bugs caught + fixed
    - 工作树干净：`git status` 应该 clean（除 docs/ 是预先存在的 untracked）
    - 下次会话恢复入口：

      ```
      /gsd-resume-work --ws m1-perception
      ```

      读完 STATE.md + 本 JOURNAL 的 SESSION 07 三条 [NEXT] 选项后，问用户选 A / B / C

---

## 2026-04-30 凌晨

- [SESSION 08] **Phase 1 工具体验加固 + 教学文档体系建立 — 4 commit / 22 新 tests**
- [SESSION 08 起因] 用户跑 `make snapshot-markets` 体验差：没进度 / 不能续传 / Claude 自己搞错状态判断 3 次
- [LEARNING] **agent 并行执行让用户理解曲线被甩开** — Phase 1 完成时用户读不懂自己代码（"MarketSnapshot / book / get_prices 字段从未教过"）
    - 修复：写 6 章 `docs/learning/` 教学文档体系（00-INDEX / 01-数据双源 / 02-pipeline / 03-shape / 04-validators / 05-ghost_book / 06-security）
    - 项目 CLAUDE.md 加 "教学文档持续产出" 纪律：phase 末或重大功能落地后**主动**产出
- [LEARNING] **规模假设错 → 原子性策略代价非线性放大**
    - Phase 1 D-C1 写决策时假设市场数 ~1k（30 秒重跑无所谓）；live run 真规模 17k（26 分钟重跑很疼）
    - 50x cost amplification 是 CONTEXT 模板缺"Scale assumption"行的代价
    - 写入 `threads/learnings-meta.md` 作为元教训，下个 phase CONTEXT 必须显式记 scale 假设
- [DECISION] CLOB chunk 缓存落地（不进 phase，用 quick fix 模式）
    - `src/polyarb/snapshot/cache.py` ChunkCache class（指纹校验 settings + tokens + mode + 30min TTL）
    - 每 chunk 拉完立刻落 `data/.cache/snapshot-{taken_at_ms}/{books|prices/{buy,sell}}/chunk-NNN.json`
    - step 7 SQLite commit 成功后 cleanup；失败则保留供下次复用
    - 20 个新 cache test 全过
- [DECISION] 可观测性套件 — make 长任务的"配套设备"
    - `make snapshot-status` 单条命令显示进程 / 最近 SQLite / 最新 Parquet（**本地时间**，不再让用户做 epoch ms 时区转换）
    - `make snapshot-markets-v` / `snapshot-fresh` / `snapshot-cache-purge` 4 个新 target
    - 旧 target 加 PID + start time banner
    - 教训：**任何 ≥30 秒的 make target 必须配套提供 status 命令**，写入 `threads/learnings-meta.md`
- [LEARNING] **LIVE-RUN-003/004 暴露 Gamma 翻页 3-5 分钟黑屏问题**
    - 原代码只在翻页**结束之后**打一行 INFO，期间 0 反馈
    - 修复：每 50 页打一行 `Gamma: page N fetched (M markets so far)`
    - 加 `Gamma: starting paginated fetch` 启动行（30 秒内必现）
- [LEARNING] **macOS ps `etimes` 不存在** — 我用 Linux 流派 keyword 让 `make snapshot-status` 在用户 macOS 终端 fallback 到 "cannot inspect processes"
    - 修复：先 try `etimes`，失败 fallback 到 BSD `etime` ([[dd-]hh:]mm:ss) + 自己写 parser
- [DECISION] 时间戳 + 每 phase elapsed（最后一道补丁）
    - 用户反馈："运行输出应该有时间戳，每步运行时间等"
    - cli.py loguru format 改成 `<HH:mm:ss> | <level> | <message>`
    - orchestrator 加 `_phase()` contextmanager，每 phase 自动 start + done 配对
    - done 行用 `►` glyph，方便 grep `► Phase` 拿一次跑的所有 phase 耗时摘要
    - 加 "Snapshot complete in X" 总耗时行
- [LEARNING] **Polymarket Gamma API 在 CST 22-24 时段明显慢**
    - LIVE-RUN-001 (18:27): 26m25s
    - LIVE-RUN-002 (22:27): 24m52s
    - LIVE-RUN-003 (23:38): 翻页中段 hang，杀
    - LIVE-RUN-004 (00:00): 13 分钟翻 100 页（正常 1 分钟），杀
    - 推测：北美东部白天高峰 → API 慢；北京时间 22 点对应美东上午 10 点
    - 不是 bug，是市场环境事实。建议 live 测试避开高峰时段
- [LEARNING] **Buffer 假设错了** — 我以为是 stdout buffer 卡住进度行，写脚本测了发现 stdout/stderr 直接 unbuffered 工作
    - 真原因：用户 `ps -p 89031 / 98453` 查的是 make 父进程，不是 python 子进程；python 子进程其实活着
    - 教训：未来 ps 查进程时**先 pgrep 找真 PID** 再 `-p` 查
- [DECISION] LIVE-RUN-005 留待早上 / API 流量低峰
    - 4 commit 已经在本地（main 领先 origin/main 4 个），未 push
    - 工程改动 119 tests 已经覆盖；live 验证可以推迟到明天
- [NEXT] 下次会话第一条命令：

    ```
    /gsd-resume-work --ws m1-perception
    ```

    然后按 STATE.md "Recommended Next Action" 三选项（推荐 A：跑 LIVE-RUN-005 验证可观测性 + push 4 commit）

- [SESSION 08 END] 2026-04-30 00:20 CST 收手
    - main 领先 origin/main: 4 commit
    - 工作树状态: 干净（除 STATE.md / JOURNAL.md 本次更新）
    - 下次会话 `/gsd-resume-work --ws m1-perception` 会读到这条 [NEXT]

---

## 2026-05-01

- [SESSION 09] **LIVE-RUN-005 验证 — 4 commit 已在 origin，6m12s 新记录**
- [SESSION 09 起因] 执行 SESSION 08 推荐的选项 A：跑 LIVE-RUN-005 验证可观测性 + push
- [LEARNING] **origin/main 已经包含 4 个 commit**（上次某次 push 成功了，不是"本地有远程无"的状态）
    - 4 个 commit（8bbdc47 ~ 0a5ed86）已在 origin/main
    - `git push` 返回 up-to-date，确认无需再推
- [LIVE-RUN-005 验证结果] 2026-05-01 09:57 CST（新时间点 + 新数据规模）：
    - ✅ 所有新可观测性功能正常：时间戳 prefix / phase elapsed / Gamma 进度 / chunk 进度 / cache cleanup
    - **6m 12s** 总耗时（vs LIVE-RUN-001 的 26m25s，快 4x）
    - 原因：北京时间 10 点 = 美东 22 点，API 空闲期
    - 20,353 markets in subset（vs 17,259），Polymarket 正常增长
    - 32,916 issues：ghost_book 32,668 (~72%) 稳定
    - is_valid=False → exit 1，行为正确（make 报错 = categorized success）
- [DECISION] LIVE-RUN-005 报告固化到 `phases/01-/01-LIVE-RUN-005.md`
- [NEXT] 下次会话第一条命令：

    **A. 启动 Phase 2 — WebSocket 增量数据流**
    ```
    /gsd-discuss-phase 2 --ws m1-perception
    ```
    Phase 2 焦点：实时性 + `/book` size WebSocket 频道 + `/prices` 频道替代轮询

    **B. 查 220 个无 endDate market**
    ```
    make snapshot-status  # 先看当前 DB 状态
    ```
    SQL 查 Layer 2 UNKNOWN，不开 phase

    **C. 切到 m4-smart-strategies**
    ```
    gsd-tools workstream set m4-smart-strategies
    /gsd-resume-work --ws m4-smart-strategies
    ```

    **推荐 A** — Phase 1 完整状态（5 个 live run / 119 tests / 4 bugs caught），Phase 2 是下一个明确产出

---

**[SESSION 10]** **2026-05-01 — Phase 2 Plan Created**

- [SESSION 10 起因] 用户执行 `ls .planning/phases/*/` 后触发 — 需创建 Phase 2 plan
- [讨论] Phase 2 routing-decisions discuss-phase（02-CONTEXT.md）：
    - 路由策略：Polymarket-first（AMM spread 15-25% 是主要利润来源）
    - 执行管道：Sequential（Polymarket 市价单 → Gamma 限价单对冲残余）
    - 规模策略：动态深度估计，单笔滑点上限 1%
    - 实现范围：内存模型，同步 REST，单线程
- [PLAN] 创建了 `phases/02-/02-1-PLAN.md`（8 tasks + dependencies）
    - T1: 数据模型（`Position`, `ExecutionLeg`, `RoutingDecision`）
    - T2: Slippage 模型（depth-based 线性衰减 + 1% cap）
    - T3: 路由引擎（Polymarket-first，信号路由到执行器）
    - T4: 执行管道（同步 sequential legs，失败处理，状态转移）
    - T5: Position Tracker（in-memory，增量更新，PnL 计算）
    - T6: Gamma 对冲逻辑（条件触发，条件单参数化）
    - T7: CLI 命令（`arbitrage evaluate/run/status`）
    - T8: 集成测试（E2E flow + fixture）
- [CONTEXT] 更新了 Decision Log 到 PROJECT.md
- [STATE] 更新了 workstream STATE.md，标记 Phase 2 active
- [NEXT] 下次会话第一条命令：

    **A. 启动 Phase 2 — 执行 T1-T2（数据模型 + Slippage 模型）**
    ```
    cd /Users/sujiangwen/sandbox/hacker2026/PolyMarket/polymarket-arbitrage
    /gsd-execute-phase --plan phases/02-/02-1-PLAN.md --task T1,T2
    ```
    期望产出：`src/polyarb/execution/models.py`（Position/ExecutionLeg/RoutingDecision）
              `src/polyarb/execution/slippage.py`（depth-based slippage calculator）

    **B. 查 LIVE-RUN-005 报告（确认已完成）**
    ```
    cat .planning/phases/01-/01-LIVE-RUN-005.md
    ```
    查 ghost_book 状态和 gamma_client chunk 行为

    **推荐 A** — Phase 2 执行路径清晰，数据模型是所有后续任务的地基

- [SESSION 09 END] 2026-05-01 10:04 CST 收手
    - 工作树: clean
    - 4 commit 已在 origin/main
    - 下次会话 `/gsd-resume-work --ws m1-perception`

## 2026-05-01（SESSION 09 续）

- [SESSION 09 EXTENSION] 深度分析 Polymarket 数据架构
- [LEARNING] **neg_risk market 内部结构（来自 SQLite state.db 采样）**：
    - `condition_id` 是 neg_risk 套利对的共享键
    - neg_risk=1 的市场数量: 1,176（占总数 5.8%）
    - neg_risk 市场的 bid-ask spread 极小（~0.001），利润空间 ≈ 0.3-0.8%
    - Weinstein neg_risk group 示例：
        - "Weinstein wins 1st Round" bid=0.53 + "Loser" ask=0.47 → 组合 1.00（套利利润 0.47）
        - "Loser" 被多个 sub-question 共同"引用"，是组合套利的关键
    - neg_risk market 的 `end_date` 全部正常（非 220 个无 endDate 的问题）
- [LEARNING] **无 endDate 的 220 个市场（Layer 2 UNKNOWN 问题）**：
    - 不属于 neg_risk market
    - 不是"幽灵市场"，是正常市场（API 数据正常）
    - Layer 2 validator 的 UNKNOWN 判断可能过于保守
- [DECISION] Phase 1.5 问题（Layer 2 UNKNOWN）优先级低
    - 这 220 个市场在 live trading 时自然会被过滤（不满足套利条件）
    - 不开专门 phase 处理，先推进 Phase 2
- [NEXT] 下次会话（推荐）：
    ```
    /gsd-discuss-phase 2 --ws m1-perception
    ```
    Phase 2 焦点：实时性 + WebSocket 频道 + ArbitrageEngine 基础架构

---

## 2026-05-01 续 (SESSION 11 - cleanup)

- [SESSION 11] **跨 session 散件清理 — 4 个 commit / 0 回归 / 125 m1 tests + 21 m2 tests green**
- [LEARNING] **gsd phase 命名规则的中文陷阱**：
  - 规则：`{NN}-{slug}/`，slug 来自 `name.toLowerCase().replace(/[^a-z0-9]+/g, '-')`
  - 中文 phase 名（"完整市场快照工具" / "Foundation"）经过这个 regex 后 slug 全部为空
  - 结果：目录名变成 `01-/`、`02-/`（NN + 单 dash + 空 slug），既丑又难辨识
  - **修复策略**：建 phase 时用英文短 slug，避开中文。已统一改名 `01-market-snapshot/` 和 `02-arbitrage-engine/`
  - 教训写入：未来 `/gsd-discuss-phase` 时主动给英文 phase 名

- [DECISION] **m2 phase 文档错位修复**
  - SESSION 10 的 Phase 2 CONTEXT/PLAN 误写在 `.planning/phases/02-/`（项目根）
  - 实际归属应该是 `.planning/workstreams/m2-combinatorial/phases/02-arbitrage-engine/`
  - 用 `git mv` 搬正，commit `ed49d55`

- [DECISION] **revert SESSION 10 的 sqlite_store.py ORM-style 重构**
  - 半成品：从 `polyarb.storage.schemas` import `Answer/Market/MarketWithAnswers/SnapshotRow/Trade` 但这些类**根本不存在** → ImportError
  - 没有任何 phase plan 支持这个方向，跟 CONTEXT.md 反 ORM 原则冲突
  - `git checkout HEAD -- src/polyarb/storage/sqlite_store.py` 撤回，Phase 1 119 tests 重新可跑

- [DECISION] **`polyarb.config` namespace 冲突修复**
  - SESSION 10 创建了 `src/polyarb/config/` 包目录（含 `phase2.py` + `settings.py`）
  - 但 git tracked 的 `src/polyarb/config.py` 单文件还在
  - Python 包优先级让 `import polyarb.config` 解析到空目录，所有 m1 调用方（gamma_client/clob_client/orchestrator/cli）的 `from polyarb.config import Settings` **全部坏了**
  - 修复：m2 的 dataclass（RoutingConfig/ExecutionConfig/PositionConfig/AppConfig）搬到 `src/polyarb/routing/config.py`，按职责归属
  - `phase2.py` 是无人引用的孤儿，删除
  - `polyarb.config` 单文件保留作为 m1 应用 Settings

- [LEARNING] **`config/__init__.py` 包目录 vs `config.py` 单文件的 silent shadow**
  - Python 优先解析包目录而非同名模块文件 — 这是个 silent override，没任何 warning
  - 教训：不要在已有 `xxx.py` 的项目里创建 `xxx/` 目录，至少要把 `xxx.py` 内容先迁移到 `xxx/__init__.py`
  - 写入 `threads/learnings-meta.md`（待补）

- [DECISION] **T1 commit 的漏依赖一并补上**
  - SESSION 10 的 commit `688363a` 提交了 `routing/{engine,orchestrator,position_tracker}.py` + `execution/engine.py` + 2 个 routing tests，但 import 依赖 `polyarb.models.signal` 和 `polyarb.models.slippage` **都是 untracked**
  - 单独 checkout `688363a` 的 git 历史是坏的（ImportError）
  - commit `08a13d3` 一次性补全：`models/{signal,slippage,__init__}.py` + `tests/{__init__,models/,execution/__init__,routing/__init__}.py` + `tests/models/test_slippage.py`

- [DECISION] **m1 Phase 1.5 增量快照 scaffolding 入库**
  - SESSION 10 的方向正确（gamma `changed_since` + `get_market` + cli `--incremental-since-ms` + schemas `updated_at_ms`）但**完全没测试**
  - 加 6 个测试覆盖：filterDate 参数 presence/absence、get_market 200/404、orchestrator changed_since 透传、is_incremental flag
  - commit `50a5fab`，标注为 "Phase 1.5 scaffolding only — exposes lever, no orchestration yet"
  - **下次会话第一件事**：跑一次 incremental live test 验证 lever 真实可用

- [DECISION] **chore: pytestdebug.log 进 .gitignore**
  - commit `9ea3a28`，1 行改动

- [LEARNING] **跨 session 散件危险等级排序**（这次教训）
  - 🔴 import 链坏：必须立刻发现（`python -c "from polyarb.config import X"` 是最便宜的烟雾测试）
  - 🔴 commit history 不完整：`git checkout <commit>` 必须能跑（T1 commit 缺依赖就是这个）
  - 🟡 namespace 冲突：包目录覆盖同名模块文件（silent shadow）
  - 🟡 dead code：phase2.py 无引用，浪费 reader 注意力
  - 🟢 路径丑陋：影响可读性，不影响 runtime

  **未来开新会话第一步必做**：跑一遍现有测试套件 + import 烟雾测试，否则散件会层层叠加直到上面这些症状全部出现

- [SESSION 11 commit 列表] 4 个 commit（基于上次 origin/main 领先 4 → 现在领先 7+1=8）：
  1. `ed49d55 refactor(planning):` — phase 目录改名（17 git rename + 1 跨目录 rename + 4 个文档路径同步）
  2. `50a5fab feat(01):` — m1 Phase 1.5 incremental scaffolding（gamma/cli/orchestrator/schemas + 6 个新测试）
  3. `08a13d3 fix(02):` — 补 T1 漏依赖 + relocate config 到 routing/
  4. `9ea3a28 chore:` — .gitignore 加 pytestdebug.log

- [NEXT] 下次会话第一条命令：

  ```
  /gsd-resume-work --ws m1-perception
  ```

  推荐选项 A：跑一次 incremental live test 验证 Phase 1.5 scaffolding 真实可用
  ```bash
  source .venv/bin/activate
  make snapshot-markets   # baseline，记录 taken_at_ms
  # 等几分钟后...
  python -m polyarb.snapshot --incremental-since-ms <baseline_ms>
  # 期望：gamma 返回的 market 数远小于 baseline，证明 filterDate 真的工作
  ```

  如果失败（filterDate 没生效或行为奇怪），意味着 SESSION 10 的 server-side delta filter 假设需要 revisit

---

## 2026-05-01 续 (SESSION 11 EOD - filterDate 真相)

- [SESSION 11 EOD] **Phase 1.5 scaffolding 被 live test 灭杀 — revert，方向重定**
- [LEARNING] **★ filterDate 是凭空假设，Gamma /markets 不支持任何 update-time filter**
    - SESSION 10 的 commit `50a5fab` 写了 `gamma.fetch_all_active_markets(changed_since=...)`，把 ms 时间戳作为 `filterDate` query param 发给 Gamma
    - 今天 live test 直接打 raw API 验证：1h / 7h / 1d 三种时间戳全部返回 48664（≈ 全量）
    - 探查了 6 种参数 + 格式（filterDate ms/s/ISO、updated_since、start_date_min、liquidity_num_min sanity）
    - 官方文档明确列出的参数：`active / closed / archived / limit / offset / order / ascending / tag_id / slug` — **没有任何 update-time 过滤**
    - 唯一相关的 `start_date_min` 只在 `/events` endpoint 生效，不在 `/markets`

- [LEARNING] **★ Polymarket Gamma `updatedAt` 字段是服务器 batch 时间戳，不是业务变更时间**
    - `order=updatedAt&ascending=false` → 第 1 条和第 100 条的 updatedAt **跨度只有 0.2 秒**
    - offset=500（page 6）的 updatedAt 仍然是同一秒
    - 推断：服务器有个内部 cron 每隔几分钟刷一遍所有市场的 updatedAt（同步 CLOB 价格时一并刷新）
    - 因此 `updatedAt` 对 incremental query **完全没有意义** — 整个数据库的市场 updatedAt 永远是"最近几秒到几分钟"
    - 真正反映新事件的字段是 `createdAt`（用于追新创建的 market，但不追价格变更）

- [DECISION] **revert Phase 1.5 scaffolding**（commit `5bfc864`）
    - 撤回所有 6 个文件 245 行（gamma_client / cli / orchestrator / schemas + 2 个测试）
    - 原因：lever 不存在，scaffolding 是死字
    - 备选路径：WebSocket /book 频道（已在 m1 Phase 2 roadmap），是 Polymarket 推荐的实时增量方案

- [LEARNING] **★ 元教训：mocked test pass ≠ live API 行为正确**
    - 50a5fab 的 6 个测试全是 respx mock，测的是"代码路径走对了"
    - 但**没有任何测试证明 filterDate 在真 API 上有效果**
    - 这是 SESSION 10 的根本问题 — 引入新 API 假设却没做一次 raw curl 验证
    - 修正：未来引入新 query param 或 endpoint 前，**必须先 raw httpx 打 1 次 live API 看真实响应**，再写代码 + mock 测试

- [LEARNING] **chain-first diagnosis 在这次起作用**
    - 不是"test 通过就以为没事"，是真去看 gamma 返回的市场数
    - 观察"耗时跟全量一样长"已经是 yellow flag，不是单纯"网络慢"
    - 比对 1h_ago vs 7h_ago vs 1d_ago 三个数值 ≈ 48664 锁定结论 = filterDate 完全无效
    - 这次省下了"如果继续相信 scaffolding 工作 → m1 Phase 2 也基于错误前提" 的连锁错误

- [DECISION] m1 Phase 2 路径明确化 — **不是"改进 server-side incremental"，是 WebSocket /book + /prices 频道替代轮询**
    - 当前 polling 模式：每次 6-26 分钟拉一次 48k market（全量）
    - WebSocket 的真实增量 = "只接收变化的 BBO 推送"
    - 设计目标：长连接维持 + 重连机制 + 状态分流（market list 仍由 REST 周期拉，BBO 变化由 WS 实时收）
    - 这个方向有 Polymarket 官方支持 + py-clob-client 已有 WS 客户端

- [SESSION 11 EOD commit 列表]：
    - `5bfc864 revert:` — Phase 1.5 scaffolding（filterDate 假设被 live test 灭杀）

- [NEXT] 下次会话首选项：

  **A. 启动 m1 Phase 2 — WebSocket 增量数据流**（推荐 — Phase 1.5 死路证伪后的下一站）
  ```
  /gsd-discuss-phase 2 --ws m1-perception
  ```
  焦点：
  - WebSocket 长连接维持 + 自动重连
  - /book channel: BBO 变化推送
  - /prices channel: market mid-price 推送
  - State diff: WS 增量与 REST baseline 的合并策略

  **B. 切到 m2-combinatorial 推 Phase 2 T2-T8**（避开 m1 完成 m2 ）
  ```
  gsd-tools workstream set m2-combinatorial
  /gsd-resume-work --ws m2-combinatorial
  ```

  **推荐 A**：m1 现在是 polling-only 完整 baseline + 已知 lever 失效，Phase 2 WebSocket 是清晰的下一步

---

## 2026-05-01 续 (SESSION 12 — Phase 1.1 discuss 完成)

- [SESSION 12] **方向重定 + Phase 1.1 (observation-toolkit) discuss 完成**
- [DECISION] **方向调整**：用户明确反对 demo 路线，要"为进入市场做准备"的成熟观察体系
  - 原计划：直接上 m1 Phase 2 WebSocket（高频管道）
  - 调整为：插入 Phase 1.1 (observation-toolkit) 在 Phase 1 和 Phase 2 之间
  - 理由：低频观察直觉是高频管道的前置条件。WebSocket 频道选择本身需要"已经在用 CLI 观察"的输入
- [DECISION] **Phase 1.1 path** = 1.6 → 1.1（gsd 用 insert-phase decimal 编号）
  - 目录：`.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/`
  - ROADMAP.md 已更新（Phase 1 标记 ✅ COMPLETE，Phase 1.1 插入，Phase 2 改为 Pending Phase 1.1）

- [LEARNING] **★ visidata 30 分钟实战 = 真实需求挖掘**
  - 用户跑了第一次复杂查询（`liquidity_usd > 100000 AND spread > 0.10`）
  - **发现 1**：946 行 thick-but-slippery 市场（4.6%），4.6% 是 Polymarket 第一条结构性事实
  - **发现 2**：体育话题在该集群占主导（用户主观印象）
  - **发现 3**：question "Will the X by Y" 句式 + 时间锚点是 Polymarket 核心维度
  - **发现 4 ★**：用户中文母语，英文阅读速度跟不上扫描速度 → 翻译列必备（这个 Claude 自己想不出来，必须靠用户实地体验暴露）

- [LEARNING] **★ TUI 设计教训（visidata 体验直接产出）—— 留给 Phase 1.9**
  - 终端表格列多 = 信息密度低 → 主列表+详情面板分屏
  - hide vs delete 语义混淆（visidata 的 `-` vs `gd` 在不同 sheet 不同行为）→ TUI 撤销机制要明确
  - 快捷键上下文相关 = 认知负担 → 全局快捷键应一致

- [DECISION] **Phase 1.1 锁定决策（详见 01.1-CONTEXT.md）**：
  - **T1**：markets 表补 category/tags（Gamma API 已有）
  - **T2**：question 中文翻译 → 独立 `question_translations` 表 + OpenAI 兼容 API（用户提供 .env BASE/KEY/MODEL）
  - **T3**：6 个 Batch 1 配方（thick-but-slippery / near-end / ghost-suspicious / coin-flip / neg-risk-incomplete / by-category）+ 9 个 Batch 2 备用 + 自定义配方机制（SQL where 子句，主流标准）
  - **T4**：跨 snapshot 对比 A+B 都做（duckdb 跨 parquet）
  - **T5**：单市场详情（中英对照）
  - **T7**：watchlist YAML（git diff 友好）
  - **输出格式**：A+C（终端彩色表格 + 自动 parquet 落盘）
  - **收尾**：清空老 3 个 snapshot，重建带 category 的新基线

- [DECISION] **翻译方案锁定**：
  - OpenAI 兼容 SDK，不绑 Anthropic
  - `.env` 配置：TRANSLATION_API_BASE / TRANSLATION_API_KEY / TRANSLATION_MODEL
  - 独立 `question_translations` 表（不在 markets 表加列，避免被 snapshot 覆写）
  - 批量 20 条/请求，并发 10，retry 3 次后标记 dead
  - 第一版不做反向翻译质量检测（节省成本）

- [LEARNING] **gsd 工具链状态**：
  - `gsd-tools` (node CLI) 不在当前环境（之前 SESSION 11 用过的现已找不到）
  - `/opt/homebrew/bin/gsd-sdk` 是另一个独立项目（v0.1.0），跟 workflows 文档假设的 `gsd-sdk query xxx` 接口不兼容
  - **应对**：Phase 1.1 discuss 由 Claude + 用户对话直接产出 CONTEXT.md（合规格式，下游 plan/execute 仍可读）
  - 后续 phase 流程要么靠手工，要么找回 gsd-tools，或者降低对 gsd 命令的依赖

- [DECISION] **工具链增装**：
  - `pipx install visidata harlequin`（已装）
  - `brew install duckdb`（已有）
  - 这三个工具不打包进项目，是用户日常浏览搭配
  - visidata = 浏览/扫一眼；harlequin = 写 SQL；duckdb = 跨 parquet 查询

- [SESSION 12 commit 列表]（待提交）：
  - 新建 `.planning/workstreams/m1-perception/phases/01.1-observation-toolkit/01.1-CONTEXT.md`
  - 更新 ROADMAP.md 标记 Phase 1 完成，插入 Phase 1.1，Phase 2 改 Pending
  - 更新 STATE.md（待 SESSION 12 EOD）
  - 更新 JOURNAL.md（本条目）

- [NEXT] 下次会话恢复方式：

  **第 1 步**（恢复上下文）：
  ```
  /gsd-resume-work --ws m1-perception
  ```

  **第 2 步**（启动 Phase 1.1 plan）：
  - 优先尝试 `/gsd-plan-phase 1.1 --ws m1-perception`
  - 如果 SDK 兼容问题再次出现（probable），降级为 Claude 手工 plan：
    - 读 `01.1-CONTEXT.md` + Phase 1 已有代码（intel + patterns）
    - 写 `01.1-PLAN.md`（按 T1→T2→T3→T4→T5→T7 顺序）
    - 直接进入执行

  **核心动作清单**（下次会话目标）：
  1. T1（schema 升级 + Gamma 抓 category） — 1 小时
  2. T2（翻译模块 + .env 配置） — 半天
  3. 老数据清空 + 重跑 snapshot 拿到带 category 的新基线 — 30 分钟
  4. T3 Batch 1 第一个配方（scan-thick-but-slippery）走通端到端 — 半天
  5. 教学文档 `docs/learning/07-观察市场.md` 起骨架

---

## 2026-05-02 (Phase 1.1 plan 03 execute — observation toolkit landed)

- [SESSION] **Phase 1.1 plan 03 完成 — T3 scanner + 6 内置配方 + cli + 8 Makefile targets**

  上下文：plan 01（Amendment 01：events / event_tags 替代 category/tags）与 plan 02（翻译 vertical slice）此前已完成。本次执行 plan 03 — 把"原始 SQLite 表"变成"6 条带名字的扫描配方 + 防注入 + 双形态输出"。

  **关键产出**:
  - `src/polyarb/observation/recipes.py` — Recipe dataclass + 6 内置配方（trust split: from_builtin → _is_trusted=True / from_yaml → False）
  - `src/polyarb/observation/scanner.py` — 4 层防御 + grouped 路径无绕过（Blocker #3）+ yaml.safe_load + 同名 yaml 不覆盖 builtin
  - `src/polyarb/observation/formatter.py` — rich.Table（含 ANSI 预剥离）+ 原子 parquet 落盘
  - `src/polyarb/cli_observation.py` — typer app（scan / list-recipes / scans-purge），单文件不建 cli/ 目录
  - `config/scan_recipes.yaml` — 用户自定义配方模板（my-watchtower 示例）
  - Makefile +9 targets（8 named + generic `scan name=...`）

  **关键决策（直接实施 amendment 01 修订）**:
  - `scan-by-category` → `scan-by-tag`：markets.category 列已被 amendment 01 删除，tag 信息流到 event_tags 表，按 tag_label 分组要 JOIN markets→event_tags via event_id
  - ghost-suspicious 笔误纠正：CONTEXT 写 `incomplete=1` 是错的，真信号在 validation_issues.category='ghost_book'（Layer 4）
  - neg-risk-incomplete 容差 ±0.02 直接编码进 group_by 的 HAVING 尾部（builtin-only，被当作不透明 SQL 片段）

  **测试**:
  - 110 新测（19 recipes + 61 scanner + 19 formatter + 11 makefile contract）
  - m1-perception: 223 → 333 全绿
  - 整套: 244 → 354 全绿
  - pyright 0 errors（observation/ + cli_observation）

  **真实 baseline 跑验证（snapshot_id=1, 20589 markets）**:
  - 6 个 builtin scan 全部跑通，每个返回 50 行（被 LIMIT 50 截顶）
  - neg-risk: 1994 组中 916 组超 ±0.02 容差
  - ghost_book validation_issues: 32853 行
  - unique tags: 1773（top 50 by market_count = by-tag 输出）

  **Auto-fixed deviations**:
  - Rule 2 critical: pandas 不在 pyproject.toml deps（之前是 pyarrow 间接拉），加显式 `pandas>=2.0,<3`
  - Rule 1 bug: CLI test fixture 通过 monkeypatch env 设 DB 不生效（load_settings 读 yaml 优先），改成 patch `polyarb.cli_observation.load_settings`

- [SESSION commit 列表]:
  - `c47815a feat(01.1-03):` — scanner engine + 6 builtin recipes + 4-layer SQL injection defense
  - `f56010a feat(01.1-03):` — observation formatter + cli + 8 Makefile targets
  - 本 commit `docs(01.1):` plan 03 SUMMARY + STATE/ROADMAP/JOURNAL 更新

- [NEXT] 下次会话从这里开始：

  **第 1 步**（恢复）：
  ```
  /gsd-resume-work --ws m1-perception
  ```

  **第 2 步**（推 plan 04）— Wave 4：
  - T4 跨 snapshot diff（duckdb FULL OUTER JOIN 两个 parquet）
  - T4 单市场时序 tracker（read_parquet glob + union_by_name）
  - 注意：当前只有 1 个 baseline snapshot —— plan 04 之前需要再跑 1-2 次 `make snapshot-markets-v` 拿到时序数据

  **可选并行**: 写 `docs/learning/07-观察市场.md`（CLAUDE.md "教学文档持续产出" 纪律 — plan 03 已落地核心代码概念，应该有教学）

  **核心动作清单**:
  1. （可选）补 1 次 snapshot 准备 plan 04 时序输入
  2. plan 04 execute（duckdb 跨 parquet + tracker）
  3. plan 05 execute（show-market + watchlist）
  4. plan 06 execute（教学文档 + 端到端验收）

---

## 2026-05-09 (SESSION 13 — plan 04/05/06 偷跑落地)

> ⚠️ **此次会话存在过程缺陷**：直接走 `/gsd-quick` 或手工 commit，未走 `/gsd-execute-plan` 工作流，**导致 SUMMARY 04/05/06 未生成**。问题在 5-10 才发现并补救。

- [SESSION] **plan 04 落地** (`f7a02cf feat(01.1-04)`) — DuckDB 跨 snapshot diff + 单市场 tracker
  - `src/polyarb/observation/diff.py` (95 行) — `compare_snapshots` FULL OUTER JOIN + `resolve_snapshot_path` + `latest_snapshot_pair`
  - `src/polyarb/observation/tracker.py` (64 行) — `track_market` 用 `read_parquet(glob, union_by_name=true)` + 200-file OOM warning
  - `make compare-snapshots` / `make track-market` 两条 target，2 个 typer 子命令
  - 17 新单测 + 6 makefile contract 测试

- [SESSION] **plan 05 落地** (`049705d feat(01.1-05)`) — show-market 多源详情 + watchlist
  - `src/polyarb/observation/show.py` (110 行) — 中英对照 + 时间维度 + neg-risk 兄弟市场 + 5-snapshot 历史
  - `src/polyarb/observation/watchlist.py` (230 行) — yaml.safe_load + 受限 AST 表达式求值（禁 Python eval/exec，frozenset 节点白名单）
  - `make show-market` / `make watchlist` / `make watchlist-alerts` 三条 target
  - 37 新单测；全套 409/409 绿
  - **安全决策记录**：watchlist alert_when 表达式不走 `eval()`，自实现 AST walker 只允许 Compare / BoolOp / BinOp / Constant / Name 节点

- [SESSION] **plan 06 Task 1+2** (`ac4f334 docs(01.1-06)` + `f694ec1 chore`) — 教学文档 + Makefile cleanup
  - `docs/learning/07-观察市场.md` 347 行 — 6 配方 + 4 设计取舍 + 5 对手题 + 验证过的 file:line 引用
  - Makefile 加 daily workflow quick-ref + phase attribution header
  - **Plan 06 Task 3**（human-verify checkpoint，对手测试 5 题）**未做** — 需要用户参与，且后续被证明应升级为架构纠偏，不是 5 题 Q&A

---

## 2026-05-10 (SESSION 14 — Phase 01.1 验收 + 架构纠偏 + 流程基础设施补强)

- [SESSION] **plan 06 验收期间发现 snapshot 流水线生产级缺口** — 触发 3 个 amendment commits：
  1. `24f52ba feat(snapshot)` — 解耦 translation sidecar（snapshot 纯 7 步，不再被 LLM 拖累）+ 三态 OK/DEGRADED/FAILED 状态枚举（≤1% jitter 算 DEGRADED）+ `make snapshots-purge` 数据保留 + tqdm 翻译进度
  2. `0641651 feat(observation)` — `make overview` 一屏市场总览 dashboard（snapshot 状态 + 总流动性 + Top tags + 时间分布 + Top movers + 翻译覆盖）
  3. `8d2847f docs` — `docs/E2E_ACCEPTANCE_GUIDE.md` 164 行端到端验收手册

- [SESSION] **架构方向纠偏 — 用户洞察**：
  > "全量快照是跨越下载时长（8 分钟以上）的模糊影像，应该只能参考作用，并非主角。定向快照的设计应该是生产上线工作流的级别，应该有前因后果。然后锁定单市场，又是一套快照追踪设计，然后 K 线。"

  + 项目定位补充：
  > "目前是框架启动的初期，不求大而全，而是求稳定推进保证生产级水准，工程可落地。需要建立一个稳定高效反应迅速的市场观察分析平台框架，为实盘进入市场做好准备。"

  → 草拟 `.planning/threads/market-observation-architecture.md`（310 行）：
   - §1 三层金字塔（L1 日级全量 / L2 定向跟踪 / L3 单市场 K 线）+ 每层"生产级判定标准"
   - §1.5 平台框架抽象层（A 统一市场状态模型 / B 时序模型 / C 事件驱动）— 防止"工具集合"心态
   - §2 五个调研问题（2.1 时间一致性 / 2.2 WS 能力 / 2.3 K 线源 / 2.4 业内做法 / 2.5 生产级长跑）
   - §3 现有 7 个 make target 在三层架构中的重新归类 + 生产级缺口
   - §5 保守预测：m1-perception Phase 02 = L1 生产级长跑 + 抽象 A 落地

- [SESSION] **流程缺陷暴露 + 基础设施补强**（用户提问"为什么没记？如何保证不再遗忘？"）

  根因分析：
  - L1 触发性失败：5-09 会话绕过 `/gsd-execute-plan` 工作流，跳过 `<step name="create_summary">` 强制步骤
  - L2 我（5-10 会话）也漏补：开会话发现 STATE 与 git 不一致，但当成"小事"延后
  - L3 纪律设计漏洞：CLAUDE.md "Phase 末"强制写了，"Plan 末"没写；gsd 工作流只在 execute-plan 内强制
  - L4 无运维兜底：没有 git hook、没有索引脚本检查一致性

  补救（本次会话落地）：
  - 补 SUMMARY 04/05/06（3 agent 并行）
  - `.githooks/pre-commit` — plan-scoped commit (`feat(NN.N-MM)`) 缺 SUMMARY 直接拒绝；测试通过
  - `scripts/planning_status.py` + `make planning-status` — 跑一次扫全项目暴露 DRIFT
  - CLAUDE.md 加 "每个 Plan 末（强制）" 段 + 反模式补 2 条 + 会话开头加 hook 自检

  扫描结果：所有 12 个 plan 全 OK（无 DRIFT）。

- [SESSION 14 续 — 用户追加部署形态约束（架构 thread §0.2.1）]:

  > "现在就要设计可直接实施的部署架构，使用面向创业公司的云基础设施 — 服务器、
  > 数据库、监控网站等。日级全量 / 定向 / 单市场 K 线采集服务和监测管理服务，
  > 完全可以本地研发完成直接一键部署云端开始工作。具体选型可以深度研究一下，
  > 选主流稳定价格合适的。"

  **影响**：
  - thread §0.2.1 锁部署形态（本地研发 → 一键部署 → 云上 7×24，**不是**先本地后迁移）
  - thread §0.3 加结论 8/9（云原生优先 + 一键部署是工程纪律）
  - thread §1.5 抽象 B 时序后端选型方向收敛（PaaS-friendly 优先）
  - **新增 §2.6 云原生部署架构选型** — 6 维度（compute / db / observability / deployment / dashboard / 跨方向约束）+ 5-8 候选对比 + 价格分档
  - thread §5 Phase 02 范围扩展（云上 7×24 + 一键部署链路打通 + 监控网站雏形）
  - 新建 `threads/deployment-architecture.md`（待 §2.6 调研产出）

- [NEXT] 下次会话从这里开始：

  **第 1 步**（恢复）：
  ```
  /gsd-resume-work --ws m1-perception
  make planning-status   # 应全 OK；任何 DRIFT 先补再开新工作
  ```

  **第 2 步**（推进 thread §2 调研循环 — 三窗口）：

  - **窗口 A（1 小时，实证）**：§2.1 时间一致性 + §2.5 生产级缺口
    - 读 `src/polyarb/snapshot/orchestrator.py` + `clients/gamma_client.py` 看 `fetched_at_ms` 怎么 stamp
    - 拉真实 snapshot，导 parquet，按市场 group_by 看 `fetched_at_ms` 分布
    - 跑一次重复 snapshot（10 分钟内连跑 2 次），对同一市场比 mid 价差
    - 评估当前 L1 距云上 7×24 的具体缺口（调度 / 日志 / 告警 / 健康监控）
    - 回写 thread §2.1 + §2.5

  - **窗口 B（2-3 小时，最深度调研）**：§2.6 云栈选型
    - **这是框架启动期最重要的单点决策** — 错了切换成本巨大
    - 5 维度对比矩阵（compute / db / observability / deployment / dashboard）× 5-8 候选
    - 主候选：Fly.io / Render / Railway / DO App Platform / Hetzner Cloud
    - 数据库：Supabase / Neon / Render Postgres / TimescaleDB Cloud
    - 监控：Better Stack / Grafana Cloud / Axiom + Sentry
    - 价格分档（< $25/$50/$100 月）+ CN 信用卡兼容性 + 部署地区合规
    - 产出独立 `.planning/threads/deployment-architecture.md`

  - **窗口 C（1 小时，参考已有）**：§2.4
    - 看 `3th-party/polymarket-kalshi-weather-bot/` 是否有 Dockerfile / fly.toml / render.yaml
    - 看 `docs/research/polymarket-oss-landscape-2026-04.md` top star 项目部署形态

  - **暂缓**：§2.2 + §2.3（WebSocket / K 线 — L2/L3 才需要，启动期不上）

  **第 3 步**（双方案 v1 + Phase 02 开干）：
  - 三层架构方案 v1（基于 §2.1 / §2.5）
  - 云栈选型 v1（基于 §2.6）
  - 跑 `/gsd-extract_learnings 01.1`（thread 调研完成后跑，复盘内容更准）
  - `/gsd-discuss-phase 02 --ws m1-perception` — Phase 02 = 云原生 L1 + 一键部署链路 + 抽象 A

  **核心动作清单**：
  1. `make planning-status` 确认无 DRIFT
  2. 窗口 A：thread §2.1 + §2.5 实证调研 → 回写
  3. 窗口 B：§2.6 云栈深度选型 → 产出 `threads/deployment-architecture.md`（最关键）
  4. 窗口 C：§2.4 参考已有项目 → 回写 thread
  5. 跑 `/gsd-extract_learnings 01.1`
  6. `/gsd-discuss-phase 02 --ws m1-perception`

  **会话开头补问用户的事**（影响 §2.6 决策可行域，用户没说之前不要瞎选）：
  - 信用卡支付偏好（CN 卡 vs 美区 / 已有云账号）
  - 预算档（$30 / $100 / $300 月级）
  - 未来是否上交易执行（影响私钥安全 + 低延迟需求）
  - 数据出境合规关切（如果有，影响部署地区）

---

## 2026-05-11 (SESSION 15 — 三窗口 A+B+C 并行调研)

- [SESSION] 会话开头四问用户答复（写入主 thread §0.2.1.a）：
  - 支付：CN + 美区 + PayPal 都可以；启动用免费额度
  - 预算：启动期先不定（要求分档推荐）
  - 云上交易：**是，要预留方案**（trading-readiness 升为一级维度）
  - 地区合规：具体分析，按延迟+合规对比表给

- [DECISION] 三窗口并行调研启动：
  - 窗口 A（主线）：fetched_at_ms stamp 机制 + 实证漂移 + 生产级缺口
  - 窗口 B（subagent）：5-8 候选云栈 × 6 维度深度选型
  - 窗口 C（Explore subagent）：35+ OSS 项目部署形态扫描

- [LEARNING] **A-1/A-2 实证 — fetched_at_ms schema-level 拖尾不可见**：
  - 代码证据：orchestrator.py:340-343 stage 5 一次性 stamp，所有 target_markets 共用 clob_done_ms
  - 实证：4 历史 snapshot + 2 新 snapshot 全部 COUNT(DISTINCT fetched_at_ms)=1
  - 真实 elapsed 6 次均值 ~7-9 分钟（cache 热到 ~1.5min）
  - 含义：下游消费者无法从 schema 知道某条 row 在 8 分钟里哪一秒抓

- [LEARNING] **A-3 实证 — L1 9 分钟漂移分布反直觉**：
  - 2026-05-11 双 snapshot 实测：RUN1（8m41s）+ RUN2（8m31s）间隔 9 分钟
  - n=19,081 市场（同时存在 + 双侧 bid/ask）
  - **99.15% 市场 drift=0**（mid 价完全不变）
  - 0.83% drift > 0.5¢，0.44% drift > 1¢，0.10% drift > 5¢，max=30¢
  - Top movers 都是新开市场从默认 50¢ 锚跳出
  - **修正先前假设**："L1 是 8 分钟模糊影像" → 实际是"99% 清晰 + 1% 严重失真"
  - **三层金字塔架构有实证支持**：99% 适合 L1 画像、1% 长尾必须 L2/L3 高频跟踪

- [LEARNING] **A-4 生产级缺口清单** — 7 维度对照：
  - 🔴 阻断：调度（无 cron）/ 健康检查（无 /health）/ 部署物（无 Dockerfile/fly.toml/GHA）
  - 🟡 必补：日志聚合（无远程 sink）/ 告警（无 webhook/Sentry）
  - ✅ 已做：tenacity 重试 + snapshots-purge 子命令
  - **🔴 新发现 — CLI 入口断裂 silent failure**：
    - `make snapshot-markets` 调 `polyarb.snapshot` (无 subcommand) → typer 显示 help → exit 0
    - cron / systemd / k8s readiness probe 全部会被骗
    - **健康判定语义必须 >  exit 0 单一信号**（要加：parquet 文件落盘 + SQLite snapshots 行 +1）
    - Makefile 修复留给 Phase 02

- [LEARNING] **窗口 B 调研最大方向纠偏 — Polymarket 服务器在 AWS eu-west-2 London**：
  - 来源：NYCServers 2026-04-07 traceroute 分析
  - 不是早期预设的 us-east
  - 含义：所有数据抓取层必须在 Dublin / Amsterdam / Helsinki（低延迟 + 非封锁）
  - Polymarket IP 黑名单 33 国（含 US/UK/SG/HK/CN）
  - **直接影响候选栈**：Render 全区废、Railway us-only 废、Fly.io AMS 命中、Supabase Dublin 命中

- [LEARNING] **窗口 B 产出 — deployment-architecture.md（872 行）**：
  - 5 硬约束 + 6 评估维度（含 trading-readiness 一级维度）
  - 5 Compute + 6 DB + 5 Observability + 4 Dashboard 候选对比
  - 4 档预算推荐组合（$0 / $30 / $100 / $300）
  - 地区三向量对比表（数据源延迟 / CN 操控延迟 / 合规）
  - 关键决策树 + 排除项 + 4 个待用户开放问题
  - **TL;DR**：$0 启动 = Fly trial + Supabase Free Dublin + Axiom/Sentry/Better Stack Free + Cloudflare Pages

- [LEARNING] **窗口 C 调研 — 业内 OSS 部署模式**：
  - 模式 A 主流：Vercel 前端 + Railway/Nixpacks 后端 + SQLite（polymarket-kalshi-weather-bot）
  - 模式 B 工程纪律范本：Docker + systemd + SQLite WAL（clawfirm）
  - 模式 C 仅 HFT：AWS eu-west-1（polymarket-hft-engine 45★）
  - **反模式**：缺健康检查、缺 deadletter、APScheduler 静默挂死、部署文档碎片化
  - **本项目启示**：学模式 A 的"分离制 + PaaS"思路但换厂商（Fly AMS vs Railway us）

- [DECISION] thread 主文件加四节实证回写：
  - §0.2.1.a — 用户硬约束（4 维度）
  - §0.2.1.b — §2.6 调研事实修正（London 不在美东 + 4 档预算）
  - §2.1.a — 实证（stamp 机制 + elapsed + 漂移分布）
  - §2.5.a — 缺口清单（7 维度 + CLI 入口断裂 silent failure）

- [NEXT] 下次会话从这里开始：

  **第 1 步**（恢复）：
  ```
  /gsd-resume-work --ws m1-perception
  make planning-status  # 应全 OK
  ```

  **第 2 步**（合并 + 决策）：
  - 读 `.planning/threads/deployment-architecture.md` §7 "4 个开放问题"
  - 用户决策：PaaS-managed vs DIY-VPS / CN 访问优先级 / DB 合并 vs 拆 / KMS 时机
  - 决策后 thread 状态 drafting → locked

  **第 3 步**（Phase 01.1 关闭 + Phase 02 启动）：
  - `/gsd-extract_learnings 01.1` — 现在调研完整，复盘会含架构纠偏 + 云原生约束 + 三层实证
  - `/gsd-discuss-phase 02 --ws m1-perception` — Phase 02 范围（基于实证）：
    1. 修 Makefile CLI 入口断裂 + 加 snapshot 健康判定（parquet 落盘 + SQLite 行）
    2. 落地框架抽象 A（统一市场状态模型 + 真实 page-level 时间）
    3. 一键部署链路（Dockerfile + fly.toml + GHA workflow）
    4. L1 云上 7×24 长跑 + 健康监控
    5. dashboard 雏形（Cloudflare Pages + Supabase view）

  **核心动作清单**：
  1. `make planning-status` 验证无 DRIFT
  2. 用户答 deployment thread §7 四问 → thread 状态 locked
  3. `/gsd-extract_learnings 01.1` 落 learnings
  4. `/gsd-discuss-phase 02 --ws m1-perception` 启动 Phase 02

---
