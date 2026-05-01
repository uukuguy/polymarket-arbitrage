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

- [SESSION 09 END] 2026-05-01 10:04 CST 收手
    - 工作树: clean
    - 4 commit 已在 origin/main
    - 下次会话 `/gsd-resume-work --ws m1-perception`

---
