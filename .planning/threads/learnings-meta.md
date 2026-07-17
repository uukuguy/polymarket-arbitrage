---
slug: learnings-meta
title: Project Meta-Learnings (项目方法论 / gsd 认知)
status: open
created: 2026-04-28
updated: 2026-04-29
---

# Thread: Meta-Learnings

> 项目方法论级别的反思 — 不是套利知识，而是"我们怎么做这个项目"的经验。

## 2026-04-28 启动期

### 关于学习方式

- ❌ 错的设计：先学 3 个月知识 → 再写代码（学院派）
- ✅ 对的设计：写代码遇到问题 → 学最小必要知识 → 继续推进
- 用户反馈："我极其擅长在项目开发中学习"

### 关于 paper / live 的关系

- ❌ 错的认知：paper 是阶段，跑完才能上 live
- ✅ 对的认知：paper / live 是同一份代码的运行模式，由质量门切换

### 关于 M1 的定位

- 第一冲动是写"扫描器"（直接出结果）
- 用户拨正：先建"市场感知层"，把眼睛装上再说手脚
- 教训：基础设施被低估时，所有上层都是脆弱的

### 关于 Claude 角色

- 用户明确要求：主动驱动，不被动响应
- 这意味着 Claude 是项目经理 + 架构师 + 教练，不是仅仅是写代码的助手
- 每个会话有义务：恢复上下文 / 推进进展 / 暴露盲点 / 更新记录

## 2026-04-28 SESSION 03 — 关于 gsd 的认知重塑

研发推进中第一次深读 gsd 源码，纠正多个错误模型。这一组教训务必内化，避免重蹈：

### 1. M1-M5 是并行能力线，不是时序里程碑

- ❌ 错：把 M1-M5 当成"做完这个再做下一个"的产品阶段
- ❌ 错：给 M1 写"完成定义"和"进入 M2 的条件"
- ✅ 对：M1-M5 是 5 条同时演进的能力线，每条对应一个 gsd workstream
- ✅ 对：M1（市场感知）永远不"完成"，"够用了"就允许 M2 启动
- 用户原话："M? 不是里程碑，是并行在做的几条线，要的是它的功能，能支持我们的整体目标"

### 2. workstream 不能按职能切，要按能力线切

- ❌ 错的命名：data-collection / market-research / strategy-rd / infrastructure / learning
- ✅ 对的命名：m1-perception / m2-combinatorial / m3-cross-platform / m4-smart-strategies / m5-industrialize
- 原因：workstream 是 gsd 的并发隔离边界（独立 STATE/ROADMAP/phases），按能力线切才能让"M2 启动后继续推进 M1"这样的并行清晰落地
- 跨能力线的认知 / 观点 / 元学习 → thread，不是 workstream

### 3. gsd 在 workstream 模式下不读项目根 ROADMAP

- 事实：`planningDir(cwd)` 在 workstream 模式下永远返回 `.planning/workstreams/{active}/`
- 所有读 ROADMAP.md 的代码（phase.cjs / roadmap.cjs / verify.cjs / state.cjs）都走这条路径
- 项目根的 `.planning/ROADMAP.md` **完全是死文件**，留着只会让 gsd 行为出奇怪
- ✅ 操作：删根 ROADMAP，把"产品宪章/能力线总览"放进 PROJECT.md

### 4. workstream 预先创建是为了"并发隔离"，不是浪费

- workstream 持有自己的 STATE.md（当前 phase / 续点）+ ROADMAP.md（phase 队列）
- 即便没有 phase 也要建 ROADMAP.md 骨架——`phase add` 第一行就是 `ROADMAP.md not found` 检查
- 多个 Claude 实例可以 `--ws m1-perception` 和 `--ws m2-combinatorial` 并发跑互不干扰

### 5. Phase 是动态长出的，不预先列

- ❌ 错：开 workstream 时把 P01-P06 全列在 ROADMAP 里
- ✅ 对：`gsd-tools phase add "..."` 按需长出，整数顺序自动 +1
- ✅ 对：`gsd-tools phase insert <after> "..."` 自动算小数位（5.1, 5.2...）
- 删除 phase 5 时 gsd 自动 renumber 后续（6→5, 7→6...）
- 研发本来就不会整齐，gsd 设计为这种"中途插队、推翻重来"的现实服务

### 6. Phase 编号才是身份，slug 只是注释

- gsd 的 `generateSlugInternal()` 只保留 `[a-z0-9]`，中文 phase 名 → slug 为空 → 目录是 `01-/`
- 第一反应是觉得这"很丑"，想改。错。
- phase 的真实身份是编号（1, 1.1, 2...），所有 phase 操作（discuss/plan/execute）都按编号走
- "phase 在做什么、为什么" → 在 ROADMAP 的 phase section、CONTEXT.md、PLAN.md 里承载
- 目录 slug 是辅助标签，空也无所谓，不影响任何功能
- 用户原话："gsd 的 Phase 都是编号，我们的研究开发过程，phase 不会是一直计划好整整齐齐的"

### 元教训

读完整个子系统再判断，不要凭片段直觉。这次为了"项目根 ROADMAP 该不该留"反复折腾了 3 轮，本质都是没读透 `planningDir()` 的路由逻辑。
**新规：在改动 gsd 文件结构前，先确认 gsd-tools 在 workstream 模式下实际读哪条路径。**

## 2026-04-28 SESSION 04 — 套利从业者的反射动作

Phase 1 discuss 期间通过对手测试题实战训练出的元纪律。这些不是知识点，是**反射动作**——遇到对应场景应该不假思索就这么反应：

### 1. "任何缺失都假设它在告诉你什么"

snapshot 漏 5 个市场不是"算了"。第一反应必须是：
- 哪 5 个？
- 为什么是这 5 个？
- 是已知类目（zombie / resolving / API jitter）还是 unknown？

unknown 类目持续增长 = 系统在欠债。每次 unknown 出现，都是一次"调查根因 → 升级归类系统"的机会。

### 2. "调阈值前先查根因"

任何"调高阈值容忍它"的冲动，都是告警麻木症的入口。规则取代阈值，强制理解，不要用麻木换便利。

### 3. "用 ask 价复算才算真机会"

任何用 mid 价识别的"机会"都是候选。下单决策必须用 best_ask 价 + 量复算。这是套利从业者第一红线，永远不要跨。

### 4. "瓶颈在窄边"

做组合套利必须两边数量相等，最大可吃量取决于窄边的 ask 量。"价差是利润，深度是上限。两个都得有。"

### 5. "snapshot 必须代表同一时间点"

部分失败补拉 = 时间漂移 = 数据真相被污染。一致性优先于完整性 — 缺数据可补，时间错位不可逆。

---

## 2026-04-28 SESSION 04 — 关于 gsd-thread 的认知

补充 SESSION 03 的 gsd 学习：

### gsd-thread 实际管什么
- ✅ 创建 thread（`/gsd-thread <desc>` 生成 frontmatter 模板）
- ✅ 列表（`/gsd-thread list`，读 frontmatter status / updated / title）
- ✅ 状态查看（`/gsd-thread status <slug>`）
- ✅ 关闭（`/gsd-thread close <slug>` → status=resolved + git commit）
- ✅ 恢复（`/gsd-thread <slug>` 加载到上下文）

### gsd-thread 不管什么
- ❌ 追加内容（没有 append 命令；需 Write/Edit 直接写）
- ❌ 跨 thread 搜索（没有索引）
- ❌ 强制 body schema（除 frontmatter，body 是自由 markdown）

### 工作模式
- 创建 thread → `/gsd-thread <desc>` 让 gsd 生成 frontmatter 骨架
- 追加内容 → Claude 直接 Edit 写
- 关闭 → `/gsd-thread close <slug>` 触发 git commit
- 列表 → `/gsd-thread list` 用户随时能查看主题清单

### 必须有的 frontmatter
```yaml
---
slug: thread-slug
title: 人类可读的标题
status: open | in_progress | resolved
created: YYYY-MM-DD
updated: YYYY-MM-DD
---
```

**没有 frontmatter 的 thread = 对 gsd-thread 工具链不可见**。SESSION 04 把项目原有 5 个手工建的 thread 都补上了 frontmatter，从此 `/gsd-thread list` 工作。

---

## SESSION 05 — Phase 1 plan-then-secure 流程（2026-04-29）

### gsd-sdk v0.1.0 接口已变 — workflow 自动化跑不通
- 旧 workflow（`$HOME/.claude/get-shit-done/workflows/plan-phase.md`）依赖 `gsd-sdk query init.plan-phase`，新版 sdk 只剩 `run / auto / init`
- 解决：手动驱动子智能体（gsd-phase-researcher → gsd-pattern-mapper → gsd-planner → gsd-plan-checker），跳过 sdk 自动化层
- 这种"workflow 描述失效但底层 agent 还在"的情况以后大概率重复 — **直接给子 agent 写自包含 prompt 比修 sdk 更省事**

### Plan-checker 和 review-plan 是不同维度
- **gsd-plan-checker**（每次 plan-phase 必跑）= field 完整性、依赖图、wave 正确、硬规则合规 — 这是"plan 能不能执行"的检查
- **/review-plan 或 单独安全审计**（可选）= 架构风险、隐藏暗坑、安全威胁 — 这是"plan 应不应该执行"的检查
- 默认信 checker。但如果是项目第一份生产代码（会成为后续 sample）、或 schema 不可逆，就值得加一轮安全 audit
- 经验法则：第一个 phase + Phase 3 异常检测 + 涉及钱包私钥的 phase 必加 audit；其它走 checker 就够

### Pattern-mapper 主动识别 anti-pattern 的价值
- 这次 pattern-mapper 不仅找 analog，还主动识别了参考实现 `polymarket-kalshi-weather-bot` 里的 4 个反模式（SQLAlchemy ORM / 每调用新建 AsyncClient / 异常吞咽 / JSON 字符串当 list）并钉在 PATTERNS.md
- planner 看到后直接在 PLAN.md 里写"DO NOT copy per-call ..."
- 这种"反向参考"比单纯指 analog 更有用 — 因为新人 / 新 phase 容易抄错 reference

### Security audit 在 plan 阶段做的边际收益
- 1 HIGH（F-1 float() 异常未捕获，违反自身 D-D3）+ 3 MEDIUM（redirect/path/fixture）+ 3 LOW + 1 INFO
- HIGH 是 audit 真值发现：planner 自己谨慎写了 D-D3"校验失败仍落库"，但具体代码写法忘了同样原则套到 `float()` 异常
- **结论：plan 阶段安全 audit 主要价值不在"找新威胁"，而在"找出 plan 自己内部的不一致"**
- 修 7 处 surgical edit，比 refactor 已写代码省 10x

### F-7 lockfile 触发条件（待 trigger）
- 当前：py-clob-client `>=0.34.6,<0.35` 范围安装，无 hash pin
- 风险接受：单人本地工具，供应链攻击概率低
- **重新触发审视的 trigger**：(1) 加 CI 时（m5-industrialize），(2) 引入钱包 / 私钥 / 任何写入操作时，(3) 升 py-clob-client v2 时
- 默认动作：那时切到 `uv lock` + `uv sync --frozen`

### F-3 path validator 与 pytest 的冲突解决
- 第一次写 validator 直接 reject `tmp_path`（pytest fixture 永远在 /tmp 之外）
- 解决：env var 逃生口 `POLYARB_ALLOW_EXTERNAL_PATHS=1`，conftest 在 module top 设置
- **设计原则**：security 默认严格，测试通过显式 opt-in 信号绕过。绝不让生产代码读这个 env var
- 同样的模式以后会反复用（数据库白名单、文件路径、URL 白名单、命令注入防御）— 记下范式

## 2026-04-29 LIVE-RUN-002 (CST 22:27 那次)

### 规模假设错 → 原子性策略代价非线性放大
- Phase 1 D-C1 决策"markets 表必须原子覆盖"，CONTEXT 写时假设市场数 ~1k（30 秒跑完，重跑无所谓）
- live run 真实规模 17k subset / 49k full —— 一次 snapshot ~26 分钟
- **没有 cache + 不能续传**让"原子性"代价从 30 秒 → 26 分钟（~50x 放大）
- 教训：**任何 atomicity / persistence 决策必须显式记"假设的工作单元规模上限"**。CONTEXT 模板加一行"Scale assumption: ~N units, ~T seconds per run"
- 触发回看：当 phase 实际规模 > 假设规模 2x 时，自动重审 atomicity 策略

### 可观测性是状态判断的根，不是 UX 装饰
- 这次 Claude 自己搞错了 3 次状态判断：
    - 第 1 次把 SQLite id=1 的旧记录当成"刚跑完的本次"
    - 第 2 次以为 stash 掉了 db（其实 db 留着）
    - 第 3 次 epoch ms 解析错时区
- 根因都是**没有 `make snapshot-status` 这种"一条命令告诉我现在状态"的工具**
- 用户和 Claude 都靠"猜 + 拼凑 ps + sqlite + ls" 拼真相
- 教训：**任何 ≥30 秒的 make target，必须配套提供 status 命令**（推断"现在哪个阶段"、"何时启动"、"最近一次何时跑完"）。如果适用，加 cancel / resume

### make 是壳子不是流水线
- `make snapshot-markets` 当成"一条命令搞定"是错的认知
- 正确认知：长任务 = 进程 + 状态文件 + 中间产物；make 只是入口
- **下次给任何 long-running make target 时，配套设计**：
    - `make {verb}-{noun}` 入口（quiet 模式，cron 友好）
    - `make {verb}-{noun}-v` 详细模式（人盯着等）
    - `make {verb}-status` 状态查询
    - 如果可恢复，`make {verb}-resume` 续传
    - 如果可取消，`make {verb}-cancel` 优雅中止
- Phase 1 后置补这套已经在做（snapshot-markets-v / snapshot-status / [TODO] snapshot-resume）

### 教学文档必须主动产出，不能等用户问
- agent 并行执行让代码进度跑过用户理解曲线
- 用户读不懂自己代码 = 教学失败 = CLAUDE.md 第 1 条原则"研发即学习"被违反
- 这次用户主动指出"那些字段从未教过"才暴露
- 已写入项目 CLAUDE.md "教学文档持续产出（教练角色的具象化）"小节作为长期约束

---

## 2026-05-19 SESSION 21 — Plan-Code 沉默分叉 18 天（m2 slippage.py 考古案例）

### 现象

用户在 SESSION 21 准备启动 m2 T2 (Slippage Model) 时让 Claude 摸状态，发现：

- `src/polyarb/models/slippage.py` 320 行已落地，4 个测试全 green
- 但代码内部是 `SlippageParams` + dataclass + **CLOB↔PM 双场所 fee differential** 模型
- 现行 `02-1-PLAN.md` Task T2 写的是 **`SlippageModel.estimate_slippage(token_id, side, size, depth_curve)` + `PolymarketDepthCurve` Protocol + 依赖 m1 `OrderBookSummary`/`GhostBookAnalyzer`** 的另一套设计

**plan 描述的 T2 ≠ 代码实现的 T2 ≠ JOURNAL 记录的 T2**。三份描述里至少两份是不同形态。代码挂在 git 里 **18 天没人看过**（2026-05-01 落地 → 2026-05-19 才被注意到）。

### 考古结论（用 git + JOURNAL 还原）

| 时间 | 事件 |
|---|---|
| 2026-05-01 SESSION 10 上午 | Phase 2 discuss → CONTEXT.md + 02-1-PLAN.md 落地。**当时**的 T2 设计是 "depth-based 线性衰减 + 1% cap"（见 JOURNAL 第 418 行） |
| 2026-05-01 15:31 (T1 commit `688363a`) | Claude 写了 `routing/engine.py` / `execution/engine.py`，但**漏 `git add` 了它们依赖的 `models/signal.py` + `models/slippage.py`**。`git checkout 688363a` 拉一份干净仓库 → ImportError |
| 2026-05-01 SESSION 11 (清理) | Claude 发现 import 链坏，commit `08a13d3` 一次性补齐 slippage.py + signal.py + 测试。**没有重新对照 plan 验证设计语义** |
| 2026-05-01 ~ 2026-05-19 | m1 主线（Phase 01.1 → Phase 02 Wave 1-5）吞掉所有注意力。m2 没人回头 |
| 后期某次 session（git 上没找到对应 plan-rewrite commit） | **plan 文件 02-1-PLAN.md 的 T2 描述被改成依赖 L2 的版本**，代码没跟着改 |
| 2026-05-19 SESSION 21 | 用户问 "M2 是啥" → Claude 摸状态 → 三份描述对不上 → 触发本次考古 |

### 根因

**多层失守，没单点责任**：

1. **commit 漏文件** — Claude 在 SESSION 10 写代码时 `git add` 不严谨，T1 commit 缺依赖；可怕的是 `git checkout 688363a` 跑不起来这件事**直到 SESSION 11 才被发现**
2. **散件清理只验证表面** — SESSION 11 教训写了"🔴 import 链坏：必须立刻发现 / 🔴 commit history 不完整：`git checkout <commit>` 必须能跑"（见 `learnings-meta.md` SESSION 11 段），但**没把"代码跟 plan 对得上"列入清理项**
3. **plan-code 漂移无检测** — 测试套件只验证 "import 不爆 + 当前代码逻辑自洽"，**测试通过 ≠ plan compliance**。Claude 写测试时是测的"当前代码做什么"，不是测"plan 要求什么"
4. **plan 被默默改写** — 某次 session 把 T2 plan 改成依赖 L2 的版本但没改代码也没记 JOURNAL，等于在没人审计的情况下变更契约
5. **沉默时间放大** — workstream 切换（m1 主线吞注意力 18 天）让分歧从"代码 ≠ plan"变成"无人记得 plan 是什么时候改的"

### 工程教训

- **测试套件不是 plan compliance 的 gate**。测试只能证"代码自洽"，不能证"代码符合 plan"
- **`/gsd-resume-work` 切到一个 workstream 时，必须扫这条线"plan vs code 漂移"**，不是只看 STATE 和 JOURNAL。具体动作：每条 active task 的 plan body 描述里点名的核心 class/function，grep 一下代码里有没有同名实体；找不到 = 漂移嫌疑
- **plan 文件被改写要留 trace**。改 plan 跟改代码一样严重；plan 是契约，悄改契约 = 后患。规则：plan body 改动必须在文件头加 `Revision: YYYY-MM-DD - <one-line reason>`，并在 JOURNAL 留 `[PLAN-REVISION]` tag
- **commit 完成度自检**：每个 commit 之后跑 `git stash && git checkout <just-committed-sha> -- <changed-paths> && uv run python -c "<import smoke test>"`。SESSION 11 已经写过这条教训但没落到 hook，现在加一条 [TODO] 给未来 hook 化（参见下方"待补的 hook"）
- **workstream 切换前必查 plan-code 一致性**。这次是 m1 把 m2 晾了 18 天才暴露；如果 18 天扩到 6 个月，slippage.py 可能直接被新人当成"祖传代码"接受了

### 修法（这次本身的 immediate action）

1. ✅ 这次先**搞清楚现状**（本节）
2. ⏳ 决定 T2 走向（见 02-07 收尾后的 followup 决策点 — 三选一：冻 M2、重定义 T2 接受现实、跳 T2 推 T3/T6）
3. ⏳ 在 02-1-PLAN.md 加 "Revision History" 头部段，把现状跟 plan 的不一致明文写出（不是悄悄改 plan 让它对上代码）
4. ⏳ 把 "plan-code drift detection" 列为 gsd-resume-work 的辅助检查（开 ticket，不在本会话做）

### 待补的 hook（项目层面）

- pre-commit 已经检查 SUMMARY-per-plan，可以再加一条 **plan-revision-trace**：plan 文件改动且没在 file head 加 `Revision:` 行 → 阻断
- gsd-resume-work skill 加一步 **workstream drift scan**：扫 active phase 所有 plan 的 `<tasks>` 块里点名的 file path，验证存在；点名的 class/function symbol，用 codegraph_search 验证存在 + signature 大致对得上

### 沉默成本估算

slippage.py 320 行 + 测试 49 行 = ~370 行未对齐代码挂了 18 天。如果今天没人考古，下次 m2 真启动时直接基于这份代码搭 T3-T8，等于把"分叉的设计"焊死到下游 routing/execution 里 — 撕一次成本 X，焊死后撕成本 5X+。这次成本是 30 min 考古；不查的成本是 2-3 个会话拆错代码。

便宜的 audit 是最划算的工程动作。

---

## 2026-05-19 SESSION 21 — Alert chain "HTTP 200 = user notified" 幻觉

### 现象

SESSION 20 (5-18) Plan 02-05 SUMMARY 自信地写："**Four redundant alert paths verified live in prod: a daemon failure now produces (1) Sentry email, (2) Better Stack incident email, (3) Telegram direct message — independently delivered**"。

SESSION 21 (5-19) 跑 Chaos Inj 1 (Fly machine stop 2 min)，**4 条 alert path 全部静默** — Gmail 没 Better Stack 邮件，Gmail 没 Sentry 邮件，Telegram 没消息。诊断脚本确认：

- Better Stack `/fail` POST → **200 OK**（signal accepted）
- Telegram `sendMessage` 直连 → **200 OK**（real send，用户手机真收到）
- `make alerts-test`（走 `send_paused_alert` 完整链路）→ **用户什么都没收到**

### 根因（5 层）

详见 `02-SOAK-LOG.md` Inj 1 段落。核心是 3 层"HTTP 200 = user notified"假设错：

1. **Better Stack `/fail` 返回 200 ≠ 用户被通知** — 200 只表示 signal accepted；是否 fanout 到邮箱/Telegram 取决于 BS 的 on-call/escalation 配置。我们的配置是默认 "primary responder + email"，但 primary responder 邮箱可能没确认。
2. **Sentry `capture_message` 成功 ≠ 用户收到邮件** — Sentry alert rule 配的是 "Notify Suggested Assignees"，issue unassigned 时无人通知。
3. **`send_paused_alert` 的 Telegram 是 fallback only** — `if not bs_ok: telegram_direct(...)`。BS 200 → TG 不发。两个"200 不代表用户被通知"叠加，等于全静默。

### SESSION 20 的验证幻觉是怎么形成的

那次"E2E verified" 是这么做的：

| 通道 | 验证手段 | 等价于什么 |
|---|---|---|
| Sentry | **Sentry dashboard 上手动点 "Send Test Issue"** | 验证 Sentry 自家 test 按钮邮件，不验证 alert rule routing |
| Better Stack | **手动 POST `/fail`** + 在 dashboard 看 incident 出现 | 验证 BS UI 显示 incident，不验证 on-call routing 真发邮件 |
| Telegram | daemon 的 alerts-test 路径 | **运气好** — 当时可能因为 BS 返回了 5xx（首次 incident？quota？）触发了 fallback，所以 TG 真收到；但 SESSION 20 没记录 BS 那次实际返回什么 |

每一条都验证了"消息通道存在"，**没一条验证"用户在真实故障路径下会被通知"**。

### 工程教训（核心）

#### Lesson 1: 监控 / 告警 / 通知是三个不同的层，分别验证

```
监控层  : 系统有没有事件发生  → /health / heartbeat / probe
告警层  : 事件有没有被记录到告警系统  → Sentry event 存在 / BS incident 存在
通知层  : 用户有没有真的收到通知  → 邮箱/手机/Telegram 真的有消息
```

**HTTP 200 只能证监控+告警层通了**，证不了通知层。

#### Lesson 2: 告警系统 alert path 的验证不能用"测试按钮"，必须 end-to-end chaos

测试按钮（Sentry "Send Test Notification" / BS "Test alert"）走的是简化路径，跟"真故障 → 告警系统 → 通知用户"的真实流程**经常不一样**。例如：
- Sentry 测试按钮直接发邮件给当前登录账号
- 真 issue 触发要先匹配 alert rule + 匹配 issue owner / member → 任何一步不对就跳过

**唯一可靠的验证 = 故意制造真故障 + 等真告警送达**。这就是 chaos injection 的本质价值。

#### Lesson 3: alert 链路必须有**至少 1 条无条件、不需要远端配置**的兜底路径

把 Telegram direct 改成无条件主路径（绕过 Sentry/BS 的远端配置）就是这个原则：
- Sentry/BS 任一是"远端配置可能漂移"的——dashboard 可能被人改、邮箱可能被人 unverify、alert rule 可能被人改、quota 可能耗尽
- Telegram bot token + chat_id 是"本地 secret 控制"的——只要 Fly secret 不动，bot 永远能发到固定 chat
- 设计原则：**至少有一条 user-facing channel，它的可达性只依赖本地 secret + Telegram API 自身，不依赖任何第三方监控系统的远端配置**

我们的修正：`send_paused_alert` 改为 Sentry + BS + **无条件 Telegram**。前两者是"试一下"，TG 是"必须送达"。

#### Lesson 4: SUMMARY 里说 "verified" 必须附验证手段

SESSION 20 的"E2E verified"是欺骗性陈述。从今往后任何 SUMMARY 里出现 "verified"/"E2E"/"tested" 时必须**附验证手段**：

| 不可接受（含义模糊） | 可接受（验证手段明文） |
|---|---|
| "alert path E2E verified" | "alert path verified: triggered real daemon FAILED via fly machine stop, observed Telegram msg 22:35 UTC, observed Gmail incident email 22:36 UTC" |
| "Sentry email delivered" | "Sentry email delivered via 'Send Test Notification' button (NOT via real issue)" — 注意这里要老老实实 |
| "Telegram works" | "Telegram works: chat_id=6319452645 received message_id=6 timestamped 23:06:22 UTC" |

### 修法

- ✅ 代码层 (`alerts.py`): Telegram direct 升无条件主路径
- ✅ 测试守住新契约 (`test_alerts.py` 加 BS 200 时 TG 必须发的 case)
- ✅ SOAK-LOG 写下 Inj 1 完整诊断 + 重新设计 Inj 2-4 触发条件
- ⏳ 配置层（用户自助）: Sentry alert rule + Better Stack on-call routing 修
- ⏳ Plan 02-07 Task 4 待回头改：Inj 2-4 的设计要让 `send_paused_alert` 真触发（连续 3 次 FAILED → PAUSED → alert），不能假设单次 DEGRADED 也触发

### 沉默成本估算（这次）

如果 Phase 02 走 7-day soak gate 而不是 chaos injection 关闭，**这个 bug 几乎 100% 不会暴露**：
- 7 天里很可能不会有真故障
- 即使有故障，daemon 失败 3 次自愈即可，scheduler 可能根本没 PAUSED
- 即使 PAUSED 触发 send_paused_alert，BS 返回 200 → fallback TG 不发 → SESSION 20 同款幻觉重演

**意思是 Phase 02 本来要"shipped with confidence"基于一个完全坏的 alert chain**。修这个 bug 的真实救助价值：未来某次真 oracle 操纵/系统挂掉时**不会错过 alert**，对一个套利系统来说这是几千刀 vs 一无所知的差别。

chaos injection 不是"演习"，是"对抗 SUMMARY 自我欺骗"的唯一手段。

---

### Sub-lesson 5 (2026-05-20): SDK 静默 no-op 是隐形成本最高的 bug

修完 Telegram 无条件路径后，Sentry email 仍然不发。诊断从"alert rule 配错"猜了一阵子，最后发现真根因：

**`make alerts-test` Makefile target 直接调 `send_paused_alert`，但漏调 `init_sentry()`**。`sentry_sdk.capture_message` 在未 init 的进程里**静默返回 None**，不报错、不 raise、不打 warning log。Daemon 进程没这个问题 (`main.py` 里 init 了)，但 ad-hoc 测试脚本会漏。

**关键模式 — SDK 静默 no-op 的危险等级排序**：

| Bug 类型 | 怎么发现 | 隐形成本 |
|---|---|---|
| 🔴 SDK 没 init → capture 静默丢 | 只能通过 dashboard 反查 event 数 | 极高，看不到 |
| 🔴 网络层 200 但 server 未通知 user (BS /fail) | 验证用户感知 (打开邮箱/手机) | 高 |
| 🟡 SDK init 了但 DSN 错 | SDK 本身会 log "invalid DSN" | 中 |
| 🟡 alert rule 没匹配 | dashboard activity log 能看 | 低 |
| 🟢 SMTP 投递失败 | dashboard 显示 "send failed" | 低 |

**工程教训**：

1. **任何走 SDK 的 ad-hoc 测试 target / 一次性脚本，必须显式 init SDK** — Makefile / scripts/ / docs 里的 "quick test" 命令是高危区，因为它们 **不走主 daemon entrypoint**
2. **SDK 应该有 "lazy fail" 而不是 "silent no-op" 默认行为** — `sentry_sdk.capture_message` 在未 init 时应该 raise (或至少 logger.warning)，而不是 return None。这是 sentry-sdk 库的设计 anti-pattern，但我们不能改库，只能改使用方
3. **未来加 hook**：考虑给 `polyarb.observability.sentry` 加一个 `assert_initialized()` helper，在 `capture_message` 调用前先 assert，没 init 就 raise。这样 Makefile target 漏 init 时会**红字 error**而不是**静默成功**

**修法（本会话）**：Makefile `alerts-test` target 改为先调 `init_sentry(s)`，并加注释说明这个 contract。详见 commit (2026-05-20)。

---

## 2026-07-17 — Agent worktree 清理必须是 scoped + loud + disposition-closed

21 个 Claude agent worktree 累积到 7.4GB。不是 Git prune 失效，而是生命周期协议同时违反三条纪律：

1. **Scope 错**：post-wave 清理枚举所有 linked worktree，而不是 current execution 创建的集合。
2. **Git 语义错**：locked worktree 需要 `git worktree remove --force --force`，单 `--force` 必然失败。
3. **失败静默**：`2>/dev/null || true` 把资源泄漏伪装成 cleanup success。

闭环模式：

- spawn 前 snapshot worktree registry，结束后只处理集合差；
- removal 失败必须 non-zero；branch 只能在 worktree removal 成功且 ancestry 已证后自动删；
- 跨会话 reaper 默认 dry-run，PID live / malformed lock / dirty / path 越界一律 BLOCK；
- non-ancestor branch 不能只“保留以后看”，必须映射 plan 后显式 recovery 或 discard，否则仍是知识孤岛。

本次 4 条 non-ancestor branch 均为已完成 plan 的重复 executor lineage；审计后显式 discard。最终 registry only-main、临时 branch 0、占用 0B。

---

## 2026-07-17 — pytest 验证路径不能同时列子文件与父目录

Phase 3 计划的完整 M2 命令同时传了 `tests/routing/test_signal.py` 和 `tests/routing`。pytest 对重叠 collection path 去重后，只跑 69 条，父目录其余 routing tests 没进门；改成互不重叠的 `tests/models/test_slippage.py tests/routing tests/execution tests/cli` 后才是 130 条。

**纪律**：SUMMARY 的“full suite”必须同时记录 collected/passed 数量；目录 gate 只列最小不重叠路径，不能仅凭退出码判断覆盖完整。

同一 phase 还暴露了另一个验证层次：业务最终 state 相同不等于 operation 幂等。重放测试必须检查 transition 调用次数或 durable ledger cardinality，否则“第二次被重复持仓检查挡住”会伪装成幂等成功。
