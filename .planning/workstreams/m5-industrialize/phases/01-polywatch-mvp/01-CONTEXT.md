# Phase 01: Polywatch MVP - Context

**Gathered:** 2026-05-26 (SESSION 28)
**Status:** Ready for planning

<domain>
## Phase Boundary

Polywatch 自动化基建第一批 4 trial 跑通 + harness 形态验证 + **global skill 同步抽象**。让本项目从"alert 到达但被忽略"升级到"alert 到达 + 自动响应/缓解 + ledger 留底"。

healthz-watcher 已是 baseline (commit 6a77e06), 本 phase:
1. 把 healthz-watcher 形式化纳入 phase 治理 (写 plan SUMMARY 兜底, 接 Phase 03.1 修好的 fail-reason 字段, 补 Sentry breadcrumb auto-fetch)
2. 落另外 3 个 trial (chaos-inj-replay / memory-sanity-check / autoresearch-validation-tuning)
3. **同步抽 `~/.claude/skills/polywatch/` global skill** (D-3 改 B — 不延后到 phase 04)
4. 决出 D-Polywatch-1/2/3/4 (已 discuss-phase 阶段决, 见 decisions)

非目标:
- M2 backtest trial (m5 phase 02-polywatch-extend)
- L2 资产集自动 curate (m5 phase 03-polywatch-l2-curate)
- 真实下单 / 资金 / push prod main / ADR 锁决 (永远红线)

</domain>

<decisions>
## Implementation Decisions

### D-Polywatch-1: trial 状态承载位置

- **D-1:** **`.planning/polywatch/trials/{trial_name}.jsonl` append-only**
- **Why**: B (`~/.polywatch/`) 在 GHA runner 完全丢失; C (Supabase 表) 工程量大且 GHA 网络依赖。 A 起步快、可 diff、跨机器一致
- **Why not git noise concern**: 起步前期 git 历史是好事 (能看 trial 演化); 若真烦, 后期 `.gitignore` 转 `~/.polywatch/` 或 Supabase 升级
- **格式**: 每行一个 trial result, schema 字段: `{trial_name, iteration, timestamp, verdict, metrics, notes, ref_commit}`

### D-Polywatch-2: cron 触发位置 — **混合策略**

- **D-2:** 各 trial 独立选 cron 位置, 不强求统一:
  - **healthz-watcher**: GHA (已 ship, 不动) — `.github/workflows/polywatch-healthz.yml` cron `*/15`
  - **chaos-inj-replay**: Fly machine cron (polyarb-l1 内部, UTC 18:00 nightly) — 离 endpoint 近, 零网络 overhead, chaos config 不必跨网传
  - **memory-sanity-check**: 无 cron, 单 ralph-loop 会话执行 — 手动触发 (memory 改动后或月度审查)
  - **autoresearch-validation-tuning**: 无 cron, 本地脚本手动跑 — 跑历史 snapshot 需 SQLite/Parquet 本地访问
- **Why**: 单一 cron 位置在某些 trial 上是 anti-pattern (chaos 跑 GHA 要拉 Fly state 多绕一圈; autoresearch 跑 Fly 要传历史数据)
- **代价**: 多个 cron 管理点, 但只有 2 个真 cron (healthz / chaos) 不致甚

### D-Polywatch-3: global skill 抽取时机

- **D-3:** **本 phase 同步抽 `~/.claude/skills/polywatch/`** (改 B,反 thread 默认倾向)
- **Why** (user override): 边做边定 skill 接口能让 trial 实现自始至终保持 skill 友好结构, 反正都要抽,避免日后重构。 跨项目化路径明确
- **Risk acknowledged**: 违反 CLAUDE.md "不要预先抽象" 一次。 缓解: skill 起步是**薄壳**, 不强求完美;经过本 phase 4 trial 实战后,m5 phase 04 再做"实战回炉" iteration
- **Skill 范围 (待 plan 阶段定细节)**:
  - skill SKILL.md 描述 4 trial 模式 + 决策树 + 红线
  - 提供 `trial_runner.py` (jsonl ledger 写入器) + `escalation.py` (D-4 分级实现)
  - 提供 trial 模板: `{trial_type}/template.py` × 3 (cron / ralph / autoresearch)
  - 项目内通过 `~/.claude/skills/polywatch/` 调用,本 project 的 4 trial 是 skill 第一个 consumer

### D-Polywatch-4: trial 失败 escalation — 4 级分级

- **D-4:** **streak=3 + L3 自动 create GH issue**, 4 级 escalation:

| Level | 触发条件 | 落到哪 |
|---|---|---|
| L0 silent | 单次 trial fail | `.planning/polywatch/trials/{trial}.jsonl` 标 `verdict=fail`, 不告警 |
| L1 breadcrumb | streak 3 连续 fail | Sentry breadcrumb (复用现有 SDK) |
| L2 Telegram | 红线触发 (max iter 用尽 / 副作用越界 / prod alert chain 自身坏) | Telegram push (复用 healthz-watcher 已用 chain) |
| L3 GH issue | 基建本身坏 (cron 没启动 ≥2 周期 / harness 启动崩) | `gh issue create` 自动 — 别等人, issue 不需要时也能 close |

- **Why streak=3 (非 5)**: 配合 healthz-watcher 15min 间隔,3 连续 = ~45min,人响应窗口合理
- **Why L3 自动**: 反正 issue 可关, 自动开比"无人察觉基建坏掉"危险性小很多

### Trial 2 chaos-inj-replay 子决策

- **Inj 子集**: 起步 = L2-1/L2-2/L2-3a (Phase 03 已 PASS 的 3 个); Phase 03.1 跑完 L2-3b/L2-4/L2-5 后, 自动纳入扩展集
- **Schedule**: UTC 18:00 nightly (Asia 凌晨 2 点, 低流量窗口, 数据噪声小)
- **fail streak 通知**: 沿用 D-4 (streak=3 → Sentry breadcrumb)
- **Staging 环境**: **prod polyarb-l1/l2 + dry-run flag** (非独立 staging app, 非 ephemeral machine)
- **前置依赖** (待 plan 阶段确认): Phase 03 Inj 代码是否原生支持 dry-run flag? 若否, 本 phase 内补 — 至少 chaos toolkit 层面要有 `--dry-run` 不真写 Supabase / Sentry

### Trial 3 memory-sanity-check (Ralph) 子决策

- **completion promise**: "全部 MEMORY.md VERIFIED 条目里的 file:line 引用, 在当前 codebase 全部存在 (file 存在 + 行号在文件内 + 行内容与 memory 描述吻合)"
- **max iter**: 10 (10 次未满足 → 人介入, ralph 报告失效条目列表)
- **失效条目处理**: A — **propose patch 走人 review** (生成 `.planning/polywatch/memory-sanity-{date}.md` 列失效条目 + 建议 patch), 不自动 commit (红线#4)
- **completion 判定**: **propose 文档生成完毕** (因为不自动 commit, 不能要求 "全 pass")
- **触发频率**: 手动 (memory 改动后 / 月度审查), 不进 cron

### Trial 4 autoresearch-validation-tuning 子决策

- **trial 1 次 = 1 天 snapshot** (起步速度优先, 1 天 ~24 tick, 算 L4 fire rate 够)
- **L4 阈值搜索空间**: grid 10 个固定值 (e.g. tolerance ∈ {0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5})
- **verdict 函数**: signal:noise ratio = (alert 真问题数 / 总 alert 数) — 真问题判定 = "事后 ≤30min 内 L1/L2 status 转 fail 的 alert" 算真
- **max trials**: 10 (grid 穷尽即停, 不放飞)
- **首次跑通形态优先于结果**: 本 trial 主要目的是验证 autoresearch 模式能否在本项目落地, 不追求最佳 L4 阈值

### the agent's Discretion

- Plan wave 切分 (gsd-planner 按依赖图自动决)
- Trial 1 healthz-watcher 接入 03.1 字段的具体接口 (等 03.1 plan 出 fail-reason schema 再对接)
- ralph-loop completion promise 的 prompt 措辞 (沿用 ralph-loop plugin 体例)
- autoresearch results.tsv schema 字段 (复用 D-1 trials.jsonl 通用 schema 即可)
- chaos Inj dry-run flag 注入位置 (若需补)

### Folded Todos

无 — 本 phase scope 直接源自 Polywatch thread + SESSION 27 Polywatch MVP shipped 顺势扩展, 无 backlog folded

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Polywatch 自身

- memory `architecture_polywatch-decision-framework.md` — 4 条件 + 8 应用点 + 3 红线 + 决策树 (权威架构定义)
- memory `project_polywatch-mvp-shipped-2026-05.md` — healthz-watcher MVP 上线状态 + 6 GHA secrets + decide_l1/l2 规则
- `.planning/threads/polywatch-architecture.md` — D-Polywatch-1..4 待定决策 + 三件套架构图 + 后续 phase 路径

### 工程纪律 (必应用)

- memory `feedback_alert-chain-discipline-2026-05.md` — alert 到达 ≠ 介入 (Polywatch 存在理由)
- memory `feedback_dashboard-access-autonomous-2026-05.md` — playwright-cli edge profile (Sentry breadcrumb fetch 复用)
- memory `feedback_verification-ownership-2026-05.md` — Claude 自闭环验证规模化 (本 phase 把它工程化)
- memory `feedback_code-vs-chain-truth-2026-05.md` — chaos dry-run 必须 surface 到 jsonl ledger, 不能只 code-level OK

### 跨 phase 依赖

- `.planning/workstreams/m1-perception/phases/02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-LEARNINGS.md` — BUG-8 `/control/unpause` HMAC endpoint (healthz-watcher 已复用)
- `.planning/workstreams/m1-perception/phases/03-l2-orderbook-tracking-daemon/03-LEARNINGS.md` — Phase 03 chaos Inj 3 个已 PASS (chaos-inj-replay 起步集)
- `.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/03.1-CONTEXT.md` — Phase 03.1 决议: GAP-103 写 fail reason / L2-3b/4/5 跑通后纳入 replay 集

### Skill 抽象参考 (D-3 决定要做)

- `~/.claude/skills/` 现有 skill 体例 (e.g. `~/.claude/skills/gsd-resume-work/SKILL.md`, `~/.claude/skills/playwright-cli/SKILL.md`)
- ralph-loop plugin spec — Stop hook self-referential 单 prompt 迭代模式 (Trial 3 实现参考)

### Skill 暂未存在的部分

- `~/.claude/skills/polywatch/` (本 phase 创建)
- `.planning/polywatch/trials/` (本 phase 创建)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `scripts/polywatch/healthz_watcher.py` — Trial 1 baseline (253 行 stdlib, 无 deps)
- `.github/workflows/polywatch-healthz.yml` — Trial 1 GHA cron baseline
- `Makefile` `polywatch-healthz` / `polywatch-healthz-dry` targets — 调用模板
- `src/polyarb/http/control.py` (Phase 02.1 BUG-8) — unpause HMAC endpoint, healthz-watcher 已复用
- `tests/chaos/` (Phase 03) — chaos Inj 实现, Trial 2 chaos-inj-replay 复用
- `scripts/chaos_*.py` (Phase 03) — chaos toolkit 入口, Trial 2 nightly 调用
- Sentry SDK 已集成 (现 `environment="dev"`, Phase 03.1 GAP-102 修); Trial 1-4 都用 breadcrumb API
- Telegram bot 已集成 (healthz-watcher 已用); Trial 2-4 escalation L2 复用

### Established Patterns

- **GHA workflow + secrets** 模式 (healthz-watcher 已建立)
- **HMAC POST /control/unpause** (Phase 02.1 BUG-8)
- **stdlib-only script** (healthz-watcher 选择, 无 deps 易跨环境)
- **JSON 字段 sub-check** (healthz_watcher.decide_l1/l2 已有 pattern, Trial 1 接入 03.1 字段时沿用)
- **ralph-loop plugin Stop hook** (`~/.claude/plugins/ralph-loop/` SKILL.md 内描述)

### Integration Points

- Trial 1 接 03.1 字段: GAP-103 `snapshots.notes` (fail reason) → healthz_watcher 读 /healthz `checks` 里若有 notes 字段, 推 Telegram 时贴上
- Trial 2 chaos dry-run flag: 若 Phase 03 chaos toolkit 不原生支持, 本 phase 补 — 至少 `--dry-run` 让 chaos 写本地 jsonl 不写 Supabase / 不真触发 Sentry
- Trial 3 memory grep: 路径是 `~/.claude/projects/-Users-sujiangwen-sandbox-hacker2026-PolyMarket-polymarket-arbitrage/memory/` (本项目 memory 目录)
- Trial 4 历史 snapshot: 本地 SQLite `data/snapshots/*.db` + Parquet (M1 Phase 1 落库格式)
- Skill `~/.claude/skills/polywatch/` 与项目内 `scripts/polywatch/` 关系: skill 提供模板/runner/escalation, project script 是 consumer

</code_context>

<specifics>
## Specific Ideas

- **trials.jsonl schema 字段**:
  ```json
  {
    "trial_name": "chaos-inj-replay",
    "iteration": 42,
    "timestamp": "2026-05-27T18:00:00Z",
    "verdict": "pass",
    "metrics": {"inj_subset": ["L2-1", "L2-2", "L2-3a"], "pass_count": 3, "fail_count": 0},
    "notes": "all 3 Inj passed, streak=15",
    "ref_commit": "1018f1f"
  }
  ```
- **L3 GH issue 模板** (`polywatch-l3-{trial_name}-{date}`):
  ```
  Title: [polywatch L3] {trial_name} infra broken — {failure_class}
  Labels: polywatch, infra-broken, auto-filed
  Body: trial.jsonl tail + last 3 cron run links + suspected cause
  ```
- **autoresearch grid 值确认 (待 plan 调整)**: L4 tolerance ∈ {0.001, 0.005, 0.01, 0.02, 0.05, 0.1, 0.15, 0.2, 0.3, 0.5} — 涵盖 1% 到 50%, 跨 2.5 个数量级
- **skill 薄壳起步** (D-3): SKILL.md + `trial_runner.py` + `escalation.py` + 3 个 `template_{type}.py` 即可, 不强求多文件多模块

</specifics>

<deferred>
## Deferred Ideas

- **trials.jsonl 升 Supabase 表** → 当本项目 polywatch dashboard 真有需求时启动 (预计 m5 phase 02-polywatch-extend)
- **PagerDuty / SMS escalation (L4)** → m5 phase 02-polywatch-extend backlog, 等 healthz-watcher + Telegram + GH issue 三级链路实战 1-2 月后真证明不够再加
- **AutoResearch trial 2 (M2 backtest harness)** → m5 phase 02-polywatch-extend, 等 M2 T2 进入实操
- **AutoResearch trial 3 (M4 prompt/model 评分)** → m5 phase 03 (M4 workstream 启动后)
- **AutoResearch trial 5 (L2 资产集 curate)** → m5 phase 03-polywatch-l2-curate, 等 L2 数据足
- **AutoResearch trial 6 (Kalshi-Polymarket pair discovery)** → m3 启动后
- **Skill 实战回炉 iteration** → m5 phase 04-polywatch-globalize (本 phase 跑通 1-2 月后, 经实战反馈重构 skill 接口)
- **trial 3 ralph 自动 commit 模式** → 等 memory-sanity 跑 3 个月 zero misjudge 后再讨论开放

### Reviewed Todos (not folded)

无 — 未做 cross_reference_todos (本 phase scope 来自 Polywatch thread + MVP 顺势扩展)

</deferred>

---

*Phase: 01-polywatch-mvp*
*Context gathered: 2026-05-26 (SESSION 28)*
