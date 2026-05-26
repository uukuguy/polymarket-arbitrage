# Polywatch — Thread 累积

> Polywatch 是本项目自动化基建的总称。横跨 M1-M5,不属于任何 workstream。
> 本 thread 累积 Polywatch 相关跨 phase 的决策、教训、模式。

## 起源

**2026-05-26 SESSION 27**:
- Phase 03 (L2 daemon) closed 后,resume 发现 L1 PAUSED 3.5 天没人响应
- Sentry RCA 真证据: 6 天里 L1 PAUSE 至少 3 次 (05-19 / 05-22 00:16 / 05-22 01:45),全部 environment=dev,告警都发出去了但都被忽略
- 暴露问题: alert chain 工作 (transport OK),但缺**主动监控 + 自动响应**机制
- 用户提议研究 karpathy/autoresearch 是否能套
- 推导得出: AutoResearch 模式不能直接套 M1 (mismatch),但抽象成"自动化基建" 后整个项目都受益

详见 [[architecture_polywatch-decision-framework]] (memory).

## 命名

**Polywatch** — Polymarket + watcher 双关 + watch market / watch self 双义。
首选名,用户 SESSION 27 确认。

## 三件套架构

```
┌──────────────────────────────────────────────────────────┐
│ Polywatch                                                 │
├──────────────────────────────────────────────────────────┤
│ Cron / Daemon       Ralph Loop          AutoResearch     │
│ (周期触发)           (收敛迭代)          (搜索对比)        │
│                                                           │
│ healthz-watcher     memory-sanity       backtest-harness │
│ chaos-replay        plan-rescue         prompt-tuning    │
│ dashboard-drift     adr-supersede       L2-curate        │
│                     debug-loop          validation-tune  │
└──────────────────────────────────────────────────────────┘
       ↓                  ↓                    ↓
   trials.tsv         git history          results.tsv
   (cron log)        (commit advance)     (trial ledger)
       ↓                  ↓                    ↓
       └────────── shared playwright-cli edge profile ──────┐
                  (验证 SaaS dashboard 不需要人介入)         │
                                                              │
       └────────── shared safety.md (3 红线 + max iter) ─────┘
```

## 第一批落地范围 (Phase m5-industrialize 01-polywatch-mvp)

| Trial | 工具 | 验证目的 | 副作用边界 |
|---|---|---|---|
| healthz-watcher | Cron (GHA / Fly cron / 本地 launchd) | 每 N 分钟拉 polyarb-l1 + polyarb-l2 /healthz,fail 即触发 Sentry breadcrumb 拉取 + Telegram 推送 + (可选) 自动 unpause | 可写 Telegram (现有 alert chain), 可改 Fly machine state (unpause), 不可改代码 |
| chaos-inj-replay | Cron (本地 launchd / GHA nightly) | 每晚跑 Phase 03 既有的 Inj 1/2/3 (+ 后续 L2-3b/L2-4/L2-5),verdict=PASS streak | 只 dry-run / staging, 不动 prod 数据 |
| memory-sanity-check | Ralph Loop (单次会话执行) | grep MEMORY.md 所有 VERIFIED 条目里的 file:line 引用,验证对应代码仍存在;失效条目 propose patch (不自动 commit) | 只读, propose 不 commit |
| autoresearch-validation-tuning | AutoResearch (本地脚本) | L4 validation rule 阈值校准: 跑 N 天历史 snapshot,搜索 L4 tolerance 最佳值。verdict=signal:noise ratio | 只读历史 snapshot, results.tsv append-only |

## 关键架构决策 — ✅ LOCKED @ SESSION 28 (2026-05-26, m5 phase 01 discuss)

> 4 决策全部锁定。canonical 出处: `.planning/workstreams/m5-industrialize/phases/01-polywatch-mvp/01-CONTEXT.md`。
> 本 thread 仅保留**结论 + why short**, 完整 rationale 看 CONTEXT.md。

### D-Polywatch-1: trial 状态承载位置 — ✅ LOCKED = A++

**决策**: `.planning/polywatch/trials/{trial_name}.jsonl` append-only (每行一个 trial result)。

- Schema: `{trial_name, iteration, timestamp, verdict, metrics, notes, ref_commit}`
- **Why A over B/C**: B (`~/.polywatch/`) 在 GHA runner 完全丢失; C (Supabase) 工程量大且 GHA 网络依赖。 A 起步快、可 diff、跨机器一致
- **Why jsonl over tsv**: schema 演化、嵌套 metrics 友好

### D-Polywatch-2: cron 触发位置 — ✅ LOCKED = 混合策略

各 trial 独立选 cron 位置:

| Trial | Cron 位置 | 备注 |
|---|---|---|
| healthz-watcher | GHA (`*/15` schedule) | 已 ship `.github/workflows/polywatch-healthz.yml`, 不动 |
| chaos-inj-replay | Fly machine cron (polyarb-l1 内部) | UTC 18:00 nightly, 零网络 overhead |
| memory-sanity-check | 无 cron (ralph 会话内手动) | memory 改动后 / 月度审查 |
| autoresearch-validation-tuning | 无 cron (本地脚本) | 需 SQLite/Parquet 本地访问 |

**Why**: 单一 cron 在某些 trial 上是 anti-pattern (chaos 跑 GHA 要拉 Fly state 多绕; autoresearch 跑 Fly 要传历史数据)。

### D-Polywatch-3: global skill 抽取时机 — ✅ LOCKED = B (本 phase 同步抽) ⚠️ user override

**决策**: 本 phase 同步抽 `~/.claude/skills/polywatch/` (改 B,反 thread 原默认 + 反 Claude 推荐)。

- **Why user override**: 边做边定 skill 接口能让 trial 实现自始至终保持 skill 友好结构, 反正都要抽,避免日后重构
- **Risk acknowledged** (写入 CONTEXT): 违反 CLAUDE.md "不要预先抽象" 一次
- **缓解**: skill 起步是**薄壳** (SKILL.md + `trial_runner.py` + `escalation.py` + 3 个 trial template), 不强求完美;经 4 trial 实战后, m5 phase 04 再"实战回炉"
- **Skill 范围**: SKILL.md 描述 4 trial 模式 + 决策树 + 红线 + 通用 jsonl ledger 写入器 + 4 级 escalation 实现

### D-Polywatch-4: trial 失败 escalation — ✅ LOCKED = 4 级 (streak=3 + L3 auto GH issue)

| Level | 触发 | 落到哪 |
|---|---|---|
| L0 silent | 单次 fail | `.planning/polywatch/trials/{trial}.jsonl` 标 `verdict=fail`, 不告警 |
| L1 breadcrumb | streak 3 连续 fail | Sentry breadcrumb (复用现有 SDK) |
| L2 Telegram | 红线触发 (max iter 用尽 / 副作用越界 / prod alert chain 自身坏) | Telegram push |
| L3 GH issue | 基建本身坏 (cron 没启动 ≥2 周期 / harness 启动崩) | `gh issue create` 自动 |

- **Why streak=3 (非 5)**: 配合 healthz-watcher 15min × 3 = ~45min, 人响应窗口合理
- **Why L3 自动**: issue 可关, 自动开比"无人察觉基建坏掉"危险小很多

### 4 trial 子决策 (Trial 1-4) 全部在 CONTEXT.md, 不复述

## 红线 (与 [[architecture_polywatch-decision-framework]] 一致,本 thread 复述强化)

1. **真实下单 / 资金操作** — 永远不放进 loop
2. **改 prod secrets / push prod main** — paper-mode 可读,写必走 staging
3. **发对外 channel** (推特/email 群发) — Telegram 单向告警除外
4. **改 ADR / 锁定决策** — 自动化只能 propose

## 与 Phase 03.1 的并行关系

- Phase 03.1 (m1-perception): 修代码,把 observability gap 填上 (12 项)
- Phase 01-polywatch-mvp (m5-industrialize): 跑监控,把"修好的 observability surface"消费起来

互为前后置:
- 03.1 修 `l2_mirror_enabled` config + `snapshots.notes` 字段 → Polywatch healthz-watcher 要 grep 这些字段做判定
- Polywatch 跑出来的 fail signal → 反过来催 03.1 优先级 (例如 Fly DNS 失败频次决定要不要把 `failure_threshold` 调高)

两条 phase 并行无依赖。

## 后续 phase 路径 (Polywatch 系列)

- Phase 01-polywatch-mvp — 4 trial 跑通 + harness 形态验证
- Phase 02-polywatch-extend — 接入 M2 backtest trial 当 M2 T2 进入实操
- Phase 03-polywatch-l2-curate — 接入 L2 资产集自动 curate 当 L2 数据足
- Phase 04-polywatch-globalize — 经实战后,抽 `~/.claude/skills/polywatch/` 跨项目可用

## 更新日志

- 2026-05-26 SESSION 27 thread 起 — 命名 + 三件套框架 + 第一批 trial 设计 + D-1..D-4 候选
- 2026-05-26 SESSION 28 — D-Polywatch-1..4 全 LOCKED (m5 phase 01 discuss-phase 输出), D-3 user override 反 Claude 推荐 + 反 thread 默认 (本 phase 同步抽 global skill). Canonical 决策出处转移到 `.planning/workstreams/m5-industrialize/phases/01-polywatch-mvp/01-CONTEXT.md`
