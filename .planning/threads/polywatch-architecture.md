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

## 关键架构决策 (待 phase discuss 阶段确认)

### D-Polywatch-1: trial 状态承载位置

候选:
- A) `.planning/polywatch/trials.tsv` (随项目 git 走)
- B) `~/.polywatch/trials/` (跨项目共享)
- C) Supabase 表 (有 dashboard 加成)

**当前倾向**: A,先随项目走,跨项目化等基建经过本项目实战再说。

### D-Polywatch-2: cron 触发位置

候选:
- A) GitHub Actions (现有 keepalive 已用,免费 cron)
- B) Fly machine cron (现有 polyarb-l1 已有 cron machine)
- C) 本地 macOS launchd (开发机不开就不跑)

**当前倾向**: 混合 — healthz-watcher 用 A (云端持续),chaos-inj-replay 用 B (Fly 内部更省网络),memory-sanity 用 ralph 走会话内,不需 cron。

### D-Polywatch-3: 是否封装成 global skill

候选:
- A) 先做项目内 phase,跑通后再抽 `~/.claude/skills/polywatch/`
- B) 直接 global skill 起步

**当前倾向**: A — 基建先经实战再封装。CLAUDE.md "不要预先抽象" 原则。

### D-Polywatch-4: trial 失败的 escalation 级别

candidate trial 失败时:
- 静默 retry → log only → trial.tsv 标 fail
- Sentry breadcrumb → 已有 alert chain
- Telegram 推送 → 仅"红线触发"时 (max iter 用尽 / 副作用越界)
- GitHub issue → 仅"基建本身坏了"时 (cron 没启动 / harness 崩溃)

**当前倾向**: 分级,默认 trial 失败只落 trials.tsv,不污染 Sentry。

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

- 2026-05-26 thread 起,SESSION 27 命名 + 框架 + 第一批 trial 设计
