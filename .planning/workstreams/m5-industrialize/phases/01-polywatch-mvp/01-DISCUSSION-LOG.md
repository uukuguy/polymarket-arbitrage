# Phase 01: Polywatch MVP - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-26 (SESSION 28)
**Phase:** 01-polywatch-mvp
**Areas discussed:** D-1/D-2 (state + cron) / D-3/D-4 (skill scope + escalation) / Trial 2 chaos-replay / Trial 3+4 ralph + autoresearch

---

## Gray Area Selection

Selected (multiSelect): all 4 areas offered.
Skipped (decided directly): Trial 1 healthz-watcher 技术细节 (已 shipped, baseline 固定)。

---

## D-1 — trials.tsv 位置

| Option | Description | Selected |
|---|---|---|
| A: .planning/polywatch/trials/*.jsonl (推荐) | 随项目 git, append-only jsonl. 起步快; 后期 git noise 真烦可 .gitignore | ✓ |
| C: Supabase 表 | 价值高但工程量大; 需增 schema migration + Python 客户端; 适合然后升级 | |
| A 起步 + C 进 backlog | 先 A 跑看看, 后期 "dashboard 需求真出现" 才 C。 渐进 | |

**User's choice:** A

---

## D-2 — cron 位置

| Option | Description | Selected |
|---|---|---|
| 混合 (推荐): healthz=GHA, chaos=Fly, ralph=会话, autoresearch=本地 | 高聪明: 每个 trial 点在资源合适的位置跱 | ✓ |
| 全 GHA | 一统 cron 管理点, 但 chaos 需 GHA pull Fly state, autoresearch 需上传历史 snapshot | |
| 全 Fly | 集中 Fly machine 帮 cron, autoresearch 在 Fly 跑需传输数据 | |

**User's choice:** 混合策略

---

## D-3 — 是否在本 phase 抽 global skill?

| Option | Description | Selected |
|---|---|---|
| A: 本 phase 跑通后再抽 (推荐) | 4 trial 跑稳 1-2 周, 后期 m5 phase 04 抽 ~/.claude/skills/polywatch/. CLAUDE.md 原则 | |
| B: 现在就抽 | 边做边定 skill 接口 — 提前抽象风险 | ✓ |

**User's choice:** B
**Notes:** User override Claude 推荐。 Phase scope 因此扩张: skill 同步设计, plan 阶段需对接 `~/.claude/skills/polywatch/` 创建任务。Risk: 违反 "不要预先抽象" 原则一次。 缓解: skill 起步是薄壳, m5 phase 04 实战回炉 iteration。

---

## D-4 — trial 失败 escalation 4 级 + 2 子决策

| Option | Description | Selected |
|---|---|---|
| A: streak=3 + L3 自动 create issue (推荐) | Sentry breadcrumb 阈值 3 连续 fail; L3 自动开 GH issue 别等人 — 反正 issue 在不需要时也能 close | ✓ |
| B: streak=5 + L3 手工 (保守) | 5 连续才告, L3 手工人创. 如果 trial 运行频率高 (chaos nightly) 过于迟钝 | |
| C: streak=2 + L3 自动 + L4 paging | 还加一级 PagerDuty — over-engineering, GAP-102 已推 backlog | |

**User's choice:** A

---

## Trial 2 chaos-inj-replay 设计

| Option | Description | Selected |
|---|---|---|
| A+B+A (推荐) | Inj=L2-1/2/3a 起步 → Phase 03.1 跑完后自动纳入 L2-3b/4/5. Schedule=UTC 18:00 (Asia 凌晨 2 点). Staging=prod+dry-run flag | ✓ |
| C+B+B (保守) | 只跑代价低 L2-1+L2-3a. Schedule UTC 18:00. 独立 staging Fly app — 增 $5/mo 但完全隔离 | |
| A+A+C (极致) | 全 Inj. Asia 早上 10 点. Ephemeral Fly machine 跑完即销 — 架构优雅但双倍复杂度 | |

**User's choice:** A+B+A

---

## Trial 3 ralph memory-sanity 设计

| Option | Description | Selected |
|---|---|---|
| 推荐: max=10 + propose review + 手动触发 + completion=propose 完毕 | ralph 收敛型, 10 次不够人介入. 不自动 commit (红线#4). 单会话不需 cron | ✓ |
| max=20 + 自动 archived 标记 | 更侵入式, 跳过人 review 打 archived. 风险: misjudge 丢真 memory | |
| max=5 + 只生 patch 不跑验证 | 低象, ralph 只生 propose 不验证. 不是真 ralph (跳过收敛) | |

**User's choice:** 推荐 (max=10 + propose review + 手动 + completion=propose 完毕)

---

## Trial 4 autoresearch validation-tuning 设计

| Option | Description | Selected |
|---|---|---|
| 推荐: 1 天数据 + grid 10 + signal:noise + max=10 | 快试跑通形态. 简单 verdict. 不放飞. 初次 autoresearch 不追求最佳结果 | ✓ |
| 7 天 + grid 20 + 复合 verdict | 更多数据信号更顺, 但 "跑通形态" 变 "调优结果", 偏离 MVP 意图 | |
| 30 天 + bayesian + max=50 | 生产级. autoresearch 未证明能跑就上重型 — 逆潜规则 | |

**User's choice:** 推荐 (1 天 + grid 10 + signal:noise + max=10)

---

## the agent's Discretion

User 在 area selection 阶段同意 Trial 1 healthz-watcher 技术细节不重谈 (已 shipped baseline)。
Claude 据此决:
- Trial 1 接 Phase 03.1 字段的具体接口 (等 03.1 plan 出 fail-reason schema 再对接)
- ralph-loop completion promise 的 prompt 措辞 (沿用 ralph-loop plugin 体例)
- autoresearch results.tsv schema 字段 (复用 D-1 trials.jsonl 通用 schema)
- chaos Inj dry-run flag 注入位置 (若需补)
- Plan wave 切分 (gsd-planner 按依赖图)
- Skill 范围细节: 薄壳起步, SKILL.md + trial_runner.py + escalation.py + 3 个 template_{type}.py

## Deferred Ideas

- trials.jsonl → Supabase 表升级 → m5 phase 02-polywatch-extend
- PagerDuty / SMS escalation (L4) → m5 phase 02-polywatch-extend backlog
- AutoResearch trial 2 (M2 backtest) → m5 phase 02, 等 M2 T2 实操
- AutoResearch trial 3 (M4 prompt scoring) → m5 phase 03, M4 workstream 启动后
- AutoResearch trial 5 (L2 资产 curate) → m5 phase 03-polywatch-l2-curate
- AutoResearch trial 6 (Kalshi-Polymarket pair) → m3 启动后
- Skill 实战回炉 iteration → m5 phase 04-polywatch-globalize
- Trial 3 ralph 自动 commit 模式 → memory-sanity 跑 3 个月 zero misjudge 后再讨论
