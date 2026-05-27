# Phase 04: Candidate Set 扩容 + L2 Throughput 验证 + 投影 Gap 收尾 - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-05-28 (SESSION 30)
**Phase:** 04-candidate-set-l2-throughput
**Areas discussed:** candidate 数据源切 Supabase / candidate 选择标准+规模 / L2 throughput pass 标准 / yes_token_id 补列 + GAP-200

---

## Gray Area Selection

Offered 4 areas (multiSelect): candidate 数据源切 Supabase / candidate 选择标准+规模 / L2 throughput pass 标准 / yes_token_id+GAP-200。
Selected: 数据源切 Supabase + throughput pass 标准 + yes_token_id+GAP-200。
选择标准+规模未单选，但因与数据源强耦合，在 Area 1 内作为子问题带出（D-03/D-04）。

**Scout finding that reshaped scope:** L2 compute_candidates 当前读 L2 本地 SQLite，但 L2 库是空的（markets 表从不在 L2 写入）→ recipe 路径在 prod 返回零行，只有 3 个 bootstrap_asset_ids 真在驱动 WS 订阅。「扩容」前提是先让 candidate 计算能读到数据。

---

## Area 1: candidate 数据源切 Supabase

### Q1 — scanner recipe 怎么在 Supabase 上跑？

| Option | Description | Selected |
|---|---|---|
| 拉全量进临时 SQLite 复用现有 scanner | NOTIFY 后拉 markets_latest 全量 → 临时 SQLite → 现有 recipe SQL 不改 | ✓ |
| recipe 重写为 PostgREST filter | WHERE 翻译成 ?param=gt.x，不落本地 | |
| 混合: 简单过滤走 REST, 复杂 recipe 拉全量本地 | 两套路径 | |
| 你先调研再决 | 交 research 定 | |

**User's choice:** 拉 markets_latest 全量进临时 SQLite, 复用现有 scanner
**Notes:** 改动面最小，保留 4 层 SQL 防护 + 6 内置 recipe + YAML 自定义。

### Q2 — 临时库形态 + narrow schema 差异？

| Option | Description | Selected |
|---|---|---|
| :memory: + 适配层填 scanner 期望 schema | 缺列 NULL，recipe SQL 不改，每次 refresh 重建 | ✓ |
| 磁盘临时文件 复用 SQLiteStore | 多一次磁盘 IO | |
| 扩 markets_latest projection 补齐 scanner 列 | 肥化 mirror 写入量 | |
| 你先调研 scanner schema 依赖 | 交 research | |

**User's choice:** :memory: 库 + 适配层把 narrow 行填成 scanner 期望的 schema
**Notes:** Claude 标注 chain-truth integration risk — 缺列填 NULL 可能让依赖该列的 recipe 静默返回错误结果；适配层须对 recipe 列依赖 fail-loud。

### Q3 — candidate 选择标准 + 规模？

| Option | Description | Selected |
|---|---|---|
| 复用现有 recipe (near-end + liquidity 排序) + cap 500 | 不另发明逻辑 | ✓ |
| 新增 L2 专用 recipe (可配阈值) | env var 阈值 | |
| 你先调研真实 market 分布 | 交 research | |

**User's choice:** 复用现有 recipe + cap 500
**Notes:** 真实规模现实不超几千 markets，near-end 子集远低于 500 cap。先用默认 recipe 验证通路。

### Q4 — bootstrap_asset_ids 去留？

| Option | Description | Selected |
|---|---|---|
| 保留作冷启动兜底 | 第一次 refresh 后被真实 candidate set 接管 | ✓ |
| 废除, 全靠 startup 主动查 Supabase | 去 bootstrap 认知负担 | |
| 你先调研 startup 时序 | 交 research | |

**User's choice:** 保留作冷启动兜底
**Notes:** bootstrap 是 WS 订阅 initial state，与 compute_candidates 计算路径独立（scout 确认已解耦），两层兜底不冲突。

---

## Area 2: L2 throughput 验证 pass 标准

### Q1 — 多大负载算"真实"？验证在哪跑？

| Option | Description | Selected |
|---|---|---|
| 真实 candidate set 规模在 prod 跑 | 最接近真实生产负载 | ✓ |
| 合成高负载 (100+ asset + 高 msg-rate) | 偏离真实分布 | |
| 你先调研 Polymarket WS 限制 | 交 research | |

**User's choice:** 用真实 candidate set 规模 (D-03 算出的 N 个) 在 prod 跑
**Notes:** candidate set 扩完是多少就用多少，不造合成流量。

### Q2 — pass 判定？

| Option | Description | Selected |
|---|---|---|
| 三指标: 零丢帧 + watchdog 不误触 + 内存稳定 | 阈值 research 定 | ✓ |
| 只要 daemon 不崩 + 恢复干净 | 沿用 Inj L2-4 逻辑标准，不加 throughput | |
| 你先调研基线再定阈值 | 交 research | |

**User's choice:** 三指标全过
**Notes:** 是对 Inj L2-4「daemon survived + recovery clean」逻辑验证标准的升级；具体阈值 research 拉 baseline 后定。

---

## Area 3: yes_token_id 补列 + GAP-200 修法

### Q1 — yes_token_id 空值处理？

| Option | Description | Selected |
|---|---|---|
| nullable 列 + 空值透传 NULL | Alembic add-only，不阻断 mirror | ✓ |
| NOT NULL + 缺值跳过该 market | 会静默丢 market | |
| 你先调研空值率 | 交 research | |

**User's choice:** nullable 列 + 空值透传 NULL
**Notes:** Phase 1 Open Items 记「top-of-book single-side, 只 yes_token_id populated」但仍可能有 market 连 yes 都缺 → nullable 安全。

### Q2 — GAP-200 /health surface 修法？

| Option | Description | Selected |
|---|---|---|
| url有/key空 → 注册 sub-check status=fail "disabled by config" | 区分两种禁用态 | ✓ |
| 只要 mirror 禁用就始终注册 fail | 没打算用 mirror 的部署会上嘴 fail 噪声 | |
| 加 startup 日志 + 外部告警, 不动 /health | 违背 chain-truth surface 纪律 | |

**User's choice:** supabase_url 有但 key 空 → 注册 sub-check status=fail "disabled by config"
**Notes:** 区分 (a) url 也空=合理不注册 vs (b) url 有 key 空=可能误操作=注册 fail。把 config-disable 变成 chain-truth 信号，是 Phase 03.1 L4 lesson 的 inverse 收尾。

---

## the agent's Discretion

- Plan 波次切分（gsd-planner 按依赖图：D-01/D-02 是 D-03/D-05 前提，D-07/D-08 独立可并行）
- 临时库建表代码 / 适配层映射细节（沿用现有 SQLiteStore schema）
- 测试覆盖（沿用 Phase 03 RED-first chaos pattern + scanner 既有测试）
- commit boundary（一个决策组一个 plan）
- D-06 三指标具体数值阈值（research 拉 baseline 定）

## Deferred Ideas

- candidate recipe 调优（D-03 先用默认验通路）→ m1-perception backlog
- 合成高负载压测（D-05 选真实负载）→ backlog / m5-polywatch chaos trial
- refresh debounce 调优（当前 60s）→ 真实 scale 观察后再调
- Supabase Pro / Neon 升级 → 触发条件未到
- POLYARB_WS_TEST_KILL nightly cron → m5-polywatch trial-2
