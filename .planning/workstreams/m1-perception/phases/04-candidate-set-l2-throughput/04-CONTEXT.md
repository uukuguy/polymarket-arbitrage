# Phase 04: Candidate Set 扩容 + L2 Throughput 验证 + 投影 Gap 收尾 - Context

**Gathered:** 2026-05-28 (SESSION 30)
**Status:** Ready for planning

<domain>
## Phase Boundary

让 polyarb-l2 的 candidate 计算真正能跑并扩到真实规模，在真实负载下验证 WS/watchdog throughput，并收尾两个投影 gap。

**关键事实（scout 发现，重塑了 scope）**：L2 的 `compute_candidates` 当前从 **L2 本地 SQLite 读**，但那个库是空的（L2 只初始化了 `l2_*` / `scheduler_state` / `l2_mirror_state` 表，markets 表从不在 L2 写入）。所以 scanner recipe / watchlist 路径在 prod 实际返回零行——**只有 3 个 `bootstrap_asset_ids` 在真正驱动 WS 订阅**。「扩 candidate set」不是简单调大一个数字，而是先让 candidate 计算能读到数据（切到 Supabase `markets_latest`），扩容才有意义。

**本 phase 交付**：
1. compute_candidates 数据源从 L2 本地空 SQLite → Supabase `markets_latest` REST 查询
2. candidate set 扩到真实规模（复用现有 recipe + cap 500）
3. 真实 candidate scale 下验证 WS storm / watchdog throughput（补 Phase 03.1 Inj L2-4 只验逻辑的欠账）
4. `markets_latest.yes_token_id` 补列（Phase 02 mirror 投影 gap）
5. GAP-200：config-disable mirror 也 surface 成 chain-truth

**非目标（不在 scope）**：
- WS /book + /prices 增量推送（Phase 05）
- Polymarket WS 订阅数/msg-rate 真实上限的全面调研（research 阶段触及即可，不展开成独立 phase）
- 升 Supabase Pro / Neon（D-01 反 research 风险预案，触发条件未到）
- M2/M3/M4 workstream 推进（独立 workstream）

</domain>

<decisions>
## Implementation Decisions

### candidate 数据源切 Supabase（核心，是其它一切前提）

- **D-01:** **compute_candidates 改读 Supabase `markets_latest`，拉全量进临时 SQLite 复用现有 scanner**。L2 收到 NOTIFY 后，从 Supabase REST 拉当前 snapshot 的 `markets_latest` 全量（现实规模不超几千行）→ 写入临时 SQLite → 现有 scanner recipe SQL 原封不动跑。复用 4 层 SQL 注入防护 + 6 内置 recipe + YAML 自定义 recipe，改动面最小。
  - **Why not B (recipe 重写成 PostgREST filter)**：现有 6 recipe + YAML 自定义都要重写，且 PostgREST 表达力不如 SQL（多条件/受限表达式求值）。
  - **Why not C (混合 REST + 本地)**：两套查询路径增复杂度，不值。

- **D-02:** **临时库形态 = `:memory:` SQLite + 适配层把 narrow 行填成 scanner 期望的 schema**。用 `sqlite3 :memory:` 建与本地 markets 表同 schema 的表，把 `markets_latest` narrow 列映射进去，缺的列（如 clob 字段）填 NULL/默认；recipe SQL 不改；每次 refresh 重建，生命周期短。
  - **⚠ Integration risk（chain-truth 关联，Phase 03.1 L4 lesson）**：`:memory:` 每次重建 + narrow 列缺失填 NULL → 如果某 recipe 依赖了被填 NULL 的列（如 clob 字段过滤），会**静默返回错误 candidate set 而不报错**。适配层必须对「recipe 依赖了 narrow 没有的列」**fail-loud**（启动期校验 recipe 列依赖 ⊆ 临时库可填列集合），而非 fail-silent。research 阶段必须列清 6 内置 recipe + watchlist 路径的列依赖清单。

### candidate 选择标准 + 规模

- **D-03:** **复用现有 recipe（near-end 过滤 + liquidity 降序排序）+ MAX_CANDIDATES=500 cap**。不另发明选择逻辑，markets_latest 跑现有 recipe，保留 cap=500。真实规模现实不超几千 markets，near-end 子集远低于 500 cap。先用默认 recipe 验证通路，调优留后续。

- **D-04:** **保留 `bootstrap_asset_ids` 作冷启动兜底**。L2 刚启动还没收到第一个 NOTIFY 时，靠 `POLYARB_BOOTSTRAP_ASSET_IDS` 先订阅几个高流动性 market；收到第一次 refresh 后被真实 candidate set 接管。两层兜底不冲突——bootstrap 是 WS 订阅 initial state，与 compute_candidates 计算路径独立（scout 确认二者当前已解耦）。

### L2 throughput 验证

- **D-05:** **用真实 candidate set 规模（D-03 算出的 N 个 near-end markets）在 polyarb-l2 prod 上跑真实 WS 订阅 + Inj L2-4 storm**。最接近真实生产负载，不造合成流量。candidate set 扩完是多少就用多少。
  - **Why not B (合成 100+ asset 高 msg-rate)**：偏离真实分布；真实负载才是要验的对象。

- **D-06:** **throughput pass = 三指标全过**：(1) 真实负载下零丢帧（或丢帧率 < 阈值）(2) watchdog 不因高负载误触 RECONNECTING (3) 内存不随 candidate 数线性爆。具体阈值由 research 阶段先跑一次正常负载拿 baseline 后定。
  - 这是对 Phase 03.1 Inj L2-4「daemon survived + recovery clean」逻辑验证标准的**升级**——加上真实 throughput 指标。

### 投影 gap 收尾

- **D-07:** **`markets_latest.yes_token_id` 加为 nullable 列**。Alembic add-only migration（遵 Phase 01.1 P7 schema 纪律，不改现有列）；从本地 SQLite `markets.yes_token_id`（schemas.py:110）取源；空值透传 NULL，不阻断 mirror。加进 `supabase_mirror.py` 的 `_NARROW_MARKET_COLUMNS` + `narrow_market_row()` 映射。
  - 背景：Phase 1 Open Items 已记「top-of-book single-side, 只 yes_token_id populated」，但仍可能有 market 连 yes 都缺 → nullable 是安全选择。

- **D-08:** **GAP-200 修法 = 区分两种 mirror 禁用态**：
  - (a) `supabase_url` 也空 = 根本没配 Supabase → 合理不注册 sub-check（保持现状）
  - (b) `supabase_url` 有但 `service_key` 空 = 可能误操作 → 注册 `mirror:l2_tob_age_seconds` status=fail + output="mirror disabled by config (service_key empty)"
  - 门控点：`src/polyarb/http/l2_health.py:180` 的 `if getattr(settings, "l2_mirror_enabled", False):`，改为区分上述两态。把 config-disable 变成 chain-truth 信号 —— 正是 Phase 03.1 L4 lesson（config-disabled fail-soft 也要 surface）的 inverse 收尾。

### the agent's Discretion

- Plan 切分波次（Wave）— gsd-planner 按依赖图自动决。明显的依赖：D-01/D-02（数据源切换）是 D-03/D-05 的前提；D-07/D-08 独立可并行。
- 临时库具体建表代码 / 适配层映射细节 — 沿用现有 SQLiteStore schema 定义。
- 测试覆盖 — 沿用 Phase 03 RED-first chaos test pattern + scanner 既有测试。
- commit boundary — 一个决策组一个 plan，plan 内多 commit 可接受。
- D-06 三指标的具体数值阈值 — research 阶段拉 baseline 定，planner 写进 verify criteria。

### Folded Todos

无新 folded todo —— 本 phase scope 全部来自 Phase 03.1 chaos 欠账 + Phase 02/03 投影 gap + GAP-200，不涉及 backlog todo。

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 03.1 直接源头（欠账定义）

- `.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/03.1-LEARNINGS.md` — L4 (config-disable chain-truth gap = GAP-200 依据) + S8 (chaos cadence) + Inj L2-4 只验逻辑的欠账
- `.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/03.1-SOAK-LOG.md` — Inj L2-4 段「3-asset bootstrap is small enough that WS storm is really WS close + reconnect (no genuine storm rate)」原话（D-05 throughput 验证的直接动因）
- `.planning/workstreams/m1-perception/phases/03.1-l2-observability-gaps-fix-up/deferred-items.md` — GAP-200 完整定义

### candidate 计算 + scanner（D-01/D-02/D-03 改动点）

- `src/polyarb/observation/l2_candidate_refresh.py:68-192` — `compute_candidates()`，当前读 `settings.db_path` 本地 SQLite（line 94 `mode=ro`）；watchlist slug→yes_token_id 在 line 151；`MAX_CANDIDATES=500` 在 line 13，`cap=500` log 在 line 264；`REFRESH_DEBOUNCE_S=60.0` 在 line 15
- `src/polyarb/observation/l2_candidate_refresh.py:213-291` — `on_snapshot_complete()` refresh trigger flow（debounce check line 240-247）
- `src/polyarb/snapshot/scanner.py:312` — `run_recipe(db_path, recipe)`，scanner SQL 入口（D-02 临时库要喂这个）
- `src/polyarb/storage/schemas.py:110` — 本地 SQLite `markets` 表 schema（含 yes_token_id，D-02 临时库要对齐的 schema 源 + D-07 yes_token_id 源）
- `src/polyarb/daemon/l2_main.py:286-293` — bootstrap_asset_ids 解析 + passed to WsConsumer（D-04 保留点）
- `src/polyarb/config.py:95-98` — `Settings.bootstrap_asset_ids`（`POLYARB_BOOTSTRAP_ASSET_IDS`）

### markets_latest 投影（D-07 改动点）

- `alembic/versions/001_initial_dashboard_schema.py:52-67` — markets_latest 当前 10 列 schema（yes_token_id 缺失，D-07 加列处）
- `src/polyarb/storage/supabase_mirror.py:45-61` — `narrow_market_row()` + `_NARROW_MARKET_COLUMNS`（D-07 narrow projection 补列处）

### GAP-200 /health（D-08 改动点）

- `src/polyarb/http/l2_health.py:174-216` — mirror sub-check 注册块；门控条件在 line 180 `if getattr(settings, "l2_mirror_enabled", False):`（D-08 改这里区分两种禁用态）
- `src/polyarb/config.py` — `l2_mirror_enabled` model_validator auto-detect（service_key 非空时自动 True）

### chaos 基础设施（D-05 throughput 验证复用）

- `src/polyarb/daemon/ws_consumer.py` — WsConsumer subscribe loop + watchdog（D-05 storm 验证对象）+ `POLYARB_WS_TEST_KILL` primitive（Phase 03.1 Plan 06）
- `Makefile` chaos-l2-inj4 target — Phase 03.1 Plan 06 建立的 WS storm orchestrator（D-05 复用/扩展）
- `docs/dev/chaos-toolkit.md` — image-aware chaos 工具矩阵（PROCESS-2）

### 用户偏好 / 工程纪律（必应用）

- memory `feedback_code-vs-chain-truth-2026-05.md` — fail-soft 必须 surface /health（D-08 + D-02 fail-loud 依据）
- memory `feedback_parallel-worktree-rebase-discipline-2026-05.md` — 并行 worktree 必 rebase main + 验证 deployed image == 最新 plan-merged main（D-05 prod chaos 前置）
- memory `feedback_fly-api-token-shadowing-2026-05.md` — flyctl 前 `FLY_API_TOKEN=` prefix（D-05 prod 操作）
- `.planning/threads/market-observation-architecture.md` §1.6 — chain-truth discipline（plan-checker review-time 检查 D-02/D-08）

### M1 上下文

- `.planning/workstreams/m1-perception/STATE.md` — 当前进度位
- `.planning/workstreams/m1-perception/ROADMAP.md` § Phase 04 — scope 权威列表

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets

- `src/polyarb/snapshot/scanner.py` — 完整 scanner 引擎 + 6 内置 recipe + 4 层 SQL 注入防护（D-01 复用，临时库喂给它）
- `src/polyarb/observation/l2_candidate_refresh.py` — compute_candidates + on_snapshot_complete + debounce（D-01 改数据源，其余逻辑保留）
- `src/polyarb/storage/supabase_mirror.py` — narrow projection 模式（D-07 加列模板）
- `src/polyarb/daemon/ws_consumer.py` — WS subscribe loop + watchdog + POLYARB_WS_TEST_KILL（D-05 storm 验证对象）
- `Makefile` chaos-l2-inj4（D-05 storm orchestrator 复用）
- `src/polyarb/storage/schemas.py` markets 表 DDL（D-02 临时库 schema 对齐源）

### Established Patterns

- **scanner recipe = SQL on SQLite**（D-01 的关键约束：要喂 SQLite-shaped 数据，故选临时库而非 REST 重写）
- **narrow projection**（supabase_mirror）— L1 把全 markets 行投影成 dashboard 用的窄列（D-07 加列遵此）
- **chain-truth own-dog-food**（Phase 03.1 P4）— 每个 fail-soft / config-disable 都 surface 到 /health（D-08 遵此）
- **RED-first chaos test**（Phase 03）— 先写 expected truth assertion（D-05 throughput 验证遵此）
- **Alembic add-only schema**（Phase 01.1 P7）— 不改/不删现有列（D-07 遵此）

### Integration Points

- **D-01 数据源切换**：on_snapshot_complete 收到 NOTIFY payload（含 snapshot_id）→ 从 Supabase 拉对应 snapshot 的 markets_latest → 临时库 → scanner。注意 payload 当前只带 snapshot_id，不带 markets 数据，必须 REST roundtrip。
- **D-02 临时库**：scanner 期望本地 markets 表 schema（含 clob/yes_token_id 等列），markets_latest 是 narrow 10 列 → 适配层填 schema，缺列 NULL，但 recipe 列依赖必须 fail-loud 校验。
- **D-05 prod chaos**：跑前必须验 deployed image == 最新 plan-merged main（Phase 03.1 L2 dueling-implementation 教训），且 candidate set 扩容代码已 deploy。
- **D-07 yes_token_id**：本地 markets.yes_token_id → narrow_market_row 映射 → Alembic 加列 → markets_latest。D-01 临时库的 yes_token_id 也从这条投影来（闭环：D-07 让 markets_latest 有 yes_token_id，D-01 临时库才能填它给 watchlist slug 解析用）。

</code_context>

<specifics>
## Specific Ideas

- **D-02 fail-loud 校验**：启动期（或临时库构建期）assert `recipe 用到的列 ⊆ 临时库可填列集合`，否则 raise + log，绝不静默跑出错误 candidate set。research 阶段产出「6 内置 recipe + watchlist 路径的列依赖清单」作为校验基准。
- **D-06 baseline 先行**：throughput pass 阈值不拍脑袋，research 阶段先在真实 candidate scale 下跑一次正常负载，记录丢帧率/延迟/内存 baseline，pass 阈值 = baseline + 合理裕量。
- **D-01 拉取失败 fail-soft**：从 Supabase 拉 markets_latest 失败时（Supabase paused / 429 / 网络），candidate refresh 应 fail-soft（保留上一次 candidate set + log + 可能 surface /health），不崩 daemon。这与 Phase 03 mirror fail-soft 同款纪律（research 确认现有 on_snapshot_complete 的 error envelope）。

</specifics>

<deferred>
## Deferred Ideas

- **candidate recipe 调优**（D-03 只先用默认 recipe 验证通路）→ 真实负载观察后，若默认 recipe 选出的 candidate 不理想，再调 recipe / 加 L2 专用 recipe。m1-perception backlog。
- **合成高负载压测**（D-05 选了真实负载）→ 若真实负载远未触及 throughput 极限，想知道天花板时再做合成 100+ asset 压测。m1-perception backlog 或 m5-polywatch chaos trial。
- **refresh debounce 调优**（当前 60s）→ 真实 candidate scale 下若 refresh 太频/太疏，再调 REFRESH_DEBOUNCE_S。本 phase 不动。
- **Supabase Pro / Neon 升级** → D-01 反 research 风险预案，触发条件（Supabase pause 真发生 + M3 实盘）未到，继续观察。
- **POLYARB_WS_TEST_KILL nightly cron**（D-05 storm 验证基础设施）→ m5-polywatch trial-2 落地后纳入 nightly cron。

### Reviewed Todos (not folded)

无 — 未做 cross_reference_todos（本 phase scope 来自 carry-over，无 backlog todo 匹配）。

</deferred>

---

*Phase: 04-candidate-set-l2-throughput*
*Context gathered: 2026-05-28 (SESSION 30)*
