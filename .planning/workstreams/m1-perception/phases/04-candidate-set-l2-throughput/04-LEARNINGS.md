---
phase: 04
phase_name: "candidate-set-l2-throughput"
project: "Polymarket Arbitrage"
generated: "2026-05-29"
counts:
  decisions: 11
  lessons: 9
  patterns: 8
  surprises: 5
missing_artifacts: []
---

# Phase 04 Learnings: candidate-set-l2-throughput

> Phase goal: 让 L2 candidate set 从 3 个 bootstrap asset 扩到真实规模 (markets_latest
> 全量, 30-200 near-end markets), 补 Phase 03.1 Inj L2-4 "只验逻辑不验 throughput" 欠账,
> 收尾投影 gap (D-07 yes_token_id) 和 chain-truth gap (D-08 GAP-200)。
>
> 实际交付: 4 plan 全 ship; G-01 cold-start trap 在 prod 暴露并修复; D-06 throughput
> verdict DEFERRED (被新发现的 G-02/G-03/G-04 三个结构性问题阻塞); Phase goal 部分达成
> ("candidate-set expansion works in prod when cold-start is fixed", throughput verdict
> 留作 follow-up plan)。

## Decisions

### D-01 数据源从 L2 本地空 SQLite 切到 Supabase markets_latest REST
L2 `compute_candidates` 原本读 `Path(settings.db_path)` (L2 本地 SQLite), 但 markets 表从不在 L2 写入 → recipe 永远返回零行, 只有 3 个 bootstrap_asset_ids 驱动 WS。改为从 Supabase `markets_latest` REST 全量拉取 → 写入命名临时 SQLite → 现有 scanner recipe SQL 原封跑。

**Rationale:** "扩容" 的前提是让通路先跑起来 — scout 发现 recipe 路径在 prod 实际返回零行, 不是 candidate set 太小的问题, 是通路根本没接通。
**Source:** 04-CONTEXT.md, 04-RESEARCH.md, 04-02-SUMMARY.md

### D-02 命名临时文件 SQLite (NOT `:memory:`)
临时库用 `tempfile.NamedTemporaryFile(suffix='.db', delete=False)` + `os.unlink` in finally, 不用 `:memory:`。

**Rationale:** scanner.run_recipe 自己开一条独立 connection (`scanner.py:142` `file:{db_path}?mode=ro` URI)。两个 `:memory:` connection 是两个独立 DB — 适配层填的数据对 scanner 的 connection 不可见。必须真实文件路径才能跨 connection 共享。
**Source:** 04-RESEARCH.md (Pitfall 1), 04-02-SUMMARY.md

### D-02 Option A — PRAGMA foreign_keys=OFF on temp DB (over seed-snapshots-row Option B)
临时库 `PRAGMA foreign_keys=OFF`, 不去 seed 一个 snapshots 行来满足 FK。

**Rationale:** FK 完整性在一个 scanner 只读打开的 throwaway DB 上毫无意义; Option A 更少 LOC。
**Source:** 04-02-SUMMARY.md

### NOT NULL 列用 sentinel-fill, nullable 列用 NULL-fill
markets DDL 有 4 个 NOT NULL 列 (condition_id / fetched_at_ms / snapshot_id / incomplete) 不在 narrow projection 里。适配层用 `_SENTINEL_FILL` dict 填默认值 (condition_id='', fetched_at_ms=0), 不是 NULL-fill。

**Rationale:** NULL-fill NOT-NULL 列会 INSERT 约束违反。recipe 引用到 NULL-filled nullable 列时 log WARNING (不 crash) — fail-loud 而非 silent 0-row。
**Source:** 04-RESEARCH.md (planner DDL 校验), 04-02-SUMMARY.md

### PostgREST 分页强制 (1000 行硬上限)
`_fetch_all_markets_latest` 用 `.range(offset, offset+999)` 循环, `len(batch) < 1000` 终止。

**Rationale:** markets_latest ~6729 行, PostgREST 1000 行硬上限会静默截断到前 1000 — 不分页 = 拿不全 candidate universe。
**Source:** 04-RESEARCH.md, 04-02-SUMMARY.md

### candidates fetch chain-truth 阈值: warn=120s (2× debounce), fail=600s (10× debounce)
`candidates:supabase_fetch_age_seconds` 子检查 cold-start=warn (NOT fail), fresh=pass, 2× debounce=warn, 10× debounce=fail。

**Rationale:** boot 不能触发 /health alarm (cold-start warn 不 fail); warn 在 2× debounce 给瞬时 blip 留 headroom, fail 在 10× 才报警, sustained Supabase outage 变成 fail status 而非 silent candidate 冻结 (防 Inj L2-2-style failure)。
**Source:** 04-02-SUMMARY.md

### D-07 yes_token_id 列类型 sa.Text nullable=True, 无特殊分支
Alembic 004 给 markets_latest 加 nullable TEXT `yes_token_id` 列; `narrow_market_row()` 不加特殊分支, 用现有默认 `out[col] = full_row.get(col)` 透传 (None 自然传过去)。

**Rationale:** 匹配源语义 (normalizer 返回 `str | None` from clobTokenIds[0]); add-only migration 对存量行填 NULL, 无数据丢失。
**Source:** 04-01-SUMMARY.md

### D-07 [BLOCKING] alembic push 必须先于任何 "列已在 live DB 存在" 的验证
Migration 文件写好 + 单测过后, 在跑任何依赖列存在的验证前, 必须用 live DSN 跑 `make supabase-migrate`。这个 plan 因此 `autonomous: false` — operator 确认 DSN 在场才推。

**Rationale:** build/import 检查在没 push 时也会过 (列在 migration 文件里, 不在 live DB), 造成 false-positive。live DB ALTER 必须先于 mirror 代码 deploy 到 polyarb-l1 (否则 11 列 INSERT 会 column-does-not-exist 失败)。
**Source:** 04-01-PLAN.md, 04-01-SUMMARY.md

### D-08 GAP-200 三分支 /health mirror gate, 不动 config.py
把 `l2_health.py` 的 `if l2_mirror_enabled:` 二分支改成三分支: (a) url 空 → 不注册 (intentional opt-out); (b) url 有 + key 空 → 注册 status=fail 子检查; (c) 都齐全 → 现有 pass/warn/fail age 逻辑不变。`l2_mirror_enabled` flag 仍 False — 只改 /health PRESENTATION。

**Rationale:** Phase 03.1 L4 lesson 的 inverse 收尾 — config-disable 的 fail-soft 也要 surface。operator 配错 (url 配了忘配 key) 不该让 daemon 静默报 healthy。output 字符串只命名缺失 FIELD, 不泄露 value/url (T-04-04)。
**Source:** 04-03-SUMMARY.md, RESEARCH Q6

### G-01 cold-start debounce 初值用 `-REFRESH_DEBOUNCE_S - 1.0`
`_last_refresh_at_s` 模块初值从 `0.0` 改为 `-REFRESH_DEBOUNCE_S - 1.0` (F1 一行修法)。

**Rationale:** `time.monotonic()` 进程启动后返回 ~0..N 秒, 初值 0.0 让 first NOTIFY 的 `elapsed = monotonic - 0` 永远 < 60s → debounce 永久吞掉首次 fetch。负初值让 first call 必过 debounce, 后续 debounce 语义不变。F1 比 F2 (sentinel None) / F3 (startup-prime) 改动面最小, 现有 monkeypatch 测试仍有效。
**Source:** 04-04 prod 实证 + 39c60ef commit; memory feedback_cold-start-debounce-trap-2026-05

### D-06 verdict DEFERRED, Phase 04 仍可关闭
Task 3 prod chaos 执行完毕但 D-06 三指标 verdict DEFERRED (被 G-02/G-03/G-04 阻塞)。决定 Phase 04 仍关闭, G-02/03/04 留作 follow-up plan。

**Rationale:** D-04 (G-01 cold-start) 已 prod 实证; D-06 真 verdict 需要先修三个独立的结构 gap, 回到 04-04 不如开新 plan 清楚; 4 plan 全 SUMMARY ✓ 符合关闭门槛。chaos primitive 跑通 + instrumentation 工作 + 三个 finding 落库 = phase 范围内能 establish 的都 establish 了。
**Source:** 04-04-SUMMARY.md, 04-SOAK-LOG.md

## Lessons

### prod 验证抓出了 plan-checker + planner 都没抓到的 cold-start trap (G-01)
Phase 04 全 planning 链 (context → research → patterns → plan → checker 11/12 PASS) 都没发现 `_last_refresh_at_s = 0.0` + `time.monotonic()` 的 cold-start trap。直到 v18 deploy 后看 prod /health 才暴露: 31 个 catchup snapshot 在 9ms 内全 debounce, D-01 fetch path 从未运行。

**Context:** planner 假设 "first NOTIFY 触发 fetch", 但没问 "first NOTIFY 当下 monotonic clock 距离 _last_refresh_at_s 的 0.0 初值是多少" — 一条 "时间链路" 的 chain 没走通。单测用 `_reset_debounce_state` fixture 显式置 `_last_refresh_at_s = 0.0` 然后 mock monotonic 跳过 trap, 集成层缺 "first NOTIFY MUST run, not debounce" 契约测试。
**Source:** 04-04-SUMMARY.md, 04-SOAK-LOG.md

### chain-truth 子检查在 prod 主动暴露了静默故障 — 设计 paid off
`candidates:supabase_fetch_age_seconds=null "cold-start: never fetched"` 正是 Plan 02 Task 3 加的 chain-truth 子检查, 它直接暴露了 G-01 bug。没有这个子检查, daemon 看起来一切正常 (3 bootstrap WS 收事件, mirror 写 Supabase, /health 只是 warn 因 ws_state=WAITING_FOR_EVENT 常态), bug 会潜伏到有人主动看 candidate set 大小。

**Context:** 这是 Phase 02-03-04 累积的 chain-truth 纪律 (fail-soft 路径必须 surface 到 /health) 的正向回报 — 与 Phase 03.1 L4 lesson (mirror disabled 静默 5 天) 是同一纪律的两面。
**Source:** 04-04-SUMMARY.md, memory feedback_code-vs-chain-truth-2026-05

### parallel-worktree rebase 纪律的反面: prod image lag 让 bug 藏了一天
第一次 deploy 前 prod 跑的是 v16 (May 27, pre-Phase-04)。如果不先验 `flyctl image show` == 最新 main 就直接跑 chaos, 会在 3 bootstrap 上跑 chaos, 不能归因到 Phase 04 代码, 制造 plan-code drift。pre-flight Step 1 (deployed image == latest main) 正确 abort 了首次尝试。

**Context:** memory feedback_parallel-worktree-rebase-discipline 的 prod 维度延伸 — "deployed image == latest plan-merged main" 是跑 prod chaos 的硬前置, 不只是 worktree 之间的 rebase。
**Source:** 04-SOAK-LOG.md, memory feedback_parallel-worktree-rebase-discipline-2026-05

### `:memory:` SQLite 不可跨 connection 共享 (推翻 CONTEXT D-02 假设)
CONTEXT 阶段假设临时库用 `:memory:`。research 推翻: scanner.run_recipe 自开一条 connection, 两个 `:memory:` 是两个独立 DB, 适配层填的数据对 scanner 不可见。

**Context:** "in-memory SQLite is shared" 是一个常见误解 — Python sqlite3 默认每个 `:memory:` connection 是独立的 (除非用 `file::memory:?cache=shared` URI + 同进程)。research 阶段的代码级 chain-walk (确认 scanner.py:142 自开 connection) 比 CONTEXT 阶段的假设可靠。
**Source:** 04-RESEARCH.md (Pitfall 1), 04-02-SUMMARY.md

### Fly `flyctl secrets set/unset` = rolling restart, 不是 in-flight env mutation (G-03)
chaos 用 `flyctl secrets set POLYARB_WS_TEST_KILL=1` 触发的是 machine-level deploy/rolling-restart, 不是给运行中进程注入 env。pre-storm 的 60-asset 进程被终止了, "storm 后 60s wait" 测的是新进程 startup 不是 kill-recovery。

**Context:** 整个 Phase 03 D-04 (POLYARB_WS_TEST_KILL 探针, 上 phase 设计) 都需要从 "env var 探针" 重设计成 "in-band signal" (如 HMAC-protected `POST /admin/chaos/ws-test-kill` 翻进程内 atomic flag, 不重启)。Fly secrets 的语义就是 deploy-per-set。
**Source:** 04-SOAK-LOG.md (G-03)

### D-01 fetch 在 restart-without-NOTIFY-backlog 时不触发 (G-02)
任何 L2 restart 落在 L1 NOTIFY 静默窗口里 (L1 cycle ~30+ min), `catchup_from_cursor` 报 "no missed snapshots", `on_snapshot_complete` 不被调用, markets_latest fetch 不跑, subs 留在 3 bootstrap。v18 首次 boot 拿到 60 是因为正好有 v17→v18 deploy 间隙累积的 31 NOTIFY backlog。

**Context:** D-01 跨 restart 不健壮。G-01 fix 只解决了 "first NOTIFY 被 debounce 吞", 没解决 "压根没有 first NOTIFY" 的情况。最小 fix: catchup 完无条件调一次 `on_snapshot_complete({"_startup_prime": True}, ...)`。
**Source:** 04-SOAK-LOG.md (G-02)

### RSS 测量读到 PID 1 = Fly hallpass, 不是 Python (G-04)
`grep VmRSS /proc/1/status` 在 Fly 机器里 PID 1 是 `/.fly/hallpass` (Go binary ~6.4MB), 不是 Python L2 进程。整个 chaos 的 RSS_baseline/recovery 都测错了对象。

**Context:** Fly machine 的 init 不是应用进程。RSS 要 `pgrep -f 'python -m polyarb.daemon.l2_main'` 或在 /health 加 `process:rss_kb` 子检查 (psutil)。
**Source:** 04-SOAK-LOG.md (G-04)

### L1 snapshot cycle ~30+ min 让 5-min prod 实测窗口不可能等到自然 NOTIFY
即使没有 G-01/G-02 bug, 在 prod 跑 5-min 实测也等不到自然 NOTIFY (L1 cycle 30+ min)。这是 plan A2 documented-deferral 隐含触发条件的另一面 — prod chaos 的时间尺度受上游 cycle 约束。

**Context:** 验证 candidate-set throughput 必须要么 (a) 主动触发 snapshot (via /scan 或 startup-prime), 要么 (b) 撞高活跃日历窗口。被动等 NOTIFY 在 5-min 窗口不现实。
**Source:** 04-SOAK-LOG.md

### Fly deploy 偶发 machines-API EOF — image push 成功 ≠ deploy 完成
首次 v17 deploy 时 image 推到 registry 成功但 machines API `EOF` 失败, machine 未更新。重试用 `--image` 复用刚推的镜像才成功。

**Context:** deploy script 应当有 "machine-update 没执行" 的检测 (image push 完不等于 deploy 完)。第二次 v18 deploy 一气呵成无 EOF — 偶发性。可入工业化 plan。
**Source:** 04-SOAK-LOG.md

## Patterns

### Real-file SQLite adapter from a narrow REST projection
从 narrow REST projection 构造一个 scanner 可读的临时 SQLite: `tempfile.NamedTemporaryFile` + 完整 DDL + sentinel-fill 缺失 NOT-NULL 列 + NULL-fill nullable 列 + recipe 引用 NULL-filled 列时 WARN + `PRAGMA foreign_keys=OFF` + finally `os.unlink`。

**When to use:** 当一个现有 SQL 消费者 (scanner) 期望本地 SQLite, 但真实数据源是远程 REST/Postgres 时 — 不改消费者, 在它前面架一个临时库适配层。
**Source:** 04-02-SUMMARY.md (l2_temp_db.py)

### Fail-soft fetch with chain-truth surface
写侧成功时调 `_record_fetch_success()` 更新 module-level timestamp; 读侧 /health 子检查通过 public getter 读同一字段; sustained failure 变成 fail status 而非 silence。失败时 fall back 到 last-known rows (candidate set 冻结而非塌成空)。

**When to use:** 任何 fail-soft 路径 — 配一个 /health 子检查读写入侧真在 mutate 的字段 (不是 dead-code config gate), 让静默失败变成可观测信号。
**Source:** 04-02-SUMMARY.md, memory feedback_code-vs-chain-truth-2026-05

### Three-branch chain-truth gate (replaces binary if-flag)
当一个内部 flag 由 N 个输入计算出来, /health surface 应该 branch on RAW INPUTS (让 observability 看到 config 看到的同样 shape), 不只 branch on 派生 flag。把 misconfiguration case 作为显式分支。

**When to use:** 任何 "flag = f(input_a, input_b)" 且 flag=False 有多种原因 (合理禁用 vs 误配) 的 fail-soft surface。让 /health 区分这些原因。
**Source:** 04-03-SUMMARY.md

### PostgREST pagination loop
`.range(offset, offset+page_size-1)` + `len(batch) < page_size` 循环终止。

**When to use:** 任何 supabase-py / PostgREST 全量拉取 — 1000 行硬上限会静默截断, 必须分页。
**Source:** 04-RESEARCH.md, 04-02-SUMMARY.md

### Pre-fetch data BEFORE compute, so one temp DB feeds multiple call paths
在 compute_candidates 之前先 fetch markets data, 让同一个临时库同时喂 NOTIFY-driven 和 ad-hoc compute 路径。

**When to use:** 当一个 compute 函数有多个触发入口 (event-driven + manual), 把数据获取提到 compute 外面, 避免每个入口各自 fetch。
**Source:** 04-02-SUMMARY.md

### Cold-start sentinel for monotonic-clock cooldown
任何 `_last_X_at_s` + `time.monotonic()` + 阈值 N 秒的 debounce/cooldown, 初值必须 < `-N` (或用 None sentinel) 以保证 first call 通过。

**When to use:** 所有 cooldown/rate-limit/debounce 模式 (WS reconnect / API retry backoff / cache invalidation / rate-limit window)。检查清单: 初值 0.0? 用 monotonic? 阈值 > 启动到首次调用的时间? first call 该立即执行? 任一 "是" → cold-start trap。
**Source:** 39c60ef commit, memory feedback_cold-start-debounce-trap-2026-05

### importlib.reload for cold-start contract tests
用 `importlib.reload(mod)` 观察模块加载时的字面状态, 免疫其它测试的 monkeypatch 泄漏, 断言 cold-start 不变量。

**When to use:** 测一个 module-level 状态变量的初值契约 (而不是运行时 monkeypatch 后的值) — autouse fixture 会污染, reload 给出干净的 cold-start 视角。
**Source:** tests/observation/test_l2_candidate_refresh_coldstart.py

### Pre-flight gate before prod chaos: deployed image == latest plan-merged main
跑任何 prod chaos 前 `flyctl image show` 比对 `git log origin/main -1`, 确认 running image 含被测代码; candidate count > 阈值确认数据通路有效; 不符就 abort, 不在错的 image 上跑 chaos。

**When to use:** 任何对 live 服务的 fault injection — 验证测的是部署的代码 (chain-truth), 不是 main 上的代码。
**Source:** 04-04-PLAN.md, 04-SOAK-LOG.md, memory feedback_parallel-worktree-rebase-discipline-2026-05

## Surprises

### D-01 "扩容" 的真相是 "通路根本没接通"
原以为 candidate set 只是 "太小 (3 个)" 要扩。scout 发现真相: L2 compute_candidates 读的本地 SQLite 是空的, recipe 路径 prod 返回零行, 3 个 bootstrap 是 hardcoded fallback。"扩容" 的前提是先让 Supabase → temp DB → scanner 通路跑起来。

**Impact:** 重塑了整个 phase scope — Plan 02 从 "调大 cap" 变成 "接通数据源 + 分页 + 临时库适配 + fail-soft"。
**Source:** 04-CONTEXT.md scout, 04-02-SUMMARY.md

### 一个 pre-existing latent bug 在新路径下才致命 (BUILTINS drop)
`compute_candidates` 有 `list_all_recipes(scanner_yaml) if scanner_yaml else {}` — 当 scanner_yaml=None 时丢掉所有 BUILTIN_RECIPES。Phase 04 D-01 prod 正常跑 scanner_yaml=None, 这个潜伏 bug 让所有 builtins (near-end/coin-flip/etc) 静默失效, 必须 Rule-1 auto-fix 才能让新路径 drive candidates。

**Impact:** 1 行 fix, 但说明: 一个 "只在某 config 路径下触发" 的 latent bug 可以潜伏很久, 直到新功能恰好走那条路径。
**Source:** 04-02-SUMMARY.md (Rule 1 deviation)

### G-01 在 prod 30s 内自证 — fix 立竿见影
v18 deploy 后 30s, `subs` 从 3 跳到 60, `fetch_age` 从 null 变 91.4s。G-01 fix 的效果在 prod 上立即可见 (catchup 重放的 31 个 backlog NOTIFY 第一个就过 debounce 触发 fetch)。

**Impact:** 验证了 fix 正确, 也验证了 chain-truth 子检查作为 fix 的 acceptance signal 是可靠的 (不用猜, 看 /health 数字)。
**Source:** 04-SOAK-LOG.md (v17 vs v18 对比)

### 一次 chaos run 暴露三个独立的结构性问题 (G-02/G-03/G-04)
计划只想拿 D-06 throughput verdict, 实际跑出来三个互相独立的设计 gap: D-01 跨 restart 不触发 (G-02), chaos primitive 是 restart 不是注入 (G-03), RSS 测错进程 (G-04)。三个都阻塞 verdict 计算。

**Impact:** D-06 verdict DEFERRED; 三个 G-* 成为 follow-up plan 的 task ladder。prod 实测的信息密度远高于 CI — 一次跑暴露三层问题。
**Source:** 04-SOAK-LOG.md

### prod `/health=fail` 是 "已知降级" 不是 "未知故障"
chaos cleanup 后 prod `/health.status=fail`, 但根因明确: mirror_age>600s 因为 3 bootstrap 资产低活跃没产生 qualifying frames (G-02 留下 3 bootstrap)。WS 连着, listener 听着, mirror pipeline 能 push (initial dump 证明)。不是 regression, 不是 chaos 残留。

**Impact:** 区分 "已知降级" 和 "未知故障" 是运维判断的关键 — 前者等下一个 L1 NOTIFY 或 G-02 fix 自然恢复, 不需要紧急介入。chain-truth 让根因可追 (mirror_age 数字 + G-02 解释), 避免误判为 prod incident。
**Source:** 04-SOAK-LOG.md (cleanup verification)
