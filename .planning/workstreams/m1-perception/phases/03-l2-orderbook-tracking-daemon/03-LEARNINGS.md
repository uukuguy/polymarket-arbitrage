---
phase: 03
phase_name: "l2-orderbook-tracking-daemon"
project: "polymarket-arbitrage"
generated: "2026-05-25"
counts:
  decisions: 11
  lessons: 10
  patterns: 8
  surprises: 7
missing_artifacts:
  - "*-VERIFICATION.md (not generated; verifier step deferred — chaos SOAK-LOG serves the role)"
  - "*-UAT.md (not used in this workstream)"
---

# Phase 03 Learnings — L2 Orderbook Tracking Daemon

> **Phase outcome (2026-05-25)**: polyarb-l2.fly.dev shipped — separate Fly app, single binary / two deployments. 8 plans / 7 waves / 1 chaos cycle. 5 chaos Inj designed → 3 live PASS (L2-1 machine restart, L2-2 fail-soft envelope partial, L2-3a B1 default OFF) + 2 deferred to Phase 03.1 (L2-3b/L2-4) + 1 ad-hoc deferred (L2-5). Phase ships with **5 GAPs carry-over** from Inj L2-2 (observability chain gap: mirror failure → /health 503 never wired despite code-level fail-soft passing unit tests).
> **Architecture footprint**: L1 (polyarb-l1) NOTIFY → L2 (polyarb-l2) LISTEN over Postgres asyncpg + Supabase realtime, feature-flagged OFF by default (B1 spawn constraint). WS market channel single connection ≥1000 tokens, 30s business-layer watchdog + exp backoff 1/2/4/8/16/30s + storm cap. Supabase Free with GHA cron keepalive + Better Stack heartbeat (rejected $25/mo Pro tier).
> **Key process upgrade**: Wave 5 deploy verification surfaced **catchup_from_cursor + bootstrap_asset_ids hybrid** (not in any PLAN.md — invented mid-deploy when fresh prod DB had no LISTEN history). Phase 03.1 must codify this as the canonical L1→L2 reconciliation path.

---

## Decisions

### D-01: DB tier = Supabase Free + GHA cron keepalive (反 research 推荐)

研究建议 Supabase Pro $25/mo 避 7-day pause 风险; 用户 lock Free + GHA cron 06:00 UTC daily wget REST + Better Stack heartbeat (25h tolerance) 三件套. Trigger to upgrade: 一次 pause 真发生 / M3 实盘启动 / Phase 04 L3 数据量升级.

**Rationale:** GHA cron 完全免费, 24h ping < 7-day 阈值, 加 BS heartbeat closes Phase 02 L8 single-path GHA-email gap. 当下 phase 价值不抵 $25/mo.
**Source:** 03-CONTEXT.md D-01 + 03-01-SUMMARY.md

### D-02: 采集方式 = WS market channel 主 + REST backfill 混合

L2 daemon 主路径 = WS subscribe candidate-set 整个 token (单 connection 无上限, Polymarket 2026-05-28 取消 100 token 限制), `price_change` / `best_bid_ask` / `last_trade_price` / `book` 四类 event. REST = 启动 backfill + WS 冻结 fallback + candidate refresh 拿新 token ids.

**Rationale:** WS 毫秒级 vs REST polling 1-5min lag, L2 时序数据信号 loss 风险高. Research evidence-based 推荐.
**Source:** 03-CONTEXT.md D-02 + 03-04-SUMMARY.md (real-WS smoke 30s 3 frames PASS)

### D-03: WS staleness watchdog = 30s 无 event → 硬重连 + initial_dump=true

业务层 watchdog 30s 无 event 触发重连 (不依赖 TCP keepalive, issue #292 silent freeze bug 2026-03 仍 open); 指数退避 1/2/4/8/16/30s capped; idempotent re-subscribe; R5 storm cap (10/hr → DEGRADED_REST_POLLING + Sentry warning).

**Rationale:** PONG 仍回但 event 流冻结 = TCP-only 心跳无效. 30s 是 research 推荐, 平衡误报 vs 时延.
**Source:** 03-CONTEXT.md D-03 + 03-04-SUMMARY.md (WsWatchdog 218 lines, 9 tests GREEN)

### D-04: 候选集 selection = Phase 01.1 scanner recipe + 手选 watchlist union

YAML 写 ranking rules (e.g. `liquidity > $10k & volume > $5k`), 多 recipes 并行, watchlist 作为 override layer. `compute_candidates(recipes, watchlist) = scanner_result ∪ watchlist`, R9 hard cap=500 (watchlist 永不 truncate), 60s debounce floor.

**Rationale:** 复用 Phase 01.1 04+05+06 现成代码, 数据驱动 + 用户控制双轨, 不 over-design (初期 1-2 recipe + 5-10 watchlist 够).
**Source:** 03-CONTEXT.md D-04 + 03-05-SUMMARY.md (l2_candidate_refresh.py 262 lines, 10 tests GREEN)

### D-05: candidate refresh 触发 = L1 snapshot.complete 事件驱动, Postgres NOTIFY (asyncpg)

L1 orchestrator step 7.7 `pg_notify('snapshot_complete', json)` → L2 daemon `asyncpg.LISTEN` 接收 → 重算 candidate-set diff → WS dynamic subscribe/unsubscribe. Event bus 实现: Postgres NOTIFY (不引入 Redis), pgbouncer transactional pooler 端口 6543.

**Rationale:** Polymarket clob/data 已用 Supabase Postgres, NOTIFY 跨进程零额外依赖, payload <100 bytes 远低于 8KB 限制. Redis 推到 Phase 04+ 多 daemon.
**Source:** 03-CONTEXT.md D-05 + 03-05-SUMMARY.md (asyncpg 0.31, bus.py + listener.py)

### D-06: L2 daemon 进程边界 = 新独立 daemon `polyarb-l2.fly.dev`

新 Fly app, 同 region `ams` (与 polyarb-l1 私网延迟最小), 1024mb memory parity with L1 (Phase 02 OOM S19 precedent argues headroom > thrift), 单 binary 两 deployment (`flyctl deploy --config fly-l2.toml`), 复用 Phase 02 Wave 4 secrets stack.

**Rationale:** WS 长连接 + L1 cron 不抢 CPU; crash 隔离 (L1/L2 互不影响); chaos verification 独立; 未来 polyarb-l3 扩展路径清晰.
**Source:** 03-CONTEXT.md D-06 + 03-02-SUMMARY.md (fly-l2.toml + deploy-l2.yml + fly_secrets_sync.sh)

### D-07: dashboard surface = Supabase mirror 4 tables + Vercel 4 pages 复用

L2 daemon 写 5 Supabase tables (Alembic 003: l2_candidates, l2_top_of_book, l2_trades, l2_signals, l2_event_cursor); Vercel dashboard 加 4 pages (`/candidates`, `/asset/[id]/tob`, `/asset/[id]/trades`, `/signals`) 复用 Phase 02 Wave 4 server-component pattern, anon key + RLS only.

**Rationale:** 复用 Phase 02 dashboard 架构 + Auth/RLS 投资, 零迁移. RLS anon SELECT + service_role bypass 是干净的写/读分离. BRIN on ts ~10x 小 footprint vs btree-only.
**Source:** 03-CONTEXT.md D-07 + 03-06-SUMMARY.md (Alembic 003 5 tables) + 03-08-SUMMARY.md (4 pages)

### D-08: trades 自累积 = WS last_trade_price 全量存 + REST 启动 backfill (idempotent)

WS subscribe candidate-set 全部 token 的 `last_trade_price` event → 写 `l2_trades` (asset_id, price, size, side, trade_hash UNIQUE, ts). 启动 backfill: Polymarket Data API `/trades` paginate (global feed + client-side filter by asset_id, NO server-side asset/before filter — Open Q 2 resolved live). MAX_OFFSET=1000 保守 (cliff at 4000 实测), AsyncLimiter(150,10) 25% headroom.

**Rationale:** issue #216 closed markets `prices-history` 退化 12h 颗粒 → REST 历史不可靠, 必须 Phase 03 一开始就开 WS 累积. trade_hash UNIQUE 让 backfill 完全 idempotent. churn 出 candidate-set 时 trades 保留 (M4 backtest 历史诉求).
**Source:** 03-CONTEXT.md D-08 + 03-06-SUMMARY.md (Open Q 2 RESOLVED block + data_api_client.py)

### D-09 (cross-cutting): Phase 02.1 LEARNINGS 全套继承

继承 Phase 02.1 9D/8L/7P/5S, 重点 mapping: L1 fail-soft 双锚点 / L2 cross-bug 前置识别 / L4 StringIO sink / L5 Sentry EU region / L8 容器 localhost fallback / P1 双锚点 audit / P3 独立 middleware / P5 helper-first / P6 VALIDATION frontmatter ledger / D-09 verification ownership.

**Rationale:** Phase 02.1 是 m1-perception 第一次 sustained production chaos discipline, 8L+5S 已 codified. Phase 03 不重复发现.
**Source:** 03-CONTEXT.md D-09 + 02.1-LEARNINGS.md (Phase 02.1 closure)

### B1 (spawn constraint): `POLYARB_EVENT_BUS_ENABLED` 默认 FALSE

Plan-checker iter 2 spawn 的硬 invariant: L1 orchestrator step 7.7 必须默认 disabled, Fly secret 显式 opt-in 才发 NOTIFY. Phase 02 行为不变. 紧急回滚 = `flyctl secrets set POLYARB_EVENT_BUS_ENABLED=0 -a polyarb-l1 && flyctl machines restart -a polyarb-l1` 30s 内禁用 step 7.7 无需代码改动.

**Rationale:** L1 是 prod-running 关键路径, 任何新 pg_notify 调用都不许 default-on 污染 Phase 02 baseline. Inj L2-3a 在 prod 实证: `flyctl secrets list -a polyarb-l1 | grep -i event_bus` 返回 unset, L2 listener 保持 `listening` state 无饥饿 — B1 invariant 端到端 honored.
**Source:** 03-VALIDATION.md Inj L2-3a + 03-05-SUMMARY.md (event_bus_enabled DEFAULT FALSE)

### Hybrid catchup + bootstrap path (Wave 5 deploy 发现, 未入任何 PLAN.md)

L2 cold start 时 `catchup_from_cursor` 读 `l2_event_cursor` 表 → 缺表/空时返 `[]` 不抛错; 同时 `bootstrap_asset_ids` 直接 SELECT 几个 watchlist token 立刻 WS subscribe (避免 "等 L1 NOTIFY 才有 asset 可订" 的冷启动饥饿). 两路 union 写入 `_subscribed_assets`. Wave 5 deploy 实证: 84 missed snapshots replay + 3 bootstrap asset 立即上线 → l2_top_of_book 写入 3 rows baseline.

**Rationale:** Plan 05 设计的纯 LISTEN-driven 路径在 fresh DB / 长期 outage 后冷启动会饥饿 (cursor=0 + 无 L1 NOTIFY 来时); bootstrap 列表确保 daemon 一启动就有 WS frames 流入. Phase 03.1 应 codify 这是 canonical 反 starvation 模式.
**Source:** 03-SOAK-LOG.md Inj L2-3b substitute evidence + 03-08-SUMMARY.md (D-09 chapter on hybrid)

---

## Lessons

### L1: code passes unit tests ≠ alert chain wired in prod (Inj L2-2 的"meta-discovery")

Plan 03-06 truths 6-8 全 GREEN — `L2SupabaseMirror` 4 methods 存在, dual-anchor breadcrumb 存在, fail-soft 抛 envelope 工作. 但 Inj L2-2 撤 `POLYARB_SUPABASE_SERVICE_KEY` 实证: daemon 不死 ✅, mirror writes 静默失败 ✅, **但 `/health` 还是 200** ❌. 原因: `_build_l2_health_checks` 的 `mirror:l2_tob_age_seconds` 子检查门控在 `settings.l2_mirror_enabled` flag — 这个 flag **从来没在 config.py 里加** (Plan 03-06 漏)! 整条链 `mirror failure → /health 503 → operator alarm` 是 dead code.

**Context:** Phase 02.1 D-09 verification ownership discipline 的延伸 — 当时 codify "Claude 自己拉 Sentry API 不要让用户翻 UI". Phase 03 plan-checker iter 2 应该在 plan-time 就 trace 整条链是否真接通, 而不只是单元代码层. 修法 = GAP-1/2/3/5 进 Phase 03.1 backlog.
**Source:** 03-SOAK-LOG.md Inj L2-2 5-layer root cause + 03-07-SUMMARY.md Deviation 3

### L2: python-slim base image 没有 procps — chaos plan 必须先 verify primitives 存在

Plan 03-07 设计的 Inj L2-1 用 `flyctl ssh ... pkill -SIGTERM -f polyarb.daemon.l2_main`. L2 容器 `python:3.12-slim` base **完全没有 pkill / ps / kill / which** (`exec: "pkill": executable file not found in $PATH`). Substitute: `flyctl machine restart <id>` 同样测试 WS-disconnect-then-reconnect invariant 顺便测 P9 cold-start gate.

**Context:** 任何 chaos primitive 在 plan-time 没在目标镜像 verify 过都是空中楼阁. Phase 03.1 应实现 `POLYARB_WS_TEST_KILL=1` code flag (~10 lines in ws_market_client.py) 用代码触发 close 替代 OS-level kill.
**Source:** 03-SOAK-LOG.md Inj L2-1 deviation + 03-07-SUMMARY.md Deviation 1

### L3: `.env` 里的 `FLY_API_TOKEN` 会 shadow `flyctl auth` keychain — silent error 误导

Inj L2-2 cleanup `set -a; . ./.env; set +a; flyctl secrets set ...` 失败 `Could not find App "polyarb-l2"`. 根因: `.env` 旧 L1-only Fly API token (Phase 02 时期), `set -a` 加载到 shell env, **覆盖** keychain. 旧 token 对 L2 无权限 → 返回误导性 "App not found" 错误 (不是 "permission denied" 让人立刻怀疑 token 问题). Workaround: `FLY_API_TOKEN= flyctl secrets set ...` 一次性清掉 env 强制 keychain fallback.

**Context:** flyctl 优先读环境变量 > keychain; 项目 `.env` 全员加载 pattern 是 Phase 02 D-22 共享 secret 设计的副产物. GAP-4 进 Phase 03.1: chaos Makefile + `scripts/fly_secrets_sync.sh` 显式 `unset FLY_API_TOKEN` 在每个 flyctl call 前.
**Source:** 03-SOAK-LOG.md Inj L2-2 "Process discovery" + 03-07-SUMMARY.md Deviation 2

### L4: 实战 WS frame 是 dict OR list — research skeleton 假设 dict-only 不完整

Plan 03-04 `stream_market_events` 第一次 `make smoke-l2-ws` 真打 Polymarket WS 直接 `AttributeError: 'list' object has no attribute 'get'` at ws_market_client.py:95. Polymarket WS 经验上两种 shape: dict (单 event) / list (initial_dump 或 burst 批量 event). RESEARCH Focus 1 + Plan 04 PATTERNS File 9 都假设 dict-only.

**Context:** Fix 是在 `stream_market_events` normalize 两种 shape, 迭代 list 逐个 yield dict, 下游 mirror 不需要知道 batching. 教训: 任何外部协议的 schema 都要 real-traffic smoke 后才能 lock; research skeleton 是起点不是 ground truth.
**Source:** 03-04-SUMMARY.md "Rule 1 — Auto-fix bug: WS frame is dict OR list"

### L5: `websockets>=16` 推不上 — supabase transitive cap 强制 `>=15,<16`

Plan 03-04 PLAN lock `websockets>=16,<17` (RESEARCH Open Q 10 预期 Python 3.12 fix in 16.0). `uv add` resolver 失败: Phase 02 lock `supabase>=2.10,<3` 的 transitive `realtime` dep pin `websockets<16`. 实际 15.0.1 已支持 Plan 04 需要的全部 API (`async for ws in websockets.connect(...)`, `ping_interval`, `max_size`, Python 3.12 ok).

**Context:** Plan 上锁版本前必须 `uv add --dry-run` resolver test, 不能纸面信 RESEARCH. 修法: 把 truth gate 从 `'websockets[^a-z].*>=\s*16'` 放松为 `'websockets.*>='` (语义等价 "websockets pinned + importable").
**Source:** 03-04-SUMMARY.md "Rule 3 — Auto-fix blocking: websockets>=15,<16"

### L6: pre-commit SUMMARY-gate hook 强制 SUMMARY 必须 Task 2 时就 land (skeleton-first)

Phase 02.1 引入的 `.githooks/pre-commit` 拦截**任何** plan-scoped commit (feat/fix/test scope `(03-XX)`) 没有对应 SUMMARY.md. 后果: Task 2 的 RED test commit 也被拦. Pattern: Task 2 时就先 land SUMMARY.md skeleton (占位 hash + 待填字段), Task 末 commit 时 backfill. Plan 03-02 / 03-03 / 03-06 全部按此模式走.

**Context:** 这不是 bug 是 feature — SUMMARY 是 "可检索不失忆" 的锚点, gate 强制 hygiene. 但 Claude 需要在 plan execution 第一步就理解此约束, 否则 Task 2 卡死.
**Source:** 03-02-SUMMARY.md Deviation 2 + 03-03-SUMMARY.md Deviation 1 + 03-06-SUMMARY.md (skeleton commit 99ea2be)

### L7: `monkeypatch.setattr(time, 'monotonic', ...)` 会冻结 asyncio event loop 内部时钟

Plan 03-04 三个 watchdog test (`test_backoff_sequence`, `test_30s_timeout_triggers_RECONNECTING`, `test_reconnect_storm_cap`) 用 monkeypatch 冒充 elapsed time 时**无限挂起**. 根因: `asyncio` event loop 内部 `loop.time()` 默认 `time.monotonic` — 全局 patch 冻结 asyncio 所有 timer 包括测试的 outer timeout.

**Context:** 修法: 直接调被测函数 (`_on_stale()`) 不走 event loop; 或 hand-set `wd._state.last_event_time_s = time.monotonic() - 29.0` 让真实经过 29s 而不动 `time.monotonic`. 教训: 时间相关 unit test 不要 patch `time.monotonic` 全局.
**Source:** 03-04-SUMMARY.md "Rule 1 — Auto-fix bug: time.monotonic patching"

### L8: Open-Q resolution-before-RED — 探产 API 再设计测试

Plan 03-06 Open Q 2 = Polymarket Data API `/trades` 是否有 `before` / `asset` server-side filter? RESEARCH 未明示, 文档不全. 解法: Task 0 先 curl 实战探活 (`beforeTimestamp` / `before` / `maxTimestamp` 全 silently ignored, `asset=` 也被 ignored, `offset=4000` 直接 400 cliff), 然后基于实测设计 RED test. 否则 RED test 测错的假设, GREEN 时 debug 循环回炉.

**Context:** 任何 `[ASSUMED]` external API contract 在 Wave 0 设计 RED 之前必须探产. Pattern: probe-then-test, not test-then-debug-when-probe-was-needed.
**Source:** 03-06-SUMMARY.md "Open Q 2 — RESOLVED (Task 0)" + patterns-established "Open-Q resolution-before-RED"

### L9: dashboard service_role 字面量在 security comment 里也会被 grep gate 拦

Plan 03-08 `dashboard/lib/supabase/l2-queries.ts` 初稿 security comment 写 "NEVER imports or references `SUPABASE_SERVICE_ROLE_KEY`" — 意图是警告别用. truth gate `grep -ic 'SERVICE_ROLE\|service_role' == 0` 拦下来. literal grep 不区分 usage vs warning prose.

**Context:** 修法: 改写为 "NEVER imports or references the privileged daemon JWT" 同样语义无 service_role 字面量. 教训: plan-checker 设计 grep gate 时 `grep -cE 'createClient\(.*service_role|process\.env.*SERVICE_ROLE\b'` 这种"只 catch usage"比 `grep -ic` 文本扫描更稳.
**Source:** 03-08-SUMMARY.md Deviation 1

### L10: 84 missed snapshots replay 是 catchup_from_cursor 的 cold-start 副作用 (Wave 5 在 prod 实证)

Wave 5 deploy 之后第一次 L2 启动, log 突然显示 `event-bus catchup: replaying 84 missed snapshots` — 这是 catchup_from_cursor 从 cursor=0 起 SELECT `snapshots` 表所有 id 全部 dispatch. 后果: candidate refresh 在 60s debounce 内被 storm-collapse 成 1 次 (设计预期); cursor advance 到 snapshot_id=86; 实际 candidate set +0 -0 (因为没有 scanner recipe 配置 = 空 union, 只 bootstrap 列表起效).

**Context:** 这是 cold-start 行为, 不是 bug, 但 PLAN.md 没明写"L2 第一次启动会 replay 全部 L1 历史 snapshots". Phase 03.1 应在 listener.py + l2_main.py 加 startup log line 显式提示, 或在 catchup_from_cursor 加 `max_replay=N` cap 避免 cursor=0 全表扫.
**Source:** 03-SOAK-LOG.md Inj L2-3b substitute evidence

---

## Patterns

### P1: 双锚点 audit log (continued from Phase 02.1 P1, applied L2-wide)

任何 fail-soft skip / config-disabled 路径 emit 双锚点: `logger.info` (本地 loguru) + `sentry_sdk.add_breadcrumb` (远端可观测). 成功路径也 emit (`category='l2-mirror' level='info'`) 防 Phase 02.1 S1 "design-unreachable breadcrumb buffer evaporation".

**When-to-use:** 所有 L2 fail-soft 路径 — l2_supabase_mirror.push_*, events.bus.publish_snapshot_complete, l2_candidate_refresh.on_snapshot_complete, l2_main mirror init disabled.
**Impact:** 03-06-SUMMARY truth 7 `grep -cE 'category="l2-mirror"' src/polyarb/storage/l2_supabase_mirror.py` → 8 occurrences (4 methods × success+failure paths).
**Source:** 03-PATTERNS.md SP1+SP2 + 03-06-SUMMARY.md "Phase 02.2 preemptive fix applied"

### P2: Hybrid catchup + bootstrap (Wave 5 在 prod 发现, 不在 PLAN.md)

L2 cold-start 同时跑 (a) `catchup_from_cursor` 从 l2_event_cursor 表 (缺表/空时 `UndefinedTableError → []`, 不抛错), (b) `bootstrap_asset_ids` 直接 SELECT 几个 token 立即 WS subscribe — union 写入 `_subscribed_assets`. 避免 fresh DB / 长 outage 后冷启动饥饿.

**When-to-use:** 任何 LISTEN-driven 消费者第一次 deploy / 长期 outage 后启动. catchup 处理"中间漏 N 个 NOTIFY", bootstrap 处理"启动时没有任何上下文".
**Impact:** Wave 5 prod deploy 实证 84 missed snapshots replay + 3 bootstrap asset 立即 WS subscribe → l2_top_of_book 3 rows baseline. Inj L2-3a B1 invariant verified in prod 实际就靠这个 hybrid 让 L2 在 L1 OFF 状态仍 healthy.
**Source:** 03-SOAK-LOG.md Inj L2-3b substitute evidence + 03-08-SUMMARY.md 教学 chapter 10

### P3: Declarative chaos plan with invariant tests (Plan 03-07)

`tests/chaos/test_l2_chaos_plan.py` (354 lines) 是 declarative L2 chaos 数据结构 + 17 个 invariant test: 每个 Inj 必须有 `programmatic_cmds` 字段 (`test_every_injection_has_programmatic_verification`), 必须有 container-localhost fallback (`test_every_injection_has_container_fallback`). chaos plan 本身是被测的对象, 不只是文档.

**When-to-use:** 任何 phase 设计 chaos verification 时. 把 chaos plan codify 成 dict + 用 pytest assert 它的结构契约 = plan-checker iter 2 不会漏验证 chain-level truth.
**Impact:** 5/5 Inj 通过 invariant test → ship phase 时知道 chaos plan 自己 schema-correct. (注意: L1 Lesson L1 显示**仅有 invariant test 还不够**, 还需要在 plan-time 真 trace alert chain.)
**Source:** 03-07-SUMMARY.md "Plan/grep contract" + tests/chaos/test_l2_chaos_plan.py

### P4: tomllib structural assertion (Plan 03-02)

`tests/test_fly_l2_config.py` 用 `tomllib.load()` parse 然后 assert dict shape 而不是 `grep` raw text. 11 assertions: `app == "polyarb-l2"`, `processes.keys() == ['app']`, `mounts[0].destination == "/data"` etc. survives reformatting + 自文档化.

**When-to-use:** 任何配置文件 (TOML/JSON/YAML) 的结构化 assertion. 比 regex robust, 比 raw text 易读.
**Impact:** Plan 03-02 11/11 GREEN — survived 1 round of comment-reformatting without breaking. Plan 04 PLAN 引用此 pattern 验证 pyproject.toml 也有效.
**Source:** 03-02-SUMMARY.md "tomllib-as-spec" patterns-established

### P5: Helper-first refactor (continued from Phase 02.1 P5)

`_build_l2_health_checks(...)` (l2_health.py 259 lines) 同时 feed `/health` (IETF strict, 503 on fail) 和 `/healthz` (always 200 for Fly probe). HTTP status code 差异在 handler, schema 共享. 加新 sub-check (Plan 04 wires ws_consumer / Plan 05 wires event_listener / Plan 06 wires mirror) 只改 helper 不改 handlers.

**When-to-use:** 任何需要 multi-endpoint serving 相同 schema 但不同 HTTP 语义的场景.
**Impact:** Plan 04-06 三次扩展 helper 添加 sub-check 零 handler 改动, 零 endpoint schema drift. (L1 教训: Plan 03-06 加 `mirror:l2_tob_age_seconds` 子检查时 gate 在不存在的 `l2_mirror_enabled` flag — dead code, GAP-1 修.)
**Source:** 03-03-SUMMARY.md Deliverables `_build_l2_health_checks` + 03-PATTERNS.md SP3

### P6: Smoke-l2-* Makefile family (CLAUDE.md command-entry obligation)

每个 plan 引入新 ops surface 时 in同步加 Makefile target: `smoke-l2-health` / `smoke-l2-health-prod` (Plan 03) / `smoke-l2-ws` (Plan 04) / `smoke-event-bus` (Plan 05) / `smoke-l2-mirror` / `migrate-l2` / `backfill-trades` (Plan 06) / `chaos-l2-baseline` / `chaos-l2-inj{1,2,3a,3b}` / `chaos-l2-cleanup` (Plan 07) / `smoke-l2-dashboard` (Plan 08). 用户不需要记长命令.

**When-to-use:** 每个 plan 落 Makefile target 是 CLAUDE.md 硬约束. plan PLAN.md must_haves 必须显式列 target 名作产出.
**Impact:** Phase 03 一共加 ~20 Makefile targets, 全程一致 — `make help` 是单一入口.
**Source:** CLAUDE.md "命令入口约定" + 全部 8 SUMMARY 的 deliverables / Makefile mods

### P7: Vercel server-component + anon RLS dashboard (Plan 03-08, 复用 Phase 02 Wave 4)

每页 = Server Component (no `'use client'`) + `dynamic="force-dynamic"` + `revalidate=0` + `getServerSupabase()` (anon key + RLS) + try/catch → empty banner (NOT 500). 4 pages 复制改模板 `dashboard/app/movers/page.tsx` 30 分钟落地. Service role key 永远不进 dashboard bundle.

**When-to-use:** 任何 L2/L3 数据 surface 给用户. RLS anon SELECT + service_role 仅在 daemon 侧 = 干净写/读 trust boundary.
**Impact:** Plan 03-08 4 pages 加 163-line shared `l2-queries.ts` helper, TypeScript 零 error, 复用度 ≥80%.
**Source:** 03-08-SUMMARY.md "4 Vercel Dashboard Pages" + Surprise 2

### P8: Cross-bug pre-check at plan-phase (continued from Phase 02.1 L2/P8)

Plan-phase wave 排序前 audit 所有 plan pairs 的 cross-bug interaction. Phase 03 4 个已知: (1) watchdog reconnect storm + NOTIFY storm; (2) mirror write + WS event SQL pool contention; (3) GHA keepalive silent fail + Plan 06 still 工作 (false confidence); (4) L1+L2 撞 Postgres connection limit. 每个在 plan-time 锁 mitigation 不留 chaos-time.

**When-to-use:** plan-phase 末, wave 排序前. plan-checker iter 2 应专门 trace cross-plan invariants.
**Impact:** Phase 03 SP8 列了 4 个, Plan 05 实际 codify 60s debounce + R9 500 cap 缓解 (1) 和 (2). Inj L2-2 暴露的 L1 Lesson 显示 plan-checker 还需要进一步**沿 chain trace observability** 不只代码层.
**Source:** 03-PATTERNS.md SP8 + Phase 02.1 LEARNINGS L2

---

## Surprises

### S1: 84 missed snapshots replay 突然出现在 Wave 5 deploy log

Wave 5 deploy 后第一次 `flyctl logs -a polyarb-l2` 显示 `event-bus catchup: replaying 84 missed snapshots` — PLAN.md 完全没 mention 这种 cold-start 行为. 根因 catchup_from_cursor 从 cursor=0 起 SELECT `snapshots` 表所有 id, dispatch 进 60s debounce, storm-collapse 成 1 次 candidate refresh, cursor advance to 86.

**Impact:** 非 bug 但反映出 cold-start observability 缺失. Phase 03.1 应加 startup log "L2 cold-start replaying N historical snapshots" 让运维不慌. 同时考虑 catchup_from_cursor 加 `max_replay=N` cap 避免 cursor=0 全表扫.
**Source:** 03-SOAK-LOG.md Inj L2-3b substitute evidence

### S2: `FLY_API_TOKEN` 在 .env 里悄悄 shadow keychain — `flyctl secrets set` 失败错误信息误导

Inj L2-2 cleanup 把 `POLYARB_SUPABASE_SERVICE_KEY` 还回去时 `flyctl secrets set` 报 `Could not find App "polyarb-l2"`. 不是权限错误, 不是网络错误, 不是 typo — 而是 `.env` 里 Phase 02 时期老 L1-only token 通过 `set -a; . ./.env` 加载覆盖了 keychain credential. Workaround = `FLY_API_TOKEN= flyctl ...`.

**Impact:** 真陷阱: 错误信息 "App not found" 让人怀疑 typo / Fly 状态, 实际是身份失误. GAP-4 进 Phase 03.1.
**Source:** 03-SOAK-LOG.md Inj L2-2 "Process discovery" + 03-07-SUMMARY.md Deviation 2

### S3: `/health` 返 200 而不是预期 503 — `l2_mirror_enabled` config field 从未存在

Inj L2-2 设计预期: 撤 SUPABASE_SERVICE_KEY → mirror writes fail → `/health` HTTP 503 (告警). 实际: `/health = 200` 只有 ws + event_bus 子检查, mirror 子检查根本没出现. l2_health.py:169-180 的 `mirror:l2_tob_age_seconds` 子检查 gate 在 `settings.l2_mirror_enabled` — Plan 03-06 **从来没在 config.py 加这个 field**. 整条 alert chain 是 dead code.

**Impact:** Phase 03 closure 的最大教训 (L1 Lesson). 单元测试 GREEN ≠ 生产 alert chain 通. GAP-1/2/3 进 Phase 03.1 P0.
**Source:** 03-SOAK-LOG.md Inj L2-2 root cause analysis layer 3

### S4: Polymarket Data API `/trades` 对 `before` / `asset` / `eventSlug` 三个查询参数全部"silently ignored"

Plan 03-06 Open Q 2 探产: `?beforeTimestamp=X` / `?before=X` / `?maxTimestamp=X` / `?endTimestamp=X` 全返 latest 不过滤; `?asset=<token_id>` 返不同 asset; `?eventSlug=<slug>` 也忽略. 但 `?user=<wallet>` 和 `?takerOnly=true` **真过滤**. 文档完全没写哪些参数生效.

**Impact:** Backfill 必须 paginate global feed + client-side filter by asset_id + stop at first `timestamp < cutoff`. 重度交易 asset 覆盖好, 稀交易 asset best-effort. M3+ 需要时考虑 Polymarket subgraph 历史数据补强.
**Source:** 03-06-SUMMARY.md "Open Q 2 — RESOLVED (Task 0)" probe table

### S5: WS frame 是 `dict` OR `list` — `initial_dump=True` 返回 list-shape baseline

Plan 03-04 设计基于 RESEARCH dict-only 假设. Real-WS smoke 第一帧 `book` (initial_dump baseline) 是 list-shape `[{...}, {...}]`, 直接 `AttributeError`. Polymarket WS 经验上两种 shape 都有: 单 event = dict, batched (initial_dump / burst) = list.

**Impact:** Plan 03-04 Rule 1 auto-fix: stream_market_events normalize 两种 shape, 迭代 list 逐个 yield. 修法 8 tests 仍 GREEN 因为 dict 是 normalization 的一支.
**Source:** 03-04-SUMMARY.md "Rule 1 — Auto-fix bug: WS frame is dict OR list"

### S6: `monkeypatch.setattr(time, "monotonic", ...)` 把 asyncio event loop 冻结导致 3 个 test 无限挂

Plan 03-04 watchdog test 用 monkeypatch 模拟 elapsed time, 三个 test 永挂. 根因 asyncio loop `loop.time()` 默认 `time.monotonic`, 全局 patch 冻结 asyncio 所有 timer (包括测试自己的 outer timeout).

**Impact:** 测时间敏感逻辑要么直接调被测函数 (`_on_stale()` 不走 event loop), 要么 hand-set state 让真实经过, 不能 patch `time.monotonic` 全局.
**Source:** 03-04-SUMMARY.md "Rule 1 — Auto-fix bug: time.monotonic patching breaks asyncio internals"

### S7: bid=None on WC2026 high-skew markets (smoke-l2-ws 实证)

Plan 03-04 smoke 跑 Iraq 2026 World Cup YES token (~$9.86M liquidity), 30s 内 3 frames: 1 book + 2 price_change. 但 `book` initial_dump frame 偶现 `bid: None` (asks 全空) — 高 skew 市场 (Iraq 不可能进 WC) 一边订单簿可能完全空. row builder 必须 nullable 处理 (`depth_yes_usd` / `depth_no_usd` nullable in l2_top_of_book schema).

**Impact:** Schema 设计 nullable 是预防性的 (not a bug); 但 m4-smart-strategies signal 计算时必须显式处理 None — 不能假设 bid/ask 都存在. M4 plan 应入 cross-bug check.
**Source:** 03-04-SUMMARY.md real-WS smoke evidence + 03-06-SUMMARY.md "depth_yes_usd / depth_no_usd nullable" Known Stubs

---

*Generated 2026-05-25 from 8 SUMMARY + CONTEXT + RESEARCH + PATTERNS + VALIDATION + SOAK-LOG. Phase 03 closed with 5-GAP carry-over to Phase 03.1.*
