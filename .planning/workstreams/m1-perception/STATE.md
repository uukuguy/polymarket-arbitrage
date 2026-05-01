---
workstream: m1-perception
created: 2026-04-28
---

# Project State

## Current Position
**Status:** Phase 1 Verified ✅ / Phase 1.5 假设证伪并 reverted — Phase 2 (WebSocket) 待启动
**Current Phase:** Phase 1 ✅ COMPLETE / Phase 1.5 ❌ DEAD（filterDate 假设不成立）
**Last Activity:** 2026-05-01 EOD
**Last Activity Description:** SESSION 11 cleanup + Phase 1.5 live test 灭杀 — Phase 目录改名、commit + revert Phase 1.5 scaffolding。Gamma `/markets` 不支持 server-side incremental filter（详见 threads/data-quality.md）

## Progress
**Phases Complete:** 1
**Phase 1.5 status:** ❌ REVERTED — `filterDate` API 参数不存在，`updatedAt` 是 server batch 时间戳无业务语义。增量方向重定为 WebSocket
**Test count:** 119 m1 tests green (Phase 1 完整套件)

## Phase 1 Deliverables (verified)
- ✅ `make snapshot-markets` (subset, default) → `python -m polyarb.snapshot`
- ✅ `make snapshot-markets-full` (--full flag) → `python -m polyarb.snapshot --full`
- ✅ `src/polyarb/{clients,storage,snapshot,validator}/` — 5 sub-packages
- ✅ Atomic SQLite (BEGIN IMMEDIATE + DELETE + executemany INSERT) + WAL mode
- ✅ Atomic Parquet writes (`data/snapshots/YYYY/MM/DD/HH-MM-SS.parquet` + tmp + os.replace)
- ✅ Validator Layer 1 (count) + Layer 2 (fields) + Layer 4 (cross-source incl. ghost-book detection per Issue #180)
- ✅ Per-row `fetched_at_ms` (best-effort consistency, NOT transactional)
- ✅ Security invariants: F-1 _safe_float, F-2 follow_redirects=False + MAX_PAGES=1000, F-3 path validator, F-4 fixture sanitization, F-5 truncation caps
- ⏸️ Live API smoke test: untested (manual gate — user runs `make snapshot-markets` against real Polymarket when ready)

## Open Items (carried to subsequent phases)
- **Live API verification** — paper run untested; user's manual step
- **F-7 lockfile** — deferred to m5-industrialize or any phase introducing wallet/auth (per SECURITY-REVIEW.md)
- **`fetched_at_ms` semantic gap** — currently stamped on ALL normalized markets including subset-filtered-out ones. Phase 2 (WebSocket increment) should clean up.
- **Top-of-book single-side** — only `yes_token_id` populated. Phase 3 strategies can't assume symmetric YES/NO; explicitly fetch NO when needed.
- **`record_fixtures.py`** at project root is a working artifact (not committed, not gitignored). User can delete or commit as a tool.

## Session Continuity
**Stopped At:** Phase 1 complete — LIVE-RUN-005 verified, 4 commits on origin
**Last Activity:** 2026-05-01 10:04 CST — LIVE-RUN-005 verified (6m12s, 20353 markets, 72% ghost_book), observability all confirmed, 4 commits on origin/main

**SESSION 08 deliverables (4 unpushed commits)**:
1. `8bbdc47 docs(learning):` — Phase 1 教学文档 6 章 (`docs/learning/`) + CLAUDE.md "教学文档持续产出" 纪律
2. `ccedb5a feat(01):` — `ChunkCache` class + `make snapshot-status` + 5 个新 make target
3. `50c4299 fix(01):` — Gamma 翻页进度 + Phase N/7 banner + macOS `ps -o etime` 兼容
4. `63797ad feat(01):` — 时间戳 prefix + 每 phase elapsed timing (`► Phase X — done in Ys`)
- Tests: 119/119 green (97 → 119, +22 new tests across cache / progress / phase timing)

**SESSION 09 results** (2026-05-01):
- LIVE-RUN-005: 20353 markets, 32916 issues, 72% ghost_book stable
- 6m12s total (vs 26m25s RUN-001) — API idle period effect
- All observability features confirmed: timestamp/phase-elapsed/progress/cache cleanup
- 4 commits already on origin/main (previous push succeeded)
- Snapshot ID 3 in SQLite

**Recommended Next Action** (下次会话首选项)：

A. **启动 m1 Phase 2 — WebSocket 增量数据流**（推荐 — Phase 1.5 死路证伪后的下一站）
   - `/gsd-discuss-phase 2 --ws m1-perception`
   - 焦点：
     - WebSocket 长连接维持 + 自动重连
     - `/book` channel: BBO 变化推送（替代 REST 轮询）
     - `/prices` channel: market mid-price 推送
     - State diff: WS 增量与 REST baseline 的合并策略
   - 这是 Polymarket 官方推荐的实时方案，py-clob-client 已有 WS 客户端

B. **切到 m2-combinatorial 推 Phase 2 T2-T8**
   - `gsd-tools workstream set m2-combinatorial`
   - T1 已 commit 落地，T2 Slippage Model 缺 PolymarketDepthCurve（Phase 1 OrderBookSummary 数据已可用）

**推荐 A**：m1 polling 已是完整 baseline，WebSocket 是清晰的下一步增量来源

**Carry-over open items**:
- 220 个市场无 endDate（Layer 2 UNKNOWN）— 需要分类调查（是 perpetual market？）
- clob_missing 在 4 小时内 +33%（CLOB 可达性漂移）— 需要时序观察
- Polymarket Gamma API 在 CST 22-24 时段明显慢（page 速度从 1.7/s 降到 0.3/s）— 暂未触发任何代码改动，但记录为环境事实
- **新观察**: Gamma 翻页速度在北京时间 10 点（美东 22 点）极快（6m12s 总耗时），但在 22-24 点（美东 10-12 点）慢（26m25s）— 推测 API 有北美白天高峰

## Phase 1 Artifacts
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-CONTEXT.md` — locked decisions
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-RESEARCH.md` — 970 lines tech research
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-PATTERNS.md` — 32 file analogs
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-SECURITY-REVIEW.md` — 1 HIGH + 3 MED + 3 LOW (resolved)
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-PLAN.md` — 5 executable plans
- `.planning/workstreams/m1-perception/phases/01-market-snapshot/01-{1..5}-SUMMARY.md` — per-plan executor output
