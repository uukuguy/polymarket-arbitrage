---
workstream: m1-perception
created: 2026-04-28
---

# Project State

## Current Position
**Status:** Phase 1 Verified (mocked-pipeline gate green)
**Current Phase:** Phase 1 — 完整市场快照工具 ✅ COMPLETE
**Last Activity:** 2026-04-29
**Last Activity Description:** Phase 1 live-run #001 verified (post 2 surgical fixes for dedupe + subset-persist); 97/97 tests; 17,259 markets in SQLite + Parquet; major finding: 72% liquid markets affected by Issue #180 ghost_book

## Progress
**Phases Complete:** 1
**Total commits:** 36 (1 baseline + 35 phase-1 work)
**Phase 1 task count:** 32 tasks across 5 plans, 4 waves
**Test count:** 95 (skeleton 5 / gamma 6 / clob 5 / sqlite 10 / parquet 7 / validator 18 / normalizer 13 / orchestrator 13 / settings 10 / makefile 8)

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
**Stopped At:** Phase 1 + observability/cache hardening complete (4 unpushed commits)
**Last Activity:** 2026-04-30 00:20 CST — SESSION 08 完成 snapshot 工具可观测性 + cache 续传 + 教学文档落地

**SESSION 08 deliverables (4 unpushed commits)**:
1. `8bbdc47 docs(learning):` — Phase 1 教学文档 6 章 (`docs/learning/`) + CLAUDE.md "教学文档持续产出" 纪律
2. `ccedb5a feat(01):` — `ChunkCache` class + `make snapshot-status` + 5 个新 make target
3. `50c4299 fix(01):` — Gamma 翻页进度 + Phase N/7 banner + macOS `ps -o etime` 兼容
4. `63797ad feat(01):` — 时间戳 prefix + 每 phase elapsed timing (`► Phase X — done in Ys`)
- Tests: 119/119 green (97 → 119, +22 new tests across cache / progress / phase timing)
- Live data points: LIVE-RUN-001/002 入账（17259 / 17377 markets, ~26 分钟 each）

**Recommended Next Action** (下次会话首选项)：

A. **跑 LIVE-RUN-005 验证新可观测性 + push 4 commit**
   - `make snapshot-markets-v 2>&1 | tee /tmp/snap-$(date +%H%M).log`
   - 看新格式（时间戳 / phase elapsed / Gamma 进度 / chunk cache cleanup）
   - 跑完 push 4 commit 到 origin/main

B. **直接 push + 跑 Phase 2 research**
   - 工程改动已 119 tests 验证；live 验证可以推迟
   - `git push origin main` → `/gsd-research-phase 2 --ws m1-perception`

C. **看一下 LIVE-RUN-002 暴露的 220 个无 endDate market**
   - 数据已在 SQLite, layer 2 UNKNOWN，CLAUDE.md D-D4 红线
   - 写 SQL + 简短分析（不开 phase）

**推荐 A**：四个 commit 都是 user-facing 工具行为改动，需要 live 验证一次再 push 才踏实。

**Carry-over open items**:
- 220 个市场无 endDate（Layer 2 UNKNOWN）— 需要分类调查（是 perpetual market？）
- clob_missing 在 4 小时内 +33%（CLOB 可达性漂移）— 需要时序观察
- Polymarket Gamma API 在 CST 22-24 时段明显慢（page 速度从 1.7/s 降到 0.3/s）— 暂未触发任何代码改动，但记录为环境事实

## Phase 1 Artifacts
- `.planning/workstreams/m1-perception/phases/01-/01-CONTEXT.md` — locked decisions
- `.planning/workstreams/m1-perception/phases/01-/01-RESEARCH.md` — 970 lines tech research
- `.planning/workstreams/m1-perception/phases/01-/01-PATTERNS.md` — 32 file analogs
- `.planning/workstreams/m1-perception/phases/01-/01-SECURITY-REVIEW.md` — 1 HIGH + 3 MED + 3 LOW (resolved)
- `.planning/workstreams/m1-perception/phases/01-/01-{1..5}-PLAN.md` — 5 executable plans
- `.planning/workstreams/m1-perception/phases/01-/01-{1..5}-SUMMARY.md` — per-plan executor output
