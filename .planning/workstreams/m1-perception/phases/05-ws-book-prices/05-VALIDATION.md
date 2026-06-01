---
phase: 05
slug: ws-book-prices
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-06-01
revision: 1
body_populated: 2026-06-01
---

# Phase 05 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution. Body populated in revision 1 per user decision 3.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | `pytest 8.x` (verified `pytest>=8.2,<9` in `pyproject.toml`) + `pytest-asyncio 0.23.x` + dashboard `pnpm typecheck` / `pnpm build` (Next.js 15 + React 19) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` + `dashboard/package.json` scripts |
| **Quick run command** | `uv run pytest tests/m1-perception/ -x -k "phase_05 or l3 or book_levels or dynamic_subscribe or alembic_005 or candidate_refresh_l3"` |
| **Full suite command** | `uv run pytest tests/m1-perception/ tests/observation/test_l2_candidate_refresh.py tests/observation/test_l2_candidate_refresh_coldstart.py -x && (cd dashboard && pnpm typecheck && pnpm build)` |
| **Estimated runtime** | ~90-120 seconds (Python suite ~45-60s; dashboard build ~30-50s; estimate based on Phase 03/04 similar surface) |

---

## Sampling Rate

- **After every task commit:** Run the per-task `<automated>` command (table below)
- **After every plan wave:** Run the full suite command
- **Before `/gsd-verify-work`:** Full suite must be green + Plan 06 Task 2 24h soak verdict captured
- **Max feedback latency:** 60 seconds (Python sub-suites complete <60s; dashboard pnpm typecheck <30s)

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 05-01-01 | 01 | 1 | PHASE05-R02, PHASE05-R03 | T-05-01-04 | Migration 005 file lint guards (date_trunc-only, no time_bucket) | lint | `uv run pytest tests/m1-perception/test_alembic_005_ohlc_views.py -x 2>&1 \| grep -E "(FAIL\|ERROR\|FileNotFoundError)" \| head -5` (RED expected) | ❌ W0 | ⬜ pending |
| 05-01-02 | 01 | 1 | PHASE05-R02, PHASE05-R03 | T-05-01-01..05 | Alembic 005 schema lands; date_trunc OHLC views; RLS + BRIN | unit | `uv run pytest tests/m1-perception/test_alembic_005_ohlc_views.py -x` | ✅ (created Wave 0) | ⬜ pending |
| 05-01-03 | 01 | 1 | PHASE05-R02 | — | Makefile target lint | grep | `grep -c "^supabase-migrate-test:" Makefile` returns `1` | ✅ | ⬜ pending |
| 05-02-01 | 02 | 1 | PHASE05-R04, PHASE05-R06 | T-05-02-01..05 | RED tests for add/remove_subscriptions + L3 race protection | unit | `uv run pytest tests/m1-perception/test_ws_consumer_dynamic_subscribe.py tests/m1-perception/test_candidate_refresh_l3_protection.py 2>&1 \| tail -20` (RED expected) | ❌ W0 | ⬜ pending |
| 05-02-02 | 02 | 1 | PHASE05-R04, PHASE05-R06 | T-05-02-01..05 | WsConsumer split sets + add/remove_subscriptions API + GAP-401 preserved + 10-token Yes+No support | unit | `uv run pytest tests/m1-perception/test_ws_consumer_dynamic_subscribe.py tests/m1-perception/test_ws_watchdog_liveness.py -x` | ✅ (created Wave 0) | ⬜ pending |
| 05-02-03 | 02 | 1 | PHASE05-R06 | T-05-02-01 | candidate_refresh uses update_candidate_set (Pitfall 5 fixed; existing observation tests stay green) | unit | `uv run pytest tests/m1-perception/test_candidate_refresh_l3_protection.py tests/observation/test_l2_candidate_refresh.py tests/observation/test_l2_candidate_refresh_coldstart.py tests/m1-perception/test_ws_watchdog_liveness.py -x` | ✅ | ⬜ pending |
| 05-03-01 | 03 | 2 | PHASE05-R02 | T-05-03-01..06 | RED tests for projector + mirror + chain-truth anchor | unit | `uv run pytest tests/m1-perception/test_l2_main_book_levels.py tests/m1-perception/test_l2_supabase_mirror_book_levels.py 2>&1 \| tail -20` (RED expected) | ❌ W0 | ⬜ pending |
| 05-03-02 | 03 | 2 | PHASE05-R02 | T-05-03-01..06 | _book_levels_rows_from_frame projector + mirror.push_book_levels + l3_promote scaffold (state + 4 getters + is_book_levels_write_overdue predicate + stub promote_run/run_periodic) | unit | `uv run pytest tests/m1-perception/test_l2_main_book_levels.py tests/m1-perception/test_l2_supabase_mirror_book_levels.py -x` | ✅ | ⬜ pending |
| 05-03-03 | 03 | 2 | PHASE05-R02 | T-05-03-06 | l2_main dispatcher gates book-event depth write on _l3_active_set membership | unit | `uv run pytest tests/m1-perception/test_l2_main_book_levels.py -x` | ✅ | ⬜ pending |
| 05-04-01 | 04 | 3 | PHASE05-R01, PHASE05-R07 | T-05-04-01..10 | RED tests for promote_run (incl. Blocker #2 epoch-ms ts predicate test + Blocker #1 l3_promoted_at_ts mirror + Yes/No double-token expansion) | unit | `uv run pytest tests/m1-perception/test_l3_promoter.py 2>&1 \| tail -30` (RED expected) | ❌ W0 | ⬜ pending |
| 05-04-02 | 04 | 3 | PHASE05-R01, PHASE05-R07 | T-05-04-01..10 | promote_run + run_periodic + Yes/No token expansion + l2_candidates write-through (fail-soft) + Plan 03 scaffold preserved | unit | `uv run pytest tests/m1-perception/test_l3_promoter.py -x` | ✅ | ⬜ pending |
| 05-04-03 | 04 | 3 | PHASE05-R01, PHASE05-R07 | T-05-04-06 | promoter wired into l2_main + 3 chain-truth /health sub-checks (strict 10-token expected count) | unit | `uv run pytest tests/m1-perception/test_l2_health_l3_subchecks.py tests/m1-perception/test_l3_promoter.py tests/m1-perception/test_ws_watchdog_liveness.py -x` | ✅ | ⬜ pending |
| 05-05-01 | 05 | 4 | PHASE05-R02, PHASE05-R03 | T-05-05-01 | Prod Supabase Alembic 005 push (human-action; evidence: alembic current + view smokes + Wave 0 re-run output) | checkpoint:human-action | (manual) developer pastes alembic current + step-4 smokes + `uv run pytest tests/m1-perception/test_alembic_005_ohlc_views.py -x 2>&1 \| tail -20` | n/a (manual) | ⬜ pending (human-action, evidence-bearing per Warning #14) |
| 05-05-02 | 05 | 4 | PHASE05-R05 | T-05-05-02..04 | Dashboard L3 query helpers + lightweight-charts dep + types | typecheck | `cd dashboard && pnpm typecheck 2>&1 \| tail -10 && grep -c "getOhlcForAsset\|getBookLevelsLatest" lib/supabase/l2-queries.ts` | ✅ | ⬜ pending |
| 05-05-03 | 05 | 4 | PHASE05-R05 | T-05-05-02..04 | /l3/[asset_id] page builds (SSR + RSC) + KlineChart dynamic import + candidates L3 badge | typecheck + build | `cd dashboard && pnpm typecheck 2>&1 \| tail -5 && pnpm build 2>&1 \| tail -10` | ✅ | ⬜ pending |
| 05-05-04 | 05 | 4 | PHASE05-R05 | — | 3 new Makefile L3 ops targets exist | grep | `grep -c "^l3-promote-dry-run:\|^ohlc-spot-check:\|^smoke-l3-dashboard:" Makefile` returns `3` | ✅ | ⬜ pending |
| 05-06-01 | 06 | 5 | PHASE05-R01..R08 | T-05-06-01..07 | polyarb-l2 prod deploy + /health 3 L3 sub-checks present + first promote tick fires + GAP-401 no false-trip | checkpoint:human-action | (manual) developer pastes deploy SHA + /health JSON + l3-promote log line + watchdog stale count | n/a (manual) | ⬜ pending |
| 05-06-02 | 06 | 5 | PHASE05-R01..R08 | T-05-06-02..07 | 24h prod soak + 3-sub-indicator strict N=5 verdict (Blocker #5) | checkpoint:human-verify | (manual) developer pastes T+24h verdict table + GAP-401 watchdog stale 24h count | n/a (manual) | ⬜ pending |
| 05-06-03 | 06 | 5 | — | — | docs/learning/11-L3-K线.md exists + INDEX entry | structural | `test -f docs/learning/11-L3-K线.md && grep -c "^## " docs/learning/11-L3-K线.md && grep -c "11-L3" docs/learning/00-INDEX.md` | ✅ | ⬜ pending |
| 05-06-04 | 06 | 5 | — | — | VALIDATION nyquist_compliant: true + STATE + ROADMAP closed Phase 05 | structural | `make planning-status 2>&1 \| tail -5 ; grep -c "nyquist_compliant: true" .planning/workstreams/m1-perception/phases/05-ws-book-prices/05-VALIDATION.md ; grep -c "Phase 05" .planning/workstreams/m1-perception/STATE.md` | ✅ | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

*W0 = test file created in Wave 0 of that plan (RED state intentional)*

---

## Wave 0 Requirements

Test files created by Wave 0 of each plan (RED-first per TDD discipline):

- [ ] `tests/m1-perception/test_alembic_005_ohlc_views.py` — Plan 01 Wave 0; 6 file-content lint tests for migration 005 (date_trunc-only / DDL shape / GRANT / l3_promoted_at_ts column / downgrade order)
- [ ] `tests/m1-perception/test_ws_consumer_dynamic_subscribe.py` — Plan 02 Wave 0; 9 tests for add/remove_subscriptions API (incl. revision-1 test #9 Yes+No 10-token payload)
- [ ] `tests/m1-perception/test_candidate_refresh_l3_protection.py` — Plan 02 Wave 0; 2 tests for Pitfall 5 race protection (uses fixture pattern from `tests/observation/test_l2_candidate_refresh.py`)
- [ ] `tests/m1-perception/test_l2_main_book_levels.py` — Plan 03 Wave 0; 7+2 tests for projector + dispatcher branch (Task 1 7 projector tests + Task 3 2 dispatcher gate tests)
- [ ] `tests/m1-perception/test_l2_supabase_mirror_book_levels.py` — Plan 03 Wave 0; 7 tests for mirror.push_book_levels (incl. chain-truth anchor mutation + fail-soft envelope)
- [ ] `tests/m1-perception/test_l3_promoter.py` — Plan 04 Wave 0; 12 tests for promote_run (revision 1 added: timestamp-predicate test for Blocker #2, write-through test for Blocker #1, Yes/No expansion test for Warning #13)
- [ ] `tests/m1-perception/test_l2_health_l3_subchecks.py` — Plan 04 Wave 0; 9 tests for /health L3 sub-checks (revision 1 added: under-filled-9-tokens warn test for Blocker #5 strict 10)

Existing test files leveraged (NOT created by Wave 0; relied on as regression anchors):

- `tests/m1-perception/test_ws_watchdog_liveness.py` (Phase 04.1 SESSION 33 quick task — GAP-401 regression suite; MUST stay green; renamed from `_gate.py` in revision 1 Blocker #3 fix)
- `tests/observation/test_l2_candidate_refresh.py` (canonical candidate-refresh regression; Plan 02 Task 3 runs it to confirm no regression from Pitfall 5 fix)
- `tests/observation/test_l2_candidate_refresh_coldstart.py` (cold-start debounce trap regression)

No framework install needed — pytest 8.x + pytest-asyncio 0.23 already in pyproject.toml dev extras (Phase 03 added).

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Alembic 005 applied to PROD Supabase | PHASE05-R02, PHASE05-R03 | Schema push to prod requires human authorization (per Phase 02 ops discipline); Claude cannot `make supabase-migrate` against prod DSN | Plan 05 Task 1: developer runs `make supabase-migrate` against prod DSN; pastes `alembic current` + step-4 smokes + Wave 0 re-run output (Warning #14 evidence-based) |
| polyarb-l2 prod deploy of Phase 05 image | PHASE05-R01..R08 | flyctl deploy to prod requires human authorization; deploy of un-deployed 04.1 + GAP-401 carry-over also rides this image | Plan 06 Task 1: developer runs `env -u FLY_API_TOKEN flyctl deploy --config fly-l2.toml --remote-only`; pastes deploy SHA + /health JSON + l3-promote log + GAP-401 stale count |
| 24h prod soak verdict (D-12 strict N=5) | PHASE05-R01..R08 | 24h wall-clock window; developer samples /health and SQL every ~6h then renders verdict at T+24h | Plan 06 Task 2: developer appends T+0/T+6h/T+12h/T+18h/T+24h readings to `05-SOAK-LOG.md`; renders 3-sub-indicator table (Blocker #5 strict — ALL 3 must == 5 for GREEN) |

These checkpoints have automated antecedents (Wave 0 lint tests + per-task `<automated>` blocks); the manual gate ONLY confirms the prod outcome.

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies (will be ✅ after Plan 06 Task 4 lands)
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify (verified — every plan has automated commands; checkpoint tasks have manual evidence-bearing resume signals)
- [ ] Wave 0 covers all MISSING references (verified — 7 test files listed above, all RED-first per TDD)
- [ ] No watch-mode flags (verified — all `<automated>` blocks use `-x` for fail-fast, no watch)
- [ ] Feedback latency < 60s (verified — Python sub-suites complete <60s per Phase 03/04 baseline)
- [ ] `nyquist_compliant: true` set in frontmatter (will flip in Plan 06 Task 4 after 24h soak verdict captured)

**Approval:** pending (final flip happens in Plan 06 Task 4 — see `signed_at` field set then)
