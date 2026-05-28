---
phase: 04
slug: candidate-set-l2-throughput
status: draft
nyquist_compliant: true
wave_0_complete: false
created: 2026-05-28
---

# Phase 04 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Populated by planner from 04-RESEARCH.md § Validation Architecture.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 7.x + pytest-asyncio (asyncio_mode=auto; pyproject testpaths=["tests"], package import mode — every subdir needs `__init__.py`) |
| **Config file** | pyproject.toml `[tool.pytest.ini_options]` (requires `uv sync --extra dev` for pytest-asyncio + psutil per 03.1 D-DEFER-1/2 / L11) |
| **Quick run command** | `uv run pytest tests/<subdir>/<changed>.py -x` |
| **Full suite command** | `uv run pytest tests/ -q` |
| **Estimated runtime** | unit tests < 30s per file; full suite ~minutes; Plan 04 Task 3 prod chaos ~7 min wall-clock (manual) |

---

## Sampling Rate

- **After every task commit:** Run quick command on changed test file (< 30s)
- **After every plan wave:** Run full m1-perception + observation + http + alembic + daemon suites
- **Before verify:** Full suite must be green
- **Max feedback latency:** < 30s per unit task; prod chaos (Plan 04 Task 3) is the only manual-latency item

---

## Per-Task Verification Map

| Task ID | Plan | Wave | Requirement | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|---------|------|------|-------------|------------|-----------------|-----------|-------------------|-------------|--------|
| 04-01-T1 | 01 | 1 | D-07 migration add-only | T-04-03 | service_role DDL access (Pitfall 3) | unit (static) | `uv run pytest tests/alembic/test_004.py -x -k "not slow"` | ❌ Wave 0 | ⬜ pending |
| 04-01-T2 | 01 | 1 | D-07 narrow projection + [BLOCKING] push | T-04-01 | DSN/key not logged; live column verified via information_schema | unit + live push | `uv run pytest tests/storage/test_supabase_mirror.py -x` (or tests/m1-perception/) + `make supabase-migrate` + information_schema query | ❌ Wave 0 / live | ⬜ pending |
| 04-03-T1 | 03 | 1 | D-08 GAP-200 three-branch | T-04-04/05/06 | config-state output reveals no secret material | unit | `uv run pytest tests/http/test_l2_health_gap200.py tests/m1-perception/test_l2_health_mirror_check.py -x` | ❌ Wave 0 | ⬜ pending |
| 04-02-T1 | 02 | 2 | D-02 temp-DB adapter + fail-loud + NULL/sentinel-fill | T-04-07 | parameterized INSERT only; no row-value f-string | unit | `uv run pytest tests/observation/test_l2_temp_db.py -x` | ❌ Wave 0 | ⬜ pending |
| 04-02-T2 | 02 | 2 | D-01 pagination + fetch + fail-soft; D-03 cap; D-04 fallback | T-04-08/10 | key via get_secret_value, never logged; bounded fetch | unit | `uv run pytest tests/observation/test_l2_candidate_refresh.py -x` | ⚠ exists (modify) | ⬜ pending |
| 04-02-T3 | 02 | 2 | D-01 fail-soft chain-truth surface | T-04-11 | sub-check reads real-mutated field (not dead-code gate, §1.6) | unit | `uv run pytest tests/http/test_l2_candidates_fetch_health.py -x` | ❌ Wave 0 | ⬜ pending |
| 04-04-T1 | 04 | 3 | D-06 indicator-1 dropped frames | — | counter only; no new surface | unit | `uv run pytest tests/daemon/test_ws_consumer_dropped_frames.py -x` | ❌ Wave 0 | ⬜ pending |
| 04-04-T2 | 04 | 3 | D-05/D-06 chaos orchestrator | T-04-12/13 | FLY_API_TOKEN= prefix; image-aware procfs RSS | dry-run | `make -n chaos-l2-inj4-throughput` (parses) + grep FLY_API_TOKEN= discipline | ❌ Wave 0 | ⬜ pending |
| 04-04-T3 | 04 | 3 | D-05/D-06 prod throughput verdict | T-04-14/15 | deployed image == latest main; false-trip recorded not hidden | manual (prod chaos) | `make chaos-l2-inj4-throughput` (human-verify, ~7min) | ❌ manual | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

- [ ] `uv sync --extra dev` — ensure pytest-asyncio + psutil present (03.1 D-DEFER-1/2 / L11 gap)
- [ ] `tests/http/__init__.py` — NEW package dir (pyproject package import mode; created by Plan 03 Task 1 / Plan 02 Task 3)
- [ ] `tests/alembic/test_004.py` — D-07 migration static + live-DB checks (copy test_003.py)
- [ ] `tests/observation/test_l2_temp_db.py` — D-01 pagination (placed in T2 file), D-02 adapter schema / NULL-fill / sentinel-fill / FK-handling / fail-loud / ghost-suspicious
- [ ] `tests/http/test_l2_health_gap200.py` — D-08 three-branch
- [ ] `tests/http/test_l2_candidates_fetch_health.py` — D-01 fail-soft /health surface
- [ ] `tests/daemon/test_ws_consumer_dropped_frames.py` — D-06 indicator-1
- [ ] Makefile `chaos-l2-inj4-throughput` target — D-05/D-06 prod chaos with frame_count + RSS + FLY_API_TOKEN= discipline
- [ ] Modify `tests/storage/test_supabase_mirror.py` OR `tests/m1-perception/test_supabase_mirror.py` — D-07 narrow_market_row includes yes_token_id

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| D-07 alembic push to live Supabase | D-07 [BLOCKING] | Requires live POLYARB_SUPABASE_DB_DSN (operator-confirmed); build/import passes without push (false-positive) | Plan 01 Task 2: `make supabase-migrate` then `psql "$DSN" -c "...information_schema..."` proves column live (is_nullable='YES') |
| Throughput at real candidate scale | D-05/D-06 | Requires prod polyarb-l2 + real WS frames + chaos injection; cannot run in CI | Plan 04 Task 3 (checkpoint:human-verify): baseline-then-threshold — frame_rate_recovery >= baseline*0.90, watchdog → WAITING_FOR_EVENT within 60s, RSS <= baseline*1.30; record in 04-SOAK-LOG.md |
| Deployed image == latest main | D-05 pre-flight | parallel-worktree-rebase discipline; image identity is a runtime property | Plan 04 Task 3 pre-flight: `FLY_API_TOKEN= flyctl image show -a polyarb-l2` vs `git log origin/main -1` |

---

## Validation Sign-Off

- [x] All tasks have `<automated>` verify or are explicit manual checkpoints with Wave 0 dependencies
- [x] Sampling continuity: no 3 consecutive tasks without automated verify (Plan 04 Task 3 is the only manual, preceded by 2 automated)
- [x] Wave 0 covers all MISSING references
- [x] No watch-mode flags
- [x] Feedback latency < 30s per unit task
- [x] `nyquist_compliant: true` set in frontmatter

**Approval:** planner-populated 2026-05-28 — pending plan-checker review
