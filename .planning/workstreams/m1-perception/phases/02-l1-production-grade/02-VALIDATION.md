---
phase: 02
slug: l1-production-grade
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-12
---

# Phase 02 — Validation Strategy

> Per-phase validation contract for feedback sampling during execution.
> Derived from RESEARCH.md §11 Validation Architecture. Phase 02 加 22+ Wave 0 tests covering 7 the agent discretion items + chaos engineering + 7-day soak gate.

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 8.2+ + pytest-asyncio 0.23 + respx 0.21 + freezegun 1.5 |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` |
| **Quick run command** | `uv run pytest -x -q tests/test_health_endpoint.py tests/test_supabase_mirror.py` |
| **Full suite command** | `uv run pytest -q` |
| **Estimated runtime** | ~4 min (m1-perception full suite, current 402 tests + Phase 02 adds ~22) |

---

## Sampling Rate

- **Per task commit:** Run `uv run pytest -x -q tests/test_<file_just_changed>.py`
- **Per wave merge:** Run `uv run pytest -q` (full m1-perception ~4 min)
- **Phase gate (before deploy to prod):** full suite + `make smoke-test` + `docker build` 全绿
- **Phase completion gate (7-day soak):** prod 7×24 跑无人值守 + Better Stack uptime ≥ 99% + 至少一次自然失败自愈或正确告警 (thread §1 生产级判定标准)
- **Max feedback latency:** 4 minutes (full suite)

---

## Per-Task Verification Map

> Maps each CONTEXT decision (D-XX) / RESEARCH section (§N) / LEARNINGS reference (LXX) to verification command. Wave 0 = test file not yet existing.

| Req | Plan | Wave | Behavior | Threat Ref | Secure Behavior | Test Type | Automated Command | File Exists | Status |
|-----|------|------|----------|------------|-----------------|-----------|-------------------|-------------|--------|
| D-08 / §6 | TBD | 0 | Dockerfile builds clean | T-02-08 supply chain | uv lock hash verified, base image pinned | smoke (CI) | `docker build -t polyarb-l1 . && docker run --rm polyarb-l1 python -c "import polyarb"` | ❌ W0 — `tests/test_docker_smoke.sh` | ⬜ pending |
| D-08 / §6 | TBD | 0 | Image starts, /health responds | T-02-09 misconfig | non-root UID 10001, healthcheck wired | smoke (CI) | `docker run -d -p 8080:8080 polyarb-l1 && sleep 5 && curl -fsS localhost:8080/health` | ❌ W0 | ⬜ pending |
| D-12 / §8 | TBD | 0 | /health returns pass when snapshot fresh | — | IETF draft-inadarei pass output | unit | `pytest tests/test_health_endpoint.py::test_pass_when_fresh -x` | ❌ W0 | ⬜ pending |
| D-12 / §8 | TBD | 0 | /health returns warn when 15h stale | — | DEGRADED differentiation | unit | `pytest tests/test_health_endpoint.py::test_warn_when_stale -x` | ❌ W0 | ⬜ pending |
| D-12 / §8 | TBD | 0 | /health returns 503 when 25h+ stale | — | FAILED differentiation | unit | `pytest tests/test_health_endpoint.py::test_fail_when_very_stale -x` | ❌ W0 | ⬜ pending |
| D-13 / §scheduler | TBD | 0 | scheduler pauses after 3 consecutive failures | T-02-04 DoS | API quota protection | unit | `pytest tests/test_scheduler.py::test_pause_after_3_failures -x` | ❌ W0 | ⬜ pending |
| D-21 / §9 | TBD | 0 | /scan rejects missing X-Signature | T-02-01 auth bypass | HMAC middleware enforced | unit | `pytest tests/test_http_scan.py::test_rejects_missing_signature -x` | ❌ W0 | ⬜ pending |
| D-21 / §9 | TBD | 0 | /scan rejects invalid HMAC | T-02-01 auth bypass | constant-time compare, secret-rotation ready | unit | `pytest tests/test_http_scan.py::test_rejects_bad_hmac -x` | ❌ W0 | ⬜ pending |
| D-21 / §9 | TBD | 0 | /scan invokes scanner.run_recipe with sanitized input | T-02-02 SQL injection | 4 层 SQL 防御 preserved | integration | `pytest tests/test_http_scan.py::test_invokes_run_recipe -x` | ❌ W0 | ⬜ pending |
| D-21 / §9 | TBD | 0 | /scan 4-layer SQL defense still applies | T-02-02 SQL injection | yaml trust-split preserved across HTTP boundary | integration | `pytest tests/test_http_scan.py::test_yaml_trust_split_preserved -x` | ❌ W0 | ⬜ pending |
| §3 / Supabase mirror | TBD | 1 | mirror failure does not fail snapshot (DEGRADED) | — | failure isolation | integration | `pytest tests/test_supabase_mirror.py::test_mirror_failure_degraded -x` | ❌ W0 | ⬜ pending |
| §3 / Supabase mirror | TBD | 1 | idempotent upsert (re-run safe) | T-02-03 data corruption | upsert on PK conflict | integration | `pytest tests/test_supabase_mirror.py::test_idempotent_upsert -x` | ❌ W0 | ⬜ pending |
| §4 / R2 sync | TBD | 1 | parquet upload to R2 (mocked boto3) | — | proper auth, signed URL | unit | `pytest tests/test_r2_sync.py::test_upload_to_r2 -x` | ❌ W0 | ⬜ pending |
| §4 / cron | TBD | 0 | snapshots-purge respects DAYS + KEEP | — | retention policy | unit | `pytest tests/test_purge.py -x` | ✅ Phase 01.1 amendment | ⬜ pending |
| §5 / page_fetched_at_ms | TBD | 0 | normalizer carries per-page stamp | — | 框架抽象 A 修 L2 | unit | `pytest tests/test_normalizer.py::test_page_fetched_at_ms -x` | ❌ W0 | ⬜ pending |
| §5 / schema | TBD | 0 | SQLite + Parquet new column | — | union_by_name compatible (P7) | unit | `pytest tests/test_schemas.py::test_page_fetched_at_ms_nullable -x` | ❌ W0 | ⬜ pending |
| D-11 / chaos | TBD | 1 | snapshot resumes after Gamma 503 mock | — | tenacity backoff working | integration | `pytest tests/test_chaos_gamma_5xx.py -x` | ❌ W0 | ⬜ pending |
| D-13 / chaos | TBD | 1 | daemon pauses after 3x consecutive failures | — | alert + manual unblock | integration | `pytest tests/test_chaos_3failures_pause.py -x` | ❌ W0 | ⬜ pending |
| D-14 / observability | TBD | 0 | loguru JSON output is Axiom-parseable | T-02-07 log injection | JSON serialize correctly | unit | `pytest tests/test_logging.py::test_json_serialize -x` | ❌ W0 | ⬜ pending |
| L11 / makefile | TBD | 0 | `make snapshot-markets` exit 0 ↔ parquet落地 + SQLite row +1 (triple check) | — | silent failure prevention | integration | `bash tests/test_makefile_triple_check.sh` | ❌ W0 — 关键修 L11 silent failure | ⬜ pending |
| D-12 / parquet validation | TBD | 0 | parquet 行数 == SQLite snapshots.market_count | — | dual-source consistency | unit | `pytest tests/test_parquet_sqlite_consistency.py -x` | ❌ W0 | ⬜ pending |
| D-09/D-10 / cadence | TBD | 1 | Fly scheduled machines fire at expected cron schedule | — | cron miss compensation | manual (prod observation, smoke) | Check Better Stack history + Fly machine logs after 1 day | manual | ⬜ pending |

*Status: ⬜ pending · ✅ green · ❌ red · ⚠️ flaky*

---

## Wave 0 Requirements

> Wave 0 必须早于 plan implementation 加入 — 否则 plan 提交的代码无法被验证。

- [ ] `tests/test_health_endpoint.py` — covers D-12 / D-16 (/health endpoint, 5 测试)
- [ ] `tests/test_http_scan.py` — covers D-21 / D-22 (/scan endpoint, 4 测试 含 HMAC + Trust-split)
- [ ] `tests/test_supabase_mirror.py` — covers §3 (DB mirror, 2 测试)
- [ ] `tests/test_r2_sync.py` — covers §4 (R2 upload, 1 测试)
- [ ] `tests/test_scheduler.py` — covers D-13 (3-failure pause, 1 测试)
- [ ] `tests/test_normalizer.py::test_page_fetched_at_ms` — covers §5 (框架抽象 A)
- [ ] `tests/test_schemas.py::test_page_fetched_at_ms_nullable` — covers §5 (schema 演进)
- [ ] `tests/test_chaos_gamma_5xx.py` — chaos engineering Gamma 5xx (respx mock)
- [ ] `tests/test_chaos_3failures_pause.py` — chaos engineering 3x failure
- [ ] `tests/test_logging.py::test_json_serialize` — covers D-14 (loguru → Axiom)
- [ ] `tests/test_makefile_triple_check.sh` — covers L11 silent failure root cause
- [ ] `tests/test_parquet_sqlite_consistency.py` — covers D-12 dual-check (parquet+SQLite)
- [ ] `tests/test_docker_smoke.sh` — covers D-08 (Docker build + run)
- [ ] **Framework install:** 已有 pytest 8.2 + pytest-asyncio 0.23 + respx 0.21（pyproject.toml dev extra）；加 `freezegun 1.5` for 时间窗口测试

---

## Chaos Engineering Sub-Strategy

> RESEARCH §11 Chaos Engineering 子节 — 直接驱动 Phase 02 完成判定（thread §1 生产级判定）。

| Chaos scenario | 模拟 | 期望行为 | Test command |
|---|---|---|---|
| Gamma API 503 × 5 | respx mock | tenacity backoff，最后 layer1 issue → FAILED | `pytest tests/test_chaos_gamma_5xx.py::test_gamma_503_5x` |
| Gamma 翻页超时 | respx delay | partial fetch + Issue → DEGRADED or FAILED | `pytest tests/test_chaos_gamma_5xx.py::test_gamma_timeout` |
| CLOB malformed book | respx mock | F-1 _safe_float capture + Issue → DEGRADED | `pytest tests/test_chaos_clob.py::test_malformed_book` |
| SQLite WAL lock contention | tmp_path | reader waits + eventually succeeds | `pytest tests/test_sqlite_concurrency.py` |
| Supabase API 500 | respx mock | mirror failure → DEGRADED but snapshot OK | `pytest tests/test_chaos_supabase.py::test_500_degraded` |
| R2 PUT 503 | botocore stubber | upload retry 3x, then Issue → DEGRADED | `pytest tests/test_chaos_r2.py::test_r2_503` |
| 3x consecutive FAILED | scheduler test | daemon paused, alert sent | `pytest tests/test_chaos_3failures_pause.py` |
| /scan flood (10 req/s × 30s) | async loop | rate limit, no crash | `pytest tests/test_chaos_scan_flood.py` (slow, optional) |

---

## Pre-prod 7-Day Soak Test (Phase Completion Gate)

> Phase 02 "完成"的真正 gate — 与 thread §1 生产级判定标准对齐。**未达 soak 通过禁开 Phase 03 L2 工作。**

| Check | Pass criteria | Source |
|---|---|---|
| Uptime | Better Stack uptime ≥ 99% (10 min downtime tolerance per week) | Better Stack dashboard |
| Cron execution | subset cron 14/14 fires (7 天 × 2 次/天)；full cron 1/1 fires | Fly machine logs |
| Snapshot success | OK + DEGRADED ≥ 95% of attempts | SQLite snapshots table |
| Alert validation | 至少 1 次自然失败 → Telegram alert 收到 + email 同步 | Telegram chat log |
| Self-healing | 失败后系统自愈 (transient) 或正确 paused (3x连续) | log review |
| Disk growth | SQLite ≤ 4GB at day 7, R2 archive growing | Fly volume usage |
| Errors | Sentry errors < 5 per day (excluding transient retries) | Sentry dashboard |

完成 7 天 soak 后跑 `/gsd-extract_learnings 02`，然后进 `/gsd-discuss-phase 03` (L2 定向跟踪)。

---

## Manual-Only Verifications

| Behavior | Requirement | Why Manual | Test Instructions |
|----------|-------------|------------|-------------------|
| Dashboard 视觉对齐 / 配色 / 响应式 | D-18..D-22 | UI 设计现场取舍 (--skip-ui 跳过 UI-SPEC) | 部署后人工浏览各页面，对比 thread §1 期望，记 issue |
| Telegram bot alert 实际触达 | D-17 | 涉及第三方平台 + 个人 Telegram 账号 | 部署后 trigger 一次故意 FAILED snapshot，确认收到 |
| Magic link email 实际接收 | D-20 | 涉及 email 投递 | 实测 |
| Fly cron miss 后行为 | D-09/D-10 | 真实时间触发 | 部署后观察 7 天 |
| 7-day soak 整体观察 | thread §1 | 时长决定 | 持续 7 天 |

---

## Validation Sign-Off

- [ ] All tasks have `<automated>` verify or Wave 0 dependencies
- [ ] Sampling continuity: no 3 consecutive tasks without automated verify
- [ ] Wave 0 covers all MISSING references (12 个 test 文件需新建)
- [ ] No watch-mode flags (pytest 走单次 / CI-friendly)
- [ ] Feedback latency < 240s (4 min full suite)
- [ ] 7-day soak gate 明确写入 phase 完成判定（thread §1 纪律）
- [ ] `nyquist_compliant: true` set in frontmatter after Wave 0 complete

**Approval:** pending — planner 在 plan 阶段映射 Wave 0 tests 到具体 plan，commit 后改为 approved。
