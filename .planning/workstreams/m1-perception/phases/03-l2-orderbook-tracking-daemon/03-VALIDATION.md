---
phase: 03
slug: l2-orderbook-tracking-daemon
status: draft
nyquist_compliant: false
wave_0_complete: false
created: 2026-05-23
---

# Phase 03 — Validation Strategy

> Per-phase validation contract. Filled in from 03-RESEARCH.md § Validation Architecture. Status flips to `complete` + `nyquist_compliant: true` + `wave_0_complete: true` at Plan 08 closure (D-09 Phase 02.1 P6 pattern).

---

## Test Infrastructure

| Property | Value |
|----------|-------|
| **Framework** | pytest 8.x (project uv-managed) |
| **Config file** | `pyproject.toml` `[tool.pytest.ini_options]` |
| **Quick run command** | `uv run pytest tests/m1-perception/ -x -q --tb=short` |
| **Full suite command** | `uv run pytest tests/ -v --tb=short` |
| **Estimated runtime** | ~90s quick / ~5min full |
| **Coverage gate** | none enforced; focus on must_haves.truths |

---

## Sampling Rate

- **After every task commit:** `uv run pytest tests/m1-perception/<plan-scoped-dir>/ -x -q`
- **After every plan wave:** Quick run command (above)
- **Before `/gsd-verify-work` or chaos run:** Full suite must be green
- **Max feedback latency:** 90 seconds (quick run)

---

## Per-Plan Verification Map

| Plan | Wave | Requirement (from CONTEXT.md) | Wave 0 RED Test | Manual / Chaos Verification |
|------|------|------------------------------|-----------------|------------------------------|
| 01 | 1 | D-01 GHA Supabase keepalive | `tests/test_supabase_keepalive_yml.py` — parse cron + wget endpoint | `gh run list -w supabase-keepalive.yml --limit 7` (one per day, 7 days观察) |
| 02 | 1 | D-06 polyarb-l2 Fly bootstrap | `tests/test_fly_l2_config.py` — app=polyarb-l2 + no cron + /healthz probe | `curl https://polyarb-l2.fly.dev/healthz` returns 200 |
| 03 | 2 | D-06 L2 daemon entry + server-started gate (P9) | `tests/daemon/test_l2_main_startup.py` — init order assertion | `flyctl logs -a polyarb-l2` 显示 "uvicorn ready" line within 30s |
| 04 | 3 | D-02 WS client + D-03 staleness watchdog | `tests/clients/test_ws_market_client.py` + `tests/daemon/test_ws_watchdog.py` | Inj L2-1 (kill WS mid-stream → watchdog reconnect within 45s) |
| 05 | 4 | D-04 + D-05 candidate refresh + event bus | `tests/observation/test_l2_candidate_refresh.py` — diff add/remove | Inj L2-3 (set/unset POLYARB_EVENT_BUS_ENABLED=1 on L1 → L2 receives/starves → fallback last-known + Sentry warning) |
| 06 | 5 | D-07 Alembic 003 + D-08 trades + REST backfill | `tests/storage/test_l2_supabase_mirror.py` + `alembic/test_003.py` + `tests/clients/test_data_api_trades.py` | Inj L2-2 (撤 SUPABASE_SERVICE_KEY → fail-soft + breadcrumb category='l2-mirror') / Inj L2-5 (Data API 429 → backfill resume) |
| 07 | 6 (checkpoint) | Phase 02 L6/L7 chaos discipline | `tests/chaos/test_l2_chaos_plan.py` — every truth has programmatic verify path | All 5 Inj L2-* run live in prod, 03-SOAK-LOG.md 5 segments |
| 08 | 7 (checkpoint) | D-07 dashboard pages + D-09 closure | none (pure docs) | `make planning-status` zero drift + Vercel deploy 4 pages verified |

> **Note on wave semantics:** Wave numbers above are *stages* (`max(depends_on_waves) + 1`), not parallel groups. After B2 serialization (Plan 05 depends on Plan 04 to avoid `pyproject.toml` / `l2_main.py` / `config.py` overlap), Plans 04 → 05 → 06 → 07 → 08 run strictly sequentially. Wave 1 (Plans 01 + 02) and Wave 2 (Plan 03) retain genuine parallelism within their own boundaries.

---

## Wave 0 Requirements (per-plan, MUST be RED before any GREEN feat commit)

- [ ] Plan 01: `tests/test_supabase_keepalive_yml.py` — parse cron schedule + wget endpoint format
- [ ] Plan 02: `tests/test_fly_l2_config.py` — fly-l2.toml structure assertion
- [ ] Plan 03: `tests/daemon/test_l2_main_startup.py` — daemon init order
- [ ] Plan 04 Task 1: `tests/clients/test_ws_market_client.py` — subscribe payload shape
- [ ] Plan 04 Task 2: `tests/daemon/test_ws_watchdog.py` — 30s timeout + exp backoff
- [ ] Plan 05: `tests/observation/test_l2_candidate_refresh.py` — diff algorithm + `tests/m1-perception/test_orchestrator.py::test_step_7_7_skipped_by_default` — verify default-disabled gate
- [ ] Plan 06: `tests/storage/test_l2_supabase_mirror.py` + `alembic/test_003.py` + `tests/clients/test_data_api_trades.py`
- [ ] Plan 07: `tests/chaos/test_l2_chaos_plan.py` — declarative chaos plan
- [ ] Plan 08: N/A (pure documentation)

**Sync gate**: `wave_0_complete: true` flips at Plan 08 closure ONLY after ALL above tests exist + initially RED + drove their respective Plan to GREEN.

---

## Programmatic Verification Surfaces (D-09 verification-ownership)

**Anti-pattern reminder (Phase 02 L7)**: Never claim "verified" without naming the exact command + grep-able output line. **Every truth in every PLAN.md must satisfy `programmatic? = yes`** — verifiable via shell/curl from Claude's seat, NOT user UI navigation.

| Surface | Programmatic command |
|---------|----------------------|
| polyarb-l2 deployed & alive | `curl -fsS https://polyarb-l2.fly.dev/healthz \| jq '.status'` |
| WS connection up | `flyctl ssh console -a polyarb-l2 -C "ss -tn dst :443"` |
| WS staleness watchdog | `curl -fsS https://polyarb-l2.fly.dev/healthz \| jq '.checks."ws:last_event_age_seconds"'` |
| candidate refresh count | psql to Supabase: `SELECT count(*) FROM l2_candidates WHERE included_at_ts > now() - interval '24 hours'` |
| trades accumulation | psql: `SELECT count(*), max(ts) FROM l2_trades` |
| Sentry event (via API, EU region) | `curl 'https://de.sentry.io/api/0/projects/speechlessai/python/events/?statsPeriod=1h' -H "Authorization: Bearer $SENTRY_TOKEN"` |
| Telegram alert fired | `curl https://api.telegram.org/bot$TG_TOKEN/getUpdates \| jq` (within recent window) |
| GHA keepalive workflow ran | `gh run list -w supabase-keepalive.yml --limit 7` (expect one per day) |

---

## Chaos Verification Plan (per Phase 02 L6/L7 + P14)

5 chaos injections required for Phase 03 closure. Each Inj **must have container-localhost fallback** (Phase 02.1 L8): if Fly proxy is broken mid-phase, every verification must work via `flyctl ssh console -C "curl localhost:8080/..."`.

| Inj # | Injection | Code path triggered | Truth verified | Cleanup |
|-------|-----------|---------------------|----------------|---------|
| **L2-1** | Kill WS connection mid-stream (`POLYARB_WS_TEST_KILL=1` flag forces `.close()` OR `iptables` block 443 inside container) | watchdog 30s timeout → reconnect → resubscribe initial_dump | watchdog state transition + reconnect succeeds + `l2_top_of_book` 写延迟 ≤45s | unset flag / unblock iptables |
| **L2-2** | 撤 `POLYARB_SUPABASE_SERVICE_KEY` from polyarb-l2 secrets | mirror write fail-soft path → loguru INFO + Sentry breadcrumb (`category='l2-mirror'`) | breadcrumb pulled via Sentry API + daemon does NOT crash + write skipped without exception | restore secret + restart |
| **L2-3** | Two-part probe of L1 NOTIFY emission gate (`POLYARB_EVENT_BUS_ENABLED` defaults FALSE per B1 fix). **L2-3a (default-state):** ensure flag is unset/0 on L1 → confirm L2 candidate refresh starves naturally → falls back to last_known set + Sentry warning crumb. **L2-3b (opt-in path):** `flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 -a polyarb-l1` → trigger L1 scan → confirm L2 receives NOTIFY + candidate refresh executes → then unset secret → confirm L2 falls back again. | L2 listener path correctly gated AND fallback works under both states | restore L1 to opt-in (`POLYARB_EVENT_BUS_ENABLED=1`) ONLY after Plan 07 chaos PASS for L2-3 |
| **L2-4 (cross-bug)** | Simulate prod = (a) WS reconnect storm + (b) Supabase Free tier paused | does daemon survive? alert dedup? GHA keepalive auto-recover? | daemon survives both + alert dedup active + GHA next run unpause | natural — Supabase auto-unpause after GHA ping |
| **L2-5** | Data API /trades 429 rate limit during 7d backfill | retry-with-backoff + partial completion checkpoint | backfill resumes from last checkpoint after rate limit clears | natural — wait 10s |

---

## Pre-existing Test Failures (deferred, NOT blocking Phase 03)

Documented in `deferred-items.md`:
- `test_pass_when_fresh` (Phase 02 P3 carry-over)
- `make_smoke_health_local` (Phase 02 leftover)
- `test_r2_retry` (Phase 02 leftover)

Phase 03 plans run against current `main` HEAD; these 3 must stay deferred to not block.

---

## Sign-Off (flips at Plan 08 closure)

- [ ] All 7 prior plans (01-07) have SUMMARY.md committed (pre-closure gate)
- [ ] Plan 08 SUMMARY committed (closure act, signals frontmatter flip readiness)
- [ ] All Wave 0 RED tests existed before respective GREEN commits
- [ ] All 5 chaos injections recorded in 03-SOAK-LOG.md with timestamps + programmatic evidence
- [ ] `make planning-status` zero drift
- [ ] Frontmatter flipped: `status: complete` + `nyquist_compliant: true` + `wave_0_complete: true`
- [ ] Before flipping frontmatter, verified `POLYARB_EVENT_BUS_ENABLED=1` set on prod L1 (Fly secret) AND Inj L2-3 chaos verdict PASS (B1 cascade gate)

**Approval**: pending (Phase 03 in plan stage)
