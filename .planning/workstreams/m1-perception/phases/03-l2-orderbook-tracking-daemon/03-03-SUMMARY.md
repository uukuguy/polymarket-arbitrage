---
phase: 03
plan: 03
status: complete (code-half — deploy half deferred to user)
subsystem: daemon-l2-entry
tags: [daemon, starlette, uvicorn, server-started-gate, health-endpoints, polyarb-l2]
wave: 2
requires: [D-06]
provides:
  - src/polyarb/daemon/l2_main.py (L2 daemon entry — init order mirrors L1 P9)
  - src/polyarb/http/l2_health.py (Phase 02.1 P5 helper-first refactor — _build_l2_health_checks)
  - src/polyarb/http/l2_app.py (Starlette app factory for L2 — /health + /healthz routes)
  - Settings.daemon_variant ("l1" | "l2") + L2 sub-checks scaffolding
  - Makefile targets: daemon-l2-run-local, smoke-l2-health, smoke-l2-health-prod
affects:
  - daemon-runtime-roster (now: polyarb-l1 + polyarb-l2)
  - sentry-service-tagging (L2 events tagged service=polyarb-l2)
  - http-route-registry (L2-only — /health + /healthz)
tech-stack-added: [starlette/Route, uvicorn.Server, sentry_sdk.set_tag]
key-files-created:
  - src/polyarb/daemon/l2_main.py (172 lines)
  - src/polyarb/http/l2_health.py (259 lines)
  - src/polyarb/http/l2_app.py (58 lines)
  - tests/daemon/__init__.py (0 lines — pytest discovery)
  - tests/daemon/test_l2_main_startup.py (235 lines, 6 tests)
  - tests/m1-perception/test_l2_health_endpoint.py (124 lines, 8 tests)
key-files-modified:
  - src/polyarb/config.py (added daemon_variant Literal["l1","l2"] field)
  - tests/m1-perception/conftest.py (added mock_ws_consumer / mock_event_listener / l2_http_test_client)
  - Makefile (3 new targets)
decisions:
  - skeleton-SUMMARY-first to satisfy pre-commit hook (Plan 03-02 precedent)
  - placeholder ws_consumer=None / event_listener=None at Plan 03 boundary — health degrades to "warn" with output="not_configured"
  - L2 daemon listens on :19081 locally (neighbors L1's :19080 per feedback_port-numbers-2026-05)
  - Sentry tag literal string "polyarb-l2" (T-03-03-04 lock)
metrics:
  duration_minutes: ~75 (well under 3-4h estimate)
  completed_date: 2026-05-24
  task_commits: 5 (skeleton + RED tests + RED health tests + GREEN health + GREEN l2_main + Makefile)
---

# Phase 03 Plan 03: L2 Daemon Entry + /health + /healthz — Summary

> **One-liner**: Built runnable polyarb-l2 daemon skeleton — `polyarb.daemon.l2_main`
> mirrors L1's P9 server-started gate, ships Starlette app factory `create_l2_app`,
> and a `/health` (IETF strict 503) + `/healthz` (always 200) endpoint pair sharing
> a single `_build_l2_health_checks` helper (Phase 02.1 P5 refactor). Skeleton's
> placeholders for `ws_consumer` / `event_listener` cleanly degrade to "warn" so
> Plans 04 and 05 can run in parallel against this skeleton.

## Deliverables Shipped

| File | Lines | Commit | Purpose |
|------|-------|--------|---------|
| `src/polyarb/daemon/l2_main.py`           | 172 | `781be48` | L2 daemon entry — uvicorn + P9 gate + asyncio.gather skeleton |
| `src/polyarb/http/l2_health.py`           | 259 | `3a01c1b` | `_build_l2_health_checks` helper + handlers (Phase 02.1 P5) |
| `src/polyarb/http/l2_app.py`              |  58 | `3a01c1b` | Starlette factory `create_l2_app` |
| `src/polyarb/config.py` (modify)          |  +9 | `3a01c1b` | `daemon_variant: Literal["l1","l2"]` field |
| `tests/daemon/__init__.py`                |   0 | `4abf7ee` | pytest discovery enabler |
| `tests/daemon/test_l2_main_startup.py`    | 235 | `4abf7ee` | 6 init-order + P9-gate + Sentry-tag + no-L1-cross-pollination tests |
| `tests/m1-perception/test_l2_health_endpoint.py` | 124 | `0bf4d3d` | 8 state-based + secret-leak + content-type tests |
| `tests/m1-perception/conftest.py` (modify)| +50 | `0bf4d3d` | `mock_ws_consumer` + `mock_event_listener` + `l2_http_test_client` fixtures |
| `Makefile` (modify)                       | +41 | `0f5c751` | 3 new targets: `daemon-l2-run-local`, `smoke-l2-health`, `smoke-l2-health-prod` |

## Truths Verified (9/9 from frontmatter must_haves)

```
$ grep -c "server.started" src/polyarb/daemon/l2_main.py
4                                                              # ≥1 ✓ (Truth 1: P9 gate present)

$ grep -cE 'set_tag\("service",\s*"polyarb-l2"\)' src/polyarb/daemon/l2_main.py
2                                                              # ≥1 ✓ (Truth 2: Sentry service tag)

$ grep -c '^def _build_l2_health_checks' src/polyarb/http/l2_health.py
1                                                              # == 1 ✓ (Truth 3: helper exists)

$ grep -c '"polyarb-l2"' src/polyarb/http/l2_health.py
2                                                              # ≥1 ✓ (Truth 4: serviceId in body)

$ grep -c "from polyarb.daemon.main import" src/polyarb/daemon/l2_main.py
0                                                              # == 0 ✓ (Truth 5: no L1 cross-pollination)

$ uv run python -c "from polyarb.daemon import l2_main; print('OK')"
OK                                                             # exit 0 ✓ (Truth 6: module importable)

$ grep -c "^daemon-l2-run-local:" Makefile
1                                                              # == 1 ✓ (Truth 7: Makefile target)

$ uv run pytest tests/daemon/test_l2_main_startup.py -q --tb=no
......                                                         # 6/6 GREEN ✓ (Truth 8: init-order + P9 gate)

$ uv run pytest tests/m1-perception/test_l2_health_endpoint.py -q --tb=no
........                                                       # 8/8 GREEN ✓ (Truth 9: health endpoint behavior)
```

## Live Smoke Test Evidence (local boot — 19091)

Boot command:
```
POLYARB_DAEMON_VARIANT=l2 POLYARB_DB_PATH=./data/l2-state-test.db \
  POLYARB_HTTP_PORT=19091 POLYARB_ALLOW_EMPTY_SECRET=1 \
  uv run python -m polyarb.daemon.l2_main
```

Key log lines (loguru JSON sink to stdout):
```
| INFO | polyarb.observability.sentry:init_sentry:122 - sentry initialized — release=dev env=dev
| INFO | __main__:main:80 - polyarb-l2 daemon starting up
| INFO | uvicorn.server:_serve:82 - Started server process [66928]
| INFO | uvicorn.lifespan.on:startup:62 - Application startup complete.
| INFO | uvicorn.server:_log_started_message:214 - Uvicorn running on http://0.0.0.0:19091
| INFO | __main__:main:135 - polyarb-l2 daemon running: http on :19091, variant=l2
```

`/healthz` body (HTTP 200, status=warn — Plan 03 boundary placeholders):
```json
{
  "status": "warn",
  "version": "0.2.0",
  "releaseId": "dev",
  "serviceId": "polyarb-l2",
  "description": "Polymarket L2 orderbook tracking daemon — WS market channel + event bus",
  "checks": {
    "ws:connection_state":      [{"componentId":"ws-consumer",   "observedValue":"not_configured","status":"warn",
                                   "output":"ws_consumer not yet wired (Plan 04 deliverable)"}],
    "event_bus:listener_state": [{"componentId":"event-listener","observedValue":"not_configured","status":"warn",
                                   "output":"event_listener not yet wired (Plan 05 deliverable)"}]
  }
}
```

`/health` HTTP 200 (warn, NOT fail → not 503) — IETF strict behavior confirmed.

SIGTERM → graceful shutdown evidence:
```
19:45:06.606 | INFO | __main__:_shutdown:114 - polyarb-l2 received SIGTERM, initiating graceful shutdown
19:45:06.606 | INFO | __main__:main:150 - polyarb-l2 daemon stopping
19:45:06.732 | INFO | uvicorn.lifespan.on:shutdown:76 - Application shutdown complete.
19:45:06.733 | INFO | __main__:main:161 - polyarb-l2 daemon stopped cleanly
```
Total shutdown latency: ~127ms (well within F-04 5s budget).

## Deviations from Plan

### 1. [Rule 3 - Blocking issue] Skeleton SUMMARY landed early (pre-commit hook)
- **Found during**: Task 2 commit attempt — pre-commit hook blocked the test/test commit with "A plan-scoped commit must have a corresponding SUMMARY.md"
- **Root cause**: hook reads `.git/COMMIT_EDITMSG` to extract phase/plan scope; once it saw `test(03-03):` from Task 1, subsequent plan-scoped commits required SUMMARY on disk
- **Fix**: created skeleton SUMMARY following the Plan 03-02 precedent (file commit `0bf4d3d`); Task 7 (this document) replaces the skeleton with the final SUMMARY
- **Commit**: `0bf4d3d docs(03-03): SUMMARY skeleton (pre-commit unblocker)` (note: bundled the Task 2 test file with the skeleton because both were staged when the hook unblocked)

### 2. [Rule 3 - Blocking issue] `pytest-asyncio` mode = auto, so `@pytest.mark.asyncio` decorator unnecessary
- **Found during**: Task 1 author
- **Detail**: `pyproject.toml` has `asyncio_mode = "auto"`; `pytest-asyncio>=0.23` discovers async tests automatically. Plan's example code used `@pytest.mark.asyncio` but real tests omit it.
- **Impact**: cosmetic — tests work either way; chose to follow repo convention (no decorator).

### Not deviations (intentional design):
- L2 listens on `:19081` locally (plan-recommended port — neighboring L1's `:19080`)
- Health endpoint sub-checks scaffold all 4 categories (ws state / ws age / event_bus / mirror) at Plan 03 — Plans 04/05/06 wire data without changing schema

## Pre-existing Test Failures (NOT caused by this plan — logged to deferred-items)

`deferred-items.md` in this phase dir captures 2 pre-existing failures verified independently
of Plan 03-03 work (confirmed via `git stash && pytest`):

1. `tests/m1-perception/test_health_endpoint.py::test_pass_when_fresh` — L1 R2 sub-check now
   returns "warn" instead of "pass" for test snapshots without R2 URL. Pre-existing on main
   before Plan 03-03 started.
2. `tests/m1-perception/test_makefile_contract.py::test_make_smoke_health_local_dry_run_recipe`
   — test expects literal `127.0.0.1:8080/health` in Makefile output but Makefile uses `$PORT`
   variable defaulting to 19080.

Both are Phase 02/02.1-era L1 regressions; out of scope for Plan 03-03 (Rule of scope boundary).
Recommend fixing during a Phase 02 fix-up plan or m1-perception housekeeping pass.

## Carry-Forward to Plan 04 + Plan 05

Plan 04 (WS client + watchdog) must wire `ws_consumer`:
```python
# In l2_main.py — replace placeholder block:
#   ws_consumer: Any = None      # placeholder — health check shows warn until Plan 04
# With:
ws_consumer = WsConsumer(settings, sqlite_store)
ws_consumer_task = asyncio.create_task(ws_consumer.run(stop_event))
```
The `WsConsumer` interface must expose `.current_state` (str: CONNECTED / WAITING_FOR_EVENT /
RECONNECTING) and `.last_event_at_s` (epoch seconds float). Mock-shaped fixtures in
`tests/m1-perception/conftest.py::mock_ws_consumer` document the contract.

Plan 05 (event bus + candidate refresh) must wire `event_listener`:
```python
event_listener = EventListener(settings, candidate_refresh_fn)
event_listener_task = asyncio.create_task(event_listener.listen(stop_event))
```
The `EventListener` interface must expose `.is_listening` (bool). Mock-shaped fixture is
`tests/m1-perception/conftest.py::mock_event_listener`.

Plan 06 (mirror) must set `settings.l2_mirror_enabled = True` and provide
`SQLiteStore.get_l2_tob_last_mirror_at_s()` returning epoch seconds. The
`_build_l2_health_checks` helper already reads this — Plan 06 only adds the data path.

## Next Action for User (Task 7 — Fly Deploy + Better Stack Monitor)

The remaining checkpoint task is hand-off to user because it involves Fly volume creation
+ production deploy + Better Stack dashboard config. Claude has NOT run any of these.

Execute IN ORDER:

```bash
# 1. Create Fly volume for L2 (1GB in AMS, same region as L1)
flyctl volumes create polyarb_l2_data --region ams --size 1 -a polyarb-l2

# 2. Sync .env secrets to polyarb-l2 (idempotent — safe to re-run on L1)
make fly-secrets-sync
flyctl secrets list -a polyarb-l2 | wc -l   # expect ≥14 secrets (parity with L1)

# 3. Deploy via GHA workflow (paths-filtered trigger)
make deploy-l2-prod                          # invokes deploy-l2.yml workflow
gh run watch                                  # follow deploy in real time

# 4. Verify L2 reachable + healthy
make smoke-l2-health-prod                    # expect HTTP 200, status=warn, serviceId=polyarb-l2
flyctl status -a polyarb-l2                  # expect 1 machine, state=started
flyctl checks list -a polyarb-l2             # expect 1 health check, status=passing

# 5. (Dashboard step — USER ACTION) Better Stack monitor
# Better Stack UI → Uptime → New Monitor:
#   URL:              https://polyarb-l2.fly.dev/health   (IETF strict — 503 = alarm)
#   Check frequency:  30s (matches L1)
#   Alert escalation: same on-call group as polyarb-l1
# Save → confirm monitor turns "Up" within 60s.

# 6. (Verification) Sentry event spot-check
curl -s "https://de.sentry.io/api/0/projects/speechlessai/python/events/?statsPeriod=1h" \
  -H "Authorization: Bearer $SENTRY_TOKEN" \
  | jq '[.[] | select(.tags[]?.value == "polyarb-l2")] | length'
# Expect ≥1 (the daemon startup will emit a Sentry breadcrumb)
```

**Expected /health body in prod** (Plan 03 boundary):
- `status: "warn"` (Plan 04/05 placeholders return "warn" — this is correct)
- `HTTP 200` (warn != fail, so /health does NOT return 503)
- `serviceId: "polyarb-l2"` (T-03-03-04 verification)

Once Plan 04 wires real `WsConsumer` and the WS is connected with a recent event,
`/health` will flip to `status: "pass"` and `HTTP 200`.

## Success Criteria (code-side — all met)

- [x] polyarb-l2 daemon **runs locally** via `make daemon-l2-run-local` — confirmed boot + SIGTERM clean
- [x] `/healthz` returns `HTTP 200` with `status: "warn"` body (BUG-6 invariant)
- [x] `/health` returns `HTTP 200` with `status: "warn"` (warn ≠ fail → not 503)
- [x] `serviceId` differentiates `polyarb-l2` from `polyarb-l1`
- [x] `_build_l2_health_checks` helper exists and feeds both endpoints (Phase 02.1 P5)
- [x] P9 server-started gate present (Phase 02 L5)
- [x] No L1 cross-pollination (T-03-03-03)
- [x] Body never leaks db_path / secret / dsn / service_role (T-03-03-06)
- [x] 14/14 new tests GREEN (6 startup + 8 health)
- [ ] (deferred to user) Fly volume created + deployed + Better Stack monitor added + Sentry events confirmed

## Self-Check: PASSED

- All 9 listed files exist on disk (verified post-write)
- 5 task commits + 1 skeleton commit landed on `main` (verified via `git log --oneline`)
- All 9 truth gates from frontmatter `must_haves` pass programmatically
- Live boot evidence captured (start + SIGTERM clean shutdown in ~127ms)
- No code violates CLAUDE.md rules: lockfile-discipline (uv), file organization, no root file writes
