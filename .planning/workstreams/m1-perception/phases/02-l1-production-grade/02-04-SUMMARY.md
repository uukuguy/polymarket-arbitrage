---
phase: 02-l1-production-grade
plan: 04
subsystem: infra
tags: [docker, fly.io, github-actions, deploy, supercronic]

requires:
  - phase: 02-02
    provides: "HTTP daemon (Starlette + uvicorn + scheduler)"
  - phase: 02-03
    provides: "Supabase mirror + R2 sync + Alembic"
  - phase: 02-08
    provides: "F-01..F-05 retro fix-ups (init_schema migration, mirror update, daemon shutdown)"
provides:
  - "Dockerfile multi-stage uv build (non-root UID 10001, Supercronic cron)"
  - "fly.toml AMS region + volume + [http_service] health check + [[vm]] 256MB"
  - "GHA ci.yml (PR test gate) + deploy.yml (main-push flyctl deploy)"
  - "First production deploy: polyarb-l1.fly.dev /health = pass"
  - "6 Makefile targets: docker-build, docker-run-local, docker-smoke, deploy, smoke-test, tail-logs"
affects: [02-05, 02-06, 02-07]

tech-stack:
  added: [supercronic, flyctl, github-actions]
  patterns: [multi-stage-docker-uv, fly-process-groups, server-started-gate]

key-files:
  created:
    - Dockerfile
    - .dockerignore
    - fly.toml
    - crontab
    - .github/workflows/ci.yml
    - .github/workflows/deploy.yml
    - scripts/deploy_smoke.sh
    - tests/m1-perception/test_docker_smoke.sh
    - README.md
  modified:
    - Makefile
    - .env.example
    - src/polyarb/daemon/main.py
    - src/polyarb/daemon/scheduler.py
    - src/polyarb/clients/gamma_client.py
    - src/polyarb/snapshot/orchestrator.py

key-decisions:
  - "D-22 amendment: /scan + /health both public Anycast; HMAC middleware is auth gate (Flycast cross-org infeasible)"
  - "Supercronic process group (W8): fly.toml [processes] app + cron; Fly 2026-05 has no native cron syntax"
  - "fly.toml uses [http_service] not legacy [[services]] (flyctl config validate requirement)"
  - "256MB VM after memory fix — strip unused Gamma fields in paginator + del raw dicts after normalize"
  - "scheduler.run() gates on uvicorn server.started before first tick"
  - "Gamma 422 offset cap → graceful stop, return partial data"

patterns-established:
  - "server-started-gate: main.py waits for uvicorn Server.started before spawning scheduler task"
  - "field-stripping-paginator: _paginate(keep_fields=...) drops unused API fields per page to control memory"
  - "asyncio-yield-per-page: asyncio.sleep(0) after each paginated fetch to keep health check responsive"

requirements-completed:
  - "Multi-stage Dockerfile with uv 0.5 + non-root user UID 10001 + healthcheck"
  - "fly.toml AMS region + Volume mount + scheduled machines (Supercronic)"
  - "GHA ci.yml test gate + deploy.yml flyctl deploy + concurrency control"
  - "/scan + /health public on Fly Anycast (D-22 amendment)"

duration: 240min
completed: 2026-05-15
---

# Plan 04: Dockerfile + Fly.io deploy + GHA CI/CD

**First production deploy of polyarb-l1.fly.dev on 256MB Fly VM — daemon → Gamma fetch → SQLite → Parquet → Supabase mirror → R2 upload 全链路云上跑通。**

## What Was Built

1. **Dockerfile** — multi-stage uv build, python:3.12-slim-bookworm, non-root UID 10001, Supercronic for cron, HEALTHCHECK curl /health
2. **fly.toml** — AMS region, 5GB volume at /data, `[http_service]` with 120s grace period, `[[vm]]` 256MB app + 256MB cron
3. **crontab** — subset 2x/day (0,12 UTC), full 1/week (Sun 02:00), purge 1/week (Sun 04:00)
4. **GHA ci.yml** — PR + push main: ruff + pytest + planning-status
5. **GHA deploy.yml** — push main: flyctl deploy --remote-only + post-deploy /health smoke
6. **Makefile** — 6 new targets (docker-build, docker-run-local, docker-smoke, deploy, smoke-test, tail-logs)

## Deploy 调试期发现与修复（8 个 fix commits）

| Issue | Root Cause | Fix | Commit |
|-------|-----------|-----|--------|
| fly.toml invalid | `[restart]` not valid + `[[services]]` is legacy | `[http_service]` + `[[checks]]` array | af88308 |
| OOM 256MB | Paginator 堆 20k+ full Gamma dicts in memory (50+ fields/obj) | **strip to ~15 fields per page + del raw after normalize** | 1a97200 |
| OOM 512MB | Same root cause; synthetic profiling underestimated real payload | Same fix above | 1a97200 |
| Health check timeout | scheduler._tick() monopolizes event loop; uvicorn never binds socket | main.py gates on `server.started` before starting scheduler | 9e822ca |
| Health check timeout | httpx HTTP/2 pages return in ~40ms, no yield | `asyncio.sleep(0)` after each page | ecf38b3 |
| Gamma 422 crash | Polymarket offset>10000 cap → _NonRetryableHTTPError | Catch 422 in _paginate, return partial data | 0bb362c |
| flyctl deploy timeout | CN network → Fly API connection instability | grace_period 30s→120s; flyctl timeout is CLI-side, not blocking | 9735737 |

## 关键教训

1. **修代码不是加内存** — 三次升内存（256→512→1024→2048）都是错误方向。根因是 paginator 保留了 50+ 字段的完整 API 响应。strip 到 15 字段后 256MB 够用。
2. **本地 profiling 不代表生产** — 合成 20-field fake dicts vs 真实 50+ field nested JSON，内存差 5 倍。应该用真实 API 数据 profile。
3. **asyncio 协作调度不是免费的** — `await httpx.get()` 返回太快时其他 coroutine 拿不到 cycle。需要显式 `asyncio.sleep(0)` yield。
4. **Fly microVM 可用内存 ≠ 分配内存** — 256MB 分配 → ~150MB 可用（kernel/init 占 ~100MB）。

## Verification

```
$ curl -sS https://polyarb-l1.fly.dev/health | python -m json.tool
{
  "status": "pass",
  "checks": {
    "snapshot:last_status": [{"observedValue": "OK", "status": "pass"}],
    "supabase:mirror_age_seconds": [{"observedValue": 2023.5, "status": "pass"}],
    "r2:upload_recent_success": [{"observedValue": true, "status": "pass"}]
  }
}
```

```
$ flyctl status -a polyarb-l1
  app     6830939c0070d8  started  1 total, 1 passing  shared-cpu-1x@256MB
  cron    8e2909a77ddd08  started
```

## Self-Check: PASSED

- [x] Dockerfile builds, non-root UID 10001
- [x] fly.toml validates (`flyctl config validate`)
- [x] GHA workflows YAML-valid
- [x] First prod deploy: /health returns pass
- [x] Supabase mirror + R2 upload confirmed in /health checks
- [x] 256MB VM — no OOM after memory fix
- [x] 459 tests pass (3 pre-existing deselected)
