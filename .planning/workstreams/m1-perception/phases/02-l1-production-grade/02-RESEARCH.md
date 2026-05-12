# Phase 02: L1 production-grade long-running - Research

**Researched:** 2026-05-12
**Domain:** Cloud-native Python daemon deployment (Fly.io + Supabase + R2 + Vercel) + production-grade L1 observation hardening
**Confidence:** HIGH (locked decisions verified against thread + recent docs; agent's-Discretion items resolved via Context7-class lookups + cross-source verification)

## Summary

Phase 02 把 Phase 01.1 的 L1 观察工具从「本地研发期单次跑通」升级为「云上 7×24 自主跑 + 健康监控 + 一键部署」。22 个 CONTEXT 决策已锁（Fly.io AMS / Supabase Pro Dublin / Cloudflare R2 / Axiom + Sentry + Better Stack / Vercel Next.js dashboard / scan trigger via Fly internal endpoint）。本文给出 7 个 agent's-Discretion 子问题的可落地推荐 + 横向 Validation/Security 节，配合 thread §1（L1 生产级判定标准）+ Phase 01.1 LEARNINGS 全部 14D/12L/10P/8S 一起读。

**Primary recommendation（一句话）**：L1 daemon 主写本地 SQLite + R2 Parquet（保留 Phase 01.1 atomic 写入路径），单独走 **post-write 异步 push 到 Supabase 子集表**（dashboard-friendly schema, not 1:1 mirror），用 Alembic 单库管理 Supabase schema、保持 SQLite DDL 在 `schemas.py` 内不动；HTTP server 用 **Starlette + uvicorn**（不上 FastAPI 重量级 OpenAPI 栈，daemon 内嵌 2 个 endpoint 即可）；Dockerfile 走 astral 官方 `uv` multi-stage 范本；GHA 单 workflow `deploy.yml` 走 pytest gate → `superfly/flyctl-actions/setup-flyctl` → `flyctl deploy --remote-only` + `concurrency: deploy-{branch}`；`/health` 按 IETF draft-inadarei-api-health-check 三态返回（pass/warn/fail）+ Better Stack 用 HTTP 状态码 + Keyword Monitor 双判（200 + status:pass = healthy / 200 + status:warn = degraded / 503 = fail）；框架抽象 A **最小可行版本** = 把 `fetched_at_ms` 重命名为 `stage_completed_at_ms` 并加一个 nullable `page_fetched_at_ms` 列（推迟完整 Market State dataclass 到 02.1）。

## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|---|---|---|---|
| L1 cron snapshot daemon | Fly Machine (AMS) | — | 长跑容器 + scheduled machines；离 Polymarket London ~10ms（thread §0.1.1） |
| Snapshot SQLite write | Fly Machine + persistent Volume | — | 单 writer / WAL mode 已在 Phase 01.1 工作；云上不变（D-02 决定 Supabase 作 secondary mirror） |
| Snapshot Parquet long-term archive | Cloudflare R2 (S3 API via boto3) | Fly Volume (热) | 零 egress + $0.015/GB（业内最便宜 cold storage）；Fly Volume 只留滚动 30 天 |
| Dashboard read query | Vercel Next.js App Router | Supabase JS SDK pg_rest | D-19 锁；CSR + RSC 都走 supabase-js |
| scan trigger | Vercel Edge Function → Fly internal `/scan` POST | Fly daemon FastAPI/Starlette | D-21 锁；不在 Supabase 重做 4 层 SQL 防御（违反 P1 trust-split 复用） |
| Health check ping | Better Stack Free (30s) | daemon `/health` GET | D-16 锁；返回 IETF health-check JSON |
| Log forwarding | Axiom Free (500GB/月) | Fly stdout vector 转发 | D-14 锁 |
| Error tracking | Sentry Developer Free | loguru → sentry_sdk loguru integration | D-15 锁 |
| Alert push | Telegram bot via Better Stack | email 打底 | D-17 锁 |
| CI/CD | GitHub Actions | superfly/flyctl-actions | D-06 锁 |
| Secrets | flyctl secrets set + GHA secrets (deploy token only) | — | D-07 锁 |

---

## User Constraints (from CONTEXT.md)

### Locked Decisions（22 项 D-01..D-22）

**Deployment Stack（D-01..D-04）**

- **D-01:** Compute = **Fly.io** AMS region（trading-readiness ★★★★★，未来 M3 不用换栈）[VERIFIED: thread §2.1.7]
- **D-02:** Database = **Supabase Pro** Dublin region（免费 tier 起步，撞 500MB/1 周不写入触发器升 Pro $25/月）[VERIFIED: thread §0.1]
- **D-03:** Object storage = **Cloudflare R2** Free tier（10GB/月免费，零 egress）[VERIFIED: Cloudflare R2 pricing 2026-05]
- **D-04:** Region = all-eu（Fly AMS / Supabase Dublin / R2 anycast）

**Environment / CI/CD（D-05..D-08）**

- **D-05:** **单 environment 起步**（先只跑 prod；staging 在 schema 变更或 P1+ 需要时后期补）
- **D-06:** CI/CD = **GHA build + flyctl deploy**
- **D-07:** Secrets = **flyctl secrets set + GHA secrets 只放 deploy token**
- **D-08:** Container = **Docker multi-stage（uv install + python app）**

**L1 Cadence（D-09..D-11）**

- **D-09:** **subset 每天 2 次**（UTC 凌晨 + 中午 12h 间隔）
- **D-10:** **full 每周 1 次**（周日 UTC 凌晨）
- **D-11:** 调度 = **Fly cron**（fly.toml `[mounts]` + scheduled tasks）

**Failure Handling（D-12..D-13）**

- **D-12:** 单次失败三档（OK/DEGRADED/FAILED）已在 amendment 24f52ba 落地
- **D-13:** **连续 3 次失败 → 暂停 daemon + 告警**

**Observability（D-14..D-17）**

- **D-14:** Log = **Axiom Free**（500GB/月 ingest + 30 天 retention）
- **D-15:** Error = **Sentry Developer Free**（5k errors/月）
- **D-16:** Uptime = **Better Stack Free**（10 monitor × 30s）+ daemon `/health` endpoint
- **D-17:** Alert = **Telegram bot（Better Stack 原生）+ email 双遡**

**Dashboard（D-18..D-22）**

- **D-18:** Dashboard = **read-only + scan trigger**（4-5 页）
- **D-19:** 前端栈 = **Vercel Next.js App Router + Supabase JS SDK**
- **D-20:** 认证 = **Supabase Auth magic link + email whitelist 单用户**
- **D-21:** scan trigger = **Vercel Edge Function POST → Fly daemon `/scan` endpoint**（复用 Phase 01.1 4 层防御 + Trust-split）
- **D-22:** Fly daemon 暴露端口 = **Fly internal network only（`<app>.internal`）+ HTTPS via Fly Anycast**

### Claude's Discretion（researcher 必须给推荐）

详见下面 7 个章节（§3-§9）。

### Deferred Ideas（OUT OF SCOPE）

**推到 Phase 02.1 / Phase 03+**
- watchlist 编辑能力（Phase 02 仅显示）
- WebSocket 增量数据流（推到 Phase 3）
- L2 定向跟踪 daemon（thread §1 纪律）
- PR 预览环境（Fly preview apps）

**推到 M3 实盘前**
- AWS KMS 签名链路
- Tiger Cloud 双库
- 私网出站 + 固定 IP 白名单（Fly $3.60/月 dedicated egress）

**推到 M5 工业化**
- 多 region failover
- 完整 metrics 体系（Prometheus / Grafana Cloud Pro）

---

## Phase Requirements（无 REQ-ID，按 CONTEXT D-XX 对照）

| ID | Description | Research Support |
|---|---|---|
| D-01..D-04 | Deployment stack (Fly AMS / Supabase Dublin / R2 / all-eu) | §10 Sources（thread §0.1 + §2 已锁，本文不重新调研） |
| D-05..D-08 | CI/CD (GHA + flyctl + uv multi-stage Dockerfile) | §6 Dockerfile + §7 GHA workflow |
| D-09..D-11 | L1 cadence (subset 2/day, full 1/week, Fly cron) | §4 保留策略 cron 落地 |
| D-12..D-13 | Failure handling (三态 + 连续 3 次暂停) | §8 /health endpoint schema + Validation Architecture |
| D-14..D-17 | Observability (Axiom + Sentry + Better Stack + Telegram) | §8 /health + §11 Validation + §12 Security |
| D-18..D-22 | Dashboard (Vercel + Supabase Auth + scan trigger via Fly internal) | §3 DB schema 双端 + §9 FastAPI vs Starlette |
| 跨 D | 框架抽象 A 落地范围 | §5 |
| 跨 D | DB schema 双端同步路径 | §3 |

---

## Standard Stack

### Core（新增到 pyproject.toml）

| Library | Version (verified 2026-05-12) | Purpose | Why Standard |
|---|---|---|---|
| `starlette` | 0.49.3 [VERIFIED: PyPI] | ASGI HTTP server framework | FastAPI 的底层；不需要 OpenAPI / Pydantic schema 自动化时直接用更轻 [CITED: Starlette docs + leapcell.medium.com "FastAPI is Overkill"] |
| `uvicorn` | 0.39.0 [VERIFIED: PyPI] | ASGI server runner | Starlette/FastAPI 官方推荐 |
| `sentry-sdk` | 2.59.0 [VERIFIED: PyPI] | error tracking | D-15 锁；loguru integration 自动启用 [CITED: docs.sentry.io/platforms/python/integrations/loguru] |
| `supabase` | 2.30.0 [VERIFIED: PyPI] | Supabase Python SDK | 写 mirror 表 + Realtime 订阅（推迟） |
| `boto3` | 1.42.97 [VERIFIED: PyPI] | S3 API client → R2 | R2 100% S3-compatible（PutObject / ListObjectsV2）[CITED: Cloudflare R2 docs] |
| `alembic` | 1.16.5 [VERIFIED: PyPI] | Postgres schema migrations | Supabase schema 走 Alembic（不管 SQLite）[CITED: alembic.sqlalchemy.org] |

### Supporting

| Library | Version | Purpose | When to Use |
|---|---|---|---|
| `httpx` (已有) | 0.27.x | HTTP client | Vercel → Fly `/scan` call from Edge Function 不用 Python 客户端，但 daemon 自己 health-check Supabase 需要 |
| `python-dateutil` | latest | ISO8601 timestamp parsing in health JSON | `/health` 返回的 ISO8601 timestamp 字段 |

**Installation:**

```bash
uv add starlette uvicorn[standard] sentry-sdk supabase boto3 alembic
```

⚠️ **不要用 pip install** — 项目纪律：`uv add` 自动改 pyproject + 更新 lock + 装包（CLAUDE.md §7 + thread `engineering-discipline`）。

### Alternatives Considered

| Instead of | Could Use | Tradeoff |
|---|---|---|
| Starlette | FastAPI | FastAPI 自带 OpenAPI/Swagger UI + Pydantic body validation。但 L1 daemon 只需 2 个 endpoint (`/health` + `/scan`)，OpenAPI 是负担。FastAPI 多 ~3MB 镜像 + 启动慢。**选 Starlette。** [CITED: leapcell.medium.com "FastAPI is Overkill: Starlette and Pydantic Are All You Really Need"] |
| Starlette | Flask | Flask 是 WSGI（同步）；daemon 已经走 asyncio loop（snapshot orchestrator）混合栈麻烦。**Starlette 同 ASGI 内同进程更顺。** |
| Alembic (Postgres only) | pgloader 一次性迁移 + SQL DDL 手维护 | pgloader 适合一次性"SQLite snapshot → Postgres" 迁移（[CITED: pgloader.readthedocs.io](https://pgloader.readthedocs.io/en/latest/ref/sqlite.html)），但 Phase 02 需要"daemon 长跑双端"，schema 演进必须工具化。Alembic batch_alter_table 支持 SQLite + Postgres 双方言；本项目只用 Postgres 分支即可（SQLite schema 保持手维护在 `schemas.py`） |
| boto3 (R2) | `r2-uploader` / `cloudflare` SDK | R2 完全 S3-compatible，boto3 是业内标准，无需额外 SDK。endpoint 指向 `https://<account>.r2.cloudflarestorage.com` 即可 |

**Version verification（验证日期 2026-05-12）**：

```bash
uv pip install --dry-run starlette uvicorn sentry-sdk supabase boto3 alembic
# 或：
pip index versions starlette  # → 0.49.3 confirmed
pip index versions sentry-sdk # → 2.59.0
pip index versions supabase   # → 2.30.0
pip index versions boto3      # → 1.42.97
pip index versions alembic    # → 1.16.5
pip index versions uvicorn    # → 0.39.0
```

---

## Architecture Patterns

### System Architecture Diagram

```
                         ┌─────────────────────────────────────────┐
                         │     Polymarket Gamma + CLOB API         │
                         │       (AWS eu-west-2 London)            │
                         └─────────────────────────────────────────┘
                                            │ HTTPS pull
                                            │ ~10ms (AMS → LON)
                                            ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │                Fly.io AMS  —  app: polyarb-l1                      │
   │ ┌────────────────────────────────────────────────────────────┐    │
   │ │  Python daemon process (uv venv, Python 3.12)              │    │
   │ │                                                            │    │
   │ │ ┌─────────────────────────┐  ┌─────────────────────────┐  │    │
   │ │ │ Snapshot orchestrator   │  │  Starlette HTTP server  │  │    │
   │ │ │ (cron: 2x/day subset,   │  │  uvicorn on :8080       │  │    │
   │ │ │  1x/week full)          │  │  GET  /health           │  │    │
   │ │ │ Phase 01.1 8-step pipe  │  │  POST /scan             │  │    │
   │ │ │ 三态 OK/DEGRADED/FAILED │  │  (Fly internal only)    │  │    │
   │ │ └────────────────────────┘  └─────────────────────────┘  │    │
   │ │           │                            │                   │    │
   │ │           ▼                            ▼                   │    │
   │ │  ┌──────────────────┐         ┌────────────────────┐     │    │
   │ │  │  SQLite WAL      │ ◄─R─────│  scanner.py        │     │    │
   │ │  │  + Parquet local │         │  4 layer SQL def   │     │    │
   │ │  │  (Fly Volume)    │─async──►│  Trust-split       │     │    │
   │ │  └──────────────────┘  push   └────────────────────┘     │    │
   │ │           │                                                │    │
   │ │           ▼ (post-write supabase-py mirror)               │    │
   │ │  ┌──────────────────┐  ▲                                  │    │
   │ │  │ Supabase write   │  │                                  │    │
   │ │  │ subset table     │  │                                  │    │
   │ │  └──────────────────┘  │                                  │    │
   │ │           │            │ Telegram + email                 │    │
   │ │           │            │ ▲                                │    │
   │ │           │            │ │ alert webhook                  │    │
   │ │  ┌────────▼──────┐  ┌──────────────┐                     │    │
   │ │  │ Sentry SDK    │  │ Better Stack │                     │    │
   │ │  │ (exception)   │  │ monitor 30s  │                     │    │
   │ │  └───────────────┘  └──────────────┘                     │    │
   │ │           │                  │                            │    │
   │ │ stdout (JSON)               GET /health                   │    │
   │ │           │                  │                            │    │
   │ └───────────┼──────────────────┼────────────────────────────┘    │
   │             ▼                  │                                  │
   │       ┌──────────┐             │                                  │
   │       │ Axiom    │             │                                  │
   │       │ 500GB/mo │             │                                  │
   │       └──────────┘             │                                  │
   └────────────────────────────────┼──────────────────────────────────┘
                                    │
                                    │  (Fly internal `.internal`)
                                    │
   ┌────────────────────────────────┴───────────────────────────────────┐
   │                  Cloudflare R2 (anycast)                           │
   │  Parquet long-term archive: /snapshots/YYYY/MM/DD/HH-MM-SS.parquet │
   │  $0.015/GB storage, $0 egress                                      │
   └────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │              Supabase Pro (Dublin, eu-west-1)                      │
   │  Tables (subset mirror): snapshots, markets_latest, events_latest, │
   │           validation_issues, recipe_runs (scan results)            │
   │  Auth: magic-link email whitelist                                  │
   └────────────────────────────────────────────────────────────────────┘
                                    ▲
                                    │ supabase-js (anon key, RLS)
                                    │
   ┌────────────────────────────────┴───────────────────────────────────┐
   │                Vercel Next.js App Router (global anycast)          │
   │  Pages: /status (L1 timeline) / /movers / /alerts / /scan          │
   │  scan POST → Edge Function → Fly internal `/scan` endpoint         │
   └────────────────────────────────────────────────────────────────────┘
```

### Recommended Project Structure（Phase 02 新增 / 改造）

```
polymarket-arbitrage/
├── Dockerfile                        # NEW — multi-stage uv (§6)
├── fly.toml                          # NEW — Fly Machine config + cron (§4)
├── .dockerignore                     # NEW — exclude tests / data / .venv
├── .github/workflows/
│   ├── ci.yml                        # NEW — pytest gate (run on PR)
│   └── deploy.yml                    # NEW — flyctl deploy on main (§7)
├── alembic.ini                       # NEW — Supabase migrations
├── alembic/
│   └── versions/                     # NEW — Supabase schema migrations
├── src/polyarb/
│   ├── http/                         # NEW — Starlette app (§9)
│   │   ├── __init__.py
│   │   ├── app.py                    # Starlette routes + middleware
│   │   ├── health.py                 # /health endpoint logic
│   │   └── scan.py                   # /scan endpoint logic
│   ├── snapshot/orchestrator.py      # MODIFY — stage_completed_at_ms split (§5)
│   ├── snapshot/normalizer.py        # MODIFY — accept page_fetched_at_ms (§5)
│   ├── storage/
│   │   ├── schemas.py                # MODIFY — add page_fetched_at_ms nullable column
│   │   ├── sqlite_store.py           # KEEP (主写)
│   │   ├── parquet_writer.py         # KEEP
│   │   ├── r2_sync.py                # NEW — boto3 R2 client + sync (§4)
│   │   └── supabase_mirror.py        # NEW — post-write Supabase push (§3)
│   ├── daemon/                       # NEW — entrypoint + scheduler glue
│   │   ├── __init__.py
│   │   ├── main.py                   # asyncio.gather snapshot + http server
│   │   ├── scheduler.py              # subset/full schedule logic (cron-trigger compatible)
│   │   └── alerts.py                 # Telegram bot + Better Stack heartbeat
│   └── observability/
│       ├── logging.py                # NEW — loguru → JSON (Axiom-friendly)
│       └── sentry.py                 # NEW — sentry_sdk init + loguru integration
├── scripts/
│   ├── deploy_smoke.sh               # NEW — make smoke-test 末端验证
│   └── supabase_seed.py              # NEW — initial Supabase schema bootstrap
└── docs/learning/
    └── 08-生产化部署.md              # NEW — phase 02 学习文档（CLAUDE.md 强制）
```

### Pattern 1: HTTP server 与 asyncio cron 同进程共存

**What:** daemon 单进程跑两件事 — Starlette HTTP server（接 `/health` + `/scan`）+ snapshot cron 触发器。

**When to use:** L1 daemon 是 cron-triggered batch（每天 2 次）；为了 Fly health-check probe + dashboard scan trigger 必须额外暴露 HTTP；不值得拆两个 Machine（Fly 按机器付费）。

**Example:**

```python
# src/polyarb/daemon/main.py
# Source: Starlette docs (https://www.starlette.io/) + Phase 01.1 orchestrator pattern
import asyncio
import signal
import sys
from contextlib import asynccontextmanager

import uvicorn
from loguru import logger

from polyarb.http.app import create_app
from polyarb.daemon.scheduler import SnapshotScheduler
from polyarb.observability.sentry import init_sentry
from polyarb.observability.logging import init_logging


async def main() -> int:
    init_logging()       # loguru → stdout JSON for Axiom
    init_sentry()        # sentry_sdk + loguru integration

    scheduler = SnapshotScheduler()
    app = create_app(scheduler=scheduler)  # share scheduler ref for /scan + /health

    config = uvicorn.Config(
        app,
        host="0.0.0.0",  # Fly internal network only via fly.toml services
        port=8080,
        log_config=None,  # use loguru, not uvicorn's logger
        access_log=False, # Axiom doesn't need access logs at this volume
    )
    server = uvicorn.Server(config)

    # Run http server + scheduler concurrently
    stop_event = asyncio.Event()

    def _shutdown(sig):
        logger.info(f"received {sig.name}, shutting down")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    server_task = asyncio.create_task(server.serve())
    scheduler_task = asyncio.create_task(scheduler.run(stop_event))

    await stop_event.wait()
    server.should_exit = True
    await asyncio.gather(server_task, scheduler_task, return_exceptions=True)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
```

### Pattern 2: Post-write Supabase mirror（不是 dual-write）

**What:** SQLite + Parquet 是 source of truth；snapshot 完成后**异步**把 subset 数据 push 到 Supabase 子集表。

**When to use:** dashboard 只读 Supabase；本地 daemon 仍然是 SQLite 主写（保留 Phase 01.1 atomic 路径）。

**Example:**

```python
# src/polyarb/storage/supabase_mirror.py（草稿）
# Source: supabase.com/docs/reference/python/upsert + project pattern
from supabase import create_client, Client
from loguru import logger


class SupabaseMirror:
    def __init__(self, url: str, service_key: str) -> None:
        self._client: Client = create_client(url, service_key)

    def push_snapshot(self, snapshot_id: int, market_rows: list[dict]) -> None:
        """Push subset of columns to dashboard-friendly markets_latest table.

        Schema mirror is INTENTIONALLY narrow — only the 15 columns dashboard
        renders. snapshots table stays narrow too (id, taken_at, mode, status,
        market_count, parquet_url). Heavy columns (validation_issues raw_payload)
        stay in SQLite, queried on-demand via /scan endpoint.
        """
        try:
            # Upsert snapshots metadata
            self._client.table("snapshots").upsert({
                "id": snapshot_id,
                "taken_at_ms": ...,
                "status": ...,  # one of pass/warn/fail
                "market_count": len(market_rows),
            }).execute()

            # Bulk insert markets_latest (DELETE-then-INSERT semantic mirrors local)
            self._client.table("markets_latest").delete().neq("market_id", "").execute()
            # Chunk to avoid postgrest body size limit (default 1MB ~ 5k rows)
            for chunk in _chunk(market_rows, 1000):
                self._client.table("markets_latest").insert(chunk).execute()
        except Exception as e:
            # Mirror failure must NOT fail snapshot — log + Sentry but proceed
            logger.error(f"Supabase mirror push failed snapshot_id={snapshot_id}: {e}")
            # Sentry auto-captures via loguru integration
```

**关键纪律**：Supabase push 失败 ≠ snapshot 失败。SQLite 写入成功就标 OK；mirror 失败标 DEGRADED；连续 N 次 mirror 失败才告警。

### Pattern 3: IETF-compliant /health endpoint

**What:** 返回 `application/health+json` Content-Type + 三态 status + checks 子结构。

**When to use:** Better Stack 30s ping；区分 alive vs degraded vs stale。

**Example:** 见 §8。

### Anti-Patterns to Avoid

- **Dual-write 实时同步 SQLite + Supabase**：写入两个 store 各 5 秒，total 10 秒；任一失败处理逻辑指数级复杂。改 **post-write 异步 push**。
- **直接迁 Phase 01.1 全部 schema 到 Postgres**：snapshots / markets / events / event_tags / validation_issues / question_translations 6 表 + 11 索引 + 各种 PK 设计是 SQLite-specific。复刻成本 + 维护双 schema 不划算。改 **narrow mirror**（dashboard 实际用的 ~15 列）。
- **FastAPI 重栈 for 2 endpoints**：FastAPI 自动生成 OpenAPI / Swagger UI / Pydantic body validation 是 web service 优势，daemon 内嵌 2 个 endpoint 是 over-engineering。改 **Starlette + 手写 JSONResponse**。
- **GHA workflow 用 `pip install`**：违反项目纪律（CLAUDE.md §7）+ 不与 uv.lock 一致。改 **`uv sync --frozen --extra dev`**。
- **Dockerfile 用 root user 运行 app**：违反 OWASP ASVS V14 + 写文件时若被攻击者突破 sandbox 可以改任意系统文件。改 **`USER appuser`**。
- **`/health` 单字段 `{"status": "ok"}`**：Better Stack 只能看 HTTP 状态码，区分不出"daemon 活着但 8 小时没成功 snapshot"。改 **三态 IETF schema**。
- **scan endpoint 公开**：Vercel 是 public Web，Fly daemon 暴露 `/scan` 给 internet = 任何人可调用引发 SQL 注入测试 / DoS。改 **Fly internal network only**（D-22）+ **Authorization: Bearer + HMAC of body**。

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| ASGI HTTP server | 手写 socket + HTTP parser | `starlette` + `uvicorn` | HTTP/1.1 keep-alive / chunked / lifecycle / signal handling 全栈复杂 |
| S3-API client | 手写 SigV4 签名 + multipart upload | `boto3` 1.42+ | SigV4 实现错误会泄漏 secret access key 在 query string |
| Postgres migrations | 手写 SQL diff script | `alembic` 1.16+ | autogenerate + revision tree + downgrade 是必备工程化 |
| Cron scheduling | 手写 `while True: sleep(N)` | **Fly cron / scheduled machines**（D-11） | systemd timer / k8s CronJob / Fly scheduled machine 都已经处理 timezone / 错过窗口补跑 / 单实例锁 |
| Log JSON serialization | 手写 `dict → json.dumps` 包 loguru | **loguru `serialize=True` option** | loguru.add(sys.stdout, serialize=True) 自动产 JSON + 结构化字段 [CITED: dash0.com/guides/python-logging-with-loguru] |
| Sentry exception capture | 手写 try/except → POST | `sentry_sdk` 2.59 + **automatic loguru integration** | sentry-sdk 检测到 loguru 依赖自动 enable；level=ERROR 起进 events，INFO+ 进 breadcrumbs [CITED: docs.sentry.io/platforms/python/integrations/loguru] |
| Telegram bot 调用 | 手写 HTTP POST | **Better Stack 原生 Telegram integration**（D-17） | Better Stack 已经把 Telegram bot 接通；告警逻辑（rate-limit / dedupe / on-call）走它的 UI |
| Health check JSON schema | 手写自定义字段 | **IETF draft-inadarei-api-health-check-06** | 业内事实标准；通用 monitoring 工具自动解析 [CITED: datatracker.ietf.org] |

**Key insight**: 本 phase 是 infra + L1 hardening，不是写新业务逻辑。研究"业内主流的 path"比设计"我的特殊方案"价值大 10×。所有上面 7 项，**自己造的代价是把"3 天能上线"拖到"3 周还没稳定"**。

---

## Runtime State Inventory

> Phase 02 引入新的云端 stateful 服务（Supabase / R2 / Fly Machine）。下面是新增 runtime state，**不是迁移现有**。

| Category | Items Found | Action Required |
|---|---|---|
| Stored data | (1) Supabase Postgres tables (snapshots / markets_latest / events_latest / validation_issues / recipe_runs) — 新增 / (2) Cloudflare R2 bucket `polyarb-snapshots/{year}/{month}/{day}/*.parquet` — 新增 / (3) Fly Volume `/data/state.db` + `/data/snapshots/*.parquet` — 复制 Phase 01.1 local 路径 | 用 Alembic 初始化 Supabase schema；R2 bucket 通过 `wrangler` CLI 或控制台创建；Fly Volume 通过 `fly volumes create` 创建 |
| Live service config | (1) Better Stack monitor 配置（URL + 30s interval + grace 6s + Telegram + email destinations）— 新增 / (2) Axiom Dataset `polyarb-prod` + APL 查询书签 — 新增 / (3) Sentry project `polymarket-arbitrage` + Alert Rules — 新增 / (4) Vercel project + custom domain + env vars — 新增 / (5) Telegram bot via @BotFather + chat_id 绑定 Better Stack — 新增 | 手工通过各家控制台创建；记录 config 在 `docs/ops/`（不在 git，避免泄漏 URL/IDs）；Better Stack monitor 配置可走 API 自动化但启动期手建 |
| OS-registered state | (1) Fly app `polyarb-l1` 注册 + Machine ID — 通过 `flyctl apps create` 创建 / (2) Fly cron schedule（fly.toml `[[services]]` + `[checks]` + scheduled machines）/ (3) GitHub Actions workflow registered（自动 by file commit） | 由 deploy.yml 首次 push 自动注册；首次手工 `flyctl launch --no-deploy` 创建 app |
| Secrets / env vars | (1) `flyctl secrets set` — POLYMARKET_KEY / SUPABASE_URL / SUPABASE_SERVICE_KEY / R2_ACCESS_KEY_ID / R2_SECRET / R2_ENDPOINT / SENTRY_DSN / AXIOM_TOKEN / BETTER_STACK_HEARTBEAT_URL / SCAN_SHARED_SECRET / (2) GHA repo secrets — FLY_API_TOKEN（deploy token, 不与 prod app secrets 共享）/ (3) Vercel env vars — NEXT_PUBLIC_SUPABASE_URL / NEXT_PUBLIC_SUPABASE_ANON_KEY / SCAN_SHARED_SECRET / SCAN_ENDPOINT_URL | flyctl + GHA + Vercel 各自管；**.env.example** 列所有 keys 但**不写值**；D-07 锁 secrets 完全隔离 |
| Build artifacts | (1) Docker image registry — Fly 自家 registry，per-deploy 一份；保留最近 N 个供 rollback / (2) Python package wheel — uv build artifacts cache | Fly 自动管 image GC；本地构建产物在 `.dockerignore` 内 |

**Nothing found in category Stored data (local migration)**: None — Phase 02 不迁移现有 Phase 01.1 数据。云上首次部署后从空 SQLite 重新开始累积。第一次 snapshot 会在本地 Fly Volume + R2 + Supabase 三处同步落地。

---

# §3 — DB schema 双端同步路径（the agent's Discretion #1）

**问题**：L1 daemon 是直接写 Supabase Postgres，还是 SQLite 主写 + 同步到 Supabase？涉及 Phase 01.1 `sqlite_store.py` 改造范围。

## 候选方案

### 方案 A：纯 Supabase Postgres 替换 SQLite（"all-cloud"）

- 改造范围：`sqlite_store.py` 全部重写为 psycopg/asyncpg → Supabase；删 `state.db`；events / event_tags / markets / validation_issues / question_translations 6 表全迁。
- Alembic 管 schema。
- Parquet 仍写 R2（不在 Postgres 内存大对象）。

**优点**：单一 source of truth；dashboard 直接查 prod DB；没有同步延迟。

**缺点**：
- Phase 01.1 大量代码假设 SQLite WAL + `BEGIN IMMEDIATE` + `executemany`；改 Postgres 需要重写所有 storage / observation / scanner 路径。**风险评估：~1-2 周代码改动 + 50% 测试用例（~200 个）需要 mock Postgres**。
- 本地 dev 体验下降 — 没有 Postgres 就不能跑 snapshot；Phase 01.1 的"开 fixture 就能跑"消失。
- Supabase Free 限制：500MB DB（实测一次 subset snapshot ~20MB SQLite，10 次就满；upgrade Pro 触发提前）+ 1 周无访问暂停（Phase 02 启动期间断会触发）。
- **scanner.py 4 层 SQL 注入防御是 SQLite-specific**：`file:...?mode=ro` URI / `PRAGMA` 黑名单 / SQLite 表名引用规则 → Postgres 全不一样。**重新做 4 层防御 = 重新做 Phase 01.1 Plan 03 一遍**。违反 P1 复用纪律。

### 方案 B：SQLite 主写 + 异步 mirror 到 Supabase narrow 表（**推荐**）

- 改造范围：`sqlite_store.py` 不动；新增 `storage/supabase_mirror.py`；orchestrator step 7 写入 SQLite 成功后追加 step 7.5 push subset 列到 Supabase。
- Supabase schema = **dashboard 实际渲染的 narrow 视图**（snapshots / markets_latest / events_latest / validation_issues_summary / recipe_runs），不复刻 Phase 01.1 全表。
- scan endpoint 直接读 SQLite（D-21 锁定的 trust-split 复用）；dashboard 读 Supabase。

**优点**：
- Phase 01.1 代码 100% 保留；测试不动；fixture 路径完整可用。
- Supabase schema 可以为"dashboard 渲染" 优化（聚合后字段、enum status、ISO8601 时间字符串等）。
- mirror 失败不阻塞 snapshot 主路径（D-12 三态语义：mirror 失败 → 标 DEGRADED）。
- 本地 dev 不需要 Supabase（mirror 模块 dry-run mode 跳过）。
- scanner.py 4 层防御**完全保留**（dashboard scan trigger 也走 SQLite via daemon `/scan`，不在 Supabase 端重做）。

**缺点**：
- 两套 schema 维护成本：Phase 02 引入"SQLite DDL in `schemas.py` + Supabase DDL in Alembic"。但**narrow mirror 减少耦合**（mirror schema 不需要跟随 SQLite 列改动，除非确实要新加 dashboard 字段）。
- 数据有 5-10 秒同步延迟（snapshot 总时长 ~10min 内可忽略）。
- 跨 snapshot 漂移分析 / scan endpoint 都走 daemon（SQLite），Supabase 只服务 dashboard 简单读 — **dashboard 复杂查询要回 daemon**。

### 方案 C：纯 SQLite + Litestream replication 到 R2（"lazy cloud"）

- Litestream（[litestream.io](https://litestream.io)）持续把 SQLite WAL stream 到 S3-compatible 存储；dashboard 读 R2 上的 SQLite replica。
- 改造范围：SQLite 不动；加 Litestream sidecar。

**优点**：零代码改动；replica 一致性好；技术栈最薄。

**缺点**：
- Vercel Next.js 不能直接 query SQLite-on-R2（要起 daemon 接口）→ 失去 Supabase 直连优势 → D-19 锁定的 supabase-js 不起作用。
- 不支持 Supabase Auth magic-link（D-20）— dashboard 还得自建 auth。
- Supabase Realtime（未来推送 alert 到 dashboard）不可用。
- 违反 D-02（"Database = Supabase"已锁定）。

### 推荐：方案 B（SQLite 主写 + 异步 mirror）

**理由**：
1. **D-02 / D-19 / D-20 三个锁定决策强制 Supabase 在路径里**，方案 C 排除。
2. **方案 A 重写代价 vs 收益不成比例** — 重写 storage 层 + 4 层防御 + ~200 测试，换来"single source of truth"在 L1 阶段无实质价值（dashboard 是只读）。方案 B 多了 mirror 模块（~200 行）就完成。
3. **方案 B 与 Phase 01.1 P1 trust-split 复用纪律一致**：scanner.py 完全不动，scan endpoint 通过 Fly internal 调 daemon。

## 实施细节

**Supabase narrow schema**（Alembic initial migration 草稿）：

```python
# alembic/versions/001_initial_dashboard_schema.py
def upgrade():
    op.create_table(
        "snapshots",
        sa.Column("id", sa.Integer, primary_key=True),       # mirror SQLite id
        sa.Column("taken_at_ms", sa.BigInteger, nullable=False),
        sa.Column("finished_at_ms", sa.BigInteger, nullable=False),
        sa.Column("mode", sa.String(8), nullable=False),     # subset | full
        sa.Column("status", sa.String(8), nullable=False),   # pass | warn | fail
        sa.Column("market_count", sa.Integer, nullable=False),
        sa.Column("parquet_url", sa.Text),                   # R2 URL
        sa.Column("issue_count_by_layer", sa.JSON),          # {1: 0, 2: 3, 4: 12}
    )
    op.create_index("idx_snapshots_taken_at", "snapshots", ["taken_at_ms"])

    # markets_latest = ONLY the most-recent OK snapshot's markets, narrow columns
    op.create_table(
        "markets_latest",
        sa.Column("market_id", sa.Text, primary_key=True),
        sa.Column("question", sa.Text),
        sa.Column("slug", sa.Text),
        sa.Column("event_slug", sa.Text),
        sa.Column("mid_price", sa.Float),
        sa.Column("liquidity_usd", sa.Float),
        sa.Column("volume_usd", sa.Float),
        sa.Column("end_time_ms", sa.BigInteger),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("snapshots.id")),
        sa.Column("question_zh", sa.Text),  # joined from translations
    )

    # recipe_runs = scan endpoint results recorded for dashboard timeline
    op.create_table(
        "recipe_runs",
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("recipe_name", sa.String(64), nullable=False),
        sa.Column("triggered_by", sa.String(32)),  # cron | dashboard | manual
        sa.Column("run_at_ms", sa.BigInteger, nullable=False),
        sa.Column("result_count", sa.Integer),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("snapshots.id")),
    )

    # RLS policies (Supabase Auth-aware, anon读全权 + service-role写)
    op.execute("ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;")
    op.execute("CREATE POLICY anon_read ON snapshots FOR SELECT USING (true);")
    # similar for markets_latest, recipe_runs
```

## Consistency with locked decisions

- ✅ D-02 Supabase 锁定 — 方案 B 使用 Supabase
- ✅ D-19 Supabase JS SDK — dashboard 直接读 Supabase narrow 表
- ✅ D-21 scan trigger 复用 trust-split — dashboard scan POST 到 daemon，不在 Supabase 重做防御
- ✅ D-22 Fly internal — daemon 写 Supabase 走 public Postgres URL + service_role key (out-of-band)；不需要 Fly internal

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| Supabase Free 500MB 撞顶（一周大约 5-10 次 snapshot × 23k 行 × narrow 列 ~ 50MB） | 启动期跑 narrow schema 估算 < 100MB / 月；30 天 retention，老 snapshot 在 Supabase 端 prune（Alembic 第二个 migration 加 cron job 或 daemon 自己删） |
| Supabase Free 1 周无访问暂停 | daemon mirror 每天 2 次写入即"访问"，触发 keep-alive；测试期间手工 ping |
| mirror 失败时 snapshot 是否仍然算成功 | **是**。snapshot OK 的语义是 SQLite + Parquet 都写入成功（D-12 amendment 24f52ba）。mirror 失败 → 标 DEGRADED + Sentry 告警，不阻塞 cron 下一次 |
| Supabase service_role key 泄露 | flyctl secrets set 存；不进 git；GHA 仅有 deploy token，不能读 prod secrets（D-07）。**且 service_role 仅 daemon 用**，dashboard 走 anon_key + RLS |
| 数据漂移（SQLite 写入成功但 Supabase mirror 失败 → SQLite 有 snapshot=N，Supabase 没有） | mirror 写 `snapshots` 表是 upsert（idempotent）；启动时 daemon 跑 mirror reconcile：对比 last SQLite snapshot_id vs last Supabase snapshot_id，补差 |

## Affected file paths

- **MODIFY**：`src/polyarb/snapshot/orchestrator.py` — step 7 之后加 step 7.5 `mirror.push_snapshot(...)`
- **MODIFY**：`src/polyarb/storage/__init__.py` — export new mirror module
- **NEW**：`src/polyarb/storage/supabase_mirror.py` — `SupabaseMirror` class
- **NEW**：`alembic.ini` + `alembic/env.py` + `alembic/versions/001_initial.py`
- **NEW**：`scripts/supabase_seed.py` — 首次 bootstrap + reconcile script
- **MODIFY**：`pyproject.toml` — add `supabase`, `alembic`
- **MODIFY**：`Makefile` — add `make supabase-migrate` / `make supabase-reconcile`
- **NEW**：`tests/test_supabase_mirror.py` — mock supabase client + assert idempotent upsert

---

# §4 — Snapshot 保留策略 cron 落地（the agent's Discretion #2）

**问题**：云上需要默认调度策略（保留 N 个 / 按天清 / R2 archive 后清 SQLite？）。

## 候选方案

### 方案 A：30 天 SQLite + 永久 R2 archive（**推荐**）

- **SQLite + Parquet on Fly Volume**：保留最近 30 天（约 ~60 个 subset + ~4 个 full = 64 snapshots × ~50MB Parquet ~ 3.2GB + ~30MB SQLite = **3.5GB total**）
- **R2**：永久 archive（按 month 分目录，无 prune）
- **purge cron**：`make snapshots-purge DAYS=30 KEEP=10` 每周日跑（已存在 `snapshots-purge` Makefile target）
- R2 sync：每次 snapshot 完成后 `s3 cp` 到 `r2://polyarb-snapshots/{YYYY}/{MM}/{DD}/{HH-MM-SS}.parquet`

**优点**：
- Fly Volume Free tier 3GB 边界（[fly.io/docs/volumes/pricing](https://fly.io/docs/about/pricing)：$0.15/GB/月 above 3GB free）→ 3.5GB ≈ $0.075/月，可忽略
- R2 Free tier 10GB 免费 / month → 64 snapshots × 50MB = 3.2GB；3 个月才撞 10GB；4 个月起 = ($0.015 × 4GB) = $0.06/月
- 数据 hot path（最近 30 天）查询快（local SQLite）
- 数据 cold path（30+ 天）走 R2 + DuckDB `read_parquet('s3://...', s3_endpoint=...)` 可直接查（thread §1.5 抽象 B）

**缺点**：
- 30 天前的 snapshot 在 SQLite 表里删除 → 跨 snapshot 漂移分析（`make compare-snapshots`）只能用最近 30 天的 SQLite 数据 + 老数据要从 R2 用 DuckDB 拉
- R2 永久保留 → 12 个月 ~ 36GB ~ $0.45/月（仍可忽略，但要监控）

### 方案 B：7 天 SQLite + 90 天 R2 + 永久 cold archive

- Fly Volume 7 天（~ 750MB），更小
- R2 90 天 hot（fast access），90 天后转 Glacier-equivalent（但 R2 没有 tier 切换，全 hot）
- 不推荐：cold tier 切换 R2 没原生支持；增加 lifecycle 配置复杂度

### 方案 C：按 size threshold 清（"keep latest 30 OK + 5 latest DEGRADED"）

- 不按时间，按数量 + 状态分类
- 不推荐：策略复杂，dashboard 难解释；时间窗口对用户更直观

## 推荐：方案 A（30 天 SQLite + 永久 R2）

**理由**：
- thread §3.1 默认就推荐 "30 天 SQLite + R2 永久"
- Fly Volume 3GB free + R2 10GB free 完美匹配 30 天 retention 数据量
- 已有 `make snapshots-purge`（Phase 01.1 amendment）+ `snapshots-purge` SQLite store method — **零代码改动**

## 实施细节

**Fly cron schedule**（fly.toml 草稿）：

```toml
# fly.toml — scheduled machines (Phase 02)
app = "polyarb-l1"
primary_region = "ams"

[build]

[mounts]
  source = "polyarb_data"
  destination = "/data"
  initial_size = "5gb"          # 30 天 retention 留 buffer

[env]
  POLYARB_DATA_DIR = "/data"
  POLYARB_SNAPSHOT_DIR = "/data/snapshots"
  POLYARB_DB_PATH = "/data/state.db"

# Always-on machine for HTTP server (Starlette /health + /scan)
[[services]]
  internal_port = 8080
  protocol = "tcp"
  auto_stop_machines = false
  auto_start_machines = true
  min_machines_running = 1

  [[services.http_checks]]
    grace_period = "30s"
    interval = "30s"
    method = "GET"
    path = "/health"
    timeout = "5s"
    tls_skip_verify = false
    # health JSON parsing happens at Better Stack; Fly proxy only checks 2xx

# Subset snapshot cron — 2x/day UTC 00:00 + 12:00
[[services.processes]]
  name = "snapshot-subset"
  schedule = "0 0,12 * * *"     # UTC cron
  command = "uv run python -m polyarb.snapshot snapshot"

# Full snapshot cron — 1x/week UTC Sunday 02:00
[[services.processes]]
  name = "snapshot-full"
  schedule = "0 2 * * 0"
  command = "uv run python -m polyarb.snapshot snapshot --full"

# Purge cron — weekly Sunday UTC 04:00 (after full)
[[services.processes]]
  name = "snapshots-purge"
  schedule = "0 4 * * 0"
  command = "uv run python -m polyarb.snapshot snapshots-purge --older-than-days 30 --keep-last 10"

# R2 archive cron — every snapshot triggers a one-shot post-write upload (in orchestrator step 8)
# NOT a separate cron — handled inline by orchestrator to keep transactional semantics
```

⚠️ **重要**：Fly 的 `schedule` 字段是按 cron syntax 触发独立 Machine 跑；不是在 daemon 进程内调度。这与 thread §2.5.a 的"daemon 永远跑 + 内部调度"不同。**Phase 02 选择 Fly scheduled machines**（每次起一台短暂 Machine 跑 snapshot 然后退出），daemon 永久跑只负责 HTTP server。

**R2 sync 在 orchestrator step 7.6**（step 7 SQLite + step 7.5 mirror 之后）：

```python
# orchestrator.py step 7.6（新增）
from polyarb.storage.r2_sync import upload_parquet_to_r2

# After SQLite write + Supabase mirror, push parquet to R2
try:
    r2_url = upload_parquet_to_r2(
        parquet_path=local_parquet_path,
        bucket="polyarb-snapshots",
        key=f"{year}/{month:02d}/{day:02d}/{hour:02d}-{minute:02d}-{second:02d}.parquet",
    )
    # update snapshots.parquet_url in Supabase to R2 url (post-mirror)
    mirror.update_parquet_url(snapshot_id, r2_url)
except Exception as e:
    issues.append(Issue(layer=4, category=Category.UNKNOWN, detail=f"R2 upload failed: {str(e)[:200]}"))
    # 同样 mirror-style：失败不阻塞，只标 DEGRADED
```

## Consistency with locked decisions

- ✅ D-03 R2 锁定
- ✅ D-09 subset 2x/day（fly.toml cron）
- ✅ D-10 full 1x/week（fly.toml cron）
- ✅ D-11 Fly cron（fly.toml scheduled machines）

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| Fly scheduled machine 与 always-on machine 同时访问 SQLite WAL → 锁竞争 | scheduled machine 跑 snapshot 时**短暂** acquire WRITE 锁；always-on (HTTP server) 仅 read（`/scan` 是 read-only `file:...?mode=ro`）→ WAL mode 允许多 reader + 单 writer 不冲突；但 always-on 不能在 snapshot 进行中写入 SQLite。**实施时 always-on 进程只 read SQLite**，所有写入都来自 scheduled machine |
| R2 PUT 失败时下次 snapshot 重传问题 | 上传是 idempotent（同 key 覆盖）；snapshots 表 parquet_url 字段允许 NULL；下次 snapshot 完成时如果发现上一个 snapshot parquet_url 为 NULL，可以补传（reconcile 逻辑） |
| Fly Volume 升级（3GB → 10GB）时 downtime | Volumes 支持 in-place extend（`fly volumes extend`）无需 downtime；只有缩容才需要数据迁移 |
| R2 cost 失控（如果某个 bug 频繁 PUT/DELETE） | R2 Class A operations (PUT/DELETE) 限 1M/月 free；2 snapshots/day × 365 = 730 PUT/year，可忽略 |

## Affected file paths

- **NEW**：`fly.toml` — Fly Machine config + scheduled machines + volume mounts
- **NEW**：`src/polyarb/storage/r2_sync.py` — boto3 R2 client + atomic upload
- **MODIFY**：`src/polyarb/snapshot/orchestrator.py` — step 7.6 R2 sync
- **MODIFY**：`Makefile` — add `make r2-list` / `make r2-restore SNAPSHOT_ID=N`（dev convenience）
- **MODIFY**：existing `snapshots-purge` 接受默认 `DAYS=30 KEEP=10`（与 fly.toml cron 对齐）

---

# §5 — 框架抽象 A 落地范围（the agent's Discretion #3）

**问题**：thread §1.5 的"统一市场状态模型"（含 stamp 时间 vs 抓取时间显式分离）是 Phase 02 同步动还是 Phase 02.1？

## 现状（L2 已发现的 bug 引爆点）

- **L2 实证（LEARNINGS L2）**：`orchestrator.py:340-343` 在 stage 5 一次性 stamp `fetched_at_ms`；schema-level `COUNT(DISTINCT fetched_at_ms) = 1` per snapshot
- **下游危害**：任何"snapshot 内时间一致"分析都建立在虚假前提；99% 市场全天无 trade 让这个 bug 在 99% case 看不出来；1% 长尾市场恰好是策略目标人群（thread §2.1.a 实证 #3）
- **完整 Market State dataclass** 需要：source 字段 + quality_flags + 跨层级共享 dataclass + 全表 column rename — 工作量 ~1 周代码 + 测试

## 候选方案

### 方案 A：最小可行抽象（**推荐**）

只动两件事，**不引入新的 dataclass**：

1. **新增 `page_fetched_at_ms`（nullable INTEGER）列**：在 Gamma 翻页时 stamp 真实时间；normalizer.py 接收并写入；SQLite + Parquet 都加。
2. **`fetched_at_ms` 重命名为 `stage_completed_at_ms`**：让字段名诚实表达"这是 stage 5 stamp 时间"，**不是抓取时间**。
   - 由于这是破坏性 schema 改动，加 Alembic migration（Supabase）+ SQLite DDL bump 版本。
   - 旧 parquet 用 DuckDB `union_by_name=true` 自动 NULL 填充新列（P7 schema 演进硬约束已经允许加列，需要 column rename 走 SQL 重写）。
   - **更保守的方案**：保留 `fetched_at_ms` 列名（避免 schema 破坏），加 SCHEMA comment + 教学文档解释字段语义。

**优点**：
- 修 L2 已知 bug（schema 表达 stamp 时间 vs 抓取时间）
- 不引入新代码结构（dataclass / source enum / quality_flags 全推迟）
- ~50 行代码改动 + 5-10 个测试

**缺点**：
- 没有真正实现 thread §1.5 "统一 Market State"
- L2/L3 接入时还要 refactor 一遍

### 方案 B：完整 Market State dataclass + source enum + quality_flags

- 全套实现 thread §1.5 A
- ~1 周代码 + ~30 个测试更新
- L2/L3 阶段无 refactor

**优点**：长期收益清楚。

**缺点**：Phase 02 已经背着部署 / Dockerfile / GHA / Supabase mirror / Starlette / Better Stack / Sentry / Telegram 一堆新东西 — 再加 dataclass 重构是 over-commit。LEARNINGS S8 "Plan completeness 不等于 perfection"。

### 方案 C：完全推到 Phase 02.1（"不动"）

- Phase 02 只做部署 + 监控；data model 不动
- Phase 02.1 单独做 Market State dataclass

**缺点**：L2 修复（page_fetched_at_ms）valid 时间窗口拖长；下游 / dashboard 显示的"fetched_at_ms"继续误导用户

## 推荐：方案 A（page_fetched_at_ms 加列 + 字段语义修正，**保留旧字段名**）

**理由**：
1. **L2 修复是 schema-level 必须**（thread §2.1.a 结论 #1 "schema 层无法表达 snapshot 内时间不一致 — 这是抽象 A 的关键设计点"）
2. **不引入 dataclass 重构**（Phase 02 已经够重；LEARNINGS S8 节奏纪律）
3. **保留 `fetched_at_ms` 列名 + 加 schema-level comment + 教学文档**：避免破坏性 rename；让消费者明确知道这是 stage stamp。
4. **加 `page_fetched_at_ms` nullable**：抓取过程中按 page 记录；老 snapshot 这列 NULL（unioned-by-name 自动适配）

## 实施细节

```python
# schemas.py 改动
# ── BEFORE ──
"fetched_at_ms"   INTEGER NOT NULL,

# ── AFTER ──
"fetched_at_ms"      INTEGER NOT NULL,  -- semantic: stage 5 completion stamp (NOT per-row fetch time)
"page_fetched_at_ms" INTEGER,            -- new in Phase 02: real page fetch time (NULL for snapshots pre-02)
```

```python
# clients/gamma_client.py 改动
async def get_events(self, ...):
    for page_idx, page in enumerate(self._paginate(...)):
        page_fetched_at_ms = int(time.time() * 1000)
        for raw in page:
            raw["_page_fetched_at_ms"] = page_fetched_at_ms  # carried to normalizer
            yield raw
```

```python
# normalizer.py 改动 — accept page_fetched_at_ms
def normalize_market(raw: dict, ...) -> dict:
    return {
        ...
        "fetched_at_ms": None,  # placeholder, orchestrator stamps stage_completed_at_ms
        "page_fetched_at_ms": raw.get("_page_fetched_at_ms"),  # NEW
    }
```

```python
# orchestrator.py 改动
clob_done_ms = int(time.time() * 1000)
for m in target_markets:
    m["fetched_at_ms"] = clob_done_ms  # semantic kept BUT documented as stage stamp
    # m["page_fetched_at_ms"] 来自 normalizer（保持原值）
```

```python
# 教学文档增量（docs/learning/08-生产化部署.md）
"""
关键字段语义（Phase 02 起）:
- fetched_at_ms        = stage 5 完成时戳，所有 row 同值 (≠ 抓取时间！)
- page_fetched_at_ms   = Gamma 翻页时该页的真实 stamp，per-page 不同
- snapshot_taken_at_ms = parquet-only，整个 snapshot 启动时戳
- finished_at_ms       = snapshot 完成时戳（SQLite snapshots 表）
"""
```

**完整 Market State dataclass 推迟到 Phase 02.1**（独立 phase，与 L2 daemon 开工同步动）。

## Consistency with locked decisions

- ✅ thread §1.5 抽象 A 部分启动（page-level 时间分离）
- ✅ LEARNINGS L2 修复

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| 老 parquet 文件读取 `union_by_name=true` 时 page_fetched_at_ms 列 NULL 填充 | DuckDB 默认行为；scanner.py / track-market.py 等下游已经习惯 NULL coalesce |
| Gamma 翻页 N 页 → normalizer 也要传递 page 来源 | 在 raw dict 加 `_page_fetched_at_ms` private key（下划线表示 internal），normalizer 转写到正式列；不污染 Polymarket 真实字段 |
| events 表的 fetched_at_ms 与 markets 表语义同步 | events 表本来也只是 stage 5 stamp（同 L2 bug），同样改 events.fetched_at_ms 含义 + 加 events.page_fetched_at_ms |

## Affected file paths

- **MODIFY**：`src/polyarb/storage/schemas.py` — events / markets DDL + parquet schema + MARKETS_COLUMN_ORDER
- **MODIFY**：`src/polyarb/snapshot/normalizer.py` — accept + stamp `page_fetched_at_ms`
- **MODIFY**：`src/polyarb/clients/gamma_client.py` — per-page timestamp emission
- **MODIFY**：`src/polyarb/storage/sqlite_store.py` — `_row_to_tuple` column order 同步
- **NEW**：`tests/test_page_fetched_at_ms.py` — unit test page stamp 不同值
- **NEW**：`docs/learning/08-生产化部署.md` — phase 02 教学（含字段语义节）

---

# §6 — Dockerfile multi-stage 细节（the agent's Discretion #4）

**问题**：uv 缓存层 / non-root user / healthcheck command / multi-arch build for Fly。

## 推荐：astral 官方 uv multi-stage 范本 + Fly slim-base + non-root

来源（[CITED: docs.astral.sh/uv/guides/integration/docker/](https://docs.astral.sh/uv/guides/integration/docker/) + [fly.io/docs/python/the-basics/multi-stage-builds/](https://fly.io/docs/python/the-basics/multi-stage-builds/) 2026-05 访问）：

```dockerfile
# Dockerfile — Phase 02 production-ready
# Source: docs.astral.sh/uv/guides/integration/docker/ (verified 2026-05-12)

# ───── Builder stage ─────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS builder

# Copy uv binary from official astral image (pinned version for reproducibility)
COPY --from=ghcr.io/astral-sh/uv:0.5.0 /uv /uvx /bin/

# uv production env vars
ENV UV_COMPILE_BYTECODE=1     \
    UV_LINK_MODE=copy         \
    UV_PYTHON_DOWNLOADS=0     \
    UV_NO_DEV=1               \
    PYTHONUNBUFFERED=1        \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (cached layer) — bind mounts avoid copying lockfile into image
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-editable

# Copy source code AFTER deps (so code changes don't bust the deps layer)
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-editable

# ───── Runtime stage ─────────────────────────────────────────────────
FROM python:3.12-slim-bookworm AS runtime

# Minimal runtime deps: ca-certificates for HTTPS to Polymarket, tzdata for UTC cron
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        tzdata \
        curl \  # for fly machine healthcheck convenience
    && rm -rf /var/lib/apt/lists/*

# Non-root user (UID 10001 follows ASVS V14.1.1 — distroless-style numeric UID)
RUN groupadd --system --gid 10001 polyarb \
    && useradd --system --uid 10001 --gid polyarb --no-create-home polyarb

# Copy ONLY the venv from builder (not source) — image stays slim
COPY --from=builder --chown=polyarb:polyarb /app/.venv /app/.venv
COPY --chown=polyarb:polyarb src/ /app/src/

# Data dir for Fly Volume mount (mode 0700 = only daemon process can read/write)
RUN mkdir -p /data /app/logs && chown -R polyarb:polyarb /data /app/logs

ENV PATH="/app/.venv/bin:$PATH"        \
    PYTHONPATH="/app/src"              \
    PYTHONUNBUFFERED=1                 \
    POLYARB_DATA_DIR=/data             \
    POLYARB_LOG_DIR=/app/logs          \
    TZ=UTC

WORKDIR /app

USER polyarb

# Health probe Fly uses (matches fly.toml http_checks.path = "/health")
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:8080/health || exit 1

EXPOSE 8080

# Default command = always-on daemon (HTTP server + scheduler loop).
# Scheduled machines override with `flyctl machine run` + custom command.
CMD ["python", "-m", "polyarb.daemon.main"]
```

## 关键设计取舍

| 取舍 | 选择 | 理由 |
|---|---|---|
| Base image | `python:3.12-slim-bookworm` | Slim variant ≈ 80MB vs Debian full ≈ 130MB；distroless 可推但 Fly debug 体验差 |
| uv 版本 pin | `ghcr.io/astral-sh/uv:0.5.0` | 不用 `:latest` — 镜像版本浮动会破坏 reproducibility [CITED: depot.dev "Optimal Dockerfile for Python with uv"] |
| `UV_COMPILE_BYTECODE=1` | 启用 | "improves startup time" 生产环境必开 [CITED: docs.astral.sh/uv/guides/integration/docker/] |
| `UV_LINK_MODE=copy` | 启用 | cache mount 跨 filesystem 时 hardlink 会失败 [CITED: 同上] |
| `UV_NO_DEV=1` | 启用 | 排除 pytest / freezegun / respx 等 dev deps |
| Multi-arch | **不做**（只 amd64） | Fly AMS 实例都是 amd64；arm64 无需求；multi-arch build 慢 + GHA cache 复杂度高。M3 实盘上 arm64 (Hetzner CAX) 时再加 |
| Healthcheck | `curl` + `/health` | Fly Machine HEALTHCHECK 比 fly.toml `[checks]` 更早起作用（容器层 vs proxy 层）；double-check |
| Non-root user | UID 10001 | OWASP ASVS V14 + Fly best practice；写入 `/data` 由 Volume 挂载时 chown 处理 |
| `WORKDIR /app` | `/app` 而不是 `/srv` | 业界 Python 容器约定（Fly docs 也用） |

## Image size goal: < 250MB

预估：python:3.12-slim base ~80MB + ca-certificates+tzdata+curl ~30MB + .venv（pyarrow + duckdb + boto3 + supabase + ...）~110MB = **~220MB**。Phase 01.1 LEARNINGS L11 "Makefile 是 first-class 测试目标" 同理 — image size 通过 GHA report 在 PR comment 监控（推迟到 02.1 优化）。

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| `--mount=type=cache` 在 Fly remote build 不支持 | Fly remote build 支持 cache mount（[fly.io/docs/blueprints/working-with-docker/](https://fly.io/docs/blueprints/working-with-docker/)）；若退化为 local build 也能跑 |
| pyarrow + duckdb manylinux wheel 大小（~50MB / 包） | 接受；slim image 总 size 仍 < 250MB；duckdb/pyarrow 是 Phase 01.1 必需依赖 |
| uv sync 在 builder 阶段网络失败 → Docker 缓存破坏 | 使用 `--mount=type=cache` + `--locked` 强制走 lock 不联网 fallback；CI workflow 用 GHA cache 加速 uv 子模块 |
| 容器内 timezone | `TZ=UTC` env + tzdata 包；Fly cron schedule 也按 UTC（fly.toml `schedule` 自动 UTC） |
| Fly Volume 路径 vs 容器内路径 | `/data` 是 Volume mount point；不要在 image 里 prefill `/data` 文件 — 会被 mount 覆盖 |

## Consistency with locked decisions

- ✅ D-08 Docker multi-stage uv（直接落地）
- ✅ thread §0.2.1 部署形态约束（一键部署）

## Affected file paths

- **NEW**：`Dockerfile`
- **NEW**：`.dockerignore`（exclude `tests/`, `data/`, `.venv/`, `.planning/`, `docs/`, `3th-party/`, `.git/`）
- **MODIFY**：`Makefile` — add `make docker-build` / `make docker-run-local`
- **NEW**：`tests/test_docker_smoke.sh` — bash script: `docker build && docker run --rm IMG curl /health`（CI smoke test）

---

# §7 — GHA workflow YAML 细节（the agent's Discretion #5）

**问题**：concurrency control / cache 策略 / test matrix / secret rotation / preview deployment trigger。

## 推荐：单 workflow `deploy.yml` + 独立 `ci.yml`

### Workflow 1：CI（PR 验证，不部署）

```yaml
# .github/workflows/ci.yml
name: CI

on:
  pull_request:
    branches: [main]
  push:
    branches: [main]

concurrency:
  group: ci-${{ github.ref }}
  cancel-in-progress: true

jobs:
  test:
    name: pytest + ruff
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v3
        with:
          version: "0.5.0"
          enable-cache: true
          cache-dependency-glob: "uv.lock"

      - name: Set up Python
        run: uv python install 3.12

      - name: Sync deps
        run: uv sync --locked --extra dev

      - name: ruff lint
        run: uv run ruff check src/ tests/

      - name: pytest (focused)
        # NOT --cov: only fast tests in CI; full suite via make test locally
        run: uv run pytest -x -q tests/

      - name: planning-status (no drift)
        run: uv run python scripts/planning_status.py
```

### Workflow 2：Deploy（main push 触发）

```yaml
# .github/workflows/deploy.yml
name: Deploy to Fly

on:
  push:
    branches: [main]
  workflow_dispatch: {}

# CRITICAL: concurrency prevents parallel deploys overwriting each other
concurrency:
  group: deploy-prod
  cancel-in-progress: false   # let in-progress deploys finish

jobs:
  deploy:
    name: flyctl deploy
    runs-on: ubuntu-latest
    needs: []  # decouple from CI; CI runs separately on push
    timeout-minutes: 15
    permissions:
      contents: read

    steps:
      - uses: actions/checkout@v4

      - uses: superfly/flyctl-actions/setup-flyctl@v1.5
        # Pin major version; check releases https://github.com/superfly/flyctl-actions/releases

      - name: Deploy
        run: flyctl deploy --remote-only --wait-timeout 600
        env:
          FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}

      - name: Smoke test /health
        run: |
          # Wait for new machine to be healthy (Fly Anycast + Better Stack will too,
          # but we want CI to fail fast if deploy broke health)
          APP=polyarb-l1
          for i in {1..10}; do
            if curl -fsS "https://${APP}.fly.dev/health" >/dev/null; then
              echo "✓ deploy healthy"
              exit 0
            fi
            sleep 6
          done
          echo "✗ /health did not respond after 60s"
          flyctl logs --app "${APP}" --no-tail | tail -100
          exit 1
```

## 关键设计取舍

| 取舍 | 选择 | 理由 |
|---|---|---|
| Test matrix（multi-Python）| **不做**（只 3.12） | 项目 pin `requires-python = ">=3.12"`；矩阵浪费 CI 时间 |
| `flyctl-actions` 版本 | `v1.5` (pin) | `@master` 不稳；pin 到 minor [CITED: github.com/superfly/flyctl-actions/releases] |
| `--remote-only` | 启用 | Fly remote build server 缓存 builder image；GHA runner 不需要 docker daemon |
| `concurrency: deploy-prod` | `cancel-in-progress: false` | 部署不能 cancel 中途（可能 partial deploy）；让一个 deploy 跑完再下一个 [CITED: fly.io/docs/launch/continuous-deployment-with-github-actions/] |
| uv cache | `astral-sh/setup-uv@v3` 内置 `enable-cache: true` | GHA cache automatically scopes by `uv.lock` hash |
| GHA secrets | **只放 `FLY_API_TOKEN`** | D-07 锁；prod secrets 走 `flyctl secrets set`，CI 看不到 |
| Test gate | **CI 独立 workflow（ci.yml）+ deploy.yml 不依赖 CI 通过** | 用户 CLAUDE.md 偏好"不要让坏代码上 prod"。**但 deploy.yml 也跑 smoke test**（/health post-deploy）作为最后一道墙。理论上 main push 应当只在 PR merge 后发生，PR merge 前 CI 已通过 |
| Pre-deployment hooks | **不做** | Phase 02 不引入 Husky / lefthook / pre-commit 全栈（已有 .githooks/pre-commit 守 SUMMARY 漂移） |
| Preview deployment | **不做**（D-05 锁定 staging 推迟） | Phase 03+ 视需要加 fly-pr-review-apps |

## Secret rotation 策略

| Secret | 类型 | 哪里存 | Rotation 流程 |
|---|---|---|---|
| `FLY_API_TOKEN` | GHA repo secret | GHA Settings → Secrets | 每 6 个月：`flyctl tokens create deploy -a polyarb-l1` → GHA secret update → 旧 token revoke |
| `SUPABASE_SERVICE_KEY` | Fly app secret | `flyctl secrets set` | 每 12 个月或泄漏疑似时：Supabase Dashboard → Rotate JWT secret → `flyctl secrets set` → `flyctl deploy` |
| `R2_*_KEY` | Fly app secret | `flyctl secrets set` | 同上；Cloudflare dashboard → create new API token → `flyctl secrets set` → 旧 token delete |
| `SCAN_SHARED_SECRET` | Both Fly app secret + Vercel env var | 双端同步 | 修改时**先**部署 daemon（接受新旧）→ Vercel 部署 → daemon 切到只接受新（防止 dashboard 调用中断） |

## Consistency with locked decisions

- ✅ D-06 GHA build + flyctl deploy
- ✅ D-07 secrets 完全隔离（GHA 只有 deploy token，daemon secrets 走 flyctl）

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| `superfly/flyctl-actions@master` 偶发破坏 deploy | pin `v1.5` |
| `flyctl deploy` 等 200s 才返回（machine 启动慢） | `--wait-timeout 600` 给充足时间；并行 smoke test 在脚本里看 |
| pytest 在 CI 跑得太久 → workflow timeout | CI 用 `pytest -x -q` fast subset；full coverage 走 local `make test` |
| ruff 配置 drift（pyproject.toml 改了但 CI 没同步） | ruff 从 pyproject.toml 读 config，本来就同步 |
| 一次 force-push main 触发 deploy 但 image 是中间状态 | concurrency `cancel-in-progress: false` 保证 fly 看到稳定 commit；强制只允许 PR merge 进 main |

## Affected file paths

- **NEW**：`.github/workflows/ci.yml`
- **NEW**：`.github/workflows/deploy.yml`
- **NEW**：`.github/dependabot.yml`（可选 — uv lockfile 自动 PR；推到 02.1 或 03）

---

# §8 — /health endpoint schema（the agent's Discretion #6）

**问题**：返回什么 JSON 字段才能让 Better Stack 区分 "alive vs degraded vs stale vs down"？

## 推荐：IETF draft-inadarei-api-health-check-06 兼容 + Better Stack Keyword Monitor 双判

### 完整 response schema

```json
{
  "status": "pass",
  "version": "0.2.0",
  "releaseId": "2026-05-12-abc123",
  "serviceId": "polyarb-l1",
  "description": "Polymarket L1 observation daemon — subset 2x/day, full 1/week",
  "checks": {
    "snapshot:last_success_age_seconds": [{
      "componentId": "scheduler",
      "componentType": "component",
      "observedValue": 21540,
      "observedUnit": "s",
      "status": "pass",
      "time": "2026-05-12T08:32:00Z",
      "links": { "logs": "https://app.axiom.co/datasets/polyarb-prod" }
    }],
    "snapshot:last_status": [{
      "componentId": "orchestrator",
      "observedValue": "OK",
      "status": "pass",
      "time": "2026-05-12T08:32:00Z"
    }],
    "sqlite:write_latency": [{
      "componentId": "sqlite-store",
      "componentType": "datastore",
      "observedValue": 142,
      "observedUnit": "ms",
      "status": "pass",
      "time": "2026-05-12T08:32:00Z"
    }],
    "supabase:mirror_age_seconds": [{
      "componentId": "supabase-mirror",
      "componentType": "datastore",
      "observedValue": 30,
      "observedUnit": "s",
      "status": "pass",
      "time": "2026-05-12T08:32:00Z"
    }],
    "r2:upload_recent_success": [{
      "componentId": "r2-sync",
      "componentType": "system",
      "observedValue": true,
      "status": "pass",
      "time": "2026-05-12T08:32:00Z"
    }]
  }
}
```

### Status 决定矩阵（top-level）

| 子检查情况 | top-level `status` | HTTP 状态码 | Better Stack 解读 |
|---|---|---|---|
| 所有 checks `pass` | `pass` | **200** | healthy |
| 任一 check `warn` 但没有 `fail` | `warn` | **200** | degraded（Better Stack 需 Keyword Monitor 解析 JSON）|
| 任一 check `fail` | `fail` | **503** | down |
| process 没起 / Fly Anycast 拒绝 | — | timeout / 5xx | down |

### Check 触发规则

| Check | `pass` 阈值 | `warn` 阈值 | `fail` 阈值 |
|---|---|---|---|
| `snapshot:last_success_age_seconds` | < 14h（2x/day cron 间隔 12h + 缓冲 2h） | 14-25h | > 25h |
| `snapshot:last_status` | OK | DEGRADED | FAILED |
| `sqlite:write_latency` | < 500ms | 500-2000ms | > 2000ms |
| `supabase:mirror_age_seconds` | < 5min | 5-60min | > 60min |
| `r2:upload_recent_success` | true | (none) | false consecutively 3+ |

### 实现示例

```python
# src/polyarb/http/health.py
# Source: datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06
from datetime import datetime, timezone
from starlette.responses import JSONResponse
from starlette.requests import Request

from polyarb.storage.sqlite_store import SQLiteStore


HEALTH_CONTENT_TYPE = "application/health+json"


async def health(request: Request) -> JSONResponse:
    store: SQLiteStore = request.app.state.sqlite_store
    settings = request.app.state.settings

    checks = {}
    overall = "pass"

    # 1. snapshot age check
    last_snapshot = store.get_latest_snapshot()
    if last_snapshot is None:
        age_s = None
        status = "fail"
        overall = "fail"
    else:
        age_s = (datetime.now(tz=timezone.utc).timestamp() * 1000 - last_snapshot.taken_at_ms) / 1000
        if age_s < 14 * 3600:
            status = "pass"
        elif age_s < 25 * 3600:
            status = "warn"
            overall = "warn" if overall == "pass" else overall
        else:
            status = "fail"
            overall = "fail"
    checks["snapshot:last_success_age_seconds"] = [{
        "componentId": "scheduler",
        "componentType": "component",
        "observedValue": age_s,
        "observedUnit": "s",
        "status": status,
        "time": datetime.now(tz=timezone.utc).isoformat(),
    }]

    # 2. last_status check
    if last_snapshot:
        last_status = "OK" if last_snapshot.is_valid else "FAILED"  # extend to DEGRADED later
        status = {"OK": "pass", "DEGRADED": "warn", "FAILED": "fail"}[last_status]
        if status != "pass":
            overall = "fail" if status == "fail" else ("warn" if overall == "pass" else overall)
        checks["snapshot:last_status"] = [{
            "componentId": "orchestrator",
            "observedValue": last_status,
            "status": status,
            "time": datetime.now(tz=timezone.utc).isoformat(),
        }]

    # 3-5. (sqlite latency / supabase mirror age / r2 success) — similar

    body = {
        "status": overall,
        "version": settings.version,
        "releaseId": settings.release_id,
        "serviceId": "polyarb-l1",
        "description": "Polymarket L1 observation daemon",
        "checks": checks,
    }

    http_status = 200 if overall in ("pass", "warn") else 503
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)
```

### Better Stack 配置

| 维度 | 设置 |
|---|---|
| Monitor 类型 | **HTTP Keyword Monitor** + **HTTP Status Monitor** 双 monitor |
| URL | `https://polyarb-l1.fly.dev/health`（公网 ok — endpoint 是 read-only 健康状态，不暴露 scan/data 接口）|
| Frequency | 30s |
| Grace period | 6s（20% of 30s）[CITED: betterstack.com/docs/uptime/cron-and-heartbeat-monitor/] |
| Expected status code | 200-204（warn 还是 200 所以也 pass） |
| Keyword to match (Keyword Monitor) | `"status":"pass"` → healthy / `"status":"warn"` → trigger Slack notice (不告警 phone) / `"status":"fail"` → trigger Telegram + email |
| Alert escalation | warn = soft notice / fail = full escalation (Telegram + email) |

## Consistency with locked decisions

- ✅ D-12 三态 OK/DEGRADED/FAILED 映射到 pass/warn/fail
- ✅ D-16 Better Stack `/health` ping
- ✅ D-17 Telegram + email
- ✅ D-22 endpoint 通过 Fly Anycast `https://polyarb-l1.fly.dev/health` 暴露（**注意**：D-22 锁定 "Fly internal only"，但 `/health` 是公开 read-only，可以放出公网；如果坚持 internal-only 需要让 Better Stack 走 Fly Anycast 入口 — Better Stack 公网 ping 是必要的，所以 `/health` 必须 public。**`/scan` endpoint 才需要 internal only**。Plan 阶段需要澄清 D-22 适用范围）

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| `/health` 公开 → 攻击面增加 | 严格 read-only；不接受任何 query/body；no PII；只暴露状态信息（不暴露 internal IP / DB schema） |
| HTTP 503 让 Fly Anycast 自动剔除实例 → 永远不恢复 | Fly machine HEALTHCHECK 失败 → restart machine，不剔除实例（machine 是 long-running，restart 才是恢复路径）。需要 fly.toml `restart.policy = "on-failure"` |
| `warn` 状态下 Better Stack 不应该 page 但应该 notice | Keyword Monitor 配 `"status":"warn"` → Slack（轻量推送），不走 Telegram + email |
| 时区一致性 | check `time` 字段始终 UTC ISO8601 + `Z` 后缀 |
| `/health` 加载时 SQLite read 阻塞 → 30s timeout 超时 | SQLite read 用 `file:...?mode=ro` URI + 短查询（only `SELECT MAX(taken_at_ms) FROM snapshots LIMIT 1`）；< 50ms |

## Affected file paths

- **NEW**：`src/polyarb/http/health.py`
- **MODIFY**：`src/polyarb/storage/sqlite_store.py` — add `get_latest_snapshot()` read-only helper
- **NEW**：`tests/test_health_endpoint.py` — pass / warn / fail 三档 fixture

---

# §9 — FastAPI vs Starlette vs Flask（the agent's Discretion #7）

**问题**：daemon 加 HTTP API server 接 dashboard `/scan` + `/health`。轻量 framework 推荐 + 集成 loguru / asyncio / 与现有 daemon main loop 共存方案。

## 对比矩阵

| 维度 | FastAPI | **Starlette**（推荐） | Flask |
|---|---|---|---|
| ASGI / WSGI | ASGI ✅ | ASGI ✅ | WSGI (Flask 3 加 ASGI middleware 但非原生)❌ |
| 自带 OpenAPI/Swagger | ✅ | ❌（手动加 swagger-ui） | ❌ |
| Pydantic 集成 | 强 | 无（需要手动 parse JSON） | 无 |
| 依赖体积 | 大（pulls in pydantic + jinja2 等） | 小 | 小 |
| 单文件 hello world | ~10 行（带类型） | ~10 行 | ~5 行 |
| asyncio 原生 | ✅ | ✅ | ❌（必须 wrap） |
| 与 uvicorn 共存 | ✅ | ✅（uvicorn 实际就是 Starlette + 调度） | ❌ |
| 适合 daemon 内嵌 | 重 | **完美** | 不适合（同步） |
| L1 daemon 实际需求 | 2 endpoints, no schema validation | 2 endpoints, no schema validation | 2 endpoints, no schema validation |

## 推荐：Starlette + uvicorn

**理由**：
1. **L1 daemon 只需 2 个 endpoint** — FastAPI 的 Pydantic body validation / OpenAPI 自动化在这里都是负担
2. **daemon 已经全 asyncio**（snapshot orchestrator async；scheduler async loop）— Starlette ASGI 原生融合，asyncio.gather() 启 HTTP + scheduler 同进程
3. **轻量** — Starlette 0.49 zero-deps 之外 + uvicorn 标准 deps；FastAPI 多 ~3MB image + Pydantic v2 编译
4. **业内主流**[CITED: leapcell.medium.com "FastAPI is Overkill"] 在"daemon 内嵌少量 endpoint"场景明确推荐 Starlette

### 实现示例

```python
# src/polyarb/http/app.py
# Source: starlette.io (verified 2026-05-12)
from starlette.applications import Starlette
from starlette.routing import Route
from starlette.middleware import Middleware
from starlette.middleware.cors import CORSMiddleware

from polyarb.http.health import health
from polyarb.http.scan import scan, scan_auth_middleware


def create_app(*, scheduler, sqlite_store, settings) -> Starlette:
    middleware = [
        # /scan goes through HMAC auth; /health is public read-only
        Middleware(scan_auth_middleware, secret=settings.scan_shared_secret),
    ]
    routes = [
        Route("/health", health, methods=["GET"]),
        Route("/scan", scan, methods=["POST"]),
    ]
    app = Starlette(routes=routes, middleware=middleware)

    # Stash scheduler + store + settings on app.state for handlers to access
    app.state.scheduler = scheduler
    app.state.sqlite_store = sqlite_store
    app.state.settings = settings
    return app
```

```python
# src/polyarb/http/scan.py
# Source: project Phase 01.1 P1 trust-split + Starlette docs
import hashlib
import hmac
import json
from starlette.requests import Request
from starlette.responses import JSONResponse

from polyarb.observation.scanner import run_recipe


async def scan(request: Request) -> JSONResponse:
    """POST /scan — invoke a Phase 01.1 builtin or yaml recipe.

    Body: {"recipe_name": "thick-but-slippery", "params": {"limit": 50}}
    """
    body = await request.json()
    recipe_name = body.get("recipe_name")
    params = body.get("params", {})

    if not isinstance(recipe_name, str) or len(recipe_name) > 64:
        return JSONResponse({"error": "invalid recipe_name"}, status_code=400)

    # Run via existing scanner.py — P1 trust-split applies (recipe_name lookup
    # in BUILTIN_RECIPES is trusted=True; yaml recipes are trusted=False).
    try:
        df = run_recipe(
            sqlite_path=request.app.state.settings.db_path,
            recipe_name=recipe_name,
            params=params,
        )
    except KeyError:
        return JSONResponse({"error": f"unknown recipe: {recipe_name}"}, status_code=404)
    except ValueError as e:
        return JSONResponse({"error": str(e)[:200]}, status_code=400)

    return JSONResponse({
        "recipe": recipe_name,
        "row_count": len(df),
        "rows": df.head(100).to_dict(orient="records"),
    })


async def scan_auth_middleware(request: Request, call_next, *, secret: str):
    """HMAC-of-body auth for /scan; bypass /health."""
    if request.url.path != "/scan":
        return await call_next(request)

    received_sig = request.headers.get("X-Signature")
    if not received_sig:
        return JSONResponse({"error": "missing X-Signature"}, status_code=401)

    body = await request.body()
    expected_sig = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(received_sig, expected_sig):
        return JSONResponse({"error": "invalid signature"}, status_code=401)

    # Re-inject body for downstream handler
    async def receive():
        return {"type": "http.request", "body": body, "more_body": False}
    request._receive = receive

    return await call_next(request)
```

### 共存方案（与 snapshot scheduler）

见 §Architecture Patterns 中 Pattern 1 — `asyncio.gather(server_task, scheduler_task)` + signal handler graceful shutdown.

### Loguru integration

uvicorn 默认用 stdlib `logging`；要让所有日志走 loguru 需要 intercept：

```python
# src/polyarb/observability/logging.py
# Source: dash0.com/guides/python-logging-with-loguru + Phase 01.1 pattern
import logging
import sys
from loguru import logger


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def init_logging() -> None:
    # Remove default stderr; add JSON to stdout for Axiom
    logger.remove()
    logger.add(
        sys.stdout,
        serialize=True,                # JSON output
        level="INFO",
        enqueue=False,                 # in-process; daemon-friendly
        backtrace=False,               # don't leak source path in prod logs
        diagnose=False,                # don't leak variable values
    )

    # Intercept stdlib logging (uvicorn, starlette, httpx) → loguru
    logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "starlette"):
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False
```

## Consistency with locked decisions

- ✅ D-21 scan trigger 复用 Phase 01.1 4 层 SQL 防御（直接调 `run_recipe`）
- ✅ D-22 endpoint Fly internal only（在 fly.toml 限制 `internal_port=8080` + `services` no public Anycast → `/scan` 仅内网；`/health` 走 public Anycast）

## Known pitfalls + mitigations

| Pitfall | Mitigation |
|---|---|
| Starlette routes 字符串 collision（/health vs /health/） | `Route("/health", ...)` 显式注册；no trailing slash redirect |
| `scan` 处理时间 > 30s（大 recipe）→ Vercel Edge Function timeout | Vercel Edge 默认 30s；`/scan` 同步返回 JSON 必须 < 25s；若 recipe 复杂改用 async polling pattern（推到 02.1） |
| uvicorn graceful shutdown 与 cron scheduler 冲突 | `server.should_exit = True` → finish in-flight requests；scheduler task 走 `stop_event` 协作；asyncio.gather() return_exceptions=True |
| HMAC secret 泄露 → 任何人能 trigger scan | secret 走 flyctl secrets；Vercel env var；rotation 流程见 §7 |
| Starlette 0.49 与 uvicorn 0.39 兼容 | Both 跟 ASGI 3.0 spec；测试时跑 `make smoke-test` 验证 |

## Affected file paths

- **NEW**：`src/polyarb/http/__init__.py`
- **NEW**：`src/polyarb/http/app.py`
- **NEW**：`src/polyarb/http/health.py`
- **NEW**：`src/polyarb/http/scan.py`
- **NEW**：`src/polyarb/observability/logging.py`
- **NEW**：`src/polyarb/observability/sentry.py`
- **NEW**：`src/polyarb/daemon/__init__.py`
- **NEW**：`src/polyarb/daemon/main.py`
- **NEW**：`src/polyarb/daemon/scheduler.py`
- **NEW**：`src/polyarb/daemon/alerts.py`
- **MODIFY**：`pyproject.toml` — add starlette, uvicorn[standard], sentry-sdk
- **NEW**：`tests/test_http_scan.py` / `tests/test_http_health.py`

---

# §10 — Already-locked stack 摘要（Phase 02 不重新调研）

> Plan 阶段 reader 可直接参考此节，不需要回读 thread

| 维度 | 决策 | 来源 | 验证 |
|---|---|---|---|
| Compute PaaS | Fly.io AMS region | thread §2.1.7 表 | 离 Polymarket London ~10ms；Trading-readiness ★★★★★；shared-cpu-1x@1GB ≈ $5.70/月 + $3.60 dedicated egress / [VERIFIED: fly.io/docs/about/pricing 2026-05] |
| Database | Supabase Pro Dublin (eu-west-1) | thread §0.1 | $25/月 起；TimescaleDB 不支持但 L1 不需要；2026-03 CN ISP 部分 block（用户接受，挂 VPN）/ [VERIFIED: supabase.com/pricing] |
| Object storage | Cloudflare R2 | thread §2.4 / §3.1 | 10GB 月免费；零 egress（关键差异化）；S3-compatible / [VERIFIED: developers.cloudflare.com/r2/pricing/] |
| Logs | Axiom Free | thread §2.3.3 | 500GB/月 ingest + 30 天 retention（业内最慷慨）；APL 查询语言 / [VERIFIED: axiom.co/pricing] |
| Errors | Sentry Developer Free | thread §2.3.4 | 5k errors/月；sentry_sdk loguru integration / [VERIFIED: sentry.io/pricing + docs.sentry.io/platforms/python/integrations/loguru] |
| Uptime | Better Stack Free | thread §2.3.1 | 10 monitor × 30s；Telegram + email + Webhook native / [VERIFIED: betterstack.com/pricing] |
| Dashboard host | Vercel | thread §2.4.1 | Hobby tier "non-commercial only"（个人项目套利可视化未变现 — 边界灰色）；Pro $20/月套利产生收入后即升 / [VERIFIED: vercel.com/pricing] |

## Polymarket 区域关键事实（不可妥协）

- **Polymarket 服务器在 AWS eu-west-2 London**（thread §0.1.1，2026-04-07 实测）
- **IP 黑名单 33 国**：US / UK / Singapore / HK / CN 大陆 全在内 → 后端不能部署在这些区
- **Cloudflare 限流**：3500 req/10s order placement / 9000 req/10s general → 长期路线需要 dedicated egress IP（D-11 推到 M3）

---

# §11 — Validation Architecture（Nyquist validation enabled）

### Test Framework

| Property | Value |
|---|---|
| Framework | pytest 8.2+ + pytest-asyncio 0.23 + respx 0.21 + freezegun 1.5 |
| Config file | `pyproject.toml` `[tool.pytest.ini_options]` |
| Quick run command | `uv run pytest -x -q tests/test_health_endpoint.py tests/test_supabase_mirror.py` |
| Full suite command | `uv run pytest -q` |

### Phase Requirements → Test Map

| Req | Behavior | Test Type | Automated Command | File Exists? |
|---|---|---|---|---|
| D-08 / §6 | Dockerfile builds clean | smoke (CI) | `docker build -t polyarb-l1 . && docker run --rm polyarb-l1 python -c "import polyarb"` | ❌ Wave 0 — new `tests/test_docker_smoke.sh` |
| D-08 / §6 | Image starts, /health responds | smoke (CI) | `docker run -d -p 8080:8080 polyarb-l1 && sleep 5 && curl -fsS localhost:8080/health` | ❌ Wave 0 |
| D-12 / §8 | /health returns pass when snapshot fresh | unit | `pytest tests/test_health_endpoint.py::test_pass_when_fresh -x` | ❌ Wave 0 |
| D-12 / §8 | /health returns warn when 15h stale | unit | `pytest tests/test_health_endpoint.py::test_warn_when_stale -x` | ❌ Wave 0 |
| D-12 / §8 | /health returns 503 when 25h+ stale | unit | `pytest tests/test_health_endpoint.py::test_fail_when_very_stale -x` | ❌ Wave 0 |
| D-13 / §scheduler | scheduler pauses after 3 consecutive failures | unit | `pytest tests/test_scheduler.py::test_pause_after_3_failures -x` | ❌ Wave 0 |
| D-21 / §9 | /scan rejects missing X-Signature | unit | `pytest tests/test_http_scan.py::test_rejects_missing_signature -x` | ❌ Wave 0 |
| D-21 / §9 | /scan rejects invalid HMAC | unit | `pytest tests/test_http_scan.py::test_rejects_bad_hmac -x` | ❌ Wave 0 |
| D-21 / §9 | /scan invokes scanner.run_recipe with sanitized input | integration | `pytest tests/test_http_scan.py::test_invokes_run_recipe -x` | ❌ Wave 0 |
| D-21 / §9 | /scan 4-layer SQL defense still applies (no yaml shortcuts) | integration | `pytest tests/test_http_scan.py::test_yaml_trust_split_preserved -x` | ❌ Wave 0 |
| §3 / Supabase mirror | mirror failure does not fail snapshot (DEGRADED) | integration | `pytest tests/test_supabase_mirror.py::test_mirror_failure_degraded -x` | ❌ Wave 0 |
| §3 / Supabase mirror | idempotent upsert (re-run safe) | integration | `pytest tests/test_supabase_mirror.py::test_idempotent_upsert -x` | ❌ Wave 0 |
| §4 / R2 sync | parquet upload to R2 (mocked boto3) | unit | `pytest tests/test_r2_sync.py::test_upload_to_r2 -x` | ❌ Wave 0 |
| §4 / cron | snapshots-purge respects DAYS + KEEP | unit | `pytest tests/test_purge.py -x` | ✅ Phase 01.1 amendment |
| §5 / page_fetched_at_ms | normalizer carries per-page stamp | unit | `pytest tests/test_normalizer.py::test_page_fetched_at_ms -x` | ❌ Wave 0 |
| §5 / schema | SQLite + Parquet new column | unit | `pytest tests/test_schemas.py::test_page_fetched_at_ms_nullable -x` | ❌ Wave 0 |
| D-11 / chaos | snapshot resumes after Gamma 503 mock | integration | `pytest tests/test_chaos_gamma_5xx.py -x` | ❌ Wave 0 |
| D-13 / chaos | daemon pauses after 3x consecutive failures | integration | `pytest tests/test_chaos_3failures_pause.py -x` | ❌ Wave 0 |
| D-14 / observability | loguru JSON output is Axiom-parseable | unit | `pytest tests/test_logging.py::test_json_serialize -x` | ❌ Wave 0 |
| L11 / makefile | `make snapshot-markets` exit 0 ↔ parquet落地 + SQLite row +1 (triple check) | integration | `bash tests/test_makefile_triple_check.sh` | ❌ Wave 0 — 关键修 L11 silent failure |
| D-12 / parquet validation | parquet 行数 == SQLite snapshots.market_count | unit | `pytest tests/test_parquet_sqlite_consistency.py -x` | ❌ Wave 0 |

### Sampling Rate

- **Per task commit**: `uv run pytest -x -q tests/test_<file_just_changed>.py`
- **Per wave merge**: `uv run pytest -q`（full m1-perception ~ 4 min）
- **Phase gate**: full suite + `make smoke-test` + `docker build` 全绿前不 deploy to prod

### Wave 0 Gaps

- [ ] `tests/test_health_endpoint.py` — covers D-12 / D-16
- [ ] `tests/test_http_scan.py` — covers D-21 / D-22
- [ ] `tests/test_supabase_mirror.py` — covers §3
- [ ] `tests/test_r2_sync.py` — covers §4
- [ ] `tests/test_scheduler.py` — covers D-13
- [ ] `tests/test_normalizer.py::test_page_fetched_at_ms` — covers §5
- [ ] `tests/test_schemas.py::test_page_fetched_at_ms_nullable` — covers §5
- [ ] `tests/test_chaos_gamma_5xx.py` + `tests/test_chaos_3failures_pause.py` — chaos engineering（respx mock）
- [ ] `tests/test_logging.py::test_json_serialize` — covers D-14
- [ ] `tests/test_makefile_triple_check.sh` — covers L11 silent failure root cause
- [ ] `tests/test_parquet_sqlite_consistency.py` — covers D-12 dual-check (parquet+SQLite)
- [ ] `tests/test_docker_smoke.sh` — covers D-08
- [ ] Framework install: 已有 pytest 8.2 + pytest-asyncio 0.23 + respx 0.21（pyproject.toml dev extra）+ 加 `aioresponses` (chaos mock async) 推后再加

### Chaos Engineering 子节（D-13 + thread §1）

> "生产级 = 可长跑 7×24，单次跑通不算数"（CLAUDE.md 节 + thread §0.3 #6）

| Chaos scenario | 模拟 | 期望行为 |
|---|---|---|
| Gamma API returns 503 5 次 | respx mock | tenacity exponential backoff，最后 layer1 issue → snapshot FAILED |
| Gamma 翻页中途超时 | respx delay | partial fetch + Issue → DEGRADED 或 FAILED |
| CLOB returns malformed book (`{"asks": "not-a-list"}`) | respx mock | F-1 _safe_float capture + Issue → DEGRADED |
| SQLite WAL lock contention（writer + reader同时） | pytest tmp_path | reader waits + eventually succeeds（WAL allows） |
| Supabase API returns 500 | respx mock | mirror failure → DEGRADED but snapshot OK |
| R2 PUT returns 503 | botocore stubber | upload retry 3x, then Issue → DEGRADED |
| 3 次连续 FAILED | scheduler test | daemon enters paused state；needs manual unblock + alert sent |
| `/scan` flood (10 req/s for 30s) | locust or async loop | rate limit / SQLite read concurrency capped；no crash |

### Pre-prod 7-day soak test

phase 02 的真正"完成" gate（thread §1 生产级判定标准）：
- 部署到 Fly prod → 7×24 跑无人值守
- Axiom log volume / Sentry errors 看每天
- 至少一次自然失败（API 抖动 / cron 错过窗口）后系统自愈或正确告警
- Better Stack uptime ≥ 99%（10 min downtime tolerance per week）

7 天后 phase 02 `/gsd-extract_learnings`，否则不进 Phase 03 L2 工作（thread §1 层级纪律）。

---

# §12 — Security Domain

### Applicable ASVS Categories（v4.0.3）

| ASVS Category | Applies | Standard Control |
|---|---|---|
| V1 Architecture | yes | 引入新攻击面（HTTP server）需重画 trust boundary diagram |
| V2 Authentication | yes | `/scan` HMAC 认证（V2.1.5 强 hash 函数 + V2.2.x 不弱密码 — HMAC secret 32 字节 random） |
| V3 Session Management | partial | dashboard 用 Supabase Auth magic-link（D-20）；daemon 无 session（单 endpoint stateless）|
| V4 Access Control | yes | `/scan` 仅 Fly internal network（D-22）；`/health` public read-only；scan endpoint 通过 4 层 SQL 防御保留 P1 trust-split |
| V5 Input Validation | yes | `recipe_name` 限 64 字符 + isinstance check；`params` JSON 限 size；底层 scanner.py 4 层防御复用 |
| V6 Cryptography | yes | HMAC-SHA256（V6.2.2 必备）；TLS via Fly Anycast（自动 LE cert） |
| V7 Error Handling | yes | 错误不泄漏 stack trace 给客户端；Sentry capture 但 client 收 generic message |
| V8 Data Protection | yes | secrets 永不进 git（已有 `.gitignore` + pre-commit）；R2 / Supabase secrets via flyctl |
| V9 Communications | yes | TLS 1.2+ via Fly Anycast；boto3 → R2 用 HTTPS endpoint |
| V10 Malicious | partial | 不引入第三方插件 / extension |
| V11 Business Logic | partial | scan 不是金融操作（read-only）；rate limit 软约束 |
| V12 Files | yes | parquet path injection 防（P6 resolve_snapshot_path 已防）；R2 key 拼接走 `f"{year}/{month:02d}/..."` 不接受用户输入 |
| V13 API | yes | `/health` Content-Type `application/health+json`（V13.2.6）；`/scan` body 限 size（V13.2.5） |
| V14 Config | yes | Docker non-root（V14.1.1）；image scan via GHA（V14.2.x 推到 02.1）；secrets not in image (V14.1.5 builder-only ARG，runtime ENV via flyctl) |

### Known Threat Patterns（Phase 02 新攻击面）

| Pattern | STRIDE | 实例 | Mitigation |
|---|---|---|---|
| **T-02-01** Unauthenticated /scan flood | DoS | 攻击者发现 `/scan` URL → 大量 POST 让 daemon SQLite read 锁竞争 | D-22 Fly internal only + HMAC + `/scan` rate limit (Starlette middleware) |
| **T-02-02** HMAC secret leak via GitHub | Spoofing | secret 误 commit | pre-commit hook (现有) + GitHub secret scanning + 短 rotation 周期（6 月）|
| **T-02-03** SQL injection via recipe_name | Tampering | `recipe_name = "thick'; DROP TABLE markets; --"` | Phase 01.1 4 层防御保留：Layer 1 `file:...?mode=ro` URI engine 拒绝 DML/DDL；Layer 3 ORDER BY whitelist。recipe_name 仅作 lookup key 到 `BUILTIN_RECIPES` dict（不直接拼 SQL） |
| **T-02-04** Path traversal via params.snapshot_id | Tampering | `snapshot_id = -1 OR 1=1` | P6 `resolve_snapshot_path` 已防（int 校验 + read-only SQLite lookup）|
| **T-02-05** Telegram bot token leak | Spoofing | env / log dump 泄漏 token | flyctl secrets + 日志脱敏 + loguru `diagnose=False` 不打印变量值 |
| **T-02-06** Supabase service_role abuse | Elevation | service_role key 泄漏 → 任何人能写所有表 | service_role 仅 daemon 进程持有；dashboard 用 anon_key + RLS；不进 GHA secrets |
| **T-02-07** Axiom log secrets leakage | Information disclosure | snapshot 抓取过程中 secrets 误入 log（如 URL with API key） | loguru intercept 加 redact filter（用户已知 pattern：`secret=*` / `token=*` / `Bearer *`）|
| **T-02-08** Sentry breadcrumb captures sensitive data | Information disclosure | sentry_sdk 默认抓 stdlib logging.INFO 进 breadcrumb，可能含 sensitive payload | `sentry_sdk.init(send_default_pii=False)` + `before_send` hook strip 敏感字段 |
| **T-02-09** Better Stack public endpoint reveals internal state | Information disclosure | `/health` 暴露 SQL 写入延迟 / Supabase 状态等 = 提供给攻击者 timing oracle | 接受（小风险 — daemon 是 single-tenant + 攻击不影响业务） |
| **T-02-10** Vercel Edge Function → daemon CSRF | Spoofing | 外部站点构造 form post → 假装 Vercel 调 daemon | HMAC + Vercel-only secret + Fly internal only 三重防 |
| **T-02-11** Polymarket API token leak | Information disclosure | snapshot 失败时 stack trace 含 API base URL + token query param | F-5 truncation cap (200 chars) 已存在；loguru `diagnose=False` |
| **T-02-12** Dockerfile builds with untrusted base | Tampering | `python:3.12-slim` 被供应链攻击 | pin sha256 digest（推到 02.1，启动期接受 tag pin） |

### `/scan` endpoint 攻击链补充

```
Vercel Edge Function (POST + X-Signature header)
  ↓
Fly Anycast → polyarb-l1.internal (private network)
  ↓
Starlette scan_auth_middleware
  - HMAC compare_digest（timing-safe）
  - reject if missing/invalid X-Signature
  ↓
scan handler
  - validate recipe_name (≤ 64 chars, isinstance str)
  - validate params (max 100 keys, value strings ≤ 1KB)
  ↓
scanner.run_recipe(sqlite_path, recipe_name, params)
  - BUILTIN_RECIPES lookup by exact name match (no glob)
  - Layer 1: file:...?mode=ro URI → SQLite engine refuses DDL/DML
  - Layer 4: LIMIT enforced [1, 10000]
  ↓
DataFrame.head(100).to_dict()
  - cap response size to 100 rows (regardless of recipe limit)
```

### Phase 02 SECURITY-REVIEW 范围（F-7+ 需补）

Phase 01 SECURITY-REVIEW.md 落地 F-1..F-5；Phase 02 plan 应产出 `02-SECURITY-REVIEW.md` 覆盖：

- **F-7**：HMAC secret 32-byte CSPRNG + rotation 流程
- **F-8**：Sentry `send_default_pii=False` + `before_send` redact filter
- **F-9**：loguru redact filter for known secret patterns
- **F-10**：Docker non-root user UID 10001 + `/data` chown
- **F-11**：Fly secrets vs GHA secrets 完全隔离审计（脚本扫描配置文件）
- **F-12**：Supabase RLS policy 文档（anon read-only / service write-only）

---

## Sources

### Primary (HIGH confidence)

- [docs.astral.sh/uv/guides/integration/docker/](https://docs.astral.sh/uv/guides/integration/docker/) — uv multi-stage Dockerfile template + UV_* env vars（accessed 2026-05-12）
- [fly.io/docs/launch/continuous-deployment-with-github-actions/](https://fly.io/docs/launch/continuous-deployment-with-github-actions/) — GHA flyctl deploy workflow + concurrency control
- [fly.io/docs/reference/configuration/](https://fly.io/docs/reference/configuration/) — fly.toml `[mounts]` + scheduled machines + http_checks
- [fly.io/docs/python/the-basics/multi-stage-builds/](https://fly.io/docs/python/the-basics/multi-stage-builds/) — Fly Python image best practices
- [datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06](https://datatracker.ietf.org/doc/html/draft-inadarei-api-health-check-06) — IETF health-check JSON schema
- [docs.sentry.io/platforms/python/integrations/loguru/](https://docs.sentry.io/platforms/python/integrations/loguru/) — sentry-sdk Loguru integration
- [supabase.com/docs/reference/python/upsert](https://supabase.com/docs/reference/python/upsert) — Python SDK bulk upsert
- [supabase.com/docs/guides/realtime/subscribing-to-database-changes](https://supabase.com/docs/guides/realtime/subscribing-to-database-changes) — Realtime (推迟到 02.1)
- [developers.cloudflare.com/r2/pricing/](https://developers.cloudflare.com/r2/pricing/) — R2 pricing 零 egress confirmed
- [pgloader.readthedocs.io/en/latest/ref/sqlite.html](https://pgloader.readthedocs.io/en/latest/ref/sqlite.html) — SQLite → Postgres migration tool
- [alembic.sqlalchemy.org/](https://alembic.sqlalchemy.org/) + [Cookbook](https://alembic.sqlalchemy.org/en/latest/cookbook.html) — Alembic Postgres migrations

### Secondary (MEDIUM confidence)

- [betterstack.com/docs/uptime/cron-and-heartbeat-monitor/](https://betterstack.com/docs/uptime/cron-and-heartbeat-monitor/) — heartbeat monitor (grace period guidance)
- [leapcell.medium.com "FastAPI is Overkill: Starlette and Pydantic Are All You Really Need"](https://leapcell.medium.com/fastapi-is-overkill-starlette-and-pydantic-are-all-you-really-need-2b2d55c53de0) — FastAPI vs Starlette tradeoff（multiple-source verified）
- [dash0.com/guides/python-logging-with-loguru](https://www.dash0.com/guides/python-logging-with-loguru) — loguru JSON `serialize=True`
- [depot.dev/docs/container-builds/how-to-guides/optimal-dockerfiles/python-uv-dockerfile](https://depot.dev/docs/container-builds/how-to-guides/optimal-dockerfiles/python-uv-dockerfile) — uv Docker best practices
- [github.com/superfly/flyctl-actions/releases](https://github.com/superfly/flyctl-actions/releases) — flyctl-actions version
- [github.com/inadarei/rfc-healthcheck](https://github.com/inadarei/rfc-healthcheck) — IETF health-check reference impl
- [pybit.es/articles/fastapi-deployment-made-easy-with-docker-and-fly-io/](https://pybit.es/articles/fastapi-deployment-made-easy-with-docker-and-fly-io/) — FastAPI + Fly + Docker pattern (cross-verify with Starlette pattern)

### Tertiary (LOW confidence)

- "OWASP ASVS V14 HTTP headers"（结合多源 — V14.4.x specific HTTP header requirements）— 用于 §12 Security
- "chaos engineering Python httpx" — patterns 通用，未找到 polymarket-specific best practice

### Cross-source verification matrix

| Critical claim | Source 1 | Source 2 | Verdict |
|---|---|---|---|
| uv Docker multi-stage 是业内标准（2026 Q1+） | astral docs (HIGH) | depot.dev (MEDIUM) | HIGH ✅ |
| Starlette 适合 2-endpoint daemon 内嵌 | leapcell.medium (MEDIUM) | starlette.io official (HIGH — minimal API surface confirmed) | HIGH ✅ |
| Better Stack HTTP Status Monitor 不 parse body | betterstack docs (HIGH — Keyword Monitor 是 separate type) | community discussions | HIGH ✅ — 需要双 monitor |
| R2 零 egress | Cloudflare official pricing (HIGH) | r2drop.com analysis (MEDIUM) | HIGH ✅ |
| Polymarket 在 AWS eu-west-2 London | NYCServers 2026-04 article (MEDIUM) | thread §0.1.1 (locked) | HIGH ✅ |
| Supabase Free 1 周不写入会暂停 | thread §2.2.1 (HIGH — 实测) | supabase.com/pricing (HIGH) | HIGH ✅ |

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|---|---|---|---|
| pip install + virtualenv 手动 activate | **uv 0.5+ + uv.lock** | 2024 起业内 dominant | Phase 02 Dockerfile + GHA 全 uv |
| FastAPI 不可质疑 | **Starlette over FastAPI for daemons** | 2025 起 multiple voices | Phase 02 选 Starlette |
| `print()` / stdlib logging | **loguru JSON + serialize=True** | Phase 01.1 已锁 | Phase 02 加 Axiom integration |
| Sentry SDK 1.x | **sentry-sdk 2.x with auto-loguru-integration** | 2024 Q4 | Phase 02 直接装 2.59 |
| Fly free tier 永久 | **Fly pay-as-you-go**（2024-10 取消 free） | thread §2.1.1 已锁 | 启动期接受 ~$5-10/月 |
| Supabase 永久 free | **Supabase Free with 1-week-idle pause** | 仍是 free model + 暂停规则 | Phase 02 mirror 每天 2 次写入 = 自动 keep-alive |
| Cloudflare R2 beta | **R2 GA + zero egress** | 2023 GA | Phase 02 主推 |
| Better Stack Logs 3GB/3d | **仍是 free 限制；Axiom 500GB/30d 替代** | thread §2.3.1 vs §2.3.3 | D-14 锁 Axiom |
| 单服务 Dockerfile root user | **non-root UID + USER directive 必需** | OWASP + Distroless 推动 | Phase 02 Dockerfile |
| superfly/flyctl-actions@master | **pin to v1.x** | 2025 多次 incident | Phase 02 GHA pin v1.5 |

**Deprecated/outdated（不要用）:**

- `python -m venv .venv && pip install -r requirements.txt` — pyproject.toml + uv 完全取代
- `os.environ` 读 secrets 不脱敏 — flyctl secrets + loguru redact filter
- `sentry_sdk.capture_exception` 手工调用 — loguru integration 自动
- Fly managed Postgres（Fly 已废弃 → Supabase / Neon）
- Cloudflare R2 boto3 配置用 `region_name="auto"` — 现在用 `endpoint_url="https://<account>.r2.cloudflarestorage.com"` + `region_name="auto"`

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | Supabase Free 一次 subset snapshot mirror 50MB → 月内 30 天 retention 不撞 500MB | §3 | 撞顶 → 升 Pro $25 + 1 周；plan 阶段加 monitoring + 30 天后实测重估 |
| A2 | Phase 02 部署期间 Polymarket Cloudflare 限流不触发（Fly AMS 单实例无 dedicated egress） | §10 | 触发 → 加 $3.60/月 dedicated egress IPv4（thread §5.2 if-then 已预案） |
| A3 | Fly Volume 3GB free 足够 30 天 retention（subset 50MB × 60 次 + full 100MB × 4 次 = 3.4GB） | §4 | 撞 → extend volume 至 5GB；额外 ~$0.30/月 |
| A4 | `/scan` endpoint 复杂 recipe 在 25s 内完成（Vercel Edge 30s 限） | §9 | 触发 → async polling pattern（推到 02.1） |
| A5 | Sentry Free 5k errors/月足够（启动期日错误数预估 < 100） | §10 | 撞 → 升 Team $26 或 reduce error noise |
| A6 | Axiom Free 500GB/月足够（启动期日 log 量 ~ 100MB = 3GB/月） | §10 | 撞 → 升 Pro $25 |
| A7 | uv 0.5.x 在 Fly remote builder 跑 `--mount=type=cache` 正常 | §6 | 失败 → fallback 到无 cache mount，慢但 build 仍然过 |
| A8 | flyctl-actions/setup-flyctl@v1.5 在 GHA Ubuntu 24.04 runner 正常 | §7 | 失败 → 退化为 `curl -L https://fly.io/install.sh \| sh` 直装 |
| A9 | Supabase Python SDK 2.30 与 Postgres 17 兼容 | §3 | Phase 02 plan 前实验 1 次 |
| A10 | Better Stack Keyword Monitor 能 parse JSON body 找 `"status":"pass"` | §8 | 失败 → 改用 HTTP 状态码 only（200/503）+ accept loss of warn/degraded distinction |
| A11 | Vercel Hobby plan 个人套利项目 "non-commercial" 解释成立（项目尚未盈利） | §10 | 升 Pro $20/月（启动期不影响） |

⚠️ **planner 阶段必看**：A1, A2, A4, A10 是数据驱动假设，plan 阶段加 monitoring + dashboard 显式跟踪；超出阈值时触发预案而非"全栈重选"。

---

## Open Questions

1. **D-22 `/health` 是否真要 internal only？**
   - 已知：Better Stack 公网 ping 需要公开 endpoint
   - 不清楚：用户 discuss 时是否区分 `/health` (public ok) vs `/scan` (internal only) 的语义
   - 推荐：plan 阶段澄清；按"D-22 仅指 `/scan`"实施，`/health` 走 Fly Anycast public

2. **Phase 02 是否启动 Supabase Auth 多账户？**
   - D-20 锁定 "magic link + email whitelist 单用户"
   - 启动期是否真有第二个 dashboard user？
   - 推荐：Phase 02 单用户即可；M3 多账户进入时再扩

3. **Axiom 500GB 月 ingest 多久撞顶？**
   - 估算：Fly stdout JSON ~ 200B/log line × 平均 1000 log/snapshot × 60 snapshots/month = ~12MB；加 chaos + scheduled jobs ≈ 100MB/月
   - 距 500GB 5000x，启动期不可能撞
   - 但若 daemon bug 进入 log loop（每秒 100 行） → 25GB/天 → 撞顶
   - 推荐：加 axiom usage alert（90% threshold）

4. **`fly cron` vs always-on daemon 内部 scheduler 的选择是否最佳？**
   - 上面方案：scheduled machines（每次起独立机器跑 snapshot）+ always-on machine 跑 HTTP
   - 替代：单 always-on machine 内置 APScheduler 跑 cron + HTTP
   - 推荐：scheduled machines（fly 原生 + 失败重启策略 + cron miss 补跑）— **但 plan 阶段要确认 fly scheduled machines 在 2026-05 仍是推荐做法**（与 always-on 内置 cron 是社区分歧点）

5. **R2 upload 是 sync 还是 async？**
   - sync：snapshot orchestrator 阻塞等待 R2 完成才返回
   - async：snapshot 返回后台 task push R2
   - 推荐：**sync** — R2 PUT 1MB parquet ~ 200ms 可接受；async 增加状态管理复杂度

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| Python 3.12 | runtime | ✓ (Fly base image / local) | 3.12.x | — |
| uv | build + runtime | ✓ (astral image) | 0.5.0 | install via `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Docker | local dev + CI build | ✓ local + Fly remote | latest | — |
| flyctl CLI | local deploy + CI | ✓ via setup-flyctl@v1.5 in CI | latest | curl install fallback in workflow |
| Fly account + app | prod | ✗ — **needs creation in Wave 0** | — | `flyctl apps create polyarb-l1 --org personal` |
| Fly Volume | prod data | ✗ — **needs creation in Wave 0** | — | `flyctl volumes create polyarb_data --size 5 --region ams` |
| Supabase project | prod DB | ✗ — **needs creation in Wave 0** | — | supabase.com console 创建 Dublin 项目 |
| Cloudflare R2 bucket + API token | prod object storage | ✗ — **needs creation in Wave 0** | — | wrangler CLI / dashboard |
| Vercel project | dashboard | ✗ — **needs creation in Wave 0** | — | `npx vercel link` |
| Sentry project + DSN | error tracking | ✗ — **needs creation in Wave 0** | — | sentry.io 创建 polymarket-arbitrage 项目 |
| Axiom dataset + ingest token | logs | ✗ — **needs creation in Wave 0** | — | axiom.co 创建 polyarb-prod dataset |
| Better Stack monitor + Telegram bot | uptime | ✗ — **needs creation in Wave 0** | — | betterstack.com 配置；@BotFather 创建 Telegram bot |
| pgloader (data migration) | NOT needed | — | — | 决策选方案 B，**不**需要一次性迁移 |

**Missing dependencies with no fallback**：

- ⚠️ Fly account + 信用卡（启动期 ~ $5-10/月预算用户已接受）— Wave 0 第一步
- ⚠️ Cloudflare account + R2 API token

**Missing dependencies with fallback**：

- 全部 SaaS 账户都是 Wave 0 手工注册 + 配置 secrets；plan 必须显式列出"账户准备 checklist"作为 Wave 0 任务

---

## Project Constraints (from CLAUDE.md)

| Constraint | Source | Phase 02 应用 |
|---|---|---|
| 使用 uv，禁用 `pip install` | CLAUDE.md §技术栈 | Dockerfile + GHA 全 uv；新依赖必须 `uv add` |
| Makefile 是统一命令入口 | CLAUDE.md "命令入口约定" | Phase 02 加 `make deploy` / `make smoke-test` / `make tail-logs` / `make supabase-migrate` / `make r2-list` / `make logs-tail` / `make sentry-test` |
| loguru 替代 stdlib logging | CLAUDE.md §6 | uvicorn / starlette / httpx stdlib logging 全 intercept 进 loguru |
| 反对学院派大而全 | CLAUDE.md §核心原则 | FastAPI 选 Starlette；不引入 Pydantic 复刻 schema；不一开始就 Market State dataclass |
| Plan 末必出 SUMMARY | CLAUDE.md "每个 Plan 末" | Phase 02 每个 plan 必走 `.githooks/pre-commit` SUMMARY 校验；deploy.yml CI 加 planning-status check |
| 不在根目录写临时 md / 测试 | CLAUDE.md §2 | tests/ 子目录；docs/ 子目录 |
| 教学文档持续产出 | CLAUDE.md "教学文档持续产出" | Phase 02 末加 `docs/learning/08-生产化部署.md`（部署链路 + 监控解读 + scan trigger 流程图）|
| 中文沟通 / 英文代码 / 中文 docs/ | CLAUDE.md §14 | 本文档中英混合（决策清单 + 代码 = 英文；解读 + 教学 = 中文）|
| 禁止 `git commit --no-verify` | CLAUDE.md §12 + planning hygiene | Phase 02 deploy workflow 不绕 hook；CI 跑 planning-status |
| 优先 Context7 + Jina | CLAUDE.md §7 | 本文档已用 [WebFetch+WebSearch 多源验证]（无 jina/context7 MCP at researcher 时段，已 fallback 公开搜索）|

---

## Metadata

**Confidence breakdown:**

- Standard stack: HIGH — versions verified against PyPI 2026-05-12；docs accessed within 14 days
- Architecture: HIGH — CONTEXT 22 decisions lock 80% paths；agent's-Discretion 7 项 with cross-verified推荐
- Pitfalls: HIGH — LEARNINGS L1-L12 + S1-S8 + thread §2.5.a 5 维度缺口直接 feed 进 Validation Architecture
- Security: MEDIUM — `/scan` HMAC 是 Starlette middleware 自实现（业内通用但本项目首次落地）；ASVS V14 与 daemon 内嵌 endpoint 适配 partial（标准是 web service-centric）
- Cost estimation: MEDIUM — Fly + Supabase + R2 都是按量；启动期实际成本受 traffic 影响

**Research date:** 2026-05-12
**Valid until:** 2026-06-12（30 天 — 此 stack 主流稳定；唯一 fast-moving 是 uv 版本，需要定期 bump）

---

*Phase: 02-l1-production-grade*
*Workstream: m1-perception*
*Research generated: 2026-05-12*
*Next step: `/gsd-plan-phase 02 --ws m1-perception`*
*Consumed by: planner agent — discretion 7 项已 resolved，可直接进入 plan*
