# Phase 02: L1 production-grade long-running — Pattern Map

**Mapped:** 2026-05-12
**Workstream:** m1-perception
**Files analyzed:** 36 (new + modify)
**Analogs found:** 24 / 36
**Source decisions:** D-01..D-22 (CONTEXT.md) + §3–§9 affected file paths (RESEARCH.md)

---

## 0. 关键事实（planner 引用前先看）

1. **Phase 02 一半新文件没有本仓库内 analog**（Dockerfile / fly.toml / GHA / Vercel / Alembic / Telegram bot）—— 这些走 RESEARCH.md `## Don't Hand-Roll` + `## Architecture Patterns` 中给出的"业内主流范本" + Context7 实时官方文档（**调用前先 resolve-library-id**）。
2. **另一半（HTTP server / scheduler / Supabase mirror / R2 sync / page_fetched_at_ms / scan endpoint）确实有 Phase 01.1 内 analog 可直接 copy 模式**——本文件聚焦这部分。
3. **强复用纪律 P1（Trust-split）**：`/scan` endpoint 必须 100% 复用 `observation/scanner.py` 的 `run_recipe` + 4 层 SQL 防御 + `Recipe._is_trusted` 工厂方法；**不允许在 Supabase 端、Vercel 端、`/scan` handler 内重复实现任何一层**（D-21）。这是 LEARNINGS L11+S5 经验的硬纪律。
4. **强复用 P7（Schema 演进只能加列）**：`page_fetched_at_ms` 必须作为 nullable 列加入，不能 rename 任何旧列；旧 parquet 由 DuckDB `union_by_name=true` 自动 NULL 填充。
5. **强复用 P8（Plan-末 SUMMARY 三件套）**：Phase 02 每个 plan 落 commit 前必须生成 `02-{NN}-SUMMARY.md`，pre-commit hook 会拦截（`.githooks/pre-commit`）。
6. **强复用 L11（Makefile 是 first-class 测试目标）**：`make deploy` / `make smoke-test` / `make tail-logs` 等所有新 target 必须有 `make -n` dry-run 测试（参考 `tests/m1-perception/test_makefile_contract.py`），且必须有真正调用路径的"triple check"（exit 0 ≠ 实际成功 — L11 silent failure 根因）。

---

## 1. File Classification

### 1.1 NEW files — Python / 配置 (Phase 01.1 内有 analog)

| New file | Role | Data flow | Closest analog | Match quality |
|---|---|---|---|---|
| `src/polyarb/http/app.py` | entry-point (HTTP) | request-response | `src/polyarb/snapshot/cli.py` | role-shift（typer→Starlette），架构同源 |
| `src/polyarb/http/health.py` | observability handler | request-response (read-only SQL) | `src/polyarb/observation/scanner.py` + `src/polyarb/snapshot/cli.py` | partial（read-only SQLite URI 复用） |
| `src/polyarb/http/scan.py` | controller (HTTP) | request-response | `src/polyarb/cli_observation.py` scan command | exact（同 run_recipe 包装） |
| `src/polyarb/http/__init__.py` | package init | — | `src/polyarb/observation/__init__.py` | exact |
| `src/polyarb/daemon/main.py` | entry-point (long-running) | event-driven (asyncio.gather) | `src/polyarb/snapshot/cli.py` | role-shift（one-shot→daemon），主体重写 |
| `src/polyarb/daemon/scheduler.py` | service (scheduling) | event-driven (timer + state machine) | **NO analog** — 现有 orchestrator 是 one-shot pipeline | none |
| `src/polyarb/daemon/alerts.py` | observability adapter | event-driven (fire-and-forget) | `src/polyarb/translation/translator.py`（外部 API 调用，retry，fail-soft 模式） | partial |
| `src/polyarb/daemon/__init__.py` | package init | — | `src/polyarb/observation/__init__.py` | exact |
| `src/polyarb/storage/supabase_mirror.py` | storage adapter | post-write mirror (CRUD upsert) | `src/polyarb/storage/sqlite_store.py` | role-match（同样是 storage writer，不同后端） |
| `src/polyarb/storage/r2_sync.py` | storage adapter | file upload (one-shot per snapshot) | `src/polyarb/storage/parquet_writer.py` | role-match（同样是 atomic write，不同 sink） |
| `src/polyarb/observability/logging.py` | observability config | startup hook | `src/polyarb/snapshot/cli.py` `_setup_logger` snippet | partial（扩展 loguru intercept） |
| `src/polyarb/observability/sentry.py` | observability config | startup hook | `src/polyarb/translation/config.py`（pydantic-settings env-load 模式） | partial |
| `scripts/supabase_seed.py` | bootstrap script | one-shot | `scripts/planning_status.py` | role-match（runtime script，typer/argparse） |
| `scripts/deploy_smoke.sh` | smoke test | shell | **NO analog** — 项目内没有 shell script | none |

### 1.2 NEW files — Tests (Phase 01.1 测试都是直接 analog)

| New test file | Coverage | Closest analog | Match quality |
|---|---|---|---|
| `tests/test_health_endpoint.py` | /health pass/warn/fail (§8) | `tests/m1-perception/test_observation_scanner.py` + freezegun | role-match（async + monkeypatch + freezegun） |
| `tests/test_http_scan.py` | HMAC + 4 层防御复用 (§9 / D-21) | `tests/m1-perception/test_observation_scanner.py` | exact（同一个 run_recipe 路径） |
| `tests/test_supabase_mirror.py` | post-write mirror (§3) | `tests/m1-perception/test_sqlite_store.py` | role-match（mock external client） |
| `tests/test_r2_sync.py` | parquet → R2 (§4) | `tests/m1-perception/test_parquet_writer.py` | role-match |
| `tests/test_scheduler.py` | 3-failure pause (D-13) | **NO analog** — freezegun + state machine 需要新设计 | none |
| `tests/test_page_fetched_at_ms.py` | per-page stamp (§5) | `tests/m1-perception/test_normalizer.py` | exact |
| `tests/test_schemas.py::test_page_fetched_at_ms_nullable` | DDL + parquet schema (§5) | `tests/m1-perception/test_schema_lockstep.py` | exact（直接扩展现有） |
| `tests/test_chaos_gamma_5xx.py` | 5xx 后 retry 行为 | `tests/m1-perception/test_gamma_client.py`（respx 已有 5xx test） | exact |
| `tests/test_chaos_3failures_pause.py` | 连续失败 daemon 暂停 | `tests/test_scheduler.py`（同上，none） | none |
| `tests/test_logging.py` | loguru JSON serialize | `tests/m1-perception/test_snapshot_cache.py` 的 fixture 风格 | partial |
| `tests/test_makefile_triple_check.sh` | exit 0 ↔ SQLite row +1 ↔ parquet 落地 | `tests/m1-perception/test_makefile_contract.py`（同样 subprocess.run + grep） | exact（但 .sh vs .py） |
| `tests/test_parquet_sqlite_consistency.py` | parquet 行数 == SQLite count | `tests/m1-perception/test_orchestrator.py` T6.1（已 e2e assert SQLite + Parquet） | partial（要扩展） |
| `tests/test_docker_smoke.sh` | docker build + /health 200 | **NO analog** | none |

### 1.3 NEW files — Deploy / Infra (NO codebase analog；走 RESEARCH 给的范本)

| New file | Role | RESEARCH 章节给的范本 | Reference |
|---|---|---|---|
| `Dockerfile` | container build | §6 完整范本（已贴）| docs.astral.sh/uv/guides/integration/docker/ + 3th-party/clawfirm/deploy/Dockerfile（partial — clawfirm 是 Go + alpine，仅 non-root user + HEALTHCHECK 思路可借鉴） |
| `.dockerignore` | build exclusion | §6 列表 | — |
| `fly.toml` | Fly Machine config | §4 完整范本 | fly.io/docs |
| `.github/workflows/ci.yml` | CI pytest gate | §7 完整范本 | astral-sh/setup-uv@v3 |
| `.github/workflows/deploy.yml` | flyctl deploy | §7 完整范本 | superfly/flyctl-actions@v1.5 |
| `alembic.ini` + `alembic/env.py` + `alembic/versions/001_initial.py` | Supabase migrations | §3 草稿 | alembic 1.16 docs |
| `.env.example` | secret enumeration | §Runtime State Inventory 列出全部 keys | — |
| `dashboard/` Next.js 子目录 | Vercel frontend | §10 + D-19/D-20 | RESEARCH 没给完整范本，需 Context7 fetch `vercel/next.js` + `supabase/supabase-js` 当代文档 |

### 1.4 MODIFY files (Phase 01.1 既有文件)

| Modified file | 改动 | 影响 |
|---|---|---|
| `src/polyarb/snapshot/orchestrator.py` | step 7 后加 7.5 (Supabase mirror) + 7.6 (R2 sync)；clob_done_ms 字段语义注释 | LEARNINGS L2 + §3 + §4 |
| `src/polyarb/snapshot/normalizer.py` | 接受 `_page_fetched_at_ms` private key 并转写到 `page_fetched_at_ms` | §5 |
| `src/polyarb/clients/gamma_client.py` | `_paginate` 在每页 stamp `page_fetched_at_ms` 并附加到 raw dict | §5 |
| `src/polyarb/storage/schemas.py` | markets DDL + MARKETS_COLUMN_ORDER + MARKETS_INSERT_SQL + SNAPSHOT_SCHEMA **4 处同步**加 `page_fetched_at_ms`（参考 P7 schema 演进硬约束 + LEARNINGS 0 节"关键三处同步" → 现在 4 处） | §5 |
| `src/polyarb/storage/sqlite_store.py` | `_row_to_tuple` 自动跟 MARKETS_COLUMN_ORDER 走，无显式改动；新增 `get_latest_snapshot()` 读 helper (供 /health) | §3 + §8 |
| `src/polyarb/storage/__init__.py` | export `SupabaseMirror` + `upload_parquet_to_r2` | §3 + §4 |
| `pyproject.toml` | 加 `starlette` `uvicorn[standard]` `supabase` `boto3` `sentry-sdk` `alembic`；dev 加 `aioresponses`（可选） | §3 + §4 + §6 + §8 + §9 |
| `Makefile` | 加 `deploy` `smoke-test` `tail-logs` `soak-status` `docker-build` `docker-run-local` `supabase-migrate` `supabase-reconcile` `r2-list` `r2-restore` 等 ~10 个 target | L11 + 全 §节 |
| `.env.example` | 加 `FLY_API_TOKEN` `SUPABASE_URL` `SUPABASE_SERVICE_KEY` `R2_*` `SENTRY_DSN` `AXIOM_TOKEN` `BETTER_STACK_HEARTBEAT_URL` `SCAN_SHARED_SECRET` `TELEGRAM_BOT_TOKEN` | §Runtime State Inventory |
| `.githooks/pre-commit` | 可能加 fly.toml / Dockerfile lint hook（推到 02.1） | P8 |
| `tests/m1-perception/test_makefile_contract.py` | 扩展覆盖 Phase 02 新 target（make deploy / smoke-test 等的 dry-run） | L11 |
| `tests/m1-perception/conftest.py` | 加 `settings_for_test_with_supabase_mirror` fixture（mock SupabaseMirror）+ `settings_for_test_with_r2` fixture（mock boto3） | §3 + §4 |

---

## 2. Pattern Assignments

### 2.1 `src/polyarb/http/scan.py` (controller, request-response)

**Analog:** `src/polyarb/cli_observation.py` `scan` command (lines 65-107) + `src/polyarb/observation/scanner.py`

**为什么是 exact match**：D-21 锁定 scan endpoint = "复用 Phase 01.1 4 层 SQL 防御 + Trust-split"。cli_observation.py 的 scan command 已经做了完全正确的"拿名字 → 查 dict → 调 run_recipe"路径；HTTP handler 只是把 typer.Option 换成 HTTP body 解析，把 typer.Exit 换成 JSONResponse。

#### Pattern A：Recipe lookup（直接照搬 cli_observation.py:84-99）

```python
# cli_observation.py:84-99 （source — 直接照搬，不在 handler 重做）
recipes = list_all_recipes(yaml_path if yaml_path.exists() else None)
if name not in recipes:
    typer.echo(f"unknown recipe: {name!r}. Available: {sorted(recipes)}", err=True)
    raise typer.Exit(1)
recipe = recipes[name]
try:
    if recipe.group_by:
        df = run_recipe_grouped(settings.db_path, recipe)
    else:
        df = run_recipe(settings.db_path, recipe)
except (ValueError, sqlite3.OperationalError) as e:
    typer.echo(f"scan failed: {e}", err=True)
    raise typer.Exit(1) from e
```

**HTTP 版改写规则**：
- `typer.echo(..., err=True) + typer.Exit(1)` → `return JSONResponse({"error": ...}, status_code=400|404)`
- `name` 参数从 `typer.Option` → `body = await request.json(); recipe_name = body.get("recipe_name")`
- **Layer 1/2/3/4 防御完全不复制**——通过调 `run_recipe` 自动应用（P1 trust-split 复用）
- **加 input length 限制**：`if not isinstance(recipe_name, str) or len(recipe_name) > 64: return 400`（V5 ASVS）

#### Pattern B：HMAC auth middleware（RESEARCH §9 已给完整范本，line 1445-1465）

```python
# RESEARCH 02-RESEARCH.md:1445-1465 已经给的完整 scan_auth_middleware
async def scan_auth_middleware(request: Request, call_next, *, secret: str):
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

**Pitfalls**:
- ⚠️ `T-02-01 /scan flood` — D-22 锁定 Fly internal only。但仍需 starlette rate-limit middleware（启动期可推迟）
- ⚠️ `T-02-03 SQL injection via recipe_name` — recipe_name **绝不能拼进 SQL**；仅作 `recipes[recipe_name]` dict lookup。Layer 1 / 2 / 3 / 4 自动应用于 recipe.where + recipe.order_by + recipe.limit（user-supplied 的 params dict 不暴露给 SQL）

#### Differences from analog
- cli_observation.py 还会调 `render_table` + `write_scan_parquet` —— HTTP handler 不做这两件事；返回 `df.head(100).to_dict(orient="records")` JSON 即可（RESEARCH §9 line 1438-1442）
- exit code → HTTP status code 映射：`ValueError` → 400 / `KeyError`/unknown recipe → 404 / `sqlite3.OperationalError` → 500
- 加 row_count 截断：`df.head(100)`（防止巨型 response 让 Vercel Edge timeout — RESEARCH §9 Pitfall "scan 处理时间 > 30s"）

---

### 2.2 `src/polyarb/http/health.py` (observability handler, read-only)

**Analog A:** `src/polyarb/observation/scanner.py:142-145`（read-only SQLite URI 模板）
**Analog B:** RESEARCH §8 line 1233-1303（完整 health endpoint 范本）

#### Pattern A：read-only SQLite URI（scanner.py:142-145）

```python
# scanner.py:142-145
uri = f"file:{db_path}?mode=ro"
con = sqlite3.connect(uri, uri=True)
try:
    # ... only SELECT ...
finally:
    con.close()
```

**直接复用规则**：health endpoint 的 SQLite 读全部走 `mode=ro` URI；查询限于 `SELECT MAX(taken_at_ms) FROM snapshots LIMIT 1`（< 50ms — 见 RESEARCH §8 Pitfall "SQLite read 阻塞 → 30s timeout"）。

#### Pattern B：三态映射（已有现成）

```python
# determine_snapshot_status() 已在 validator/layers.py:279 实现
# SnapshotStatus enum: OK / DEGRADED / FAILED
# /health 直接调:
#   from polyarb.validator.layers import SnapshotStatus
#   status_map = {SnapshotStatus.OK: "pass", SnapshotStatus.DEGRADED: "warn", SnapshotStatus.FAILED: "fail"}
```

**好处**：D-12 三态判定（OK/DEGRADED/FAILED）已在 validator amendment 落地；health endpoint 只是把 enum 翻译成 IETF schema 的 `pass/warn/fail`。LEARNINGS L12 "二态太粗" 的修复直接受益。

#### Pattern C：JSONResponse Content-Type 设置

RESEARCH §8 line 1243 给的 `HEALTH_CONTENT_TYPE = "application/health+json"` — 必须用 `media_type=` 参数显式传给 `JSONResponse`（不是 `headers=`）。

#### Pitfalls
- ⚠️ HTTP 503 让 Fly Anycast 自动剔除实例 → 永远不恢复 — fly.toml 必须 `restart.policy = "on-failure"`（RESEARCH §8 Pitfall #2）
- ⚠️ time 字段始终 UTC ISO8601 + Z 后缀 — 不依赖容器时区（虽然 Dockerfile 已 pin `TZ=UTC`，但 ISO 字符串自己也明示）

---

### 2.3 `src/polyarb/http/app.py` (entry-point, ASGI factory)

**Analog:** `src/polyarb/cli_observation.py:49`（app 工厂模式）

```python
# cli_observation.py:49 — typer.Typer factory
app = typer.Typer(no_args_is_help=True, add_completion=False)
```

**改写规则**（RESEARCH §9 line 1372-1398 已给完整 starlette 范本）：
- `typer.Typer(...)` → `Starlette(routes=[...], middleware=[...])`
- `app.command()` decorator → `Route("/path", handler, methods=[...])`
- typer 装配在 module top-level → Starlette 装配在 `create_app(scheduler, sqlite_store, settings)` 函数里（让 daemon/main.py 注入依赖）

**关键设计**：用 `app.state.scheduler` / `app.state.sqlite_store` / `app.state.settings` 把依赖注入给 handler（RESEARCH §9 line 1394-1398）；不要用全局 module-level 变量。

---

### 2.4 `src/polyarb/daemon/main.py` (entry-point, long-running)

**Analog:** `src/polyarb/snapshot/cli.py`（typer entry-point + asyncio.run）

```python
# snapshot/cli.py:64-67 — 现有 entry-point 模式
settings = load_settings(config)
mode = "full" if full else "subset"
result = asyncio.run(run_snapshot(settings, mode=mode, use_cache=not no_cache))
```

**Differences**（daemon 是 long-running，不是 one-shot）：
- 把 `asyncio.run(run_snapshot(...))` 替换成 `asyncio.run(main())` 内部 `asyncio.gather(server.serve(), scheduler.run(stop_event))`（RESEARCH §Architecture Patterns Pattern 1，line 295-349 完整范本）
- 加 signal handler（SIGINT/SIGTERM）让 stop_event 触发 graceful shutdown
- 加 `init_logging()` + `init_sentry()` 启动顺序：**必须在任何 logger.info 之前**（否则 logs 不会进 Axiom JSON）

#### Pattern：signal handler graceful shutdown（直接抄 RESEARCH §Pattern 1 line 329-344）

```python
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
```

#### Pitfalls
- ⚠️ uvicorn 默认 stdlib logging — 必须先 `init_logging()` 装 InterceptHandler，否则 uvicorn access log 不进 Axiom（RESEARCH §9 line 1471-1513 完整 InterceptHandler 范本）
- ⚠️ scheduler 失败时 server 不能跟着死 — `asyncio.gather(..., return_exceptions=True)` 必须开

---

### 2.5 `src/polyarb/daemon/scheduler.py` (service, state machine + timer)

**No exact analog in codebase.**

但有两个 partial：
1. `src/polyarb/snapshot/orchestrator.py:81-93` — `_phase` 上下文管理器 + 计时（可借鉴 "每个 cron tick 包一层 try + 计时"）
2. `src/polyarb/storage/sqlite_store.py:108-222` — BEGIN IMMEDIATE 事务包整个 write_snapshot（可借鉴 "整个 cron tick 原子化" 的纪律）

**新增需求（D-13）：3-failure pause 状态机**
```
state ∈ {RUNNING, PAUSED}
counter ∈ {0, 1, 2, 3}

on_cron_tick():
    if state == PAUSED:
        log "paused, skipping tick"
        return
    try:
        result = await run_snapshot(...)
        if result.status == "ok":
            counter = 0
        elif result.status == "degraded":
            counter = 0   # DEGRADED 也算"成功" — D-12 amendment
        else:   # failed
            counter += 1
    except Exception:
        counter += 1
        ...
    if counter >= 3:
        state = PAUSED
        await alerts.send_paused_alert()
```

**测试设计参考**（也无 analog，需新建 freezegun pattern）：
```python
# tests/test_scheduler.py — 用 freezegun 推进时钟 + monkeypatch run_snapshot
@freeze_time("2026-05-12 00:00:00")
def test_pause_after_3_failures(monkeypatch):
    scheduler = SnapshotScheduler(...)
    monkeypatch.setattr(scheduler, "_run_snapshot_async",
                        AsyncMock(side_effect=Exception("API down")))
    # tick 3 次
    for _ in range(3):
        await scheduler._tick()
    assert scheduler.state == State.PAUSED
    # 第 4 tick 必须 skip
    await scheduler._tick()
    assert scheduler._run_snapshot_async.call_count == 3  # 没再被调
```

#### Pitfalls
- ⚠️ Fly scheduled machine 与 always-on machine 写竞争（RESEARCH §4 Pitfall #1）— **always-on machine 只读 SQLite**，所有写都来自 scheduled machine；scheduler 状态写也走 SQLite（snapshots 表的 latest row 的 status 字段）
- ⚠️ daemon 重启时 counter 重置 → 可能掩盖 3-failure 信号 — 把 counter 持久化到 SQLite（新表 `scheduler_state` 或扩展 `snapshots.notes` JSON），daemon 启动时从 SQLite recovery

---

### 2.6 `src/polyarb/storage/supabase_mirror.py` (storage adapter, post-write mirror)

**Analog:** `src/polyarb/storage/sqlite_store.py`

#### Pattern A：构造器 + 资源管理（sqlite_store.py:92-98）

```python
# sqlite_store.py:92-98
class SQLiteStore:
    def __init__(self, db_path: Path) -> None:
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def db_path(self) -> Path:
        return self._db_path
```

**复用规则**：`SupabaseMirror.__init__(url, service_key)` 一次性建立 `supabase.create_client(...)` 长生命客户端，类似 GammaClient 的 httpx.AsyncClient pattern（**不要每次 push 都建 client**）。

#### Pattern B：失败不抛、失败成 Issue 的"记录不抛"精神（orchestrator.py:194-204）

```python
# orchestrator.py:194-204 — Gamma 失败成 Issue 模板
try:
    raw_events = await gamma.fetch_all_active_events()
except Exception as e:  # noqa: BLE001
    logger.error(f"Gamma /events fetch failed: {e!r}")
    issues.append(Issue(
        layer=1, category=Category.API_UNREACHABLE,
        market_id=None, detail=f"Gamma /events unreachable: {str(e)[:200]}",
    ))
    raw_events = []
```

**复用规则**：SupabaseMirror.push_snapshot() 内部 try/except 包整个调用，**失败 → log error + 不抛** → 让上层（orchestrator）把 snapshot 标 DEGRADED（D-12 三态）。具体范本 RESEARCH §Pattern 2 line 379-396 已给完整。

#### Pattern C：narrow schema + DELETE-then-INSERT（sqlite_store.py:153）

```python
# sqlite_store.py:153 — 现有 DELETE FROM markets 模板
con.execute("DELETE FROM markets")  # full overwrite (D-C1)
```

**Supabase mirror 改写**：`self._client.table("markets_latest").delete().neq("market_id", "").execute()` 然后 chunked bulk insert（postgrest body size limit 默认 1MB ~ 5k rows，所以 RESEARCH §Pattern 2 line 391 给的 `_chunk(market_rows, 1000)` 是必要的）。

#### Pitfalls (RESEARCH §3 "Known pitfalls + mitigations")
- ⚠️ Supabase Free 500MB DB — 启动期 narrow schema 估算 < 100MB / 月
- ⚠️ Supabase Free 1 周无访问暂停 — 每天 2 次 cron 写入即可保活
- ⚠️ service_role key 泄露 — flyctl secrets 存（不进 git，不进 GHA）— D-07 锁
- ⚠️ 数据漂移（mirror 失败累积）— `snapshots` 表 upsert idempotent；daemon 启动时跑 reconcile（对比 last SQLite snapshot_id vs last Supabase snapshot_id，补差）

---

### 2.7 `src/polyarb/storage/r2_sync.py` (storage adapter, atomic file upload)

**Analog:** `src/polyarb/storage/parquet_writer.py` (64 行整体)

#### Pattern A：atomic write 思想（parquet_writer.py:50-63）

```python
# parquet_writer.py:50-63 — atomic write
out_path = Path(out_path)
out_path.parent.mkdir(parents=True, exist_ok=True)
table = pa.Table.from_pylist(rows, schema=SNAPSHOT_SCHEMA)
tmp = out_path.with_suffix(out_path.suffix + ".tmp")
try:
    pq.write_table(table, tmp, compression="snappy")
except Exception:
    tmp.unlink(missing_ok=True)
    raise
os.replace(tmp, out_path)
```

**R2 sync 改写规则**：
- R2 是 S3-compatible — 用 boto3 `s3.upload_file(Filename=str(parquet_path), Bucket=..., Key=...)` 即可（boto3 multipart upload 已内置 atomic — 失败不留 partial object）
- key 拼接走 `f"{year}/{month:02d}/{day:02d}/{HH-MM-SS}.parquet"` — **绝不接受用户输入**（F-12 V12 ASVS 防 path injection；T-02-04 mitigation）
- 失败 → log error + raise Issue（不抛）— 同 SupabaseMirror 的 fail-soft 模式（D-12 DEGRADED）

#### Pattern B：deterministic key computation（parquet_writer.py:25-38）

```python
# parquet_writer.py:25-38 — UTC-based deterministic path
def compute_snapshot_path(parquet_root: Path, taken_at_ms: int) -> Path:
    dt = datetime.fromtimestamp(taken_at_ms / 1000, tz=timezone.utc)
    return (
        Path(parquet_root)
        / dt.strftime("%Y")
        / dt.strftime("%m")
        / dt.strftime("%d")
        / dt.strftime("%H-%M-%S.parquet")
    )
```

**R2 sync 直接复用**：`def compute_r2_key(taken_at_ms: int) -> str` 拷贝同一个 UTC-format 逻辑，只输出 string key（不是 Path）。

#### Pitfalls (RESEARCH §4 "Known pitfalls + mitigations")
- ⚠️ R2 PUT 503 / 网络抖 — boto3 retry config（`Config(retries={'max_attempts': 3, 'mode': 'standard'})`）
- ⚠️ R2 cost 失控 — 启动期 2 snapshots/day × 365 = 730 PUT/year，远低于 1M/月 free（可忽略）
- ⚠️ 上传失败时下次 snapshot 补传 — `snapshots.parquet_url` 字段允许 NULL；reconcile 路径补传

---

### 2.8 `src/polyarb/observability/logging.py` (loguru intercept + JSON)

**Analog A:** `src/polyarb/snapshot/cli.py:57-62` — 现有 `_setup_logger` 模板（最小版）

```python
# snapshot/cli.py:57-62 — 现有 _setup_logger
logger.remove()
logger.add(
    sys.stderr,
    level="DEBUG" if verbose else "INFO",
    format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
)
```

**Phase 02 升级路径**：
- `sys.stderr` → `sys.stdout`（Fly 推 stdout 给 Axiom）
- `format=...` 彩色 console format → `serialize=True` JSON format（loguru built-in，免手写 dict→json.dumps — RESEARCH "Don't Hand-Roll" 表）
- `level="DEBUG" if verbose else "INFO"` → 固定 `level="INFO"`（prod 无 verbose 概念）
- **新增 InterceptHandler** 把 uvicorn / starlette / httpx stdlib logging 路由到 loguru（RESEARCH §9 line 1483-1513 完整范本，直接照抄）

#### Pitfall
- ⚠️ `backtrace=False` + `diagnose=False` 必须在 prod 设置 — 否则 loguru 会打印源文件路径 + 变量值，泄漏 T-02-07 / T-02-08 / T-02-11

---

### 2.9 `src/polyarb/observability/sentry.py` (sentry_sdk init)

**Analog:** `src/polyarb/translation/config.py`（pydantic-settings env-load 模式）

#### Pattern A：env-load + 缺失值优雅降级

```python
# translation/config.py 模式（同 polyarb/config.py:43）
model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")
```

**Sentry 改写规则**：
- 从 env var 读 `SENTRY_DSN` — 缺失则跳过 `sentry_sdk.init()`（local dev 不需 Sentry）
- `init(send_default_pii=False)` — T-02-08 mitigation
- 加 `before_send` hook 过滤敏感字段（同 T-02-07 redact filter 思路）
- loguru integration **自动启用** — RESEARCH "Don't Hand-Roll" 表 line 430 "sentry-sdk 检测到 loguru 依赖自动 enable"，不需要手动配

---

### 2.10 `src/polyarb/snapshot/normalizer.py` (modify — page_fetched_at_ms)

**Analog:** 该文件自身（lines 83-130，`normalize_market` 核心逻辑）

#### Pattern：增量字段注入（仿照现有 `market_to_event_map` 注入模式）

```python
# normalizer.py:83-85（现状）
def normalize_market(
    raw: dict, market_to_event_map: dict[str, str] | None = None
) -> dict | None:
```

**Phase 02 改动（§5 line 822-828）**：
```python
# normalizer.py:83+ AFTER
def normalize_market(raw: dict, ...) -> dict | None:
    return {
        ...
        "fetched_at_ms": None,  # placeholder, orchestrator stamps stage 5
        "page_fetched_at_ms": raw.get("_page_fetched_at_ms"),  # NEW
    }
```

**关键纪律 P7（LEARNINGS）**：
- 加列，**不 rename** `fetched_at_ms`（保留旧字段名 + 加 schema-level 注释解释语义）
- 老 parquet 读取时 `union_by_name=true` 自动 NULL 填充新列（DuckDB 已经在 diff.py / tracker.py 用了这模式 — D5）

---

### 2.11 `src/polyarb/clients/gamma_client.py` (modify — per-page stamp)

**Analog:** 该文件自身（lines 176-222，`_paginate` 循环）

#### Pattern：per-page hook 注入

```python
# gamma_client.py:176-218 — 现有 _paginate 循环
async def _paginate(self, *, path, params, label):
    while True:
        page_params = {**params, "limit": self.PAGE_LIMIT, "offset": offset}
        page = await self._get(path, page_params)
        if not isinstance(page, list):
            raise RuntimeError(...)
        out.extend(page)
        ...
```

**Phase 02 改动**：在 `out.extend(page)` 之前注入 stamp：
```python
page_fetched_at_ms = int(time.time() * 1000)
for raw in page:
    raw["_page_fetched_at_ms"] = page_fetched_at_ms  # private key → normalizer pulls
out.extend(page)
```

**为什么 `_` 前缀**：标记这是 internal carry field — 不污染真正的 Polymarket API 字段名（RESEARCH §5 Pitfall #2 mitigation）。

---

### 2.12 `src/polyarb/storage/schemas.py` (modify — 加 column with 4-point lockstep)

**Analog:** 该文件自身（lines 91-114 DDL；lines 157-180 MARKETS_COLUMN_ORDER + INSERT_SQL；lines 237-263 SNAPSHOT_SCHEMA）

#### LEARNINGS 0 节"关键三处同步"现在升级为**4 处同步**：

| Sync point | Line | Add |
|---|---|---|
| 1. DDL `CREATE TABLE markets(...)` | schemas.py:110 | `page_fetched_at_ms INTEGER,` (nullable) |
| 2. `MARKETS_COLUMN_ORDER` tuple | schemas.py:157 | `"page_fetched_at_ms",` |
| 3. `MARKETS_INSERT_SQL` SQL string | schemas.py:182 | 加 column name + 加 `?` placeholder |
| 4. `SNAPSHOT_SCHEMA` pyarrow.Schema | schemas.py:237 | `pa.field("page_fetched_at_ms", pa.int64(), nullable=True)` |

**测试 lockstep**：`tests/m1-perception/test_schema_lockstep.py` 已经在 Phase 01.1 落地了"4 点 lockstep"自检；Phase 02 加新列时该测试**自动**会因 lockstep 不一致而失败 — 这是 P7 schema 演进硬约束的安全网。

#### Pattern：events 表同样加列

`events.fetched_at_ms` 也是 stage stamp（同 L2 bug）— 同样加 `events.page_fetched_at_ms`（schemas.py:57-70 DDL + EVENTS_COLUMN_ORDER + EVENTS_INSERT_SQL；events 不在 SNAPSHOT_SCHEMA，所以只 3 点同步）。

---

### 2.13 `src/polyarb/snapshot/orchestrator.py` (modify — 加 step 7.5 + 7.6)

**Analog:** 该文件自身（lines 427-468，step 7 Persist 块）

#### Pattern A：step 7.5 Supabase mirror（紧跟 SQLite 写入之后）

```python
# orchestrator.py:451-463 — 现有 SQLite 写入
store = SQLiteStore(settings.db_path)
store.init_schema()
snapshot_id = store.write_snapshot(...)

# Phase 02 NEW — step 7.5 mirror（fail-soft，复用 Issue 模式）
try:
    mirror = SupabaseMirror(settings.supabase_url, settings.supabase_service_key)
    mirror.push_snapshot(snapshot_id, target_markets)
except Exception as e:  # noqa: BLE001
    logger.error(f"Supabase mirror failed snapshot_id={snapshot_id}: {e}")
    issues.append(Issue(
        layer=4, category=Category.UNKNOWN,
        detail=f"Supabase mirror failed: {str(e)[:200]}",
    ))
    # 不抛 — snapshot 仍标 OK/DEGRADED 取决于其它 issue
```

**复用纪律**：完全照搬 orchestrator.py:194-204 Gamma fail-soft 模板，把 `Gamma /events` 换成 `Supabase mirror`，错的 layer/category 调整。

#### Pattern B：step 7.6 R2 upload（紧跟 mirror 之后）

```python
# Phase 02 NEW — step 7.6
try:
    r2_url = upload_parquet_to_r2(
        parquet_path=parquet_path,
        bucket=settings.r2_bucket,
        key=compute_r2_key(taken_at_ms),
    )
    mirror.update_parquet_url(snapshot_id, r2_url)  # optional — Supabase 表的 parquet_url
except Exception as e:  # noqa: BLE001
    logger.error(f"R2 upload failed: {e}")
    issues.append(Issue(layer=4, category=Category.UNKNOWN, detail=f"R2 upload failed: {str(e)[:200]}"))
```

**Pitfalls**：
- ⚠️ step 7.5 / 7.6 **绝不能** 阻塞 cache.cleanup()（orchestrator.py:467-468）— cache cleanup 必须在 SQLite commit 之后无条件跑
- ⚠️ Issue 数量增加可能误判 status — `determine_snapshot_status` 把 mirror/R2 失败当作 Layer 4 issue 计入；要确认 amendment 之后 DEGRADED/FAILED 阈值仍合理（RESEARCH §3 Known pitfalls "mirror 失败时 snapshot 是否仍然算成功 - 是"）

---

### 2.14 `Makefile` (modify — Phase 02 新 target)

**Analog:** `Makefile` 自身（lines 14-219，全部现有 target 风格）

#### Pattern A：每个 target 必须遵守"4 项规则"（LEARNINGS 2.11）

1. `## <target>: <一句中文描述>` 格式（双井号 + 冒号 + 空格）— `make help` grep 这个
2. `.PHONY: <target>` 声明（防文件名冲突 shadow）
3. recipe 内 `uv run python -m polyarb.xxx` 走 uv（CLAUDE.md uv 锁定）
4. 接受 Make variable（`$(VAR)`）作为 CLI flag（如 Makefile:93 `make snapshots-purge DAYS=30 KEEP=5`）

#### Pattern B：echo 头 + tip 行（Makefile:56-60）

```makefile
# Makefile:56-60 — 现有模板
snapshot-markets:
	@echo ">> snapshot-markets (quiet mode) — PID $$$$ — started $$(date '+%Y-%m-%d %H:%M:%S')"
	@echo ">> tip: open another terminal and run 'make snapshot-status' to check progress"
	@echo ""
	uv run python -m polyarb.snapshot snapshot
```

**Phase 02 新 target 列表**（每个都加上述 echo 头）：

| Target | 用途 | Recipe 草稿 |
|---|---|---|
| `make deploy` | Fly 一键部署 | `flyctl deploy --remote-only --wait-timeout 600` |
| `make smoke-test` | 部署后 health probe | `curl -fsS https://polyarb-l1.fly.dev/health \| jq .status` + assert == "pass\|warn" |
| `make tail-logs` | 看 Axiom 实时日志 | `flyctl logs --app polyarb-l1` |
| `make soak-status` | 看 7-day soak 进度 | 调 `/health` + 计算 since-deploy 时间 + 列出 Better Stack incidents |
| `make docker-build` | 本地 docker build | `docker build -t polyarb-l1 .` |
| `make docker-run-local` | 本地 container 跑 daemon（验镜像）| `docker run -d -p 8080:8080 -v $(PWD)/data:/data polyarb-l1` |
| `make supabase-migrate` | Alembic 升级 | `uv run alembic upgrade head` |
| `make supabase-reconcile` | 修 SQLite ↔ Supabase 漂移 | `uv run python scripts/supabase_seed.py reconcile` |
| `make r2-list` | 列 R2 bucket 内容（dev convenience） | `aws s3 ls s3://polyarb-snapshots/ --endpoint-url=$$R2_ENDPOINT` |
| `make r2-restore SNAPSHOT_ID=N` | 从 R2 拉回 parquet | shell 调 `aws s3 cp` |

#### Pattern C：**L11 修复硬纪律** — 每个新 target 必须有 `make -n` dry-run 测试

参考 `tests/m1-perception/test_makefile_contract.py:37-72`，Phase 02 加新 target 时**同步加测试**（不要等用户撞 silent failure 才补）：

```python
def test_make_deploy_dry_run_recipe() -> None:
    result = subprocess.run(["make", "-n", "deploy"], cwd=PROJECT_ROOT, ...)
    assert "flyctl deploy" in result.stdout
    assert "--remote-only" in result.stdout
```

---

### 2.15 `pyproject.toml` (modify — 加 deps)

**Analog:** 该文件自身（lines 10-39 dependencies 块；lines 41-48 dev extra）

**Phase 02 加列**（按 CONTEXT/RESEARCH 决议）：

```toml
# core deps
"starlette>=0.49,<0.50",
"uvicorn[standard]>=0.32,<0.33",
"supabase>=2.10,<3",
"boto3>=1.42,<2",
"sentry-sdk>=2.20,<3",
"alembic>=1.16,<2",

# dev extra
"aioresponses>=0.7,<0.8",  # async respx alternative for chaos tests
```

**Pin 策略**（RESEARCH §Standard Stack 已锁的范围）：每个包 pin major + minor，避免 minor 内 breaking change（参考现有 `httpx[http2]>=0.27,<0.28` 模板）。

---

## 3. Shared Patterns (cross-cutting — apply to multiple Phase 02 files)

### 3.1 Loguru 一律走 stdout JSON

**Source:** `src/polyarb/snapshot/cli.py:57-62`（最小版）+ RESEARCH §9 line 1483-1513（完整 InterceptHandler）

**Apply to:** `daemon/main.py`, `daemon/scheduler.py`, `http/app.py`, `http/health.py`, `http/scan.py`, `storage/supabase_mirror.py`, `storage/r2_sync.py`

**强制规则**：
- 项目内**不允许直接 `import logging`** — 全部走 `from loguru import logger`
- prod 部署时 `logger.add(sys.stdout, serialize=True, backtrace=False, diagnose=False)`
- uvicorn / starlette / httpx 的 stdlib logging 通过 InterceptHandler 路由到 loguru

### 3.2 失败成 Issue，不抛

**Source:** `src/polyarb/snapshot/orchestrator.py:194-204`（Gamma 失败模板）

**Apply to:** `storage/supabase_mirror.py`, `storage/r2_sync.py`, `daemon/scheduler.py`, `daemon/alerts.py`

**精神**：任何外部 IO 失败（Supabase / R2 / Telegram / Better Stack heartbeat）必须 try/except 包住，转成 `Issue(layer=4, category=UNKNOWN, detail=str(e)[:200])` 累加到 issues list，**绝不让一个外部 IO 失败把 snapshot 整个 abort**（D-12 DEGRADED 语义）。

### 3.3 Python 3.12 类型注解

**Source:** `pyproject.toml:9` `requires-python = ">=3.12"`

**Apply to:** all new `.py` files

- `list[str]` / `dict[str, int]` / `X | None` — 不用 `Optional` / `List` / `Dict`
- 公开函数全签名 type annotation
- 来自 PATTERNS §3.2 of 01.1-PATTERNS.md

### 3.4 错误处理三档分明（按已落地的 P5 模式）

**Source:** Phase 01.1 P5 + `src/polyarb/clients/gamma_client.py:47-53` (_NonRetryableHTTPError pattern)

**Apply to:** `daemon/alerts.py` (Telegram + Better Stack 调用), `storage/supabase_mirror.py`

| 档 | 触发 | 处理 |
|---|---|---|
| Config error | env var 缺失 / 格式不对 | startup 即抛 / 拒启 |
| Transient error | 网络抖 / 5xx / 429 | tenacity retry exponential backoff |
| Permanent error | 4xx 非 429 / schema mismatch | 不 retry，log + Issue + continue |

### 3.5 路径校验 F-3（external paths 安全）

**Source:** `src/polyarb/config.py:45-60` field_validator

**Apply to:** 所有 Phase 02 用到 `Path` 的新模块（`storage/r2_sync.py` 本地 parquet path / `daemon/scheduler.py` lock file path）

**规则**：所有 Path 字段必须走 `Settings` 类的 `_within_project` 校验（除非测试环境 `POLYARB_ALLOW_EXTERNAL_PATHS=1`）— Fly Volume `/data` 路径要么显式 allow，要么在 `Settings.db_path` env override 时显式 trust（CONTEXT.md `POLYARB_DATA_DIR=/data` 模式）。

### 3.6 Atomic file write

**Source:** `src/polyarb/storage/parquet_writer.py:50-63`

**Apply to:** 任何 Phase 02 新增的本地文件写（如 daemon scheduler 写 lock file / state checkpoint）

**模板**：tmp file + `os.replace()` — 失败 `unlink(missing_ok=True)` + raise；不依赖文件系统原子性以外的任何东西。

### 3.7 测试 fixtures 复用（不重写）

**Source:** `tests/m1-perception/conftest.py`

**Apply to:** `tests/test_health_endpoint.py`, `tests/test_http_scan.py`, `tests/test_supabase_mirror.py`, `tests/test_r2_sync.py`, `tests/test_scheduler.py`, `tests/test_chaos_*.py`

**规则**：
- `tmp_db_path` / `tmp_parquet_root` / `tmp_cache_root` / `settings_for_test` — 直接 import
- F-3 escape hatch：`os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")` 在 module top（conftest.py 已经做了 — 子测试不用重复）
- 新加 fixtures（如 `mocked_supabase` / `mocked_r2`）放进同一个 conftest.py，不重复定义

### 3.8 Read-only SQLite URI（任何只读路径硬要求）

**Source:** `src/polyarb/observation/scanner.py:142` `f"file:{db_path}?mode=ro"`

**Apply to:** `http/health.py`, `http/scan.py`（间接通过 run_recipe）, 任何 always-on machine 上的 SQLite 读

**规则**：always-on machine 进程（HTTP server）**绝对不能写 SQLite**（只有 scheduled machine 才写）— 通过强制 `?mode=ro` URI 在 engine 层杜绝（RESEARCH §4 Pitfall #1）。

### 3.9 Trust-split 工厂方法（**Phase 02 强约束**）

**Source:** `src/polyarb/observation/recipes.py:53-73` (Recipe.from_builtin / from_yaml)

**Apply to:** `http/scan.py` — 严禁绕开

**规则**：scan endpoint 必须**经过 `list_all_recipes()` lookup** 才能拿到 Recipe 实例 — 不可让 user-supplied 字段直接构造 Recipe（绕过 `_is_trusted=False` strict validation 路径）。Phase 01.1 P1 在这里直接体现为 Phase 02 安全门。

---

## 4. No Analog Found

以下文件在项目内**没有可对照的现成代码**，planner 必须走 RESEARCH.md 的官方范本 + Context7 实时文档：

| File | Role | RESEARCH 章节 | Action for planner |
|---|---|---|---|
| `Dockerfile` | container build | §6 完整范本（line 884-958） | 直接照抄 §6，pin uv `0.5.0` + Python `3.12-slim-bookworm`；调用 Context7 `docs.astral.sh/uv` 二次确认 multi-stage 范本未变（截至 2026-05-12） |
| `fly.toml` | Fly Machine config | §4 完整范本（line 639-715） | 直接照抄 §4 — primary_region=ams + volume mount + scheduled machines for subset/full cron |
| `.dockerignore` | build exclusion | §6 exclude list | 列：`tests/ data/ .venv/ .planning/ docs/ 3th-party/ .git/` |
| `.github/workflows/ci.yml` | CI gate | §7 完整范本（line 1011-1053） | 直接照抄 |
| `.github/workflows/deploy.yml` | flyctl deploy | §7 完整范本（line 1057-1106） | 直接照抄 |
| `alembic.ini` + `alembic/env.py` + `alembic/versions/001_initial.py` | Supabase migrations | §3 草稿（line 514-562） | Context7 `alembic` 文档拉 env.py 模板；versions/001 直接抄 §3 草稿 |
| `dashboard/` (Next.js + Supabase JS SDK) | Vercel frontend | RESEARCH 没给完整范本 | Context7 `vercel/next.js` + `supabase/supabase-js` 当代文档；按 D-19 / D-20 设计 magic-link auth + 4-5 页内容；**不在 Phase 02 plan 阶段深挖**（D-18 启动期 read-only + scan trigger 即可） |
| `tests/test_scheduler.py` | 3-failure pause state machine | §11 测试矩阵 line 1589 + freezegun usage | 用 freezegun 推时钟 + AsyncMock 模拟 run_snapshot；assert state machine 转换 |
| `tests/test_chaos_3failures_pause.py` | chaos: 3 次连续失败 | §11 Chaos 表 line 1633-1641 | 与 test_scheduler.py 类似，更高 level（mock entire run_snapshot 链） |
| `tests/test_docker_smoke.sh` | docker build + run | §6 line 998 | bash script: `docker build && docker run + curl /health` |
| `scripts/deploy_smoke.sh` | post-deploy verification | 与 `make smoke-test` 一致 | bash script |

---

## 5. Plan Hand-off Notes（planner 必读）

### 5.1 Phase 02 → 真正 hidden gotcha（CONTEXT/RESEARCH 都没显式说但会咬人）

1. **uvicorn graceful shutdown 与 cron tick 冲突** — scheduler 正在跑 snapshot 时收到 SIGTERM，强 kill 会丢 cache.cleanup() 不跑（orchestrator.py:467）→ 下次启动时 cache 漂移让人困惑。`scheduler.run(stop_event)` 必须在每个 tick 入口检查 `stop_event.is_set()`，让 long-running snapshot 自然完成 + cleanup（**不打断当前 snapshot**）。
2. **Fly Volume 路径 vs Settings._within_project 路径校验冲突** — `POLYARB_DATA_DIR=/data` 是 absolute path，会被 config.py:52 的 `_within_project` 拒（除非 ALLOW_EXTERNAL_PATHS=1）。Phase 02 必须**在 prod 环境显式 set `POLYARB_ALLOW_EXTERNAL_PATHS=1`**（这与"never set in production code"的 F-3 注释冲突 — 需要在 plan 阶段重新评估 F-3 注释，或加 prod-specific allowlist `/data` 的逻辑）。
3. **SQLite WAL mode 在 Fly Volume 上的 fsync 行为** — Fly Volume 是 NVMe 但 fsync 路径可能慢；WAL mode `synchronous=NORMAL`（schemas.py:39）在云上是否够安全（崩溃恢复）— RESEARCH 没正面回答，plan 阶段需要验证（Fly Volume FAQ + SQLite docs）。
4. **Supabase 表 RLS policy 对 service_role 的豁免行为** — service_role 默认 bypass RLS（Supabase docs），但 mirror 应该写 `markets_latest` 表，dashboard anon_key 只读；如果 RLS policy 配错，service_role 写不进或 anon 读不出 — plan 阶段必须显式测试两方权限。
5. **Sentry breadcrumb 默认捕 logging.INFO** — 即使 `send_default_pii=False`，breadcrumb 仍可能含 SQL 字段值；T-02-08 mitigation 必须 plan 阶段就配 `before_send` hook（不是事后补）。

### 5.2 Phase 01.1 LEARNINGS 直接复用到 Phase 02 的清单

| LEARNINGS ref | Phase 02 应用 |
|---|---|
| **P1 Trust-split 工厂方法** | `/scan` endpoint 强约束（D-21）— 见 §2.1 / §3.9 |
| **P3 Multi-source single-entity detail** | `/health` 三态判定（snapshot age + status + sqlite latency + supabase mirror + r2 success 5 个子 check） — 见 §2.2 |
| **P5 Two-path error mapping** | Sentry config error vs transient error — 见 §3.4 |
| **P6 resolve_snapshot_path int + read-only lookup** | `/scan` endpoint `params.snapshot_id` 处理路径 — T-02-04 mitigation |
| **P7 Schema 演进硬约束（只能加列）** | `page_fetched_at_ms` 加列（不 rename）— 见 §2.10 / §2.12 |
| **P8 Plan SUMMARY 三件套** | Phase 02 每个 plan 落地必须有 SUMMARY — pre-commit hook 守 |
| **L2 fetched_at_ms 误导性** | §5 修复（page_fetched_at_ms 加列 + 字段语义注释）— 见 §2.10 |
| **L11 Makefile silent failure** | Phase 02 加新 target 必须同步加 dry-run test — 见 §2.14 + §1.2 `test_makefile_triple_check.sh` |
| **L12 二态健康判定太粗** | D-12 三态 OK/DEGRADED/FAILED 已 amendment 落地；/health 直接 map 到 IETF pass/warn/fail — 见 §2.2 Pattern B |
| **S5 SUMMARY 漂移事故** | Phase 02 每个 plan commit 前 `make planning-status` 必须全绿 — pre-commit hook 拦 |

### 5.3 Phase 02 plan 切分推荐（仅参考，最终由 gsd-planner 决定）

按 RESEARCH 的"3 Wave 推进"思路（line 1612-1623 隐含），Plan 切分可能为：

- **Plan 01** — page_fetched_at_ms 落地（schemas + normalizer + gamma_client + tests）→ 修 L2 bug，纯本地，零部署依赖
- **Plan 02** — http server + scheduler（daemon/* + http/*）→ 本地 docker 跑通 /health + /scan
- **Plan 03** — Supabase mirror + R2 sync（storage/supabase_mirror.py + storage/r2_sync.py + orchestrator step 7.5/7.6 + alembic）→ 仍可本地 docker 跑通
- **Plan 04** — Dockerfile + fly.toml + GHA workflows → 真正 deploy 到 Fly prod
- **Plan 05** — Sentry + Axiom + Better Stack + Telegram 接入（observability/*.py + alerts.py）→ 监控全链路打通
- **Plan 06** — Vercel dashboard scaffold（dashboard/）→ 可选，启动期最小可视化
- **Plan 07** — Chaos engineering tests + 7-day soak（tests/test_chaos_*.py + soak monitoring）→ Phase gate 验收

每个 plan 末必须有 `{phase}-{plan}-SUMMARY.md`（P8 硬纪律）。

### 5.4 修改/新增对照速查（planner 写 plan 时 file list 直接抄）

```
NEW Python files:
  src/polyarb/http/__init__.py
  src/polyarb/http/app.py
  src/polyarb/http/health.py
  src/polyarb/http/scan.py
  src/polyarb/daemon/__init__.py
  src/polyarb/daemon/main.py
  src/polyarb/daemon/scheduler.py
  src/polyarb/daemon/alerts.py
  src/polyarb/storage/supabase_mirror.py
  src/polyarb/storage/r2_sync.py
  src/polyarb/observability/logging.py
  src/polyarb/observability/sentry.py
  scripts/supabase_seed.py
  scripts/deploy_smoke.sh

NEW infra files:
  Dockerfile
  .dockerignore
  fly.toml
  .github/workflows/ci.yml
  .github/workflows/deploy.yml
  alembic.ini
  alembic/env.py
  alembic/versions/001_initial_dashboard_schema.py
  .env.example  (or extend existing)
  dashboard/  (Next.js scaffold — Plan 06)

NEW tests:
  tests/test_health_endpoint.py
  tests/test_http_scan.py
  tests/test_supabase_mirror.py
  tests/test_r2_sync.py
  tests/test_scheduler.py
  tests/test_page_fetched_at_ms.py
  tests/test_chaos_gamma_5xx.py
  tests/test_chaos_3failures_pause.py
  tests/test_logging.py
  tests/test_makefile_triple_check.sh
  tests/test_parquet_sqlite_consistency.py
  tests/test_docker_smoke.sh

MODIFY:
  src/polyarb/snapshot/orchestrator.py    (加 step 7.5 + 7.6)
  src/polyarb/snapshot/normalizer.py      (接 _page_fetched_at_ms)
  src/polyarb/clients/gamma_client.py     (per-page stamp)
  src/polyarb/storage/schemas.py          (4-point lockstep + events 表 3-point)
  src/polyarb/storage/__init__.py         (export new modules)
  src/polyarb/storage/sqlite_store.py     (加 get_latest_snapshot 读 helper)
  pyproject.toml                          (加 starlette/uvicorn/supabase/boto3/sentry-sdk/alembic deps)
  Makefile                                (加 ~10 个 Phase 02 target)
  tests/m1-perception/test_makefile_contract.py  (扩展覆盖新 target)
  tests/m1-perception/conftest.py         (加 mocked_supabase + mocked_r2 fixtures)

NEW docs (CLAUDE.md 强制):
  docs/learning/08-生产化部署.md  (phase 02 教学文档)
```

---

## 6. Metadata

- **Analog search scope:** `src/polyarb/**` + `tests/m1-perception/**` + `3th-party/clawfirm/**`
- **Files scanned:** ~25（src + tests + 3rd-party reference）
- **Files read in detail:** 13（orchestrator.py / sqlite_store.py / scanner.py / recipes.py / schemas.py / parquet_writer.py / cli_observation.py / snapshot/cli.py / gamma_client.py / normalizer.py / Makefile / pyproject.toml / conftest.py + LEARNINGS partial + clawfirm Dockerfile partial）
- **Pattern extraction date:** 2026-05-12
- **Phase:** 02-l1-production-grade
- **Workstream:** m1-perception
- **Next step:** `/gsd-plan-phase` planner consumes this + RESEARCH.md + CONTEXT.md to produce per-plan PLAN.md files
