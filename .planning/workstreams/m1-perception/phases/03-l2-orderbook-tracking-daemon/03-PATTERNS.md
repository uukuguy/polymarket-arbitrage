---
phase: 03
phase_name: "L2 Orderbook Tracking (分钟级 daemon)"
workstream: "m1-perception"
generated: "2026-05-23"
files_mapped: 33
plans_covered: 8
analog_sources:
  - "Phase 02 (L1 production-grade)"
  - "Phase 02.1 (L1 fix-up — fail-soft visibility / control endpoint / healthz split)"
  - "Phase 01.1 (scanner recipes + watchlist)"
---

# Phase 03 PATTERNS — L2 Orderbook Tracking 文件 → analog 映射

> 用法 (planner): 每个 plan 的 PLAN.md 在描述新文件时, 必须引用本表对应行的
> "analog path" + "key code excerpt", 不允许自由发挥。pitfall 列直接预防 Phase
> 02 / 02.1 已 documented 的 LEARNINGS surface — drift = 重复 9 个月前的坑。
>
> **核心 reuse 原则**: L2 daemon ≠ from-scratch project; 它是 L1 daemon 的兄弟
> 进程, 共享 Dockerfile / Settings / loguru / Sentry / supabase-py / alembic /
> conftest fixtures。**Single binary, two deployments** (Focus 7 verbatim)。

## File Classification (33 files across 8 plans)

| New / Modified file | Role | Data Flow | Closest Analog | Match Quality |
|---|---|---|---|---|
| `.github/workflows/supabase-keepalive.yml` | config (CI workflow) | scheduled HTTP | `.github/workflows/deploy.yml` | role-match (cron schedule vs push trigger) |
| `tests/test_supabase_keepalive_yml.py` | test (YAML structural) | static parse | (no direct analog) | foundational pytest pattern only |
| `fly-l2.toml` | config (deploy) | platform descriptor | `fly.toml` | exact (copy + 4 diffs) |
| `.github/workflows/deploy-l2.yml` | config (CI workflow) | scheduled HTTP | `.github/workflows/deploy.yml` | exact |
| `scripts/fly_secrets_sync.sh` | utility (ops script) | shell | (no direct analog) | new pattern; Makefile reference only |
| `tests/test_fly_l2_config.py` | test (TOML structural) | static parse | `tests/m1-perception/test_makefile_contract.py` | role-match (config structural assert) |
| `src/polyarb/daemon/l2_main.py` | daemon entry | event-driven | `src/polyarb/daemon/main.py` | exact (1:1 init order) |
| `tests/daemon/test_l2_main_startup.py` | test (daemon startup) | mocked init | `tests/m1-perception/test_daemon_shutdown.py` | role-match |
| `src/polyarb/clients/ws_market_client.py` | client (WS) | streaming | `src/polyarb/clients/clob_client.py` + sentry.py init pattern | role-match (async client class) |
| `tests/clients/test_ws_market_client.py` | test (WS protocol shape) | mocked socket | `tests/m1-perception/test_clob_client.py` | exact (mocked SDK pattern) |
| `src/polyarb/daemon/ws_watchdog.py` | daemon (state machine) | event-driven | `src/polyarb/daemon/scheduler.py` | role-match (state machine + persistence subset) |
| `tests/daemon/test_ws_watchdog.py` | test (state machine) | event injection | `tests/m1-perception/test_scheduler.py` | exact |
| `src/polyarb/events/__init__.py` | new module marker | — | `src/polyarb/observability/__init__.py` | trivial |
| `src/polyarb/events/bus.py` | service (NOTIFY publisher) | event-driven | `src/polyarb/storage/supabase_mirror.py` | role-match (long-lived client + fail-soft) |
| `src/polyarb/events/listener.py` | service (LISTEN consumer) | event-driven (reverse) | (no direct analog — asyncpg LISTEN is new pattern) | foundational; reference watchdog reconnect philosophy |
| `tests/events/test_bus_publish.py` | test (NOTIFY publisher) | mocked asyncpg | `tests/m1-perception/test_supabase_mirror.py` | role-match |
| `tests/events/test_listener_catchup.py` | test (cursor catch-up) | mocked asyncpg | `tests/m1-perception/test_orchestrator_mirror_skip.py` | partial (loguru StringIO sink pattern) |
| `src/polyarb/observation/l2_candidate_refresh.py` | service (candidate compute) | CRUD over scanner | `src/polyarb/observation/scanner.py` (run_recipe) + watchlist.py (load_watchlist) | exact (reuse 2 public functions verbatim) |
| `tests/observation/test_l2_candidate_refresh.py` | test (diff algo) | pure-fn | `tests/m1-perception/test_observation_scanner.py` | role-match |
| `alembic/versions/003_l2_tables.py` | migration | DDL | `alembic/versions/001_initial_dashboard_schema.py` + `002_add_top_movers_view.py` | exact (5 tables + RLS reuse) |
| `tests/alembic/test_003.py` | test (alembic upgrade) | DDL replay | (no analog — first alembic test in tree) | foundational pytest + tempdir pattern |
| `src/polyarb/storage/l2_supabase_mirror.py` | service (supabase mirror) | CRUD upsert | `src/polyarb/storage/supabase_mirror.py` | exact (D-12 fail-soft pattern, 1:1) |
| `tests/storage/test_l2_supabase_mirror.py` | test (mirror upsert) | mocked supabase | `tests/m1-perception/test_supabase_mirror.py` | exact |
| `src/polyarb/clients/data_api_client.py` | client (REST trades) | batch backfill | `src/polyarb/clients/gamma_client.py` | exact (httpx + aiolimiter + tenacity) |
| `tests/clients/test_data_api_trades.py` | test (REST pagination) | mocked httpx | `tests/m1-perception/test_gamma_client.py` | exact |
| `src/polyarb/snapshot/orchestrator.py` (modify) | modify — emit snapshot_complete NOTIFY | post-write fan-out | self (Phase 02 Plan 03 step 7.5 pattern) | self-reference |
| `tests/m1-perception/test_orchestrator.py` (extend) | test (snapshot_complete emit) | mocked asyncpg | self (existing test_orchestrator) | self-reference |
| `tests/chaos/test_l2_chaos_plan.py` | test (declarative chaos) | static parse | `tests/m1-perception/test_chaos_supabase.py` | role-match (chaos verification pattern) |
| `src/polyarb/http/l2_health.py` (or extend `health.py`) | HTTP handler | request-response | `src/polyarb/http/health.py` (`_build_health_checks` helper) | exact (Phase 02.1 P5 helper-first refactor) |
| `tests/m1-perception/test_l2_health_endpoint.py` | test (health endpoint) | request-response | `tests/m1-perception/test_health_endpoint.py` | exact |
| `docs/learning/10-L2-跟踪.md` | doc (教学) | markdown | `docs/learning/09-生产化运维.md` | exact (体例 + file:line discipline) |
| Makefile (extend) | config (make targets) | shell wrappers | Makefile existing targets | self (5 new targets) |
| 03-SOAK-LOG.md | doc (chaos verification log) | structured markdown | `02-SOAK-LOG.md` | exact |

---

## Pattern Assignments — Per-File Detail

### File 1: `.github/workflows/supabase-keepalive.yml` (Plan 01 — D-01)

**Analog:** `.github/workflows/deploy.yml` (lines 1-40)

**Adaptation rationale:** Reuse the GHA Actions skeleton (concurrency / permissions / timeout / smoke-loop) but replace push trigger with `schedule: cron`, and replace `flyctl deploy` step with `curl wget` to Supabase REST endpoint. The keepalive ping ONLY needs to touch the project so the 4-day idle pause clock resets (per D-01 + R4 risk).

**Key code excerpt** (deploy.yml lines 1-21):

```yaml
name: Deploy to Fly

on:
  push:
    branches: [main]
  workflow_dispatch: {}

concurrency:
  group: deploy-prod
  cancel-in-progress: false

jobs:
  deploy:
    name: flyctl deploy
    runs-on: ubuntu-latest
    timeout-minutes: 15
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: superfly/flyctl-actions/setup-flyctl@1.6
```

**Adaptation for L2 keepalive:**
- `on: schedule: - cron: "0 6 * * *"` (daily 06:00 UTC — well under Supabase 4-day pause threshold)
- Add `workflow_dispatch:` so manual trigger is possible (debug)
- Step = `curl -fsS "${{ secrets.POLYARB_SUPABASE_URL }}/rest/v1/snapshots?limit=1&apikey=${{ secrets.POLYARB_SUPABASE_ANON_KEY }}" -H "apikey: ..."` (or psql DSN ping — pick one path; psql DSN is more reliable)
- Optional: POST to Better Stack heartbeat URL after success → BS heartbeat with 25h tolerance catches workflow miss within 24h+1h margin

**Gotcha / pitfall** (from Phase 02 L8):
- `superfly/flyctl-actions/setup-flyctl@v1.5` was non-existent → 4 days silent deploy fail. **Pin to @1.6 (without `v` prefix)**, NEVER use `@v1.5` / `@v2` placeholder.
- The smoke-loop pattern in deploy.yml (lines 26-38: `for i in $(seq 1 10); do curl ... done`) is a good fallback if the first ping fails — borrow that loop.
- CI failure email goes to GitHub primary, which Phase 02 user missed for 4 days. **Plan 01 MUST also configure Better Stack heartbeat** with 25h tolerance so a failed GHA produces a Telegram alert via the existing alert chain (per Open Question #5 in RESEARCH).

---

### File 2: `tests/test_supabase_keepalive_yml.py` (Plan 01 Wave 0 RED test)

**Analog:** (no direct analog — Phase 02 has no YAML structural test)

**Foundational pattern only:** pytest + `pathlib.Path` + `yaml.safe_load`. Parse `.github/workflows/supabase-keepalive.yml`, assert:
1. `on.schedule[0].cron` matches `"0 * * * *"` daily pattern (NOT weekly — would exceed pause threshold)
2. Steps include at least one of: (a) `curl ... supabase.co/rest/v1/...`, (b) `psql ... -c "SELECT 1"`
3. Step uses `secrets.POLYARB_SUPABASE_*` references (not hardcoded URLs)

**Gotcha:** GHA YAML uses `on:` (reserved keyword) which `pyyaml` happily parses as `True` boolean key on Python 3.12 → assert via `data[True]` or use `yaml.safe_load` then convert. Use `assert "schedule" in str(data)` defensively.

---

### File 3: `fly-l2.toml` (Plan 02 — D-06)

**Analog:** `fly.toml` (verbatim copy + 4 line-level diffs)

**Adaptation rationale:** Single binary, two deployments. The L2 daemon shares the same Docker image as L1; fly-l2.toml differs only in app name, process group composition (no cron), mount size, and VM memory.

**Key code excerpt** (fly.toml lines 1-40):

```toml
# Phase 02 Fly.io config — Supercronic cron (W8 RESOLVED, 2026-05-12)
app = "polyarb-l1"
primary_region = "ams"

[build]

[mounts]
  source = "polyarb_data"
  destination = "/data"
  initial_size = "5gb"

[env]
  POLYARB_DATA_DIR = "/data"
  POLYARB_SNAPSHOT_DIR = "/data/snapshots"
  POLYARB_DB_PATH = "/data/state.db"
  POLYARB_ALLOW_EXTERNAL_PATHS = "1"

[processes]
  app = "python -m polyarb.daemon.main"
  cron = "supercronic /app/crontab"

[http_service]
  internal_port = 8080
  force_https = true
  ...
  [[http_service.checks]]
    grace_period = "120s"
    interval = "30s"
    method = "GET"
    path = "/healthz"        # Phase 02.1 D-05 — Fly-friendly always-200
    timeout = "10s"
```

**Adaptation diffs for fly-l2.toml:**
1. `app = "polyarb-l2"` (instead of `polyarb-l1`)
2. `[mounts] initial_size = "1gb"` (no parquet archive, just SQLite state — see Focus 7)
3. **REMOVE entire `cron = "supercronic /app/crontab"` line** from `[processes]`
4. **REMOVE the second `[[vm]] processes = ["cron"]` block** at bottom
5. Add `POLYARB_DAEMON_VARIANT = "l2"` in `[env]` (selects l2_main.py)
6. Change `POLYARB_DB_PATH = "/data/l2-state.db"` (separate SQLite from L1)
7. `[[vm]] memory = "512mb"` (vs L1's 1024mb — no CLOB cache, WS stream is leaner)
8. `path = "/healthz"` stays exactly the same — Phase 02.1 D-05/D-06 invariant carries over

**Gotcha / pitfall** (from Phase 02.1 L2 + Phase 02 BUG-6):
- Probe path **MUST be `/healthz`** not `/health` — copying L1's BUG-6 trade-off is mandatory, because L2 will eventually need a `/control/*` admin surface (Plan 03+ stub registered per Plan 02) and BUG-8 cross-bug interaction (Inj 4 实证) shows /health=503 + `/control/unpause` 经 Fly proxy 同时阻塞。Don't repeat。
- **Memory budget caution** (Phase 02 L1 + L4): "512mb" is an initial estimate. Per RESEARCH Risk R7, profile actual RSS via `flyctl logs anon-rss` after first chaos run and scale up if needed. macOS profiling underestimates Linux RSS by ~80MB (Phase 02 L3 + S2).

---

### File 4: `.github/workflows/deploy-l2.yml` (Plan 02 — D-06)

**Analog:** `.github/workflows/deploy.yml` (full file, 40 lines)

**Adaptation rationale:** Almost 1:1 copy. Change app target + config flag + add path filter to avoid deploying L2 on L1-only commits.

**Key code excerpt** (deploy.yml verbatim):

```yaml
name: Deploy to Fly
on:
  push:
    branches: [main]
  workflow_dispatch: {}
concurrency:
  group: deploy-prod
  cancel-in-progress: false
jobs:
  deploy:
    name: flyctl deploy
    runs-on: ubuntu-latest
    timeout-minutes: 15
    steps:
      - uses: actions/checkout@v4
      - uses: superfly/flyctl-actions/setup-flyctl@1.6
      - name: Deploy
        run: flyctl deploy --remote-only --wait-timeout 600
        env:
          FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}
      - name: Smoke test /health
        env:
          APP: polyarb-l1
        run: |
          for i in $(seq 1 10); do
            if curl -fsS "https://${APP}.fly.dev/health" >/dev/null; then
              echo "deploy healthy"; exit 0
            fi
            sleep 6
          done
```

**Adaptation diffs for deploy-l2.yml** (per RESEARCH Focus 7 section "deploy-l2.yml"):
- `name: Deploy L2 to Fly`
- `concurrency.group: deploy-l2-prod`
- `paths:` filter so L1-only commits don't trigger L2 deploy:
  ```yaml
  paths:
    - 'src/polyarb/daemon/l2_main.py'
    - 'src/polyarb/clients/ws_market_client.py'
    - 'src/polyarb/observation/l2_candidate_refresh.py'
    - 'src/polyarb/events/**'
    - 'fly-l2.toml'
    - 'Dockerfile'
  ```
- `flyctl deploy --config fly-l2.toml --remote-only ...`
- Smoke: `APP: polyarb-l2`, path `/healthz` (not `/health` — per BUG-6 fix; matches the Fly probe target).

**Gotcha** (Phase 02 S4 + S8):
- `setup-flyctl@v1.5` non-existent — pin to `@1.6` exactly。
- Vercel author verification trap does NOT apply to Fly — but the same "silently fails" lesson applies: deploy-l2.yml MUST have a fail-loud smoke step (already in template).
- Phase 02 S4: 57 秒 "deploy" 可能是 image hash 没变的 no-op — 不要把 57s GHA success 解读为真 build 完成; 必须看 `flyctl logs` 看真 startup line。

---

### File 5: `scripts/fly_secrets_sync.sh` (Plan 02 — secret propagation)

**Analog:** (no direct analog — new ops script)

**Foundational pattern:** read `.env`, push to both `polyarb-l1` + `polyarb-l2` via `flyctl secrets set -a $APP`. Skeleton in RESEARCH Focus 7 "Secret propagation" section.

**Gotcha:**
- Per RESEARCH Pitfall: comments inside `.env` (lines starting with `#`) must be filtered out before `flyctl secrets set` (`grep -v '^#' .env`).
- 14 Fly secrets per Phase 02.1 D-22 (复用 `POLYARB_SCAN_SHARED_SECRET` — do NOT mint a new one for L2)
- script MUST be idempotent (re-run safe — `flyctl secrets set` is upsert by default)
- Phase 02.1 D-22 reuse: L2's `/control/*` middleware (when Plan 03 stub fills in) reuses same secret — do not generate separate `POLYARB_L2_SHARED_SECRET`

---

### File 6: `tests/test_fly_l2_config.py` (Plan 02 Wave 0 RED test)

**Analog:** `tests/m1-perception/test_makefile_contract.py` (TOML structural pattern — use `tomllib` from Python 3.11+)

**Adaptation rationale:** Static parse of `fly-l2.toml`. Use Python 3.11+ stdlib `tomllib.load(fp, mode="rb")`.

**Key assertions (RED until Plan 02 implements):**
```python
import tomllib
from pathlib import Path

def test_fly_l2_config_shape():
    config = tomllib.loads(Path("fly-l2.toml").read_text())
    assert config["app"] == "polyarb-l2"
    assert config["primary_region"] == "ams"
    # NO cron process — would conflict with single WS-driven loop
    assert "cron" not in config["processes"]
    # /healthz probe (Phase 02.1 D-05 carry-over — NOT /health)
    checks = config["http_service"]["checks"]
    assert any(c["path"] == "/healthz" for c in checks)
    # Volume sized down from L1's 5gb (no parquet archive)
    assert config["mounts"]["initial_size"] in ("1gb", "1GB")
    # Single VM group (only "app", no "cron")
    vm_blocks = config["vm"] if isinstance(config["vm"], list) else [config["vm"]]
    process_groups = {p for vm in vm_blocks for p in vm["processes"]}
    assert process_groups == {"app"}
```

**Gotcha:**
- `[[vm]]` (double-bracket array of tables) parses as `list` in tomllib BUT a single `[[vm]]` block parses to a 1-element list — handle both.
- Test runs `Path("fly-l2.toml")` relative to repo root; pytest conftest should set `Path.cwd()` to repo root (existing conftest pattern).

---

### File 7: `src/polyarb/daemon/l2_main.py` (Plan 03 — D-06)

**Analog:** `src/polyarb/daemon/main.py` (full file, 117 lines)

**Adaptation rationale:** 1:1 init order. Phase 02 P9 (server-started gate) is **mandatory** — uvicorn binds socket before any long-running task starts, otherwise Fly probe times out. The only structural diff: replace `SnapshotScheduler` + `scheduler.run()` with `WSConsumer + WsWatchdog + EventListener` tasks gathered via `asyncio.gather`.

**Key code excerpt — init order** (main.py lines 38-90):

```python
async def main() -> int:
    init_logging()                                    # 1. FIRST — sets up JSON sink
    settings = load_settings()                        # 2. config
    init_sentry(settings)                             # 3. AFTER logging (Loguru integ.)
    logger.info("polyarb daemon starting up")

    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    scheduler = SnapshotScheduler(settings=settings, sqlite_store=sqlite_store)
    app = create_app(scheduler=scheduler, sqlite_store=sqlite_store, settings=settings)

    config = uvicorn.Config(app, host="0.0.0.0", port=settings.http_port, ...)
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    server_task = asyncio.create_task(server.serve())

    # P9: WAIT for uvicorn socket bind BEFORE starting scheduler.
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)
    logger.info(f"daemon running: http server on :{settings.http_port}, starting scheduler")

    scheduler_task = asyncio.create_task(scheduler.run(stop_event))
    await stop_event.wait()
    server.should_exit = True
    scheduler_task.cancel()
```

**Adaptation for l2_main.py:**
- Steps 1-7 identical (init_logging → load_settings → init_sentry → sqlite_store)
- Replace `scheduler` + `scheduler.run()` with:
  ```python
  sentry_sdk.set_tag("service", "polyarb-l2")  # Focus 7 — Sentry environment differentiation
  watchdog = WsWatchdog(stale_s=30.0)
  ws_consumer = WsConsumer(settings, on_event=mirror.on_event, watchdog=watchdog)
  event_listener = EventListener(dsn=settings.supabase_db_dsn.get_secret_value(),
                                  on_event=lambda p: asyncio.create_task(on_snapshot_complete(p)))
  app = create_l2_app(ws_consumer=ws_consumer, sqlite_store=sqlite_store, settings=settings)
  ```
- Tasks gathered (after server-started gate):
  ```python
  watchdog_task = asyncio.create_task(watchdog.watch(stop_event))
  ws_task = asyncio.create_task(ws_consumer.run(stop_event))
  listener_task = asyncio.create_task(event_listener.listen(stop_event))
  ```
- Shutdown: cancel all 4 tasks; bounded `wait_for(..., timeout=5.0)`.

**Gotcha / pitfall** (Phase 02 L5 + P9):
- **MUST keep `await server.started` gate** — Phase 02 L5 showed `asyncio.gather` without this gate causes Fly health probe to timeout (scheduler/WS monopolizes event loop before uvicorn binds). The 100-iteration / 0.1s sleep pattern is **exact** and **mandatory**.
- **`logger.info("...")` BEFORE long-running tasks** — Sentry SDK needs the loguru sink installed first (Phase 02 LearningP9).
- Sentry `set_tag("service", "polyarb-l2")` MUST be in l2_main.py, not in sentry.py — keeps L1 sentry.py untouched (P3 — independent modules).

---

### File 8: `tests/daemon/test_l2_main_startup.py` (Plan 03 Wave 0 RED)

**Analog:** `tests/m1-perception/test_daemon_shutdown.py` (existing SIGINT shutdown test pattern)

**Adaptation rationale:** Assert init order. Use `unittest.mock.patch.multiple` or sequential patches to assert order:
```python
calls = []
with (
    patch("polyarb.daemon.l2_main.init_logging", side_effect=lambda: calls.append("logging")),
    patch("polyarb.daemon.l2_main.load_settings", side_effect=lambda: (calls.append("settings"), MagicMock())[1]),
    patch("polyarb.daemon.l2_main.init_sentry", side_effect=lambda s: calls.append("sentry")),
    patch("polyarb.daemon.l2_main.SQLiteStore"),
    patch("polyarb.daemon.l2_main.uvicorn.Server"),
):
    # Run main() with a stop_event pre-set so we exit immediately
    ...
assert calls == ["logging", "settings", "sentry"]  # P9 init order invariant
```

**Gotcha** (Phase 02 L9 — mock import-site, not definition-site):
- Patch `polyarb.daemon.l2_main.init_sentry` (l2_main's import site), NOT `polyarb.observability.sentry.init_sentry` — otherwise the patch doesn't apply (Phase 02 L9 verbatim).

---

### File 9: `src/polyarb/clients/ws_market_client.py` (Plan 04 Task 1 — D-02)

**Analog:** `src/polyarb/clients/clob_client.py` (async client class shape) + `src/polyarb/observability/sentry.py` (no-op when secret missing pattern)

**Adaptation rationale:** New library (`websockets>=16.0,<17`) but same project pattern: long-lived async client + aiolimiter (rate cap) + structured logger. No retry tenacity (websockets's async-for connect handles reconnect on transport error; staleness is the watchdog's job, not tenacity's).

**Key code excerpt (clob_client.py lines 50-75 — async client class shape):**

```python
class ClobReaderClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # L0 read-only: only host needed. NO key/creds/chain_id.
        self._client = ClobClient(settings.clob_url)
        self._limiter = AsyncLimiter(settings.clob_batch_rate_per_10s, 10)

    async def get_books(self, token_ids: list[str], ...) -> list[Any]:
        if not token_ids:
            return []
        chunks = _chunked(token_ids, self._settings.clob_batch_size)
        for i, chunk in enumerate(chunks, start=1):
            params = [BookParams(token_id=t) for t in chunk]
            async with self._limiter:
                books = await asyncio.to_thread(self._client.get_order_books, params)
            ...
```

**Adaptation for ws_market_client.py:** RESEARCH Focus 1 has the verbatim skeleton (~30 lines). Key shape:
```python
WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

async def stream_market_events(
    assets_ids: list[str],
    *,
    initial_dump: bool = True,
    ping_interval_s: int = 10,  # Polymarket REQUIRES 10s (NOT default 20s)
) -> AsyncIterator[dict]:
    async for ws in websockets.connect(
        WS_URL,
        ping_interval=ping_interval_s,
        ping_timeout=ping_interval_s,
        max_size=2**22,  # 4 MiB cap for initial_dump book snapshots
    ):
        try:
            sub = {"type": "market", "assets_ids": assets_ids, "initial_dump": initial_dump}
            await ws.send(json.dumps(sub))
            logger.info(f"ws subscribed: {len(assets_ids)} assets, initial_dump={initial_dump}")
            async for raw in ws:
                try:
                    yield json.loads(raw)
                except json.JSONDecodeError as e:
                    logger.warning(f"ws non-JSON frame ignored: {e!r}")
        except websockets.ConnectionClosed as e:
            logger.warning(f"ws connection closed code={e.code}; reconnecting…")
```

**Gotcha / pitfall** (RESEARCH Focus 1 + Open Q 10):
- `ping_interval=10` (NOT default 20) — Polymarket server drops the connection at ~10s silence (docs.polymarket.com). Using the websockets default 20s will cause silent disconnects within minutes.
- `max_size=2**22` (4 MiB) — `initial_dump=True` book snapshots can be large. Default 2**20 (1 MiB) may throw `PayloadTooBig` on big orderbooks (Phase 02 D-23 OOM precedent shows fat payloads bite).
- `async for ws in websockets.connect(...)` is the **reconnect-iterator** form (16.0+ idiom); do NOT use `async with` — it disables auto-reconnect.
- The `operation` field for dynamic subscribe/unsubscribe is `[ASSUMED]` per RESEARCH §1; Plan 04 MUST verify via wscat sanity check before final commit (RESEARCH Open Q 3).

---

### File 10: `tests/clients/test_ws_market_client.py` (Plan 04 Task 1 Wave 0)

**Analog:** `tests/m1-perception/test_clob_client.py` (mocked SDK pattern)

**Adaptation rationale:** mock `websockets.connect` to yield a fake socket object that records `.send(payload)` calls. Assert the subscribe payload shape matches docs.polymarket.com `{type, assets_ids, initial_dump}`.

**Key pattern (similar to test_clob_client.py):**

```python
@pytest.mark.asyncio
async def test_subscribe_payload_shape(monkeypatch):
    sent_payloads = []

    class FakeWs:
        async def send(self, raw): sent_payloads.append(json.loads(raw))
        async def __aiter__(self): return; yield  # never yield
        async def close(self): pass

    async def fake_connect_iter(*args, **kw):
        yield FakeWs()
        return

    monkeypatch.setattr("polyarb.clients.ws_market_client.websockets.connect",
                        lambda *a, **kw: fake_connect_iter())

    gen = stream_market_events(["0xabc", "0xdef"], initial_dump=True)
    # consume one yield window then break
    ...
    assert sent_payloads[0] == {"type": "market", "assets_ids": ["0xabc", "0xdef"], "initial_dump": True}
```

**Gotcha** (Phase 02 L9 + L4):
- Patch the import site (`polyarb.clients.ws_market_client.websockets.connect`), not the websockets module.
- Use loguru StringIO sink if asserting log output (Phase 02.1 L4 — caplog doesn't see loguru).

---

### File 11: `src/polyarb/daemon/ws_watchdog.py` (Plan 04 Task 2 — D-03)

**Analog:** `src/polyarb/daemon/scheduler.py` (state machine shape, but without 3-failure persistence)

**Adaptation rationale:** Both are state machines triggering recovery actions (PAUSED alert vs RECONNECTING reconnect). The watchdog is **simpler** (no SQLite persistence; in-memory only because a daemon restart re-establishes WS fresh anyway). Reuse the state-enum + asyncio.Event + tick-loop pattern from scheduler.run().

**Key code excerpt — scheduler.py state pattern (lines 35-200 condensed):**

```python
class SchedulerState(str, Enum):
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"

class SnapshotScheduler:
    FAILURE_THRESHOLD = 3

    def __init__(self, settings, sqlite_store):
        self._failure_counter = 0
        self.state = SchedulerState.RUNNING

    async def _tick(self):
        if self.state == SchedulerState.PAUSED:
            return
        try:
            result = await self._run_snapshot()
            if result.status in (OK, DEGRADED):
                self._failure_counter = 0  # reset on success
            else:
                self._failure_counter += 1
        except asyncio.CancelledError:
            raise  # F-04: must propagate
        except Exception:
            self._failure_counter += 1
        if self._failure_counter >= self.FAILURE_THRESHOLD:
            self.state = SchedulerState.PAUSED
            await self._on_paused()  # alert hook
```

**Adaptation for ws_watchdog.py** (RESEARCH Focus 2 verbatim skeleton, ~70 lines):

```python
@dataclass
class WatchdogState:
    last_event_time_s: float = field(default_factory=time.monotonic)
    reconnect_attempt: int = 0
    state: str = "CONNECTED"

class WsWatchdog:
    def __init__(self, stale_s: float = 30.0, on_reconnect: Callable | None = None):
        self.stale_s = stale_s
        self._state = WatchdogState()
        self._last_touch_event = asyncio.Event()
        self._on_reconnect = on_reconnect

    def touch(self) -> None:
        """Call from event loop on EVERY incoming WS frame."""
        self._state.last_event_time_s = time.monotonic()
        self._state.state = "WAITING_FOR_EVENT"
        self._last_touch_event.set()
        self._last_touch_event.clear()
        self._state.reconnect_attempt = 0  # reset on healthy frame

    async def watch(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            elapsed = time.monotonic() - self._state.last_event_time_s
            if elapsed > self.stale_s:
                self._state.state = "RECONNECTING"
                attempt = min(self._state.reconnect_attempt, len(_BACKOFF_S) - 1)
                wait_s = _BACKOFF_S[attempt]
                self._state.reconnect_attempt += 1
                if self._on_reconnect:
                    self._on_reconnect()
                await asyncio.sleep(wait_s)
                self._state.last_event_time_s = time.monotonic()
            else:
                try:
                    await asyncio.wait_for(
                        self._last_touch_event.wait(),
                        timeout=self.stale_s - elapsed,
                    )
                except asyncio.TimeoutError:
                    pass
```

**Gotcha / pitfall** (Phase 02 D-13 + R5):
- **Backoff cap = 30s** (per D-03), max reconnect storm protection. RESEARCH R5 risk: without a cap, Polymarket may rate-limit / IP-ban the daemon during a sustained outage. Add a hard cap (e.g., "10 reconnects per hour → fall back to REST polling") per RESEARCH R5 mitigation.
- **`asyncio.CancelledError` must propagate** (Phase 02 F-04 / scheduler.py line 171): `watch()` MUST not swallow `CancelledError`, otherwise SIGTERM cannot interrupt mid-sleep.
- **`touch()` reset semantics**: reset `reconnect_attempt = 0` ONLY when a healthy frame arrives (NOT during the sleep) — otherwise consecutive immediate-fail reconnects look "fresh" and don't backoff.

---

### File 12: `tests/daemon/test_ws_watchdog.py` (Plan 04 Task 2 Wave 0)

**Analog:** `tests/m1-perception/test_scheduler.py` (state machine + counter persistence test pattern)

**Adaptation rationale:** Inject events via direct `watchdog.touch()` calls + `time.monotonic` monkeypatch to simulate elapsed time. Assert state transitions + backoff sequence.

**Key test scenarios:**
1. **30s timeout triggers RECONNECTING**: prime watchdog, advance `time.monotonic` by 31s, call `watch()` once → state == RECONNECTING + `on_reconnect` callback fired.
2. **Backoff sequence (1, 2, 4, 8, 16, 30)**: simulate 6 consecutive stalls, assert `reconnect_attempt` count + the actual `asyncio.sleep` wait sequence (monkeypatch `asyncio.sleep` to record values).
3. **`touch()` resets attempt counter**: after 3 stalls (attempt=3), single `touch()` → next stall returns to attempt=1 backoff=1s.
4. **`stop_event.set()` cancels mid-sleep within 1s**: per Phase 02 F-04, MUST verify `wait_for` interrupts; use `asyncio.wait_for(watch_task, timeout=1.5)` post `stop_event.set()`.

**Gotcha:**
- Use `monkeypatch.setattr(time, "monotonic", iter([0.0, 31.0, 62.0, ...]).__next__)` for deterministic time progression.
- `asyncio.wait_for` cancellation propagation — assert `CancelledError` is NOT eaten.

---

### Files 13-14: `src/polyarb/events/__init__.py` + `src/polyarb/events/bus.py` (Plan 05 — D-05)

**Analog (for bus.py):** `src/polyarb/storage/supabase_mirror.py` (long-lived client + fail-soft pattern + module logger)

**Adaptation rationale:** Reuse the "open client → execute → close → fail-soft" envelope. Replace `supabase-py.create_client` with `asyncpg.connect(dsn=...)`. NOTIFY publisher is short-lived (open conn → notify → close) because:
1. NOTIFY is fire-and-forget — no ongoing subscription to maintain
2. asyncpg conn cleanup is explicit (no connection pool needed for sub-100ms operations)

**Key code excerpt — supabase_mirror.py fail-soft envelope (lines 109-119):**

```python
def push_snapshot(self, snapshot_id, snapshot_meta, market_rows) -> bool:
    try:
        self._client.table("snapshots").upsert(snapshot_meta).execute()
        self._client.table("markets_latest").delete().neq("market_id", "").execute()
        for chunk in _chunk(market_rows, 1000):
            self._client.table("markets_latest").insert(chunk).execute()
        return True
    except Exception as e:  # noqa: BLE001 — fail-soft per RESEARCH §3
        logger.error(f"Supabase mirror failed snapshot_id={snapshot_id}: {str(e)[:200]}")
        return False
```

**Adaptation for `events/bus.py`** (RESEARCH Focus 3 verbatim, ~15 lines):

```python
import asyncpg
import json
from loguru import logger
import sentry_sdk

async def publish_snapshot_complete(settings, *, snapshot_id, taken_at_ms):
    """L1 → L2 cross-process NOTIFY on Postgres channel 'snapshot_complete'.

    Fail-soft (per D-12): never raises. Mirrors supabase_mirror.push_snapshot envelope.
    """
    try:
        conn = await asyncpg.connect(dsn=settings.supabase_db_dsn.get_secret_value())
        try:
            payload = json.dumps({"snapshot_id": snapshot_id, "taken_at_ms": taken_at_ms})
            await conn.execute("SELECT pg_notify('snapshot_complete', $1)", payload)
        finally:
            await conn.close()
        return True
    except Exception as e:  # noqa: BLE001
        logger.warning(f"event bus publish failed (fail-soft): {e!r}")
        sentry_sdk.add_breadcrumb(
            category="event-bus", level="warning",
            message=f"publish snapshot_complete failed: {snapshot_id}",
            data={"error": str(e)[:200]},
        )
        return False
```

**Gotcha / pitfall** (Phase 02 P2 + P7 + Phase 02.1 L1):
- **Fail-soft contract** (D-12): NOTIFY publish failure MUST NOT block L1 orchestrator. Same envelope as `supabase_mirror.push_snapshot` (return False; log + breadcrumb).
- **Breadcrumb upload pitfall** (Phase 02.1 L1 / S1): If the L1 daemon never crashes, the breadcrumb buffer never uploads to Sentry → breadcrumb is "design-unreachable" in prod. Mitigation: an explicit `sentry_sdk.capture_message(level="warning")` on the FIRST consecutive failure within a window. Phase 02.2 backlog item — mirror success path also breadcrumbs. Apply preemptively per RESEARCH Open Q 9.
- **No connection pool** in Phase 03 — keep it dead simple. If Phase 04+ needs higher throughput (NOTIFY storms), switch to `asyncpg.create_pool` with `max_size=2`.
- **NOTIFY payload limit**: 8000 bytes (Postgres hard limit). Phase 03 payload = `{snapshot_id, taken_at_ms}` ≈ 80 bytes — safe (RESEARCH Focus 3).

---

### File 15: `src/polyarb/events/listener.py` (Plan 05 — D-05 receive side)

**Analog:** (no direct analog — asyncpg LISTEN is a new pattern); reference watchdog reconnect philosophy + scheduler restoration pattern.

**Foundational pattern** (RESEARCH Focus 3 verbatim, ~35 lines):

```python
import asyncpg
import asyncio
import json
from typing import Callable
from loguru import logger

async def listen_snapshot_complete(
    dsn: str, on_event: Callable, stop_event: asyncio.Event,
) -> None:
    """LISTEN on 'snapshot_complete'; invoke on_event(snapshot_id) per payload.

    asyncpg has no built-in auto-reconnect — we wrap in a while loop with 5s
    backoff. Drop mitigation via l2_event_cursor catch-up (Focus 4 schema).
    """
    while not stop_event.is_set():
        try:
            conn = await asyncpg.connect(dsn=dsn)
            await conn.add_listener("snapshot_complete", _make_callback(on_event))
            await conn.execute("LISTEN snapshot_complete")
            logger.info("listener connected to snapshot_complete channel")
            try:
                await stop_event.wait()
            finally:
                await conn.close()
        except Exception as e:
            logger.warning(f"event listener reconnecting in 5s: {e!r}")
            await asyncio.sleep(5)

def _make_callback(on_event):
    def _cb(conn, pid, channel, payload):
        try:
            data = json.loads(payload)
            on_event(data)
        except Exception as e:
            logger.error(f"on_event callback failed: {e!r}")
    return _cb
```

**Cursor catch-up pattern** (Focus 3 NOTIFY drop mitigation):
```python
async def catchup_from_cursor(dsn: str, consumer: str = "l2-candidate-refresh") -> list[dict]:
    conn = await asyncpg.connect(dsn=dsn)
    try:
        cursor_row = await conn.fetchrow(
            "SELECT last_snapshot_id FROM l2_event_cursor WHERE consumer=$1", consumer)
        last_seen = cursor_row["last_snapshot_id"] if cursor_row else 0
        missed = await conn.fetch(
            "SELECT id, taken_at_ms FROM snapshots WHERE id > $1 ORDER BY id", last_seen)
        return [dict(r) for r in missed]
    finally:
        await conn.close()
```

**Gotcha / pitfall** (Phase 02 R2 + LEARNINGS Phase 02 L9 mock import site):
- **NOTIFY drops** when L2 is offline → cursor table catch-up (Focus 3) is **mandatory** in startup. l2_event_cursor table is in Alembic 003 (File 19).
- **asyncpg callback is sync** — `_cb` runs in the asyncpg thread, not the asyncio event loop. If `on_event` needs to do async work (e.g., trigger candidate refresh via WS), schedule via `asyncio.create_task(...)` from inside `_cb` (NOTE: `_cb` is sync — use `asyncio.run_coroutine_threadsafe(on_event_async(data), loop)` if listener runs in a thread; for asyncpg's default same-loop model, `loop.call_soon_threadsafe` is the right tool).
- **Connection-loss handling**: asyncpg does NOT auto-reconnect — the outer `while not stop_event.is_set()` + 5s backoff in the skeleton above is the prescribed handling.

---

### Files 16-17: `tests/events/test_bus_publish.py` + `tests/events/test_listener_catchup.py` (Plan 05 Wave 0)

**Analog:** `tests/m1-perception/test_supabase_mirror.py` (mocked supabase pattern) + `tests/m1-perception/test_orchestrator_mirror_skip.py` (loguru StringIO sink pattern)

**Key fixture (loguru StringIO sink — Phase 02.1 L4 verbatim — repeat for L2 tests):**

```python
import io
from loguru import logger

@pytest.fixture
def loguru_string_sink():
    sink = io.StringIO()
    sink_id = logger.add(sink, format="{message}", level="INFO")
    yield sink
    logger.remove(sink_id)

def test_publish_failsoft_logs(loguru_string_sink, monkeypatch):
    # mock asyncpg.connect to raise
    async def fake_connect(**kw): raise RuntimeError("connection refused")
    monkeypatch.setattr("polyarb.events.bus.asyncpg.connect", fake_connect)
    settings = MagicMock()
    settings.supabase_db_dsn.get_secret_value.return_value = "postgresql://x"
    result = asyncio.run(publish_snapshot_complete(settings, snapshot_id=42, taken_at_ms=1234567890))
    assert result is False
    assert "event bus publish failed" in loguru_string_sink.getvalue()
```

**Gotcha** (Phase 02.1 L3 + L4):
- `.env` 渗透 pitfall (L3): `_make_settings(...)` must explicitly pass empty `supabase_url=""` etc, otherwise dev `.env` real DSN bleeds into test.
- **caplog 看不到 loguru** (L4): always use `logger.add(StringIO(), ...)` fixture, NEVER `caplog`. Phase 02.1 P1 patterns-established.

---

### File 18: `src/polyarb/observation/l2_candidate_refresh.py` (Plan 05 — D-04 + D-05)

**Analog:** `src/polyarb/observation/scanner.py` (`run_recipe` + `list_all_recipes`) + `src/polyarb/observation/watchlist.py` (`load_watchlist`)

**Adaptation rationale:** **Direct reuse** — RESEARCH §Focus 5 explicitly says "reuse Phase 01.1 scanner". `compute_candidates()` calls `run_recipe()` from scanner.py and `load_watchlist()` from watchlist.py verbatim. New code is just the union + diff layer (~85 lines per RESEARCH skeleton).

**Key code excerpt — scanner.py:131 (run_recipe — read-only SQLite URI):**

```python
def run_recipe(db_path: Path, recipe: Recipe) -> pd.DataFrame:
    """Execute a row-level recipe, returning a DataFrame with question_zh joined.
    Layer-1 (engine read-only): mode=ro URI rejects all writes.
    """
    _validate_where(recipe.where, trusted=recipe._is_trusted)
    _validate_order_by(recipe.order_by, trusted=recipe._is_trusted)
    limit = _validate_limit(recipe.limit)
    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        sql = f"SELECT m.*, qt.question_zh FROM markets m LEFT JOIN ... LIMIT {limit}"
        return pd.read_sql_query(sql, con)
    finally:
        con.close()
```

**Key code excerpt — watchlist.py:173 (load_watchlist):**

```python
def load_watchlist(yaml_path: Path) -> list[WatchlistEntry]:
    if not yaml_path.exists():
        return []
    data = yaml.safe_load(yaml_path.read_text()) or []
    ...
    return entries
```

**Adaptation for l2_candidate_refresh.py:** RESEARCH Focus 5 has the verbatim ~85-line skeleton. Three public functions:
1. `compute_candidates(settings, scanner_yaml, watchlist_yaml) -> list[CandidateRow]` — pure function, scanner recipes ∪ watchlist
2. `diff_candidate_sets(old_asset_ids, new_rows) -> (removed_ids, added_rows)` — set diff for WS subscribe/unsubscribe
3. `on_snapshot_complete(payload)` — coroutine that wires diff to ws_consumer.add/remove_subscriptions + persist_candidates + mark_removed

**Sanity caps (per Focus 5):**
- max candidate-set size: **500 assets** (R9 risk mitigation)
- recipe LIMIT: already validated 1-10000 (scanner.py:`_validate_limit`)
- refresh debounce: **60s minimum between refreshes** (R1 NOTIFY storm protection — multiple snapshots in burst → 1 refresh)

**Gotcha / pitfall** (Phase 02 L9 + Phase 02.1 L2):
- **L2 cross-bug pre-check (Phase 02.1 L2 verbatim)**: candidate refresh + NOTIFY storm hitting same Postgres pool simultaneously — debounce ≥60s.
- **Watchlist YAML schema**: `WatchlistEntry.slug` is the join key — query `markets` table by slug to resolve `yes_token_id` + `no_token_id` (RESEARCH Focus 5 verbatim).
- **Diff semantics**: hard-cut at refresh (per Open Q 8) — removed candidates' WS subscriptions are unsubscribed immediately; existing `l2_trades` rows are retained for backtest reconstruction (D-08).

---

### File 19: `alembic/versions/003_l2_tables.py` (Plan 06 — D-07 + D-08)

**Analog:** `alembic/versions/001_initial_dashboard_schema.py` (full schema + RLS pattern) + `alembic/versions/002_add_top_movers_view.py` (revision chain pattern)

**Adaptation rationale:** Same DDL idiom (sa.Column / op.create_table / op.create_index / op.execute for RLS). 5 new tables: `l2_candidates / l2_top_of_book / l2_trades / l2_signals / l2_event_cursor`. RLS anon SELECT identical to 001.

**Key code excerpt — 001 RLS pattern (lines 81-90):**

```python
op.execute("ALTER TABLE snapshots ENABLE ROW LEVEL SECURITY;")
op.execute("CREATE POLICY anon_read ON snapshots FOR SELECT USING (true);")
op.execute("ALTER TABLE markets_latest ENABLE ROW LEVEL SECURITY;")
op.execute("CREATE POLICY anon_read ON markets_latest FOR SELECT USING (true);")
op.execute("ALTER TABLE recipe_runs ENABLE ROW LEVEL SECURITY;")
op.execute("CREATE POLICY anon_read ON recipe_runs FOR SELECT USING (true);")
```

**Adaptation for 003** (RESEARCH Focus 4 verbatim ~100-line skeleton). Key shape:

```python
revision = "003"
down_revision = "002"   # NOT "001" — chain follows 002_add_top_movers_view
branch_labels = None
depends_on = None

def upgrade() -> None:
    op.create_table("l2_candidates", ...)
    op.create_index("idx_l2_candidates_asset", "l2_candidates", ["asset_id", "included_at_ts"])
    op.create_table("l2_top_of_book", ...)
    op.execute("CREATE INDEX idx_l2_tob_ts_brin ON l2_top_of_book USING BRIN (ts);")
    op.create_table("l2_trades", ...)
    op.execute("CREATE INDEX idx_l2_trades_ts_brin ON l2_trades USING BRIN (ts);")
    op.create_table("l2_signals", ...)
    op.create_table("l2_event_cursor", ...)
    for tbl in ("l2_candidates", "l2_top_of_book", "l2_trades", "l2_signals", "l2_event_cursor"):
        op.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY;")
        op.execute(f"CREATE POLICY anon_read ON {tbl} FOR SELECT USING (true);")
```

**Gotcha / pitfall** (Phase 02 L15 + R10):
- **`down_revision = "002"`** (NOT "001") — chain after `002_add_top_movers_view`. RESEARCH Focus 4 had a `[ASSUMED]` comment "verify via `alembic history`" — this is now confirmed by `ls alembic/versions/` (only 001 + 002 exist).
- **Schema add-only discipline** (Phase 02 L15 / LEARNINGS P7): never DROP/RENAME/RETYPE later — only ALTER ADD COLUMN. If dashboard needs more fields after Phase 03 lands → migration 004 (Phase 04), don't sneak ALTERs into dashboard PR.
- **service_role bypasses RLS** automatically — DO NOT add explicit service_role write policies (Phase 02 line 18-22 of 001.py warns this; same for 003).
- **BRIN index choice** (RESEARCH Focus 4): trades + tob arrive roughly chronologically → BRIN(ts) is cheap (~kB) and fast for time-range scans. btree(asset_id, ts) handles per-asset latest-N lookups.
- **`Numeric(10, 6)`** for prices (Polymarket bounded 0.0001-0.9999) — exact precision, no float-rounding surprises in dashboard aggregations (RESEARCH Focus 4 verbatim rationale).

---

### File 20: `tests/alembic/test_003.py` (Plan 06 Wave 0)

**Analog:** (no existing alembic test in tree — foundational)

**Foundational pattern:** spin up a local PostgreSQL via testcontainers OR use a Supabase fixture URL. Run `alembic upgrade head` from a clean DB and assert:
1. All 5 tables exist (`SELECT tablename FROM pg_tables WHERE schemaname='public'`)
2. RLS enabled on all 5 (`SELECT relrowsecurity FROM pg_class`)
3. `anon_read` policy exists on each (`SELECT polname FROM pg_policy`)
4. Indexes present (btree + BRIN)
5. **`alembic downgrade base` then `alembic upgrade head` is idempotent** (Phase 02 P8 — idempotent migration pattern)

**Gotcha:**
- testcontainers Postgres takes 5-10s startup — mark test `@pytest.mark.slow` and exclude from default `make test` (only run in CI).
- Alternative: hit a real Supabase dev project URL (skip RLS check if anon key only — use psql DSN service_role for full assertions).

---

### File 21: `src/polyarb/storage/l2_supabase_mirror.py` (Plan 06 — D-07 + D-08)

**Analog:** `src/polyarb/storage/supabase_mirror.py` (full file, 250 lines) — **1:1 reuse**

**Adaptation rationale:** Same long-lived client + chunked insert + fail-soft envelope. Just different table names (`l2_top_of_book` / `l2_trades` instead of `markets_latest`) and **append-only** semantics (no DELETE+INSERT — these are time-series tables; mirror `INSERT ON CONFLICT (trade_hash) DO NOTHING`).

**Key code excerpt — supabase_mirror.py constructor + push pattern (lines 70-119 verbatim):**

```python
class SupabaseMirror:
    def __init__(self, url: str, service_key: str) -> None:
        self._client: Client = create_client(url, service_key)

    def push_snapshot(self, snapshot_id, snapshot_meta, market_rows) -> bool:
        try:
            self._client.table("snapshots").upsert(snapshot_meta).execute()
            self._client.table("markets_latest").delete().neq("market_id", "").execute()
            for chunk in _chunk(market_rows, 1000):
                self._client.table("markets_latest").insert(chunk).execute()
            return True
        except Exception as e:  # noqa: BLE001 — fail-soft per RESEARCH §3
            logger.error(f"Supabase mirror failed snapshot_id={snapshot_id}: {str(e)[:200]}")
            return False
```

**Adaptation for `l2_supabase_mirror.py`:** Three public methods:
1. `push_top_of_book(rows: list[dict]) -> bool` — bulk INSERT into `l2_top_of_book`
2. `push_trades(rows: list[dict]) -> bool` — INSERT ON CONFLICT (trade_hash) DO NOTHING (idempotent backfill)
3. `upsert_candidates(rows: list[dict]) -> bool` — UPSERT into `l2_candidates` (append + mark `removed_at_ts` for diffs)

Each method uses **the same fail-soft envelope** (try/except → log error → return False, never raise).

**`_NARROW_TOB_COLUMNS` / `_NARROW_TRADE_COLUMNS`** projections — per Focus 4 schema, narrow to mirror-relevant fields only.

**Gotcha / pitfall** (Phase 02 P2 + Phase 02.1 D-01 + Phase 02.2 backlog):
- **D-12 fail-soft contract**: mirror failure → log + breadcrumb + return False → daemon does NOT crash. Mirror disabled (no SUPABASE_SERVICE_KEY) → step 7.5 else branch with **double-anchor audit** (loguru INFO + Sentry breadcrumb `category='l2-mirror'`).
- **Phase 02.2 backlog application** (per RESEARCH Open Q 9): success path ALSO emits `category='l2-mirror'` breadcrumb. Apply preemptively — both branches breadcrumb.
- **`category='l2-mirror'` vs `category='mirror'`**: Phase 02.1 P2 — use distinct category per service (l2-mirror vs L1's mirror) for clean Sentry filtering.
- **W6 (Phase 02)**: two Supabase URLs — `POLYARB_SUPABASE_URL` (HTTPS REST, supabase-py uses) vs `POLYARB_SUPABASE_DB_DSN` (postgresql://, asyncpg + alembic use). DO NOT confuse them. l2_supabase_mirror.py uses URL; events/bus.py uses DSN.
- **Connection pool** (RESEARCH R8): use **pgbouncer port `:6543`** for L2's DSN (not 5432 direct), per Focus 7 secret propagation note. L1 + L2 + alembic share Supabase Pro 90-conn pool.

---

### File 22: `tests/storage/test_l2_supabase_mirror.py` (Plan 06 Wave 0)

**Analog:** `tests/m1-perception/test_supabase_mirror.py` (mocked supabase pattern — 19,653 bytes, well-tested template)

**Key fixture pattern** (existing conftest.py `mocked_supabase` fixture, lines 283-300):

```python
@pytest.fixture
def mocked_supabase() -> Any:
    """MagicMock supabase client supporting .table(name).upsert/insert/delete/select chain."""
    client = MagicMock()
    def _table_mock(name: str) -> MagicMock:
        tbl = MagicMock()
        ...  # chainable .insert().execute() etc.
    client.table.side_effect = _table_mock
    return client
```

**Adaptation:** Add `mocked_supabase_l2` fixture (same shape) or just reuse and verify `.table("l2_top_of_book").insert(chunk).execute()` calls. Assert:
1. `push_top_of_book(rows)` chunks at 1000 rows
2. `push_trades(rows)` calls `.upsert(rows, on_conflict="trade_hash")` (idempotent backfill — note supabase-py API for ON CONFLICT)
3. Fail-soft: when mocked client raises, `push_*` returns False + logs error (use loguru StringIO sink per Phase 02.1 L4)

---

### File 23: `src/polyarb/clients/data_api_client.py` (Plan 06 — D-08)

**Analog:** `src/polyarb/clients/gamma_client.py` (httpx + aiolimiter + tenacity, full file 350 lines)

**Adaptation rationale:** Exact reuse — long-lived `httpx.AsyncClient` + `aiolimiter.AsyncLimiter` + `tenacity.AsyncRetrying`. Different endpoint, different rate limit (200/10s vs Gamma's 280/10s), different pagination scheme (limit≤500, offset≤1000 vs Gamma's offset≤10000).

**Key code excerpt — gamma_client.py constructor (lines 70-83 verbatim):**

```python
class GammaClient:
    PAGE_LIMIT = 100
    MAX_PAGES = 1000  # F-2 SECURITY: ceiling on pagination loop

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._limiter = AsyncLimiter(settings.gamma_rate_per_10s, 10)
        self._http = httpx.AsyncClient(
            timeout=settings.http_timeout_s,
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
            headers={"User-Agent": "polyarb/0.1"},
            http2=True,
            follow_redirects=False,
        )
```

**Key tenacity envelope (lines 95-130 condensed):**

```python
async for attempt in AsyncRetrying(
    stop=stop_after_attempt(s.retry_attempts),
    wait=wait_exponential(multiplier=1, min=s.retry_min_wait_s, max=s.retry_max_wait_s),
    retry=retry_if_exception_type(
        (httpx.RequestError, httpx.HTTPStatusError, httpx.TimeoutException)
    ),
    reraise=True,
):
    with attempt:
        async with self._limiter:
            r = await self._http.get(url, params=params)
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise _NonRetryableHTTPError(...)  # don't retry 4xx-non-429
```

**Adaptation for data_api_client.py** (RESEARCH Focus 6 has skeleton ~70 lines):

```python
DATA_API_BASE = "https://data-api.polymarket.com"
_LIMITER = AsyncLimiter(150, 10)  # 25% headroom under 200/10s limit

async def backfill_trades_for_asset(market_id, *, days=7, page_size=500) -> AsyncIterator[dict]:
    """yield trade dicts; offset≤1000 then time-window slide."""
    upper_bound_ts = None
    seen_trade_hashes = set()
    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            for offset in range(0, 1001, page_size):
                params = {"market": market_id, "limit": page_size, "offset": offset}
                if upper_bound_ts is not None:
                    params["beforeTimestamp"] = int(upper_bound_ts)  # [ASSUMED] — verify
                async with _LIMITER:
                    resp = await client.get(f"{DATA_API_BASE}/trades", params=params)
                if resp.status_code == 429:
                    await asyncio.sleep(10); continue
                resp.raise_for_status()
                ...
```

**Gotcha / pitfall** (Phase 02 S3 + Open Q 2):
- **offset ≤ 1000** (NOT 10000 like Gamma) — different ceiling. Phase 02 S3 precedent: Gamma offset>10000 silent 422. Data API offset>1000 will likely 422 / truncate too. Use time-window slide when offset would exceed 1000 (RESEARCH Focus 6 skeleton).
- **`beforeTimestamp` is `[ASSUMED]`** (RESEARCH Open Q 2): Plan 06 Wave 0 test MUST issue a real probe to docs example endpoint to confirm exact param name. Phase 1.5 precedent: `filterDate` param did NOT exist, silent fail.
- **Rate limit**: 200/10s = 20 req/s; configure aiolimiter `AsyncLimiter(150, 10)` for 25% headroom. With 4 concurrent backfill workers × 100 markets ≈ 1200 reqs ≈ 60s (RESEARCH Focus 6 calc).
- **Idempotent backfill**: `l2_trades.trade_hash UNIQUE` constraint (Alembic 003) → re-run safe (ON CONFLICT DO NOTHING).
- **`follow_redirects=False`** (Phase 02 F-2): explicit even though default — Polymarket CDN should never redirect us. Pin to prevent SSRF on future httpx default flip.

---

### File 24: `tests/clients/test_data_api_trades.py` (Plan 06 Wave 0)

**Analog:** `tests/m1-perception/test_gamma_client.py` (21,634 bytes — full template)

**Key test scenarios:**
1. **Pagination 0→500→1000 sequence**: mock httpx to return 500 rows each → assert 3 calls with offsets [0, 500, 1000].
2. **Time-window slide trigger**: at offset=1000, assert next loop starts at offset=0 with `beforeTimestamp` = oldest seen ts.
3. **429 retry**: inject one 429 → assert `asyncio.sleep(10)` + retry succeeds.
4. **Cutoff stop**: when oldest trade ts < (now - 7d), assert iteration ends without further pages.
5. **`trade_hash` dedup**: seed `seen_trade_hashes` with one hash; assert duplicate not yielded.

**Gotcha:** Use `respx` (httpx mocking) or `aioresponses`. Verify the `beforeTimestamp` param name BEFORE locking the test — RESEARCH Open Q 2.

---

### File 25: `src/polyarb/snapshot/orchestrator.py` (Plan 05 — MODIFY to emit snapshot_complete NOTIFY)

**Analog:** self — Phase 02 Plan 03's step 7.5 fail-soft pattern at the same orchestrator file.

**Adaptation rationale:** Insert a new step 7.7 (after step 7.6 R2 upload) that calls `events.bus.publish_snapshot_complete(...)`. Same fail-soft contract: never raises out, log + breadcrumb on failure.

**Insertion pattern (after current step 7.6 R2 update_parquet_url tail):**

```python
# Step 7.7 (Plan 05 / D-05): emit snapshot.complete NOTIFY to event bus.
# Fail-soft: failure does NOT block snapshot completion (P2 + D-12 amendment).
if settings.event_bus_enabled:
    try:
        from polyarb.events.bus import publish_snapshot_complete
        await publish_snapshot_complete(
            settings, snapshot_id=snapshot_id, taken_at_ms=taken_at_ms,
        )
    except Exception as e:
        logger.warning(f"event bus publish failed (fail-soft): {e!r}")
        sentry_sdk.add_breadcrumb(
            category="event-bus", level="warning",
            message=f"publish snapshot_complete failed: {snapshot_id}",
            data={"error": str(e)[:200]},
        )
```

**Gotcha:**
- **`event_bus_enabled` feature flag** (RESEARCH Open Q 6): default to `True` once Plan 05 lands; gate via `POLYARB_EVENT_BUS_ENABLED=1` Fly secret. L2 can smoke-test against staging emit before L1 prod fan-out.
- **Don't add this to step 7.5/7.6** — keep ordering clean: SQLite (source of truth) → mirror → R2 → event emit. Failures in event emit MUST NOT block earlier writes.
- **Tests**: extend `tests/m1-perception/test_orchestrator.py` to mock `events.bus.publish_snapshot_complete` (per Phase 02 L9 mock import site rule: `patch("polyarb.snapshot.orchestrator.publish_snapshot_complete", ...)`).

---

### File 26: `tests/m1-perception/test_orchestrator.py` (EXTEND)

**Analog:** self — existing orchestrator test file (42,088 bytes).

**Adaptation rationale:** Add 2-3 tests:
1. `test_step_7_7_emits_snapshot_complete_when_enabled` — assert `publish_snapshot_complete` called once with correct args
2. `test_step_7_7_failsoft_when_publish_raises` — inject exception, assert orchestrator continues + breadcrumb emitted
3. `test_step_7_7_skipped_when_event_bus_disabled` — `settings.event_bus_enabled=False` → publish NOT called

**Gotcha:** patch `polyarb.snapshot.orchestrator.publish_snapshot_complete` (import site), not `polyarb.events.bus.publish_snapshot_complete`. Phase 02 L9 verbatim.

---

### File 27: `tests/chaos/test_l2_chaos_plan.py` (Plan 07 Wave 0)

**Analog:** `tests/m1-perception/test_chaos_supabase.py` (Phase 02 chaos verification structural pattern)

**Adaptation rationale:** Declarative chaos plan as `dataclass`. Each injection (Inj L2-1..5 from RESEARCH) has fields:
- `inj_id: str` (e.g., "L2-1")
- `description: str`
- `code_path_triggered: str`
- `truths_verified: list[str]`
- `programmatic_verification_cmds: list[str]` (Phase 02.1 D-09 verification-ownership)
- `container_localhost_fallback_cmd: str` (Phase 02.1 L8)
- `cleanup: str`

Test asserts every truth has `programmatic? = yes` (Phase 02.1 D-09 + L7 anti-pattern).

```python
@dataclass(frozen=True)
class ChaosInjection:
    inj_id: str
    truths: list[str]
    programmatic_cmds: list[str]
    container_localhost_fallback: str | None  # P8 must NOT be None

L2_CHAOS_PLAN = [
    ChaosInjection(
        inj_id="L2-1",
        truths=["watchdog reconnect within 45s", "l2_top_of_book write resumes"],
        programmatic_cmds=[
            "curl -fsS https://polyarb-l2.fly.dev/healthz | jq '.checks.\"ws:last_event_age_seconds\"'",
            "psql $SUPABASE_DSN -c \"SELECT max(ts) FROM l2_top_of_book WHERE ts > now() - interval '5 minutes'\"",
        ],
        container_localhost_fallback="flyctl ssh console -a polyarb-l2 -C 'curl localhost:8080/healthz'",
    ),
    ...  # L2-2 through L2-5
]

def test_every_injection_has_programmatic_verification():
    for inj in L2_CHAOS_PLAN:
        assert inj.programmatic_cmds, f"{inj.inj_id}: no programmatic verification"
        assert inj.container_localhost_fallback is not None, f"{inj.inj_id}: no fallback"
```

**Gotcha** (Phase 02 L7 + L8 + Phase 02.1 L8):
- **NO UI navigation steps** allowed — Phase 02 L7 "SESSION 20 E2E self-deception" reverberates. Every truth = curl / psql / flyctl ssh / Sentry API command.
- **Container-localhost fallback** is **mandatory** (Phase 02.1 L8) — if Fly proxy is mid-broken (e.g., another chaos in flight), verification still works.
- **Inj L2-4 is the cross-bug pre-check** (per RESEARCH Plan Task table cross-bug column) — WS reconnect storm + Supabase Free paused simultaneously. Phase 02.1 L2: cross-bug interaction MUST be designed in wave-1, not discovered chaos-time.

---

### File 28: `src/polyarb/http/l2_health.py` (Plan 03 — D-06 health endpoints for L2)

**Analog:** `src/polyarb/http/health.py` (271 lines, Phase 02.1 P5 helper-first refactor pattern)

**Adaptation rationale:** Phase 02.1 D-05/D-06 split (`/health` IETF strict / `/healthz` always-200) carries over **verbatim**. Decision: separate file `l2_health.py` OR extend `health.py` with a `_build_l2_health_checks` helper. **Recommendation: separate file `l2_health.py`** (per Phase 02.1 P3 — independent middleware per subsystem).

**Key code excerpt — health.py `_build_health_checks` helper (lines 77-110 condensed):**

```python
def _build_health_checks(store, settings, now_s) -> tuple[dict, str]:
    """Compute all health sub-checks and the overall status.
    Shared by /health (IETF strict — fail → 503) and /healthz (always 200).
    """
    checks = {}
    overall = "pass"

    # Check 1: snapshot age
    last_snapshot = store.get_latest_snapshot()
    ...
    checks["snapshot:last_success_age_seconds"] = [{...}]

    # Check 3: Supabase mirror age (when settings.supabase_mirror_enabled)
    if settings.supabase_mirror_enabled:
        ...

    return checks, overall

async def health(request):
    checks, overall = _build_health_checks(store, settings, time.time())
    body = _build_health_body(overall, checks, settings)
    http_status = 503 if overall == "fail" else 200
    return JSONResponse(body, status_code=http_status, media_type=HEALTH_CONTENT_TYPE)

async def healthz(request):
    checks, overall = _build_health_checks(store, settings, time.time())
    body = _build_health_body(overall, checks, settings)
    return JSONResponse(body, status_code=200, media_type=HEALTH_CONTENT_TYPE)  # ALWAYS 200
```

**Adaptation for l2_health.py — new sub-checks per Phase 03:**
- `ws:connection_state` — pass when WS state == CONNECTED, warn when WAITING_FOR_EVENT, fail when RECONNECTING > 60s
- `ws:last_event_age_seconds` — pass < 30s, warn 30-120s, fail > 120s (corresponds to watchdog threshold + 1 reconnect cycle)
- `ws:candidates_tracked` — informational (current count of subscribed assets)
- `mirror:l2_tob_age_seconds` — pass < 5min, warn 5-30min, fail > 30min
- `event_bus:listener_state` — pass when listening, warn during 5s reconnect window, fail when DSN unavailable

**Gotcha / pitfall** (Phase 02.1 P5 + D-05 + D-06):
- **`_build_l2_health_checks` helper**: same pattern — handler shrinks to wrapper, all logic in helper (Phase 02.1 P5).
- **`/healthz` MUST always return 200** (Phase 02.1 D-05 + BUG-6 fix): Fly proxy routes traffic based on `/healthz` status. If L2 daemon is degraded but TCP still alive, we want Fly to keep routing so `/control/*` admin endpoints remain reachable (BUG-8 cross-bug interaction).
- **`/health` IETF strict** (Phase 02.1 D-05): Better Stack external probe target. 503 is the告警 signal.
- **`serviceId = "polyarb-l2"`** (NOT polyarb-l1) — Sentry can split events per service tag (Focus 7 Sentry env differentiation).
- **WS state surface**: ws_consumer must expose `request.app.state.ws_consumer.current_state` (CONNECTED / WAITING_FOR_EVENT / RECONNECTING) + `last_event_at_s` for the health check helper to read. Wire via `app.state.ws_consumer` in create_l2_app factory.

---

### File 29: `tests/m1-perception/test_l2_health_endpoint.py` (Plan 03 Wave 0)

**Analog:** `tests/m1-perception/test_health_endpoint.py` (Phase 02.1 verbatim template, 7,579 bytes)

**Key test scenarios** (mirror Phase 02.1 L8 sub-tests):
1. `test_l2_pass_when_ws_connected_and_fresh` — mock ws_consumer.state=CONNECTED + last_event_age_s=5 → `/health` 200 status=pass
2. `test_l2_fail_when_ws_reconnecting_too_long` — mock state=RECONNECTING + age=120s → `/health` 503 status=fail
3. `test_healthz_always_200_even_when_failing` — even with failing checks, `/healthz` returns HTTP 200 (BUG-6 invariant)
4. `test_pass_response_includes_serviceId_polyarb_l2` — assert body["serviceId"] == "polyarb-l2"

---

### File 30: `docs/learning/10-L2-跟踪.md` (Plan 08 — D-07 + D-09)

**Analog:** `docs/learning/09-生产化运维.md` (Phase 02.1 P7 — file:line discipline)

**Adaptation rationale:** Same 体例 — 30秒心智模型 + 关键代码片段 + 设计取舍 + 自检题 + FAQ 增量区. Topic = "L2 daemon 如何把 candidate WS 流变成实时信号源".

**Gotcha / pitfall** (Phase 02.1 P7 verbatim):
- **All file:line references MUST come from `grep -n` on landed code**, NOT from RESEARCH.md estimates. Plan execution shifts line numbers; ditto for new file additions.
- Chinese only (per CLAUDE.md §4 docs/ language convention).
- Phase 02.1 baseline: 324 lines, 21 file:line refs — match approximate scale.

---

### File 31: Makefile (extend with L2 targets)

**Analog:** self — existing Makefile patterns (e.g., `make snapshot-markets`)

**New targets (per RESEARCH §"Project Constraints"):**
- `make daemon-l2-run-local` — `uv run python -m polyarb.daemon.l2_main` with dev env
- `make smoke-l2-health` — `curl -fsS https://polyarb-l2.fly.dev/healthz | jq`
- `make smoke-l2-ws` — 30s WS test against a known liquid asset
- `make migrate-l2` — `uv run alembic upgrade head` (same as L1 — single Alembic chain, both migrations apply)
- `make backfill-trades MARKET=<id>` — `uv run python -m polyarb.clients.data_api_client --market $(MARKET)` for manual REST backfill

**Gotcha** (CLAUDE.md §"命令入口约定"):
- **Every new command** introduced by Plans 01-08 MUST have a `make` target. Plans without Makefile targets are non-compliant (pre-commit hook enforces SUMMARY but Makefile is a soft convention — Plan PLAN.md must explicitly list "Makefile target" as a deliverable).

---

### File 32: `03-SOAK-LOG.md` (Plan 07 — chaos verification log)

**Analog:** `.planning/workstreams/m1-perception/phases/02-l1-production-grade/02-SOAK-LOG.md` (Phase 02 + 02.1 chaos verification log)

**Adaptation rationale:** Verbatim 体例 — each chaos injection as a dated segment with:
- Timestamp + chaos action
- Code path triggered
- Verification commands (the programmatic ones per File 27)
- Output evidence (grep-able lines from logs / Sentry / Telegram screenshots replaced by API JSON dumps)
- Verdict (PASS / partial / fail) + root cause if not PASS

**Phase 02.1 5-Layer Root Cause analysis precedent** (Inj 1 + Inj 4 segments): when an injection produces unexpected results, do not summary-skip; document 5-layer root cause.

---

### File 33: `tests/m1-perception/conftest.py` (EXTEND with L2 fixtures)

**Analog:** self — existing conftest.py (22,634 bytes)

**Adaptation rationale:** Add L2-specific fixtures alongside existing `http_test_client` / `make_signed_request`:
- `l2_http_test_client(daemon_settings_for_test)` — Starlette TestClient with L2 routes (similar to existing `http_test_client`, but uses `create_l2_app` factory + injects `mock_ws_consumer`)
- `mock_ws_consumer` — MagicMock with `.state`, `.last_event_at_s`, `.subscribed_assets` attributes
- `mocked_supabase_l2` — copy `mocked_supabase` shape, just verify l2 table names

**Gotcha** (Phase 02.1 L3):
- `daemon_settings_for_test` factory must explicitly pass `supabase_url=""` + `supabase_db_dsn=SecretStr("")` etc to override dev `.env`. Phase 02.1 L3 .env-渗透 trap.

---

## Shared Patterns (Cross-Cutting, Applied to All Relevant Plans)

### SP1: Fail-soft envelope (D-12 invariant — Phase 02 P2)

**Source:** `src/polyarb/storage/supabase_mirror.py` lines 109-119

**Apply to:** All L2 mirror writes (Plan 06), event bus publish (Plan 05), Data API backfill (Plan 06)

```python
try:
    # main operation
    return True
except Exception as e:  # noqa: BLE001 — fail-soft per RESEARCH §3
    logger.error(f"<service> failed <context>: {str(e)[:200]}")
    sentry_sdk.add_breadcrumb(category="<service-name>", level="warning",
                              message=f"<service> <context> failed",
                              data={"error": str(e)[:200]})
    return False
```

**Pitfall:** Phase 02.1 S1 — breadcrumb upload requires a triggering event. If daemon never crashes, breadcrumb never reaches Sentry (design-unreachable). Apply Phase 02.2 backlog fix preemptively: emit breadcrumb on **both** success and failure paths (`category='l2-mirror'` + `level='info'` for success, `level='warning'` for failure).

---

### SP2: Double-anchor audit log (Phase 02.1 P1)

**Source:** `src/polyarb/snapshot/orchestrator.py` step 7.5 else branch (Plan 02.1-01 commit `7e1a719`)

**Apply to:** All config-disabled fail-soft skip paths in L2 daemon (mirror disabled, event bus disabled, R2 disabled).

```python
if not settings.l2_mirror_enabled:
    logger.info(f"l2-mirror skipped: mirror_enabled=False")
    sentry_sdk.add_breadcrumb(
        category="l2-mirror", level="info",
        message=f"l2-mirror skipped at snapshot_id={snapshot_id} (mirror_enabled=False)",
        data={"snapshot_id": snapshot_id, "rows": len(rows)},
    )
    return False
```

**Pitfall:** `category='l2-mirror'` (NOT 'mirror' — that's L1's). Phase 02.1 P2 — semantic separation for clean filtering.

---

### SP3: Helper-first refactor (Phase 02.1 P5)

**Source:** `src/polyarb/http/health.py` `_build_health_checks` (lines 77-208)

**Apply to:** `/health` + `/healthz` in L2 (File 28 — both endpoints call `_build_l2_health_checks(...)` helper, only the HTTP status code differs).

---

### SP4: Server-started gate (Phase 02 P9)

**Source:** `src/polyarb/daemon/main.py` lines 78-86

**Apply to:** L2 daemon entry (File 7) — uvicorn MUST bind socket before any WS / event listener / scheduler task starts.

```python
server_task = asyncio.create_task(server.serve())
for _ in range(100):
    if server.started: break
    await asyncio.sleep(0.1)
# NOW start WS + listener tasks
```

**Pitfall:** Without this gate, Fly's 120s grace period times out — daemon never gets a chance to handle a real probe. Phase 02 L5 verbatim.

---

### SP5: Mock at import site (Phase 02 L9)

**Source:** `tests/m1-perception/test_orchestrator.py` (and all Phase 02 tests)

**Apply to:** Every test in `tests/clients/test_ws_market_client.py`, `tests/events/test_*.py`, `tests/observation/test_l2_candidate_refresh.py`.

```python
# WRONG: patch("polyarb.events.bus.publish_snapshot_complete", ...)
# RIGHT: patch("polyarb.snapshot.orchestrator.publish_snapshot_complete", ...)  # import site
```

---

### SP6: Loguru StringIO sink (Phase 02.1 L4)

**Source:** `tests/m1-perception/test_orchestrator_mirror_skip.py` patterns-established

**Apply to:** All L2 tests asserting log output.

```python
import io
from loguru import logger

sink = io.StringIO()
sink_id = logger.add(sink, format="{message}", level="INFO")
try:
    # ... run code that should log
    assert "expected log" in sink.getvalue()
finally:
    logger.remove(sink_id)
```

**Pitfall:** `pytest caplog` does NOT see loguru output. Phase 02.1 L4 verbatim.

---

### SP7: Verification ownership (Phase 02.1 D-09)

**Apply to:** Every truth in every PLAN.md for Phase 03.

Each truth MUST be verifiable via shell/curl from Claude's seat. NEVER delegate UI navigation to user. RESEARCH §"Programmatic Verification Surfaces" has the complete table. If a truth cannot be programmatically verified, redesign the truth (or the underlying code) until it can.

**Pitfall:** Phase 02 L7 ("SESSION 20 E2E self-deception") — `make sentry-test` and `make telegram-test` are unit-style triggers; they do NOT verify the real alert chain. Verification = chaos injection real-trigger + grep-able Sentry/Telegram evidence.

---

### SP8: Cross-bug pre-check (Phase 02.1 L2)

**Apply to:** Plan-phase wave ordering for Phases 03.

Before locking wave order, audit all plan pairs for cross-bug interaction. RESEARCH "Cross-bug pre-check" table lists 4 known interactions for Phase 03:
1. Watchdog reconnect storm + NOTIFY storm
2. Mirror write + WS event SQL pool contention
3. GHA keepalive silent fail + Plan 06 still working (false confidence)
4. L1 + L2 hitting same Postgres connection limit

Each row needs a mitigation **locked at plan-time**, not discovered chaos-time.

---

## No Analog Found (planner uses RESEARCH.md verbatim skeleton)

| File | Role | Data Flow | Reason |
|---|---|---|---|
| `src/polyarb/events/listener.py` | service (asyncpg LISTEN) | event-driven (reverse) | First LISTEN consumer in tree |
| `tests/alembic/test_003.py` | test (alembic upgrade) | DDL replay | First alembic test in tree |
| `scripts/fly_secrets_sync.sh` | utility (ops shell script) | shell | First multi-app secret sync script |

For these 3, use RESEARCH.md §Focus 3 + §Focus 4 + §Focus 7 verbatim skeletons; reference Phase 02 foundational patterns (fail-soft envelope from SP1, loguru sink from SP6) where applicable.

---

## Metadata

**Analog search scope:**
- `src/polyarb/daemon/` (3 files)
- `src/polyarb/clients/` (2 files — gamma + clob)
- `src/polyarb/http/` (4 files — app + health + control + scan)
- `src/polyarb/storage/` (5 files — supabase_mirror as primary)
- `src/polyarb/observation/` (8 files — scanner + watchlist + recipes as primary)
- `src/polyarb/observability/` (3 files — sentry + logging + redact)
- `src/polyarb/snapshot/orchestrator.py` (step 7.5 / 7.6 patterns)
- `.github/workflows/` (2 files)
- `alembic/versions/` (2 files)
- `tests/m1-perception/` (45 test files)
- `fly.toml` + `Dockerfile` + `Makefile`

**Files scanned:** ~75 source + test files; 5 phase-level documents (Phase 02 + 02.1 LEARNINGS + Phase 03 CONTEXT/RESEARCH/VALIDATION).

**Pattern extraction date:** 2026-05-23

**Confidence:** HIGH for 27/33 files (exact / role-match analogs in tree). FOUNDATIONAL for 3 (RESEARCH.md skeleton drives). MEDIUM for 3 (no analog but Phase 02 foundational patterns apply: SP1 fail-soft + SP6 loguru sink).

---

## Planner Quick-Reference (use this for PLAN.md authoring)

When a Plan PLAN.md describes a new file, cite this PATTERNS.md row directly:

> "src/polyarb/daemon/l2_main.py — per `03-PATTERNS.md File 7`, copy init order from `src/polyarb/daemon/main.py:38-90` verbatim (P9 server-started gate mandatory); replace scheduler with WSConsumer + WsWatchdog + EventListener tasks per RESEARCH Focus 1+2+3."

This locks pattern fidelity at plan time and reduces gsd-executor's freedom to drift.
