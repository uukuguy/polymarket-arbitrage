---
phase: 03
phase_name: "L2 Orderbook Tracking (分钟级 daemon)"
workstream: "m1-perception"
generated: "2026-05-23"
domain: "WebSocket realtime + event-driven daemon + Supabase mirror + Fly deploy"
confidence: HIGH (CONTEXT decisions locked, refine to implementation-ready)
research_refs:
  - .planning/threads/market-observation-architecture.md §2.2 (WS) + §2.6 (DB tier)
  - .planning/workstreams/m1-perception/phases/02-l1-production-grade/02-LEARNINGS.md (18D/15L/14P/9S)
  - .planning/workstreams/m1-perception/phases/02.1-phase-02-fix-up-2-p1-backlog-health-503-trade-off/02.1-LEARNINGS.md (9D/8L/7P/5S)
  - docs.polymarket.com/market-data/websocket/overview (fetched 2026-05-23)
  - docs.polymarket.com/changelog (fetched 2026-05-23) — 2025-05-28 "Websocket Changes"
---

# Phase 03 RESEARCH — L2 Orderbook Tracking 实现技术深拓

## TL;DR

WS market channel 单连接订阅整个 candidate-set 无 token 上限 (`assets_ids[]`, 动态 subscribe/unsubscribe) — 用 `websockets` 16.0 库 + `async for ... in connect(...)` 内建重连 + 业务层 30s staleness watchdog (TCP-keepalive 不可信, py-clob-client #292)。Cross-process event bus 用 **Postgres LISTEN/NOTIFY via asyncpg** (复用 Supabase Pro DSN, 零额外厂商, < 100ms latency); Supabase realtime 作为 stretch 备选 (有 supabase-py 2.10 限制风险)。4 个新 Supabase 表 (l2_candidates / l2_top_of_book / l2_trades / l2_signals) + Alembic 002 migration + RLS anon-SELECT 模式复用 001。polyarb-l2 Fly app = 复制 polyarb-l1 fly.toml/Dockerfile/deploy.yml 三件套, 改 `app="polyarb-l2"` + 单 process group (无 cron) + 仍 `ams` region。

**Primary recommendation:** WS 主路径用 stdlib-style `websockets>=16.0,<17` (已 verify PyPI 2026-01-10 发布), 业务层 watchdog 独立于库 builtin keepalive; event bus first-cut 选 `asyncpg` + Postgres NOTIFY (轻、跨厂商无锁、Supabase DSN 已有); L2 数据写直接复用 `supabase-py` (httpx 已 in dep), 不引入 websocket 路径; 部署 100% 复制 L1 三件套, 改 3 处文本即可。

---

## Validation Architecture

### Wave 0 RED tests (failing tests required before any GREEN feat commit)

| Plan | Wave 0 test file | What it pins | Why before code |
|---|---|---|---|
| 01 (GHA Supabase keepalive) | `tests/test_supabase_keepalive_yml.py` | parse `.github/workflows/supabase-keepalive.yml`, assert cron schedule daily + wget endpoint format | catch YAML typo on first commit, not after 7 天 silent fail (L8 Phase 02 precedent) |
| 02 (polyarb-l2 Fly bootstrap) | `tests/test_fly_l2_config.py` | parse fly-l2.toml, assert `app="polyarb-l2"` + region=ams + processes 仅含 app (NO cron) + http_service.checks.path="/healthz" | 防 cron line copy from L1 fly.toml — wrong daemon shape |
| 03 (L2 daemon entry) | `tests/daemon/test_l2_main_startup.py` | construct daemon (mock everything), assert init order: init_logging → init_sentry → SQLite store → WSClient init → app create → uvicorn started gate | structural — daemon entry shape must match L1 main.py pattern (P9 server-started gate) |
| 04 Task 1 (WS client) | `tests/clients/test_ws_market_client.py` | mock websockets.connect, assert subscribe payload shape `{"assets_ids":[...], "type":"market", "initial_dump":true}` + dynamic subscribe/unsubscribe operation field present | locks WS protocol shape — protocol drift → silent data loss |
| 04 Task 2 (Watchdog) | `tests/daemon/test_ws_watchdog.py` | inject 30s no event → assert reconnect triggered + exp backoff sequence (1,2,4,8,16,30) + initial_dump=true on resubscribe | the silent-freeze bug (#292) is the whole reason this phase exists — pin it |
| 05 (Candidate refresh) | `tests/observation/test_l2_candidate_refresh.py` | given old_set ∪ new_set, assert WSClient.subscribe(added) + .unsubscribe(removed) called with diff lists | diff algo bug → 漏订阅 / 重订阅 storm |
| 06 (Alembic 002 + mirror) | `tests/storage/test_l2_supabase_mirror.py` + `alembic/test_002.py` | run alembic upgrade head, assert 4 tables exist + RLS policies applied + index list matches plan | schema drift catches via alembic re-run (Phase 02 L15 "adapt UI, don't grow schema") |
| 06 (Data API backfill) | `tests/clients/test_data_api_trades.py` | mock httpx, assert pagination loop respects offset≤1000 + time-window sliding fallback | offset>1000 silent truncation reproduce (S3 Phase 02 precedent for Gamma >10000) |
| 07 (Chaos verification) | `tests/chaos/test_l2_chaos_plan.py` | declarative chaos plan as dataclass, assert each truth has programmatic verification path (Sentry API / flyctl ssh / curl) | Phase 02.1 L7 "Pyright noise" + L1 fail-soft 反例 — chaos plan 不能依赖 UI 翻找 |

### Manual verification points (per Phase 02.1 D-09 verification-ownership)

Every truth in 03-VALIDATION.md must satisfy: **`programmatic? = yes`** — verifiable via shell command from Claude's seat, NOT user UI navigation.

| Verification surface | Programmatic command |
|---|---|
| polyarb-l2 deployed & alive | `curl -fsS https://polyarb-l2.fly.dev/healthz \| jq '.status'` |
| WS connection up | `flyctl ssh console -a polyarb-l2 -C "ss -tn dst :443"` |
| WS staleness watchdog state | `curl -fsS https://polyarb-l2.fly.dev/healthz \| jq '.checks.ws:last_event_age_seconds'` (Plan 03 adds this check) |
| candidate refresh count | Supabase Postgres query `SELECT count(*) FROM l2_candidates WHERE included_at_ts > now() - interval '24 hours'` via psql DSN |
| trades accumulation | `SELECT count(*), max(ts) FROM l2_trades` via Supabase DSN |
| Sentry event lookup | `curl https://de.sentry.io/api/0/projects/<org>/<proj>/events/?statsPeriod=1h -H "Authorization: Bearer $SENTRY_TOKEN"` |
| Telegram alert fired | `curl https://api.telegram.org/bot$TG_TOKEN/getUpdates \| jq` (within recent window) |
| GHA keepalive workflow ran | `gh run list -w supabase-keepalive.yml --limit 7` (one per day) |

**Anti-pattern reminder (Phase 02 L7):** Never claim "verified" without naming the exact command + grep-able output line.

### Chaos verification points (per Phase 02 L6/L7 + P14)

Each prod-grade truth needs an injection plan. **Design = reverse from alert code path, not "what chaos can I think of"** (P14).

| Chaos injection | Code path triggered | Truth verified | Cleanup |
|---|---|---|---|
| **Inj L2-1**: kill WS connection mid-stream (`flyctl machine restart` on remote ws upstream isn't feasible — substitute: `iptables` block 443 inside container OR set `POLYARB_WS_TEST_KILL=1` to force `.close()` in code) | watchdog 30s timeout → reconnect → resubscribe with initial_dump | watchdog state transition + reconnect succeeds + `l2_top_of_book` 写延迟 ≤45s | unset flag / unblock iptables |
| **Inj L2-2**: 撤 `POLYARB_SUPABASE_SERVICE_KEY` from Fly secrets → restart machine | mirror write fail-soft path → loguru + Sentry breadcrumb (`category='l2-mirror'`) emit | breadcrumb pulled via Sentry API + daemon does NOT crash + `l2_top_of_book` write skipped without exception | restore secret + restart |
| **Inj L2-3**: 撤 L1 `snapshot.complete` event emission (mock Postgres NOTIFY channel block) | L2 candidate refresh starves → falls back to last_known set + Sentry warning crumb after 24h | dashboard shows stale candidates timestamp + warning log line | restore L1 emission |
| **Inj L2-4 (cross-bug pre-check, per Phase 02.1 L2)**: simulate prod = (a) WS reconnect storm + (b) Supabase Free tier paused at same time | does daemon hold L1 connection state? Does GHA keepalive auto-recover? Are alerts deduped? | daemon survives both + alert deduplication active + GHA next run unpause | natural — Supabase auto-unpause after GHA ping |
| **Inj L2-5**: Data API /trades 429 rate limit (200/10s) during 7d backfill | backfill retry-with-backoff + partial completion checkpoint | backfill resumes from last checkpoint after rate limit clears | natural — wait 10s |

Each Inj must have a **container-localhost fallback path** (Phase 02.1 L8): if Fly proxy is broken (e.g., /healthz still being修复 mid-phase), every verification must work via `flyctl ssh console -C "curl localhost:8080/..."`.

---

## Focus 1: Python WS client library 选型 (D-02)

### Options evaluated

| Library | Latest version (verified 2026-05-23) | Pros | Cons | Verdict |
|---|---|---|---|---|
| **`websockets`** ([CITED: pypi.org/project/websockets — 16.0 published 2026-01-10]) | **16.0** | stdlib-style asyncio-native; `async for ws in connect(...)` 内建 reconnect; PING/PONG 自动 (`ping_interval=20s` default, configurable); 单一职责无 HTTP 包袱; widely-used (2k+ GitHub stars); excellent type hints since 12.0 | 不自带"业务层" watchdog (only TCP-level keepalive); 默认 20s PING — Polymarket 要 10s, 必须 override | ✅ **RECOMMENDED** |
| `aiohttp` (`aiohttp.ClientSession().ws_connect`) | 3.11.x | 项目已部分 in dep chain (via supabase-py? — actually `httpx` is the http lib; aiohttp NOT in dep); WS API mature | 引入完整 HTTP server stack 只为 WS = 大锤打小钉; no built-in connect-iterator reconnect; supabase-py 不依赖它 (verified — supabase-py 用 httpx) | ❌ rejected — overhead |
| `python-socketio` | 5.x | 不适用 — Polymarket 是 raw WS 非 Socket.IO | — | ❌ wrong protocol |
| `websocket-client` (sync legacy) | 1.8.x | thread-based 兼容旧代码 | 不是 asyncio-native; 与 daemon asyncio 架构冲突 | ❌ rejected |

**Decision: `websockets>=16.0,<17`** [VERIFIED: PyPI 2026-05-23 via `pip index versions`].

**Why over aiohttp:** project HTTP lib is `httpx[http2]>=0.27,<0.28` not aiohttp. Introducing aiohttp purely for WS adds a 2nd HTTP stack + duplicate connection pool. `websockets` is the focused choice with no HTTP-server overhead.

**Critical override:** `websockets.connect(ping_interval=10, ping_timeout=10)` to match Polymarket 10s requirement [CITED: docs.polymarket.com/market-data/websocket/overview]. Default 20s WILL be dropped server-side ~10s after silence.

### Minimal connect-subscribe skeleton (~30 lines)

```python
# src/polyarb/clients/ws_market_client.py
"""Polymarket WS market channel client — websockets 16.0 + asyncio.

Endpoint: wss://ws-subscriptions-clob.polymarket.com/ws/market
Field name: assets_ids (with 's', confirmed thread §2.2 Q1)
Heartbeat: server requires PING every 10s (docs.polymarket.com).
"""
from __future__ import annotations
import asyncio
import json
from typing import AsyncIterator
import websockets
from loguru import logger

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"

async def stream_market_events(
    assets_ids: list[str],
    *,
    initial_dump: bool = True,
    ping_interval_s: int = 10,  # Polymarket REQUIRES 10s, NOT default 20s
) -> AsyncIterator[dict]:
    """Subscribe to market channel; yield parsed event dicts forever.

    Auto-reconnect via websockets 16.0 connect-iterator pattern. Re-subscribe
    on each reconnect (server doesn't persist subscriptions).
    """
    async for ws in websockets.connect(
        WS_URL,
        ping_interval=ping_interval_s,
        ping_timeout=ping_interval_s,
        max_size=2**22,  # 4 MiB cap for initial_dump book snapshots
    ):
        try:
            sub = {
                "type": "market",
                "assets_ids": assets_ids,
                "initial_dump": initial_dump,
            }
            await ws.send(json.dumps(sub))
            logger.info(f"ws subscribed: {len(assets_ids)} assets, initial_dump={initial_dump}")
            async for raw in ws:
                try:
                    yield json.loads(raw)
                except json.JSONDecodeError as e:
                    logger.warning(f"ws non-JSON frame ignored: {e!r}")
        except websockets.ConnectionClosed as e:
            logger.warning(f"ws connection closed code={e.code}; reconnecting…")
            # falls through to next async-for iteration → reconnect
```

[CITED: websockets.readthedocs.io/en/stable/reference/asyncio/client.html — `connect()` as async iterator for auto-reconnect]

### Dynamic subscribe / unsubscribe shape

[CITED: docs.polymarket.com/changelog 2025-09-24 — "Dynamic Subscriptions: Add, remove, and modify subscriptions without reconnecting"]

```python
async def add_subscriptions(ws, added_ids: list[str]) -> None:
    """Add token subscriptions on a live WS without reconnecting."""
    await ws.send(json.dumps({
        "operation": "subscribe",  # field name confirmed thread §2.2 Q1
        "assets_ids": added_ids,
    }))

async def remove_subscriptions(ws, removed_ids: list[str]) -> None:
    await ws.send(json.dumps({
        "operation": "unsubscribe",
        "assets_ids": removed_ids,
    }))
```

[ASSUMED] The exact `operation` field schema — docs.polymarket.com/changelog only said "Dynamic Subscriptions" exist (2025-09-24). The field name `operation` is consistent with the broader API reference (see thread §2.2 Q1 referencing "{operation: 'subscribe', assets_ids: [...]}") but not explicitly re-confirmed in the changelog fetch. **Plan-phase MUST verify** by reading `https://docs.polymarket.com/api-reference/wss/market` Subscription Request example for current authoritative payload schema — if changed, update accordingly.

### Reconnect philosophy

`websockets` 16.0's `async for ws in connect(...)` reconnects automatically on `EOFError | OSError | asyncio.TimeoutError | HTTP 500/502/503/504`. **This is necessary but NOT sufficient** for Polymarket silent-freeze (issue #292) — TCP stays open, PONG keeps flowing, but business events stop. That's the watchdog's job (Focus 2).

---

## Focus 2: WS staleness watchdog state machine (D-03)

### State machine design

```
┌─────────────┐  ws frame received   ┌─────────────────────┐
│  CONNECTED  │ ───────────────────► │ WAITING_FOR_EVENT   │
│  (initial)  │                      │ (last_event_time T) │
└─────────────┘                      └──────────┬──────────┘
       ▲                                         │
       │ resubscribe + initial_dump=true        │ now - T > 30s
       │                                         ▼
       │                              ┌─────────────────────┐
       │                              │   RECONNECTING      │
       │      reconnect success       │   (close + reopen)  │
       └──────────────────────────────┤   exp backoff:      │
                                      │   1, 2, 4, 8, 16,30s│
                                      └─────────────────────┘
```

### Implementation skeleton (~70 lines using asyncio.Event + asyncio.wait)

```python
# src/polyarb/daemon/ws_watchdog.py
"""30s staleness watchdog — independent of websockets-builtin TCP keepalive.

Per thread §2.2 Q4 + py-clob-client issue #292: TCP-level PING/PONG can keep
flowing while business event stream silently freezes. We MUST measure event
inter-arrival and force-reconnect when it stalls.
"""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, field
from typing import Callable
from loguru import logger

# Exp backoff sequence: 1, 2, 4, 8, 16, capped 30
_BACKOFF_S = [1, 2, 4, 8, 16, 30]


@dataclass
class WatchdogState:
    last_event_time_s: float = field(default_factory=time.monotonic)
    reconnect_attempt: int = 0
    state: str = "CONNECTED"  # CONNECTED | WAITING_FOR_EVENT | RECONNECTING


class WsWatchdog:
    """Bracket a WS consumer loop with staleness detection.

    Usage:
        async with WsWatchdog(stale_s=30) as wd:
            async for event in stream_market_events(...):
                wd.touch()
                # ... process event
            # if loop exits abnormally (ConnectionClosed), wd raises to trigger
            # the surrounding `async for ws in connect(...)` outer loop's
            # reconnect path. backoff is applied here, not in websockets lib.
    """

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
        self._state.reconnect_attempt = 0  # reset backoff on healthy frame

    async def watch(self, stop_event: asyncio.Event) -> None:
        """Background task: trigger reconnect if no touch within stale_s."""
        while not stop_event.is_set():
            elapsed = time.monotonic() - self._state.last_event_time_s
            if elapsed > self.stale_s:
                # Trigger reconnect — caller's connect-iterator will catch
                self._state.state = "RECONNECTING"
                attempt = min(self._state.reconnect_attempt, len(_BACKOFF_S) - 1)
                wait_s = _BACKOFF_S[attempt]
                self._state.reconnect_attempt += 1
                logger.warning(
                    f"ws watchdog: stale {elapsed:.1f}s > {self.stale_s}s; "
                    f"reconnect attempt {self._state.reconnect_attempt}, "
                    f"backoff {wait_s}s"
                )
                if self._on_reconnect:
                    self._on_reconnect()
                await asyncio.sleep(wait_s)
                # Reset timer so we don't immediately re-fire on next watch tick
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

### Integration with stream_market_events

```python
async def run_ws_consumer(
    initial_assets: list[str],
    on_event: Callable[[dict], None],
    stop_event: asyncio.Event,
) -> None:
    watchdog = WsWatchdog(stale_s=30.0)
    wd_task = asyncio.create_task(watchdog.watch(stop_event))
    try:
        async for event in stream_market_events(initial_assets):
            if stop_event.is_set():
                break
            watchdog.touch()
            on_event(event)
    finally:
        wd_task.cancel()
```

### Forcing reconnect from watchdog (the tricky bit)

`websockets.connect()` async-iterator only reconnects on `ConnectionClosed`. To force-close from outside, we need a handle to the live `ws` object. Two patterns:

**Pattern A (recommended)** — watchdog gets ws ref via callback, calls `await ws.close()`:

```python
class WsConsumer:
    def __init__(self):
        self._current_ws = None
        self._reconnect_signal = asyncio.Event()

    async def run(self, ...):
        async for ws in websockets.connect(WS_URL, ...):
            self._current_ws = ws
            try:
                await ws.send(json.dumps(subscribe_payload))
                async for raw in ws:
                    self.watchdog.touch()
                    self.on_event(json.loads(raw))
                    if self._reconnect_signal.is_set():
                        self._reconnect_signal.clear()
                        await ws.close()  # triggers outer reconnect
            except websockets.ConnectionClosed:
                logger.info("ws reconnecting after watchdog signal")
```

Watchdog `on_reconnect` callback = `lambda: self._reconnect_signal.set()`.

[ASSUMED — implementation detail not from docs] This pattern is idiomatic; alternative is `asyncio.Task.cancel()` on the outer iterator, but that's brittle.

**Re-subscribe with `initial_dump=true`** [CITED: docs.polymarket.com/changelog 2025-05-28]: every new `ws` iteration sends `subscribe` with `initial_dump=True` so the orderbook baseline is re-established. Without it, the L2 top-of-book table would carry stale state until the next `price_change` event.

---

## Focus 3: Event bus implementation choice (D-05)

### Options evaluated

| Option | Setup cost | Latency (L1 emit → L2 receive) | Backpressure | Multi-app delivery | Verdict |
|---|---|---|---|---|---|
| **Postgres LISTEN/NOTIFY via asyncpg** | already have Supabase DSN — add `asyncpg` dep (~3MB wheel) | ✅ < 100ms typical | ⚠️ NOTIFY drops if no listener at fire time (events lost during L2 downtime; need outbox table fallback) | ✅ both apps already point at same Supabase Postgres | ✅ **RECOMMENDED** |
| Supabase Realtime channel | supabase-py 2.10 has realtime — but realtime-py has historical asyncio integration friction | 100-500ms | ✅ better — buffers events server-side briefly | ✅ same Supabase project | ⚠️ second-choice — verify supabase-py realtime asyncio cleanliness |
| Redis Pub/Sub (Fly Redis or Upstash) | add 1 dep + Fly Redis $5/mo OR Upstash free 10k cmds/day | < 10ms | ⚠️ same as NOTIFY — no persistence | ✅ both apps connect | ❌ adds new vendor — out-of-CONTEXT scope |
| HTTP webhook (L1 POSTs to L2 /event) | zero deps | 50-200ms | ❌ L2 down = events lost without retry queue | ✅ trivial | ❌ requires L2 always-up; reverses dependency direction |

**Decision: `asyncpg` + Postgres LISTEN/NOTIFY** [CITED: pypi.org/project/asyncpg — 0.31.0 verified 2026-05-23].

**Why over Supabase realtime:** Supabase Free tier has 200 concurrent realtime connections + 2M messages/month limit. For 2 L2 daemon instances pinging "snapshot.complete" 2-4x/day, well within free tier — BUT realtime-py asyncio integration has reported flakiness across supabase-py versions (no specific issue verified — `[ASSUMED]` based on community reports). asyncpg is rock-solid and the project already pays for Postgres via Supabase.

**NOTIFY payload limit:** 8000 bytes (Postgres hard limit) — payload = `snapshot_id` (int) + `taken_at_ms` (int) only, well under.

### Persistence-aware design (NOTIFY drop mitigation)

NOTIFY fires once; if no listener at that moment, event is lost. Mitigation pattern:

```sql
-- Already exists: snapshots table from Alembic 001 (id, taken_at_ms, ...)
-- Add a "last_seen_snapshot_id" tracking table for L2.

CREATE TABLE l2_event_cursor (
  consumer TEXT PRIMARY KEY,  -- e.g. 'l2-candidate-refresh'
  last_snapshot_id INTEGER NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

L2 startup logic:
1. Read `l2_event_cursor.last_snapshot_id`
2. Query `SELECT id FROM snapshots WHERE id > last_snapshot_id ORDER BY id` (catch-up)
3. Start LISTEN; on NOTIFY, update cursor + refresh candidate set
4. NOTIFY drop → next LISTEN payload still triggers correct refresh because cursor is < current snapshot_id

### L1 emit-side wiring

L1 orchestrator step 7 (after SQLite commit, before 7.5 mirror) emits:

```python
# src/polyarb/storage/sqlite_store.py — write_snapshot_streaming returns snapshot_id
# After commit, in orchestrator.py step 7 tail:

if settings.event_bus_enabled:
    try:
        from polyarb.events.bus import publish_snapshot_complete
        await publish_snapshot_complete(settings, snapshot_id=snapshot_id, taken_at_ms=taken_at_ms)
    except Exception as e:
        logger.warning(f"event bus publish failed (fail-soft): {e!r}")
        sentry_sdk.add_breadcrumb(category="event-bus", level="warning",
                                  message=f"publish snapshot_complete failed: {snapshot_id}",
                                  data={"error": str(e)[:200]})
```

```python
# src/polyarb/events/bus.py
import asyncpg
import json

async def publish_snapshot_complete(settings, *, snapshot_id, taken_at_ms):
    """L1 → L2 cross-process NOTIFY on Postgres channel 'snapshot_complete'."""
    conn = await asyncpg.connect(dsn=settings.supabase_db_dsn.get_secret_value())
    try:
        payload = json.dumps({"snapshot_id": snapshot_id, "taken_at_ms": taken_at_ms})
        await conn.execute("SELECT pg_notify('snapshot_complete', $1)", payload)
    finally:
        await conn.close()
```

[CITED: magicstack.github.io/asyncpg/current/api/index.html — `pg_notify` usage]

### L2 receive-side wiring

```python
# src/polyarb/events/listener.py
import asyncpg
import asyncio
import json
from loguru import logger

async def listen_snapshot_complete(
    dsn: str,
    on_event: Callable,
    stop_event: asyncio.Event,
) -> None:
    """LISTEN on 'snapshot_complete'; invoke on_event(snapshot_id) per payload.

    Reconnect on connection loss — asyncpg doesn't auto-reconnect by default.
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

### Stretch (post-Phase-03): Redis Pub/Sub

When L3/L4 add more daemons that need richer event routing (e.g., signal events fanning to multiple consumers), revisit. Phase 03 NOTIFY is intentionally simple and reversible.

---

## Focus 4: Alembic 002 migration shape (D-07)

### 4 tables — schema

Schema follows Phase 02 P1 pattern (4-point lockstep) + RLS anon-SELECT (Plan 02-03 precedent).

```python
# alembic/versions/002_l2_orderbook_tables.py
"""L2 orderbook tracking tables

Revision ID: 002
Revises: 001  (or 002 after Plan 02-08 top_movers_view exists — check!)
Create Date: 2026-05-23

Phase 03 Plan 06 — D-07 L2 dashboard mirror.

Creates 4 tables for L2 candidate / top-of-book / trades / signals mirror.
Vercel dashboard reads via anon_key + RLS; service_role writes from polyarb-l2.

Schema add-only discipline (Phase 02 LEARNINGS P7): never DROP/RENAME/RETYPE
later — only ALTER ADD COLUMN.
"""
from __future__ import annotations
import sqlalchemy as sa
from alembic import op

revision = "002"
# IMPORTANT: confirm down_revision via `alembic history` — if Plan 02-08
# 002_add_top_movers_view.py already exists, change to "002" and rename this.
down_revision = "002"  # likely current head after Plan 02-08; verify
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── l2_candidates: candidate set membership history ────────────────────────
    # One row PER (snapshot_id, asset_id) admitted to the L2 tracking set.
    # Append-only — old rows kept for backtest reconstruction.
    op.create_table(
        "l2_candidates",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("snapshot_id", sa.Integer, sa.ForeignKey("snapshots.id", ondelete="SET NULL"), nullable=True),
        sa.Column("recipe_name", sa.String(64), nullable=False),  # scanner recipe OR 'watchlist'
        sa.Column("asset_id", sa.Text, nullable=False),  # =clob_token_id
        sa.Column("market_id", sa.Text),
        sa.Column("event_id", sa.Text),
        sa.Column("included_at_ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("removed_at_ts", sa.TIMESTAMP(timezone=True)),  # NULL = currently in candidate set
        sa.Column("ranking_score", sa.JSON),  # recipe-specific {liquidity, volume, spread, ...}
        sa.Column("source", sa.String(16), nullable=False),  # 'recipe' | 'watchlist'
    )
    op.create_index("idx_l2_candidates_asset", "l2_candidates", ["asset_id", "included_at_ts"])
    op.create_index("idx_l2_candidates_active", "l2_candidates", ["recipe_name", "removed_at_ts"])

    # ── l2_top_of_book: per-asset best-bid / best-ask time series ──────────────
    # Append-only; write at most every N seconds per asset (mirror.py debounce).
    # Time-series scan pattern: WHERE asset_id = ? AND ts BETWEEN ? AND ?
    op.create_table(
        "l2_top_of_book",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("best_bid", sa.Numeric(precision=10, scale=6)),  # 0.000001 — 1.000000
        sa.Column("best_ask", sa.Numeric(precision=10, scale=6)),
        sa.Column("spread", sa.Numeric(precision=10, scale=6)),  # ask - bid (computed write-time)
        sa.Column("mid_price", sa.Numeric(precision=10, scale=6)),
        sa.Column("depth_yes_usd", sa.Numeric(precision=14, scale=2)),  # bid-side liquidity at top
        sa.Column("depth_no_usd", sa.Numeric(precision=14, scale=2)),  # ask-side liquidity at top
        sa.Column("source_event", sa.String(24)),  # 'price_change' | 'best_bid_ask' | 'book'
    )
    op.create_index("idx_l2_tob_asset_ts", "l2_top_of_book", ["asset_id", "ts"])
    # BRIN index on ts for cheap time-range scans (thread §2.6 §B confirms 26M rows/year is BRIN-friendly)
    op.execute("CREATE INDEX idx_l2_tob_ts_brin ON l2_top_of_book USING BRIN (ts);")

    # ── l2_trades: per-asset trade execution time series ──────────────────────
    # WS last_trade_price + REST /trades backfill writes both go here.
    # trade_hash UNIQUE prevents duplicate insertions on backfill replay.
    op.create_table(
        "l2_trades",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("market_id", sa.Text),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("price", sa.Numeric(precision=10, scale=6), nullable=False),
        sa.Column("size", sa.Numeric(precision=14, scale=4), nullable=False),
        sa.Column("side", sa.String(4)),  # 'buy' | 'sell' | null
        sa.Column("taker_address", sa.Text),  # nullable; not always present
        sa.Column("trade_hash", sa.Text, unique=True),  # tx hash or proxy hash; UNIQUE for idempotent backfill
        sa.Column("source", sa.String(8), nullable=False),  # 'ws' | 'rest'
    )
    op.create_index("idx_l2_trades_asset_ts", "l2_trades", ["asset_id", "ts"])
    op.execute("CREATE INDEX idx_l2_trades_ts_brin ON l2_trades USING BRIN (ts);")

    # ── l2_signals: signal events (Phase 03 后期 wave 5+ 写入; reserve schema now) ─
    op.create_table(
        "l2_signals",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("asset_id", sa.Text, nullable=False),
        sa.Column("ts", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("signal_type", sa.String(48), nullable=False),
        sa.Column("severity", sa.String(8), nullable=False),  # 'info' | 'warn' | 'alert'
        sa.Column("payload", sa.JSON),
        sa.Column("acknowledged_by", sa.Text),  # set when ops marks resolved
        sa.Column("acknowledged_at", sa.TIMESTAMP(timezone=True)),
    )
    op.create_index("idx_l2_signals_unack", "l2_signals", ["acknowledged_at", "ts"])

    # ── Cursor table for asyncpg LISTEN/NOTIFY catch-up (Focus 3) ─────────────
    op.create_table(
        "l2_event_cursor",
        sa.Column("consumer", sa.String(64), primary_key=True),
        sa.Column("last_snapshot_id", sa.Integer, nullable=False),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("NOW()")),
    )

    # ── RLS policies (anon SELECT for dashboard) ──────────────────────────────
    for tbl in ("l2_candidates", "l2_top_of_book", "l2_trades", "l2_signals", "l2_event_cursor"):
        op.execute(f"ALTER TABLE {tbl} ENABLE ROW LEVEL SECURITY;")
        op.execute(f"CREATE POLICY anon_read ON {tbl} FOR SELECT USING (true);")
    # service_role bypasses RLS by default (Plan 02-03 precedent — no write policy needed).


def downgrade() -> None:
    for tbl in ("l2_event_cursor", "l2_signals", "l2_trades", "l2_top_of_book", "l2_candidates"):
        op.drop_table(tbl)
```

### Index strategy

- **btree on (asset_id, ts)**: primary lookup pattern is "give me last N events for this asset"
- **BRIN on ts alone**: cheap time-range scans across all assets (e.g., "all trades in last hour"). BRIN is right because rows arrive in roughly chronological order — typical small index size, fast scans.
- **No partitioning**: thread §2.6 §B confirms 26M rows/year is sub-100ms on PG16 with these indexes. Revisit at >1B rows or >100GB (years away).

### RLS reuse

Pattern identical to 001:
```sql
ALTER TABLE <tbl> ENABLE ROW LEVEL SECURITY;
CREATE POLICY anon_read ON <tbl> FOR SELECT USING (true);
-- service_role writes implicit (Supabase bypasses RLS for service_role)
```

### Schema-add discipline (Phase 02 L15 "adapt UI, don't grow schema")

Dashboard pages MUST read what migration 002 creates — UI cannot append columns. Reverse pressure (UI wants X) → propose Amendment migration 003 in a follow-up plan, NOT a sneaky ALTER in dashboard branch.

### Numeric precision rationale

`Numeric(10, 6)` for prices: Polymarket prices are 0.0001 — 0.9999 (binary outcome bounded). 6 decimal places preserves micro-cent diffs (sub-bp). PostgreSQL Numeric is exact — no float-rounding surprises in dashboard aggregations.

---

## Focus 5: Candidate refresh engine (D-04 + D-05)

### Reuse Phase 01.1 scanner

Existing assets (verified file:line via grep):
- `src/polyarb/observation/scanner.py:131` — `run_recipe(db_path, recipe) -> pd.DataFrame`
- `src/polyarb/observation/scanner.py:253` — `list_all_recipes(yaml_path) -> dict[str, Recipe]`
- `src/polyarb/observation/watchlist.py:173` — `load_watchlist(yaml_path) -> list[WatchlistEntry]`
- `src/polyarb/observation/recipes.py` — `BUILTIN_RECIPES` (6 patterns: thick-but-slippery, ghost-suspicious, etc.)

L2 candidate refresh = **union of**:
1. Multiple scanner recipes (each returns top-N markets by ranking criteria)
2. Watchlist YAML (user-pinned markets)

### Handler skeleton (~85 lines)

```python
# src/polyarb/observation/l2_candidate_refresh.py
"""L2 candidate-set refresh — triggered by snapshot.complete event.

Recipes return markets (each with yes_token_id + no_token_id); we project
to asset_ids (= clob_token_ids) and diff against the currently-tracked set.

D-04 union: scanner-recipes ∪ watchlist (manual override layer).
D-05 trigger: invoked from event listener (Focus 3) on each snapshot_complete.
"""
from __future__ import annotations
import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from loguru import logger
import pandas as pd

from polyarb.observation.scanner import list_all_recipes, run_recipe
from polyarb.observation.watchlist import load_watchlist
from polyarb.config import Settings


@dataclass(frozen=True)
class CandidateRow:
    """A single asset_id added to the L2 candidate set + its provenance."""
    asset_id: str
    market_id: str | None
    event_id: str | None
    recipe_name: str            # 'watchlist' if from watchlist YAML
    source: str                 # 'recipe' | 'watchlist'
    ranking_score: dict | None  # recipe-specific scoring fields


def compute_candidates(
    settings: Settings,
    scanner_yaml: Path | None = None,
    watchlist_yaml: Path | None = None,
) -> list[CandidateRow]:
    """Return the full candidate set for the *current* SQLite state.

    Pure function over (current SQLite snapshot, recipe definitions, watchlist
    YAML). Caller computes the diff vs previous tracked set.
    """
    db_path = Path(settings.db_path)
    out: dict[str, CandidateRow] = {}  # dedup by asset_id; later sources win

    # ── 1) Run all recipes (builtin + user yaml) ──────────────────────────
    recipes = list_all_recipes(scanner_yaml)
    for name, recipe in recipes.items():
        try:
            df: pd.DataFrame = run_recipe(db_path, recipe)
        except Exception as e:
            logger.warning(f"recipe {name!r} failed: {e!r}")
            continue
        for _, row in df.iterrows():
            for tid_col in ("yes_token_id", "no_token_id"):
                tid = row.get(tid_col)
                if not tid:
                    continue
                out[str(tid)] = CandidateRow(
                    asset_id=str(tid),
                    market_id=row.get("market_id"),
                    event_id=row.get("event_id"),
                    recipe_name=name,
                    source="recipe",
                    ranking_score={
                        "liquidity_usd": float(row.get("liquidity_usd") or 0),
                        "volume_usd": float(row.get("volume_usd") or 0),
                        "side": tid_col.split("_")[0],  # 'yes' | 'no'
                    },
                )

    # ── 2) Watchlist (override layer — last writer wins, source='watchlist') ──
    if watchlist_yaml is not None:
        # join to markets table for asset_ids by slug
        import sqlite3
        con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            entries = load_watchlist(watchlist_yaml)
            for entry in entries:
                row = con.execute(
                    "SELECT market_id, event_id, yes_token_id, no_token_id "
                    "FROM markets WHERE slug = ? "
                    "ORDER BY snapshot_id DESC LIMIT 1",
                    (entry.slug,),
                ).fetchone()
                if row is None:
                    logger.warning(f"watchlist {entry.slug!r}: not in DB")
                    continue
                market_id, event_id, yes_tid, no_tid = row
                for tid in (yes_tid, no_tid):
                    if tid:
                        out[str(tid)] = CandidateRow(
                            asset_id=str(tid),
                            market_id=market_id,
                            event_id=event_id,
                            recipe_name="watchlist",
                            source="watchlist",
                            ranking_score=None,
                        )
        finally:
            con.close()

    return list(out.values())


def diff_candidate_sets(
    old: Iterable[str],
    new: Iterable[CandidateRow],
) -> tuple[list[str], list[CandidateRow]]:
    """Compute (removed_asset_ids, added_rows) for WS subscribe/unsubscribe."""
    old_set = set(old)
    new_by_id = {c.asset_id: c for c in new}
    new_set = set(new_by_id)
    added = [new_by_id[a] for a in (new_set - old_set)]
    removed = sorted(old_set - new_set)
    return removed, added
```

### Wire-in to event listener

```python
# src/polyarb/daemon/l2_main.py — daemon entry
async def on_snapshot_complete(payload: dict):
    snapshot_id = payload["snapshot_id"]
    logger.info(f"snapshot_complete received: snapshot_id={snapshot_id}; refreshing candidates")
    new_candidates = compute_candidates(settings, scanner_yaml, watchlist_yaml)
    current_assets = [c.asset_id for c in app.state.tracked_candidates]
    removed_ids, added_rows = diff_candidate_sets(current_assets, new_candidates)

    if removed_ids:
        await ws_consumer.remove_subscriptions(removed_ids)
    if added_rows:
        await ws_consumer.add_subscriptions([r.asset_id for r in added_rows])

    # Persist new candidate-set to Supabase mirror
    await persist_candidates(new_candidates, snapshot_id)
    # Mark removed candidates with removed_at_ts in l2_candidates
    await mark_candidates_removed(removed_ids, snapshot_id)

    app.state.tracked_candidates = new_candidates
    logger.info(
        f"L2 candidate refresh: -{len(removed_ids)} +{len(added_rows)} "
        f"(total tracked={len(new_candidates)})"
    )
```

### Sanity caps

| Cap | Value | Why |
|---|---|---|
| max candidate-set size | 500 assets | Polymarket WS no hard limit but watchdog reasoning + memory budget. Phase 03 ROADMAP cited 10-100 markets → ≤500 assets (2× safety) |
| recipe LIMIT clause | already enforced 1-10000 by scanner.py:_validate_limit | unchanged |
| refresh debounce | min 60s between refreshes per snapshot_complete | NOTIFY storm protection (multiple snapshots in burst → 1 refresh) |

---

## Focus 6: Polymarket Data API /trades backfill (D-08)

### Endpoint facts

[CITED: docs.polymarket.com/api-reference/rate-limits + thread §2.2 Q5]

| Field | Value |
|---|---|
| URL | `https://data-api.polymarket.com/trades` |
| Auth | none (public) |
| Rate limit | 200 req / 10s |
| Pagination | `limit` max **500**, `offset` max **1000** [CITED: changelog 2025-08-26 "Updated /trades and /activity endpoints"] |
| Filters | `market=<condition_id>`, `user=<address>`, `takerOnly=true`, time-window via timestamp ordering |

### Pagination strategy for 7-day backfill

Offset≤1000 is the hard ceiling — going deeper requires **time-window sliding**. For ~26M rows/year ≈ 72k/day worst-case (whole market), per-asset 7-day trade count is usually 100s-1000s — single page often sufficient. Conservative skeleton:

```python
# src/polyarb/clients/data_api_client.py
"""Polymarket Data API /trades client — backfill 7 days of trade history.

D-08: WS-only accumulation begins from daemon-start; for the prior 7d window
we backfill via REST. closed-markets prices-history is degraded to 12h
granularity (issue #216) — for L2 we trust WS + this REST trade stream.
"""
from __future__ import annotations
import asyncio
import time
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator
import httpx
from loguru import logger
from aiolimiter import AsyncLimiter

DATA_API_BASE = "https://data-api.polymarket.com"

# Rate limit: 200 req / 10s, leave headroom — 150/10s
_LIMITER = AsyncLimiter(150, 10)


async def backfill_trades_for_asset(
    market_id: str,
    *,
    days: int = 7,
    page_size: int = 500,
) -> AsyncIterator[dict]:
    """Yield trade dicts for `market_id` over the last `days` days.

    Strategy:
      1. Request offset=0 with limit=500
      2. If 500 rows returned, increment offset (max 1000)
      3. If offset would exceed 1000 OR rows < 500, switch to time-window slide:
         use the OLDEST trade ts seen as new upper bound, restart from offset=0
      4. Stop when oldest seen trade < (now - days)
    """
    cutoff = datetime.now(tz=timezone.utc) - timedelta(days=days)
    cutoff_ts = cutoff.timestamp()
    upper_bound_ts: float | None = None  # exclusive upper bound for time-slide
    seen_trade_hashes: set[str] = set()

    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            for offset in range(0, 1001, page_size):  # offset 0, 500, 1000
                params = {"market": market_id, "limit": page_size, "offset": offset}
                if upper_bound_ts is not None:
                    # Use a Polymarket time filter — actual param name TBD,
                    # likely 'before' or via taker_address etc. CHECK DOCS.
                    params["beforeTimestamp"] = int(upper_bound_ts)  # [ASSUMED] param name
                async with _LIMITER:
                    resp = await client.get(f"{DATA_API_BASE}/trades", params=params)
                if resp.status_code == 429:
                    logger.warning(f"rate limited; sleeping 10s")
                    await asyncio.sleep(10)
                    continue
                resp.raise_for_status()
                trades = resp.json()
                if not trades:
                    return  # exhausted
                for t in trades:
                    h = t.get("transactionHash") or t.get("id")
                    if h in seen_trade_hashes:
                        continue
                    seen_trade_hashes.add(h)
                    yield t
                # Update upper_bound for time-slide
                oldest = min(float(t["timestamp"]) for t in trades)
                if oldest < cutoff_ts:
                    return  # passed cutoff
                upper_bound_ts = oldest
                if len(trades) < page_size:
                    break  # natural end of this page
            else:
                # offset exhausted (=1000 ceiling); next outer-loop iter time-slides
                pass
```

[ASSUMED] The exact filter param name for "older than timestamp" (`beforeTimestamp`, `endDate`, etc.) — docs.polymarket.com/api-reference/data-api/trades schema must be re-fetched at plan time. Phase 1.5 历史踩过 `filterDate` 不存在的坑 (STATE.md L46 references this revert). **Plan-phase MUST verify** before implementation.

### Per-asset backfill orchestration

```python
async def backfill_all_candidates(candidates: list[CandidateRow], settings) -> int:
    """Backfill 7d trades for every candidate market, writing to l2_trades.

    Per-market parallelism = 4 (rate-limit aware via _LIMITER).
    """
    sem = asyncio.Semaphore(4)
    total_written = 0

    async def _one(c: CandidateRow):
        nonlocal total_written
        async with sem:
            try:
                async for trade in backfill_trades_for_asset(c.market_id):
                    await write_l2_trade(trade, asset_id=c.asset_id, source="rest")
                    total_written += 1
            except Exception as e:
                logger.warning(f"backfill {c.market_id} failed: {e!r}")

    await asyncio.gather(*[_one(c) for c in candidates], return_exceptions=True)
    return total_written
```

### Idempotency

`l2_trades.trade_hash UNIQUE` constraint (from Focus 4 migration) makes backfill safe to re-run. Use upsert (`ON CONFLICT (trade_hash) DO NOTHING`).

### Rate budget calculation

200 req/10s = 20 req/s. With 4 concurrent backfill workers × ~3 req/market × 100 markets = 1200 req ≈ 60s real time. Well under limit; leaves headroom for other API calls.

---

## Focus 7: polyarb-l2 Fly deployment (D-06)

### What to copy/diff from L1 stack

| File | L1 location | L2 differences |
|---|---|---|
| `fly.toml` → `fly-l2.toml` | repo root | `app="polyarb-l2"`; remove `[processes].cron` line; remove `[[vm]].processes=["cron"]` block; keep `ams` region; reduce vm memory to `512mb` (no CLOB cache, just WS stream) |
| `Dockerfile` | repo root, reused 1:1 | None — same image, different CMD via fly-l2.toml |
| `crontab` | repo root | NOT used by L2 (no Supercronic process group) |
| `.github/workflows/deploy.yml` → `deploy-l2.yml` | `.github/workflows/` | name=Deploy L2; APP=polyarb-l2; same flyctl actions |
| Daemon entry | `src/polyarb/daemon/main.py` | new `src/polyarb/daemon/l2_main.py` — different scheduler (no cron, instead WS + event-listener) |

### fly-l2.toml (complete, ready to drop)

```toml
# Phase 03 Fly.io config — polyarb-l2 (L2 orderbook tracking daemon)
# Differs from polyarb-l1: NO cron process, single WS-driven asyncio loop
app = "polyarb-l2"
primary_region = "ams"

[build]

[mounts]
  source = "polyarb_l2_data"
  destination = "/data"
  initial_size = "1gb"  # smaller — no parquet archive, just SQLite for state

[env]
  POLYARB_DATA_DIR = "/data"
  POLYARB_DB_PATH = "/data/l2-state.db"
  POLYARB_ALLOW_EXTERNAL_PATHS = "1"
  POLYARB_DAEMON_VARIANT = "l2"  # selects l2_main.py entry

# Single process group — long-running WS + event-listener + HTTP
[processes]
  app = "python -m polyarb.daemon.l2_main"

# HTTP service — /health (IETF strict) + /healthz (Fly probe) inherited from L1 pattern
[http_service]
  internal_port = 8080
  force_https = true
  auto_stop_machines = "off"
  auto_start_machines = true
  min_machines_running = 1
  processes = ["app"]

  [[http_service.checks]]
    grace_period = "120s"
    interval = "30s"
    method = "GET"
    path = "/healthz"  # Phase 02.1 D-05/D-06 — always 200 to keep Fly proxy happy
    timeout = "10s"

# Memory budget — WS stream + 4 candidate-mirror table writes
# Initial estimate 256MB; if WS message backlog grows, scale up post-profile (L4 caveat)
[[vm]]
  size = "shared-cpu-1x"
  memory = "512mb"  # 2x headroom for initial run
  processes = ["app"]
```

### Dockerfile reuse

The L1 Dockerfile builds `/app` with `src/` mounted. The CMD is overridden by `fly-l2.toml`'s `[processes].app`. ZERO Dockerfile changes.

**Single binary, two deployments** — exactly the pattern Fly recommends for multi-app projects sharing code.

### .github/workflows/deploy-l2.yml

```yaml
name: Deploy L2 to Fly

on:
  push:
    branches: [main]
    paths:
      - 'src/polyarb/daemon/l2_main.py'
      - 'src/polyarb/clients/ws_market_client.py'
      - 'src/polyarb/observation/l2_candidate_refresh.py'
      - 'src/polyarb/events/**'
      - 'fly-l2.toml'
      - 'Dockerfile'
  workflow_dispatch: {}

concurrency:
  group: deploy-l2-prod
  cancel-in-progress: false

jobs:
  deploy:
    name: flyctl deploy --config fly-l2.toml
    runs-on: ubuntu-latest
    timeout-minutes: 15
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
      - uses: superfly/flyctl-actions/setup-flyctl@1.6  # pinned per L8 lesson
      - name: Deploy
        run: flyctl deploy --config fly-l2.toml --remote-only --wait-timeout 600
        env:
          FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}
      - name: Smoke /healthz
        env:
          APP: polyarb-l2
        run: |
          for i in $(seq 1 10); do
            if curl -fsS "https://${APP}.fly.dev/healthz" >/dev/null; then
              echo "L2 deploy healthy"; exit 0
            fi
            sleep 6
          done
          flyctl logs --app "${APP}" --no-tail | tail -100
          exit 1
```

### Secret propagation

Both polyarb-l1 and polyarb-l2 need: Sentry DSN, Telegram bot token+chat_id, Supabase URL+DB_DSN+service_key, R2 keys (l2 doesn't need R2 for now but reserve).

**Pattern (one-time setup commands):**
```bash
# Get list of L1 secrets first
flyctl secrets list -a polyarb-l1

# Replicate to L2 (manually for each)
flyctl secrets set -a polyarb-l2 \
  POLYARB_SENTRY_DSN="$(flyctl ssh console -a polyarb-l1 -C 'env' | grep SENTRY_DSN | cut -d= -f2-)" \
  POLYARB_SUPABASE_URL="..." \
  POLYARB_SUPABASE_DB_DSN="..." \
  POLYARB_SUPABASE_SERVICE_KEY="..." \
  POLYARB_TELEGRAM_BOT_TOKEN="..." \
  POLYARB_TELEGRAM_CHAT_ID="..." \
  POLYARB_SCAN_SHARED_SECRET="..."  # for /control/* HMAC if L2 grows ops endpoints
```

**Better pattern (Plan 02 task):** create `scripts/fly_secrets_sync.sh` that reads `.env` and pushes to BOTH apps in one shot. Avoids drift between L1 and L2 over time.

### Sentry environment differentiation

`init_sentry` (src/polyarb/observability/sentry.py:103) auto-detects environment via `settings.release_id`. For L2, set `serviceId` in `/healthz` body to `polyarb-l2` (vs L1's `polyarb-l1`) so Sentry can split events per service tag.

Recommended Sentry init augmentation:
```python
# In l2_main.py before init_sentry
import os
os.environ.setdefault("POLYARB_SERVICE_ID", "polyarb-l2")
init_sentry(settings)
sentry_sdk.set_tag("service", "polyarb-l2")  # explicit tag for filter
```

### Cost projection

| Resource | Cost |
|---|---|
| polyarb-l2 Fly VM 512MB | $3.89/mo |
| Fly volume 1GB | $0.15/mo |
| Supabase Pro (shared with L1) | $0 marginal — already paid |
| Sentry / Telegram / Better Stack | $0 marginal |
| **Total marginal cost for L2** | **~$4/mo** |

Combined L1+L2 cost: ~$11/mo Fly + $25/mo Supabase Pro = **$36/mo**.

---

## Plan Task Recommendations

Suggested 6-7 plans across 4 waves. Rationale: parallel where possible (Phase 02.1 paid back this discipline), serialize on cross-bug dependencies (L2 Phase 02.1 lesson).

| # | Plan name | Wave | Parallel with | Dependencies | Estimated effort |
|---|---|---|---|---|---|
| **01** | GHA Supabase keepalive workflow + Better Stack heartbeat config | 1 (autonomous) | 02 | none — purely YAML | 1-2h |
| **02** | polyarb-l2 Fly app bootstrap (fly-l2.toml + deploy-l2.yml + Dockerfile reuse + secret sync script) | 1 (autonomous) | 01 | none — config only | 2-3h |
| **03** | L2 daemon entry (l2_main.py + Starlette app with /health + /healthz) | 2 | none | 02 | 3-4h |
| **04** | WS client + market channel + staleness watchdog (D-02 + D-03) | 2 | 05 | 03 | 5-6h |
| **05** | Event bus (asyncpg LISTEN/NOTIFY) + candidate refresh engine (D-04 + D-05) | 2 | 04 | 03 + Alembic 002 (sequence Wave 2→2.5) | 4-5h |
| **06** | Alembic 002 migration + Supabase 4-table mirror + REST /trades backfill (D-07 + D-08) | 2.5 | none | 05 (cursor table dep) | 4-5h |
| **07** | Chaos verification (Inj L2-1..5) + 03-SOAK-LOG.md | 3 (checkpoint) | none | 04+05+06 all live in prod | 4-6h |
| **08** | Phase closure — docs/learning/10-L2-tracking.md + 03-VALIDATION flip + Vercel dashboard 4 pages | 3 (checkpoint, optional split) | none | 07 PASS | 3-4h |

### Wave dependencies (mermaid)

```
Wave 1 (parallel autonomous):
  Plan 01 (GHA keepalive) ──┐
  Plan 02 (Fly L2 bootstrap)┴── prerequisites ready

Wave 2 (parallel within wave):
  Plan 03 (daemon entry) ── must precede 04+05
  Plan 04 (WS + watchdog) ──┐
  Plan 05 (event bus + refresh)┴── share daemon state

Wave 2.5 (sequential — schema first):
  Plan 06 (Alembic 002 + mirror + backfill)

Wave 3 (checkpoint):
  Plan 07 (chaos) → user reviews live verdict
  Plan 08 (closure) → unblocks Phase 04
```

### Cross-bug pre-check (Phase 02.1 L2 application)

| Possible interaction | Identified? | Mitigation |
|---|---|---|
| Plan 04 watchdog reconnect storm + Plan 05 NOTIFY storm hitting same candidate refresh | ⚠ | Debounce candidate refresh ≥60s; Plan 05 must include rate-limit test |
| Plan 06 mirror writing while Plan 04 WS feeds events to same SQL pool | ⚠ | Use separate asyncpg pool for writes vs notifies; Plan 06 must spec pool sizing |
| GHA keepalive (Plan 01) failing silently while Plan 06 mirror still works | ⚠ | Better Stack heartbeat on GHA monitor MUST be configured Plan 01 — not later |
| L1 emit + L2 listen both target same Postgres DSN simultaneously during chaos | ⚠ | Plan 05 must include Inj-L2-3 (Postgres connection limit reached scenario) |

---

## Open Questions for Planner

1. **WS event throughput at 500-asset scale** — docs.polymarket.com doesn't quantify events/sec for `best_bid_ask` + `price_change`. **Recommendation:** Plan 04 add a 1-hour 10-asset smoke run to measure baseline; if >100 events/s, debounce writes to `l2_top_of_book` (write per-asset at most every 5s).

2. **Data API /trades filter param name** — `beforeTimestamp` is `[ASSUMED]`. **Recommendation:** Plan 06 Wave 0 test issues a real request to docs example endpoint to confirm exact param name before writing backfill code.

3. **WS `operation` field for dynamic subscribe** — schema confirmed by docs prose (changelog 2025-09-24) but exact JSON payload only implied. **Recommendation:** Plan 04 RED test mocks an actual capture from a manual WS session if possible (use `wscat -c wss://ws-subscriptions-clob.polymarket.com/ws/market` interactive sanity).

4. **Vercel dashboard scope for Phase 03** — D-07 says "4 new pages" but Plan 08 only lightly mentioned. **Recommendation:** Plan 08 explicit task list — at minimum a `/candidates`, `/asset/[id]/tob`, `/asset/[id]/trades`, `/signals` page; if scope too big, defer to Phase 04.

5. **GHA keepalive failure detection** — Better Stack heartbeat at 24h tolerance means up to ~25h before alert fires. **Recommendation:** Plan 01 also write a daily monitoring job that checks GHA workflow last_success_at < 28h via `gh api` from a separate alert pathway (Sentry breadcrumb in L1 daemon).

6. **L1 `event_bus_enabled` flag default** — should L1 publish snapshot_complete from the next deploy, or behind a feature flag? **Recommendation:** Default `enabled=True` once Plan 05 lands; gate via `POLYARB_EVENT_BUS_ENABLED=1` Fly secret. Allows Plan 04/05 to land + smoke-test in L2 before L1 fan-out goes prod.

7. **Watchdog stale_s sensitivity in low-traffic candidates** — 30s might trip on extremely illiquid markets (no trades for hours, but `best_bid_ask` should still tick on minor moves). **Recommendation:** Plan 04 Wave 0 includes a "low-traffic asset" fixture test to confirm watchdog doesn't false-positive when book is stable but connection healthy. If false-positive observed, lengthen stale_s to 60s (still well under typical silent-freeze duration in #292 reports).

8. **Candidate set persistence semantics** — when a candidate is removed (`removed_at_ts`), do we keep the WS subscription for N more minutes to capture trail-off trades, or hard-cut? **Recommendation:** Hard-cut at refresh time; rely on `l2_trades` table already-accumulated rows. Simpler semantics, less state.

9. **Phase 02.2 backlog truth-2 fix-up** — currently uncoupled, but if Plan 05 introduces L2 mirror with "config-disabled" branch, apply Phase 02.2's modification (mirror success branch ALSO emits `category=mirror` breadcrumb) preemptively. **Recommendation:** Plan 06 implementation includes both branches.

10. **`websockets` library version pin** — `>=16.0,<17` recommended. 16.0 released 2026-01-10 [VERIFIED PyPI]. There's a Python 3.13 compatibility note in changelog — verify Python 3.12 compat (project pins 3.12) via test before pinning version.

---

## Risk Register

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | WS silent-freeze (#292) not caught by 30s watchdog (e.g., partial event stream where some assets tick but candidate of interest is stalled) | MEDIUM | HIGH (L2 dataset corruption) | Watchdog stale_s applied per-asset OR per-connection? Phase 03 chooses per-connection (simpler); if R1 manifests in soak, move to per-asset in Phase 04 |
| R2 | Postgres NOTIFY dropped when L2 daemon restarting → missed snapshot.complete events | HIGH | MEDIUM (1 refresh cycle missed = 6-12h stale candidates) | `l2_event_cursor` catch-up logic on startup is the prescribed fix (Focus 3); test in Plan 05 Wave 0 |
| R3 | Polymarket WS endpoint changes URL or schema (announcement may be subtle) | LOW | HIGH (silent data outage) | Sentry alert on parse failures + weekly checksum of `docs.polymarket.com/changelog` content (could be a separate GHA monitoring job) |
| R4 | Supabase Free 4-day idle pause during phase 03 dev (when no GHA keepalive yet — Plan 01 lands first) | MEDIUM (dev burst pattern) | LOW (recoverable manually) | Plan 01 MUST be Wave 1, landed before any L2 daemon code |
| R5 | Watchdog reconnect storm → Polymarket WS endpoint rate-limit / abuse detection bans IP | LOW | HIGH (multi-day cooling-off) | Exp backoff cap 30s, max reconnect attempts per minute (add hard cap in WatchdogState — e.g., 10 reconnects/hour → fall back to REST polling) |
| R6 | Data API /trades 7-day backfill rate-limit during initial 100-asset rollout | MEDIUM | MEDIUM (delayed historical data, not data loss) | Conservative limiter (150 req/10s, headroom 25%); persistent checkpoint file under `/data/l2-backfill-cursor.json` so partial completion resumes |
| R7 | L2 daemon OOM via accumulated WS message buffer (similar to Phase 02 D-23 OOM) | MEDIUM | HIGH (crashloop) | Bounded `asyncio.Queue(maxsize=10000)` between WS receive and mirror write; drop OLDEST on full + emit warning Sentry breadcrumb |
| R8 | L1 + L2 both writing to Supabase same time → connection limit exceeded | LOW | MEDIUM (write failures = fail-soft, but visible) | Supabase Pro pgbouncer pooler (default 90 connections shared) is plenty for 2 daemons; document in plan that L2 should use pgbouncer connection (`...supabase.co:6543` port, not 5432) |
| R9 | initial_dump=true on every reconnect floods inbound bandwidth/parse CPU for large candidate sets (500 assets × ~50 orderbook levels) | LOW | MEDIUM (slow recovery from reconnect, missed events) | Cap candidate set at 200 in initial deploy; profile peak WS receive throughput in Plan 04 smoke |
| R10 | Schema drift between L1 alembic 001 and L2 alembic 002 — wrong `down_revision` causing migration replay errors | LOW (caught by Wave 0 test) | LOW (just a debug session) | Wave 0 test_l2_supabase_mirror.py runs `alembic upgrade head` from clean DB and asserts all tables exist (already in plan) |

---

## Project Constraints (from CLAUDE.md)

- **Chinese for `docs/` writing** — `docs/learning/10-L2-tracking.md` MUST be Chinese
- **Makefile entry points** — every new command must have a `make` target; Phase 03 candidates:
  - `make daemon-l2-run-local` (local L2 daemon dev mode)
  - `make smoke-l2-health` (curl /healthz)
  - `make smoke-l2-ws` (run 30s WS test against a known liquid asset)
  - `make migrate-l2` (alembic upgrade head — same as L1 but reminder for L2-touching plans)
  - `make backfill-trades` (manual REST backfill trigger for specific asset)
- **uv package manager** — all new deps via `uv add asyncpg` / `uv add 'websockets>=16,<17'`, never direct pip
- **No `--no-verify` commits** — pre-commit SUMMARY hook is non-negotiable; every plan SHA must have an accompanying SUMMARY.md committed in the same logical unit
- **Plan-end SUMMARY discipline** — Each `03-{NN}-PLAN.md` must produce `03-{NN}-SUMMARY.md` per pre-commit hook gate
- **Verification ownership** (Phase 02.1 D-09) — Claude self-verifies via Sentry API / flyctl ssh / curl; never delegate UI翻找 to user
- **No --no-verify, no rewrite history, no force-push to main** (Git Safety Protocol)

---

## Source Hierarchy

### Primary (HIGH confidence — Context7 / official docs / verified packages)

- [CITED: docs.polymarket.com/market-data/websocket/overview, fetched 2026-05-23] — WS URL `wss://ws-subscriptions-clob.polymarket.com/ws/market`, field `assets_ids`, 10s PING requirement
- [CITED: docs.polymarket.com/changelog, fetched 2026-05-23] — 2025-05-28 `initial_dump` field added + 100-token limit removed; 2025-09-24 dynamic subscribe/unsubscribe; 2025-08-26 /trades limit=500/offset=1000
- [CITED: docs.polymarket.com/api-reference/rate-limits, fetched 2026-05-23 (via thread §2.2 Q4)] — Data API /trades 200/10s, CLOB rates
- [CITED: pypi.org/project/websockets, verified 2026-05-23 via `pip index versions`] — 16.0 published 2026-01-10
- [CITED: pypi.org/project/asyncpg, verified 2026-05-23 via `pip index versions`] — 0.31.0 latest
- [CITED: websockets.readthedocs.io/en/stable/reference/asyncio/client.html] — `connect()` as async iterator for auto-reconnect
- [VERIFIED: codebase `src/polyarb/observation/scanner.py`, `watchlist.py`, `recipes.py`] — scanner engine + recipes + watchlist patterns to reuse in Focus 5

### Secondary (MEDIUM confidence — cross-verified facts)

- [CITED: github.com/Polymarket/py-clob-client issue #292, thread §2.2 Q4] — silent-freeze bug 2026-03 still open
- [CITED: github.com/Polymarket/py-clob-client issue #216, thread §2.2 Q5] — prices-history degrades for closed markets to 12h
- [CITED: magicstack.github.io/asyncpg/current/] — LISTEN/NOTIFY via `add_listener` API
- [CITED: Phase 02 LEARNINGS L6/L7/L8 + Phase 02.1 LEARNINGS L1-L8] — alert chain + cross-bug + container-localhost patterns

### Tertiary (LOW confidence — `[ASSUMED]`, plan-phase must verify)

- [ASSUMED] Exact `operation` field schema for dynamic subscribe — verify in Plan 04 RED test
- [ASSUMED] Data API /trades filter param name `beforeTimestamp` — verify in Plan 06 Wave 0
- [ASSUMED] Supabase realtime-py asyncio integration friction — basis for asyncpg recommendation; if Plan 05 deems supabase-py realtime usable, switch is fine
- [ASSUMED] WS event throughput at 500 candidates — measure in Plan 04 smoke

---

## Assumptions Log

| # | Claim | Section | Risk if Wrong |
|---|---|---|---|
| A1 | WS dynamic subscribe uses `operation` field name | Focus 1 (subscribe payload) | Plan 04 RED test reveals real schema; minor refactor |
| A2 | Data API /trades supports `beforeTimestamp` filter (or equivalent) | Focus 6 | Backfill rewrites — may need to slide via offset+limit only, larger time-window granularity |
| A3 | `supabase-py` realtime channels have asyncio integration issues | Focus 3 | Recommendation defaults to asyncpg; if supabase-py works fine, no harm done |
| A4 | 30s watchdog stale_s doesn't false-positive on low-liquidity markets | Focus 2 | Plan 04 Wave 0 fixture catches it; tunable parameter |
| A5 | `websockets>=16.0` works on Python 3.12 | Focus 1 | Plan 02 Wave 0 import test catches incompatibility |
| A6 | Polymarket WS endpoint URL unchanged through Phase 03 execution | Focus 1 | Sentry alert on parse failures + R3 mitigation |
| A7 | Supabase Free → Pro upgrade not needed for Phase 03 (D-01 explicit) | Top-level | If Pro forced earlier, Plan 01 can be replaced with the upgrade |
| A8 | NOTIFY 8000-byte limit accommodates `{snapshot_id, taken_at_ms}` payload | Focus 3 | Hard limit physical; we send ~50 bytes — safe |
| A9 | Per-asset trade volume in 7 days fits offset≤1000 for most candidates | Focus 6 | Time-slide fallback covers extreme cases (R6) |
| A10 | `flyctl ssh console -C "cmd"` works for chaos verification fallback | Validation Architecture | Phase 02.1 L8 confirmed for L1; same on L2 unless Fly proxy ACL changes |

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|---|---|---|---|---|
| Python 3.12 | All | ✓ | 3.12 (pyproject.toml `.python-version`) | — |
| uv package manager | Build/dep | ✓ | 0.5+ (Dockerfile pinned 0.5.0) | — |
| `websockets` 16.0 | Focus 1 WS client | ✓ on PyPI | 16.0 (2026-01-10) | Pin `>=15.0,<17` if 16.0 has 3.12 friction |
| `asyncpg` 0.31.0 | Focus 3 event bus | ✓ on PyPI | 0.31.0 | — |
| `supabase-py` 2.10 | Focus 4 mirror writes | ✓ in dep | (already in pyproject) | — |
| `httpx[http2]` | Focus 6 REST backfill | ✓ in dep | 0.27 | — |
| Fly.io API + `flyctl` 1.6 | Focus 7 deploy | ✓ working (L1 in prod) | latest | — |
| Supabase Pro | D-01 says NO, stay Free + GHA keepalive | ✓ Free tier active | — | If pause incidents recur, upgrade Pro $25/mo |
| Sentry EU region | observability | ✓ in prod (L1 uses it) | — | — |
| Better Stack | heartbeat for GHA keepalive | ✓ free 10 monitors | — | — |
| Telegram bot | alerts | ✓ in prod (L1 uses it) | — | — |
| GitHub Actions cron | Plan 01 keepalive | ✓ active (CI/Deploy workflows in use) | — | — |

**No blocking dependencies missing.** Every external requirement either reuses Phase 02 stack or is a new PyPI dep installed via `uv add`.

---

## Architecture Patterns

### System Architecture Diagram (data flow)

```
                                        Polymarket WS
                                        wss://ws-subscriptions-clob…/ws/market
                                                ▲
                                                │ subscribe {assets_ids:[...], initial_dump:true}
                                                │ events: price_change | best_bid_ask | last_trade_price | book
                                                ▼
┌────────────────────────────────────┐                                  ┌──────────────────────┐
│  polyarb-l1 (existing — ams)       │                                  │  polyarb-l2 (new — ams)│
│                                    │                                  │                       │
│  cron: snapshot every 6/12h        │                                  │  WsConsumer ──┐       │
│  ↓                                 │                                  │  WsWatchdog 30s│       │
│  Gamma+CLOB snapshot               │                                  │  ↓ events     │       │
│  ↓                                 │                                  │  candidate_refresh    │
│  SQLite (truth) + Parquet (R2)     │                                  │  ↓                    │
│  ↓                                 │                                  │  l2 mirror writer ──┐ │
│  Supabase mirror (markets_latest)  │                                  │                     │ │
│  ↓                                 │                                  │                     │ │
│  pg_notify('snapshot_complete')────┼──────► Postgres LISTEN ◄──────────┤                     │ │
│                                    │   (Supabase Pro / asyncpg)       │  on_snapshot_complete│ │
│                                    │                                  │  ↓                  │ │
│                                    │                                  │  diff(old, new)    │ │
│                                    │                                  │  ↓                  │ │
│                                    │                                  │  ws.subscribe(+)/un│ │
└────────────────────────────────────┘                                  │  ────────────────── │
                                                                        │                     │
                                                                        │  REST backfill ─────┘
                                                                        │  /trades 7d ▶│
                                                                        │              │
                                                                        ▼              ▼
                                                          ┌──────────────────────────────────┐
                                                          │  Supabase Postgres (shared)        │
                                                          │  ┌──────────────────────────────┐  │
                                                          │  │ Alembic 001: snapshots,      │  │
                                                          │  │   markets_latest, recipe_runs│  │
                                                          │  │ Alembic 002: l2_candidates,  │  │
                                                          │  │   l2_top_of_book,            │  │
                                                          │  │   l2_trades, l2_signals,     │  │
                                                          │  │   l2_event_cursor            │  │
                                                          │  └──────────────────────────────┘  │
                                                          └──────────────────────────────────┘
                                                                          ▲
                                                                          │ anon SELECT (RLS)
                                                                          │
                                                          ┌──────────────────────────────────┐
                                                          │  Vercel dashboard (existing + 4)  │
                                                          │  /candidates, /asset/[id]/tob,    │
                                                          │  /asset/[id]/trades, /signals     │
                                                          └──────────────────────────────────┘

                              ┌─────────────────────────────────────────┐
                              │  Cross-cutting (existing)                │
                              │  Sentry EU (de.sentry.io) ◄── 2 daemons │
                              │  Telegram alerts ◄── alerts.py          │
                              │  Better Stack heartbeat for GHA cron    │
                              │  Better Stack heartbeat for /health     │
                              └─────────────────────────────────────────┘

                              ┌─────────────────────────────────────────┐
                              │  GHA Supabase keepalive (Plan 01)        │
                              │  daily wget Supabase REST → Better Stack │
                              │  heartbeat (24h tolerance)               │
                              └─────────────────────────────────────────┘
```

### Component responsibilities

| Component | File | Responsibility |
|---|---|---|
| `WsConsumer` | `src/polyarb/clients/ws_market_client.py` (new) | Single WS connection, subscribe/unsubscribe, parse frames, yield events |
| `WsWatchdog` | `src/polyarb/daemon/ws_watchdog.py` (new) | 30s staleness detection, trigger reconnect, exp backoff |
| `compute_candidates` | `src/polyarb/observation/l2_candidate_refresh.py` (new) | Pure function: recipes ∪ watchlist → list[CandidateRow] |
| `diff_candidate_sets` | same file | Compute (removed, added) for WS sub/unsub |
| `publish_snapshot_complete` | `src/polyarb/events/bus.py` (new) | asyncpg pg_notify('snapshot_complete', payload) — L1 side |
| `listen_snapshot_complete` | `src/polyarb/events/listener.py` (new) | asyncpg LISTEN with reconnect — L2 side |
| `L2SupabaseMirror` | `src/polyarb/storage/l2_supabase_mirror.py` (new) | Batch upsert to l2_candidates/l2_top_of_book/l2_trades, fail-soft |
| `backfill_trades_for_asset` | `src/polyarb/clients/data_api_client.py` (new) | REST /trades 7d backfill with pagination + rate limit |
| `l2_main` | `src/polyarb/daemon/l2_main.py` (new) | Entry: init_logging → init_sentry → SQLite store → WsConsumer + listener + HTTP app via asyncio.gather |
| `/healthz` + `/health` | `src/polyarb/http/health.py` (extend) | Already exists, reuse `_build_health_checks` helper from Phase 02.1 P5; ADD `ws:last_event_age_seconds` check for L2 |

### Recommended project structure (additions only)

```
src/polyarb/
├── clients/
│   ├── ws_market_client.py    (NEW — Focus 1)
│   └── data_api_client.py     (NEW — Focus 6)
├── daemon/
│   ├── l2_main.py             (NEW — Focus 7 entry)
│   └── ws_watchdog.py         (NEW — Focus 2)
├── events/
│   ├── __init__.py            (NEW)
│   ├── bus.py                 (NEW — Focus 3 publisher)
│   └── listener.py            (NEW — Focus 3 listener)
├── observation/
│   └── l2_candidate_refresh.py (NEW — Focus 5)
└── storage/
    └── l2_supabase_mirror.py   (NEW — Focus 4 writes)

alembic/versions/
└── 003_l2_orderbook_tables.py  (NEW — Focus 4, NUMBER 003 not 002 — verify with `alembic history`!)

.github/workflows/
├── deploy-l2.yml               (NEW — Focus 7)
└── supabase-keepalive.yml      (NEW — D-01)

repo root:
└── fly-l2.toml                 (NEW — Focus 7)
```

### Anti-patterns to avoid

- **Don't unite L1 + L2 into one daemon** — D-06 locked, but the temptation will be there. CLOB cron + WS long-loop in one process group = OOM nightmare (L4 + L3 Phase 02 lessons combined).
- **Don't share SQLite between L1 and L2** — L2 has its own `/data/l2-state.db` for local state (e.g., WS reconnect cursor); L1 owns `state.db`. Cross-mounted volumes between Fly apps = data races.
- **Don't fall into Phase 02.1 L7 "verified via test button"** — every truth in 03-VALIDATION.md must specify the programmatic command (this RESEARCH's Validation Architecture table is the model).
- **Don't write to Supabase from event_listener callback** — keep `on_event` thin (queue.put or in-memory state update); actual writes go to a separate batched writer task to avoid blocking the LISTEN socket.
- **Don't trust `ws.recv()` to surface silent-freeze** — TCP keepalive cannot distinguish "alive but starved" from "dead". Watchdog is the only authority on liveness.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---|---|---|---|
| WS protocol + framing | custom websocket-over-asyncio | `websockets` 16.0 | RFC 6455 compliance, fragmentation handling, masking, ping/pong, TLS — all subtle |
| WS auto-reconnect on `ConnectionClosed` | manual try/except + while-True | `async for ws in connect(...)` builtin | Library handles exception classification + retry-on-network-error |
| Postgres LISTEN/NOTIFY connection mgmt | psycopg2 sync + threading | `asyncpg.add_listener` | Native asyncio integration, no thread-pool dance |
| 7-day trade pagination across offset≤1000 boundary | offset-only loop | Hybrid offset + time-slide pattern (Focus 6 skeleton) | Polymarket's 2025-08-26 offset cap means offset-only loops silently truncate |
| HMAC for L2 /control endpoints | custom HMAC code | Reuse `ScanAuthMiddleware` pattern (Phase 02 P11) | constant-time compare, proven shape, no reinvention |
| Health endpoint with multi-check | hand-rolled JSON | IETF draft-inadarei + reuse `_build_health_checks` from Phase 02.1 P5 | Already battle-tested with Better Stack + Fly proxy |
| Sentry breadcrumb dedup | per-emit dedup logic | `_LAST_ALERT_TIME_MS` pattern from Phase 02 P6 | Single source of truth for "this situation already emitted in window N" |
| Alembic schema diff for L2 vs L1 | side-by-side review | `alembic upgrade head` + `--sql` flag in Wave 0 test | Forces both branches converge |

**Key insight:** The Phase 02 + Phase 02.1 patterns library is exceptionally large and reusable. ~80% of Phase 03 is *composition* of existing patterns (P1 schema lockstep, P2 fail-soft post-write, P3 singleton state, P5 redact, P6 dedup, P7 Sentry breadcrumb, P8 idempotent migration, P9 server-started gate, P11 HMAC, P14 chaos reverse) onto a new domain (WS + event bus).

---

## Common Pitfalls

### Pitfall 1: 20s default PING interval — server drops connection

**What goes wrong:** `websockets.connect(url)` uses 20s default PING. Polymarket requires 10s. After ~10s of silence (no client PING), server closes connection. Reconnect storm.

**Why it happens:** Library defaults != endpoint requirements. Easy to miss in initial implementation.

**How to avoid:** ALWAYS specify `ping_interval=10, ping_timeout=10` in `connect()`. Plan 04 Wave 0 test asserts these parameters.

**Warning signs:** "Connection drops after about 10 seconds" in logs, matching the docs.polymarket.com troubleshooting entry.

### Pitfall 2: subscribe after connect race

**What goes wrong:** Server closes connection if subscribe payload isn't sent immediately after handshake. [CITED: docs.polymarket.com troubleshooting]

**Why it happens:** asyncio scheduling — if your handler does `await some_other_setup()` between `connect()` yielding and `ws.send(subscribe)`, the server may give up.

**How to avoid:** First `await ws.send(...)` must be the subscribe; any other setup happens after. The skeleton in Focus 1 follows this.

**Warning signs:** Connection closes within 1-2s of every connect, no subscribe acknowledgment in server-side logs (per docs).

### Pitfall 3: NOTIFY payload UTF-8 truncation at 8000 bytes

**What goes wrong:** `pg_notify(channel, payload)` truncates silently at 8000 bytes per spec.

**Why it happens:** If snapshot_complete payload ever grows (e.g., embed full candidate list), silent data loss.

**How to avoid:** Keep payload to `{snapshot_id, taken_at_ms}` only. Consumer fetches full state from DB via the cursor pattern (Focus 3). Plan 05 Wave 0 test asserts payload < 1000 bytes.

### Pitfall 4: asyncpg connection NOT shared across event loops

**What goes wrong:** asyncpg connections are bound to the event loop they were created in. Reuse across loops (e.g., spawning a sync task) → cryptic errors.

**Why it happens:** asyncpg internals are async-only.

**How to avoid:** One connection per task; for connection pooling use `asyncpg.create_pool()` early in daemon startup.

### Pitfall 5: WS message buffer OOM (Phase 02 D-23 redux)

**What goes wrong:** During reconnect, server may push initial_dump for ALL subscribed assets in rapid succession. At 500 assets × ~20KB orderbook each = 10MB burst. Naive `messages.append(...)` accumulates.

**Why it happens:** initial_dump is correct semantically (we asked for fresh state) but consumer must drain promptly.

**How to avoid:** Async pipeline pattern — `WsConsumer.run()` reads from `ws` and pushes to `asyncio.Queue(maxsize=10000)`; separate worker drains queue → mirror writes. Queue full → drop oldest + Sentry breadcrumb (R7 mitigation).

**Warning signs:** Memory RSS spike right after every WS reconnect; Fly OOM events post-reconnect.

### Pitfall 6: Sentry breadcrumb upload not triggered (Phase 02.1 L1 redux)

**What goes wrong:** Fail-soft path adds breadcrumb but never raises an exception, so Sentry never receives a payload + the breadcrumb is lost on next restart.

**Why it happens:** Sentry SDK is event-triggered upload — no event = no breadcrumb upload.

**How to avoid:** Apply Phase 02.2 backlog modification: mirror SUCCESS path ALSO emits a low-frequency `category=mirror` breadcrumb (e.g., every Nth success). This guarantees Sentry has crumbs from this category by the time any real event fires.

**Warning signs:** Sentry API for events shows breadcrumbs from other categories (orchestrator, scheduler) but never `category=mirror` or `category=event-bus`.

### Pitfall 7: Vercel dashboard reads stale RLS-cached anon view

**What goes wrong:** Supabase PostgREST caches view responses briefly; high-frequency L2 writes + dashboard polling = visible lag in `l2_top_of_book` "latest" view.

**Why it happens:** PostgREST `Cache-Control: max-age` defaults.

**How to avoid:** Plan 08 dashboard adds explicit `cache: 'no-store'` to Supabase JS SDK fetch options or use realtime subscriptions instead of polling (Phase 03 maybe overkill; flag for Phase 04).

### Pitfall 8: alembic 002 down_revision wrong

**What goes wrong:** Plan 02-08 introduced `002_add_top_movers_view.py`. Phase 03 migration MUST be `003` not `002`, with `down_revision = "002"`.

**Why it happens:** Easy mental-model bug if research assumed alembic chain ended at 001.

**How to avoid:** Plan 06 Wave 0 test runs `alembic history` first, asserts current head, then derives 003 number programmatically.

### Pitfall 9: docs/learning/ file:line drift (Phase 02.1 P7)

**What goes wrong:** RESEARCH.md cites speculative file:line ("see Focus 1 around l. 25"). When code lands, line numbers shift.

**How to avoid:** Plan 08 docs/learning/10-L2-tracking.md uses `grep -n` post-merge to populate file:line refs — NEVER copy from this RESEARCH.md.

### Pitfall 10: GHA setup-flyctl version drift (Phase 02 L8)

**What goes wrong:** Pin to `superfly/flyctl-actions/setup-flyctl@1.6` not `@v1.x` or `@v1.5`. v1.5 tag was non-existent — silent CI fail for days.

**How to avoid:** Plan 02 deploy-l2.yml copies the EXACT version pin from deploy.yml. Wave 0 test asserts exact string match.

---

## State of the Art

| Old Approach | Current Approach (Phase 03) | When Changed | Impact |
|---|---|---|---|
| Polymarket WS 100-token subscription limit | Single WS unlimited tokens | 2025-05-28 changelog | Phase 03 architecture trivializes "multi-shard" — one connection covers entire candidate set |
| WS sub req sends complete state every connect | `initial_dump=true` optional fresh dump | 2025-05-28 changelog | Reconnect recovers full orderbook baseline cleanly |
| WS only static subscription set | Dynamic subscribe/unsubscribe without reconnect | 2025-09-24 changelog | Candidate-set refresh = lightweight `operation` msgs, not full reconnect |
| Data API /trades unlimited pagination | offset≤1000, limit≤500 | 2025-08-26 changelog | Deep historical backfill requires time-window slide |
| `websockets` 15.x | `websockets` 16.0 | 2026-01-10 PyPI | Better Python 3.13 support; verify 3.12 still passes (likely yes) |

**Deprecated / outdated:**
- `polymarket-kalshi-weather-bot/` as WS implementation reference — verified to NOT contain real Polymarket WS code (thread §2.2 Q1 note). Don't lean on it.
- `clawfirm/` Polymarket arb module — empty shell, also not reference.

---

## Metadata

**Confidence breakdown:**
- Standard stack (Focus 1, 3): HIGH — both versions verified on PyPI, both libraries production-grade
- Architecture (Focus 4, 7): HIGH — patterns mirror Phase 02 proven stack
- WS protocol details (Focus 1, 2): HIGH — official docs cited 2026-05-23
- Event bus (Focus 3): MEDIUM — recommendation strong but A3 `[ASSUMED]` re: realtime-py friction; if wrong, swap to supabase-py realtime
- REST backfill (Focus 6): MEDIUM — A2 `[ASSUMED]` re: filter param name; verify in Plan 06 Wave 0
- Pitfalls + risks: HIGH — drawn from Phase 02 + Phase 02.1 LEARNINGS, applied to new domain

**Research date:** 2026-05-23
**Valid until:** ~2026-06-23 (30 days for stable patterns; re-fetch docs.polymarket.com changelog at plan-phase if any of D-02/D-03/D-08 implementation details are tight to the cited dates)

**Total locked decisions covered:** D-01 (Focus N/A — Plan 01 task spec), D-02 (Focus 1+2), D-03 (Focus 2), D-04 (Focus 5), D-05 (Focus 3+5), D-06 (Focus 7), D-07 (Focus 4), D-08 (Focus 6), D-09 (cross-cutting — applied throughout)

**End of Phase 03 RESEARCH.md. Ready for `/gsd-plan-phase 03 --ws m1-perception` to spawn planner.**
