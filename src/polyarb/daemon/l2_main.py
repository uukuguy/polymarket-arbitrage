"""Polyarb L2 daemon entry — WS market channel + event listener + Starlette health.

Phase 03 Plan 03 — D-06: separate process from L1 snapshot daemon. This file
ships the runnable skeleton; Plan 04 (WS client + watchdog) and Plan 05
(event bus + candidate refresh) extend `ws_consumer` / `event_listener`
without re-engineering the init order or shutdown semantics.

Init order mirrors `polyarb.daemon.main` (Phase 02 P9 server-started gate is
MANDATORY — otherwise Fly's 120s grace period times out before uvicorn binds
the socket and the platform never observes a live port).

Run locally:
    POLYARB_DAEMON_VARIANT=l2 POLYARB_DB_PATH=./data/l2-state.db \
      POLYARB_HTTP_PORT=19081 uv run python -m polyarb.daemon.l2_main
    curl http://127.0.0.1:19081/health   # IETF strict
    curl http://127.0.0.1:19081/healthz  # always 200

Architecture:
    1. init_logging() — loguru JSON stdout (must be FIRST)
    2. load_settings() — pydantic-settings + .env
    3. init_sentry()   — AFTER logging (LoguruIntegration hook)
       + sentry_sdk.set_tag("service", "polyarb-l2")  — T-03-03-04 / cross-stream filter
    4. SQLiteStore(settings.db_path).init_schema()
    5. create_l2_app(...) — Starlette factory
    6. uvicorn.Server(...) — bound but not yet listening
    7. server_task = asyncio.create_task(server.serve())
    8. P9 server-started gate: for _ in range(100): if server.started: break
                                       else: await asyncio.sleep(0.1)
    9. await stop_event.wait() — signal-driven shutdown
    10. server.should_exit = True
    11. await asyncio.wait_for(server_task, timeout=5.0)  — F-04 bounded shutdown

Plan 04/05 placeholders: `ws_consumer` / `event_listener` start as None;
health checks render "warn" with output="not_configured" until those plans wire
the real components.

Cross-pollination guard (T-03-03-03): this module MUST NOT import from
`polyarb.daemon.main`. Both files share the L1/L2 init contract via
parallel implementations; symbol sharing would obscure the separate
process boundary.
"""
from __future__ import annotations

import asyncio
import signal
import sys
from typing import Any

import sentry_sdk
import uvicorn
from loguru import logger

# All imports below are patched at IMPORT SITE (polyarb.daemon.l2_main.*)
# by tests — Phase 02 L9. Never patch at definition site.
from polyarb.config import load_settings
from polyarb.http.l2_app import create_l2_app
from polyarb.observability.logging import init_logging
from polyarb.observability.sentry import init_sentry
from polyarb.storage.sqlite_store import SQLiteStore


async def main() -> int:
    # 1. FIRST — sets up JSON stdout sink + InterceptHandler
    init_logging()

    # 2. config (load_settings reads pydantic env + YAML overrides)
    settings = load_settings()
    if getattr(settings, "daemon_variant", "l1") != "l2":
        logger.warning(
            f"daemon_variant={settings.daemon_variant!r} but l2_main.py invoked; "
            f"proceeding (POLYARB_DAEMON_VARIANT env override missing?)"
        )

    # 3. sentry AFTER logging (LoguruIntegration needs the loguru sink installed)
    init_sentry(settings)
    # Phase 03 Plan 03 — differentiate polyarb-l2 from polyarb-l1 in Sentry stream
    # so cross-service event filtering works. T-03-03-04 mitigation: literal string.
    sentry_sdk.set_tag("service", "polyarb-l2")

    logger.info("polyarb-l2 daemon starting up")

    # 4. SQLite (separate DB path from L1 — settings.db_path = /data/l2-state.db on Fly)
    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    # ── Plan 04/05 placeholders ──────────────────────────────────────────────
    # Plan 04 will replace with: WsConsumer + WsWatchdog wired together
    # Plan 05 will replace with: EventListener wired to candidate_refresh
    # Until then, l2_health.py renders "warn" with output="not_configured".
    ws_consumer: Any = None
    event_listener: Any = None
    # ─────────────────────────────────────────────────────────────────────────

    app = create_l2_app(
        sqlite_store=sqlite_store,
        settings=settings,
        ws_consumer=ws_consumer,
        event_listener=event_listener,
    )

    config = uvicorn.Config(
        app,
        host="0.0.0.0",   # Fly internal network only — fly-l2.toml controls exposure
        port=settings.http_port,
        log_config=None,   # use loguru, not uvicorn's logger
        access_log=False,  # Axiom doesn't need access logs at this volume
        loop="asyncio",
    )
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info(f"polyarb-l2 received {sig.name}, initiating graceful shutdown")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown, sig)
        except NotImplementedError:
            # Windows fallback — never hit on Fly Linux but keeps test-host portable
            pass

    server_task = asyncio.create_task(server.serve())

    # P9 server-started gate — MANDATORY per Phase 02 L5.
    # uvicorn must bind the socket BEFORE any long-running task (WS / event
    # listener) starts, else Fly's 120s grace period times out and the
    # machine never gets to handle a real request.
    for _ in range(100):
        if server.started:
            break
        await asyncio.sleep(0.1)
    logger.info(
        f"polyarb-l2 daemon running: http on :{settings.http_port}, "
        f"variant={getattr(settings, 'daemon_variant', 'unknown')}"
    )

    # Plan 04 will create ws_consumer task here.
    # Plan 05 will create event_listener task here.
    # Plan 03 boundary: just wait on stop_event.
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        # F-04 contract — MUST propagate, not swallow.
        logger.info("polyarb-l2 daemon shutdown via CancelledError")
        raise
    finally:
        logger.info("polyarb-l2 daemon stopping")
        server.should_exit = True
        # F-04 bounded shutdown — even if uvicorn ignores should_exit, exit within 5s
        try:
            await asyncio.wait_for(server_task, timeout=5.0)
        except asyncio.TimeoutError:
            logger.warning("uvicorn server did not stop within 5s — forcing")
        except asyncio.CancelledError:
            # Re-raise so the run() caller exits cleanly
            raise

    logger.info("polyarb-l2 daemon stopped cleanly")
    return 0


def run() -> None:
    """Entry point for `python -m polyarb.daemon.l2_main`."""
    rc = asyncio.run(main())
    sys.exit(rc)


if __name__ == "__main__":
    run()
