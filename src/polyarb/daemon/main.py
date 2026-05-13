"""Daemon entry-point: asyncio.gather(http server + scheduler loop).

Phase 02 Plan 02 — asyncio SIGINT/SIGTERM graceful shutdown.

Run locally:
    POLYARB_ALLOW_EMPTY_SECRET=1 uv run python -m polyarb.daemon.main
    curl http://127.0.0.1:8080/health

Architecture (Plan 02):
    1. init_logging() — loguru JSON to stdout (must be FIRST before any logger calls)
    2. Build Settings + SQLiteStore + SnapshotScheduler
    3. create_app(scheduler, sqlite_store, settings) → Starlette app
    4. uvicorn.Server + asyncio.gather(server_task, scheduler_task)
    5. SIGINT/SIGTERM → stop_event → server.should_exit + scheduler stops cleanly

Plan 04 will add Dockerfile + fly.toml [processes] group.
Plan 05 will add init_sentry() before init_logging().

Source: RESEARCH.md §Architecture Patterns Pattern 1 (lines 295-349, verbatim)
"""
from __future__ import annotations

import asyncio
import signal
import sys

import uvicorn
from loguru import logger

from polyarb.config import load_settings
from polyarb.http.app import create_app
from polyarb.daemon.scheduler import SnapshotScheduler
from polyarb.observability.logging import init_logging
from polyarb.storage.sqlite_store import SQLiteStore


async def main() -> int:
    # MUST be first — sets up JSON stdout sink + InterceptHandler
    init_logging()

    logger.info("polyarb daemon starting up")

    settings = load_settings()
    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    scheduler = SnapshotScheduler(settings=settings, sqlite_store=sqlite_store)
    app = create_app(scheduler=scheduler, sqlite_store=sqlite_store, settings=settings)

    config = uvicorn.Config(
        app,
        host="0.0.0.0",      # Fly internal network only — fly.toml controls exposure
        port=8080,
        log_config=None,      # use loguru, not uvicorn's logger
        access_log=False,     # Axiom doesn't need access logs at this volume
    )
    server = uvicorn.Server(config)

    stop_event = asyncio.Event()

    def _shutdown(sig: signal.Signals) -> None:
        logger.info(f"received {sig.name}, initiating graceful shutdown")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown, sig)

    server_task = asyncio.create_task(server.serve())
    scheduler_task = asyncio.create_task(scheduler.run(stop_event))

    logger.info("daemon running: http server on :8080, scheduler started")

    await stop_event.wait()
    logger.info("stop_event set, shutting down server")
    server.should_exit = True

    await asyncio.gather(server_task, scheduler_task, return_exceptions=True)
    logger.info("polyarb daemon stopped cleanly")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
