"""Production-grade loguru configuration with stdlib InterceptHandler.

Phase 02 Plan 02 — D-14 (Axiom Free log stack).

init_logging() must be called ONCE at daemon startup, before any other logger.info calls.

Design:
- JSON output to stdout (Fly.io stdout → Axiom ingestion pipeline)
- backtrace=False, diagnose=False: no source paths / local variable values in prod logs
  (T-02-07 mitigation: prevents information disclosure)
- InterceptHandler redirects uvicorn / starlette / httpx stdlib logging → loguru
  so all structured log output flows through one JSON sink

Source references:
- dash0.com/guides/python-logging-with-loguru
- RESEARCH.md §9 lines 1483-1513 (verbatim)
"""
from __future__ import annotations

import logging
import sys

from loguru import logger


class InterceptHandler(logging.Handler):
    """stdlib logging handler that redirects records to loguru.

    Hooks into any Python library that uses stdlib logging (uvicorn, starlette,
    httpx, requests, etc.) so all logs flow through loguru's JSON serializer.

    Source: dash0.com/guides/python-logging-with-loguru (Phase 02 Plan 02)
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = str(record.levelno)

        # Walk up the call stack to find the original caller frame
        # (skip the stdlib logging internals)
        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back  # type: ignore[assignment]
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


def init_logging() -> None:
    """Initialize loguru for production: JSON to stdout, InterceptHandler for stdlib libs.

    Must be called once at daemon entry point before any imports that log.

    Security (T-02-07):
    - backtrace=False: no source file paths in exception output
    - diagnose=False: no local variable values in exception output
    Both prevent accidental information disclosure in prod logs.
    """
    # Remove default stderr handler
    logger.remove()

    # Add JSON stdout sink for Axiom ingestion
    logger.add(
        sys.stdout,
        serialize=True,   # JSON output — one line per log record
        level="INFO",
        enqueue=False,    # in-process; daemon-friendly (no background thread)
        backtrace=False,  # T-02-07: no source path in exception output
        diagnose=False,   # T-02-07: no local variable values in exception output
    )

    # Intercept stdlib logging (uvicorn, starlette, httpx) → loguru
    logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "starlette", "httpx"):
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False
