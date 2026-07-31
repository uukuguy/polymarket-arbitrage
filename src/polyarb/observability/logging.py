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
from typing import Any

from loguru import logger

from polyarb.observability.redact import (
    _is_sensitive_key,
    _redact_string,
)


def redact_secrets(record: dict[str, Any]) -> bool:
    """Loguru filter — masks known secret patterns in message + extras.

    Plan 02-05 — T-02-07 mitigation. Applied BEFORE ``serialize=True`` so the
    JSON output going to stdout (and on to Axiom) never contains the raw
    secret values.

    Loguru passes the record dict by reference and respects in-place mutation.
    We always return True (keep the line) — the goal is to keep the diagnostic
    log entry, just without the secret bytes.

    Coverage:
      - record["message"]: pattern-based redaction (Bearer, token=, JWT, sk-*)
      - record["extra"]: per-key redaction. If the key matches a sensitive
        name (api_key, token, secret, telegram_bot_token, ...) the value is
        replaced wholesale with "[REDACTED]". Otherwise string values are
        passed through pattern redaction.
    """
    if "message" in record:
        record["message"] = _redact_string(record["message"])

    extra = record.get("extra")
    if isinstance(extra, dict):
        record["extra"] = {
            k: (
                "[REDACTED]"
                if _is_sensitive_key(k)
                else (_redact_string(v) if isinstance(v, str) else v)
            )
            for k, v in extra.items()
        }

    return True


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
        serialize=True,  # JSON output — one line per log record
        level="INFO",
        enqueue=False,  # in-process; daemon-friendly (no background thread)
        backtrace=False,  # T-02-07: no source path in exception output
        diagnose=False,  # T-02-07: no local variable values in exception output
        filter=redact_secrets,  # Plan 05 T-02-07: mask Bearer/token=/key=/JWT before serialize
    )

    # Intercept stdlib logging (uvicorn, starlette, httpx) → loguru
    logging.basicConfig(handlers=[InterceptHandler()], level=logging.INFO, force=True)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "starlette", "httpx"):
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False
    # httpx INFO includes complete request URLs.  Some provider APIs (notably
    # Telegram) require credentials in the path, so suppress the redundant
    # library request line and keep our own sanitized transport outcome logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
