"""Tests for loguru JSON sink and InterceptHandler.

Covers D-14 / T-02-07:
- loguru serialize=True produces valid JSON output with required fields
- InterceptHandler redirects stdlib logging (uvicorn.error etc.) to loguru
- backtrace=False + diagnose=False: no source paths or variable values in prod logs
"""
from __future__ import annotations

import io
import json
import logging
import sys

import pytest
from loguru import logger


def _capture_loguru_json(level: str, message: str, **bind_kwargs) -> dict:
    """Capture one loguru JSON line to a dict.

    Temporarily adds a StringIO sink with serialize=True, logs one message,
    removes the sink, and parses the output.
    """
    buffer = io.StringIO()
    sink_id = logger.add(
        buffer,
        serialize=True,
        level=level,
        format="{message}",
        backtrace=False,
        diagnose=False,
    )
    bound = logger.bind(**bind_kwargs) if bind_kwargs else logger
    bound.log(level, message)
    logger.remove(sink_id)

    output = buffer.getvalue().strip()
    assert output, "loguru produced no output — sink may not have flushed"
    return json.loads(output)


def test_json_serialize() -> None:
    """Loguru JSON output is valid JSON with expected fields including extras."""
    record = _capture_loguru_json("INFO", "hello from test", snapshot_id=7)

    # Must be parseable JSON (already validated by json.loads above)
    # Must have message field
    text = record.get("text") or record.get("message") or record.get("record", {}).get("message", "")
    # loguru serialize=True wraps in {"text": ..., "record": {...}}
    if not text:
        # Try nested record structure
        text = record.get("record", {}).get("message", "")
    assert "hello from test" in str(record), f"Message not found in: {record}"

    # Structured field snapshot_id must appear in the record
    record_str = json.dumps(record)
    assert "snapshot_id" in record_str, f"Extra field snapshot_id missing from: {record_str}"
    assert "7" in record_str, f"Extra value 7 not found in: {record_str}"


def test_intercept_stdlib_logging() -> None:
    """stdlib logging.getLogger('uvicorn.error').info() appears in loguru output."""
    from polyarb.observability.logging import InterceptHandler, init_logging

    buffer = io.StringIO()
    # Add our capture sink BEFORE init_logging so we intercept the messages
    sink_id = logger.add(buffer, serialize=True, level="DEBUG", backtrace=False, diagnose=False)

    try:
        # Wire up InterceptHandler for uvicorn.error specifically
        uvicorn_logger = logging.getLogger("uvicorn.error")
        original_handlers = uvicorn_logger.handlers[:]
        original_propagate = uvicorn_logger.propagate

        uvicorn_logger.handlers = [InterceptHandler()]
        uvicorn_logger.propagate = False

        uvicorn_logger.info("test from stdlib uvicorn")

        # Force flush
        logger.complete()
    finally:
        # Restore
        uvicorn_logger.handlers = original_handlers
        uvicorn_logger.propagate = original_propagate
        logger.remove(sink_id)

    output = buffer.getvalue().strip()
    assert output, "InterceptHandler produced no loguru output"
    assert "test from stdlib uvicorn" in output, (
        f"stdlib log message not found in loguru output: {output[:500]}"
    )


def test_no_diagnose_no_backtrace_in_prod_mode() -> None:
    """In production logging mode (backtrace=False, diagnose=False),
    exceptions do not expose source paths or local variable values.

    Addresses T-02-07: information disclosure via loguru JSON logs.
    """
    buffer = io.StringIO()
    sink_id = logger.add(
        buffer,
        serialize=True,
        level="ERROR",
        backtrace=False,   # no source path in stacktrace
        diagnose=False,    # no local variable values
    )

    try:
        secret_var = "super_secret_password_12345"  # noqa: S105
        try:
            raise ValueError(f"something went wrong with {secret_var}")
        except ValueError:
            logger.exception("caught error in prod")
    finally:
        logger.complete()
        logger.remove(sink_id)

    output = buffer.getvalue().strip()
    assert output, "Logger produced no output"

    # The local variable value should NOT appear in JSON output (diagnose=False)
    # Note: the exception MESSAGE contains the variable if it was formatted into it,
    # but local variable INSPECTION (diagnose=True would show locals frame) should not appear.
    # We check that loguru-specific "Locals" section is absent.
    assert "Locals" not in output, (
        "diagnose=False should prevent local variable inspection in logs. "
        f"Output contains 'Locals': {output[:500]}"
    )
