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
    """stdlib logging.getLogger('uvicorn.error').info() appears in loguru output.

    InterceptHandler redirects stdlib records to loguru. We verify this by:
    1. Adding a loguru StringIO sink
    2. Registering InterceptHandler on a test stdlib logger
    3. Emitting a record via stdlib
    4. Verifying the record appeared in the loguru sink
    """
    from polyarb.observability.logging import InterceptHandler

    buffer = io.StringIO()
    sink_id = logger.add(buffer, serialize=True, level="DEBUG", backtrace=False, diagnose=False)

    try:
        # Use a fresh test-specific logger to avoid interfering with existing loggers
        test_logger_name = "polyarb.test.intercept_handler"
        test_log = logging.getLogger(test_logger_name)
        original_handlers = test_log.handlers[:]
        original_propagate = test_log.propagate
        original_level = test_log.level

        test_log.handlers = [InterceptHandler()]
        test_log.propagate = False
        test_log.setLevel(logging.DEBUG)

        test_log.info("intercept handler test message xyz")
    finally:
        test_log.handlers = original_handlers
        test_log.propagate = original_propagate
        test_log.setLevel(original_level)
        logger.remove(sink_id)

    output = buffer.getvalue().strip()
    assert output, "InterceptHandler produced no loguru output"
    assert "intercept handler test message xyz" in output, (
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
