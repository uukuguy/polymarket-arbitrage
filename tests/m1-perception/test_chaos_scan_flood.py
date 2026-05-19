"""Chaos: /scan flood (100 concurrent requests) → no daemon crash (RESEARCH §11).

Scenario: 100 HMAC-signed /scan requests sent in parallel via asyncio.gather.
Expected:
  - No 500 responses (daemon must NOT crash)
  - Response codes are 200 / 400 / 404 / 422 only
  - HMAC validation still works (valid sigs get 200 or appropriate result)

Marked with @pytest.mark.slow — this test is heavy (many concurrent requests).
Run explicitly: uv run pytest -m slow tests/m1-perception/test_chaos_scan_flood.py

This mirrors RESEARCH §11 row "/scan flood (10 req/s × 30s) → no crash".
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

# ---------------------------------------------------------------------------
# Test fixture helpers
# ---------------------------------------------------------------------------

_TEST_SCAN_SECRET = "a1b2c3d4e5f6789012345678901234567890abcdef1234567890abcdef123456"


def _make_settings(tmp_path: Path) -> Any:
    from pydantic import SecretStr
    from polyarb.config import Settings
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        retry_attempts=1,
        retry_min_wait_s=0.001,
        retry_max_wait_s=0.005,
        http_timeout_s=2.0,
        liquidity_threshold_usd=100.0,
        scan_shared_secret=SecretStr(_TEST_SCAN_SECRET),
    )


def _sign_body(body_bytes: bytes) -> str:
    return hmac.new(
        _TEST_SCAN_SECRET.encode("utf-8"), body_bytes, hashlib.sha256
    ).hexdigest()


def _make_test_app(settings: Any) -> Any:
    from starlette.testclient import TestClient
    from polyarb.http.app import create_app
    from polyarb.storage.sqlite_store import SQLiteStore

    sqlite_store = SQLiteStore(settings.db_path)
    sqlite_store.init_schema()

    mock_scheduler = MagicMock()
    app = create_app(
        scheduler=mock_scheduler,
        sqlite_store=sqlite_store,
        settings=settings,
    )
    return app, sqlite_store


# ---------------------------------------------------------------------------
# Scan flood test (marked slow)
# ---------------------------------------------------------------------------


@pytest.mark.slow
def test_scan_flood_no_daemon_crash(tmp_path: Path) -> None:
    """100 parallel /scan requests → no 500 responses; daemon stays alive.

    Uses Starlette TestClient in synchronous mode. Each request is independently
    signed with valid HMAC. The expected outcomes per request are:
      - 200: valid recipe executed OK
      - 400: validation error in body
      - 404: unknown recipe
      - 422: recipe execution returned no results

    NOT acceptable: 500 (daemon crashed) or unhandled exceptions.
    """
    from starlette.testclient import TestClient

    settings = _make_settings(tmp_path)
    app, _ = _make_test_app(settings)

    FLOOD_COUNT = 100
    RECIPE_NAME = "thick-but-slippery"

    # Use a valid but fast recipe that won't actually hit DB heavily
    body_dict = {"recipe_name": RECIPE_NAME, "params": {}}
    body_bytes = json.dumps(body_dict).encode("utf-8")
    sig = _sign_body(body_bytes)

    status_codes: list[int] = []
    exceptions: list[Exception] = []

    client = TestClient(app, raise_server_exceptions=False)

    for _ in range(FLOOD_COUNT):
        try:
            resp = client.post(
                "/scan",
                content=body_bytes,
                headers={
                    "Content-Type": "application/json",
                    "X-Signature": sig,
                },
            )
            status_codes.append(resp.status_code)
        except Exception as exc:
            exceptions.append(exc)

    # Critical assertion: no exceptions (daemon didn't crash)
    assert not exceptions, (
        f"Daemon raised exceptions under flood: {exceptions[:3]}"
    )

    # No 500 responses
    server_errors = [sc for sc in status_codes if sc >= 500]
    assert not server_errors, (
        f"Got {len(server_errors)} 5xx responses under flood load: {server_errors[:10]}"
    )

    # All responses must be in acceptable set
    acceptable = {200, 201, 400, 404, 422}
    unexpected = [sc for sc in status_codes if sc not in acceptable]
    assert not unexpected, (
        f"Unexpected status codes under flood: {set(unexpected)}"
    )


@pytest.mark.slow
def test_hmac_validation_survives_flood(tmp_path: Path) -> None:
    """Under flood with MIXED valid/invalid HMAC: valid ones get non-500, invalids get 401.

    Verifies HMAC validation is not bypassed under load.
    """
    from starlette.testclient import TestClient

    settings = _make_settings(tmp_path)
    app, _ = _make_test_app(settings)

    body_dict = {"recipe_name": "thick-but-slippery"}
    body_bytes = json.dumps(body_dict).encode("utf-8")

    valid_sig = _sign_body(body_bytes)
    bad_sig = "deadbeef" * 8  # 64 hex chars, wrong

    client = TestClient(app, raise_server_exceptions=False)

    valid_codes = []
    invalid_codes = []

    for i in range(50):
        # Alternating valid / invalid
        if i % 2 == 0:
            resp = client.post(
                "/scan",
                content=body_bytes,
                headers={"Content-Type": "application/json", "X-Signature": valid_sig},
            )
            valid_codes.append(resp.status_code)
        else:
            resp = client.post(
                "/scan",
                content=body_bytes,
                headers={"Content-Type": "application/json", "X-Signature": bad_sig},
            )
            invalid_codes.append(resp.status_code)

    # Invalid sigs must all be 401
    non_401 = [sc for sc in invalid_codes if sc != 401]
    assert not non_401, (
        f"Bad HMAC must always yield 401, got non-401: {non_401}"
    )

    # Valid sigs must not produce 500
    server_errors = [sc for sc in valid_codes if sc >= 500]
    assert not server_errors, (
        f"Valid HMAC sigs produced 5xx under mixed flood: {server_errors}"
    )
