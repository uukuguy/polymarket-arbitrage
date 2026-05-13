"""Wave 0 tests for R2 sync — deterministic key + fail-soft upload.

Task 1 (02-03): TDD RED phase. polyarb.storage.r2_sync does not yet exist,
so test collection fails with ImportError. That is the expected RED state.

Tests use botocore.stub.Stubber (no real network calls). Key contract:
- compute_r2_key(taken_at_ms: int) -> str  (UTC-based deterministic key)
- upload_parquet_to_r2(*, parquet_path, bucket, key, endpoint, access_key, secret_key) -> str
- R2UploadError: raised on upload failure (project-typed, not bare boto exception)
"""
from __future__ import annotations

import inspect
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_R2_TEST_ENDPOINT = "https://test.r2.cloudflarestorage.com"
_R2_TEST_BUCKET = "test-bucket"
_R2_TEST_ACCESS_KEY = "dummy-access-key"
_R2_TEST_SECRET_KEY = "dummy-secret-key"


def _make_boto3_client(endpoint: str = _R2_TEST_ENDPOINT) -> "Any":
    import boto3
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=_R2_TEST_ACCESS_KEY,
        aws_secret_access_key=_R2_TEST_SECRET_KEY,
        region_name="auto",
    )


# ---------------------------------------------------------------------------
# Test: upload to R2 with botocore Stubber
# ---------------------------------------------------------------------------


def test_upload_to_r2(tmp_path: Path) -> None:
    """upload_parquet_to_r2 sends PutObject with correct bucket+key and returns URL."""
    import boto3
    from botocore.stub import Stubber

    from polyarb.storage.r2_sync import compute_r2_key, upload_parquet_to_r2

    # Parquet bytes (dummy content — stubber never actually sends them)
    parquet_file = tmp_path / "test.parquet"
    parquet_file.write_bytes(b"PAR1dummy")

    # taken_at_ms: 2026-05-12 08:32:00 UTC
    # datetime(2026, 5, 12, 8, 32, 0, tzinfo=timezone.utc)
    taken_at_ms = int(datetime(2026, 5, 12, 8, 32, 0, tzinfo=timezone.utc).timestamp() * 1000)
    key = compute_r2_key(taken_at_ms)
    assert key == "2026/05/12/08-32-00.parquet", f"unexpected key: {key!r}"

    # Use a fresh boto3 client with the Stubber
    client = _make_boto3_client()
    with Stubber(client) as stubber:
        stubber.add_response(
            "put_object",
            {},
            expected_params={
                "Bucket": _R2_TEST_BUCKET,
                "Key": key,
                "Body": b"PAR1dummy",
            },
        )

        # We patch boto3.client inside r2_sync so the Stubber client is used
        import unittest.mock as mock
        with mock.patch("polyarb.storage.r2_sync._build_client", return_value=client):
            url = upload_parquet_to_r2(
                parquet_path=parquet_file,
                bucket=_R2_TEST_BUCKET,
                key=key,
                endpoint=_R2_TEST_ENDPOINT,
                access_key=_R2_TEST_ACCESS_KEY,
                secret_key=_R2_TEST_SECRET_KEY,
            )

    assert _R2_TEST_BUCKET in url
    assert key in url


# ---------------------------------------------------------------------------
# Test: compute_r2_key is deterministic
# ---------------------------------------------------------------------------


def test_compute_r2_key_deterministic() -> None:
    """Same taken_at_ms → same key string, called twice."""
    from polyarb.storage.r2_sync import compute_r2_key

    ts_ms = 1715500000000
    key1 = compute_r2_key(ts_ms)
    key2 = compute_r2_key(ts_ms)
    assert key1 == key2, f"non-deterministic: {key1!r} vs {key2!r}"
    # Format: YYYY/MM/DD/HH-MM-SS.parquet
    assert re.match(r"^\d{4}/\d{2}/\d{2}/\d{2}-\d{2}-\d{2}\.parquet$", key1), (
        f"key format mismatch: {key1!r}"
    )


# ---------------------------------------------------------------------------
# Test: compute_r2_key uses UTC only
# ---------------------------------------------------------------------------


def test_compute_r2_key_utc_only() -> None:
    """compute_r2_key uses UTC regardless of local timezone."""
    from polyarb.storage.r2_sync import compute_r2_key

    # midnight UTC: 2026-05-12 00:00:00 UTC
    midnight_utc_ms = int(datetime(2026, 5, 12, 0, 0, 0, tzinfo=timezone.utc).timestamp() * 1000)
    key = compute_r2_key(midnight_utc_ms)

    # The key must start with 2026/05/12/00-00-00
    expected_prefix = "2026/05/12/00-00-00"
    assert key == f"{expected_prefix}.parquet", (
        f"expected UTC midnight key {expected_prefix!r}, got {key!r}. "
        f"Check that datetime.fromtimestamp uses tz=timezone.utc not local time."
    )


# ---------------------------------------------------------------------------
# Test: R2 upload failure raises R2UploadError (not bare boto exception)
# ---------------------------------------------------------------------------


def test_r2_upload_failure_raises_known_exception(tmp_path: Path) -> None:
    """When R2 upload fails, R2UploadError is raised (project-typed)."""
    import boto3
    from botocore.stub import Stubber

    from polyarb.storage.r2_sync import R2UploadError, compute_r2_key, upload_parquet_to_r2

    parquet_file = tmp_path / "fail.parquet"
    parquet_file.write_bytes(b"PAR1")
    key = compute_r2_key(1715500000000)

    client = _make_boto3_client()
    with Stubber(client) as stubber:
        # Add an error response (503 Service Unavailable)
        stubber.add_client_error(
            "put_object",
            service_error_code="ServiceUnavailable",
            service_message="R2 is temporarily unavailable",
            http_status_code=503,
        )

        import unittest.mock as mock
        with mock.patch("polyarb.storage.r2_sync._build_client", return_value=client):
            with pytest.raises(R2UploadError) as exc_info:
                upload_parquet_to_r2(
                    parquet_path=parquet_file,
                    bucket=_R2_TEST_BUCKET,
                    key=key,
                    endpoint=_R2_TEST_ENDPOINT,
                    access_key=_R2_TEST_ACCESS_KEY,
                    secret_key=_R2_TEST_SECRET_KEY,
                )

    # Must be R2UploadError, not any boto or generic exception
    assert isinstance(exc_info.value, R2UploadError)
    assert "key=" in str(exc_info.value) or "R2" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test: retry config — 1 failure + 1 success = upload succeeds
# ---------------------------------------------------------------------------


def test_r2_retry_config_applied(tmp_path: Path) -> None:
    """Stubber: first call 503 + second call 200 → upload succeeds with retry."""
    import boto3
    from botocore.stub import Stubber

    from polyarb.storage.r2_sync import R2UploadError, compute_r2_key, upload_parquet_to_r2

    parquet_file = tmp_path / "retry.parquet"
    parquet_file.write_bytes(b"PAR1retry")
    key = compute_r2_key(1715500000000)

    # The retry config has max_attempts=3; one 503 followed by 200 should succeed.
    # However, botocore Stubber does not actually retry automatically through the
    # stub mechanism — it only intercepts exactly in sequence. We verify retry config
    # is set on the client by inspecting the config, not by simulating retry.
    from polyarb.storage.r2_sync import _R2_RETRY_CONFIG

    retry_cfg = _R2_RETRY_CONFIG.retries
    assert retry_cfg.get("max_attempts") == 3, (
        f"Expected max_attempts=3, got {retry_cfg}"
    )
    assert retry_cfg.get("mode") == "standard", (
        f"Expected mode='standard', got {retry_cfg}"
    )


# ---------------------------------------------------------------------------
# Test: compute_r2_key rejects user input — signature is (taken_at_ms: int)
# ---------------------------------------------------------------------------


def test_r2_key_rejects_user_input() -> None:
    """compute_r2_key accepts ONLY taken_at_ms: int — no string path components."""
    from polyarb.storage.r2_sync import compute_r2_key

    sig = inspect.signature(compute_r2_key)
    params = list(sig.parameters.keys())
    assert params == ["taken_at_ms"], (
        f"compute_r2_key must accept ONLY 'taken_at_ms' parameter (T-02-12 path injection). "
        f"Got: {params}"
    )
    # Verify it's typed as int in annotations
    ann = compute_r2_key.__annotations__
    assert ann.get("taken_at_ms") == int or str(ann.get("taken_at_ms")) == "<class 'int'>", (
        f"taken_at_ms must be annotated as int, got: {ann.get('taken_at_ms')}"
    )
