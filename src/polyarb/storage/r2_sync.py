"""Cloudflare R2 parquet archive sync.

Phase 02 Plan 03 — D-03 / D-12 amendment.

Architecture: After a successful SQLite + Parquet local write (and optional
Supabase mirror), the orchestrator calls upload_parquet_to_r2 as a fail-soft
post-write step. Upload failure raises R2UploadError (project-typed exception)
so the orchestrator's try/except can catch it cleanly and add a DEGRADED Issue.

Design decisions:
- boto3 with S3-compatible endpoint (Cloudflare R2 supports S3 API)
- Deterministic key: year/month/day/HH-MM-SS.parquet (UTC only)
  T-02-12 mitigation: compute_r2_key accepts ONLY taken_at_ms: int (no user input)
- Retry config: max_attempts=3, mode='standard' (exponential backoff)
- R2UploadError: project-typed exception wrapping boto exceptions so orchestrator
  doesn't need to know botocore internals
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import boto3
from botocore.config import Config
from loguru import logger

# ─────────────────────────────────────────────────────────────────────────────
# Retry config (T-02-08 / RESEARCH §4)
# max_attempts=3: initial attempt + 2 retries on transient errors (503 / 500)
# mode='standard': exponential backoff with jitter
# ─────────────────────────────────────────────────────────────────────────────
_R2_RETRY_CONFIG = Config(
    connect_timeout=5,
    read_timeout=15,
    retries={"max_attempts": 3, "mode": "standard"},
)


def control_plane_r2_config(provider_timeout_seconds: float) -> Config:
    """Build the no-hidden-retry R2 envelope used by formal runtime-v2 jobs."""
    if provider_timeout_seconds <= 0:
        raise ValueError("provider_timeout_seconds must be positive")
    connect_timeout = min(5.0, provider_timeout_seconds / 3)
    read_timeout = provider_timeout_seconds - connect_timeout
    return Config(
        connect_timeout=connect_timeout,
        read_timeout=read_timeout,
        retries={"total_max_attempts": 1, "mode": "standard"},
    )


class R2UploadError(Exception):
    """Raised when an R2 upload fails after retries.

    Project-typed exception so orchestrator step 7.6 can catch R2UploadError
    specifically, without needing to import botocore exception types.
    """


def _build_client(
    endpoint: str,
    access_key: str,
    secret_key: str,
    *,
    config: Config | None = None,
):
    """Build a boto3 s3 client configured for Cloudflare R2.

    R2 uses S3-compatible API with a custom endpoint URL. The region_name='auto'
    is R2's required value (Cloudflare ignores the region but boto3 requires one).
    """
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="auto",
        config=_R2_RETRY_CONFIG if config is None else config,
    )


def compute_r2_key(taken_at_ms: int) -> str:
    """Compute deterministic R2 object key from UTC snapshot timestamp.

    T-02-12 mitigation: accepts ONLY taken_at_ms: int — no user-controlled
    path components. The key is computed purely from the UTC timestamp.

    Key format: YYYY/MM/DD/HH-MM-SS.parquet

    Example: taken_at_ms=1715500000000 → "2024/05/12/08-26-40.parquet" (UTC)
    """
    dt = datetime.fromtimestamp(taken_at_ms / 1000, tz=UTC)
    return f"{dt:%Y}/{dt:%m}/{dt:%d}/{dt:%H}-{dt:%M}-{dt:%S}.parquet"


def upload_parquet_to_r2(
    *,
    parquet_path: Path,
    bucket: str,
    key: str,
    endpoint: str,
    access_key: str,
    secret_key: str,
) -> str:
    """Upload a local parquet file to Cloudflare R2.

    Uses keyword-only arguments to prevent positional-argument confusion
    (boto3 upload_file has a different argument order).

    Args:
        parquet_path: Local path to the parquet file to upload.
        bucket: R2 bucket name (from POLYARB_R2_BUCKET).
        key: R2 object key (use compute_r2_key for deterministic key).
        endpoint: R2 endpoint URL (https://<account-id>.r2.cloudflarestorage.com).
        access_key: R2 access key ID (from POLYARB_R2_ACCESS_KEY_ID).
        secret_key: R2 secret access key (from POLYARB_R2_SECRET_ACCESS_KEY).

    Returns:
        The R2 object URL: {endpoint}/{bucket}/{key}

    Raises:
        R2UploadError: If upload fails after retries (wraps boto exception).
    """
    client = _build_client(endpoint, access_key, secret_key)
    try:
        body = Path(parquet_path).read_bytes()
        client.put_object(Bucket=bucket, Key=key, Body=body)
    except Exception as e:
        raise R2UploadError(f"R2 upload failed key={key}: {str(e)[:200]}") from e
    url = f"{endpoint.rstrip('/')}/{bucket}/{key}"
    logger.info(f"R2 upload success: {url}")
    return url
