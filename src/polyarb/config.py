"""Settings + YAML loader for polyarb.

Precedence (highest wins):
    1. environment variables (POLYARB_*)
    2. YAML kwargs passed to Settings(**data)
    3. dataclass defaults

The F-3 path validator constrains db_path / parquet_root to live under the project
root unless POLYARB_ALLOW_EXTERNAL_PATHS=1 is set (test escape hatch only — never
set in production code).

Phase 02 Plan 02 additions:
    - scan_shared_secret: HMAC-SHA256 key for /scan endpoint auth (D-22 amendment).
      Env var: POLYARB_SCAN_SHARED_SECRET (daemon side with POLYARB_ prefix).
      Vercel side uses SCAN_SHARED_SECRET (no prefix — Next.js doesn't load pydantic Settings).
      Both sides compute hmac-sha256(body_bytes, secret.encode('utf-8')) → hex.
    - version: returned in /health JSON (GHA injects via env in Plan 04)
    - release_id: deployment identifier (GHA commit SHA in Plan 04)

    SECURITY NOTE (BLOCKER-3):
      If scan_shared_secret is empty AND POLYARB_ALLOW_EMPTY_SECRET != "1",
      Settings construction raises a ValidationError to prevent silent insecure deploys.
      Tests must set POLYARB_ALLOW_EMPTY_SECRET=1 in env or pass a secret via fixture.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"

    gamma_rate_per_10s: int = 280
    clob_batch_rate_per_10s: int = 450
    clob_batch_size: int = 500

    # 2026-05-20 (Inj 2 P0 fix): scheduler tick interval, was hardcoded 3600
    # via getattr fallback. Now explicit + env-var configurable so chaos
    # injection (and operations) can dial it down without redeploy.
    scheduler_interval_s: int = 3600

    liquidity_threshold_usd: float = 1000.0

    retry_attempts: int = 3
    retry_min_wait_s: float = 1.0
    retry_max_wait_s: float = 4.0

    http_timeout_s: float = 15.0

    db_path: Path = Path("data/state.db")
    parquet_root: Path = Path("data/snapshots")
    cache_root: Path = Path("data/.cache")

    # Phase 02 Plan 02: HTTP daemon fields
    # HMAC-SHA256 shared secret for /scan endpoint.
    # Env var: POLYARB_SCAN_SHARED_SECRET (daemon); SCAN_SHARED_SECRET (Vercel, no prefix).
    scan_shared_secret: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "HMAC-SHA256 shared secret for /scan endpoint auth (Plan 02). "
            "Required at runtime; empty only for tests (set POLYARB_ALLOW_EMPTY_SECRET=1)."
        ),
    )
    # Version returned in /health JSON response
    version: str = Field(default="0.2.0")
    # GHA injects commit SHA in Plan 04 deploy
    release_id: str = Field(default="dev")
    # recipes yaml path for /scan handler
    recipes_yaml_path: Path = Path("config/scan_recipes.yaml")
    # HTTP daemon listen port. Default 19080 (uncommon, avoids 8080/8000 collisions
    # with IDE / IM / Docker / other dev servers). Override via POLYARB_HTTP_PORT.
    http_port: int = Field(default=19080)

    # Phase 03 Plan 03: which daemon variant this process is — "l1" (default) or "l2".
    # Drives Sentry service tag, log line differentiation, and (later) wave 2 health
    # check selection. Env var: POLYARB_DAEMON_VARIANT.
    daemon_variant: Literal["l1", "l2"] = Field(
        default="l1",
        description="Daemon variant identifier — 'l1' for snapshot daemon, 'l2' for orderbook daemon",
    )

    # ── Supabase (D-02) — Plan 03 additions ──────────────────────────────────
    # TWO distinct env vars: supabase-py SDK uses REST URL; Alembic uses DB DSN.
    # See W6 fix in 02-03-PLAN.md for explanation.
    #
    # POLYARB_SUPABASE_URL  = REST URL (https://<ref>.supabase.co) — supabase-py
    # POLYARB_SUPABASE_DB_DSN = Postgres DSN (postgresql://postgres:...) — alembic ONLY
    supabase_url: str = Field(default="", description="Supabase REST URL — supabase-py SDK")
    supabase_db_dsn: SecretStr = Field(
        default=SecretStr(""),
        description="Supabase Postgres DSN — used ONLY by alembic (not supabase-py)",
    )
    supabase_service_key: SecretStr = Field(default=SecretStr(""))
    supabase_mirror_enabled: bool = Field(default=False)  # auto-set by model_validator

    # ── Cloudflare R2 (D-03) — Plan 03 additions ─────────────────────────────
    r2_endpoint: str = Field(default="")
    r2_access_key_id: SecretStr = Field(default=SecretStr(""))
    r2_secret_access_key: SecretStr = Field(default=SecretStr(""))
    r2_bucket: str = Field(default="polyarb-snapshots")
    r2_enabled: bool = Field(default=False)  # auto-set by model_validator

    # ── Observability (D-14/D-15/D-16/D-17) — Plan 05 additions ─────────────
    # Sentry DSN — empty string skips init_sentry (dev mode).
    sentry_dsn: str = Field(
        default="",
        description="Sentry DSN (D-15); empty = skip init_sentry for dev",
    )
    # Axiom ingest token + dataset — only used by Fly stdout forwarder (no
    # direct daemon code path); included here so a typo in env var raises
    # at boot instead of silently dropping logs.
    axiom_token: SecretStr = Field(
        default=SecretStr(""),
        description="Axiom ingest token (D-14)",
    )
    axiom_dataset: str = Field(default="polyarb-prod")
    # Better Stack heartbeat URL — daemon POSTs to <url>/fail on alert,
    # GETs <url> on heartbeat-OK. Empty = skip Better Stack code paths.
    better_stack_heartbeat_url: str = Field(
        default="",
        description="Better Stack heartbeat URL (D-16)",
    )
    # Telegram bot used as direct fallback when Better Stack is unreachable
    # (e.g. Better Stack returns 5xx, or network partition blocks Better Stack
    # but not api.telegram.org). Normal alert path goes through Better Stack
    # native Telegram integration; this is the redundancy.
    telegram_bot_token: SecretStr = Field(
        default=SecretStr(""),
        description="Telegram bot token — direct fallback if Better Stack outage",
    )
    telegram_chat_id: str = Field(default="")
    # Dedup window: a paused-alert fired twice within this many seconds counts
    # as one alert (suppresses storm during flaky-network episodes).
    alert_dedupe_window_seconds: int = Field(default=300)

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def _require_secret_in_prod(self) -> "Settings":
        """Raise if scan_shared_secret is empty and not running in test mode.

        Also auto-sets supabase_mirror_enabled and r2_enabled based on whether
        the respective credentials are fully populated (Plan 03).
        """
        secret_val = self.scan_shared_secret.get_secret_value()
        if not secret_val and os.environ.get("POLYARB_ALLOW_EMPTY_SECRET") != "1":
            raise ValueError(
                "POLYARB_SCAN_SHARED_SECRET must be set in production. "
                "To run tests or local dev without a secret, set POLYARB_ALLOW_EMPTY_SECRET=1."
            )
        # Auto-enable Supabase mirror if both URL + service key are set
        if self.supabase_url and self.supabase_service_key.get_secret_value():
            object.__setattr__(self, "supabase_mirror_enabled", True)
        # Auto-enable R2 if endpoint + access key + secret key are all set
        if (
            self.r2_endpoint
            and self.r2_access_key_id.get_secret_value()
            and self.r2_secret_access_key.get_secret_value()
        ):
            object.__setattr__(self, "r2_enabled", True)
        return self

    @field_validator("db_path", "parquet_root", "cache_root")
    @classmethod
    def _within_project(cls, v: Path) -> Path:
        # Test escape hatch: pytest's tmp_path is outside project root by design.
        # Tests set POLYARB_ALLOW_EXTERNAL_PATHS=1 to bypass this check.
        if os.environ.get("POLYARB_ALLOW_EXTERNAL_PATHS") == "1":
            return v.resolve() if v.is_absolute() else (Path.cwd() / v).resolve()
        project_root = Path.cwd().resolve()
        resolved = (project_root / v).resolve() if not v.is_absolute() else v.resolve()
        try:
            resolved.relative_to(project_root)
        except ValueError as e:
            raise ValueError(
                f"path {v} resolves outside project root {project_root}"
            ) from e
        return resolved


def load_settings(config_path: Path | None = None) -> Settings:
    """Load Settings, optionally merging values from a YAML file.

    Resolution order for the YAML path:
        1. explicit ``config_path`` argument
        2. ``POLYARB_CONFIG`` environment variable
        3. ``config/snapshot.yaml`` relative to current working directory
        4. None (defaults + env vars only)
    """
    if config_path is None:
        env_path = os.environ.get("POLYARB_CONFIG")
        if env_path:
            config_path = Path(env_path)
        else:
            default = Path("config/snapshot.yaml")
            if default.exists():
                config_path = default

    if config_path is not None and Path(config_path).exists():
        data = yaml.safe_load(Path(config_path).read_text()) or {}
        return Settings(**data)
    return Settings()
