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
from pydantic import AliasChoices, Field, SecretStr, field_validator, model_validator
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

    # Phase 05.5: dedicated public CLOB quote producer for the M1→M2
    # known-universe opportunity feed. Disabled unless a deployment explicitly
    # opts in; production cadence stays below the hard 300-second feed SLA.
    neg_risk_quote_worker_enabled: bool = False
    neg_risk_quote_interval_s: int = Field(default=120, gt=0, le=240)

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
        description=(
            "Daemon variant identifier — 'l1' for snapshot daemon, 'l2' for orderbook daemon"
        ),
    )

    # ── Plan 03 Wave 5 deploy-bootstrap (2026-05-25) ─────────────────────
    # Comma-separated list of Polymarket asset_ids to subscribe at L2 startup
    # BEFORE any L1 NOTIFY arrives. Lets L2 connect to WS immediately on
    # cold start (otherwise WsConsumer idles with empty subscribed_assets).
    # Phase 03.1 will replace this with Supabase markets_latest query.
    bootstrap_asset_ids: str = Field(
        default="",
        description="Comma-separated asset_ids for L2 WS bootstrap (POLYARB_BOOTSTRAP_ASSET_IDS)",
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
    l2_runtime_db_dsn: SecretStr = Field(
        default=SecretStr(""),
        description=(
            "Least-privileged L2 daemon LOGIN DSN for direct PostgreSQL access; "
            "production provisioning remains gated by Phase 05.4 Plan 05"
        ),
    )
    supabase_service_key: SecretStr = Field(default=SecretStr(""))
    supabase_mirror_enabled: bool = Field(default=False)  # auto-set by model_validator

    # Phase 03.1 Plan 02 — l2_mirror_enabled gates /health mirror:l2_tob_age_seconds
    # sub-check. SAME secrets as supabase_mirror_enabled (L1 + L2 share Supabase
    # project), so the auto-detect block sets BOTH flags. Distinct field so an
    # operator can opt out of the L2 mirror code path independently (e.g. paused
    # L2 daemon during chaos) without disabling L1 dashboard mirroring.
    l2_mirror_enabled: bool = Field(default=False)  # auto-set by model_validator

    # Phase 03.1 Plan 02 (Plan 07 chaos hook): /health l2_tob_age sub-check
    # thresholds are explicit Settings fields so chaos can temporarily lower
    # them via env var to flip status within 60s instead of waiting 10 minutes
    # for the default 600s threshold.
    #   Pre-step before Inj L2-2 re-run (Plan 07 Task 2 option b):
    #     fly secrets set POLYARB_L2_TOB_AGE_FAIL_S=30 POLYARB_L2_TOB_AGE_WARN_S=15 -a polyarb-l2
    #   Post-cleanup:
    #     fly secrets unset POLYARB_L2_TOB_AGE_FAIL_S POLYARB_L2_TOB_AGE_WARN_S -a polyarb-l2
    l2_tob_age_warn_s: int = Field(
        default=300,
        description=(
            "WARN threshold for /health mirror:l2_tob_age_seconds (env POLYARB_L2_TOB_AGE_WARN_S)"
        ),
    )
    l2_tob_age_fail_s: int = Field(
        default=600,
        description=(
            "FAIL threshold for /health mirror:l2_tob_age_seconds (env POLYARB_L2_TOB_AGE_FAIL_S)"
        ),
    )

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
    # Phase 03.1-05 GAP-102 — explicit Sentry environment tag (was derived from
    # `release_id != "dev"` which silently failed to flip prod deploys to
    # `environment=production`. See sentry-audit-report.md.) Override via
    # POLYARB_SENTRY_ENV. Canonical values: dev / staging / production.
    #
    # `validation_alias="POLYARB_SENTRY_ENV"` is required because the env_prefix
    # convention would otherwise expect `POLYARB_SENTRY_ENVIRONMENT` (field
    # name `sentry_environment` minus `POLYARB_` prefix). We deliberately use
    # the shorter `POLYARB_SENTRY_ENV` to match Sentry's own ergonomics.
    sentry_environment: str = Field(
        default="dev",
        validation_alias=AliasChoices("sentry_environment", "POLYARB_SENTRY_ENV"),
        description=(
            "Sentry environment tag (dev/staging/production). Override via "
            "POLYARB_SENTRY_ENV. Replaces the buggy `release_id != 'dev'` "
            "derivation (Phase 03.1-05 GAP-102)."
        ),
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

    # ── Event bus (D-05) — Plan 03-05 additions ──────────────────────────────
    # Feature-flag for L1 orchestrator step 7.7 (pg_notify fan-out to L2).
    # B1 spawn constraint: DEFAULT FALSE — explicit opt-in via Fly secret
    # `POLYARB_EVENT_BUS_ENABLED=1` ONLY after Plan 07 chaos PASS for Inj L2-3.
    event_bus_enabled: bool = Field(
        default=False,
        description=(
            "Plan 05 D-05 — when True, L1 orchestrator step 7.7 emits "
            "pg_notify('snapshot_complete') after R2 upload. Default FALSE; "
            "opt-in via flyctl secrets set POLYARB_EVENT_BUS_ENABLED=1 only "
            "after Plan 07 chaos PASS for Inj L2-3."
        ),
    )
    event_reconcile_poll_seconds: int = Field(
        default=60,
        gt=0,
        description=(
            "L2 durable cursor reconciliation interval. NOTIFY may wake the "
            "same serialized pump earlier."
        ),
    )
    event_reconcile_stale_seconds: int = Field(
        default=180,
        gt=0,
        description=(
            "Health stale threshold for successful L2 reconciliation; sized "
            "to allow three default polling intervals."
        ),
    )
    # Plan 05 D-04 — scanner-recipes YAML path (REUSE Phase 01.1 scanner verbatim)
    candidate_scanner_yaml: Path | None = Field(
        default=None,
        description="YAML path for scanner recipes consumed by candidate refresh (D-04)",
    )
    # Plan 05 D-04 — watchlist YAML path (REUSE Phase 01.1 watchlist verbatim)
    candidate_watchlist_yaml: Path | None = Field(
        default=None,
        description="YAML path for watchlist entries unioned into candidate set (D-04)",
    )

    # Phase 05.4 — locked continuous-soak acceptance boundaries. Evidence
    # health is unconditional; these values deliberately are not feature gates.
    l3_evidence_sample_interval_s: int = Field(default=30, gt=0)
    l3_evidence_max_sample_gap_s: int = Field(default=75, gt=30)
    l3_promote_interval_s: int = Field(default=300, gt=0)
    l3_promote_max_start_gap_s: int = Field(default=360, gt=300)
    l3_evidence_retention_days: int = Field(default=30, ge=30)
    l3_market_book_fresh_s: int = Field(default=120, gt=0)
    l3_market_ohlc_fresh_s: int = Field(default=120, gt=0)

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    @model_validator(mode="after")
    def _require_secret_in_prod(self) -> Settings:
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
        # Phase 03.1-02: same secrets gate l2_mirror_enabled (L2 daemon uses the
        # same Supabase project for the L2 dashboard mirror).
        if self.supabase_url and self.supabase_service_key.get_secret_value():
            object.__setattr__(self, "supabase_mirror_enabled", True)
            object.__setattr__(self, "l2_mirror_enabled", True)
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
            raise ValueError(f"path {v} resolves outside project root {project_root}") from e
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
