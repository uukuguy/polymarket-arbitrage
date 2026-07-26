"""Routing and execution configuration.

T6 (2026-06-07) — env-var support via pydantic-settings BaseSettings.
Each config class reads from POLYARB_ prefixed env vars (and .env file).
Precedence: explicit kwarg > env var > default.

Usage:
    # Explicit (env vars ignored for overridden fields):
    config = RoutingConfig(min_profit_threshold_pct=2.0)

    # From env: POLYARB_MIN_PROFIT_THRESHOLD_PCT=2.0
    config = RoutingConfig()

    # Convenience factory:
    from polyarb.routing.config import load_m2_settings
    app = load_m2_settings()
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class RoutingConfig(BaseSettings):
    """Routing engine configuration — env overridable via POLYARB_*."""

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    min_profit_threshold_pct: float = 1.0
    min_leg_size: float = 0.01
    max_liquidity_age_seconds: float = 60.0
    enable_conditional_routing: bool = True


class ExecutionConfig(BaseSettings):
    """Execution pipeline configuration — env overridable via POLYARB_*."""

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    max_slippage_bps: float = 50.0
    leg_timeout_seconds: float = 30.0
    total_timeout_seconds: float = 120.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 2.0


class PositionConfig(BaseSettings):
    """Position tracking configuration — env overridable via POLYARB_*."""

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    initial_balance: float = 1000.0
    max_position_per_asset: float = 500.0
    max_total_exposure: float = 5000.0
    enable_pnl_stop: bool = True
    stop_loss_pct: float = 5.0
    db_path: Path = Field(
        default=Path("data/m2-positions.db"),
        validation_alias=AliasChoices("db_path", "POLYARB_POSITION_DB_PATH"),
    )
    busy_timeout_ms: int = Field(
        default=5000,
        validation_alias=AliasChoices("busy_timeout_ms", "POLYARB_POSITION_BUSY_TIMEOUT_MS"),
    )


@dataclass
class AppConfig:
    """Root configuration aggregating all sub-configs.

    Each sub-config is a BaseSettings so env vars are read independently.
    Explicit construction overrides env, for example
    `AppConfig(routing=RoutingConfig(min_profit_threshold_pct=2.0))`.
    """

    routing: RoutingConfig = field(default_factory=RoutingConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    position: PositionConfig = field(default_factory=PositionConfig)


def load_m2_settings() -> AppConfig:
    """Read m2 config from env vars (POLYARB_*) + defaults.

    Equivalent to `AppConfig()`. Provided as a named entry point so
    production daemons and scripts have a single call site for m2 config,
    matching the pattern of m1 `polyarb.config.load_settings()`.
    """
    return AppConfig()
