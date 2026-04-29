"""Settings + YAML loader for polyarb.

Precedence (highest wins):
    1. environment variables (POLYARB_*)
    2. YAML kwargs passed to Settings(**data)
    3. dataclass defaults

The F-3 path validator constrains db_path / parquet_root to live under the project
root unless POLYARB_ALLOW_EXTERNAL_PATHS=1 is set (test escape hatch only — never
set in production code).
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    gamma_url: str = "https://gamma-api.polymarket.com"
    clob_url: str = "https://clob.polymarket.com"

    gamma_rate_per_10s: int = 280
    clob_batch_rate_per_10s: int = 450
    clob_batch_size: int = 500

    liquidity_threshold_usd: float = 1000.0

    retry_attempts: int = 3
    retry_min_wait_s: float = 1.0
    retry_max_wait_s: float = 4.0

    http_timeout_s: float = 15.0

    db_path: Path = Path("data/state.db")
    parquet_root: Path = Path("data/snapshots")

    model_config = SettingsConfigDict(env_prefix="POLYARB_", env_file=".env", extra="ignore")

    @field_validator("db_path", "parquet_root")
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
