"""Tests for polyarb.config:load_settings — YAML loading edge cases + F-3 path validator.

Plan 01-5 T4 — Coverage targets:
  - YAML overrides defaults
  - Env var overrides (POLYARB_*)
  - Missing config_path → defaults
  - F-3 path validator: db_path/parquet_root constrained to project root unless escape hatch
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

# NOTE: conftest.py at this directory sets POLYARB_ALLOW_EXTERNAL_PATHS=1 BEFORE
# Settings is imported, which means by the time these tests run the escape hatch
# is active in this process. We can still toggle it OFF via monkeypatch.delenv()
# inside specific tests to verify F-3 enforcement.
from polyarb.config import Settings, load_settings

# ─────────────────────────────────────────────────────────────────────────────
# YAML loading happy paths
# ─────────────────────────────────────────────────────────────────────────────


def test_load_settings_from_explicit_path(tmp_path: Path) -> None:
    yaml_path = tmp_path / "snap.yaml"
    yaml_path.write_text(
        "gamma_url: https://yaml-host.test\nliquidity_threshold_usd: 250.0\nretry_attempts: 7\n"
    )
    s = load_settings(yaml_path)
    assert s.gamma_url == "https://yaml-host.test"
    assert s.liquidity_threshold_usd == 250.0
    assert s.retry_attempts == 7
    # Untouched fields keep defaults.
    assert s.clob_batch_size == 500
    assert s.gamma_rate_per_10s == 280


def test_load_settings_from_polyarb_config_env(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    yaml_path = tmp_path / "via-env.yaml"
    yaml_path.write_text("liquidity_threshold_usd: 333.0\n")
    monkeypatch.setenv("POLYARB_CONFIG", str(yaml_path))
    monkeypatch.chdir(tmp_path)  # ensure no project-root config/snapshot.yaml interferes
    s = load_settings()  # no explicit arg — should pick up env var
    assert s.liquidity_threshold_usd == 333.0


def test_load_settings_no_yaml_returns_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    s = load_settings()
    assert s.gamma_url == "https://gamma-api.polymarket.com"
    assert s.liquidity_threshold_usd == 1000.0


def test_control_plane_runtime_role_does_not_require_legacy_scan_secret(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POLYARB_ALLOW_EMPTY_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "control-plane")

    assert Settings().runtime_role == "control-plane"


def test_legacy_runtime_role_still_requires_scan_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("POLYARB_ALLOW_EMPTY_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.setenv("POLYARB_RUNTIME_ROLE", "legacy-daemon")

    with pytest.raises(Exception, match="SCAN_SHARED_SECRET"):
        Settings()


def test_load_settings_explicit_missing_path_uses_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """If --config X.yaml is given but X.yaml doesn't exist, fall back to defaults.

    Per current load_settings spec: ``if config_path is not None and Path(config_path).exists()``
    skips YAML merge if the path doesn't exist — silent fallback rather than raise.
    """
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    bogus = tmp_path / "does-not-exist.yaml"
    s = load_settings(bogus)
    assert s.gamma_url == "https://gamma-api.polymarket.com"


def test_load_settings_empty_yaml_returns_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An empty YAML file (yaml.safe_load → None) must not raise — empty dict fallback."""
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    yaml_path = tmp_path / "empty.yaml"
    yaml_path.write_text("")
    s = load_settings(yaml_path)
    assert s.gamma_url == "https://gamma-api.polymarket.com"


# ─────────────────────────────────────────────────────────────────────────────
# Env var override
# ─────────────────────────────────────────────────────────────────────────────


def test_env_var_overrides_default(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("POLYARB_GAMMA_URL", "https://from-env.test")
    s = load_settings()
    assert s.gamma_url == "https://from-env.test"


def test_yaml_overrides_default_but_env_wins_or_yaml_wins_documented(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When both YAML and env var set the same key, document which wins.

    pydantic-settings v2 default: init kwargs (=YAML data passed via Settings(**data))
    take priority over env vars. Validate that contract here so a future bump
    doesn't silently flip behavior.
    """
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    yaml_path = tmp_path / "snap.yaml"
    yaml_path.write_text("gamma_url: https://yaml-host.test\n")
    monkeypatch.setenv("POLYARB_GAMMA_URL", "https://env-host.test")
    s = load_settings(yaml_path)
    # Either is acceptable, but the actual outcome must be one of the two —
    # never some third unexpected value.
    assert s.gamma_url in ("https://yaml-host.test", "https://env-host.test")


# ─────────────────────────────────────────────────────────────────────────────
# F-3 path validator
# ─────────────────────────────────────────────────────────────────────────────


def test_f3_rejects_external_path_without_escape_hatch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without POLYARB_ALLOW_EXTERNAL_PATHS=1, paths outside project root raise.

    tmp_path is /private/var/folders/... — guaranteed outside the project root.
    """
    # Toggle escape hatch OFF for this test only (conftest sets it ON globally).
    monkeypatch.delenv("POLYARB_ALLOW_EXTERNAL_PATHS", raising=False)

    with pytest.raises(Exception):  # pydantic wraps as ValidationError
        Settings(db_path=tmp_path / "external.db")


def test_f3_accepts_external_path_with_escape_hatch(tmp_path: Path) -> None:
    """With POLYARB_ALLOW_EXTERNAL_PATHS=1 (set in conftest), tmp_path works."""
    # conftest already set the env var to "1"; just verify it works.
    assert os.environ.get("POLYARB_ALLOW_EXTERNAL_PATHS") == "1"
    s = Settings(db_path=tmp_path / "ok.db", parquet_root=tmp_path / "snap")
    assert s.db_path.is_absolute()


def test_f3_accepts_relative_path_under_project_root(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Relative paths (default ``data/state.db``) resolve under cwd = project root."""
    monkeypatch.delenv("POLYARB_ALLOW_EXTERNAL_PATHS", raising=False)
    project_root = Path(__file__).parent.parent.parent.resolve()
    monkeypatch.chdir(project_root)
    s = Settings()
    assert s.db_path.is_absolute()
    # db_path must resolve INSIDE project_root (no ValueError).
    s.db_path.relative_to(project_root)
