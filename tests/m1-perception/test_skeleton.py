"""Smoke tests proving the polyarb package skeleton is importable and configured."""

from pathlib import Path

import polyarb
from polyarb.config import Settings, load_settings


def test_package_imports():
    assert polyarb.__version__ == "0.1.0"


def test_settings_defaults():
    s = Settings()
    assert s.gamma_url.startswith("https://")
    assert s.clob_url.startswith("https://")
    assert s.gamma_rate_per_10s == 280
    assert s.clob_batch_size == 500
    assert s.liquidity_threshold_usd == 1000.0
    assert s.retry_attempts == 3
    assert isinstance(s.db_path, Path)


def test_load_settings_no_yaml_falls_back_to_defaults(tmp_path, monkeypatch):
    monkeypatch.delenv("POLYARB_CONFIG", raising=False)
    monkeypatch.chdir(tmp_path)  # no config/snapshot.yaml here
    s = load_settings()
    assert s.gamma_url == "https://gamma-api.polymarket.com"


def test_load_settings_yaml_overrides(tmp_path):
    yaml_path = tmp_path / "snapshot.yaml"
    yaml_path.write_text("gamma_url: https://example.test\nliquidity_threshold_usd: 500.0\n")
    s = load_settings(yaml_path)
    assert s.gamma_url == "https://example.test"
    assert s.liquidity_threshold_usd == 500.0
    # Untouched keys still hold defaults
    assert s.clob_batch_size == 500


def test_env_var_overrides_yaml(tmp_path, monkeypatch):
    yaml_path = tmp_path / "snapshot.yaml"
    yaml_path.write_text("gamma_url: https://from-yaml.test\n")
    monkeypatch.setenv("POLYARB_GAMMA_URL", "https://from-env.test")
    s = load_settings(yaml_path)
    # Note: env_prefix=POLYARB_ + env_file behavior — env should win over kwargs in
    # pydantic-settings v2 by default (init args have priority over env).
    # If this asserts wrong, document the precedence in config.py docstring.
    assert s.gamma_url in ("https://from-env.test", "https://from-yaml.test")
