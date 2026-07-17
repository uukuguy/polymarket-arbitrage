"""T6 (2026-06-07): verify env-var override for routing/execution/position configs.

Each config class is now a pydantic BaseSettings with POLYARB_ prefix.
Tests must isolate env state since BaseSettings reads os.environ at import time.
"""
from __future__ import annotations

from pathlib import Path

import pytest

from polyarb.routing.config import (
    AppConfig,
    ExecutionConfig,
    PositionConfig,
    RoutingConfig,
    load_m2_settings,
)


class TestRoutingConfigEnvOverride:
    def test_default_value(self):
        cfg = RoutingConfig()
        assert cfg.min_profit_threshold_pct == 1.0

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_MIN_PROFIT_THRESHOLD_PCT", "2.5")
        cfg = RoutingConfig()
        assert cfg.min_profit_threshold_pct == 2.5

    def test_explicit_kwarg_wins_over_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_MIN_PROFIT_THRESHOLD_PCT", "2.5")
        cfg = RoutingConfig(min_profit_threshold_pct=0.5)
        assert cfg.min_profit_threshold_pct == 0.5

    def test_env_override_bool(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_ENABLE_CONDITIONAL_ROUTING", "false")
        cfg = RoutingConfig()
        assert cfg.enable_conditional_routing is False

    def test_env_override_bool_true(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_ENABLE_CONDITIONAL_ROUTING", "1")
        cfg = RoutingConfig()
        assert cfg.enable_conditional_routing is True


class TestExecutionConfigEnvOverride:
    def test_default_values(self):
        cfg = ExecutionConfig()
        assert cfg.retry_attempts == 3
        assert cfg.retry_delay_seconds == 2.0
        assert cfg.max_slippage_bps == 50.0

    def test_env_override_retry(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_RETRY_ATTEMPTS", "5")
        monkeypatch.setenv("POLYARB_RETRY_DELAY_SECONDS", "0.5")
        cfg = ExecutionConfig()
        assert cfg.retry_attempts == 5
        assert cfg.retry_delay_seconds == 0.5

    def test_kwarg_wins_over_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_RETRY_ATTEMPTS", "5")
        cfg = ExecutionConfig(retry_attempts=1)
        assert cfg.retry_attempts == 1


class TestPositionConfigEnvOverride:
    def test_default_values(self):
        cfg = PositionConfig()
        assert cfg.initial_balance == 1000.0
        assert cfg.stop_loss_pct == 5.0
        assert cfg.enable_pnl_stop is True
        assert cfg.db_path == Path("data/m2-positions.db")
        assert cfg.busy_timeout_ms == 5000

    def test_env_override_stop_loss(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_STOP_LOSS_PCT", "10.0")
        monkeypatch.setenv("POLYARB_INITIAL_BALANCE", "5000.0")
        cfg = PositionConfig()
        assert cfg.stop_loss_pct == 10.0
        assert cfg.initial_balance == 5000.0

    def test_kwarg_wins_over_env(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_STOP_LOSS_PCT", "10.0")
        cfg = PositionConfig(stop_loss_pct=3.0)
        assert cfg.stop_loss_pct == 3.0

    def test_position_db_path_env_and_explicit_precedence(
        self, monkeypatch: pytest.MonkeyPatch
    ):
        monkeypatch.setenv("POLYARB_POSITION_DB_PATH", "data/from-env.db")
        assert PositionConfig().db_path == Path("data/from-env.db")
        assert PositionConfig(db_path=Path("data/explicit.db")).db_path == Path(
            "data/explicit.db"
        )


class TestAppConfig:
    def test_default_aggregation(self):
        app = AppConfig()
        assert isinstance(app.routing, RoutingConfig)
        assert isinstance(app.execution, ExecutionConfig)
        assert isinstance(app.position, PositionConfig)
        assert app.routing.min_profit_threshold_pct == 1.0
        assert app.execution.retry_attempts == 3
        assert app.position.stop_loss_pct == 5.0

    def test_explicit_sub_config(self):
        app = AppConfig(
            routing=RoutingConfig(min_profit_threshold_pct=0.5),
            execution=ExecutionConfig(retry_attempts=10),
        )
        assert app.routing.min_profit_threshold_pct == 0.5
        assert app.execution.retry_attempts == 10
        assert app.position.stop_loss_pct == 5.0  # default

    def test_env_override_cascades(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_STOP_LOSS_PCT", "7.5")
        app = AppConfig()
        assert app.position.stop_loss_pct == 7.5


class TestLoadM2Settings:
    def test_returns_app_config(self):
        cfg = load_m2_settings()
        assert isinstance(cfg, AppConfig)

    def test_env_override_through_factory(self, monkeypatch: pytest.MonkeyPatch):
        monkeypatch.setenv("POLYARB_MIN_PROFIT_THRESHOLD_PCT", "3.0")
        cfg = load_m2_settings()
        assert cfg.routing.min_profit_threshold_pct == 3.0
