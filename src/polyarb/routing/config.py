"""Routing and execution configuration."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class RoutingConfig:
    """Routing engine configuration."""

    min_profit_threshold_pct: float = 1.0
    min_leg_size: float = 0.01
    max_liquidity_age_seconds: float = 60.0
    enable_conditional_routing: bool = True


@dataclass
class ExecutionConfig:
    """Execution pipeline configuration."""

    max_slippage_bps: float = 50.0
    leg_timeout_seconds: float = 30.0
    total_timeout_seconds: float = 120.0
    retry_attempts: int = 3
    retry_delay_seconds: float = 2.0


@dataclass
class PositionConfig:
    """Position tracking configuration."""

    initial_balance: float = 1000.0
    max_position_per_asset: float = 500.0
    max_total_exposure: float = 5000.0
    enable_pnl_stop: bool = True
    stop_loss_pct: float = 5.0


@dataclass
class AppConfig:
    """Root configuration aggregating all sub-configs."""

    routing: RoutingConfig = field(default_factory=RoutingConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    position: PositionConfig = field(default_factory=PositionConfig)
