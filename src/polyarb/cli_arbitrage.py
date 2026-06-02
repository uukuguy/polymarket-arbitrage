"""polyarb CLI: arbitrage (m2) subcommands.

Naming follows cli_observation.py / cli_translation.py convention — single
flat file, NOT a `cli/` directory (Phase 01.1 PATTERNS §5.3 namespace
shadowing lesson). Entry points:

    python -m polyarb.cli_arbitrage evaluate --mid 0.45 --stake 1000
    python -m polyarb.cli_arbitrage run --mid 0.45 --stake 1000
    python -m polyarb.cli_arbitrage status

Wrapped by Makefile targets:
    make eval-arb mid=0.45 stake=1000
    make run-arb  mid=0.45 stake=1000
    make status-arb

T7 Revision 8 (2026-06-02 SESSION 36) — first end-to-end visible surface
for m2. `evaluate` exercises T2 (slippage) + T3 (routing). `run` adds T4
(execution shell, paper mode via no-op leg_executor by default — real
fills will arrive when T5+ wires py-clob-client). `status` reads T5's
PositionTracker. None of these touch real venues yet; they're the
operator's window into what the system *would* do.
"""
from __future__ import annotations

import json
import sys

import typer
from loguru import logger

from polyarb.execution.engine import ExecutionEngine
from polyarb.models.signal import (
    ArbitrageSignal,
    MarketSignal,
)
from polyarb.models.slippage import SlippageCalculator
from polyarb.routing.config import ExecutionConfig, RoutingConfig
from polyarb.routing.engine import RoutingEngine
from polyarb.routing.position_tracker import PositionTracker

app = typer.Typer(no_args_is_help=True, add_completion=False)

# Module-level shared tracker so `run` accumulates positions across
# repeated invocations within a single Python process. Real persistence
# (SQLite / Supabase) is a T5+ concern; this is enough for CLI smoke.
_TRACKER = PositionTracker()


def _setup_logger(verbose: bool) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )


def _build_synthetic_signal(
    mid: float,
    stake: float,
    profit_pct: float,
    n_legs: int,
    venue: str,
) -> ArbitrageSignal:
    """Construct an ArbitrageSignal from CLI inputs.

    Real signals come from m1 scanner → m2 combinator (future). For now
    CLI users hand-build via flags to exercise the routing/execution
    pipeline without waiting on real market data.
    """
    markets: list[MarketSignal] = []
    for i in range(n_legs):
        markets.append(
            MarketSignal(
                id=f"synth-m{i}",
                condition_id=f"cond-{i}",
                venue=venue,
                price=mid,
            )
        )
    return ArbitrageSignal(
        opportunity_id="cli-synth",
        markets=markets,
        max_arbitrage_pct=profit_pct,
        max_stake_per_leg=stake,
        confidence=0.8,
    )


def _format_decision(decision) -> dict:
    """Compact JSON-able view of a RoutingDecision for terminal output."""
    return {
        "signal_id": decision.signal_id,
        "is_profitable": decision.is_profitable,
        "expected_profit_pct": round(decision.expected_profit_pct, 4),
        "expected_profit_abs": round(decision.expected_profit_abs, 4),
        "reason": decision.reason,
        "legs": [
            {
                "leg_id": leg.leg_id,
                "exchange": leg.exchange,
                "action": leg.action,
                "asset": leg.asset,
                "size": leg.size,
                "limit_price": leg.limit_price,
                "estimated_price": leg.estimated_price,
                "estimated_cost": round(leg.estimated_cost, 4),
            }
            for leg in decision.plan.legs
        ],
    }


@app.command()
def evaluate(
    mid: float = typer.Option(0.5, "--mid", help="Mid price for synthetic markets (0..1)"),
    stake: float = typer.Option(1000.0, "--stake", help="Max stake per leg, USD"),
    profit_pct: float = typer.Option(
        2.5, "--profit-pct", help="Synthetic arb profit pct to feed gate"
    ),
    n_legs: int = typer.Option(2, "--legs", help="Number of legs in the signal"),
    venue: str = typer.Option(
        "polymarket", "--venue", help="venue hint on each market (polymarket | gamma | clob)"
    ),
    min_threshold_pct: float = typer.Option(
        1.0, "--min-threshold-pct", help="Routing gate: min profit pct"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Synth a signal → run RoutingEngine → print routed decision (no execution)."""
    _setup_logger(verbose)
    signal = _build_synthetic_signal(mid, stake, profit_pct, n_legs, venue)
    config = RoutingConfig(min_profit_threshold_pct=min_threshold_pct)
    engine = RoutingEngine(config=config, slippage_calc=SlippageCalculator())
    decision = engine.route(signal)
    if decision is None:
        typer.secho(
            f"[gate] signal rejected — profit {profit_pct:.2f}% < threshold {min_threshold_pct:.2f}%",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=1)
    typer.echo(json.dumps(_format_decision(decision), indent=2))


@app.command()
def run(
    mid: float = typer.Option(0.5, "--mid"),
    stake: float = typer.Option(1000.0, "--stake"),
    profit_pct: float = typer.Option(2.5, "--profit-pct"),
    n_legs: int = typer.Option(2, "--legs"),
    venue: str = typer.Option("polymarket", "--venue"),
    min_threshold_pct: float = typer.Option(1.0, "--min-threshold-pct"),
    retry_attempts: int = typer.Option(3, "--retries"),
    retry_delay_seconds: float = typer.Option(0.0, "--retry-delay"),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Synth signal → route → execute via paper-mode executor → print result.

    Paper mode: uses the default no-op leg_executor (always succeeds). When
    T5+ wires a real venue client, swap by passing `leg_executor=...` to
    ExecutionEngine. The CLI defaults preserve a safe "see-what-it-would-do"
    contract — no orders go to any exchange.
    """
    import asyncio

    _setup_logger(verbose)
    signal = _build_synthetic_signal(mid, stake, profit_pct, n_legs, venue)

    routing_engine = RoutingEngine(
        config=RoutingConfig(min_profit_threshold_pct=min_threshold_pct),
        slippage_calc=SlippageCalculator(),
    )
    decision = routing_engine.route(signal)
    if decision is None:
        typer.secho(
            f"[gate] signal rejected — profit {profit_pct:.2f}% < threshold {min_threshold_pct:.2f}%",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=1)

    exec_engine = ExecutionEngine(
        config=ExecutionConfig(
            retry_attempts=retry_attempts,
            retry_delay_seconds=retry_delay_seconds,
        ),
        tracker=_TRACKER,
    )
    result = asyncio.run(exec_engine.execute(decision))

    out = {
        "decision": _format_decision(decision),
        "execution": {
            "status": result.status.value,
            "legs_executed": result.legs_executed,
            "legs_total": result.legs_total,
            "realized_pnl": round(result.realized_pnl, 4),
            "error_message": result.error_message,
            "leg_results": [
                {
                    "leg_id": r.leg.leg_id,
                    "success": r.success,
                    "attempts": r.attempts,
                    "skipped": r.skipped,
                    "error": r.error,
                }
                for r in result.leg_results
            ],
        },
    }
    typer.echo(json.dumps(out, indent=2))


@app.command()
def status(
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Print current PositionTracker state (open positions + portfolio metrics).

    Note: tracker state is per-process. Across separate CLI invocations
    state resets. Persistent state is T5+ scope.
    """
    _setup_logger(verbose)
    open_positions = [
        {
            "market_id": pos.market_id,
            "condition_id": pos.condition_id,
            "side": pos.side,
            "outcome": pos.outcome,
            "stake": pos.stake,
            "entry_price": pos.entry_price,
            "current_price": pos.current_price,
            "pnl": round(pos.pnl, 4),
            "pnl_pct": round(pos.pnl_pct, 4),
        }
        for pos in _TRACKER._open_positions.values()
    ]
    metrics = {
        "open_positions": len(open_positions),
        # Best-effort: only call methods that exist on PositionTracker.
        "balance": getattr(_TRACKER, "balance", None),
    }
    out = {"open_positions": open_positions, "metrics": metrics}
    typer.echo(json.dumps(out, indent=2))


if __name__ == "__main__":
    app()
