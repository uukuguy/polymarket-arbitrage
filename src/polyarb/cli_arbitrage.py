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

import asyncio
import json
import sqlite3
import sys
from pathlib import Path
from uuid import uuid4

import typer
from loguru import logger

from polyarb.clients.clob_client import ClobReaderClient
from polyarb.config import Settings
from polyarb.execution.engine import ExecutionEngine
from polyarb.models.signal import (
    ArbitrageSignal,
    MarketSignal,
)
from polyarb.models.slippage import SlippageCalculator
from polyarb.routing.config import ExecutionConfig, PositionConfig, RoutingConfig
from polyarb.routing.engine import RoutingEngine
from polyarb.routing.money import Money
from polyarb.routing.neg_risk_quote_collector import collect_neg_risk_quotes
from polyarb.routing.neg_risk_quote_store import (
    NegRiskQuoteStore,
    QuoteUniverseUnavailableError,
)
from polyarb.routing.opportunity_diagnosis import diagnose_opportunity_feed
from polyarb.routing.opportunity_scanner import (
    QUOTE_SLA_SECONDS,
    QuoteRunUnavailableError,
    StaleQuoteRunError,
    StaleSnapshotError,
    StaleUniverseError,
    scan_neg_risk_buy_all,
    scan_verified_neg_risk_quote_run,
)
from polyarb.routing.position_repository import (
    RepositoryStateError,
    SettlementReceipt,
    SQLitePositionRepository,
)
from polyarb.routing.position_tracker import Fill, PositionTracker, VenueSettlement
from polyarb.storage.sqlite_store import SQLiteStore

app = typer.Typer(no_args_is_help=True, add_completion=False)


def _build_tracker(db_path: Path | None = None) -> PositionTracker:
    """Build one durable tracker for a CLI command invocation."""
    config = PositionConfig()
    try:
        repository = SQLitePositionRepository(
            db_path or config.db_path,
            initial_balance=config.initial_balance,
            busy_timeout_ms=config.busy_timeout_ms,
        )
    except (sqlite3.Error, RepositoryStateError) as exc:
        typer.secho(f"position database error: {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from exc
    return PositionTracker(config=config, repository=repository)


def _setup_logger(verbose: bool) -> None:
    logger.remove()
    logger.add(
        sys.stderr,
        level="DEBUG" if verbose else "INFO",
        format="<green>{time:HH:mm:ss}</green> | <level>{level:<7}</level> | {message}",
    )


@app.command("collect-neg-risk-quotes")
def collect_neg_risk_quotes_command(
    db_path: Path = typer.Option(
        Path("data/state.db"),
        "--db-path",
        help="Local SQLite sidecar written by this one collection only",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    attempt_id: int = typer.Option(0, "--attempt-id", min=0),
) -> None:
    """Collect one local read-only CLOB quote run; not a scheduler or order command."""
    _setup_logger(verbose)
    try:
        settings = Settings()
        SQLiteStore(db_path).init_schema()
        result = asyncio.run(
            collect_neg_risk_quotes(
                quote_store=NegRiskQuoteStore(
                    db_path,
                    structure_generation_read_mode=(
                        settings.structure_generation_read_mode
                    ),
                ),
                reader=ClobReaderClient(settings),
                attempt_id=attempt_id,
            )
        )
    except Exception as error:
        typer.secho(f"quote collection failed: {error}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=2) from error
    typer.echo(
        json.dumps(
            {
                "elapsed_ms": result.elapsed_ms,
                "quote_taken_at_ms": result.quote_taken_at_ms,
                "requested_token_count": result.requested_token_count,
                "run_id": result.run_id,
                "status": result.status,
                "successful_response_count": result.successful_response_count,
                "universe_snapshot_id": result.universe_snapshot_id,
                "universe_hash": result.universe_hash,
                "attempt_id": result.attempt_id,
                "universe_ms": result.universe_ms,
                "admission_ms": result.admission_ms,
                "fetch_ms": result.fetch_ms,
                "transform_ms": result.transform_ms,
                "persist_ms": result.persist_ms,
                "structure_receipt_digest": result.structure_receipt_digest,
            },
            sort_keys=True,
        )
    )


@app.command("cleanup-neg-risk-quotes")
def cleanup_neg_risk_quotes_command(
    db_path: Path = typer.Option(Path("data/state.db"), "--db-path"),
    max_runs: int = typer.Option(20, "--max-runs", min=1, max=1_000),
) -> None:
    """Catch up terminal quote retention using one short transaction per run."""
    store = NegRiskQuoteStore(db_path)
    deleted_runs = 0
    try:
        while deleted_runs < max_runs:
            deleted = store.purge_old_runs(
                keep_last_per_status=10,
                max_runs=1,
            )
            if deleted == 0:
                break
            deleted_runs += deleted
    except (sqlite3.Error, ValueError) as error:
        typer.secho(f"quote retention cleanup failed: {error}", err=True)
        raise typer.Exit(code=2) from error
    typer.echo(
        json.dumps(
            {
                "deleted_runs": deleted_runs,
                "keep_last_per_status": 10,
                "max_runs": max_runs,
                "status": "complete",
            },
            sort_keys=True,
        )
    )


def _build_synthetic_signal(
    mid: float,
    stake: float,
    profit_pct: float,
    n_legs: int,
    venue: str,
    signal_id: str | None = None,
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
    signal = ArbitrageSignal(
        opportunity_id="cli-synth",
        markets=markets,
        max_arbitrage_pct=profit_pct,
        max_stake_per_leg=stake,
        confidence=0.8,
    )
    if signal_id is not None:
        signal.signal_id = signal_id
    return signal


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
                "size": leg.quantity,
                "quantity": leg.quantity,
                "cost_basis": leg.cost_basis_money.to_float(),
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
            f"[gate] signal rejected — profit {profit_pct:.2f}% "
            f"< threshold {min_threshold_pct:.2f}%",
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
    paper_close: bool = typer.Option(
        False,
        "--paper-close",
        help="T5: synth Fill at estimated_price → exercise full open→close lifecycle (zero PnL)",
    ),
    signal_id: str | None = typer.Option(
        None,
        "--signal-id",
        help="Stable synthetic signal identity; reuse only when retrying the same run",
    ),
    db_path: Path | None = typer.Option(
        None,
        "--db-path",
        help="SQLite paper-account path (overrides POLYARB_POSITION_DB_PATH)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Synth signal → route → execute via paper-mode executor → print result.

    Paper mode: uses the default no-op leg_executor (always succeeds). When
    T5+ wires a real venue client, swap by passing `leg_executor=...` to
    ExecutionEngine. The CLI defaults preserve a safe "see-what-it-would-do"
    contract — no orders go to any exchange.

    T5 addition: `--paper-close` synthesizes a Fill at each leg's
    estimated_price after successful execution, exercising the close path.
    With `--paper-close`, `make status-arb` after `make run-arb` shows
    closed lifecycle (open_count=0, realized_pnl=0). Without it, positions
    accumulate across runs and stay open (legacy T4 behaviour).
    """
    import asyncio

    _setup_logger(verbose)
    signal = _build_synthetic_signal(mid, stake, profit_pct, n_legs, venue, signal_id=signal_id)

    routing_engine = RoutingEngine(
        config=RoutingConfig(min_profit_threshold_pct=min_threshold_pct),
        slippage_calc=SlippageCalculator(),
    )
    decision = routing_engine.route(signal)
    if decision is None:
        typer.secho(
            f"[gate] signal rejected — profit {profit_pct:.2f}% "
            f"< threshold {min_threshold_pct:.2f}%",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=1)

    tracker = _build_tracker(db_path)
    exec_engine = ExecutionEngine(
        config=ExecutionConfig(
            retry_attempts=retry_attempts,
            retry_delay_seconds=retry_delay_seconds,
        ),
        tracker=tracker,
        paper_close=paper_close,
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
            "stop_loss": (
                {
                    "loss_pct": round(result.stop_loss.loss_pct, 4),
                    "realized_pnl": round(result.stop_loss.realized_pnl, 4),
                    "threshold_pct": result.stop_loss.threshold_pct,
                    "recommendation": result.stop_loss.recommendation,
                }
                if result.stop_loss is not None
                else None
            ),
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
def scan(
    db_path: Path = typer.Option(Path("data/state.db"), "--db-path"),
    min_edge_bps: float = typer.Option(0.0, "--min-edge-bps"),
    max_snapshot_age_s: float | None = typer.Option(None, "--max-snapshot-age-s"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Discover executable neg-risk buy-all bundles from an M1 database."""
    try:
        settings = Settings()
        found = scan_neg_risk_buy_all(
            db_path,
            min_edge_bps=min_edge_bps,
            max_snapshot_age_s=max_snapshot_age_s,
            limit=limit,
            structure_generation_read_mode=settings.structure_generation_read_mode,
        )
    except (sqlite3.Error, StaleSnapshotError, ValueError) as error:
        typer.secho(f"opportunity scan failed: {error}", err=True)
        raise typer.Exit(code=2) from error
    typer.echo(
        json.dumps(
            {
                "strategy": "neg-risk-buy-all",
                "profit_basis": "gross-before-fees",
                "count": len(found),
                "opportunities": [item.to_dict() for item in found],
            },
            indent=2,
        )
    )


@app.command("scan-quotes")
def scan_quotes(
    db_path: Path = typer.Option(Path("data/state.db"), "--db-path"),
    min_edge_bps: float = typer.Option(0.0, "--min-edge-bps"),
    max_quote_age_s: int = typer.Option(300, "--max-quote-age-s", min=0),
    max_universe_age_s: float = typer.Option(50_400.0, "--max-universe-age-s"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Scan exactly one verified quote run, never a snapshot fallback."""
    try:
        if max_quote_age_s != QUOTE_SLA_SECONDS:
            raise ValueError(
                "max_quote_age_s must equal the canonical 300-second SLA"
            )
        result = scan_verified_neg_risk_quote_run(
            db_path,
            min_edge_bps=min_edge_bps,
            max_quote_age_s=max_quote_age_s,
            max_universe_age_s=max_universe_age_s,
            limit=limit,
        )
    except (
        sqlite3.Error,
        QuoteUniverseUnavailableError,
        QuoteRunUnavailableError,
        StaleQuoteRunError,
        StaleUniverseError,
        ValueError,
    ) as error:
        typer.secho(f"quote opportunity scan failed: {error}", err=True)
        raise typer.Exit(code=2) from error
    typer.echo(
        json.dumps(
            {
                "strategy": "neg-risk-buy-all",
                "profit_basis": "gross-before-fees",
                "coverage": "verified-standard-neg-risk",
                "source_snapshot_id": result.source_snapshot_id,
                "universe_hash": result.universe_hash,
                "quote_run_id": result.quote_run_id,
                "quote_sla_seconds": QUOTE_SLA_SECONDS,
                "count": len(result.opportunities),
                "rejections": dict(result.rejections),
                "opportunities": [
                    item.to_dict() for item in result.opportunities
                ],
            },
            indent=2,
        )
    )


@app.command("diagnose-feed")
def diagnose_feed(
    http_status: int = typer.Option(..., "--http-status"),
    body_file: Path = typer.Option(..., "--body-file"),
) -> None:
    """Classify one saved opportunity-feed HTTP response."""
    try:
        body = body_file.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as exc:
        typer.secho(
            "opportunity diagnostic input unavailable: read error",
            err=True,
        )
        raise typer.Exit(code=2) from exc

    diagnostic = diagnose_opportunity_feed(http_status, body)
    typer.echo(json.dumps(diagnostic.to_dict(), sort_keys=True))
    raise typer.Exit(code=diagnostic.exit_code)


@app.command()
def status(
    db_path: Path | None = typer.Option(
        None,
        "--db-path",
        help="SQLite paper-account path (overrides POLYARB_POSITION_DB_PATH)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Print current PositionTracker state (open positions + portfolio metrics).

    T5: now uses tracker.snapshot() to surface realized PnL, balance, and ROI.
    State is loaded from SQLite on every invocation, so independent run,
    status, and close processes share the same paper account.
    """
    _setup_logger(verbose)
    tracker = _build_tracker(db_path)
    open_positions = [
        {
            "market_id": pos.market_id,
            "condition_id": pos.condition_id,
            "side": pos.side,
            "outcome": pos.outcome,
            "stake": pos.stake,
            "quantity": pos.quantity,
            "cost_basis": pos.cost_basis,
            "entry_price": pos.entry_price,
            "current_price": pos.current_price,
            "pnl": round(pos.pnl, 4),
            "pnl_pct": round(pos.pnl_pct, 4),
        }
        for pos in tracker.open_positions()
    ]
    snap = tracker.snapshot()
    metrics = {
        "open_positions": snap.open_positions,
        "balance": round(snap.balance, 4),
        "total_unrealized_pnl": round(snap.total_unrealized_pnl, 4),
        "total_realized_pnl": round(snap.total_realized_pnl, 4),
        "total_pnl": round(snap.total_pnl, 4),
        "roi_pct": round(snap.roi_pct, 4),
        "max_exposure": round(snap.max_exposure, 4),
    }
    stop_loss_event = tracker.check_stop_loss_event()
    stop_loss = (
        {
            "loss_pct": round(stop_loss_event.loss_pct, 4),
            "realized_pnl": round(stop_loss_event.realized_pnl, 4),
            "threshold_pct": stop_loss_event.threshold_pct,
            "recommendation": stop_loss_event.recommendation,
        }
        if stop_loss_event is not None
        else None
    )
    out = {
        "open_positions": open_positions,
        "metrics": metrics,
        "stop_loss": stop_loss,
    }
    typer.echo(json.dumps(out, indent=2))


@app.command()
def close(
    market_id: str = typer.Option(..., "--market-id", help="Open position market_id to close"),
    exit_price: float = typer.Option(..., "--exit-price", help="Fill exit price (0..1)"),
    size: float | None = typer.Option(
        None,
        "--size",
        help="Filled share quantity (defaults to all remaining shares)",
    ),
    db_path: Path | None = typer.Option(
        None,
        "--db-path",
        help="SQLite paper-account path (overrides POLYARB_POSITION_DB_PATH)",
    ),
    operation_id: str | None = typer.Option(
        None,
        "--operation-id",
        help="Caller-owned immutable close identity for cross-process retry",
    ),
    fill_id: str | None = typer.Option(
        None,
        "--fill-id",
        help="Venue-owned immutable fill identity; required for partial fills",
    ),
    venue_cash: str | None = typer.Option(None, "--venue-cash", help="Venue-confirmed gross cash"),
    venue_fee: str | None = typer.Option(None, "--venue-fee", help="Venue-confirmed fee"),
    venue_status: str | None = typer.Option(
        None, "--venue-status", help="Venue terminal status (CONFIRMED)"
    ),
    venue_ref: str | None = typer.Option(
        None, "--venue-ref", help="Immutable venue trade/source reference"
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v"),
) -> None:
    """Close an open position via a synthesized Fill (operator-driven).

    Production close path goes through `ExecutionEngine` + `fill_provider`.
    This subcommand exists so operators can exercise the close path
    manually — e.g., after a `run --paper-close=false`, observe an open
    position via `status`, then `close --market-id=... --exit-price=...`
    to see realized PnL flow.

    The command loads the configured SQLite paper account, so it can close a
    position opened by an earlier independent `run` process.
    """
    _setup_logger(verbose)
    venue_values = (venue_cash, venue_fee, venue_status, venue_ref)
    venue_requested = any(value is not None for value in venue_values)
    if venue_requested and (
        not all(value is not None for value in venue_values) or fill_id is None or size is None
    ):
        typer.secho(
            "venue truth requires --venue-cash, --venue-fee, --venue-status, "
            "--venue-ref, --fill-id, and explicit --size",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    if fill_id is not None and size is None:
        typer.secho(
            "immutable --fill-id requires explicit --size so retries can be verified",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=2)
    settlement = None
    if venue_requested:
        assert venue_cash is not None
        assert venue_fee is not None
        assert venue_status is not None
        assert venue_ref is not None
        try:
            settlement = VenueSettlement(
                gross_cash=Money.from_value(venue_cash),
                fee=Money.from_value(venue_fee),
                status=venue_status,
                source_ref=venue_ref,
            )
        except (TypeError, ValueError, OverflowError) as exc:
            typer.secho(f"venue truth rejected: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2) from exc

    tracker = _build_tracker(db_path)
    caller_supplied = fill_id is not None or operation_id is not None
    effective_operation_id = (
        f"venue-fill:{fill_id}"
        if fill_id is not None
        else operation_id or f"local:operator-close:{market_id}:{uuid4()}"
    )
    replayed = False
    receipt = tracker.operation_receipt(effective_operation_id) if caller_supplied else None
    if receipt is not None and fill_id is None and not venue_requested:
        if receipt.operation_type != "close" or receipt.target_id != market_id:
            typer.secho(
                "operation identity conflict: "
                f"{effective_operation_id!r} was already used for "
                f"{receipt.operation_type!r}/{receipt.target_id!r}",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)
        if isinstance(receipt.result, SettlementReceipt):
            typer.secho(
                "venue settlement replay requires the complete original venue inputs",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)
        if isinstance(receipt.result, Money):
            pnl = receipt.result.to_float()
        elif type(receipt.result) is float:
            pnl = receipt.result
        else:
            typer.secho(
                f"corrupt close receipt: {effective_operation_id!r} "
                "does not contain a money result",
                fg=typer.colors.RED,
                err=True,
            )
            raise typer.Exit(code=2)
        replayed = True
    else:
        replayed = receipt is not None
        pos = next(
            (p for p in tracker.open_positions() if p.market_id == market_id),
            None,
        )
        if pos is None and not replayed:
            typer.secho(
                f"no open position for market_id={market_id}",
                fg=typer.colors.YELLOW,
                err=True,
            )
            raise typer.Exit(code=1)
        fill = Fill(
            market_id=market_id,
            exit_price=exit_price,
            filled_quantity=(size if size is not None else pos.quantity if pos is not None else 0),
            fill_id=fill_id or "",
            settlement=settlement,
        )
        try:
            pnl = tracker.close_position_with_fill(
                fill,
                operation_id=effective_operation_id,
            )
        except ValueError as exc:
            typer.secho(f"close rejected: {exc}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=2)
    settlement_output = None
    if isinstance(pnl, SettlementReceipt):
        settlement_output = {
            "source": pnl.source,
            "gross_cash": pnl.gross_cash.to_float(),
            "fee": pnl.fee.to_float(),
            "net_cash": pnl.net_cash.to_float(),
            "realized_pnl": pnl.realized_pnl.to_float(),
        }
        realized_pnl = pnl.realized_pnl.to_float()
    else:
        realized_pnl = pnl
    typer.echo(
        json.dumps(
            {
                "closed": market_id,
                "operation_id": effective_operation_id,
                "fill_id": fill_id,
                "replayed": replayed,
                "retry_safe": caller_supplied,
                "exit_price": exit_price,
                "realized_pnl": round(realized_pnl, 4),
                "settlement": settlement_output,
                "total_realized_pnl": round(tracker.total_realized_pnl, 4),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    app()
