"""Smoke tests for `polyarb.cli_arbitrage`.

Verifies the CLI surface created in T7 Revision 8 (SESSION 36):
- `evaluate` happy path returns parseable JSON with routed legs
- `evaluate` below-threshold exits non-zero
- `run` happy path returns JSON with execution=completed
- `status` returns JSON with the expected envelope

These tests don't exercise real venue code — `run` uses the default no-op
leg_executor so all legs trivially succeed (paper mode).
"""
from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from polyarb import cli_arbitrage as cli_mod
from polyarb.cli_arbitrage import app
from polyarb.routing.position_tracker import PositionTracker

runner = CliRunner()


@pytest.fixture(autouse=True)
def isolated_tracker(monkeypatch: pytest.MonkeyPatch) -> PositionTracker:
    tracker = PositionTracker()
    monkeypatch.setattr(cli_mod, "_build_tracker", lambda db_path=None: tracker)
    return tracker


def test_evaluate_happy_path_returns_routed_decision_json():
    result = runner.invoke(
        app, ["evaluate", "--mid", "0.45", "--stake", "500", "--profit-pct", "3.0"]
    )
    assert result.exit_code == 0, f"non-zero exit: {result.exit_code}\n{result.output}"
    payload = json.loads(result.stdout)
    assert payload["is_profitable"] is True
    assert len(payload["legs"]) == 2
    assert payload["legs"][0]["exchange"] == "polymarket"
    assert payload["expected_profit_pct"] == 3.0
    # estimated_cost MUST come from slippage model, not naive price × stake.
    naive = 0.45 * 500
    assert payload["legs"][0]["estimated_cost"] != naive


def test_evaluate_below_threshold_exits_nonzero():
    result = runner.invoke(
        app,
        [
            "evaluate", "--mid", "0.5", "--stake", "100",
            "--profit-pct", "0.5", "--min-threshold-pct", "2.0",
        ],
    )
    assert result.exit_code != 0, (
        f"expected non-zero exit for below-threshold signal; got 0\n{result.output}"
    )
    # Output (captured combined stdout+stderr) should mention the gate rejection.
    assert "gate" in result.output.lower() or "threshold" in result.output.lower(), (
        f"expected 'gate' or 'threshold' in output; got: {result.output!r}"
    )


def test_run_happy_path_paper_executor_marks_completed():
    result = runner.invoke(
        app,
        [
            "run", "--mid", "0.45", "--stake", "500", "--profit-pct", "3.0",
            "--legs", "2", "--retry-delay", "0",
        ],
    )
    assert result.exit_code == 0, f"non-zero exit: {result.exit_code}\n{result.output}"
    payload = json.loads(result.stdout)
    assert "decision" in payload and "execution" in payload
    assert payload["execution"]["status"] == "completed"
    assert payload["execution"]["legs_executed"] == 2
    assert payload["execution"]["legs_total"] == 2
    assert len(payload["execution"]["leg_results"]) == 2
    assert all(r["success"] for r in payload["execution"]["leg_results"])


def test_status_returns_expected_envelope():
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0, f"non-zero exit: {result.exit_code}\n{result.output}"
    payload = json.loads(result.stdout)
    assert "open_positions" in payload
    assert "metrics" in payload
    assert "stop_loss" in payload  # T5: top-level stop-loss field
    assert isinstance(payload["open_positions"], list)
    # T5: metrics now sourced from tracker.snapshot()
    metrics = payload["metrics"]
    for field in (
        "open_positions",
        "balance",
        "total_unrealized_pnl",
        "total_realized_pnl",
        "total_pnl",
        "roi_pct",
        "max_exposure",
    ):
        assert field in metrics, f"missing snapshot field: {field}"


# ──────────────────────────────────────────────────────────────────────────
# T5 — paper_close lifecycle + close subcommand
# ──────────────────────────────────────────────────────────────────────────


def test_run_paper_close_closes_lifecycle_zero_pnl():
    """`run --paper-close` synths Fill at estimated_price → open then close
    same-process → tracker has 0 open positions, realized_pnl ≈ 0."""
    result = runner.invoke(
        app,
        [
            "run", "--mid", "0.45", "--stake", "500", "--profit-pct", "3.0",
            "--legs", "2", "--retry-delay", "0", "--paper-close",
        ],
    )
    assert result.exit_code == 0, f"non-zero exit: {result.exit_code}\n{result.output}"
    payload = json.loads(result.stdout)
    assert payload["execution"]["status"] == "completed"
    assert payload["execution"]["stop_loss"] is None  # no losses booked

    # status should now show open_count=0 (all closed via paper Fill).
    status_result = runner.invoke(app, ["status"])
    assert status_result.exit_code == 0
    status_payload = json.loads(status_result.stdout)
    assert status_payload["metrics"]["open_positions"] == 0
    assert status_payload["metrics"]["total_realized_pnl"] == 0.0


def test_close_subcommand_books_realized_pnl(isolated_tracker):
    """run (without paper-close) leaves position open → `close` books PnL.

    Same-process flow (CliRunner shares module state across invokes)."""
    # mid<0.5 → BUY leg (RoutingEngine sets action by price-vs-0.5).
    # We want a BUY position so close at higher price → profit.
    run_result = runner.invoke(
        app,
        [
            "run", "--mid", "0.40", "--stake", "100", "--profit-pct", "3.0",
            "--legs", "1", "--retry-delay", "0",
        ],
    )
    assert run_result.exit_code == 0
    # Without --paper-close, position stays open in the tracker. The tracker
    # uses market.condition_id as the position market_id (RoutingEngine
    # sets `leg.asset = market.condition_id` in _build_execution_legs).
    open_positions = list(isolated_tracker.open_positions())
    assert len(open_positions) == 1, f"expected 1 open position; got {len(open_positions)}"
    open_pos = open_positions[0]
    open_market_id = open_pos.market_id
    open_stake = open_pos.stake
    status_result = runner.invoke(app, ["status"])
    status_position = json.loads(status_result.stdout)["open_positions"][0]
    assert status_position["quantity"] == open_pos.quantity
    assert status_position["cost_basis"] == open_pos.cost_basis
    assert status_position["stake"] == open_pos.quantity

    close_result = runner.invoke(
        app,
        [
            "close",
            "--market-id", open_market_id,
            "--exit-price", str(open_pos.entry_price + 0.05),  # 5¢ profit
        ],
    )
    assert close_result.exit_code == 0, f"close exit={close_result.exit_code}\n{close_result.output}"
    close_payload = json.loads(close_result.stdout)
    # BUY profit: stake × (exit - entry) = stake × 0.05
    expected_pnl = open_stake * 0.05
    assert close_payload["realized_pnl"] > 0
    # Approximate compare to tolerate float arithmetic.
    assert abs(close_payload["realized_pnl"] - expected_pnl) < 1e-3, (
        f"realized_pnl={close_payload['realized_pnl']}, expected≈{expected_pnl}"
    )


def test_close_unknown_market_exits_nonzero():
    result = runner.invoke(
        app,
        ["close", "--market-id", "nonexistent", "--exit-price", "0.5"],
    )
    assert result.exit_code == 1, f"expected exit=1 for unknown market; got {result.exit_code}"
    assert "no open position" in result.output.lower()
