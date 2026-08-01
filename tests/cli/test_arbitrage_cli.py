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
from unittest.mock import MagicMock, call

import pytest
from typer.testing import CliRunner

from polyarb import cli_arbitrage as cli_mod
from polyarb.cli_arbitrage import app
from polyarb.routing.opportunity_scanner import (
    NegRiskOpportunity,
    OpportunityLeg,
    OpportunityScanResult,
)
from polyarb.routing.position_tracker import PositionTracker

runner = CliRunner()


def test_cleanup_neg_risk_quotes_runs_bounded_single_run_transactions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    purge = MagicMock(side_effect=[1, 1, 0])
    store = MagicMock()
    store.purge_old_runs = purge
    monkeypatch.setattr(cli_mod, "NegRiskQuoteStore", MagicMock(return_value=store))

    result = runner.invoke(
        app,
        ["cleanup-neg-risk-quotes", "--db-path", "state.db", "--max-runs", "20"],
    )

    assert result.exit_code == 0
    assert json.loads(result.stdout) == {
        "deleted_runs": 2,
        "keep_last_per_status": 10,
        "max_runs": 20,
        "status": "complete",
    }
    assert purge.call_args_list == [
        call(keep_last_per_status=10, max_runs=1),
        call(keep_last_per_status=10, max_runs=1),
        call(keep_last_per_status=10, max_runs=1),
    ]


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
            "evaluate",
            "--mid",
            "0.5",
            "--stake",
            "100",
            "--profit-pct",
            "0.5",
            "--min-threshold-pct",
            "2.0",
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
            "run",
            "--mid",
            "0.45",
            "--stake",
            "500",
            "--profit-pct",
            "3.0",
            "--legs",
            "2",
            "--retry-delay",
            "0",
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


@pytest.mark.parametrize(
    "args",
    [
        ["--venue-cash", "13.80"],
        ["--venue-cash", "13.80", "--venue-fee", "0.30"],
        [
            "--venue-cash",
            "13.80",
            "--venue-fee",
            "0.30",
            "--venue-status",
            "CONFIRMED",
            "--venue-ref",
            "trade-001",
            "--fill-id",
            "fill-001",
        ],
    ],
)
def test_close_rejects_incomplete_or_nonreconstructible_venue_truth(args):
    result = runner.invoke(
        app,
        ["close", "--market-id", "m1", "--exit-price", "0.9", *args],
    )

    assert result.exit_code == 2
    assert "venue" in result.output.lower()


def test_close_rejects_fill_id_without_explicit_size():
    result = runner.invoke(
        app,
        [
            "close",
            "--market-id",
            "m1",
            "--exit-price",
            "0.5",
            "--fill-id",
            "fill-001",
        ],
    )

    assert result.exit_code == 2
    assert "explicit --size" in result.output


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


def test_scan_quotes_emits_diagnosable_verified_feed(
    monkeypatch,
    tmp_path,
) -> None:
    opportunity = NegRiskOpportunity(
        group_id="g1",
        snapshot_id=10,
        snapshot_age_seconds=100.0,
        sum_asks=0.95,
        gross_edge_bps=500.0,
        executable_quantity=8.0,
        gross_profit=0.4,
        legs=(
            OpportunityLeg("m1", "c1", "one", "t1", 0.4, 8.0),
            OpportunityLeg("m2", "c2", "two", "t2", 0.55, 9.0),
        ),
        quote_run_id=20,
        quote_age_seconds=10.0,
        universe_snapshot_id=10,
        universe_age_seconds=100.0,
        event_id="e1",
        membership_hash="m1",
        quality="complete-supported",
    )
    monkeypatch.setattr(
        cli_mod,
        "scan_verified_neg_risk_quote_run",
        lambda *_args, **_kwargs: OpportunityScanResult(
            opportunities=(opportunity,),
            rejections={"augmented-neg-risk-not-supported": 4},
            source_snapshot_id=10,
            universe_hash="u1",
            quote_run_id=20,
        ),
    )

    result = runner.invoke(app, ["scan-quotes"])

    assert result.exit_code == 0
    payload = json.loads(result.stdout)
    assert payload["coverage"] == "verified-standard-neg-risk"
    assert payload["source_snapshot_id"] == 10
    assert payload["universe_hash"] == "u1"
    assert payload["quote_run_id"] == 20
    assert type(payload["quote_sla_seconds"]) is int
    assert payload["quote_sla_seconds"] == 300
    assert payload["rejections"] == {"augmented-neg-risk-not-supported": 4}
    assert payload["opportunities"][0]["membership_hash"] == "m1"
    body_file = tmp_path / "scan-quotes.json"
    body_file.write_text(result.stdout)

    diagnosis = runner.invoke(
        app,
        [
            "diagnose-feed",
            "--http-status",
            "200",
            "--body-file",
            str(body_file),
        ],
    )

    assert diagnosis.exit_code == 0
    assert json.loads(diagnosis.stdout)["kind"] == "available-opportunities"


def test_scan_quotes_rejects_noncanonical_quote_sla(monkeypatch) -> None:
    scanner_called = False

    def scan_should_not_run(*_args, **_kwargs):
        nonlocal scanner_called
        scanner_called = True
        raise AssertionError("scanner must not run for a noncanonical quote SLA")

    monkeypatch.setattr(cli_mod, "scan_verified_neg_risk_quote_run", scan_should_not_run)

    result = runner.invoke(
        app,
        ["scan-quotes", "--max-quote-age-s", "299"],
    )

    assert result.exit_code == 2
    assert "max_quote_age_s must equal the canonical 300-second SLA" in result.output
    assert scanner_called is False


# ──────────────────────────────────────────────────────────────────────────
# T5 — paper_close lifecycle + close subcommand
# ──────────────────────────────────────────────────────────────────────────


def test_run_paper_close_closes_lifecycle_zero_pnl():
    """`run --paper-close` synths Fill at estimated_price → open then close
    same-process → tracker has 0 open positions, realized_pnl ≈ 0."""
    result = runner.invoke(
        app,
        [
            "run",
            "--mid",
            "0.45",
            "--stake",
            "500",
            "--profit-pct",
            "3.0",
            "--legs",
            "2",
            "--retry-delay",
            "0",
            "--paper-close",
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
            "run",
            "--mid",
            "0.40",
            "--stake",
            "100",
            "--profit-pct",
            "3.0",
            "--legs",
            "1",
            "--retry-delay",
            "0",
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
            "--market-id",
            open_market_id,
            "--exit-price",
            str(open_pos.entry_price + 0.05),  # 5¢ profit
        ],
    )
    assert close_result.exit_code == 0, (
        f"close exit={close_result.exit_code}\n{close_result.output}"
    )
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
