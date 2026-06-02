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

from typer.testing import CliRunner

from polyarb.cli_arbitrage import app

runner = CliRunner()


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
    assert isinstance(payload["open_positions"], list)
    assert "open_positions" in payload["metrics"]
