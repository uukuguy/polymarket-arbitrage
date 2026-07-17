from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from pathlib import Path

ROOT = Path(__file__).parents[2]


def _cli(
    *args: str,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["uv", "run", "python", "-m", "polyarb.cli_arbitrage", *args],
        cwd=ROOT,
        env={**os.environ, **(env or {})},
        text=True,
        capture_output=True,
        check=False,
    )


def test_run_status_close_status_across_four_processes(tmp_path) -> None:
    path = tmp_path / "positions.db"

    run = _cli(
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
        "--db-path",
        str(path),
    )
    assert run.returncode == 0, run.stderr

    status = _cli("status", "--db-path", str(path))
    assert status.returncode == 0, status.stderr
    before = json.loads(status.stdout)
    assert before["metrics"]["open_positions"] == 1
    assert before["open_positions"][0]["market_id"] == "cond-0"

    close = _cli(
        "close",
        "--market-id",
        "cond-0",
        "--exit-price",
        "0.50",
        "--db-path",
        str(path),
    )
    assert close.returncode == 0, close.stderr
    closed = json.loads(close.stdout)
    assert closed["realized_pnl"] > 0

    final_status = _cli("status", "--db-path", str(path))
    assert final_status.returncode == 0, final_status.stderr
    after = json.loads(final_status.stdout)
    assert after["metrics"]["open_positions"] == 0
    assert after["metrics"]["total_realized_pnl"] == closed["realized_pnl"]


def test_close_receipt_recovers_lost_response_across_processes(tmp_path) -> None:
    path = tmp_path / "positions.db"
    run_args = (
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
        "--db-path",
        str(path),
    )

    opened = _cli(*run_args, "--signal-id", "receipt-open-001")
    assert opened.returncode == 0, opened.stderr

    first_close = _cli(
        "close",
        "--market-id",
        "cond-0",
        "--exit-price",
        "0.50",
        "--operation-id",
        "close-001",
        "--db-path",
        str(path),
    )
    assert first_close.returncode == 0, first_close.stderr
    # Deliberately do not parse first_close.stdout: simulate a lost response.

    replay = _cli(
        "close",
        "--market-id",
        "cond-0",
        "--exit-price",
        "0.50",
        "--operation-id",
        "close-001",
        "--db-path",
        str(path),
    )
    assert replay.returncode == 0, replay.stderr
    recovered = json.loads(replay.stdout)
    assert recovered["operation_id"] == "close-001"
    assert recovered["replayed"] is True
    assert recovered["retry_safe"] is True
    assert recovered["realized_pnl"] == 10.0
    assert recovered["total_realized_pnl"] == 10.0

    status = _cli("status", "--db-path", str(path))
    assert status.returncode == 0, status.stderr
    state = json.loads(status.stdout)
    assert state["metrics"]["balance"] == 1010.0
    assert state["metrics"]["total_realized_pnl"] == 10.0
    assert state["metrics"]["open_positions"] == 0
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM m2_applied_operations WHERE operation_id = ?",
            ("close-001",),
        ).fetchone()[0] == 1

    conflict = _cli(
        "close",
        "--market-id",
        "cond-other",
        "--exit-price",
        "0.50",
        "--operation-id",
        "close-001",
        "--db-path",
        str(path),
    )
    assert conflict.returncode != 0
    assert "operation identity conflict" in conflict.stderr

    reopened = _cli(*run_args, "--signal-id", "receipt-open-002")
    assert reopened.returncode == 0, reopened.stderr
    second_close = _cli(
        "close",
        "--market-id",
        "cond-0",
        "--exit-price",
        "0.50",
        "--operation-id",
        "close-002",
        "--db-path",
        str(path),
    )
    assert second_close.returncode == 0, second_close.stderr
    assert json.loads(second_close.stdout)["total_realized_pnl"] == 20.0
    with sqlite3.connect(path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM m2_applied_operations "
            "WHERE operation_type = 'close'"
        ).fetchone()[0] == 2


def test_close_without_caller_identity_reports_not_retry_safe(tmp_path) -> None:
    path = tmp_path / "positions.db"
    opened = _cli(
        "run",
        "--mid",
        "0.40",
        "--stake",
        "100",
        "--legs",
        "1",
        "--retry-delay",
        "0",
        "--signal-id",
        "generated-close-id",
        "--db-path",
        str(path),
    )
    assert opened.returncode == 0, opened.stderr

    closed = _cli(
        "close",
        "--market-id",
        "cond-0",
        "--exit-price",
        "0.50",
        "--db-path",
        str(path),
    )

    assert closed.returncode == 0, closed.stderr
    response = json.loads(closed.stdout)
    assert response["operation_id"].startswith("local:operator-close:cond-0:")
    assert response["replayed"] is False
    assert response["retry_safe"] is False


def test_explicit_db_path_overrides_environment(tmp_path) -> None:
    env_path = tmp_path / "from-env.db"
    explicit_path = tmp_path / "explicit.db"

    result = _cli(
        "status",
        "--db-path",
        str(explicit_path),
        env={"POLYARB_POSITION_DB_PATH": str(env_path)},
    )

    assert result.returncode == 0, result.stderr
    assert explicit_path.exists()
    assert not env_path.exists()


def test_run_replay_with_explicit_signal_id_is_idempotent(tmp_path) -> None:
    path = tmp_path / "positions.db"
    args = (
        "run",
        "--mid",
        "0.40",
        "--stake",
        "100",
        "--legs",
        "1",
        "--retry-delay",
        "0",
        "--signal-id",
        "cli-replay-1",
        "--db-path",
        str(path),
    )

    assert _cli(*args).returncode == 0
    assert _cli(*args).returncode == 0

    status = _cli("status", "--db-path", str(path))
    assert status.returncode == 0, status.stderr
    assert json.loads(status.stdout)["metrics"]["open_positions"] == 1
    with sqlite3.connect(path) as con:
        count = con.execute("SELECT COUNT(*) FROM m2_applied_operations").fetchone()[0]
    assert count == 1


def test_corrupt_database_fails_closed(tmp_path) -> None:
    path = tmp_path / "corrupt.db"
    path.write_bytes(b"not a sqlite database")

    result = _cli("status", "--db-path", str(path))

    assert result.returncode != 0
    assert '"metrics"' not in result.stdout


def test_busy_database_respects_bounded_timeout(tmp_path) -> None:
    path = tmp_path / "busy.db"
    initialized = _cli("status", "--db-path", str(path))
    assert initialized.returncode == 0, initialized.stderr

    con = sqlite3.connect(path, isolation_level=None)
    con.execute("BEGIN IMMEDIATE")
    try:
        result = _cli(
            "run",
            "--mid",
            "0.40",
            "--stake",
            "100",
            "--legs",
            "1",
            "--retry-delay",
            "0",
            "--db-path",
            str(path),
            env={"POLYARB_POSITION_BUSY_TIMEOUT_MS": "20"},
        )
    finally:
        con.rollback()
        con.close()

    assert result.returncode != 0
    check = _cli("status", "--db-path", str(path))
    assert check.returncode == 0, check.stderr
    assert json.loads(check.stdout)["metrics"]["open_positions"] == 0
