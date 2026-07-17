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
