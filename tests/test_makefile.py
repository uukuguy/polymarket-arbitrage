from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).parent.parent


def _make(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["make", *args],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_help_lists_durable_arbitrage_commands() -> None:
    result = _make("help")

    assert result.returncode == 0, result.stderr
    for target in ("eval-arb:", "run-arb:", "status-arb:", "close-arb:"):
        assert target in result.stdout
    assert "db=" in result.stdout
    assert "operation_id=" in result.stdout


def test_status_uses_the_canonical_current_state() -> None:
    result = _make("status")

    assert result.returncode == 0, result.stderr
    assert "唯一当前状态入口" in result.stdout
    assert "还不是可以投入真实资金运行的套利产品" in result.stdout
    assert "## 当前 checkout" in result.stdout


def test_post_commit_hook_never_amends_the_users_commit() -> None:
    hook = ROOT / ".githooks" / "post-commit"
    content = hook.read_text() if hook.exists() else ""

    assert "commit --amend" not in content
    assert "--no-verify" not in content


def test_arbitrage_make_targets_forward_database_path() -> None:
    database = "build/test-m2-positions.db"
    cases = (
        ("run-arb",),
        ("status-arb",),
        ("close-arb", "market_id=cond-0", "exit_price=0.5"),
    )

    for args in cases:
        result = _make("-n", *args, f"db={database}")
        assert result.returncode == 0, result.stderr
        assert f'--db-path "{database}"' in result.stdout


def test_close_arbitrage_target_forwards_operation_identity() -> None:
    result = _make(
        "-n",
        "close-arb",
        "db=build/test-m2-positions.db",
        "market_id=cond-0",
        "exit_price=0.5",
        "operation_id=close-001",
    )

    assert result.returncode == 0, result.stderr
    assert '--operation-id "close-001"' in result.stdout


def test_close_arbitrage_target_forwards_partial_fill_identity() -> None:
    result = _make(
        "-n",
        "close-arb",
        "db=build/test-m2-positions.db",
        "market_id=cond-0",
        "exit_price=0.5",
        "size=30",
        "fill_id=venue-fill-001",
    )

    assert result.returncode == 0, result.stderr
    assert 'SIZE_FLAG="--size ${size}"' in result.stdout
    assert '--fill-id "venue-fill-001"' in result.stdout


def test_close_arbitrage_target_forwards_complete_venue_truth() -> None:
    result = _make(
        "-n", "close-arb", "market_id=cond-0", "exit_price=0.99", "size=30",
        "fill_id=fill-001", "venue_cash=13.80", "venue_fee=0.30",
        "venue_status=CONFIRMED", "venue_ref=trade-001",
    )

    assert result.returncode == 0, result.stderr
    for expected in (
        '--venue-cash "13.80"', '--venue-fee "0.30"',
        '--venue-status "CONFIRMED"', '--venue-ref "trade-001"',
    ):
        assert expected in result.stdout
