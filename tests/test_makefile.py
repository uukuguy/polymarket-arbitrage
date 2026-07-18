from __future__ import annotations

import re
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


def _make_recipe(target: str) -> str:
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(rf"(?m)^{re.escape(target)}:\n(?P<recipe>(?:\t.*\n)+)", makefile)
    assert match is not None, f"{target} target must exist"
    return match.group("recipe")


def test_help_lists_durable_arbitrage_commands() -> None:
    result = _make("help")

    assert result.returncode == 0, result.stderr
    for target in ("eval-arb:", "run-arb:", "status-arb:", "close-arb:"):
        assert target in result.stdout
    assert "db=" in result.stdout
    assert "operation_id=" in result.stdout
    assert "scan-arb-live:" in result.stdout


def test_opportunity_diagnosis_target_is_read_only_and_preserves_body() -> None:
    recipe = _make_recipe("diagnose-arb-feed-prod")
    assert "curl --disable --request GET" in recipe
    assert '-o "$$BODY" -w "%{http_code}"' in recipe
    assert "cli_arbitrage diagnose-feed" in recipe
    assert "polyarb-l1.fly.dev/arbitrage/opportunities" in recipe
    assert not re.search(
        r"\b(flyctl|POST|deploy|scale|restart|secret|schema|migrat|chaos)\b",
        recipe,
        re.I,
    )


def test_diagnose_arb_feed_make_entry_is_listed_and_dry_runs() -> None:
    help_result = _make("help")
    dry_run = _make("-n", "diagnose-arb-feed-prod")

    assert help_result.returncode == 0, help_result.stderr
    assert "diagnose-arb-feed-prod:" in help_result.stdout
    assert dry_run.returncode == 0, dry_run.stderr
    assert "curl --disable --request GET" in dry_run.stdout
    assert "https://polyarb-l1.fly.dev/arbitrage/opportunities?min_edge_bps=0" in dry_run.stdout


def test_status_uses_the_canonical_current_state() -> None:
    result = _make("status")

    assert result.returncode == 0, result.stderr
    assert "唯一当前状态入口" in result.stdout
    assert "还不是可以投入真实资金运行的套利产品" in result.stdout
    assert "## 当前 checkout" in result.stdout


def test_climb_hook_never_amends_the_users_commit() -> None:
    post_hook = ROOT / ".githooks" / "post-commit"
    hook = ROOT / "tools" / "climb" / "hooks" / "pre-commit"
    content = (post_hook.read_text() if post_hook.exists() else "") + hook.read_text()

    assert "commit --amend" not in content
    assert "--no-verify" not in content
    assert "git commit" not in content


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
        "-n",
        "close-arb",
        "market_id=cond-0",
        "exit_price=0.99",
        "size=30",
        "fill_id=fill-001",
        "venue_cash=13.80",
        "venue_fee=0.30",
        "venue_status=CONFIRMED",
        "venue_ref=trade-001",
    )

    assert result.returncode == 0, result.stderr
    for expected in (
        '--venue-cash "13.80"',
        '--venue-fee "0.30"',
        '--venue-status "CONFIRMED"',
        '--venue-ref "trade-001"',
    ):
        assert expected in result.stdout
