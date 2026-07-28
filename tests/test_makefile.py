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


def test_opportunity_targets_are_discoverable_and_cloud_only() -> None:
    for target in (
        "build-market-map",
        "inspect-market-map",
        "scan-neg-risk-map",
        "watch-opportunities-status",
        "watch-opportunities",
        "watch-opportunity-history",
    ):
        assert f"{target}:" in _make("help").stdout
        assert "data/state.db" not in _make_recipe(target)


def test_scan_l3_seed_make_entry_is_discoverable_and_exact() -> None:
    recipe = _make_recipe("scan-l3-seed")
    help_result = _make("help")
    dry_run = _make("-n", "scan-l3-seed")

    assert "cli_observation scan --name l3-seed --verbose" in recipe
    assert help_result.returncode == 0, help_result.stderr
    assert "scan-l3-seed:" in help_result.stdout
    assert dry_run.returncode == 0, dry_run.stderr
    assert "cli_observation scan --name l3-seed --verbose" in dry_run.stdout


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


def test_local_quote_make_entries_are_safe_discoverable_and_forward_options() -> None:
    collect_recipe = _make_recipe("collect-neg-risk-quotes")
    scan_recipe = _make_recipe("scan-arb-quotes")
    unsafe = (
        r"\b(flyctl|POST|PUT|PATCH|DELETE|deploy|scale|restart|secret|schema|migrat|"
        r"order|wallet|cron)\b"
    )

    collect_entry = (
        'cli_arbitrage collect-neg-risk-quotes --db-path "$(if $(strip $(db)),$(db),data/state.db)"'
    )
    assert collect_entry in collect_recipe
    assert "cli_arbitrage scan-quotes" in scan_recipe
    assert '--db-path "$(if $(strip $(db)),$(db),data/state.db)"' in scan_recipe
    assert '--min-edge-bps "$(or $(min_edge_bps),0)"' in scan_recipe
    assert '--max-quote-age-s "$(or $(max_quote_age_s),300)"' in scan_recipe
    assert '--max-universe-age-s "$(or $(max_universe_age_s),50400)"' in scan_recipe
    assert not re.search(unsafe, collect_recipe, re.I)
    assert not re.search(unsafe, scan_recipe, re.I)

    help_result = _make("help")
    assert help_result.returncode == 0, help_result.stderr
    for target in ("collect-neg-risk-quotes:", "scan-arb-quotes:"):
        assert target in help_result.stdout

    default_collect = _make("-n", "collect-neg-risk-quotes")
    default_scan = _make("-n", "scan-arb-quotes")
    override_collect = _make("-n", "collect-neg-risk-quotes", "db=build/quotes.db")
    override_scan = _make(
        "-n",
        "scan-arb-quotes",
        "db=build/quotes.db",
        "min_edge_bps=25",
        "max_quote_age_s=30",
        "max_universe_age_s=900",
    )
    for result in (default_collect, default_scan, override_collect, override_scan):
        assert result.returncode == 0, result.stderr
    assert '--db-path "data/state.db"' in default_collect.stdout
    assert '--db-path "data/state.db"' in default_scan.stdout
    assert '--db-path "build/quotes.db"' in override_collect.stdout
    for expected in (
        '--db-path "build/quotes.db"',
        '--min-edge-bps "25"',
        '--max-quote-age-s "30"',
        '--max-universe-age-s "900"',
    ):
        assert expected in override_scan.stdout


def test_eval_local_quote_profile_is_offline_and_discoverable() -> None:
    dry_run = _make("-n", "eval-local", "profile=opportunity-feed-cadence-sla")

    assert dry_run.returncode == 0, dry_run.stderr
    assert "opportunity-feed-cadence-sla" in dry_run.stdout
    assert "eval_local" in dry_run.stdout


def test_chaos_image_check_accepts_current_and_legacy_fly_status_shapes() -> None:
    recipe = _make_recipe("chaos-l2-fly-image-check")

    assert ".Machines[0].image_ref" in recipe
    assert ".Machines[0].config.image" in recipe
    assert ".ImageRef" in recipe
    assert "$$ref.Digest // $$ref.digest" in recipe
    assert "docker run --rm --entrypoint /bin/sh" in recipe


def test_chaos_image_check_separates_observed_from_required_tools() -> None:
    """Known optional MISS results must not fail the default Python gate."""
    recipe = _make_recipe("chaos-l2-fly-image-check")

    assert 'OBSERVED_TOOLS="pkill ps kill which dig ping curl python"' in recipe
    assert 'REQUIRED_TOOLS="$(if $(strip $(required)),$(strip $(required)),python)"' in recipe
    assert "Missing optional primitives:" in recipe
    assert "Missing required primitives:" in recipe
    assert "Required primitives present:" in recipe
    assert "exit $$rc" not in recipe


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
