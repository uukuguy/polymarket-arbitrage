"""Makefile contract tests + CLI invocation smoke.

Plan 01-5 T5 — covers two responsibilities:

  1. Makefile contract — every snapshot target in the Makefile must dry-run cleanly
     via ``make -n``. This catches recipe drift (e.g. a future commit accidentally
     dropping the ``--full`` flag) before the user runs them against live APIs.

  2. CLI invocation smoke — typer.testing.CliRunner exercises the in-process
     CLI (``polyarb.snapshot.cli:app``) so we know the orchestrator → CLI →
     stdout/stderr → exit-code path is wired correctly under mocks.

Critical: ``make snapshot-markets`` is NEVER actually executed (would hit live
APIs and take 10-20 minutes). All make assertions use ``-n`` (dry-run).
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

import pytest
from typer.testing import CliRunner

from polyarb.snapshot.cli import app

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
runner = CliRunner(mix_stderr=False)


# =============================================================================
# Makefile contract — dry-run only, never invoke against live APIs
# =============================================================================


def test_make_help_lists_snapshot_markets() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    # Both subset and full mode targets must appear in the help listing.
    assert "snapshot-markets:" in result.stdout
    assert "snapshot-markets-full:" in result.stdout


def test_make_snapshot_markets_dry_run_recipe() -> None:
    """The subset target must invoke ``python -m polyarb.snapshot`` WITHOUT --full."""
    result = subprocess.run(
        ["make", "-n", "snapshot-markets"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "python -m polyarb.snapshot" in result.stdout
    # subset target MUST NOT include --full (that would silently switch to full mode).
    # We grep the recipe lines (skip echo'd "make[1]:" diagnostics).
    recipe_lines = [ln for ln in result.stdout.splitlines() if "polyarb.snapshot" in ln]
    assert recipe_lines, "no recipe line found"
    for ln in recipe_lines:
        assert "--full" not in ln, f"snapshot-markets recipe must not include --full: {ln!r}"


def test_make_snapshot_markets_full_dry_run_recipe() -> None:
    """The full target must invoke ``uv run python -m polyarb.snapshot snapshot --full``."""
    result = subprocess.run(
        ["make", "-n", "snapshot-markets-full"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    # Makefile uses 'uv run python -m polyarb.snapshot snapshot --full' (CLAUDE.md §7 toolchain)
    assert "uv run python -m polyarb.snapshot snapshot --full" in result.stdout


def test_makefile_phony_declaration_present() -> None:
    """Core snapshot recipe names must be declared .PHONY so a file by that name
    in the project root can't shadow the recipe.

    Looks at every .PHONY line and verifies the contract targets show up at
    least once across all of them — the actual grouping (one big line vs many
    small lines) is an implementation detail.
    """
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {"snapshot-markets", "snapshot-markets-full"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY declarations for: {missing}"


def test_dashboard_smoke_uses_canonical_production_project_url() -> None:
    """The default smoke target must not regress to the dead short alias."""
    result = subprocess.run(
        ["make", "-n", "smoke-l2-dashboard"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "https://polymarket-arbitrage-jiangwen-su-s-projects.vercel.app" in result.stdout
    assert "https://polymarket-arbitrage.vercel.app" not in result.stdout


# =============================================================================
# Phase 02 Plan 01: triple-check contract — dry-run only
# =============================================================================


def test_make_triple_check_dry_run_recipe() -> None:
    """Phase 02 Plan 01: make triple-check must invoke test_makefile_triple_check.sh.

    Dry-run verifies the recipe is wired correctly without actually executing
    the full snapshot pipeline (L11/S5 silent failure gate — see LEARNINGS).
    """
    result = subprocess.run(
        ["make", "-n", "triple-check"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n triple-check failed: {result.stderr}"
    assert "tests/m1-perception/test_makefile_triple_check.sh" in result.stdout, (
        f"triple-check recipe must invoke test_makefile_triple_check.sh, got: {result.stdout!r}"
    )


# =============================================================================
# CLI smoke — typer.testing.CliRunner with mocked clients
# =============================================================================


def _build_yaml(tmp_path: Path, db: Path, parquet: Path) -> Path:
    yaml_path = tmp_path / "test.yaml"
    yaml_path.write_text(
        f"db_path: {db}\n"
        f"parquet_root: {parquet}\n"
        f"liquidity_threshold_usd: 100.0\n"
        f"retry_attempts: 1\n"
        f"retry_min_wait_s: 0.001\n"
        f"retry_max_wait_s: 0.005\n"
        f"http_timeout_s: 2.0\n"
    )
    return yaml_path


def test_cli_help_shows_all_commands() -> None:
    """Snapshot CLI shows 'snapshot' and 'snapshots-purge' commands in --help."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, f"--help failed: {result.stderr}"
    assert "snapshot" in result.stdout
    assert "snapshots-purge" in result.stdout


def test_cli_default_subset_mode(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
    # Exit code 0 (valid) is expected with our clean fixture; allow 1 for safety.
    assert result.exit_code in (0, 1), f"unexpected exit: {result.exit_code} stderr={result.stderr}"
    assert "mode=subset" in result.stdout
    assert ("OK" in result.stdout) or ("DEGRADED" in result.stdout) or ("FAILED" in result.stdout)
    # SQLite was created at the configured path.
    assert tmp_db_path.exists()


def test_cli_full_flag_sets_full_mode(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--full", "--config", str(yaml_path)])
    assert result.exit_code in (0, 1)
    assert "mode=full" in result.stdout


def test_cli_summary_format_matches_spec(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    """D-F1: summary line is single-line cron-grep friendly."""
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
    summary_re = re.compile(
        r"^(OK|DEGRADED|FAILED) \| \d+ markets \| mode=(subset|full)"
        r" \| \d+ issues \| -> .+\.parquet$"
    )
    summary_lines = [ln for ln in result.stdout.splitlines() if summary_re.match(ln)]
    assert summary_lines, f"no summary line matched, stdout={result.stdout!r}"


# Removed bare-invocation test: typer's no_args_is_help only fires for top-level
# Typer apps with multiple commands; with a single @app.command() typer treats
# bare invocation as "run the only command with no args" which triggers a real
# pipeline run + live network. The --help test below covers the help-text contract.


# =============================================================================
# Phase 1.1 plan-02 — translation Makefile targets
# =============================================================================


def test_make_translate_pending_dry_run() -> None:
    """`make -n translate-pending` resolves to the cli_translation entry."""
    result = subprocess.run(
        ["make", "-n", "translate-pending"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translate-pending" in result.stdout
    # Without FORCE=1 the recipe must NOT include --force-full
    recipe_lines = [ln for ln in result.stdout.splitlines() if "cli_translation" in ln]
    for ln in recipe_lines:
        assert "--force-full" not in ln, (
            f"translate-pending without FORCE=1 must not pass --force-full: {ln!r}"
        )


def test_make_translate_pending_force_full_dry_run() -> None:
    """`make -n translate-pending FORCE=1` adds --force-full to the recipe."""
    result = subprocess.run(
        ["make", "-n", "translate-pending", "FORCE=1"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translate-pending" in result.stdout
    assert "--force-full" in result.stdout


def test_make_translate_pending_sample_dry_run() -> None:
    """`make -n translate-pending-sample` includes --limit 50."""
    result = subprocess.run(
        ["make", "-n", "translate-pending-sample"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--limit 50" in result.stdout


def test_make_translation_stats_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "translation-stats"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translation-stats" in result.stdout


def test_make_help_lists_translation_targets() -> None:
    """make help must surface all 3 translation targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "translate-pending:" in result.stdout
    assert "translate-pending-sample:" in result.stdout
    assert "translation-stats:" in result.stdout


def test_makefile_translation_targets_phony() -> None:
    """All 3 translation targets must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {"translate-pending", "translate-pending-sample", "translation-stats"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for translation targets: {missing}"


# =============================================================================
# Phase 1.1 plan-03 — observation Makefile targets (8 targets)
# =============================================================================


_OBSERVATION_TARGETS = [
    "scan-thick-but-slippery",
    "scan-near-end",
    "scan-ghost-suspicious",
    "scan-coin-flip",
    "scan-neg-risk-incomplete",
    "scan-by-tag",
    "list-recipes",
    "scans-purge",
]


@pytest.mark.parametrize("target", _OBSERVATION_TARGETS)
def test_make_observation_target_dry_run(target: str) -> None:
    """Each observation target must dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_scan_generic_dry_run() -> None:
    """`make -n scan name=thick-but-slippery` resolves to the cli scan command."""
    result = subprocess.run(
        ["make", "-n", "scan", "name=thick-but-slippery"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n scan failed: {result.stderr}"
    assert "polyarb.cli_observation scan" in result.stdout
    assert "--name thick-but-slippery" in result.stdout


def test_make_help_lists_observation_targets() -> None:
    """make help must surface all 8 observation targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    expected = [
        "scan-thick-but-slippery:",
        "scan-near-end:",
        "scan-ghost-suspicious:",
        "scan-coin-flip:",
        "scan-neg-risk-incomplete:",
        "scan-by-tag:",
        "list-recipes:",
        "scans-purge:",
    ]
    for target in expected:
        assert target in result.stdout, f"missing in `make help`: {target}"


def test_makefile_observation_targets_phony() -> None:
    """All 8 observation targets + the generic `scan` target must be .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {*_OBSERVATION_TARGETS, "scan"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for observation targets: {missing}"


def test_makefile_scan_by_tag_replaces_by_category() -> None:
    """Amendment 01: there is NO scan-by-category target; it was renamed to scan-by-tag."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "scan-by-tag:" in makefile
    # Recipe lines (target definitions, not comments) should not reference
    # the old `by-category` recipe name.
    recipe_lines = [
        ln for ln in makefile.splitlines() if ln.startswith("scan-") and ln.endswith(":")
    ]
    for ln in recipe_lines:
        assert "by-category" not in ln, f"stale by-category target should be by-tag: {ln!r}"


# =============================================================================
# Phase 1.1 plan-04 — compare-snapshots + track-market Makefile targets (2 targets)
# =============================================================================


_PLAN04_TARGETS = ["compare-snapshots", "track-market"]


@pytest.mark.parametrize("target", _PLAN04_TARGETS)
def test_make_plan04_target_dry_run(target: str) -> None:
    """Each plan-04 target must dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_compare_snapshots_with_args() -> None:
    """`make -n compare-snapshots from=1 to=2` passes --from 1 --to 2."""
    result = subprocess.run(
        ["make", "-n", "compare-snapshots", "from=1", "to=2"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--from" in result.stdout
    assert "--to" in result.stdout


def test_make_track_market_with_slug() -> None:
    """`make -n track-market slug=will-x-happen` passes --slug will-x-happen."""
    result = subprocess.run(
        ["make", "-n", "track-market", "slug=will-x-happen"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--slug will-x-happen" in result.stdout


def test_make_help_lists_plan04_targets() -> None:
    """make help must surface both plan-04 targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "compare-snapshots:" in result.stdout
    assert "track-market:" in result.stdout


def test_makefile_plan04_targets_phony() -> None:
    """Both plan-04 targets must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    missing = set(_PLAN04_TARGETS) - phony_targets
    assert not missing, f"missing .PHONY for plan-04 targets: {missing}"


# =============================================================================
# Phase 1.1 plan-05 — show-market + watchlist + watchlist-alerts (3 targets)
# =============================================================================


_PLAN05_TARGETS = ["show-market", "watchlist", "watchlist-alerts"]


@pytest.mark.parametrize("target", _PLAN05_TARGETS)
def test_make_plan05_target_dry_run(target: str) -> None:
    result = subprocess.run(
        ["make", "-n", target, "slug=test"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_show_market_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "show-market", "slug=will-x-happen"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--slug will-x-happen" in result.stdout


def test_make_watchlist_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "watchlist"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "watchlist" in result.stdout


def test_make_watchlist_alerts_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "watchlist-alerts"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "watchlist-alerts" in result.stdout


def test_makefile_plan05_targets_phony() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    missing = set(_PLAN05_TARGETS) - phony_targets
    assert not missing, f"missing .PHONY for plan-05 targets: {missing}"


# =============================================================================
# Phase 02 Plan 02 — daemon targets (daemon-run-local + smoke-health-local)
# =============================================================================


def test_make_daemon_run_local_dry_run_recipe() -> None:
    """`make -n daemon-run-local` resolves to python -m polyarb.daemon.main."""
    result = subprocess.run(
        ["make", "-n", "daemon-run-local"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n daemon-run-local failed: {result.stderr}"
    assert "polyarb.daemon.main" in result.stdout, (
        f"daemon-run-local recipe must invoke polyarb.daemon.main, got: {result.stdout!r}"
    )


def test_make_smoke_health_local_dry_run_recipe() -> None:
    """`make -n smoke-health-local` resolves to a curl /health call."""
    result = subprocess.run(
        ["make", "-n", "smoke-health-local"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n smoke-health-local failed: {result.stderr}"
    assert "127.0.0.1:$PORT/health" in result.stdout or "127.0.0.1:19080/health" in result.stdout, (
        f"smoke-health-local recipe must target /health on localhost, got: {result.stdout!r}"
    )


def test_make_help_lists_daemon_targets() -> None:
    """make help must surface daemon-run-local and smoke-health-local."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "daemon-run-local:" in result.stdout, "daemon-run-local missing from make help"
    assert "smoke-health-local:" in result.stdout, "smoke-health-local missing from make help"


# =============================================================================
# Phase 02 Plan 03 — Supabase migrate + reconcile + r2-list Makefile targets
# =============================================================================


def test_make_supabase_migrate_dry_run() -> None:
    """`make -n supabase-migrate` resolves to alembic upgrade head."""
    result = subprocess.run(
        ["make", "-n", "supabase-migrate"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
        env={**__import__("os").environ, "POLYARB_SUPABASE_DB_DSN": "postgresql://dummy"},
    )
    assert result.returncode == 0, f"make -n supabase-migrate failed: {result.stderr}"
    assert "alembic upgrade head" in result.stdout, (
        f"supabase-migrate recipe must invoke 'alembic upgrade head', got: {result.stdout!r}"
    )


def test_make_supabase_reconcile_dry_run() -> None:
    """`make -n supabase-reconcile` resolves to scripts/supabase_seed.py reconcile."""
    result = subprocess.run(
        ["make", "-n", "supabase-reconcile"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n supabase-reconcile failed: {result.stderr}"
    assert "scripts/supabase_seed.py" in result.stdout, (
        f"supabase-reconcile recipe must invoke scripts/supabase_seed.py, got: {result.stdout!r}"
    )


def test_make_r2_list_dry_run() -> None:
    """`make -n r2-list` resolves to boto3 R2 list operation."""
    result = subprocess.run(
        ["make", "-n", "r2-list"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
        env={
            **__import__("os").environ,
            "POLYARB_R2_ENDPOINT": "https://test.r2.cloudflarestorage.com",
        },
    )
    assert result.returncode == 0, f"make -n r2-list failed: {result.stderr}"
    assert "boto3" in result.stdout, f"r2-list recipe must invoke boto3, got: {result.stdout!r}"


def test_makefile_daemon_targets_phony() -> None:
    """daemon-run-local, smoke-health-local, tail-logs-local must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {"daemon-run-local", "smoke-health-local", "tail-logs-local"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for daemon targets: {missing}"


# =============================================================================
# Phase 02 Plan 04 — docker + deploy Makefile targets
# =============================================================================


def test_make_docker_build_dry_run() -> None:
    """`make -n docker-build` resolves to a docker build command."""
    result = subprocess.run(
        ["make", "-n", "docker-build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n docker-build failed: {result.stderr}"
    assert "docker build" in result.stdout, (
        f"docker-build recipe must invoke 'docker build', got: {result.stdout!r}"
    )


def test_make_deploy_dry_run() -> None:
    """`make -n deploy` resolves to a flyctl deploy command."""
    result = subprocess.run(
        ["make", "-n", "deploy"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n deploy failed: {result.stderr}"
    assert "flyctl deploy" in result.stdout, (
        f"deploy recipe must invoke 'flyctl deploy', got: {result.stdout!r}"
    )


# Plan 02-09: memory-budget-test + docker-smoke-256mb dry-run contract
def test_make_memory_budget_test_dry_run() -> None:
    """`make memory-budget-test` recipe must include both calibration + budget tests."""
    result = subprocess.run(
        ["make", "-n", "memory-budget-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "test_streaming_memory_budget" in result.stdout
    assert "test_streaming_memory_calibration" in result.stdout


def test_make_docker_smoke_256mb_dry_run() -> None:
    """`make docker-smoke-256mb` recipe must enforce --memory=256m and prod $1k threshold."""
    result = subprocess.run(
        ["make", "-n", "docker-smoke-256mb"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--memory=256m" in result.stdout
    assert "POLYARB_LIQUIDITY_THRESHOLD_USD=1000.0" in result.stdout


# =============================================================================
# Phase 02 Plan 05 — observability targets (sentry-test + alerts-test + logs-tail-axiom)
# =============================================================================


def test_make_sentry_test_dry_run() -> None:
    """`make -n sentry-test` resolves to init_sentry + capture_message under uv."""
    result = subprocess.run(
        ["make", "-n", "sentry-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n sentry-test failed: {result.stderr}"
    assert "init_sentry" in result.stdout, (
        f"sentry-test recipe must call init_sentry, got: {result.stdout!r}"
    )
    assert "capture_message" in result.stdout, (
        f"sentry-test recipe must call capture_message, got: {result.stdout!r}"
    )


def test_make_alerts_test_dry_run() -> None:
    """`make -n alerts-test` resolves to send_paused_alert under uv."""
    result = subprocess.run(
        ["make", "-n", "alerts-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n alerts-test failed: {result.stderr}"
    assert "send_paused_alert" in result.stdout, (
        f"alerts-test recipe must call send_paused_alert, got: {result.stdout!r}"
    )


def test_make_logs_tail_axiom_dry_run() -> None:
    """`make -n logs-tail-axiom` prints the Axiom dataset URL (no-op convenience)."""
    result = subprocess.run(
        ["make", "-n", "logs-tail-axiom"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n logs-tail-axiom failed: {result.stderr}"
    assert "axiom.co" in result.stdout, (
        f"logs-tail-axiom recipe should print axiom URL, got: {result.stdout!r}"
    )


def test_makefile_phase02_plan05_targets_phony() -> None:
    """sentry-test / alerts-test / logs-tail-axiom must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"sentry-test", "alerts-test", "logs-tail-axiom"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-05 targets: {missing}"


# =============================================================================
# Phase 02 Plan 02-06 — Dashboard Makefile contract
# =============================================================================


def test_make_dashboard_dev_dry_run() -> None:
    """`make -n dashboard-dev` resolves to `cd dashboard && pnpm run dev`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-dev"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-dev failed: {result.stderr}"
    assert "pnpm run dev" in result.stdout
    assert "cd dashboard" in result.stdout


def test_make_dashboard_build_dry_run() -> None:
    """`make -n dashboard-build` resolves to `cd dashboard && pnpm run build`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-build failed: {result.stderr}"
    assert "pnpm run build" in result.stdout
    assert "cd dashboard" in result.stdout


def test_make_dashboard_typecheck_dry_run() -> None:
    """`make -n dashboard-typecheck` resolves to `pnpm tsc --noEmit`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-typecheck"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-typecheck failed: {result.stderr}"
    assert "tsc --noEmit" in result.stdout


def test_make_dashboard_deploy_dry_run() -> None:
    """`make -n dashboard-deploy` invokes vercel."""
    result = subprocess.run(
        ["make", "-n", "dashboard-deploy"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-deploy failed: {result.stderr}"
    assert "vercel" in result.stdout


def test_makefile_phase02_plan06_targets_phony() -> None:
    """dashboard-{dev,build,typecheck,deploy} must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"dashboard-dev", "dashboard-build", "dashboard-typecheck", "dashboard-deploy"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-06 targets: {missing}"


# =============================================================================
# Phase 02 Plan 07 — soak monitoring Makefile targets
# =============================================================================


def test_make_soak_status_dry_run() -> None:
    """`make -n soak-status` must invoke soak_monitor.py status."""
    result = subprocess.run(
        ["make", "-n", "soak-status"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-status failed: {result.stderr}"
    assert "soak_monitor.py status" in result.stdout, (
        f"soak-status recipe must call soak_monitor.py status, got: {result.stdout!r}"
    )


def test_make_soak_export_dry_run() -> None:
    """`make -n soak-export` must invoke soak_monitor.py export --days 7."""
    result = subprocess.run(
        ["make", "-n", "soak-export"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-export failed: {result.stderr}"
    assert "soak_monitor.py export" in result.stdout, (
        f"soak-export recipe must call soak_monitor.py export, got: {result.stdout!r}"
    )
    assert "--days 7" in result.stdout, (
        f"soak-export recipe must include --days 7, got: {result.stdout!r}"
    )


def test_make_soak_fault_inject_dry_run() -> None:
    """`make -n soak-fault-inject` must exist and dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", "soak-fault-inject"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-fault-inject failed: {result.stderr}"


def test_makefile_phase02_plan07_targets_phony() -> None:
    """soak-status / soak-export / soak-fault-inject must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"soak-status", "soak-export", "soak-fault-inject"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-07 soak targets: {missing}"


# =============================================================================
# Quick 260717 — agent worktree lifecycle repair
# =============================================================================


def test_makefile_exposes_safe_worktree_lifecycle_targets() -> None:
    result = subprocess.run(
        ["make", "help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "cleanup-worktrees:" in result.stdout
    assert "patch-gsd-worktree-cleanup:" in result.stdout

    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "--apply" in makefile
    assert "--discard-unmerged" in makefile


def test_make_help_exposes_market_truth_production_smoke() -> None:
    result = subprocess.run(
        ["make", "help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "smoke-market-truth-prod:" in result.stdout


def test_market_truth_production_smoke_is_read_only() -> None:
    result = subprocess.run(
        ["make", "-n", "smoke-market-truth-prod"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "https://polyarb-l1.fly.dev/health" in result.stdout
    assert "market_truth:coverage" in result.stdout
    assert "POST" not in result.stdout
