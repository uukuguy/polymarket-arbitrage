from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT))

import scripts.check_m1_manual as manual_checker  # noqa: E402
from scripts.check_m1_manual import (  # noqa: E402
    L3_CREDENTIAL_PROOF_TARGETS,
    L3_LOCAL_MUTATION_MAKE_TARGETS,
    L3_MUTATION_MAKE_TARGETS,
    L3_READ_ONLY_MAKE_TARGETS,
    M1_MAKE_TARGETS,
    MANUAL,
    _decode_nul_paths,
    classify_staged_impact,
    main,
    manual_sync_is_meaningful,
    validate_manual,
)

HEADINGS = tuple(f"## {n}. " for n in range(1, 11))


def test_canonical_entry_points_link_the_manual() -> None:
    expected = "M1-市场感知平台使用手册.md"
    assert expected in (ROOT / "README.md").read_text()
    assert "../M1-市场感知平台使用手册.md" in (ROOT / "docs/learning/00-INDEX.md").read_text()
    assert "../docs/M1-市场感知平台使用手册.md" in (ROOT / ".planning/CURRENT.md").read_text()


def _valid_manual() -> str:
    sections = "\n".join(f"## {n}. section-{n}\nbody" for n in range(1, 11))
    l3_targets = "\n".join(
        f"`make {target}`"
        for target in (
            *L3_READ_ONLY_MAKE_TARGETS,
            *L3_LOCAL_MUTATION_MAKE_TARGETS,
            *L3_MUTATION_MAKE_TARGETS,
            *L3_CREDENTIAL_PROOF_TARGETS,
        )
    )
    return f"""# M1 市场感知平台使用手册

> 最后核验：2026-07-18
> 动态状态：`.planning/CURRENT.md`

{sections}

| 能力 | 状态 | 用途 | 数据源 | 验证方法 | 已知限制 | 禁止用途 |
|---|---|---|---|---|---|---|
| L1 | 已验证可用 | snapshot | Gamma+CLOB | `make snapshot-status` | local only | live order |

[CURRENT](../.planning/CURRENT.md)
{l3_targets}
<!-- m1-contract: health=snapshot:last_success_age_seconds file=src/polyarb/http/health.py -->
<!-- m1-contract: health=event_bus:cursor_lag file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=ws:last_event_age_seconds file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=mirror:l2_tob_age_seconds file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: health=l3:active_count file=src/polyarb/http/l2_health.py -->
<!-- m1-contract: route=/status file=dashboard/app/status/page.tsx -->
<!-- m1-contract: route=/candidates file=dashboard/app/candidates/page.tsx -->
<!-- m1-contract: route=/signals file=dashboard/app/signals/page.tsx -->
<!-- m1-contract: route=/l3/[asset_id] file=dashboard/app/l3/[asset_id]/page.tsx -->
"""


def _repo(tmp_path: Path) -> Path:
    (tmp_path / "src/polyarb/http").mkdir(parents=True)
    for path in ("status", "candidates", "signals", "l3/[asset_id]"):
        (tmp_path / "dashboard/app" / path).mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    (tmp_path / ".planning").mkdir()
    make_targets = (
        "snapshot-status",
        *L3_READ_ONLY_MAKE_TARGETS,
        *L3_LOCAL_MUTATION_MAKE_TARGETS,
        *L3_MUTATION_MAKE_TARGETS,
        *L3_CREDENTIAL_PROOF_TARGETS,
    )
    (tmp_path / "Makefile").write_text(
        "".join(f"{target}:\n\t@true\n" for target in make_targets)
    )
    (tmp_path / ".planning/CURRENT.md").write_text("current\n")
    (tmp_path / "src/polyarb/http/health.py").write_text(
        'checks["snapshot:last_success_age_seconds"] = []\n'
    )
    (tmp_path / "src/polyarb/http/l2_health.py").write_text(
        'checks["event_bus:cursor_lag"] = []\n'
        'checks["ws:last_event_age_seconds"] = []\n'
        'checks["mirror:l2_tob_age_seconds"] = []\n'
        'checks["l3:active_count"] = []\n'
    )
    for path in ("status", "candidates", "signals", "l3/[asset_id]"):
        (tmp_path / "dashboard/app" / path / "page.tsx").write_text("export default 1\n")
    return tmp_path


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True, check=False)


def _hook_repo(tmp_path: Path) -> Path:
    repo = _repo(tmp_path)
    (repo / "scripts").mkdir()
    (repo / ".githooks").mkdir()
    (repo / "tools/climb/hooks").mkdir(parents=True)
    shutil.copy(ROOT / "scripts/check_m1_manual.py", repo / "scripts")
    shutil.copy(ROOT / ".githooks/pre-commit", repo / ".githooks")
    shutil.copy(ROOT / "tools/climb/hooks/pre-commit", repo / "tools/climb/hooks")
    (repo / MANUAL).write_text(_valid_manual())
    assert _git(repo, "init", "-q").returncode == 0
    assert _git(repo, "config", "user.email", "test@example.com").returncode == 0
    assert _git(repo, "config", "user.name", "Test User").returncode == 0
    assert _git(repo, "add", ".").returncode == 0
    assert _git(repo, "commit", "--no-verify", "-qm", "fixture").returncode == 0
    return repo


def _run_precommit(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", ".githooks/pre-commit"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )


def test_valid_manual_has_no_errors(tmp_path: Path) -> None:
    assert validate_manual(_repo(tmp_path), _valid_manual()) == []


def test_rejects_missing_section_and_unknown_label(tmp_path: Path) -> None:
    text = _valid_manual().replace("## 10. section-10\nbody\n", "")
    text = text.replace("已验证可用", "大概可用")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("required section 10" in error for error in errors)
    assert any("大概可用" in error for error in errors)


def test_rejects_missing_make_link_route_and_health(tmp_path: Path) -> None:
    text = _valid_manual().replace("snapshot-status", "missing-target")
    text = text.replace("../.planning/CURRENT.md", "missing.md")
    text = text.replace("snapshot:last_success_age_seconds", "missing:health")
    text = text.replace("dashboard/app/status/page.tsx", "dashboard/app/missing/page.tsx")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("Make target missing-target" in error for error in errors)
    assert any("link" in error for error in errors)
    assert any("health missing:health" in error for error in errors)
    assert any("route /status" in error for error in errors)


def test_rejects_empty_capability_field(tmp_path: Path) -> None:
    text = _valid_manual().replace("| local only |", "|  |")
    errors = validate_manual(_repo(tmp_path), text)
    assert any("empty required field" in error for error in errors)


def test_staged_classifier_is_narrow() -> None:
    assert classify_staged_impact(["Makefile"], " snapshot-status:\n-\told recipe\n+\tnew recipe\n")
    assert classify_staged_impact(
        ["src/polyarb/http/l2_health.py"],
        '+    checks["event_bus:cursor_lag"] = []\n',
    )
    assert classify_staged_impact(["dashboard/app/status/page.tsx"], "+export default Status\n")
    assert classify_staged_impact(
        ["alembic/versions/0001_contract.py"],
        "+op.create_table('l2_book_levels')\n",
    )
    assert not classify_staged_impact(
        ["src/polyarb/http/l2_health.py"], "+    logger.info('refactor only')\n"
    )
    assert not classify_staged_impact(
        ["tests/m1-perception/test_health_endpoint.py"], "+def test_internal(): pass\n"
    )


def test_staged_classifier_ignores_contract_syntax_outside_production_paths() -> None:
    test_only_diff = """diff --git a/tests/unit/test_helpers.py b/tests/unit/test_helpers.py
--- a/tests/unit/test_helpers.py
+++ b/tests/unit/test_helpers.py
@@ -0,0 +1 @@
+checks["fake:age"] = []
"""
    assert not classify_staged_impact(
        ["tests/unit/test_helpers.py", "src/polyarb/http/health.py"],
        test_only_diff,
    )

    unrelated_tool_diff = """diff --git a/tools/admin.py b/tools/admin.py
--- a/tools/admin.py
+++ b/tools/admin.py
@@ -0,0 +1 @@
+@app.command()
"""
    assert not classify_staged_impact(
        ["tools/admin.py", "src/polyarb/cli_observation.py"],
        unrelated_tool_diff,
    )


def test_staged_classifier_uses_explicit_dashboard_and_migration_registry() -> None:
    assert classify_staged_impact(
        ["dashboard/app/status/page.tsx"], "+export const status = true\n"
    )
    assert not classify_staged_impact(
        ["dashboard/app/login/page.tsx"], "+export const login = true\n"
    )
    assert not classify_staged_impact(
        ["alembic/versions/999_unrelated.py"], "+op.create_table('users')\n"
    )


def test_make_trigger_requires_registered_target_recipe_change() -> None:
    assert classify_staged_impact(
        ["Makefile"],
        "diff --git a/Makefile b/Makefile\n smoke-health-prod:\n-\tcurl old\n+\tcurl new\n",
    )
    assert not classify_staged_impact(
        ["Makefile"],
        "diff --git a/Makefile b/Makefile\n unrelated-tool:\n-\told\n+\tnew\n",
    )
    assert not classify_staged_impact(["Makefile"], "+## smoke-health-prod: comment wording only\n")


def test_required_markers_cannot_be_silently_removed(tmp_path: Path) -> None:
    text = _valid_manual().replace(
        "<!-- m1-contract: route=/signals file=dashboard/app/signals/page.tsx -->\n",
        "",
    )
    errors = validate_manual(_repo(tmp_path), text)
    assert any("required contract marker" in error for error in errors)


def test_manual_sync_rejects_whitespace_and_unowned_edits() -> None:
    before = _valid_manual()
    assert not manual_sync_is_meaningful(before, before.replace("body", "body  ", 1))
    assert not manual_sync_is_meaningful(before, before + "\nunrelated appendix\n")


def test_manual_sync_accepts_owned_section_or_valid_new_six_field_record() -> None:
    before = _valid_manual()
    assert manual_sync_is_meaningful(
        before, before.replace("## 7. section-7\nbody", "## 7. section-7\nnew operator action")
    )
    record = (
        "\n- `2026-07-18 | abc123 | L1 health field | no operator impact: rename only | "
        "make docs-m1-check | Reviewer`\n"
    )
    assert manual_sync_is_meaningful(before, before + record)


def test_staged_classifier_matches_contract_syntax_on_production_paths() -> None:
    health_diff = """diff --git a/src/polyarb/http/health.py b/src/polyarb/http/health.py
--- a/src/polyarb/http/health.py
+++ b/src/polyarb/http/health.py
@@ -0,0 +1 @@
+checks["snapshot:freshness"] = []
"""
    assert classify_staged_impact(["src/polyarb/http/health.py"], health_diff)

    cli_diff = """diff --git a/src/polyarb/cli_observation.py b/src/polyarb/cli_observation.py
--- a/src/polyarb/cli_observation.py
+++ b/src/polyarb/cli_observation.py
@@ -0,0 +1 @@
+@app.command()
"""
    assert classify_staged_impact(["src/polyarb/cli_observation.py"], cli_diff)

    route_diff = """diff --git a/src/polyarb/http/app.py b/src/polyarb/http/app.py
--- a/src/polyarb/http/app.py
+++ b/src/polyarb/http/app.py
@@ -0,0 +1 @@
+Route("/status", status)
"""
    assert classify_staged_impact(["src/polyarb/http/app.py"], route_diff)


@pytest.mark.parametrize(
    ("path", "line"),
    [
        (
            "src/polyarb/http/l2_app.py",
            '+            "/control/chaos/ws-test-kill-v2",\n',
        ),
        ("src/polyarb/snapshot/cli.py", '+        "--no-cache-v2",\n'),
        ("src/polyarb/cli_observation.py", '+        "-x",\n'),
    ],
)
def test_staged_classifier_matches_multiline_contract_continuations(path: str, line: str) -> None:
    assert classify_staged_impact([path], line)


@pytest.mark.parametrize(
    ("path", "line"),
    [
        (
            "src/polyarb/http/health.py",
            "+logger.info('example checks[\"snapshot:freshness\"] = []')\n",
        ),
        (
            "src/polyarb/http/l2_health.py",
            '+# checks["event_bus:cursor_lag"] = [] is documented elsewhere\n',
        ),
        (
            "src/polyarb/cli_observation.py",
            "+logger.info('example @app.command() and typer.Option(...)')\n",
        ),
        (
            "src/polyarb/cli_translation.py",
            "+# verbose: bool = typer.Option(False, '--verbose')\n",
        ),
        (
            "src/polyarb/http/app.py",
            "+logger.info('example Route(\"/status\", status)')\n",
        ),
        (
            "src/polyarb/http/l2_app.py",
            '+# Route("/status", status) is intentionally absent\n',
        ),
    ],
)
def test_staged_classifier_ignores_contract_wording_in_logs_and_comments(
    path: str, line: str
) -> None:
    assert not classify_staged_impact([path], line)


def test_nul_staged_paths_preserve_unicode_manual_path() -> None:
    raw = os.fsencode(str(MANUAL)) + b"\0"
    assert _decode_nul_paths(raw) == ["docs/M1-市场感知平台使用手册.md"]


def test_staged_unicode_manual_path_runs_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = _repo(tmp_path)
    manual = root / MANUAL
    manual.write_text(_valid_manual())
    parsed_paths = _decode_nul_paths(os.fsencode(str(MANUAL)) + b"\0")
    path_calls: list[tuple[str, ...]] = []
    validations: list[tuple[Path, str]] = []

    def fake_git_paths(*args: str) -> list[str]:
        path_calls.append(args)
        return parsed_paths

    monkeypatch.setattr(manual_checker, "__file__", str(root / "scripts/check_m1_manual.py"))
    monkeypatch.setattr(manual_checker, "_git_paths", fake_git_paths)
    monkeypatch.setattr(manual_checker, "_git", lambda *args: "")
    monkeypatch.setattr(
        manual_checker,
        "_index_view",
        lambda: (
            lambda path: _valid_manual() if path == MANUAL else "fixture",
            lambda path: True,
        ),
    )
    monkeypatch.setattr(
        manual_checker,
        "validate_manual",
        lambda checked_root, text, **kwargs: validations.append((checked_root, text)) or [],
    )

    assert main(["--staged"]) == 0
    assert path_calls == [("diff", "--cached", "--name-only", "-z")]
    assert validations == [(root, _valid_manual())]


def test_precommit_invokes_staged_m1_manual_check_before_summary_exit() -> None:
    text = (ROOT / ".githooks/pre-commit").read_text()
    check = "uv run python scripts/check_m1_manual.py --staged"
    assert check in text
    assert text.index(check) < text.index("PHASE_PLAN=")


def test_precommit_blocks_staged_public_contract_without_manual_sync(
    tmp_path: Path,
) -> None:
    repo = _hook_repo(tmp_path)
    health = repo / "src/polyarb/http/health.py"
    health.write_text(health.read_text() + 'checks["snapshot:freshness"] = []\n')
    assert _git(repo, "add", str(health.relative_to(repo))).returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "M1 operator contract changed" in result.stdout


def test_precommit_blocks_staged_opportunity_diagnosis_recipe_without_manual_sync(
    tmp_path: Path,
) -> None:
    repo = _hook_repo(tmp_path)
    makefile = repo / "Makefile"
    makefile.write_text(
        makefile.read_text()
        + "\ndiagnose-arb-feed-prod:\n\t@curl --disable --request GET https://example.test\n"
    )
    assert _git(repo, "add", "Makefile").returncode == 0
    assert _git(repo, "commit", "--no-verify", "-qm", "diagnosis fixture").returncode == 0
    makefile.write_text(makefile.read_text().replace("example.test", "changed.example.test"))
    assert _git(repo, "add", "Makefile").returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "M1 operator contract changed" in result.stdout


@pytest.mark.parametrize(
    ("path", "before", "after"),
    [
        (
            "src/polyarb/http/l2_app.py",
            'routes = [\n    Route(\n        "/control/chaos/ws-test-kill",\n'
            "        ws_test_kill_handler,\n    ),\n]\n",
            'routes = [\n    Route(\n        "/control/chaos/ws-test-kill-v2",\n'
            "        ws_test_kill_handler,\n    ),\n]\n",
        ),
        (
            "src/polyarb/snapshot/cli.py",
            'no_cache: bool = typer.Option(\n    False,\n    "--no-cache",\n'
            '    help="Disable cache",\n)\n',
            'no_cache: bool = typer.Option(\n    False,\n    "--no-cache-v2",\n'
            '    help="Disable cache",\n)\n',
        ),
    ],
)
def test_precommit_blocks_multiline_contract_continuation_without_manual_sync(
    tmp_path: Path, path: str, before: str, after: str
) -> None:
    repo = _hook_repo(tmp_path)
    changed = repo / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text(before)
    assert _git(repo, "add", path).returncode == 0
    assert _git(repo, "commit", "--no-verify", "-qm", "multiline fixture").returncode == 0
    changed.write_text(after)
    assert _git(repo, "add", path).returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "M1 operator contract changed" in result.stdout


def test_precommit_allows_staged_public_contract_with_manual_sync(
    tmp_path: Path,
) -> None:
    repo = _hook_repo(tmp_path)
    health = repo / "src/polyarb/http/health.py"
    health.write_text(health.read_text() + 'checks["snapshot:freshness"] = []\n')
    manual = repo / MANUAL
    manual.write_text(
        manual.read_text() + "\n- `2026-07-18 | review fixture | snapshot:freshness health field "
        "added | no operator workflow change; field is diagnostic-only | "
        "make docs-m1-check: OK | Test Reviewer`\n"
    )
    assert _git(repo, "add", str(health.relative_to(repo)), str(MANUAL)).returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "M1 manual contract: OK" in result.stdout


@pytest.mark.parametrize("manual_edit", ["\n", "\nunrelated appendix\n"])
def test_precommit_rejects_trivial_manual_edit_for_public_contract(
    tmp_path: Path, manual_edit: str
) -> None:
    repo = _hook_repo(tmp_path)
    health = repo / "src/polyarb/http/health.py"
    health.write_text(health.read_text() + 'checks["snapshot:freshness"] = []\n')
    manual = repo / MANUAL
    manual.write_text(manual.read_text() + manual_edit)
    assert _git(repo, "add", str(health.relative_to(repo)), str(MANUAL)).returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "whitespace/unowned text" in result.stdout


def test_precommit_validates_staged_manual_not_unstaged_repair(tmp_path: Path) -> None:
    repo = _hook_repo(tmp_path)
    health = repo / "src/polyarb/http/health.py"
    health.write_text(health.read_text() + 'checks["snapshot:freshness"] = []\n')
    manual = repo / MANUAL
    original = manual.read_text()
    staged_invalid = original.replace(
        "## 7. section-7\nbody", "## 7. section-7\nupdated operator action"
    ).replace(
        "<!-- m1-contract: route=/signals file=dashboard/app/signals/page.tsx -->\n",
        "",
    )
    manual.write_text(staged_invalid)
    assert _git(repo, "add", str(health.relative_to(repo)), str(MANUAL)).returncode == 0
    manual.write_text(original)
    assert _git(repo, "status", "--short", str(MANUAL)).stdout.startswith("MM")

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "required contract marker is missing" in result.stdout


def test_precommit_validates_staged_surface_not_unstaged_repair(tmp_path: Path) -> None:
    repo = _hook_repo(tmp_path)
    health = repo / "src/polyarb/http/health.py"
    original_health = health.read_text()
    health.write_text(
        original_health.replace("snapshot:last_success_age_seconds", "snapshot:renamed")
    )
    manual = repo / MANUAL
    manual.write_text(
        manual.read_text()
        + "\n- `2026-07-18 | staged-source-fixture | health rename | no operator impact: fixture | "
        "make docs-m1-check | Test Reviewer`\n"
    )
    assert _git(repo, "add", str(health.relative_to(repo)), str(MANUAL)).returncode == 0
    health.write_text(original_health)
    assert _git(repo, "status", "--short", str(health.relative_to(repo))).stdout.startswith("MM")

    result = _run_precommit(repo)

    assert result.returncode == 1
    assert "health snapshot:last_success_age_seconds is absent" in result.stdout


@pytest.mark.parametrize(
    ("path", "addition"),
    [
        (
            "src/polyarb/http/health.py",
            "logger.info('example checks[\"snapshot:freshness\"] = []')\n",
        ),
        (
            "src/polyarb/cli_observation.py",
            "# @app.command() and typer.Option(...) are docs examples\n",
        ),
        (
            "src/polyarb/http/app.py",
            "logger.info('example Route(\"/status\", status)')\n",
        ),
        (
            "tests/m1-perception/test_health_endpoint.py",
            'checks["fake:age"] = []\n',
        ),
        ("scripts/maintenance.py", "print('operational log only')\n"),
    ],
)
def test_precommit_ignores_staged_non_contract_changes(
    tmp_path: Path, path: str, addition: str
) -> None:
    repo = _hook_repo(tmp_path)
    changed = repo / path
    changed.parent.mkdir(parents=True, exist_ok=True)
    changed.write_text((changed.read_text() if changed.exists() else "") + addition)
    assert _git(repo, "add", path).returncode == 0

    result = _run_precommit(repo)

    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout == ""


def test_repository_m1_manual_passes_contract() -> None:
    manual = ROOT / "docs/M1-市场感知平台使用手册.md"
    assert manual.is_file(), "living M1 manual must exist"
    assert validate_manual(ROOT, manual.read_text()) == []


def test_surface_registry_covers_every_make_target_named_by_manual() -> None:
    text = (ROOT / MANUAL).read_text()
    referenced = set(re.findall(r"`make ([a-z0-9][a-z0-9-]*)", text))
    assert referenced <= M1_MAKE_TARGETS


def test_manual_keeps_real_money_boundary_explicit() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()
    assert "不构成真实资金下单授权" in text
    assert "`.planning/CURRENT.md`" in text
    assert "本地数据不代表生产状态" in text


def test_manual_explains_opportunity_feed_diagnosis_and_non_readiness() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()

    assert "`make diagnose-arb-feed-prod`" in text
    assert "HTTP 503" in text
    assert "不是零机会" in text
    assert "exit=0" in text
    assert "仍不代表机会 feed 已准备就绪" in text


def test_docs_m1_check_make_target() -> None:
    result = subprocess.run(["make", "docs-m1-check"], cwd=ROOT, text=True, capture_output=True)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "M1 manual contract: OK" in result.stdout


def test_smoke_health_prod_make_target_is_strict_and_read_only() -> None:
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(r"(?m)^smoke-health-prod:\n(?P<recipe>(?:\t.*\n)+)", makefile)
    assert match is not None, "strict production health target must exist"
    recipe = match.group("recipe")
    assert "curl --disable" in recipe
    assert re.search(r"--request\s+GET\b", recipe)
    assert "https://polyarb-l1.fly.dev/health" in recipe
    assert "/healthz" not in recipe
    forbidden = ("flyctl", "scale", "post", "deploy", "secret", "restart", "chaos")
    assert not any(re.search(rf"\b{token}\b", recipe.lower()) for token in forbidden)


def test_smoke_l2_health_strict_prod_make_target_is_strict_and_read_only() -> None:
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(r"(?m)^smoke-l2-health-strict-prod:\n(?P<recipe>(?:\t.*\n)+)", makefile)
    assert match is not None, "strict L2 production health target must exist"
    recipe = match.group("recipe")
    assert "curl --disable" in recipe
    assert re.search(r"--request\s+GET\b", recipe)
    assert "https://polyarb-l2.fly.dev/health" in recipe
    assert "/healthz" not in recipe
    assert re.search(
        r'if \[ "\$\$HTTP_STATUS" = "200" \]; then.*else.*exit 1; fi',
        recipe,
        re.DOTALL,
    ), "strict target must exit nonzero unless HTTP status is 200"
    forbidden = (
        "flyctl",
        "scale",
        "post",
        "deploy",
        "secret",
        "secrets",
        "restart",
        "schema",
        "migrate",
        "migration",
        "chaos",
    )
    recipe_lower = recipe.lower()
    assert not any(
        re.search(rf"\b{re.escape(operation)}\b", recipe_lower) for operation in forbidden
    )


def test_manual_routes_l2_strict_health_through_make() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()
    daily = text.split("## 3. ", 1)[1].split("## 4. ", 1)[0]
    read_only = text.split("生产巡检（只读）", 1)[1].split("L1→L2 市场候选链（只读观察）", 1)[0]
    candidates = text.split("L1→L2 市场候选链（只读观察）", 1)[1].split("### 本地验证", 1)[0]

    for section in (daily, read_only, candidates):
        assert "`make smoke-l2-health-strict-prod`" in section
    assert "`make smoke-l2-health-prod`" in daily
    assert "`make smoke-l2-health-prod`" in read_only
    assert "`make smoke-l2-health-prod`" in candidates
    assert "smoke-l2-health-prod` 只证明" in text


def test_manual_keeps_reviewed_operator_safety_facts() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()
    daily = text.split("## 3. ", 1)[1].split("## 4. ", 1)[0]
    read_only = text.split("### 日常只读", 1)[1].split("### 本地 mutation", 1)[0]

    assert "`make smoke-health-prod`" in daily
    assert "`make smoke-test`" not in daily
    assert "`make smoke-health-prod`" in read_only
    assert "`make smoke-test`" not in read_only
    assert "候选不是独立 staging 部署" in text
    assert "`POLYARB_SCAN_SHARED_SECRET`" in text
    section_10 = text.split("## 10. ", 1)[1]
    assert "staged guard 已在 pre-commit hook 生效" in section_10
    assert "public contract" in section_10
    assert "unrelated、test-only、log-only" in section_10
    assert "05.2-03 将把检查器集成到 pre-commit hook" not in section_10
    assert "candidates、asset TOB/trades 和 signals" in text
    assert "`make smoke-l3-dashboard asset_id=...`" in text
    assert "手工检查 `/status`" in text


def test_phase_054_operator_commands_are_centralized_and_documented() -> None:
    read_only = (
        "l3-evidence-status",
        "l3-soak-checkpoint",
        "l3-soak-verify",
        "l3-evidence-retention-check",
    )
    mutations = (
        "l3-soak-manifest-bind",
        "l3-evidence-retention-cleanup",
    )
    credential_proofs = (
        "l3-runtime-credential-check",
        "l3-retention-operator-check",
        "supabase-prod-revision",
    )
    text = (ROOT / MANUAL).read_text()

    assert L3_READ_ONLY_MAKE_TARGETS == read_only
    assert L3_MUTATION_MAKE_TARGETS == mutations
    assert L3_CREDENTIAL_PROOF_TARGETS == credential_proofs
    for target in (*read_only, *mutations, *credential_proofs):
        assert f"`make {target}" in text


def test_phase_054_manual_has_exact_r08_and_release_37_boundaries() -> None:
    text = (ROOT / MANUAL).read_text()
    required = (
        "sample interval = 30s",
        "sample gap ≤ 75s",
        "promoter interval = 300s",
        "promoter start gap ≤ 360s",
        "book / OHLC age < 120s",
        "5 markets / 10 tokens",
        "10/10/10 desired/committed/evidenced",
        "all 10 book tokens",
        "all 5 Yes OHLC",
        "retention ≥ 30 days",
        "schema revision 007",
        "release 37",
        "diagnostic-only",
        "LOCAL PASS DOES NOT AUTHORIZE PRODUCTION",
    )

    for phrase in required:
        assert phrase in text


def test_phase_054_manual_separates_read_only_and_mutation_commands() -> None:
    text = (ROOT / MANUAL).read_text()
    read_only = text.split("### 日常只读", 1)[1].split("### 本地 mutation", 1)[0]
    production_mutation = text.split("### 生产 mutation", 1)[1].split("### chaos", 1)[0]
    assert L3_READ_ONLY_MAKE_TARGETS
    assert L3_MUTATION_MAKE_TARGETS

    for target in L3_READ_ONLY_MAKE_TARGETS:
        assert f"`make {target}" in read_only
        assert f"`make {target}" not in production_mutation
    for target in L3_MUTATION_MAKE_TARGETS:
        assert f"`make {target}" in production_mutation
        assert f"`make {target}" not in read_only


def test_phase_054_learning_chapter_has_complete_contract_and_live_line_references() -> None:
    chapter = ROOT / "docs/learning/22-L3连续浸泡证据.md"
    assert chapter.is_file()
    text = chapter.read_text()
    required = (
        "## 30 秒心智模型",
        "membership",
        "recorded_at",
        "append-only",
        "AcceptanceConfig",
        "T+0",
        "T+6",
        "T+12",
        "T+18",
        "T+24",
        "soak_hash",
        "interval_hash",
        "report_hash",
        "raw_row_set_hash",
        "event kind",
        "severity",
        "l3_retention_operator",
        "## 设计取舍",
        "## 失败模式",
        "## 对手测试",
        "## FAQ 增量",
    )
    for phrase in required:
        assert phrase in text

    questions = re.findall(r"(?m)^[1-5]\. ", text.split("## 对手测试", 1)[1])
    assert len(questions) == 5

    references = re.findall(r"`([^`]+\.py):(\d+)(?:-(\d+))?`", text)
    assert len(references) >= 8
    for raw_path, raw_start, raw_end in references:
        source = ROOT / raw_path
        assert source.is_file(), f"learning reference does not resolve: {raw_path}"
        line_count = len(source.read_text().splitlines())
        start = int(raw_start)
        end = int(raw_end or raw_start)
        assert 1 <= start <= end <= line_count, (
            f"learning reference is outside HEAD: {raw_path}:{start}-{end}"
        )


def test_phase_054_learning_index_links_new_chapter() -> None:
    text = (ROOT / "docs/learning/00-INDEX.md").read_text()
    assert "[L3 连续浸泡证据](22-L3连续浸泡证据.md)" in text


def test_task3_image_gate_operator_truth_is_preserved() -> None:
    text = (ROOT / MANUAL).read_text()
    assert "完整打印当前 Fly image 的工具矩阵" in text
    assert "默认只把 `python` 当作 required gate" in text
    assert "optional MISS 是替代设计证据而非失败" in text
    assert 'required="python curl"' in text
