from __future__ import annotations

import os
import re
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(ROOT))

import scripts.check_m1_manual as manual_checker  # noqa: E402
from scripts.check_m1_manual import (  # noqa: E402
    MANUAL,
    _decode_nul_paths,
    classify_staged_impact,
    main,
    validate_manual,
)

HEADINGS = tuple(f"## {n}. " for n in range(1, 11))


def _valid_manual() -> str:
    sections = "\n".join(f"## {n}. section-{n}\nbody" for n in range(1, 11))
    return f"""# M1 市场感知平台使用手册

> 最后核验：2026-07-18
> 动态状态：`.planning/CURRENT.md`

{sections}

| 能力 | 状态 | 用途 | 数据源 | 验证方法 | 已知限制 | 禁止用途 |
|---|---|---|---|---|---|---|
| L1 | 已验证可用 | snapshot | Gamma+CLOB | `make snapshot-status` | local only | live order |

[CURRENT](../.planning/CURRENT.md)
<!-- m1-contract: health=snapshot:last_success_age_seconds file=src/polyarb/http/health.py -->
<!-- m1-contract: route=/status file=dashboard/app/status/page.tsx -->
"""


def _repo(tmp_path: Path) -> Path:
    (tmp_path / "src/polyarb/http").mkdir(parents=True)
    (tmp_path / "dashboard/app/status").mkdir(parents=True)
    (tmp_path / "docs").mkdir()
    (tmp_path / ".planning").mkdir()
    (tmp_path / "Makefile").write_text("snapshot-status:\n\t@true\n")
    (tmp_path / ".planning/CURRENT.md").write_text("current\n")
    (tmp_path / "src/polyarb/http/health.py").write_text(
        'checks["snapshot:last_success_age_seconds"] = []\n'
    )
    (tmp_path / "dashboard/app/status/page.tsx").write_text("export default 1\n")
    return tmp_path


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
    assert classify_staged_impact(
        ["Makefile"], "+## snapshot-status: changed operator contract\n"
    )
    assert classify_staged_impact(
        ["src/polyarb/http/l2_health.py"],
        '+    checks["event_bus:cursor_lag"] = []\n',
    )
    assert classify_staged_impact(
        ["dashboard/app/new-view/page.tsx"], "diff --git a/x b/x\n"
    )
    assert classify_staged_impact(["alembic/versions/0001_contract.py"], "")
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

    monkeypatch.setattr(
        manual_checker, "__file__", str(root / "scripts/check_m1_manual.py")
    )
    monkeypatch.setattr(manual_checker, "_git_paths", fake_git_paths)
    monkeypatch.setattr(manual_checker, "_git", lambda *args: "")
    monkeypatch.setattr(
        manual_checker,
        "validate_manual",
        lambda checked_root, text: validations.append((checked_root, text)) or [],
    )

    assert main(["--staged"]) == 0
    assert path_calls == [("diff", "--cached", "--name-only", "-z")]
    assert validations == [(root, _valid_manual())]


def test_repository_m1_manual_passes_contract() -> None:
    manual = ROOT / "docs/M1-市场感知平台使用手册.md"
    assert manual.is_file(), "living M1 manual must exist"
    assert validate_manual(ROOT, manual.read_text()) == []


def test_manual_keeps_real_money_boundary_explicit() -> None:
    text = (ROOT / "docs/M1-市场感知平台使用手册.md").read_text()
    assert "不构成真实资金下单授权" in text
    assert "`.planning/CURRENT.md`" in text
    assert "本地数据不代表生产状态" in text


def test_docs_m1_check_make_target() -> None:
    result = subprocess.run(
        ["make", "docs-m1-check"], cwd=ROOT, text=True, capture_output=True
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "M1 manual contract: OK" in result.stdout


def test_smoke_health_prod_make_target_is_strict_and_read_only() -> None:
    makefile = (ROOT / "Makefile").read_text()
    match = re.search(
        r"(?m)^smoke-health-prod:\n(?P<recipe>(?:\t.*\n)+)", makefile
    )
    assert match is not None, "strict production health target must exist"
    recipe = match.group("recipe")
    assert "https://polyarb-l1.fly.dev/health" in recipe
    assert "/healthz" not in recipe
    forbidden = ("flyctl", "scale ", " post ", "deploy", "secrets", "restart")
    assert not any(token in recipe.lower() for token in forbidden)


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
    assert "05.2-03 将把检查器集成到 pre-commit hook" in text
    assert "candidates、asset TOB/trades 和 signals" in text
    assert "`make smoke-l3-dashboard asset_id=...`" in text
    assert "手工检查 `/status`" in text
