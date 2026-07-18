from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(["git", *args], cwd=repo, text=True, capture_output=True, check=False)


def _repo(tmp_path: Path, generator: str) -> Path:
    repo = tmp_path / "repo"
    (repo / "docs/status/climb").mkdir(parents=True)
    (repo / "tools/climb/hooks").mkdir(parents=True)
    (repo / "tools/climb").mkdir(exist_ok=True)
    shutil.copy(ROOT / "tools/climb/hooks/pre-commit", repo / "tools/climb/hooks")
    (repo / "tools/climb/regen-tree.py").write_text(generator)
    for path, content in {
        "hypotheses.yaml": "items: []\n",
        "research-tree.md": "old\n",
        "research-tree.json": "{}\n",
    }.items():
        (repo / "docs/status/climb" / path).write_text(content)
    assert _git(repo, "init", "-q").returncode == 0
    assert _git(repo, "config", "user.email", "test@example.com").returncode == 0
    assert _git(repo, "config", "user.name", "Test User").returncode == 0
    assert _git(repo, "add", ".").returncode == 0
    assert _git(repo, "commit", "-qm", "fixture").returncode == 0
    return repo


def _run(repo: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", "tools/climb/hooks/pre-commit"],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )


def test_hook_generates_but_refuses_unreviewed_tree(tmp_path: Path) -> None:
    repo = _repo(
        tmp_path,
        "from pathlib import Path\n"
        "Path('docs/status/climb/research-tree.md').write_text('new\\n')\n"
        "Path('docs/status/climb/research-tree.json').write_text('{\\\"new\\\": true}\\n')\n",
    )
    source = repo / "docs/status/climb/hypotheses.yaml"
    source.write_text("items: [H-1]\n")
    assert _git(repo, "add", str(source.relative_to(repo))).returncode == 0

    result = _run(repo)

    assert result.returncode == 1
    assert "must be reviewed and staged" in result.stderr
    assert _git(repo, "log", "-1", "--format=%s").stdout.strip() == "fixture"


def test_hook_rejects_dirty_climb_source_before_generation(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "raise SystemExit('generator must not run')\n")
    source = repo / "docs/status/climb/hypotheses.yaml"
    source.write_text("items: [staged]\n")
    assert _git(repo, "add", str(source.relative_to(repo))).returncode == 0
    source.write_text("items: [unstaged]\n")

    result = _run(repo)

    assert result.returncode == 1
    assert "unstaged climb source" in result.stderr
    assert "generator must not run" not in result.stderr


def test_hook_reports_generator_failure_without_committing(tmp_path: Path) -> None:
    repo = _repo(tmp_path, "raise SystemExit(7)\n")
    source = repo / "docs/status/climb/hypotheses.yaml"
    source.write_text("items: [H-1]\n")
    assert _git(repo, "add", str(source.relative_to(repo))).returncode == 0

    result = _run(repo)

    assert result.returncode == 1
    assert "generator failed" in result.stderr
    assert _git(repo, "log", "-1", "--format=%s").stdout.strip() == "fixture"
