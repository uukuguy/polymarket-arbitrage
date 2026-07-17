from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from scripts import cleanup_agent_worktrees as cleanup


def _git(cwd: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=check,
    )


@pytest.fixture
def repo(tmp_path: Path) -> Path:
    root = tmp_path / "repo"
    root.mkdir()
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.name", "Worktree Test")
    _git(root, "config", "user.email", "worktree-test@example.invalid")
    (root / "README.md").write_text("base\n", encoding="utf-8")
    _git(root, "add", "README.md")
    _git(root, "commit", "-m", "base")
    return root


def _add_agent_worktree(
    repo: Path,
    name: str,
    *,
    pid: int = 999_999,
    add_commit: bool = False,
    dirty: bool = False,
) -> tuple[Path, str]:
    path = repo / ".claude" / "worktrees" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    branch = f"worktree-{name}"
    _git(repo, "worktree", "add", "-b", branch, str(path))
    if add_commit:
        (path / "branch-only.txt").write_text("unique\n", encoding="utf-8")
        _git(path, "add", "branch-only.txt")
        _git(path, "commit", "-m", "branch-only")
    if dirty:
        (path / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    _git(repo, "worktree", "lock", "--reason", f"claude agent {name} (pid {pid})", str(path))
    return path, branch


def test_parse_porcelain_keeps_lock_reason_and_branch() -> None:
    records = cleanup.parse_porcelain(
        "worktree /tmp/repo/.claude/worktrees/agent-abc\n"
        "HEAD 0123456789\n"
        "branch refs/heads/worktree-agent-abc\n"
        "locked claude agent agent-abc (pid 1234)\n\n"
    )

    assert records == [
        cleanup.Worktree(
            path=Path("/tmp/repo/.claude/worktrees/agent-abc"),
            branch="worktree-agent-abc",
            locked_reason="claude agent agent-abc (pid 1234)",
        )
    ]


@pytest.mark.parametrize("reason", [None, "", "claude agent without pid", "(pid 0)"])
def test_extract_pid_rejects_missing_or_malformed_reason(reason: str | None) -> None:
    assert cleanup.extract_pid(reason) is None


def test_dry_run_reports_dead_pid_clean_merged_worktree_without_mutation(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path, branch = _add_agent_worktree(repo, "agent-dead-clean")

    result = cleanup.cleanup(repo, apply=False, discard_unmerged=frozenset())

    assert result == 0
    assert path.exists()
    assert _git(repo, "show-ref", "--verify", f"refs/heads/{branch}", check=False).returncode == 0
    assert f"REMOVE {path}" in capsys.readouterr().out


def test_live_pid_blocks_cleanup(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path, _ = _add_agent_worktree(repo, "agent-live", pid=os.getpid())

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset())

    output = capsys.readouterr().out
    assert result == 1
    assert path.exists()
    assert "BLOCK" in output
    assert "owner-pid-alive" in output


def test_dirty_worktree_blocks_cleanup(repo: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path, _ = _add_agent_worktree(repo, "agent-dirty", dirty=True)

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset())

    output = capsys.readouterr().out
    assert result == 1
    assert path.exists()
    assert "BLOCK" in output
    assert "dirty" in output


def test_path_outside_agent_root_is_ignored(
    repo: Path, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    outside = tmp_path / "agent-outside"
    _git(repo, "worktree", "add", "-b", "worktree-agent-outside", str(outside))
    _git(
        repo,
        "worktree",
        "lock",
        "--reason",
        "claude agent agent-outside (pid 999999)",
        str(outside),
    )

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset())

    assert result == 0
    assert outside.exists()
    assert f"IGNORE {outside}" in capsys.readouterr().out


def test_nonancestor_branch_blocks_without_explicit_disposition(
    repo: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    path, branch = _add_agent_worktree(repo, "agent-unmerged", add_commit=True)

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset())

    output = capsys.readouterr().out
    assert result == 1
    assert path.exists()
    assert "BLOCK" in output
    assert "non-ancestor" in output
    assert branch in output


def test_explicit_discard_removes_nonancestor_worktree_and_branch(repo: Path) -> None:
    path, branch = _add_agent_worktree(repo, "agent-audited", add_commit=True)

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset({branch}))

    assert result == 0
    assert not path.exists()
    assert _git(repo, "show-ref", "--verify", f"refs/heads/{branch}", check=False).returncode != 0


def test_remove_failure_does_not_delete_branch(
    repo: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path, branch = _add_agent_worktree(repo, "agent-remove-fails")
    original = cleanup.run_git

    def fail_remove(
        cwd: Path, *args: str, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        if args[:2] == ("worktree", "remove"):
            return subprocess.CompletedProcess(["git", *args], 1, "", "synthetic remove failure")
        return original(cwd, *args, check=check)

    monkeypatch.setattr(cleanup, "run_git", fail_remove)

    result = cleanup.cleanup(repo, apply=True, discard_unmerged=frozenset())

    assert result == 1
    assert path.exists()
    assert _git(repo, "show-ref", "--verify", f"refs/heads/{branch}", check=False).returncode == 0
