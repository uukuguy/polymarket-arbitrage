from __future__ import annotations

from pathlib import Path

import pytest

from scripts import patch_gsd_worktree_cleanup as patcher

EXECUTE_FIXTURE = (
    """Before spawning, capture the current HEAD:
   ```bash
   EXPECTED_BASE=$(git rev-parse HEAD)
   ```

5.5. Worktree cleanup:
```bash
   # List worktrees created by this wave's agents
   WORKTREES=$(git worktree list --porcelain | grep "^worktree " | """
    """grep -v "$(pwd)$" | sed 's/^worktree //')

   for WT in $WORKTREES; do
     WT_BRANCH=$(git -C "$WT" rev-parse --abbrev-ref HEAD 2>/dev/null)
       # Remove the worktree
       git worktree remove "$WT" --force 2>/dev/null || true

       # Delete the temporary branch
       git branch -D "$WT_BRANCH" 2>/dev/null || true
     fi
   done
```
"""
)


QUICK_FIXTURE = (
    """Capture current HEAD before spawning (used for worktree branch check):
```bash
EXPECTED_BASE=$(git rev-parse HEAD)
```

After executor returns:
```bash
   # Find worktrees created by the executor
   WORKTREES=$(git worktree list --porcelain | grep "^worktree " | """
    """grep -v "$(pwd)$" | sed 's/^worktree //')
   for WT in $WORKTREES; do
     WT_BRANCH=$(git -C "$WT" rev-parse --abbrev-ref HEAD 2>/dev/null)
       git worktree remove "$WT" --force 2>/dev/null || true
       git branch -D "$WT_BRANCH" 2>/dev/null || true
     fi
   done
```
"""
)


@pytest.mark.parametrize(
    ("rewriter", "fixture"),
    [
        (patcher.rewrite_execute_phase, EXECUTE_FIXTURE),
        (patcher.rewrite_quick, QUICK_FIXTURE),
    ],
)
def test_rewriter_scopes_cleanup_and_uses_locked_removal(rewriter: object, fixture: str) -> None:
    rewritten = rewriter(fixture)  # type: ignore[operator]

    assert "GSD_WORKTREE_CLEANUP_V2" in rewritten
    assert "comm -13" in rewritten
    assert 'git worktree remove "$WT" --force --force' in rewritten
    assert 'if git worktree remove "$WT" --force --force; then' in rewritten
    assert 'git merge-base --is-ancestor "$WT_BRANCH" HEAD' in rewritten
    assert "CLEANUP_FAILED=1" in rewritten
    assert rewriter(rewritten) == rewritten  # type: ignore[operator]


def test_unknown_upstream_shape_is_refused() -> None:
    with pytest.raises(patcher.PatchShapeError):
        patcher.rewrite_execute_phase("new upstream layout")


def test_apply_then_check_on_workflow_directory(tmp_path: Path) -> None:
    root = tmp_path / "get-shit-done"
    workflows = root / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "execute-phase.md").write_text(EXECUTE_FIXTURE, encoding="utf-8")
    (workflows / "quick.md").write_text(QUICK_FIXTURE, encoding="utf-8")

    assert patcher.patch_root(root, check=False) == 0
    assert patcher.patch_root(root, check=True) == 0
    assert (workflows / "execute-phase.md").read_text(encoding="utf-8").count(
        "GSD_WORKTREE_CLEANUP_V2"
    ) == 1
    assert (workflows / "quick.md").read_text(encoding="utf-8").count(
        "GSD_WORKTREE_CLEANUP_V2"
    ) == 1


def test_check_fails_when_workflow_is_unpatched(tmp_path: Path) -> None:
    root = tmp_path / "get-shit-done"
    workflows = root / "workflows"
    workflows.mkdir(parents=True)
    (workflows / "execute-phase.md").write_text(EXECUTE_FIXTURE, encoding="utf-8")
    (workflows / "quick.md").write_text(QUICK_FIXTURE, encoding="utf-8")

    assert patcher.patch_root(root, check=True) == 1
