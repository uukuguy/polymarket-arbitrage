#!/usr/bin/env python3
"""Idempotently harden GSD's executor worktree cleanup instructions."""

from __future__ import annotations

import argparse
import os
import tempfile
from collections.abc import Callable, Sequence
from pathlib import Path

MARKER = "GSD_WORKTREE_CLEANUP_V2"
QUICK_HEAD_CAPTURE = """```bash
EXPECTED_BASE=$(git rev-parse HEAD)
```"""
EXECUTE_HEAD_CAPTURE = """   ```bash
   EXPECTED_BASE=$(git rev-parse HEAD)
   ```"""

EXECUTE_LIST = """   # List worktrees created by this wave's agents
   WORKTREES=$(git worktree list --porcelain | grep "^worktree " | grep -v "$(pwd)$" | sed 's/^worktree //')"""  # noqa: E501

QUICK_LIST = """   # Find worktrees created by the executor
   WORKTREES=$(git worktree list --porcelain | grep "^worktree " | grep -v "$(pwd)$" | sed 's/^worktree //')"""  # noqa: E501

EXECUTE_REMOVE = """       # Remove the worktree
       git worktree remove "$WT" --force 2>/dev/null || true

       # Delete the temporary branch
       git branch -D "$WT_BRANCH" 2>/dev/null || true
     fi
   done"""

QUICK_REMOVE = """       git worktree remove "$WT" --force 2>/dev/null || true
       git branch -D "$WT_BRANCH" 2>/dev/null || true
     fi
   done"""


class PatchShapeError(RuntimeError):
    """Raised when installed GSD text no longer matches the audited shape."""


def _replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise PatchShapeError(f"{label}: expected one upstream anchor, found {count}")
    return text.replace(old, new, 1)


def _is_current(text: str) -> bool:
    return (
        text.count(MARKER) == 1
        and "comm -13" in text
        and 'if git worktree remove "$WT" --force --force; then' in text
        and 'git merge-base --is-ancestor "$WT_BRANCH" HEAD' in text
        and "CLEANUP_FAILED=1" in text
    )


def _capture_block(snapshot_name: str, *, indent: str) -> str:
    lines = [
        "```bash",
        "EXPECTED_BASE=$(git rev-parse HEAD)",
        f"# {MARKER}: snapshot registry before spawning; cleanup processes only new paths.",
        f'PRE_WORKTREES_FILE="$(git rev-parse --git-common-dir)/{snapshot_name}.before"',
        "git worktree list --porcelain | sed -n 's/^worktree //p' | sort > \"$PRE_WORKTREES_FILE\"",
        "```",
    ]
    return "\n".join(f"{indent}{line}" for line in lines)


def _scoped_list(comment: str) -> str:
    return f"""   {comment}
   if [ ! -f "$PRE_WORKTREES_FILE" ]; then
     echo "ERROR: missing pre-execution worktree snapshot $PRE_WORKTREES_FILE" >&2
     exit 1
   fi
   CURRENT_WORKTREES_FILE=$(mktemp)
   git worktree list --porcelain | sed -n 's/^worktree //p' | sort > "$CURRENT_WORKTREES_FILE"
   WORKTREES=$(comm -13 "$PRE_WORKTREES_FILE" "$CURRENT_WORKTREES_FILE")
   rm -f "$CURRENT_WORKTREES_FILE" "$PRE_WORKTREES_FILE"
   CLEANUP_FAILED=0"""


def _safe_remove(include_comments: bool) -> str:
    prefix = """       # Unlock after the Task has returned; locked removal needs --force twice.
       git worktree unlock "$WT" 2>/dev/null || true
"""
    if not include_comments:
        prefix = """       git worktree unlock "$WT" 2>/dev/null || true
"""
    return prefix + """       if git worktree remove "$WT" --force --force; then
         if git merge-base --is-ancestor "$WT_BRANCH" HEAD; then
           git branch -D "$WT_BRANCH"
         else
           echo "BLOCKED: removed worktree but retained non-ancestor branch $WT_BRANCH" >&2
           CLEANUP_FAILED=1
         fi
       else
         echo "ERROR: failed to remove executor worktree $WT" >&2
         CLEANUP_FAILED=1
       fi
     fi
   done
   if [ "$CLEANUP_FAILED" -ne 0 ]; then
     echo "ERROR: executor worktree cleanup incomplete" >&2
     exit 1
   fi"""


def rewrite_execute_phase(text: str) -> str:
    if MARKER in text:
        if not _is_current(text):
            raise PatchShapeError("execute-phase: partial or incompatible existing patch")
        return text
    text = _replace_once(
        text,
        EXECUTE_HEAD_CAPTURE,
        _capture_block(
            "gsd-worktrees-phase-{phase_number}-wave-{wave}", indent="   "
        ),
        "execute-phase HEAD capture",
    )
    text = _replace_once(
        text,
        EXECUTE_LIST,
        _scoped_list("# List only worktrees created by this wave's agents"),
        "execute-phase worktree enumeration",
    )
    return _replace_once(
        text,
        EXECUTE_REMOVE,
        _safe_remove(include_comments=True),
        "execute-phase removal",
    )


def rewrite_quick(text: str) -> str:
    if MARKER in text:
        if not _is_current(text):
            raise PatchShapeError("quick: partial or incompatible existing patch")
        return text
    text = _replace_once(
        text,
        QUICK_HEAD_CAPTURE,
        _capture_block("gsd-worktrees-quick-${quick_id}", indent=""),
        "quick HEAD capture",
    )
    text = _replace_once(
        text,
        QUICK_LIST,
        _scoped_list("# Find only worktrees created by this executor"),
        "quick worktree enumeration",
    )
    return _replace_once(
        text,
        QUICK_REMOVE,
        _safe_remove(include_comments=False),
        "quick removal",
    )


def _atomic_write(path: Path, text: str) -> None:
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        handle.write(text)
        temporary = Path(handle.name)
    os.replace(temporary, path)


def patch_root(root: Path, *, check: bool) -> int:
    workflows = root / "workflows"
    targets: tuple[tuple[Path, Callable[[str], str]], ...] = (
        (workflows / "execute-phase.md", rewrite_execute_phase),
        (workflows / "quick.md", rewrite_quick),
    )
    originals = {path: path.read_text(encoding="utf-8") for path, _ in targets}

    if check:
        stale = [path for path, _ in targets if not _is_current(originals[path])]
        for path, _ in targets:
            state = "CURRENT" if path not in stale else "STALE"
            print(f"{state} {path}")
        return 1 if stale else 0

    rewritten = {
        path: rewriter(originals[path]) for path, rewriter in targets
    }
    for path, _ in targets:
        if rewritten[path] == originals[path]:
            print(f"UNCHANGED {path}")
            continue
        _atomic_write(path, rewritten[path])
        print(f"PATCHED {path}")
    return 0


def _default_root() -> Path:
    codex_root = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex"))
    return codex_root / "get-shit-done"


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=_default_root())
    parser.add_argument("--check", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    try:
        return patch_root(args.root, check=args.check)
    except (OSError, PatchShapeError) as exc:
        print(f"ERROR {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
