#!/usr/bin/env python3
"""Safely inspect and reclaim stale Claude agent git worktrees.

The command is dry-run by default. It only considers linked worktrees under
``.claude/worktrees/agent-*`` and requires explicit disposition for branches
that are not ancestors of the current main-worktree branch.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


AGENT_DIR_RE = re.compile(r"^agent-[A-Za-z0-9_-]+$")
PID_RE = re.compile(r"\(pid (?P<pid>[1-9][0-9]*)\)")


@dataclass(frozen=True)
class Worktree:
    path: Path
    branch: str | None
    locked_reason: str | None


def run_git(
    cwd: Path, *args: str, check: bool = True
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        text=True,
        capture_output=True,
        check=check,
    )


def parse_porcelain(text: str) -> list[Worktree]:
    records: list[Worktree] = []
    for block in text.strip().split("\n\n") if text.strip() else []:
        path: Path | None = None
        branch: str | None = None
        locked_reason: str | None = None
        for line in block.splitlines():
            key, _, value = line.partition(" ")
            if key == "worktree":
                path = Path(value)
            elif key == "branch":
                prefix = "refs/heads/"
                branch = value[len(prefix) :] if value.startswith(prefix) else value
            elif key == "locked":
                locked_reason = value or None
        if path is not None:
            records.append(Worktree(path=path, branch=branch, locked_reason=locked_reason))
    return records


def extract_pid(reason: str | None) -> int | None:
    if not reason:
        return None
    match = PID_RE.search(reason)
    return int(match.group("pid")) if match else None


def pid_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _is_dirty(path: Path) -> bool:
    result = run_git(path, "status", "--porcelain", check=False)
    return result.returncode != 0 or bool(result.stdout.strip())


def _is_ancestor(repo_root: Path, branch: str, current_branch: str) -> bool:
    result = run_git(
        repo_root,
        "merge-base",
        "--is-ancestor",
        branch,
        current_branch,
        check=False,
    )
    return result.returncode == 0


def cleanup(
    repo_root: Path,
    *,
    apply: bool,
    discard_unmerged: frozenset[str],
) -> int:
    repo_root = repo_root.resolve()
    expected_parent = (repo_root / ".claude" / "worktrees").resolve()
    listed = run_git(repo_root, "worktree", "list", "--porcelain").stdout
    current_branch = run_git(repo_root, "rev-parse", "--abbrev-ref", "HEAD").stdout.strip()
    errors = 0
    dispositions_used: set[str] = set()

    for worktree in parse_porcelain(listed):
        path = worktree.path.resolve()
        if path.parent != expected_parent or not AGENT_DIR_RE.fullmatch(path.name):
            if path != repo_root:
                print(f"IGNORE {path} reason=outside-agent-root")
            continue

        pid = extract_pid(worktree.locked_reason)
        if pid is None:
            print(f"BLOCK {path} reason=unknown-lock-owner")
            errors += 1
            continue
        if pid_is_alive(pid):
            print(f"BLOCK {path} reason=owner-pid-alive pid={pid}")
            errors += 1
            continue
        if _is_dirty(path):
            print(f"BLOCK {path} reason=dirty")
            errors += 1
            continue
        if not worktree.branch:
            print(f"BLOCK {path} reason=detached-or-missing-branch")
            errors += 1
            continue

        merged = _is_ancestor(repo_root, worktree.branch, current_branch)
        audited_discard = worktree.branch in discard_unmerged
        if not merged and not audited_discard:
            print(
                f"BLOCK {path} branch={worktree.branch} "
                "reason=non-ancestor-explicit-disposition-required"
            )
            errors += 1
            continue
        if audited_discard:
            dispositions_used.add(worktree.branch)

        disposition = "MERGED" if merged else "DISCARD-AUDITED"
        print(f"REMOVE {path} branch={worktree.branch} disposition={disposition}")
        if not apply:
            continue

        run_git(repo_root, "worktree", "unlock", str(path), check=False)
        removed = run_git(
            repo_root,
            "worktree",
            "remove",
            str(path),
            "--force",
            "--force",
            check=False,
        )
        if removed.returncode != 0:
            detail = removed.stderr.strip() or removed.stdout.strip() or "unknown git error"
            print(f"ERROR {path} reason=remove-failed detail={detail}")
            errors += 1
            continue

        deleted = run_git(repo_root, "branch", "-D", worktree.branch, check=False)
        if deleted.returncode != 0:
            detail = deleted.stderr.strip() or deleted.stdout.strip() or "unknown git error"
            print(f"ERROR {worktree.branch} reason=branch-delete-failed detail={detail}")
            errors += 1

    unused_dispositions = discard_unmerged - dispositions_used
    for branch in sorted(unused_dispositions):
        print(f"BLOCK branch={branch} reason=audited-disposition-did-not-match-candidate")
        errors += 1

    if apply:
        pruned = run_git(repo_root, "worktree", "prune", check=False)
        if pruned.returncode != 0:
            detail = pruned.stderr.strip() or pruned.stdout.strip() or "unknown git error"
            print(f"ERROR reason=worktree-prune-failed detail={detail}")
            errors += 1
    return 1 if errors else 0


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="perform eligible removals")
    parser.add_argument(
        "--discard-unmerged",
        action="append",
        default=[],
        metavar="BRANCH",
        help="explicitly delete this audited non-ancestor branch after worktree removal",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    return cleanup(
        Path.cwd(),
        apply=args.apply,
        discard_unmerged=frozenset(args.discard_unmerged),
    )


if __name__ == "__main__":
    raise SystemExit(main())
