#!/usr/bin/env python3
"""planning_status — index of .planning/ vs git reality.

Surfaces the "code shipped but doc didn't follow" class of drift.
For every PLAN.md under .planning/workstreams/*/phases/, plus explicitly
registered external-plan summaries, reports:
  - Whether SUMMARY.md exists
  - Which commits in `git log --grep` reference this plan's scope (feat/fix/refactor)
  - Consistency verdict: OK / NO-SUMMARY / NO-CODE / OK-NO-COMMITS

Run as `make planning-status` (entry added to Makefile separately).

This script is read-only — it never writes files. It exists because
file-system inconsistency is invisible until someone explicitly looks.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any

PLANNING_ROOT = Path(".planning")
REQUIRED_RUNTIME_OBSERVE_EVIDENCE = Path(
    ".planning/workstreams/m1-perception/phases/"
    "05.6-self-healing-structure-production/evidence/runtime-observe-only.json"
)

# Match a plan filename like 01.1-04-PLAN.md or 02-3-PLAN.md
PLAN_RX = re.compile(r"^(?P<phase>\d+(?:\.\d+)?)-(?P<plan>\d+)-PLAN\.md$")
SUMMARY_RX = re.compile(r"^(?P<phase>\d+(?:\.\d+)?)-(?P<plan>\d+)-SUMMARY\.md$")
SUMMARY_TPL = "{phase}-{plan}-SUMMARY.md"

# Match commit subject scope like feat(01.1-04) / fix(02-3) — tolerates leading
# whitespace from `git log --pretty` formats and any conventional-commit type.
COMMIT_SCOPE_RX = re.compile(
    r"^[a-f0-9]+\s+(?:feat|fix|refactor|test|perf|chore|docs)"
    r"\((?P<phase>\d+(?:\.\d+)?)-(?P<plan>\d+)\):"
)


# ANSI helpers (terminal-only; no rich dep so this stays portable in CI)
def _c(code: str, text: str) -> str:
    if not sys.stdout.isatty():
        return text
    return f"\033[{code}m{text}\033[0m"


green = lambda t: _c("32", t)  # noqa: E731
yellow = lambda t: _c("33", t)  # noqa: E731
red = lambda t: _c("31", t)  # noqa: E731
dim = lambda t: _c("2", t)  # noqa: E731
bold = lambda t: _c("1", t)  # noqa: E731


@dataclass
class PlanRow:
    workstream: str
    phase_dir: Path
    phase: str
    plan: str
    plan_md: Path
    summary_md: Path
    summary_exists: bool
    commits: list[tuple[str, str]]  # [(short_hash, subject), ...]
    anchor_exists: bool = True

    @property
    def has_code_commit(self) -> bool:
        # Any feat/fix/refactor/test/perf scoped commit means code shipped.
        # `subj` is the bare subject line (no hash prefix).
        return any(
            re.match(r"^(?:feat|fix|refactor|test|perf)\(", subj) for _, subj in self.commits
        )

    @property
    def verdict(self) -> str:
        if not self.anchor_exists:
            return "DRIFT"  # registered external SUMMARY points at no plan-side anchor
        if self.summary_exists and self.has_code_commit:
            return "OK"
        if self.summary_exists and not self.has_code_commit:
            return "OK-NO-COMMITS"  # SUMMARY exists but no scoped feat commit (rare)
        if not self.summary_exists and self.has_code_commit:
            return "DRIFT"  # code shipped, no SUMMARY — the bad case
        return "NOT-STARTED"  # PLAN exists, no SUMMARY, no code

    @property
    def verdict_painted(self) -> str:
        v = self.verdict
        if v == "OK":
            return green("OK")
        if v == "DRIFT":
            return red("DRIFT")
        if v == "OK-NO-COMMITS":
            return yellow("OK-NO-COMMITS")
        return dim("NOT-STARTED")


@dataclass(frozen=True, slots=True)
class _GitCommit:
    sha: str
    short_sha: str
    parents: tuple[str, ...]
    subject: str
    changes: tuple[tuple[str, ...], ...]


@dataclass(slots=True)
class _GitHistory:
    """One immutable Git read indexed for every planning row."""

    commits: tuple[_GitCommit, ...]
    creation_by_path: dict[str, str]
    _ancestors_by_sha: dict[str, frozenset[str]]

    @property
    def head_sha(self) -> str | None:
        return None if not self.commits else self.commits[0].sha

    def creation_sha(self, path: Path) -> str | None:
        return self.creation_by_path.get(path.as_posix())

    def ancestors(self, sha: str) -> frozenset[str]:
        cached = self._ancestors_by_sha.get(sha)
        if cached is not None:
            return cached
        parents_by_sha = {commit.sha: commit.parents for commit in self.commits}
        reached: set[str] = set()
        pending = [sha]
        while pending:
            current = pending.pop()
            if current in reached:
                continue
            reached.add(current)
            pending.extend(parents_by_sha.get(current, ()))
        frozen = frozenset(reached)
        self._ancestors_by_sha[sha] = frozen
        return frozen

    def scoped_commits(
        self,
        *,
        phase: str,
        plan: str,
        plan_md: Path,
        summary_md: Path,
    ) -> list[tuple[str, str]]:
        plan_creation_sha = self.creation_sha(plan_md)
        if plan_creation_sha is None:
            return []
        upper_sha = self.creation_sha(summary_md) or self.head_sha
        if upper_sha is None:
            return []
        eligible = self.ancestors(upper_sha) - self.ancestors(plan_creation_sha)
        scope = re.compile(rf"^[a-z]+\({re.escape(phase)}-{re.escape(plan)}\):")
        return [
            (commit.short_sha, commit.subject)
            for commit in self.commits
            if commit.sha in eligible and scope.match(commit.subject)
        ]


def _load_git_history() -> _GitHistory:
    """Read commit graph, subjects, additions and renames in one subprocess."""
    try:
        out = subprocess.check_output(
            [
                "git",
                "log",
                "--format=%x1e%H%x1f%h%x1f%P%x1f%s",
                "--name-status",
                "--find-renames",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError):
        return _GitHistory((), {}, {})

    commits: list[_GitCommit] = []
    for block in out.split("\x1e"):
        lines = block.lstrip("\n").splitlines()
        if not lines:
            continue
        fields = lines[0].split("\x1f", 3)
        if len(fields) != 4:
            continue
        sha, short_sha, parents, subject = fields
        changes: list[tuple[str, ...]] = []
        for line in lines[1:]:
            if not line:
                continue
            parts = tuple(line.split("\t"))
            if len(parts) >= 2:
                changes.append(parts)
        commits.append(
            _GitCommit(
                sha=sha,
                short_sha=short_sha,
                parents=tuple(parent for parent in parents.split() if parent),
                subject=subject,
                changes=tuple(changes),
            )
        )

    aliases: dict[str, str] = {}

    def find(path: str) -> str:
        root = aliases.setdefault(path, path)
        while aliases[root] != root:
            root = aliases[root]
        while aliases[path] != path:
            parent = aliases[path]
            aliases[path] = root
            path = parent
        return root

    def union(left: str, right: str) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            aliases[right_root] = left_root

    for commit in commits:
        for change in commit.changes:
            if change[0].startswith("R") and len(change) == 3:
                union(change[1], change[2])

    oldest_addition_by_alias: dict[str, tuple[int, str]] = {}
    for index, commit in enumerate(commits):
        for change in commit.changes:
            if change[0] != "A" or len(change) != 2:
                continue
            root = find(change[1])
            prior = oldest_addition_by_alias.get(root)
            if prior is None or index > prior[0]:
                oldest_addition_by_alias[root] = (index, commit.sha)
    creation_by_path = {
        path: oldest_addition_by_alias[root][1]
        for path in aliases
        if (root := find(path)) in oldest_addition_by_alias
    }
    return _GitHistory(tuple(commits), creation_by_path, {})


def _git_log_for(
    phase: str,
    plan: str,
    plan_md: Path,
    summary_md: Path | None = None,
    *,
    history: _GitHistory | None = None,
) -> list[tuple[str, str]]:
    """Return scoped commits inside this exact plan's documented lifetime.

    Plan numbers are only unique inside a workstream. Searching ``--all`` for
    ``feat(03-01)`` lets an older M1 plan make a new M2 plan look shipped. The
    plan creation is the lower boundary; once its SUMMARY is committed, SUMMARY
    creation is the upper boundary. A later workstream may safely reuse the
    numeric scope. An uncommitted plan has no code commits by definition.
    """
    if history is not None:
        return history.scoped_commits(
            phase=phase,
            plan=plan,
            plan_md=plan_md,
            summary_md=summary_md
            or plan_md.with_name(SUMMARY_TPL.format(phase=phase, plan=plan)),
        )

    pattern = rf"^[a-z]+\({re.escape(phase)}-{re.escape(plan)}\):"

    def creation_sha(path: Path) -> str | None:
        creation_out = subprocess.check_output(
            [
                "git",
                "log",
                "--follow",
                "--diff-filter=A",
                "--format=%H",
                "--",
                str(path),
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        commits = [line for line in creation_out.splitlines() if line]
        return commits[-1] if commits else None

    try:
        plan_creation_sha = creation_sha(plan_md)
        if plan_creation_sha is None:
            return []
        effective_summary_md = summary_md or plan_md.with_name(
            SUMMARY_TPL.format(phase=phase, plan=plan)
        )
        summary_creation_sha = creation_sha(effective_summary_md)
        upper_bound = summary_creation_sha or "HEAD"
        out = subprocess.check_output(
            [
                "git",
                "log",
                f"{plan_creation_sha}..{upper_bound}",
                "-E",
                f"--grep={pattern}",
                "--pretty=%h %s",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        )
    except subprocess.CalledProcessError:
        return []
    rows: list[tuple[str, str]] = []
    for line in out.splitlines():
        line = line.strip()
        if not line:
            continue
        h, _, subj = line.partition(" ")
        rows.append((h, subj))
    return rows


def _registered_external_plan_source(summary_md: Path) -> Path | None:
    """Return an explicit external plan anchor from SUMMARY frontmatter."""

    try:
        lines = summary_md.read_text().splitlines()
    except OSError:
        return None
    if not lines or lines[0].strip() != "---":
        return None
    for line in lines[1:80]:
        stripped = line.strip()
        if stripped == "---":
            return None
        if not stripped.startswith("plan-source:"):
            continue
        value = stripped.removeprefix("plan-source:").strip().strip("'\"")
        if not value:
            return None
        return Path(value)
    return None


def collect() -> list[PlanRow]:
    rows: list[PlanRow] = []
    if not PLANNING_ROOT.exists():
        return rows

    workstreams_root = PLANNING_ROOT / "workstreams"
    search_roots: list[tuple[str, Path]] = []
    if workstreams_root.exists():
        for ws_dir in sorted(workstreams_root.iterdir()):
            phases_dir = ws_dir / "phases"
            if phases_dir.exists():
                search_roots.append((ws_dir.name, phases_dir))
    # Legacy non-workstream layout
    legacy_phases = PLANNING_ROOT / "phases"
    if legacy_phases.exists():
        search_roots.append(("(legacy)", legacy_phases))

    history = _load_git_history()

    seen: set[tuple[Path, str, str]] = set()
    for ws_name, phases_dir in search_roots:
        for phase_dir in sorted(phases_dir.iterdir()):
            if not phase_dir.is_dir():
                continue
            for plan_md in sorted(phase_dir.iterdir()):
                m = PLAN_RX.match(plan_md.name)
                if not m:
                    continue
                phase = m.group("phase")
                plan = m.group("plan")
                summary_md = phase_dir / SUMMARY_TPL.format(phase=phase, plan=plan)
                seen.add((phase_dir, phase, plan))
                rows.append(
                    PlanRow(
                        workstream=ws_name,
                        phase_dir=phase_dir,
                        phase=phase,
                        plan=plan,
                        plan_md=plan_md,
                        summary_md=summary_md,
                        summary_exists=summary_md.exists(),
                        commits=_git_log_for(phase, plan, plan_md, history=history),
                    )
                )
            for summary_md in sorted(phase_dir.iterdir()):
                m = SUMMARY_RX.match(summary_md.name)
                if not m:
                    continue
                phase = m.group("phase")
                plan = m.group("plan")
                if (phase_dir, phase, plan) in seen:
                    continue
                plan_source = _registered_external_plan_source(summary_md)
                if plan_source is None:
                    continue
                anchor_exists = plan_source.exists()
                rows.append(
                    PlanRow(
                        workstream=ws_name,
                        phase_dir=phase_dir,
                        phase=phase,
                        plan=plan,
                        plan_md=plan_source,
                        summary_md=summary_md,
                        summary_exists=True,
                        commits=(
                            _git_log_for(
                                phase,
                                plan,
                                plan_source,
                                summary_md,
                                history=history,
                            )
                            if anchor_exists
                            else []
                        ),
                        anchor_exists=anchor_exists,
                    )
                )
    return rows


def verify_evidence_hashes(
    project_root: Path = Path("."),
    evidence_files: tuple[Path, ...] | None = None,
) -> list[str]:
    """Recompute every explicitly named evidence artifact SHA256."""

    root = project_root.resolve()
    files = evidence_files or (REQUIRED_RUNTIME_OBSERVE_EVIDENCE,)
    errors: list[str] = []
    for evidence_file in files:
        evidence_path = evidence_file if evidence_file.is_absolute() else root / evidence_file
        evidence_label = _project_label(root, evidence_path)
        try:
            payload: Any = json.loads(evidence_path.read_text())
        except FileNotFoundError:
            errors.append(f"{evidence_label}: required evidence missing")
            continue
        except OSError:
            errors.append(f"{evidence_label}: required evidence unreadable")
            continue
        except json.JSONDecodeError:
            errors.append(f"{evidence_label}: invalid JSON")
            continue
        if not isinstance(payload, dict):
            errors.append(f"{evidence_label}: evidence must be an object")
            continue
        local = payload.get("local_implementation")
        if not isinstance(local, dict):
            errors.append(f"{evidence_label}: local_implementation must be an object")
            continue
        reviewed = local.get("reviewed_artifacts")
        if not isinstance(reviewed, list) or not reviewed:
            errors.append(f"{evidence_label}: reviewed_artifacts must be a non-empty list")
            continue
        for entry in reviewed:
            if not isinstance(entry, dict):
                errors.append(f"{evidence_label}: invalid reviewed artifact entry")
                continue
            named_path = entry.get("path")
            expected_hash = entry.get("sha256")
            if not isinstance(named_path, str) or not isinstance(expected_hash, str):
                errors.append(f"{evidence_label}: invalid reviewed artifact entry")
                continue
            relative_path = Path(named_path)
            if relative_path.is_absolute() or ".." in relative_path.parts:
                errors.append(f"{evidence_label}: non-canonical artifact path {named_path}")
                continue
            artifact_path = (root / relative_path).resolve()
            try:
                artifact_path.relative_to(root)
                actual_hash = sha256(artifact_path.read_bytes()).hexdigest()
            except (OSError, ValueError):
                errors.append(f"{evidence_label}: missing artifact {named_path}")
                continue
            if actual_hash != expected_hash:
                errors.append(f"{evidence_label}: stale SHA256 for {named_path}")
    return errors


def _project_label(root: Path, path: Path) -> str:
    try:
        return path.resolve().relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def render(rows: list[PlanRow]) -> int:
    if not rows:
        print("No PLAN.md files found under .planning/")
        return 0

    drift_count = sum(1 for r in rows if r.verdict == "DRIFT")
    print()
    print(
        bold(
            f"Planning status — {len(rows)} plans across "
            f"{len({r.workstream for r in rows})} workstreams"
        )
    )
    print()

    current_ws = None
    current_phase = None
    for r in rows:
        if r.workstream != current_ws:
            current_ws = r.workstream
            current_phase = None
            print(bold(f"  workstream: {current_ws}"))
        if r.phase_dir.name != current_phase:
            current_phase = r.phase_dir.name
            print(f"    {dim(current_phase)}")
        n_commits = len(r.commits)
        n_str = f"{n_commits} commit{'s' if n_commits != 1 else ''}"
        summary_marker = "✓" if r.summary_exists else "✗"
        summary_marker_painted = green(summary_marker) if r.summary_exists else red(summary_marker)
        print(
            f"      plan {r.phase}-{r.plan:>2}  "
            f"SUMMARY {summary_marker_painted}  "
            f"{n_str:<10}  "
            f"→ {r.verdict_painted}"
        )
        if r.verdict == "DRIFT":
            for h, subj in r.commits[:3]:
                print(f"        {dim(h)} {subj[:80]}")
    print()
    if drift_count:
        print(
            red(
                f"⚠ {drift_count} plan{'s' if drift_count != 1 else ''} in DRIFT — "
                "code shipped but SUMMARY missing."
            )
        )
        print(red("  Fix: write the SUMMARY, then commit."))
        print()
        return 1
    print(green("✓ no drift detected — every shipped plan has a SUMMARY."))
    print()
    return 0


def main() -> int:
    plan_status = render(collect())
    hash_errors = verify_evidence_hashes()
    if hash_errors:
        print(red(f"⚠ {len(hash_errors)} stale or invalid evidence artifact hash(es)."))
        for error in hash_errors:
            print(red(f"  {error}"))
        print()
        return 1
    print(green("✓ reviewed evidence artifact hashes match current bytes."))
    print()
    return plan_status


if __name__ == "__main__":
    sys.exit(main())
