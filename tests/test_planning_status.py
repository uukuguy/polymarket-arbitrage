from __future__ import annotations

import json
import subprocess
from hashlib import sha256
from pathlib import Path
from unittest.mock import patch

from scripts import planning_status
from scripts.planning_status import _git_log_for


def test_uncommitted_plan_does_not_inherit_historical_matching_scopes() -> None:
    with patch("scripts.planning_status.subprocess.check_output", return_value="") as call:
        commits = _git_log_for("03", "01", Path("new/03-01-PLAN.md"))

    assert commits == []
    assert call.call_count >= 1
    assert "--diff-filter=A" in call.call_args_list[0].args[0]


def test_scoped_commits_are_searched_only_after_plan_creation() -> None:
    with patch(
        "scripts.planning_status.subprocess.check_output",
        side_effect=[
            "plan_creation_sha\n",
            "",
            "abc1234 feat(03-01): persist position state\n",
        ],
    ) as call:
        commits = _git_log_for("03", "01", Path("m2/03-01-PLAN.md"))

    assert commits == [("abc1234", "feat(03-01): persist position state")]
    commit_log_command = call.call_args_list[2].args[0]
    assert "plan_creation_sha..HEAD" in commit_log_command
    assert "--all" not in commit_log_command


def test_closed_plan_scope_stops_at_summary_creation() -> None:
    with patch(
        "scripts.planning_status.subprocess.check_output",
        side_effect=[
            "plan_creation_sha\n",
            "summary_creation_sha\n",
            "abc1234 feat(03-01): original workstream work\n",
        ],
    ) as call:
        commits = _git_log_for("03", "01", Path("m1/03-01-PLAN.md"))

    assert commits == [("abc1234", "feat(03-01): original workstream work")]
    commit_log_command = call.call_args_list[2].args[0]
    assert "plan_creation_sha..summary_creation_sha" in commit_log_command
    assert "HEAD" not in commit_log_command


def test_plan_without_summary_is_reported_as_drift(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _init_repo(tmp_path)
    phase_dir = tmp_path / ".planning/workstreams/m1-perception/phases/05.6-test"
    phase_dir.mkdir(parents=True)
    plan = phase_dir / "05.6-207-PLAN.md"
    plan.write_text("# Plan\n")
    assert _git(tmp_path, "add", ".").returncode == 0
    assert _git(tmp_path, "commit", "-qm", "docs(05.6-207): plan").returncode == 0
    (tmp_path / "implementation.py").write_text("value = 1\n")
    assert _git(tmp_path, "add", "implementation.py").returncode == 0
    assert _git(tmp_path, "commit", "-qm", "feat(05.6-207): implement").returncode == 0

    monkeypatch.chdir(tmp_path)

    rows = planning_status.collect()

    assert [(row.phase, row.plan, row.verdict) for row in rows] == [
        ("05.6", "207", "DRIFT")
    ]


def _git(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=repo,
        text=True,
        capture_output=True,
        check=False,
    )


def _init_repo(repo: Path) -> None:
    assert _git(repo, "init", "-q").returncode == 0
    assert _git(repo, "config", "user.email", "test@example.com").returncode == 0
    assert _git(repo, "config", "user.name", "Test User").returncode == 0


def test_registered_external_plan_summary_is_audited(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _init_repo(tmp_path)
    phase_dir = (
        tmp_path
        / ".planning/workstreams/m1-perception/phases/"
        / "05.6-self-healing-structure-production"
    )
    phase_dir.mkdir(parents=True)
    external_plan = tmp_path / "docs/superpowers/plans/plan-207.md"
    external_plan.parent.mkdir(parents=True)
    external_plan.write_text("# Plan 207\n")
    assert _git(tmp_path, "add", ".").returncode == 0
    assert _git(tmp_path, "commit", "-qm", "docs(05.6-207): plan").returncode == 0

    (tmp_path / "implementation.py").write_text("value = 1\n")
    assert _git(tmp_path, "add", "implementation.py").returncode == 0
    assert _git(tmp_path, "commit", "-qm", "feat(05.6-207): implement").returncode == 0

    summary = phase_dir / "05.6-207-SUMMARY.md"
    summary.write_text("---\nplan-source: docs/superpowers/plans/plan-207.md\n---\n")
    assert _git(tmp_path, "add", str(summary.relative_to(tmp_path))).returncode == 0
    assert _git(tmp_path, "commit", "-qm", "docs(05.6-207): summarize").returncode == 0

    monkeypatch.chdir(tmp_path)

    rows = planning_status.collect()

    assert [(row.phase, row.plan, row.verdict) for row in rows] == [
        ("05.6", "207", "OK")
    ]


def test_registered_external_plan_summary_drifts_when_anchor_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    _init_repo(tmp_path)
    phase_dir = (
        tmp_path
        / ".planning/workstreams/m1-perception/phases/"
        / "05.6-self-healing-structure-production"
    )
    phase_dir.mkdir(parents=True)
    summary = phase_dir / "05.6-207-SUMMARY.md"
    summary.write_text("---\nplan-source: docs/superpowers/plans/missing-207.md\n---\n")
    assert _git(tmp_path, "add", ".").returncode == 0
    assert _git(tmp_path, "commit", "-qm", "docs(05.6-207): orphan summary").returncode == 0

    monkeypatch.chdir(tmp_path)

    rows = planning_status.collect()

    assert [(row.phase, row.plan, row.verdict) for row in rows] == [
        ("05.6", "207", "DRIFT")
    ]


def test_evidence_hash_gate_recomputes_named_artifacts_and_rejects_stale_bytes(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "deploy/control-plane/runtime.toml.template"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("version = 1\n")
    evidence = tmp_path / ".planning/evidence/runtime-observe-only.json"
    evidence.parent.mkdir(parents=True)
    evidence.write_text(
        json.dumps(
            {
                "status": "not-run",
                "local_implementation": {
                    "reviewed_artifacts": [
                        {
                            "path": "deploy/control-plane/runtime.toml.template",
                            "sha256": sha256(artifact.read_bytes()).hexdigest(),
                        }
                    ]
                },
            }
        )
    )

    assert planning_status.verify_evidence_hashes(tmp_path, (evidence,)) == []

    artifact.write_text("version = 2\n")

    assert planning_status.verify_evidence_hashes(tmp_path, (evidence,)) == [
        ".planning/evidence/runtime-observe-only.json: stale SHA256 for "
        "deploy/control-plane/runtime.toml.template"
    ]
