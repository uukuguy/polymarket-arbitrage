from __future__ import annotations

import csv
import json
import os
import subprocess
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
STATE_DIR = ROOT / "docs/status/climb"


def test_config_declares_local_phase_gate_adapter() -> None:
    config = yaml.safe_load((STATE_DIR / "config.yaml").read_text())

    assert config == {
        "score_name": "phase_gate_score",
        "score_direction": "max",
        "subscores": ["planning", "unit", "integration", "cli", "restart"],
        "push_mode": "manual-csv",
        "state_dir": "docs/status/climb",
        "artifact_dir": "runs/climb",
        "run_tag_marker": "-climb-",
        "paradigm_field": "implementation_hypothesis",
    }


def test_state_files_exist_and_runs_header_matches_contract() -> None:
    required = {
        "session-target.md",
        "hypotheses.yaml",
        "runs.csv",
        "calibration.json",
        "pending-lb.json",
        "session-state.json",
        "adjudicator-log.md",
        "research-tree.json",
    }

    assert required <= {path.name for path in STATE_DIR.iterdir()}
    with (STATE_DIR / "runs.csv").open(newline="") as handle:
        assert next(csv.reader(handle)) == [
            "run_id",
            "cycle",
            "session",
            "hypothesis_id",
            "paradigm",
            "parent_run",
            "pushed_at",
            "local_score",
            "planning",
            "unit",
            "integration",
            "cli",
            "restart",
            "push_decision",
            "decision_reason",
            "verdict",
            "cost_h",
            "manifest_path",
        ]


def test_tracked_state_is_resumable_and_best_effort() -> None:
    hypotheses = yaml.safe_load((STATE_DIR / "hypotheses.yaml").read_text())
    session = json.loads((STATE_DIR / "session-state.json").read_text())
    target = (STATE_DIR / "session-target.md").read_text()
    with (STATE_DIR / "runs.csv").open(newline="") as handle:
        runs = list(csv.DictReader(handle))

    by_id = {item["id"]: item for item in hypotheses["hypotheses"]}
    assert by_id["H-001"]["status"] == "confirmed"
    assert by_id["H-001"]["results"]
    assert all(
        item["status"] in {"pending", "confirmed", "falsified"}
        for item in by_id.values()
    )
    assert isinstance(session["session"], str)
    assert session["session"].strip()
    assert session["last_cycle"] == len(runs)
    assert session["in_flight"] is None
    pending = [item for item in by_id.values() if item["status"] == "pending"]
    if pending:
        assert session["next_action"]
    else:
        assert session["next_action"] == "rank next pending hypothesis"
    assert "target_value:" in target


def test_run_artifacts_are_gitignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text()
    assert "runs/climb/" in gitignore.splitlines()


def test_required_adapter_scripts_are_executable() -> None:
    required = {
        "train.sh",
        "eval-local.sh",
        "cycle.sh",
        "push.sh",
        "apply-lb-score.sh",
        "consult-ais.sh",
    }
    scripts = ROOT / "tools/climb"

    for name in required:
        path = scripts / name
        assert path.is_file(), name
        assert os.access(path, os.X_OK), name


def test_push_creates_local_artifact_without_external_submission(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "hypothesis_id": "H-001",
                "paradigm": "repository",
                "git_head": "abc123",
                "status": "ready-for-eval",
            }
        )
    )
    (tmp_path / "local-eval.json").write_text(
        json.dumps({"total": 100.0, "subscores": {}})
    )

    completed = subprocess.run(
        ["bash", "tools/climb/push.sh", str(tmp_path)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    artifact = json.loads((tmp_path / "verification-artifact.json").read_text())
    assert artifact == {
        "external_submission": False,
        "git_head": "abc123",
        "hypothesis_id": "H-001",
        "local_score": 100.0,
    }


def test_external_score_and_consultation_commands_fail_closed() -> None:
    for script in ("apply-lb-score.sh", "consult-ais.sh"):
        completed = subprocess.run(
            ["bash", f"tools/climb/{script}"],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )
        assert completed.returncode != 0
        assert "disabled" in completed.stderr.lower()


def test_makefile_exposes_climb_entry_points() -> None:
    makefile = (ROOT / "Makefile").read_text()
    for target in ("climb-status:", "climb-cycle:", "climb-check:"):
        assert target in makefile


def test_regen_tree_script_runs_from_repository_root() -> None:
    completed = subprocess.run(
        ["uv", "run", "python", "tools/climb/regen-tree.py"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert "# Climb Research Tree" in completed.stdout
    assert (STATE_DIR / "research-tree.md").is_file()
