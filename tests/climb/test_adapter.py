from __future__ import annotations

import csv
import json
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


def test_initial_state_is_resumable_and_best_effort() -> None:
    hypotheses = yaml.safe_load((STATE_DIR / "hypotheses.yaml").read_text())
    session = json.loads((STATE_DIR / "session-state.json").read_text())
    target = (STATE_DIR / "session-target.md").read_text()

    assert hypotheses["hypotheses"][0]["id"] == "H-001"
    assert hypotheses["hypotheses"][0]["status"] == "pending"
    assert session == {
        "session": "2026-07-17-m2-position-persistence",
        "last_cycle": 0,
        "in_flight": None,
        "next_action": "run H-001",
    }
    assert "target_value:" in target


def test_run_artifacts_are_gitignored() -> None:
    gitignore = (ROOT / ".gitignore").read_text()
    assert "runs/climb/" in gitignore.splitlines()
