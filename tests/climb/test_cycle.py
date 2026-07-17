from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

import yaml


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.climb.cycle import sync_cycle  # noqa: E402
from tools.climb.regen_tree import regenerate  # noqa: E402


RUN_HEADER = (
    "run_id,cycle,session,hypothesis_id,paradigm,parent_run,pushed_at,"
    "local_score,planning,unit,integration,cli,restart,push_decision,"
    "decision_reason,verdict,cost_h,manifest_path\n"
)


def _state_dir(tmp_path: Path) -> Path:
    state = tmp_path / "climb"
    state.mkdir()
    (state / "runs.csv").write_text(RUN_HEADER)
    (state / "hypotheses.yaml").write_text(
        yaml.safe_dump(
            {
                "hypotheses": [
                    {
                        "id": "H-001",
                        "description": "persist state",
                        "parent_paradigm": "repository",
                        "expected_lift": "+100",
                        "cost_h": 1.0,
                        "ranking": 1.0,
                        "status": "in-flight",
                        "created_at": "2026-07-17T00:00:00+08:00",
                        "results": [],
                    }
                ]
            },
            sort_keys=False,
        )
    )
    (state / "session-state.json").write_text(
        json.dumps(
            {
                "session": "test-session",
                "last_cycle": 0,
                "in_flight": {"hypothesis_id": "H-001"},
                "next_action": "evaluate H-001",
            }
        )
    )
    return state


def _completed_run(state: Path) -> dict:
    manifest = state.parent / "run" / "manifest.json"
    manifest.parent.mkdir()
    manifest.write_text(
        json.dumps(
            {
                "hypothesis_id": "H-001",
                "paradigm": "repository",
                "git_head": "abc123",
                "status": "ready-for-eval",
            }
        )
    )
    eval_path = manifest.parent / "local-eval.json"
    eval_path.write_text(
        json.dumps(
            {
                "total": 100.0,
                "subscores": {
                    "planning": 100.0,
                    "unit": 100.0,
                    "integration": 100.0,
                    "cli": 100.0,
                    "restart": 100.0,
                },
                "disaster_pattern": False,
                "commands": {},
            }
        )
    )
    return {
        "run_id": "20260717-climb-h001",
        "run_dir": str(manifest.parent),
        "manifest_path": str(manifest),
        "local_eval_path": str(eval_path),
        "decision": "PUSH",
        "reason": "all local gates passed",
        "cost_h": 1.0,
    }


def test_regen_tree_is_deterministic(tmp_path: Path) -> None:
    state = _state_dir(tmp_path)

    first = regenerate(state)
    second = regenerate(state)

    assert first == second
    assert "H-001" in first
    assert "In flight" in first
    assert (state / "research-tree.md").read_text() == first


def test_cycle_appends_exactly_one_run_and_advances_state(tmp_path: Path) -> None:
    state = _state_dir(tmp_path)

    sync_cycle(state, _completed_run(state))

    with (state / "runs.csv").open(newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    assert rows[0]["hypothesis_id"] == "H-001"
    assert rows[0]["local_score"] == "100.0"

    hypotheses = yaml.safe_load((state / "hypotheses.yaml").read_text())
    hypothesis = hypotheses["hypotheses"][0]
    assert hypothesis["status"] == "confirmed"
    assert len(hypothesis["results"]) == 1

    session = json.loads((state / "session-state.json").read_text())
    assert session["last_cycle"] == 1
    assert session["in_flight"] is None
    assert session["next_action"] == "rank next pending hypothesis"


def test_cycle_rejects_duplicate_run_id(tmp_path: Path) -> None:
    state = _state_dir(tmp_path)
    completed = _completed_run(state)
    sync_cycle(state, completed)

    try:
        sync_cycle(state, completed)
    except ValueError as exc:
        assert "duplicate run_id" in str(exc)
    else:
        raise AssertionError("duplicate run_id was accepted")
