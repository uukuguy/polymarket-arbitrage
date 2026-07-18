from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

import yaml
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.climb.cycle import (  # noqa: E402
    DIAGNOSE_FEED_COMMAND,
    collect_opportunity_feed_evidence,
    record_production_evidence_after_gates,
    sync_cycle,
)
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
    assert b"\r\n" not in (state / "runs.csv").read_bytes()

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


def test_opportunity_feed_cycle_cannot_confirm_without_production_evidence(
    tmp_path: Path,
) -> None:
    """A local-only 100-point H-008 run must leave durable state untouched."""
    state = _state_dir(tmp_path)
    completed = _completed_run(state)
    manifest_path = Path(completed["manifest_path"])
    manifest = json.loads(manifest_path.read_text())
    manifest["paradigm"] = "opportunity-feed-chain-truth"
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(ValueError, match="production evidence"):
        sync_cycle(state, completed)

    with (state / "runs.csv").open(newline="") as handle:
        assert list(csv.DictReader(handle)) == []
    hypotheses = yaml.safe_load((state / "hypotheses.yaml").read_text())
    assert hypotheses["hypotheses"][0]["status"] == "in-flight"


def test_opportunity_feed_evidence_runs_one_diagnostic_before_confirmation(
    tmp_path: Path,
) -> None:
    """The one diagnostic is recorded before state can be confirmed."""
    state = _state_dir(tmp_path)
    completed = _completed_run(state)
    manifest_path = Path(completed["manifest_path"])
    manifest = json.loads(manifest_path.read_text())
    manifest["paradigm"] = "opportunity-feed-chain-truth"
    manifest_path.write_text(json.dumps(manifest))
    invocations: list[list[str]] = []

    class Result:
        returncode = 2
        stdout = json.dumps(
            {
                "http_status": 503,
                "kind": "stale-snapshot",
                "reason": "snapshot-age-exceeded",
            }
        )
        stderr = ""

    def runner(command: list[str]) -> Result:
        invocations.append(command)
        return Result()

    evidence_path = collect_opportunity_feed_evidence(
        Path(completed["run_dir"]),
        manifest,
        runner=runner,
        observed_at="2026-07-19T00:00:00Z",
    )

    assert invocations == [DIAGNOSE_FEED_COMMAND]
    evidence = json.loads(evidence_path.read_text())
    assert evidence["command"] == {
        "argv": DIAGNOSE_FEED_COMMAND,
        "count": 1,
        "returncode": 2,
    }
    assert evidence["classification"] == evidence["response"]["kind"]
    assert evidence["reason"] == evidence["response"]["reason"]
    hypotheses = yaml.safe_load((state / "hypotheses.yaml").read_text())
    assert hypotheses["hypotheses"][0]["status"] == "in-flight"

    sync_cycle(state, completed)

    hypotheses = yaml.safe_load((state / "hypotheses.yaml").read_text())
    assert hypotheses["hypotheses"][0]["status"] == "confirmed"
    assert hypotheses["hypotheses"][0]["results"][0]["decision_reason"] == (
        "all local gates passed; production evidence recorded"
    )


def test_opportunity_feed_evidence_never_invokes_production_before_gates(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    manifest = {
        "hypothesis_id": "H-008",
        "paradigm": "opportunity-feed-chain-truth",
    }
    invocations: list[list[str]] = []

    with pytest.raises(ValueError, match="local gates"):
        record_production_evidence_after_gates(
            run_dir,
            manifest,
            local_gates_passed=False,
            runner=lambda command: invocations.append(command),
        )

    assert invocations == []
