from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tools.climb import eval_local  # noqa: E402
from tools.climb.eval_local import (  # noqa: E402
    GATE_COMMANDS,
    GateResult,
    build_score,
    evaluate_gates,
)


def test_living_doc_contract_selects_focused_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "living-doc-contract"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-q",
        ],
        "integration": ["make", "docs-m1-check"],
        "cli": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_makefile_contract.py",
            "tests/test_makefile.py",
            "-q",
        ],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-k",
            "precommit",
            "-q",
        ],
    }


def test_unknown_or_missing_paradigm_uses_existing_gate_profile() -> None:
    assert eval_local.gate_commands_for({"paradigm": "repository"}) == GATE_COMMANDS
    assert eval_local.gate_commands_for({"paradigm": "unknown"}) == GATE_COMMANDS
    assert eval_local.gate_commands_for({}) == GATE_COMMANDS


def test_main_selects_gates_from_run_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(json.dumps({"paradigm": "living-doc-contract"}))
    executed: list[list[str]] = []

    def runner(command: list[str]) -> GateResult:
        executed.append(command)
        return GateResult(True, 0, "ok")

    monkeypatch.setattr(eval_local, "run_command", runner)
    monkeypatch.setattr(sys, "argv", ["eval_local.py", str(run_dir)])

    assert eval_local.main() == 0
    assert executed == list(
        eval_local.gate_commands_for({"paradigm": "living-doc-contract"}).values()
    )
    payload = json.loads((run_dir / "local-eval.json").read_text())
    assert payload["total"] == 100.0


def test_score_is_mean_of_five_binary_gates() -> None:
    results = {
        "planning": GateResult(True, 0, "ok"),
        "unit": GateResult(True, 0, "ok"),
        "integration": GateResult(False, 1, "failed"),
        "cli": GateResult(True, 0, "ok"),
        "restart": GateResult(False, 1, "failed"),
    }

    payload = build_score(results)

    assert payload["total"] == 60.0
    assert payload["subscores"] == {
        "planning": 100.0,
        "unit": 100.0,
        "integration": 0.0,
        "cli": 100.0,
        "restart": 0.0,
    }
    assert payload["disaster_pattern"] is True


def test_all_green_score_is_100_without_disaster() -> None:
    results = {
        name: GateResult(True, 0, "ok")
        for name in ("planning", "unit", "integration", "cli", "restart")
    }

    payload = build_score(results)

    assert payload["total"] == 100.0
    assert payload["disaster_pattern"] is False


def test_evaluate_gates_records_bounded_command_evidence(tmp_path: Path) -> None:
    commands = {
        "planning": ["fake", "planning"],
        "unit": ["fake", "unit"],
    }

    def runner(command: list[str]) -> GateResult:
        return GateResult(
            passed=command[-1] == "planning",
            returncode=0 if command[-1] == "planning" else 1,
            output="x" * 20_000,
        )

    output_path = tmp_path / "local-eval.json"
    payload = evaluate_gates(commands, runner=runner, output_path=output_path)

    assert payload["subscores"] == {"planning": 100.0, "unit": 0.0}
    assert payload["total"] == 50.0
    assert len(payload["commands"]["planning"]["output"]) == 8_000
    assert json.loads(output_path.read_text()) == payload


def test_train_script_is_compatible_with_system_bash(tmp_path: Path) -> None:
    env = os.environ.copy()
    env["CLIMB_ARTIFACT_DIR"] = str(tmp_path)

    completed = subprocess.run(
        ["bash", "tools/climb/train.sh", "H-001"],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    run_dir = Path(completed.stdout.strip())
    manifest = json.loads((run_dir / "manifest.json").read_text())
    assert manifest["hypothesis_id"] == "H-001"
    assert manifest["status"] == "ready-for-eval"
