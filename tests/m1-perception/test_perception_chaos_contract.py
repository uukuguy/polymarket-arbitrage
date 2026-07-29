import json
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "scripts/perception_chaos.py"
FAULT_IDS = (
    "gamma-timeout",
    "gamma-partial",
    "gamma-malformed",
    "gamma-cursor",
    "clob-missing-leg",
    "clob-429",
    "clob-latency",
    "candidate-exit",
    "discovery-exit",
    "reconciliation-stall",
    "sqlite-busy",
    "disk-pressure",
    "telegram-failure",
    "daemon-restart",
    "deploy-interrupt",
    "contention",
)


@pytest.mark.parametrize("fault_id", FAULT_IDS)
def test_every_fault_has_a_complete_readonly_plan(fault_id: str) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", fault_id],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert plan["fault_id"] == fault_id
    assert plan["component"]
    assert plan["expected_incident_kind"]
    assert plan["recovery_writer"]
    assert plan["cleanup"]
    assert plan["required_tools"] == ["python"]
    assert plan["image_check"] == "make chaos-l2-fly-image-check"
    assert plan["execute_supported"] is False


def test_execute_fails_before_mutation_when_adapter_is_not_ready(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "execute",
            "--fault",
            "gamma-timeout",
            "--expected-release",
            "a" * 40,
            "--authorization",
            f"fault:gamma-timeout:{'a' * 40}",
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "adapter-not-implemented" in result.stderr
    assert not (tmp_path / "evidence").exists()


@pytest.mark.parametrize("fault_id", FAULT_IDS)
def test_every_fault_has_a_plan_only_make_entry(fault_id: str) -> None:
    target = f"chaos-perception-{fault_id}"
    result = subprocess.run(
        ["make", "-s", target],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["fault_id"] == fault_id

    help_result = subprocess.run(
        ["make", "-s", "help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert target in help_result.stdout


def test_make_exposes_release_bound_recovery_verifier() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "verify-perception-recovery",
            "evidence=evidence.json",
            "output=verdict.json",
            f"expected_release={'a' * 40}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "perception_fault_acceptance.py" in result.stdout
    assert "--require-scope production-fault" in result.stdout
    assert f'--expected-release \"{"a" * 40}\"' in result.stdout
