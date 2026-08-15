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


def test_opportunity_feed_chain_truth_profile_is_dedicated() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "opportunity-feed-chain-truth"})

    assert commands == {
        "planning": ["make", "planning-status"],
        "unit": [
            "uv",
            "run",
            "pytest",
            "tests/routing/test_opportunity_diagnosis.py",
            "-q",
        ],
        "integration": [
            "uv",
            "run",
            "pytest",
            "tests/cli/test_arbitrage_cli_process.py",
            "-k",
            "diagnose_feed",
            "-q",
        ],
        "cli": ["make", "docs-m1-check"],
        "restart": [
            "uv",
            "run",
            "pytest",
            "tests/m1-perception/test_m1_manual_contract.py",
            "-k",
            "opportunity_diagnosis",
            "-q",
        ],
    }


def test_opportunity_feed_cadence_sla_profile_is_fixture_only() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "opportunity-feed-cadence-sla"})

    assert tuple(commands) == ("planning", "unit", "integration", "cli", "restart")
    required_tests = {
        "tests/routing/test_neg_risk_quote_store.py",
        "tests/routing/test_neg_risk_quote_collector.py",
        "tests/routing/test_opportunity_scanner.py",
        "tests/m1-perception/test_arbitrage_opportunities_http.py",
    }
    flattened = [argument for command in commands.values() for argument in command]
    assert required_tests <= set(flattened)
    assert ["make", "-n", "collect-neg-risk-quotes"] in commands.values()
    assert ["make", "-n", "scan-arb-quotes"] in commands.values()
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "cron")
        )
    }


def test_l3_prerequisite_profile_uses_only_local_relevant_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "l3-prerequisite-chain-truth"})

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/alembic/test_006.py",
        "tests/storage/test_supabase_mirror.py",
        "tests/observation/test_l2_candidate_refresh.py",
        "tests/m1-perception/test_l3_promoter.py",
        "tests/m1-perception/test_l3_promote_dry_run.py",
        "tests/m1-perception/test_candidate_refresh_l3_protection.py",
    ):
        assert required in flattened
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "migrate")
        )
    }


def test_checkpointed_structure_recovery_profile_uses_bounded_local_gates() -> None:
    commands = eval_local.gate_commands_for({"paradigm": "checkpointed-structure-recovery"})

    flattened = [argument for command in commands.values() for argument in command]
    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/m1-perception/test_structure_generation_publication.py",
        "tests/m1-perception/test_control_plane_postgres.py",
        "tests/m1-perception/test_control_plane_shadow.py",
    ):
        assert required in flattened
    assert commands["unit"][-3:] == [
        "-k",
        "expired_read_budget or preserves_prior_checkpoint or certification_rejects",
        "-q",
    ]
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("http://", "https://", "flyctl", "deploy", "migrate")
        )
    }


def test_transactional_production_promotion_profile_uses_only_local_proof_gates() -> None:
    commands = eval_local.gate_commands_for(
        {"paradigm": "transactional-production-promotion"}
    )

    flattened = [argument for command in commands.values() for argument in command]

    assert commands["planning"] == ["make", "planning-status"]
    for required in (
        "tests/m1-perception/test_control_plane_postgres.py",
        "tests/m1-perception/test_control_plane_rollout.py",
        "tests/m1-perception/test_control_plane_shadow.py",
    ):
        assert required in flattened
    assert not {
        argument.lower()
        for argument in flattened
        if any(
            forbidden in argument.lower()
            for forbidden in ("flyctl", "deploy", "migrate", "http://", "https://")
        )
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


def test_main_without_manifest_preserves_legacy_direct_invocation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    run_dir = tmp_path / "legacy-run"
    run_dir.mkdir()
    executed: list[list[str]] = []

    def runner(command: list[str]) -> GateResult:
        executed.append(command)
        return GateResult(True, 0, "ok")

    monkeypatch.setattr(eval_local, "run_command", runner)

    assert eval_local.main([str(run_dir)]) == 0
    assert executed == list(GATE_COMMANDS.values())
    assert (run_dir / "local-eval.json").is_file()


def test_main_reports_malformed_manifest_without_running_gates(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    run_dir = tmp_path / "bad-run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{not json")
    monkeypatch.setattr(
        eval_local,
        "run_command",
        lambda command: pytest.fail(f"must not run gate: {command}"),
    )

    assert eval_local.main([str(run_dir)]) == 2
    assert "invalid climb manifest" in capsys.readouterr().err
    assert not (run_dir / "local-eval.json").exists()


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


def test_run_command_records_timeout_as_a_failed_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    def timeout(*args, **kwargs):
        raise subprocess.TimeoutExpired(args[0], kwargs["timeout"], output="partial output")

    monkeypatch.setattr(eval_local.subprocess, "run", timeout)

    result = eval_local.run_command(["pytest", "slow-test"])

    assert result.passed is False
    assert result.returncode == 124
    assert "timed out" in result.output


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
