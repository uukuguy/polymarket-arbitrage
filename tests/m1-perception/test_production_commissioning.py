from __future__ import annotations

import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from polyarb.control_plane.production_commissioning import (
    ATTACK_CONTRACTS,
    PRODUCTION_CHAIN,
    CommissioningEvidenceError,
    build_commissioning_plan,
    verify_attack_proof,
    verify_commissioning_evidence,
)
from polyarb.control_plane.runtime_contract import RUNTIME_STAGE_REGISTRY
from polyarb.control_plane.runtime_deadlines import RUNTIME_JOB_ORDER

ROOT = Path(__file__).parents[2]


def test_commissioning_registry_covers_the_exact_runtime_dag() -> None:
    assert tuple(PRODUCTION_CHAIN) == RUNTIME_JOB_ORDER
    assert tuple(PRODUCTION_CHAIN) == tuple(RUNTIME_STAGE_REGISTRY)
    assert PRODUCTION_CHAIN["structure-fetch"].upstream_nodes == ()
    assert PRODUCTION_CHAIN["opportunity-certify"].upstream_nodes == ("quote-certify",)

    for node_id, node in PRODUCTION_CHAIN.items():
        assert node.node_id == node_id
        assert node.owner_role in {"structure-worker", "quote-worker"}
        assert node.required_input
        assert node.success_fact
        assert node.liveness_fact == "m1_job_runtime_state"
        assert node.business_postcondition
        assert node.required_attacks
        assert set(node.required_attacks) <= set(ATTACK_CONTRACTS)


def test_every_attack_is_an_executable_closed_loop_contract() -> None:
    covered_nodes: set[str] = set()
    for attack_id, attack in ATTACK_CONTRACTS.items():
        assert attack.attack_id == attack_id
        assert attack.targets
        assert set(attack.targets) <= set(PRODUCTION_CHAIN)
        assert attack.injector
        assert attack.detector
        assert attack.recovery_action
        assert attack.postcondition
        assert attack.execution_scope in {
            "disposable-exact-image",
            "production-canary",
        }
        assert attack.qualification_impact in {"pause", "block", "invalidate"}
        covered_nodes.update(attack.targets)
    assert covered_nodes == set(PRODUCTION_CHAIN)


def test_standalone_attack_proof_verifier_rejects_identity_drift() -> None:
    evidence = _complete_evidence()
    node_id = "structure-fetch"
    attack_id = PRODUCTION_CHAIN[node_id].required_attacks[0]
    nodes = evidence["nodes"]
    assert isinstance(nodes, dict)
    node = nodes[node_id]
    assert isinstance(node, dict)
    attacks = node["attacks"]
    assert isinstance(attacks, dict)
    proof = attacks[attack_id]

    verified_at = verify_attack_proof(
        proof,
        node_id=node_id,
        attack_id=attack_id,
        expected_release="a" * 40,
        expected_config="sha256:" + "b" * 64,
    )
    assert verified_at.tzinfo is not None

    assert isinstance(proof, dict)
    proof["config_id"] = "sha256:" + "c" * 64
    with pytest.raises(CommissioningEvidenceError, match="attack-proof-identity"):
        verify_attack_proof(
            proof,
            node_id=node_id,
            attack_id=attack_id,
            expected_release="a" * 40,
            expected_config="sha256:" + "b" * 64,
        )


def _complete_evidence() -> dict[str, object]:
    release_id = "a" * 40
    config_id = "sha256:" + "b" * 64
    started = datetime(2030, 1, 1, tzinfo=UTC)
    nodes: dict[str, object] = {}
    for node_index, (node_id, node) in enumerate(PRODUCTION_CHAIN.items()):
        normal_at = started + timedelta(seconds=node_index * 10)
        attacks: dict[str, object] = {}
        for attack_index, attack_id in enumerate(node.required_attacks):
            injected = normal_at + timedelta(seconds=attack_index + 1)
            attacks[attack_id] = {
                "experiment_id": f"commission:{node_id}:{attack_id}",
                "release_id": release_id,
                "config_id": config_id,
                "node_id": node_id,
                "attack_id": attack_id,
                "injected_at": injected.isoformat(),
                "detected_at": (injected + timedelta(seconds=1)).isoformat(),
                "recovery_started_at": (injected + timedelta(seconds=2)).isoformat(),
                "recovered_at": (injected + timedelta(seconds=3)).isoformat(),
                "verified_at": (injected + timedelta(seconds=4)).isoformat(),
                "detector_fact_id": f"detector:{node_id}:{attack_id}",
                "recovery_action_id": f"action:{node_id}:{attack_id}",
                "recovery_fact_id": f"recovery:{node_id}:{attack_id}",
                "postcondition_fact_id": f"postcondition:{node_id}:{attack_id}",
                "cleanup_verified": True,
                "qualification_impact": ATTACK_CONTRACTS[attack_id].qualification_impact,
            }
        nodes[node_id] = {
            "normal_turn": {
                "attempt_id": f"attempt:{node_id}",
                "terminal_fact_id": f"terminal:{node_id}",
                "success_fact_id": f"success:{node_id}",
                "postcondition_fact_id": f"normal-postcondition:{node_id}",
                "succeeded_at": normal_at.isoformat(),
            },
            "attacks": attacks,
        }
    return {
        "schema_version": "m1-production-commissioning-v1",
        "release_id": release_id,
        "config_id": config_id,
        "nodes": nodes,
        "end_to_end": {
            "lineage_id": "structure:generation-a",
            "structure_pointer_fact_id": "pointer:structure:generation-a",
            "quote_pointer_fact_id": "pointer:quote:generation-a",
            "opportunity_pointer_fact_id": "pointer:opportunity:generation-a",
            "verified_at": (started + timedelta(minutes=10)).isoformat(),
        },
    }


def test_complete_attack_evidence_commissions_every_node() -> None:
    evidence = _complete_evidence()

    result = verify_commissioning_evidence(
        evidence,
        expected_release="a" * 40,
        expected_config="sha256:" + "b" * 64,
    )

    assert result["status"] == "ready"
    assert result["node_count"] == len(PRODUCTION_CHAIN)
    assert result["attack_proof_count"] == sum(
        len(node.required_attacks) for node in PRODUCTION_CHAIN.values()
    )
    assert result["qualification_may_start"] is True
    evidence_digest = result["evidence_sha256"]
    assert isinstance(evidence_digest, str)
    assert len(evidence_digest) == 64


def test_missing_attack_blocks_qualification_without_resetting_history() -> None:
    evidence = _complete_evidence()
    nodes = evidence["nodes"]
    assert isinstance(nodes, dict)
    first = nodes["structure-fetch"]
    assert isinstance(first, dict)
    attacks = first["attacks"]
    assert isinstance(attacks, dict)
    attacks.pop(PRODUCTION_CHAIN["structure-fetch"].required_attacks[0])

    with pytest.raises(CommissioningEvidenceError, match="missing-attack-proof"):
        verify_commissioning_evidence(
            evidence,
            expected_release="a" * 40,
            expected_config="sha256:" + "b" * 64,
        )


def test_attack_proof_requires_ordered_detection_recovery_and_postcondition() -> None:
    evidence = _complete_evidence()
    nodes = evidence["nodes"]
    assert isinstance(nodes, dict)
    first = nodes["quote-batch"]
    assert isinstance(first, dict)
    attacks = first["attacks"]
    assert isinstance(attacks, dict)
    proof = attacks[PRODUCTION_CHAIN["quote-batch"].required_attacks[0]]
    assert isinstance(proof, dict)
    proof["recovered_at"] = proof["detected_at"]

    with pytest.raises(CommissioningEvidenceError, match="lifecycle-order"):
        verify_commissioning_evidence(
            evidence,
            expected_release="a" * 40,
            expected_config="sha256:" + "b" * 64,
        )


def test_plan_exposes_node_fault_recovery_and_postcondition() -> None:
    plan = build_commissioning_plan()

    assert plan["schema_version"] == "m1-production-commissioning-plan-v1"
    assert plan["status"] == "evidence-required"
    nodes = plan["nodes"]
    assert isinstance(nodes, list)
    assert [node["node_id"] for node in nodes] == list(RUNTIME_JOB_ORDER)
    assert all(node["required_attacks"] for node in nodes)
    assert all(node["success_fact"] for node in nodes)
    attacks = plan["attacks"]
    assert isinstance(attacks, list)
    assert all(attack["recovery_action"] for attack in attacks)
    assert all(attack["postcondition"] for attack in attacks)


def test_module_cli_prints_the_fail_closed_commissioning_plan() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "polyarb.control_plane.production_commissioning",
            "plan",
            "--json",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["status"] == "evidence-required"


def test_make_exposes_plan_and_exact_identity_verifier() -> None:
    plan = subprocess.run(
        ["make", "-s", "m1-production-commissioning-plan"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert plan.returncode == 0, plan.stderr
    assert json.loads(plan.stdout)["status"] == "evidence-required"

    verify = subprocess.run(
        [
            "make",
            "-n",
            "m1-production-commissioning-verify",
            "evidence=commissioning.json",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert verify.returncode == 0, verify.stderr
    assert "production_commissioning verify" in verify.stdout
    assert '--evidence "commissioning.json"' in verify.stdout
    assert f'--expected-release "{"a" * 40}"' in verify.stdout
    assert f'--expected-config "sha256:{"b" * 64}"' in verify.stdout

    help_result = subprocess.run(
        ["make", "-s", "help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert "m1-production-commissioning-plan" in help_result.stdout
    assert "m1-production-commissioning-verify" in help_result.stdout
