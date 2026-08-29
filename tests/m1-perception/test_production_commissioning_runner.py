from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import polyarb.control_plane.production_commissioning_runner as runner_module
from polyarb.control_plane.production_commissioning import (
    ATTACK_CONTRACTS,
    CommissioningEvidenceError,
)
from polyarb.control_plane.production_commissioning_runner import (
    AttackIdentity,
    AttackStageReceipt,
    CommissioningAttackError,
    assemble_commissioning_evidence,
    run_disposable_attack,
)

RELEASE = "a" * 40
CONFIG = f"sha256:{'b' * 64}"
NOW = datetime(2026, 8, 30, 12, 0, tzinfo=UTC)


@dataclass
class RecordingAdapter:
    fail_stage: str | None = None
    cleanup_fails: bool = False

    def __post_init__(self) -> None:
        self.calls: list[str] = []

    def _receipt(self, stage: str, offset: int) -> AttackStageReceipt:
        self.calls.append(stage)
        if self.fail_stage == stage:
            raise RuntimeError(f"failed:{stage}")
        return AttackStageReceipt(
            stage=stage,
            receipt_id=f"fact:{stage}",
            occurred_at=NOW + timedelta(seconds=offset),
        )

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt:
        assert identity.release_id == RELEASE
        return self._receipt("preflight", 0)

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt:
        return self._receipt("injected", 1)

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        assert injected.stage == "injected"
        return self._receipt("detected", 2)

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt:
        assert detected.stage == "detected"
        return self._receipt("recovery-started", 3)

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt:
        self.calls.append("cleanup")
        if self.cleanup_fails:
            raise RuntimeError("failed:cleanup")
        return AttackStageReceipt(
            stage="cleanup",
            receipt_id="fact:cleanup",
            occurred_at=NOW + timedelta(seconds=4),
        )

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt:
        assert cleanup.stage == "cleanup"
        return self._receipt("recovered", 5)

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt:
        assert recovered.stage == "recovered"
        return self._receipt("verified", 6)


def _identity(
    *,
    node_id: str = "structure-normalize",
    attack_id: str = "heartbeat-outage",
) -> AttackIdentity:
    return AttackIdentity(
        experiment_id=f"commission:{node_id}:{attack_id}",
        release_id=RELEASE,
        config_id=CONFIG,
        node_id=node_id,
        attack_id=attack_id,
    )


def test_disposable_runner_writes_verifier_compatible_ordered_proof(tmp_path: Path) -> None:
    adapter = RecordingAdapter()
    evidence_dir = tmp_path / "attack"

    proof = run_disposable_attack(
        identity=_identity(),
        adapter=adapter,
        evidence_dir=evidence_dir,
    )

    assert adapter.calls == [
        "preflight",
        "injected",
        "detected",
        "recovery-started",
        "cleanup",
        "recovered",
        "verified",
    ]
    assert proof == {
        "experiment_id": "commission:structure-normalize:heartbeat-outage",
        "release_id": RELEASE,
        "config_id": CONFIG,
        "node_id": "structure-normalize",
        "attack_id": "heartbeat-outage",
        "qualification_impact": "pause",
        "detector_fact_id": "fact:detected",
        "recovery_action_id": "fact:recovery-started",
        "recovery_fact_id": "fact:recovered",
        "postcondition_fact_id": "fact:verified",
        "cleanup_verified": True,
        "injected_at": "2026-08-30T12:00:01+00:00",
        "detected_at": "2026-08-30T12:00:02+00:00",
        "recovery_started_at": "2026-08-30T12:00:03+00:00",
        "recovered_at": "2026-08-30T12:00:05+00:00",
        "verified_at": "2026-08-30T12:00:06+00:00",
    }
    assert json.loads((evidence_dir / "proof.json").read_text()) == proof
    assert sorted(path.name for path in evidence_dir.iterdir()) == [
        "00-intent.json",
        "10-preflight.json",
        "20-injected.json",
        "30-detected.json",
        "40-recovery-started.json",
        "50-cleanup.json",
        "60-recovered.json",
        "70-verified.json",
        "proof.json",
    ]


def test_disposable_runner_always_cleans_up_after_injection_failure(tmp_path: Path) -> None:
    adapter = RecordingAdapter(fail_stage="detected")
    evidence_dir = tmp_path / "attack"

    with pytest.raises(RuntimeError, match="failed:detected"):
        run_disposable_attack(
            identity=_identity(),
            adapter=adapter,
            evidence_dir=evidence_dir,
        )

    assert adapter.calls == ["preflight", "injected", "detected", "cleanup"]
    assert (evidence_dir / "50-cleanup.json").is_file()
    assert not (evidence_dir / "proof.json").exists()


def test_disposable_runner_preserves_attack_and_cleanup_failures(tmp_path: Path) -> None:
    adapter = RecordingAdapter(fail_stage="detected", cleanup_fails=True)

    with pytest.raises(BaseExceptionGroup) as raised:
        run_disposable_attack(
            identity=_identity(),
            adapter=adapter,
            evidence_dir=tmp_path / "attack",
        )

    assert [str(error) for error in raised.value.exceptions] == [
        "failed:detected",
        "failed:cleanup",
    ]


def test_disposable_runner_rejects_production_canary_contract(tmp_path: Path) -> None:
    with pytest.raises(CommissioningAttackError, match="scope-production-canary"):
        run_disposable_attack(
            identity=_identity(node_id="structure-fetch", attack_id="gamma-timeout"),
            adapter=RecordingAdapter(),
            evidence_dir=tmp_path / "attack",
        )


def test_disposable_runner_refuses_existing_evidence_directory(tmp_path: Path) -> None:
    evidence_dir = tmp_path / "attack"
    evidence_dir.mkdir()

    with pytest.raises(CommissioningAttackError, match="evidence-dir-exists"):
        run_disposable_attack(
            identity=_identity(),
            adapter=RecordingAdapter(),
            evidence_dir=evidence_dir,
        )


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _complete_evidence_tree(root: Path) -> None:
    for node_index, node_id in enumerate(
        (
            "structure-fetch",
            "structure-materialize",
            "structure-normalize",
            "structure-certify",
            "quote-admit",
            "quote-batch",
            "quote-certify",
            "opportunity-certify",
        )
    ):
        _write_json(
            root / "normal-turns" / f"{node_id}.json",
            {
                "attempt_id": f"attempt:{node_id}",
                "terminal_fact_id": f"terminal:{node_id}",
                "success_fact_id": f"success:{node_id}",
                "postcondition_fact_id": f"postcondition:{node_id}",
                "succeeded_at": (NOW + timedelta(minutes=node_index)).isoformat(),
            },
        )
        for attack_index, attack_id in enumerate(
            attack_id
            for attack_id, contract in ATTACK_CONTRACTS.items()
            if node_id in contract.targets
        ):
            base = NOW + timedelta(hours=1, minutes=node_index, seconds=attack_index * 10)
            _write_json(
                root / "attacks" / node_id / attack_id / "proof.json",
                {
                    "experiment_id": f"commission:{node_id}:{attack_id}",
                    "release_id": RELEASE,
                    "config_id": CONFIG,
                    "node_id": node_id,
                    "attack_id": attack_id,
                    "qualification_impact": ATTACK_CONTRACTS[
                        attack_id
                    ].qualification_impact,
                    "detector_fact_id": f"detector:{node_id}:{attack_id}",
                    "recovery_action_id": f"action:{node_id}:{attack_id}",
                    "recovery_fact_id": f"recovery:{node_id}:{attack_id}",
                    "postcondition_fact_id": f"postcondition:{node_id}:{attack_id}",
                    "cleanup_verified": True,
                    "injected_at": base.isoformat(),
                    "detected_at": (base + timedelta(seconds=1)).isoformat(),
                    "recovery_started_at": (base + timedelta(seconds=2)).isoformat(),
                    "recovered_at": (base + timedelta(seconds=3)).isoformat(),
                    "verified_at": (base + timedelta(seconds=4)).isoformat(),
                },
            )
    _write_json(
        root / "end-to-end.json",
        {
            "lineage_id": "lineage:final",
            "structure_pointer_fact_id": "pointer:structure",
            "quote_pointer_fact_id": "pointer:quote",
            "opportunity_pointer_fact_id": "pointer:opportunity",
            "verified_at": (NOW + timedelta(hours=2)).isoformat(),
        },
    )


def test_assembler_builds_and_verifies_complete_exact_identity_tree(tmp_path: Path) -> None:
    root = tmp_path / "commissioning"
    _complete_evidence_tree(root)

    result = assemble_commissioning_evidence(
        root=root,
        expected_release=RELEASE,
        expected_config=CONFIG,
    )

    assert result["status"] == "ready"
    assert result["node_count"] == 8
    assert result["attack_proof_count"] == 66
    assert (root / "commissioning-evidence.json").is_file()


def test_assembler_fails_closed_on_missing_node_attack_proof(tmp_path: Path) -> None:
    root = tmp_path / "commissioning"
    _complete_evidence_tree(root)
    (root / "attacks" / "quote-certify" / "quote-batch-incomplete" / "proof.json").unlink()

    with pytest.raises(CommissioningEvidenceError, match="evidence-file-missing"):
        assemble_commissioning_evidence(
            root=root,
            expected_release=RELEASE,
            expected_config=CONFIG,
        )


def test_assembler_cli_emits_ready_verdict(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    root = tmp_path / "commissioning"
    _complete_evidence_tree(root)

    assert runner_module.main(
        [
            "assemble",
            "--root",
            str(root),
            "--expected-release",
            RELEASE,
            "--expected-config",
            CONFIG,
            "--json",
        ]
    ) == 0

    assert json.loads(capsys.readouterr().out)["status"] == "ready"


def test_makefile_exposes_commissioning_assemble_entrypoint() -> None:
    makefile = (Path(__file__).parents[2] / "Makefile").read_text(encoding="utf-8")

    assert "m1-production-commissioning-assemble:" in makefile
    assert "production_commissioning_runner assemble" in makefile
    assert "evidence_root" in makefile
