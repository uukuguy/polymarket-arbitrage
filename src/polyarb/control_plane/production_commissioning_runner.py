"""Append-only execution and collection for M1 commissioning fault proofs."""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Final, Literal, Protocol, cast

from polyarb.safe_artifact import read_stable_bytes, write_exclusive_bytes

from .production_commissioning import (
    ATTACK_CONTRACTS,
    PRODUCTION_CHAIN,
    CommissioningEvidenceError,
    verify_commissioning_evidence,
)

AttackStage = Literal[
    "preflight",
    "injected",
    "detected",
    "recovery-started",
    "cleanup",
    "recovered",
    "verified",
]

_RELEASE_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{40}")
_CONFIG_RE: Final[re.Pattern[str]] = re.compile(r"sha256:[0-9a-f]{64}")
_EVIDENCE_SCHEMA: Final[str] = "m1-production-commissioning-v1"
_STAGE_FILES: Final[Mapping[AttackStage, str]] = {
    "preflight": "10-preflight.json",
    "injected": "20-injected.json",
    "detected": "30-detected.json",
    "recovery-started": "40-recovery-started.json",
    "cleanup": "50-cleanup.json",
    "recovered": "60-recovered.json",
    "verified": "70-verified.json",
}


class CommissioningAttackError(RuntimeError):
    """A directed attack could not produce a complete, safely cleaned proof."""


def _nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value) > 512:
        raise CommissioningAttackError(f"invalid-{field}")
    return value


def _aware(value: datetime, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise CommissioningAttackError(f"invalid-{field}")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class AttackIdentity:
    experiment_id: str
    release_id: str
    config_id: str
    node_id: str
    attack_id: str

    def __post_init__(self) -> None:
        _nonempty(self.experiment_id, "experiment-id")
        if _RELEASE_RE.fullmatch(self.release_id) is None:
            raise CommissioningAttackError("invalid-release-id")
        if _CONFIG_RE.fullmatch(self.config_id) is None:
            raise CommissioningAttackError("invalid-config-id")
        if self.node_id not in PRODUCTION_CHAIN:
            raise CommissioningAttackError("unknown-node")
        if self.attack_id not in ATTACK_CONTRACTS:
            raise CommissioningAttackError("unknown-attack")
        if self.attack_id not in PRODUCTION_CHAIN[self.node_id].required_attacks:
            raise CommissioningAttackError("attack-not-required-for-node")

    def payload(self) -> dict[str, str]:
        return {
            "experiment_id": self.experiment_id,
            "release_id": self.release_id,
            "config_id": self.config_id,
            "node_id": self.node_id,
            "attack_id": self.attack_id,
        }


@dataclass(frozen=True, slots=True)
class AttackStageReceipt:
    stage: AttackStage
    receipt_id: str
    occurred_at: datetime

    def __post_init__(self) -> None:
        if self.stage not in _STAGE_FILES:
            raise CommissioningAttackError("invalid-stage")
        _nonempty(self.receipt_id, "receipt-id")
        object.__setattr__(self, "occurred_at", _aware(self.occurred_at, "occurred-at"))

    def payload(self, identity: AttackIdentity) -> dict[str, str]:
        return {
            **identity.payload(),
            "stage": self.stage,
            "receipt_id": self.receipt_id,
            "occurred_at": self.occurred_at.isoformat(),
        }


class CommissioningAttackAdapter(Protocol):
    """Typed adapter whose own policies bound provider and database operations."""

    def preflight(self, identity: AttackIdentity) -> AttackStageReceipt: ...

    def inject(self, identity: AttackIdentity) -> AttackStageReceipt: ...

    def detect(
        self,
        identity: AttackIdentity,
        injected: AttackStageReceipt,
    ) -> AttackStageReceipt: ...

    def start_recovery(
        self,
        identity: AttackIdentity,
        detected: AttackStageReceipt,
    ) -> AttackStageReceipt: ...

    def cleanup(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt | None,
    ) -> AttackStageReceipt: ...

    def recover(
        self,
        identity: AttackIdentity,
        recovery_started: AttackStageReceipt,
        cleanup: AttackStageReceipt,
    ) -> AttackStageReceipt: ...

    def verify(
        self,
        identity: AttackIdentity,
        recovered: AttackStageReceipt,
    ) -> AttackStageReceipt: ...


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    return (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    ).encode("utf-8")


def _write_payload(path: Path, payload: Mapping[str, object]) -> None:
    try:
        write_exclusive_bytes(path, _canonical_bytes(payload))
    except (OSError, ValueError) as error:
        raise CommissioningAttackError(f"artifact-write-failed:{path.name}") from error


def _record_stage(
    evidence_dir: Path,
    identity: AttackIdentity,
    receipt: AttackStageReceipt,
    expected_stage: AttackStage,
) -> AttackStageReceipt:
    if receipt.stage != expected_stage:
        raise CommissioningAttackError(f"stage-mismatch:{expected_stage}")
    _write_payload(evidence_dir / _STAGE_FILES[expected_stage], receipt.payload(identity))
    return receipt


def _ordered(*receipts: AttackStageReceipt) -> None:
    if any(
        left.occurred_at >= right.occurred_at
        for left, right in zip(receipts, receipts[1:])
    ):
        raise CommissioningAttackError("lifecycle-order")


def run_disposable_attack(
    *,
    identity: AttackIdentity,
    adapter: CommissioningAttackAdapter,
    evidence_dir: Path,
) -> dict[str, object]:
    """Run one disposable exact-image attack with mandatory cleanup.

    There is deliberately no orchestration timeout here. Each adapter uses the
    named provider/database/runtime policies it is proving. Stage receipts are
    append-only, so an interrupted or failed experiment remains auditable and
    can never be mistaken for a complete proof.
    """

    contract = ATTACK_CONTRACTS[identity.attack_id]
    if contract.execution_scope != "disposable-exact-image":
        raise CommissioningAttackError(f"scope-{contract.execution_scope}")
    try:
        evidence_dir.mkdir(parents=False, exist_ok=False)
    except FileExistsError as error:
        raise CommissioningAttackError("evidence-dir-exists") from error
    except OSError as error:
        raise CommissioningAttackError("evidence-dir-create-failed") from error
    _write_payload(
        evidence_dir / "00-intent.json",
        {
            **identity.payload(),
            "execution_scope": contract.execution_scope,
            "qualification_impact": contract.qualification_impact,
        },
    )

    injected: AttackStageReceipt | None = None
    detected: AttackStageReceipt | None = None
    recovery_started: AttackStageReceipt | None = None
    cleanup: AttackStageReceipt | None = None
    original: BaseException | None = None
    try:
        _record_stage(
            evidence_dir,
            identity,
            adapter.preflight(identity),
            "preflight",
        )
        injected = _record_stage(
            evidence_dir,
            identity,
            adapter.inject(identity),
            "injected",
        )
        detected = _record_stage(
            evidence_dir,
            identity,
            adapter.detect(identity, injected),
            "detected",
        )
        recovery_started = _record_stage(
            evidence_dir,
            identity,
            adapter.start_recovery(identity, detected),
            "recovery-started",
        )
    except BaseException as error:
        original = error

    cleanup_error: BaseException | None = None
    if injected is not None:
        try:
            cleanup = _record_stage(
                evidence_dir,
                identity,
                adapter.cleanup(identity, recovery_started),
                "cleanup",
            )
        except BaseException as error:
            cleanup_error = error
    if cleanup_error is not None:
        if original is not None:
            raise BaseExceptionGroup(
                "commissioning-attack-and-cleanup-failed",
                [original, cleanup_error],
            )
        raise cleanup_error
    if original is not None:
        raise original
    assert (
        injected is not None
        and detected is not None
        and recovery_started is not None
        and cleanup is not None
    )

    recovered = _record_stage(
        evidence_dir,
        identity,
        adapter.recover(identity, recovery_started, cleanup),
        "recovered",
    )
    verified = _record_stage(
        evidence_dir,
        identity,
        adapter.verify(identity, recovered),
        "verified",
    )
    _ordered(injected, detected, recovery_started, cleanup, recovered, verified)
    proof: dict[str, object] = {
        **identity.payload(),
        "qualification_impact": contract.qualification_impact,
        "detector_fact_id": detected.receipt_id,
        "recovery_action_id": recovery_started.receipt_id,
        "recovery_fact_id": recovered.receipt_id,
        "postcondition_fact_id": verified.receipt_id,
        "cleanup_verified": True,
        "injected_at": injected.occurred_at.isoformat(),
        "detected_at": detected.occurred_at.isoformat(),
        "recovery_started_at": recovery_started.occurred_at.isoformat(),
        "recovered_at": recovered.occurred_at.isoformat(),
        "verified_at": verified.occurred_at.isoformat(),
    }
    _write_payload(evidence_dir / "proof.json", proof)
    return proof


def _read_mapping(path: Path) -> Mapping[str, object]:
    try:
        raw = read_stable_bytes(path)
        value = json.loads(raw)
    except FileNotFoundError as error:
        raise CommissioningEvidenceError(f"evidence-file-missing:{path.name}") from error
    except (OSError, ValueError, json.JSONDecodeError) as error:
        raise CommissioningEvidenceError(f"evidence-file-invalid:{path.name}") from error
    if not isinstance(value, Mapping):
        raise CommissioningEvidenceError(f"evidence-file-invalid:{path.name}")
    return cast(Mapping[str, object], value)


def assemble_commissioning_evidence(
    *,
    root: Path,
    expected_release: str,
    expected_config: str,
) -> dict[str, object]:
    """Collect exact node artifacts and write one verifier-approved envelope."""

    nodes: dict[str, object] = {}
    for node_id, node in PRODUCTION_CHAIN.items():
        attacks = {
            attack_id: dict(
                _read_mapping(root / "attacks" / node_id / attack_id / "proof.json")
            )
            for attack_id in node.required_attacks
        }
        nodes[node_id] = {
            "normal_turn": dict(
                _read_mapping(root / "normal-turns" / f"{node_id}.json")
            ),
            "attacks": attacks,
        }
    evidence: dict[str, object] = {
        "schema_version": _EVIDENCE_SCHEMA,
        "release_id": expected_release,
        "config_id": expected_config,
        "nodes": nodes,
        "end_to_end": dict(_read_mapping(root / "end-to-end.json")),
    }
    verdict = verify_commissioning_evidence(
        evidence,
        expected_release=expected_release,
        expected_config=expected_config,
    )
    try:
        _write_payload(root / "commissioning-evidence.json", evidence)
    except CommissioningAttackError as error:
        raise CommissioningEvidenceError("commissioning-evidence-write-failed") from error
    return verdict


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    assemble = subcommands.add_parser("assemble")
    assemble.add_argument("--root", type=Path, required=True)
    assemble.add_argument("--expected-release", required=True)
    assemble.add_argument("--expected-config", required=True)
    assemble.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = assemble_commissioning_evidence(
            root=args.root,
            expected_release=args.expected_release,
            expected_config=args.expected_config,
        )
    except CommissioningEvidenceError as error:
        print(f"commissioning-blocked: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "AttackIdentity",
    "AttackStageReceipt",
    "CommissioningAttackAdapter",
    "CommissioningAttackError",
    "assemble_commissioning_evidence",
    "main",
    "run_disposable_attack",
]


if __name__ == "__main__":
    raise SystemExit(main())
