"""Fail-closed commissioning contract for the M1 production data chain.

Qualification measures sustained availability only after this contract proves
that every runtime node can complete a normal turn and can close each known
fault lifecycle.  A fault proof is deliberately stronger than an alert: it
must bind injection, detection, recovery, cleanup, and a business
postcondition to the exact release and configuration under test.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from hashlib import sha256
from pathlib import Path
from types import MappingProxyType
from typing import Final, Literal, cast

from .runtime_contract import RUNTIME_STAGE_REGISTRY
from .runtime_deadlines import RUNTIME_JOB_ORDER, RUNTIME_JOB_SUCCESSORS

QualificationImpact = Literal["pause", "block", "invalidate"]
ExecutionScope = Literal["disposable-exact-image", "production-canary"]

_RELEASE_RE: Final[re.Pattern[str]] = re.compile(r"[0-9a-f]{40}")
_CONFIG_RE: Final[re.Pattern[str]] = re.compile(r"sha256:[0-9a-f]{64}")
_MAX_EVIDENCE_BYTES: Final[int] = 8 * 1024 * 1024
_EVIDENCE_SCHEMA: Final[str] = "m1-production-commissioning-v1"
_PLAN_SCHEMA: Final[str] = "m1-production-commissioning-plan-v1"


class CommissioningEvidenceError(ValueError):
    """Commissioning evidence is incomplete, inconsistent, or unbound."""


@dataclass(frozen=True, slots=True)
class AttackContract:
    attack_id: str
    targets: tuple[str, ...]
    injector: str
    detector: str
    recovery_action: str
    postcondition: str
    execution_scope: ExecutionScope
    qualification_impact: QualificationImpact


@dataclass(frozen=True, slots=True)
class ProductionNodeContract:
    node_id: str
    upstream_nodes: tuple[str, ...]
    owner_role: str
    required_input: str
    success_fact: str
    liveness_fact: str
    business_postcondition: str
    required_attacks: tuple[str, ...]


_ALL_NODES = tuple(RUNTIME_JOB_ORDER)


def _attack(
    attack_id: str,
    targets: Sequence[str],
    *,
    injector: str,
    detector: str,
    recovery_action: str,
    postcondition: str,
    execution_scope: ExecutionScope = "disposable-exact-image",
    qualification_impact: QualificationImpact = "pause",
) -> AttackContract:
    return AttackContract(
        attack_id=attack_id,
        targets=tuple(targets),
        injector=injector,
        detector=detector,
        recovery_action=recovery_action,
        postcondition=postcondition,
        execution_scope=execution_scope,
        qualification_impact=qualification_impact,
    )


ATTACK_CONTRACTS: Mapping[str, AttackContract] = MappingProxyType(
    {
        attack.attack_id: attack
        for attack in (
            _attack(
                "heartbeat-outage",
                _ALL_NODES,
                injector="interrupt the scoped PostgreSQL heartbeat transport",
                detector="durable heartbeat-at-risk runtime fact",
                recovery_action="retry renewal inside the current fenced lease",
                postcondition="same attempt renews its lease and reaches a terminal fact",
            ),
            _attack(
                "progress-stall",
                _ALL_NODES,
                injector="hold one stage after a durable progress checkpoint",
                detector="policy-derived progress deadline opens one runtime incident",
                recovery_action="cancel or reclaim the exact fenced attempt",
                postcondition="a successor attempt advances beyond the held checkpoint",
                qualification_impact="block",
            ),
            _attack(
                "worker-exit",
                _ALL_NODES,
                injector="terminate the disposable worker after claim",
                detector="lease expiry or process-loss observation names the exact attempt",
                recovery_action="reclaim the job under a new worker and lease epoch",
                postcondition="replacement attempt succeeds without a stale terminal write",
                qualification_impact="block",
            ),
            _attack(
                "stale-owner-terminal-write",
                _ALL_NODES,
                injector="attempt terminal mutation with the superseded lease epoch",
                detector="transactional fence rejects the stale owner",
                recovery_action="preserve the current owner and discard the stale mutation",
                postcondition="only the current attempt owns the durable terminal fact",
            ),
            _attack(
                "retry-budget-exhaustion",
                _ALL_NODES,
                injector="repeat the node's typed retryable failure through its exact budget",
                detector="closed circuit and one deduplicated incident",
                recovery_action="probe only after policy-derived holdoff",
                postcondition="successful probe releases one runnable successor attempt",
                qualification_impact="block",
            ),
            _attack(
                "gamma-timeout",
                ("structure-fetch",),
                injector="return a scoped timeout from the Gamma canary proxy",
                detector="structure-fetch retryable terminal fact and incident",
                recovery_action="replace the transport generation and retry durably",
                postcondition="a newer source page receipt is committed",
                execution_scope="production-canary",
            ),
            _attack(
                "gamma-malformed-page",
                ("structure-fetch",),
                injector="return one malformed Gamma page to the exact canary call",
                detector="page validation rejects the response before upload",
                recovery_action="remove the scoped response and retry the page",
                postcondition="the validated page receipt advances exactly once",
                execution_scope="production-canary",
            ),
            _attack(
                "source-receipt-gap",
                ("structure-materialize",),
                injector="withhold one required source-page receipt",
                detector="materializer records an incomplete input barrier",
                recovery_action="requeue from the missing durable receipt boundary",
                postcondition="one complete source bundle receipt is committed",
            ),
            _attack(
                "r2-read-timeout",
                (
                    "structure-materialize",
                    "structure-normalize",
                    "structure-certify",
                    "quote-admit",
                    "quote-certify",
                    "opportunity-certify",
                ),
                injector="hold the exact artifact GET beyond its provider envelope",
                detector="typed R2 read timeout names the current runtime stage",
                recovery_action="retry the job from its durable input identity",
                postcondition="the same immutable artifact digest is consumed successfully",
            ),
            _attack(
                "r2-write-timeout",
                (
                    "structure-fetch",
                    "structure-materialize",
                    "structure-normalize",
                    "structure-certify",
                    "quote-admit",
                    "quote-batch",
                    "opportunity-certify",
                ),
                injector="hold PUT or HEAD before its receipt commit",
                detector="typed R2 write timeout leaves no terminal receipt",
                recovery_action="retry upload under the same content identity",
                postcondition="verified object digest and one fenced receipt agree",
            ),
            _attack(
                "normalization-payload-corrupt",
                ("structure-normalize",),
                injector="supply one schema-invalid immutable range artifact",
                detector="normalizer rejects it before publishing a range receipt",
                recovery_action="quarantine the input generation and surface operator action",
                postcondition="last certified Structure pointer remains authoritative",
                qualification_impact="block",
            ),
            _attack(
                "structure-parity-mismatch",
                ("structure-certify",),
                injector="alter the disposable parity result before certification",
                detector="certifier refuses manifest and pointer publication",
                recovery_action="retain the prior certified generation and investigate input",
                postcondition="no mismatched generation becomes current",
                qualification_impact="invalidate",
            ),
            _attack(
                "quote-admission-missing-shard",
                ("quote-admit",),
                injector="withhold one certified Structure shard",
                detector="admission barrier records the missing immutable input",
                recovery_action="retry admission after shard proof is available",
                postcondition="all quote batches bind one Structure generation",
            ),
            _attack(
                "clob-429",
                ("quote-batch",),
                injector="return a scoped CLOB 429 to the exact canary batch",
                detector="quote-batch records a typed provider failure",
                recovery_action="apply durable backoff and retry the batch",
                postcondition="a newer complete quote-batch receipt is committed",
                execution_scope="production-canary",
            ),
            _attack(
                "clob-missing-leg",
                ("quote-batch",),
                injector="omit one requested CLOB book leg",
                detector="batch validation rejects incomplete market coverage",
                recovery_action="retry the same immutable batch input",
                postcondition="receipt proves every requested leg is present",
                execution_scope="production-canary",
            ),
            _attack(
                "quote-batch-incomplete",
                ("quote-certify",),
                injector="withhold one required quote-batch receipt",
                detector="certification barrier remains incomplete and opens an incident",
                recovery_action="recover the missing batch before re-running certification",
                postcondition="quote pointer publishes only after all batch receipts agree",
                qualification_impact="block",
            ),
            _attack(
                "publication-pointer-conflict",
                ("structure-certify", "quote-certify", "opportunity-certify"),
                injector="race a stale generation against the current pointer transaction",
                detector="compare-and-swap fence rejects the stale publisher",
                recovery_action="preserve the current lineage and retry only current work",
                postcondition="one current pointer and one matching success event remain",
            ),
            _attack(
                "stale-quote-pointer",
                ("opportunity-certify",),
                injector="present a quote pointer outside the business freshness SLO",
                detector="opportunity input gate refuses stale quote authority",
                recovery_action="restore quote publication before opportunity recomputation",
                postcondition="opportunity pointer binds a fresh current quote generation",
                qualification_impact="block",
            ),
        )
    }
)


_NODE_INPUTS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "structure-fetch": "Gamma source cursor and source-window identity",
        "structure-materialize": "complete immutable source-page receipts",
        "structure-normalize": "one immutable materialized source bundle",
        "structure-certify": "complete normalized range receipts",
        "quote-admit": "current certified Structure manifest and shards",
        "quote-batch": "one immutable quote admission batch",
        "quote-certify": "complete quote-batch receipts for one generation",
        "opportunity-certify": "current certified quote pointer",
    }
)
_NODE_SUCCESS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "structure-fetch": "committed source-page receipt",
        "structure-materialize": "committed source-bundle receipt",
        "structure-normalize": "committed normalized-range receipt",
        "structure-certify": "Structure certification success event plus current pointer",
        "quote-admit": "committed quote-admission batch set",
        "quote-batch": "committed quote-batch receipt",
        "quote-certify": "Quote certification success event plus current pointer",
        "opportunity-certify": "Opportunity success event plus current pointer",
    }
)
_NODE_POSTCONDITIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "structure-fetch": "materializer can read the new page receipt",
        "structure-materialize": "normalizer can claim the bundle generation",
        "structure-normalize": "certifier sees a complete range set",
        "structure-certify": "quote admission binds the published Structure generation",
        "quote-admit": "every planned quote batch becomes claimable",
        "quote-batch": "quote certifier sees the batch as complete",
        "quote-certify": "opportunity certifier reads the current Quote generation",
        "opportunity-certify": "current opportunity API serves the new lineage",
    }
)
_NODE_OWNERS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "structure-fetch": "structure-worker",
        "structure-materialize": "structure-worker",
        "structure-normalize": "structure-worker",
        "structure-certify": "structure-worker",
        "quote-admit": "quote-worker",
        "quote-batch": "quote-worker",
        "quote-certify": "quote-worker",
        "opportunity-certify": "quote-worker",
    }
)


def _upstream_nodes(node_id: str) -> tuple[str, ...]:
    return tuple(
        candidate
        for candidate, successors in RUNTIME_JOB_SUCCESSORS.items()
        if node_id in successors
    )


def _required_attacks(node_id: str) -> tuple[str, ...]:
    return tuple(
        attack_id for attack_id, attack in ATTACK_CONTRACTS.items() if node_id in attack.targets
    )


PRODUCTION_CHAIN: Mapping[str, ProductionNodeContract] = MappingProxyType(
    {
        node_id: ProductionNodeContract(
            node_id=node_id,
            upstream_nodes=_upstream_nodes(node_id),
            owner_role=_NODE_OWNERS[node_id],
            required_input=_NODE_INPUTS[node_id],
            success_fact=_NODE_SUCCESS[node_id],
            liveness_fact="m1_job_runtime_state",
            business_postcondition=_NODE_POSTCONDITIONS[node_id],
            required_attacks=_required_attacks(node_id),
        )
        for node_id in RUNTIME_JOB_ORDER
    }
)


def _validate_contract() -> None:
    if tuple(PRODUCTION_CHAIN) != tuple(RUNTIME_STAGE_REGISTRY):
        raise RuntimeError("production chain must exactly cover the runtime registry")
    if tuple(PRODUCTION_CHAIN) != tuple(RUNTIME_JOB_ORDER):
        raise RuntimeError("production chain must retain runtime DAG order")
    for attack_id, attack in ATTACK_CONTRACTS.items():
        if attack.attack_id != attack_id or not attack.targets:
            raise RuntimeError("production attack registry is malformed")
        if any(target not in PRODUCTION_CHAIN for target in attack.targets):
            raise RuntimeError("production attack targets an unknown node")
    for node in PRODUCTION_CHAIN.values():
        if not node.required_attacks:
            raise RuntimeError("every production node requires fault attacks")


_validate_contract()


def build_commissioning_plan() -> dict[str, object]:
    """Return the closed pre-qualification commissioning manifest."""
    return {
        "schema_version": _PLAN_SCHEMA,
        "status": "evidence-required",
        "qualification_gate": (
            "all normal turns, attacks, cleanup proofs, business postconditions, "
            "and end-to-end lineage must pass before healthy seconds accrue"
        ),
        "nodes": [asdict(node) for node in PRODUCTION_CHAIN.values()],
        "attacks": [asdict(attack) for attack in ATTACK_CONTRACTS.values()],
    }


def _mapping(value: object, reason: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise CommissioningEvidenceError(reason)
    return cast(Mapping[str, object], value)


def _nonempty(value: object, reason: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value or len(value) > 512:
        raise CommissioningEvidenceError(reason)
    return value


def _timestamp(value: object, reason: str) -> datetime:
    text = _nonempty(value, reason)
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as error:
        raise CommissioningEvidenceError(reason) from error
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise CommissioningEvidenceError(reason)
    return parsed.astimezone(UTC)


def _validate_normal_turn(node_id: str, value: object) -> datetime:
    turn = _mapping(value, f"normal-turn-invalid:{node_id}")
    for field in (
        "attempt_id",
        "terminal_fact_id",
        "success_fact_id",
        "postcondition_fact_id",
    ):
        _nonempty(turn.get(field), f"normal-turn-invalid:{node_id}:{field}")
    return _timestamp(turn.get("succeeded_at"), f"normal-turn-invalid:{node_id}:succeeded-at")


def _validate_attack_proof(
    value: object,
    *,
    node_id: str,
    attack: AttackContract,
    release_id: str,
    config_id: str,
) -> datetime:
    proof = _mapping(value, f"attack-proof-invalid:{node_id}:{attack.attack_id}")
    identities = {
        "release_id": release_id,
        "config_id": config_id,
        "node_id": node_id,
        "attack_id": attack.attack_id,
        "qualification_impact": attack.qualification_impact,
    }
    for field, expected in identities.items():
        if proof.get(field) != expected:
            raise CommissioningEvidenceError(
                f"attack-proof-identity:{node_id}:{attack.attack_id}:{field}"
            )
    for field in (
        "experiment_id",
        "detector_fact_id",
        "recovery_action_id",
        "recovery_fact_id",
        "postcondition_fact_id",
    ):
        _nonempty(proof.get(field), f"attack-proof-invalid:{node_id}:{attack.attack_id}:{field}")
    if proof.get("cleanup_verified") is not True:
        raise CommissioningEvidenceError(f"cleanup-unverified:{node_id}:{attack.attack_id}")
    timeline = tuple(
        _timestamp(
            proof.get(field),
            f"attack-proof-invalid:{node_id}:{attack.attack_id}:{field}",
        )
        for field in (
            "injected_at",
            "detected_at",
            "recovery_started_at",
            "recovered_at",
            "verified_at",
        )
    )
    if any(left >= right for left, right in zip(timeline, timeline[1:])):
        raise CommissioningEvidenceError(f"lifecycle-order:{node_id}:{attack.attack_id}")
    return timeline[-1]


def _canonical_bytes(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as error:
        raise CommissioningEvidenceError("evidence-not-canonical-json") from error


def verify_commissioning_evidence(
    evidence: Mapping[str, object],
    *,
    expected_release: str,
    expected_config: str,
) -> dict[str, object]:
    """Verify exact-release commissioning evidence without mutating production."""
    if _RELEASE_RE.fullmatch(expected_release) is None:
        raise CommissioningEvidenceError("invalid-expected-release")
    if _CONFIG_RE.fullmatch(expected_config) is None:
        raise CommissioningEvidenceError("invalid-expected-config")
    if evidence.get("schema_version") != _EVIDENCE_SCHEMA:
        raise CommissioningEvidenceError("evidence-schema-mismatch")
    if evidence.get("release_id") != expected_release:
        raise CommissioningEvidenceError("evidence-release-mismatch")
    if evidence.get("config_id") != expected_config:
        raise CommissioningEvidenceError("evidence-config-mismatch")
    nodes = _mapping(evidence.get("nodes"), "nodes-invalid")
    if set(nodes) != set(PRODUCTION_CHAIN):
        raise CommissioningEvidenceError("node-set-mismatch")

    latest_proof_at: datetime | None = None
    attack_proof_count = 0
    node_results: list[dict[str, object]] = []
    for node_id, node in PRODUCTION_CHAIN.items():
        node_evidence = _mapping(nodes[node_id], f"node-evidence-invalid:{node_id}")
        normal_at = _validate_normal_turn(node_id, node_evidence.get("normal_turn"))
        attacks = _mapping(node_evidence.get("attacks"), f"attack-evidence-invalid:{node_id}")
        required = set(node.required_attacks)
        missing = required - set(attacks)
        if missing:
            raise CommissioningEvidenceError(f"missing-attack-proof:{node_id}:{sorted(missing)[0]}")
        if set(attacks) != required:
            raise CommissioningEvidenceError(f"unexpected-attack-proof:{node_id}")
        node_latest = normal_at
        for attack_id in node.required_attacks:
            verified_at = _validate_attack_proof(
                attacks[attack_id],
                node_id=node_id,
                attack=ATTACK_CONTRACTS[attack_id],
                release_id=expected_release,
                config_id=expected_config,
            )
            node_latest = max(node_latest, verified_at)
            attack_proof_count += 1
        latest_proof_at = (
            node_latest if latest_proof_at is None else max(latest_proof_at, node_latest)
        )
        node_results.append(
            {
                "node_id": node_id,
                "state": "ready",
                "attack_proof_count": len(node.required_attacks),
                "verified_at": node_latest.isoformat(),
            }
        )

    end_to_end = _mapping(evidence.get("end_to_end"), "end-to-end-invalid")
    for field in (
        "lineage_id",
        "structure_pointer_fact_id",
        "quote_pointer_fact_id",
        "opportunity_pointer_fact_id",
    ):
        _nonempty(end_to_end.get(field), f"end-to-end-invalid:{field}")
    end_to_end_at = _timestamp(end_to_end.get("verified_at"), "end-to-end-invalid:verified-at")
    if latest_proof_at is None or end_to_end_at < latest_proof_at:
        raise CommissioningEvidenceError("end-to-end-before-node-proof")

    return {
        "schema_version": _EVIDENCE_SCHEMA,
        "status": "ready",
        "qualification_may_start": True,
        "release_id": expected_release,
        "config_id": expected_config,
        "node_count": len(node_results),
        "attack_proof_count": attack_proof_count,
        "end_to_end_verified_at": end_to_end_at.isoformat(),
        "nodes": node_results,
        "evidence_sha256": sha256(_canonical_bytes(evidence)).hexdigest(),
    }


def _read_evidence(path: Path) -> Mapping[str, object]:
    try:
        size = path.stat().st_size
        if size <= 0 or size > _MAX_EVIDENCE_BYTES:
            raise CommissioningEvidenceError("evidence-size-invalid")
        value = json.loads(path.read_bytes())
    except CommissioningEvidenceError:
        raise
    except (OSError, json.JSONDecodeError) as error:
        raise CommissioningEvidenceError("evidence-unreadable") from error
    return _mapping(value, "evidence-root-invalid")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    plan = subcommands.add_parser("plan")
    plan.add_argument("--json", action="store_true")
    verify = subcommands.add_parser("verify")
    verify.add_argument("--evidence", type=Path, required=True)
    verify.add_argument("--expected-release", required=True)
    verify.add_argument("--expected-config", required=True)
    verify.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "plan":
            result = build_commissioning_plan()
        else:
            result = verify_commissioning_evidence(
                _read_evidence(args.evidence),
                expected_release=args.expected_release,
                expected_config=args.expected_config,
            )
    except CommissioningEvidenceError as error:
        print(f"commissioning-blocked: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "ATTACK_CONTRACTS",
    "PRODUCTION_CHAIN",
    "AttackContract",
    "CommissioningEvidenceError",
    "ProductionNodeContract",
    "build_commissioning_plan",
    "verify_commissioning_evidence",
]


if __name__ == "__main__":
    raise SystemExit(main())
