"""Isolated-database harness for executable M1 commissioning attacks."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from .production_commissioning import PRODUCTION_CHAIN
from .production_commissioning_disposable import (
    HeartbeatOutageCommissioningAdapter,
    NormalizationPayloadCorruptCommissioningAdapter,
    ProgressStallCommissioningAdapter,
    PublicationPointerConflictCommissioningAdapter,
    QuoteAdmissionMissingShardCommissioningAdapter,
    QuoteBatchIncompleteCommissioningAdapter,
    R2ReadTimeoutCommissioningAdapter,
    RetryBudgetCommissioningAdapter,
    SourceReceiptGapCommissioningAdapter,
    StaleOwnerCommissioningAdapter,
    StructureParityMismatchCommissioningAdapter,
    WorkerExitCommissioningAdapter,
)
from .production_commissioning_runner import (
    AttackIdentity,
    CommissioningAttackAdapter,
    CommissioningAttackError,
    run_disposable_attack,
)
from .runtime_fault_matrix import (
    RuntimeFaultMatrixError,
    migrated_disposable_control_plane_database,
    validated_control_plane_test_dsn,
)

_STALE_OWNER_ATTACK_ID: Final[str] = "stale-owner-terminal-write"
_PROGRESS_STALL_ATTACK_ID: Final[str] = "progress-stall"
_RETRY_BUDGET_ATTACK_ID: Final[str] = "retry-budget-exhaustion"
_HEARTBEAT_OUTAGE_ATTACK_ID: Final[str] = "heartbeat-outage"
_WORKER_EXIT_ATTACK_ID: Final[str] = "worker-exit"
_SOURCE_RECEIPT_GAP_ATTACK_ID: Final[str] = "source-receipt-gap"
_QUOTE_BATCH_INCOMPLETE_ATTACK_ID: Final[str] = "quote-batch-incomplete"
_QUOTE_ADMISSION_MISSING_SHARD_ATTACK_ID: Final[str] = "quote-admission-missing-shard"
_NORMALIZATION_PAYLOAD_CORRUPT_ATTACK_ID: Final[str] = "normalization-payload-corrupt"
_STRUCTURE_PARITY_MISMATCH_ATTACK_ID: Final[str] = "structure-parity-mismatch"
_PUBLICATION_POINTER_CONFLICT_ATTACK_ID: Final[str] = "publication-pointer-conflict"
_R2_READ_TIMEOUT_ATTACK_ID: Final[str] = "r2-read-timeout"
_BASE_NOW: Final[datetime] = datetime(2031, 1, 1, 12, 0, tzinfo=UTC)


class CommissioningHarnessError(RuntimeError):
    """The isolated commissioning harness could not produce complete proof."""


def _selected_nodes(node_ids: Sequence[str] | None) -> tuple[str, ...]:
    requested = tuple(PRODUCTION_CHAIN) if node_ids is None else tuple(node_ids)
    if not requested:
        raise CommissioningHarnessError("at least one commissioning node is required")
    if len(requested) != len(set(requested)):
        raise CommissioningHarnessError("duplicate commissioning node")
    unknown = set(requested).difference(PRODUCTION_CHAIN)
    if unknown:
        raise CommissioningHarnessError("unknown commissioning node")
    requested_set = set(requested)
    return tuple(node_id for node_id in PRODUCTION_CHAIN if node_id in requested_set)


def _run_commissioning(
    *,
    attack_id: str,
    adapter_factory: Callable[..., CommissioningAttackAdapter],
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Run one shared attack on real transactions in disposable databases.

    ``node_ids`` exists for integration tests. The command-line contract always
    executes the complete eight-node production chain.
    """

    selected = _selected_nodes(node_ids)
    try:
        identities = tuple(
            AttackIdentity(
                experiment_id=f"commission:{node_id}:{attack_id}",
                release_id=release_id,
                config_id=config_id,
                node_id=node_id,
                attack_id=attack_id,
            )
            for node_id in selected
        )
    except CommissioningAttackError as error:
        raise CommissioningHarnessError(str(error)) from error
    try:
        validated_control_plane_test_dsn()
    except RuntimeFaultMatrixError as error:
        raise CommissioningHarnessError(str(error)) from error

    attacks_root = root / "attacks"
    attacks_root.mkdir(parents=True, exist_ok=True)
    proof_count = 0
    try:
        for index, identity in enumerate(identities):
            node_root = attacks_root / identity.node_id
            node_root.mkdir(parents=False, exist_ok=True)
            with migrated_disposable_control_plane_database() as database:
                run_disposable_attack(
                    identity=identity,
                    adapter=adapter_factory(
                        control_plane=database.control_plane,
                        started_at=_BASE_NOW + timedelta(minutes=index),
                    ),
                    evidence_dir=node_root / identity.attack_id,
                )
            proof_count += 1
    except Exception as error:
        raise CommissioningHarnessError(
            f"commissioning-failed:{type(error).__name__}"
        ) from error

    return {
        "attack_id": attack_id,
        "execution_scope": "disposable-exact-image",
        "node_count": len(identities),
        "proof_count": proof_count,
        "status": "pass",
    }


def run_stale_owner_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove the stale-owner fence on all selected production transactions."""

    return _run_commissioning(
        attack_id=_STALE_OWNER_ATTACK_ID,
        adapter_factory=StaleOwnerCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=node_ids,
    )


def run_progress_stall_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove policy-classified progress-stall recovery on selected transactions."""

    return _run_commissioning(
        attack_id=_PROGRESS_STALL_ATTACK_ID,
        adapter_factory=ProgressStallCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=node_ids,
    )


def run_retry_budget_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove retry-budget circuit recovery on selected transactions."""

    return _run_commissioning(
        attack_id=_RETRY_BUDGET_ATTACK_ID,
        adapter_factory=RetryBudgetCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=node_ids,
    )


def run_heartbeat_outage_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove controller-renewed heartbeat recovery on selected transactions."""

    return _run_commissioning(
        attack_id=_HEARTBEAT_OUTAGE_ATTACK_ID,
        adapter_factory=HeartbeatOutageCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=node_ids,
    )


def run_worker_exit_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove lease-expired worker reclaim on selected transactions."""

    return _run_commissioning(
        attack_id=_WORKER_EXIT_ATTACK_ID,
        adapter_factory=WorkerExitCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=node_ids,
    )


def run_source_receipt_gap_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove the source receipt barrier on its sole materializer target."""

    return _run_commissioning(
        attack_id=_SOURCE_RECEIPT_GAP_ATTACK_ID,
        adapter_factory=SourceReceiptGapCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("structure-materialize",),
    )


def run_quote_batch_incomplete_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove the incomplete Quote batch barrier on its sole certifier target."""

    return _run_commissioning(
        attack_id=_QUOTE_BATCH_INCOMPLETE_ATTACK_ID,
        adapter_factory=QuoteBatchIncompleteCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("quote-certify",),
    )


def run_quote_admission_missing_shard_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove exact missing-shard diagnosis and recovery on Quote admission."""

    return _run_commissioning(
        attack_id=_QUOTE_ADMISSION_MISSING_SHARD_ATTACK_ID,
        adapter_factory=QuoteAdmissionMissingShardCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("quote-admit",),
    )


def run_normalization_payload_corrupt_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove bad immutable Structure input is visibly quarantined."""

    return _run_commissioning(
        attack_id=_NORMALIZATION_PAYLOAD_CORRUPT_ATTACK_ID,
        adapter_factory=NormalizationPayloadCorruptCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("structure-normalize",),
    )


def run_structure_parity_mismatch_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove frozen Structure parity conflicts visibly invalidate qualification."""

    return _run_commissioning(
        attack_id=_STRUCTURE_PARITY_MISMATCH_ATTACK_ID,
        adapter_factory=StructureParityMismatchCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("structure-certify",),
    )


def run_publication_pointer_conflict_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove stale publishers cannot move any current production pointer."""

    return _run_commissioning(
        attack_id=_PUBLICATION_POINTER_CONFLICT_ATTACK_ID,
        adapter_factory=PublicationPointerConflictCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=("structure-certify", "quote-certify", "opportunity-certify"),
    )


def run_r2_read_timeout_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
) -> dict[str, object]:
    """Prove each R2-reading node retries one exact immutable GET."""

    return _run_commissioning(
        attack_id=_R2_READ_TIMEOUT_ATTACK_ID,
        adapter_factory=R2ReadTimeoutCommissioningAdapter,
        root=root,
        release_id=release_id,
        config_id=config_id,
        node_ids=(
            "structure-materialize",
            "structure-normalize",
            "structure-certify",
            "quote-admit",
            "quote-certify",
            "opportunity-certify",
        ),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    for command in (
        "stale-owner",
        "progress-stall",
        "retry-budget",
        "heartbeat-outage",
        "worker-exit",
        "source-receipt-gap",
        "quote-batch-incomplete",
        "quote-admission-missing-shard",
        "normalization-payload-corrupt",
        "structure-parity-mismatch",
        "publication-pointer-conflict",
        "r2-read-timeout",
    ):
        attack = subcommands.add_parser(command)
        attack.add_argument("--root", type=Path, required=True)
        attack.add_argument("--release-id", required=True)
        attack.add_argument("--config-id", required=True)
        attack.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    runners: dict[str, Callable[..., dict[str, object]]] = {
        "stale-owner": run_stale_owner_commissioning,
        "progress-stall": run_progress_stall_commissioning,
        "retry-budget": run_retry_budget_commissioning,
        "heartbeat-outage": run_heartbeat_outage_commissioning,
        "worker-exit": run_worker_exit_commissioning,
        "source-receipt-gap": run_source_receipt_gap_commissioning,
        "quote-batch-incomplete": run_quote_batch_incomplete_commissioning,
        "quote-admission-missing-shard": run_quote_admission_missing_shard_commissioning,
        "normalization-payload-corrupt": run_normalization_payload_corrupt_commissioning,
        "structure-parity-mismatch": run_structure_parity_mismatch_commissioning,
        "publication-pointer-conflict": run_publication_pointer_conflict_commissioning,
        "r2-read-timeout": run_r2_read_timeout_commissioning,
    }
    try:
        result = runners[args.command](
            root=args.root,
            release_id=args.release_id,
            config_id=args.config_id,
        )
    except CommissioningHarnessError as error:
        print(f"commissioning-blocked: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "CommissioningHarnessError",
    "main",
    "run_heartbeat_outage_commissioning",
    "run_normalization_payload_corrupt_commissioning",
    "run_publication_pointer_conflict_commissioning",
    "run_r2_read_timeout_commissioning",
    "run_progress_stall_commissioning",
    "run_quote_admission_missing_shard_commissioning",
    "run_quote_batch_incomplete_commissioning",
    "run_retry_budget_commissioning",
    "run_source_receipt_gap_commissioning",
    "run_structure_parity_mismatch_commissioning",
    "run_stale_owner_commissioning",
    "run_worker_exit_commissioning",
]


if __name__ == "__main__":
    raise SystemExit(main())
