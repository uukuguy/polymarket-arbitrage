"""Isolated-database harness for executable M1 commissioning attacks."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Final

from .production_commissioning import PRODUCTION_CHAIN
from .production_commissioning_disposable import StaleOwnerCommissioningAdapter
from .production_commissioning_runner import (
    AttackIdentity,
    CommissioningAttackError,
    run_disposable_attack,
)
from .runtime_fault_matrix import (
    RuntimeFaultMatrixError,
    migrated_disposable_control_plane_database,
    validated_control_plane_test_dsn,
)

_ATTACK_ID: Final[str] = "stale-owner-terminal-write"
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


def run_stale_owner_commissioning(
    *,
    root: Path,
    release_id: str,
    config_id: str,
    node_ids: Sequence[str] | None = None,
) -> dict[str, object]:
    """Prove the stale-owner fence on real transactions in disposable databases.

    ``node_ids`` exists for integration tests. The command-line contract always
    executes the complete eight-node production chain.
    """

    selected = _selected_nodes(node_ids)
    try:
        identities = tuple(
            AttackIdentity(
                experiment_id=f"commission:{node_id}:{_ATTACK_ID}",
                release_id=release_id,
                config_id=config_id,
                node_id=node_id,
                attack_id=_ATTACK_ID,
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
                    adapter=StaleOwnerCommissioningAdapter(
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
        "attack_id": _ATTACK_ID,
        "execution_scope": "disposable-exact-image",
        "node_count": len(identities),
        "proof_count": proof_count,
        "status": "pass",
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subcommands = parser.add_subparsers(dest="command", required=True)
    stale_owner = subcommands.add_parser("stale-owner")
    stale_owner.add_argument("--root", type=Path, required=True)
    stale_owner.add_argument("--release-id", required=True)
    stale_owner.add_argument("--config-id", required=True)
    stale_owner.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = run_stale_owner_commissioning(
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
    "run_stale_owner_commissioning",
]


if __name__ == "__main__":
    raise SystemExit(main())
