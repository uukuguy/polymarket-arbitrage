#!/usr/bin/env python3
"""Fail-closed production qualification matrix for M1 perception faults.

The plan command is read-only.  Execute intentionally refuses every fault until
its adapter has an independently reviewed implementation.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from pathlib import Path

_RELEASE_RE = re.compile(r"[0-9a-f]{40}")


@dataclass(frozen=True)
class FaultSpec:
    fault_id: str
    component: str
    expected_incident_kind: str
    recovery_writer: str
    cleanup: str
    required_tools: tuple[str, ...] = ("python",)
    image_check: str = "make chaos-l2-fly-image-check"
    execute_supported: bool = False

    def plan(self) -> dict[str, object]:
        result = asdict(self)
        result["required_tools"] = list(self.required_tools)
        return result


def _spec(
    fault_id: str,
    component: str,
    expected_incident_kind: str,
    recovery_writer: str,
    cleanup: str,
) -> FaultSpec:
    return FaultSpec(
        fault_id=fault_id,
        component=component,
        expected_incident_kind=expected_incident_kind,
        recovery_writer=recovery_writer,
        cleanup=cleanup,
    )


FAULTS = {
    spec.fault_id: spec
    for spec in (
        _spec(
            "gamma-timeout",
            "discovery",
            "not-wired: batch error log; adapter must force child-timeout",
            "neg_risk_discovery_batches",
            "remove scoped Gamma timeout proxy and verify a newer completed batch",
        ),
        _spec(
            "gamma-partial",
            "discovery",
            "not-wired: batch error log; adapter must force child-failed",
            "neg_risk_discovery_batches",
            "remove scoped partial-response proxy and verify cursor progress",
        ),
        _spec(
            "gamma-malformed",
            "discovery",
            "not-wired: batch error log; adapter must force child-failed",
            "neg_risk_discovery_batches",
            "remove scoped malformed-response proxy and verify a completed batch",
        ),
        _spec(
            "gamma-cursor",
            "reconciliation",
            "not-wired: batch error log; adapter must force child-failed",
            "neg_risk_reconciliation_windows",
            "remove scoped cursor loop and verify pages_completed advances",
        ),
        _spec(
            "clob-missing-leg",
            "candidate",
            "not-wired: group error state; adapter must force child-failed",
            "neg_risk_candidate_success_receipts",
            "restore the scoped CLOB leg and verify a newer success receipt",
        ),
        _spec(
            "clob-429",
            "candidate",
            "not-wired: group error state; adapter must force child-failed",
            "neg_risk_candidate_success_receipts",
            "remove scoped 429 proxy and verify a newer success receipt",
        ),
        _spec(
            "clob-latency",
            "candidate",
            "child-timeout",
            "neg_risk_candidate_success_receipts",
            "remove scoped latency proxy and verify a newer success receipt",
        ),
        _spec(
            "candidate-exit",
            "candidate",
            "child-failed",
            "neg_risk_candidate_success_receipts",
            "allow supervisor restart and verify a newer success receipt",
        ),
        _spec(
            "discovery-exit",
            "discovery",
            "child-failed",
            "neg_risk_discovery_batches",
            "allow supervisor restart and verify a newer completed batch",
        ),
        _spec(
            "reconciliation-stall",
            "reconciliation",
            "child-timeout",
            "neg_risk_reconciliation_windows",
            "release scoped stall and verify pages_completed advances",
        ),
        _spec(
            "sqlite-busy",
            "candidate",
            "child-failed",
            "neg_risk_candidate_success_receipts",
            "release bounded SQLite lock and verify a newer success receipt",
        ),
        _spec(
            "disk-pressure",
            "resource",
            "not-wired: resource decision only; adapter must open an incident",
            "neg_risk_resource_decisions",
            "remove bounded filler and verify a newer healthy resource decision",
        ),
        _spec(
            "telegram-failure",
            "notification",
            "not-wired: notification error state; adapter must open an incident",
            "opportunity_watcher_state",
            "remove scoped Telegram failure and verify notification recovery",
        ),
        _spec(
            "daemon-restart",
            "http",
            "child-abandoned",
            "neg_risk_http_probe_receipts",
            "verify new boot identity and a responsive release-bound probe",
        ),
        _spec(
            "deploy-interrupt",
            "http",
            "child-abandoned",
            "neg_risk_http_probe_receipts",
            "verify expected release and a responsive release-bound probe",
        ),
        _spec(
            "contention",
            "resource",
            "not-wired: resource decision only; adapter must open an incident",
            "neg_risk_resource_decisions",
            "stop bounded load and verify a newer healthy resource decision",
        ),
    )
}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan = subparsers.add_parser("plan")
    plan.add_argument("--fault", required=True, choices=sorted(FAULTS))
    execute = subparsers.add_parser("execute")
    execute.add_argument("--fault", required=True, choices=sorted(FAULTS))
    execute.add_argument("--expected-release", required=True)
    execute.add_argument("--authorization", required=True)
    execute.add_argument("--evidence-dir", type=Path, required=True)
    return parser


def _refuse_execute(args: argparse.Namespace) -> int:
    release = args.expected_release
    if _RELEASE_RE.fullmatch(release) is None:
        print("invalid-expected-release", file=sys.stderr)
        return 2
    if args.authorization != f"fault:{args.fault}:{release}":
        print("invalid-fault-authorization", file=sys.stderr)
        return 2
    if args.evidence_dir.exists():
        print("evidence-dir-already-exists", file=sys.stderr)
        return 2
    print(f"adapter-not-implemented: {args.fault}", file=sys.stderr)
    return 2


def main() -> int:
    args = _parser().parse_args()
    if args.command == "plan":
        print(json.dumps(FAULTS[args.fault].plan(), sort_keys=True))
        return 0
    return _refuse_execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
