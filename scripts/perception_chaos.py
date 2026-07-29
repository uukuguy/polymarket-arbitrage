#!/usr/bin/env python3
"""Fail-closed production qualification matrix for M1 perception faults.

The plan command is read-only.  Execute intentionally refuses every fault until
its adapter has an independently reviewed implementation.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any

if __package__:
    from scripts import perception_fault_readonly as readonly
else:  # pragma: no cover - direct operator entrypoint
    import perception_fault_readonly as readonly

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


class AdapterFailedError(RuntimeError):
    """The bounded fault adapter could not prove a safe complete lifecycle."""


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
            "child-nonzero",
            "neg_risk_candidate_success_receipts",
            "allow supervisor restart and verify a newer success receipt",
        ),
        _spec(
            "discovery-exit",
            "discovery",
            "child-nonzero",
            "neg_risk_discovery_batches",
            "allow supervisor restart and verify a newer completed batch",
        ),
        _spec(
            "reconciliation-stall",
            "reconciliation",
            "child-stalled",
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
FAULTS["candidate-exit"] = replace(
    FAULTS["candidate-exit"],
    execute_supported=True,
)
FAULTS["discovery-exit"] = replace(
    FAULTS["discovery-exit"],
    execute_supported=True,
)
FAULTS["reconciliation-stall"] = replace(
    FAULTS["reconciliation-stall"],
    execute_supported=True,
)


def _write_exclusive(path: Path, payload: Mapping[str, object]) -> None:
    serialized = (
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        + "\n"
    )
    with path.open("x") as stream:
        stream.write(serialized)
        stream.flush()
        os.fsync(stream.fileno())


def _command(
    argv: Sequence[str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv,
        text=True,
        capture_output=True,
        check=False,
    )


def _json_stdout(result: subprocess.CompletedProcess[str], reason: str) -> Mapping[str, Any]:
    if result.returncode != 0:
        raise AdapterFailedError(f"{reason}:exit-{result.returncode}")
    for line in reversed(result.stdout.splitlines()):
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            return payload
    raise AdapterFailedError(f"{reason}:json-missing")


def _available(value: object, reason: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or value.get("status") != "available":
        raise AdapterFailedError(reason)
    return value


def _resume_reconciliation_worker(
    *,
    command: Callable[[Sequence[str]], subprocess.CompletedProcess[str]],
    machine_id: str,
    pid: int,
    expected_release: str,
    inner_authorization: str,
) -> None:
    resumed = _json_stdout(
        command(
            (
                "flyctl",
                "ssh",
                "console",
                "-a",
                "polyarb-l1",
                "--machine",
                machine_id,
                "-C",
                "python -m polyarb.perception.chaos_primitive "
                f"resume --expected-pid {pid} "
                f"--expected-release {expected_release} "
                f"--authorization {inner_authorization}",
            )
        ),
        "reconciliation-resume",
    )
    if (
        resumed.get("action") != "sigcont"
        or resumed.get("component") != "reconciliation"
        or resumed.get("pid") != pid
    ):
        raise AdapterFailedError("reconciliation-resume-invalid")


def execute_producer_fault(
    *,
    component: str,
    fault_id: str,
    primitive: str,
    expected_action: str,
    expected_incident_kind: str,
    base_url: str,
    expected_release: str,
    authorization: str,
    evidence_dir: Path,
    timeout_s: float,
    command: Callable[[Sequence[str]], subprocess.CompletedProcess[str]] = _command,
    fetch_json: Callable[[str, str], tuple[object, float]] = readonly._fetch_json,
    collect_rounds: Callable[..., list[dict[str, object]]] = readonly.collect_rounds,
    build_evidence: Callable[..., dict[str, object]] = readonly.build_evidence,
    clock_ms: Callable[[], int] = lambda: int(time.time() * 1_000),
    monotonic: Callable[[], float] = time.monotonic,
    sleeper: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    if (
        component not in {"candidate", "discovery", "reconciliation"}
        or primitive not in {"terminate", "stall"}
    ):
        raise AdapterFailedError("unsupported-producer-fault")
    if timeout_s <= 0 or timeout_s > 600:
        raise AdapterFailedError("invalid-timeout")
    image_check = command(("make", "chaos-l2-fly-image-check", "required=python"))
    if image_check.returncode != 0:
        raise AdapterFailedError("image-check-failed")

    baseline_rounds = collect_rounds(
        base_url,
        sample_count=5,
        interval_s=1,
        fetch_json=fetch_json,
        clock_ms=clock_ms,
        sleeper=sleeper,
    )
    baseline = build_evidence(
        baseline_rounds,
        expected_release=expected_release,
    )
    if baseline.get("open_incident_count") != 0:
        raise AdapterFailedError("baseline-open-incident")
    if baseline.get("cross_membership_quote_batches") != 0:
        raise AdapterFailedError("baseline-cross-membership")
    if baseline.get("orphan_collecting_runs") != 0:
        raise AdapterFailedError("baseline-orphan-collecting")
    machine_id = baseline.get("machine_id")
    boot_id = baseline.get("boot_id")
    if not isinstance(machine_id, str) or not machine_id:
        raise AdapterFailedError("baseline-machine-missing")
    if not isinstance(boot_id, str) or not boot_id:
        raise AdapterFailedError("baseline-boot-missing")

    locate = _json_stdout(
        command(
            (
                "flyctl",
                "ssh",
                "console",
                "-a",
                "polyarb-l1",
                "--machine",
                machine_id,
                "-C",
                "python -m polyarb.perception.chaos_primitive "
                f"locate --component {component}",
            )
        ),
        f"{component}-locate",
    )
    pid = locate.get("pid")
    if (
        locate.get("action") != "locate"
        or locate.get("component") != component
        or type(pid) is not int
        or pid <= 1
    ):
        raise AdapterFailedError(f"{component}-locate-invalid")

    evidence_dir.mkdir()
    _write_exclusive(
        evidence_dir / "intent.json",
        {
            "authorization": authorization,
            "boot_id": boot_id,
            "expected_release": expected_release,
            "fault_id": fault_id,
            "machine_id": machine_id,
            "pid": pid,
        },
    )
    injection_started_at_ms = clock_ms()
    inner_authorization = f"fault:{fault_id}:{expected_release}:{pid}"
    primitive_args = (
        f"terminate --component {component}"
        if primitive == "terminate"
        else "stall"
    )
    injected = _json_stdout(
        command(
            (
                "flyctl",
                "ssh",
                "console",
                "-a",
                "polyarb-l1",
                "--machine",
                machine_id,
                "-C",
                "python -m polyarb.perception.chaos_primitive "
                f"{primitive_args} --expected-pid {pid} "
                f"--expected-release {expected_release} "
                f"--authorization {inner_authorization}",
            )
        ),
        f"{component}-terminate",
    )
    if (
        injected.get("action") != expected_action
        or injected.get("component") != component
        or injected.get("pid") != pid
    ):
        raise AdapterFailedError(f"{component}-terminate-invalid")

    deadline = monotonic() + timeout_s
    history: Mapping[str, Any] | None = None
    incident_id: str | None = None
    resumed = primitive != "stall"
    try:
        while monotonic() < deadline:
            recent_body, _ = fetch_json(
                base_url,
                "/perception/incidents/recent"
                f"?scope={component}&after_ms={injection_started_at_ms}&limit=10",
            )
            recent = _available(recent_body, "recent-incidents-unavailable")
            items = recent.get("items")
            if not isinstance(items, list):
                raise AdapterFailedError("recent-incidents-invalid")
            matches = [
                item
                for item in items
                if isinstance(item, Mapping)
                and item.get("kind") == expected_incident_kind
            ]
            ids = {
                item.get("incident_id")
                for item in matches
                if isinstance(item.get("incident_id"), str)
            }
            if len(ids) > 1:
                raise AdapterFailedError(f"{component}-incident-ambiguous")
            if ids:
                incident_id = next(iter(ids))
                if not resumed:
                    _resume_reconciliation_worker(
                        command=command,
                        machine_id=machine_id,
                        pid=pid,
                        expected_release=expected_release,
                        inner_authorization=inner_authorization,
                    )
                    resumed = True
                history_body, _ = fetch_json(
                    base_url,
                    f"/perception/incidents/{incident_id}/history",
                )
                history = _available(history_body, "incident-history-unavailable")
                history_items = history.get("items")
                if not isinstance(history_items, list) or not history_items:
                    raise AdapterFailedError("incident-history-invalid")
                terminal = history_items[-1]
                if isinstance(terminal, Mapping) and terminal.get("state") == "escalated":
                    raise AdapterFailedError(f"{component}-incident-escalated")
                if isinstance(terminal, Mapping) and terminal.get("state") == "verified":
                    break
            sleeper(0.25)
        else:
            raise AdapterFailedError(f"{component}-recovery-timeout")
    finally:
        if not resumed:
            _resume_reconciliation_worker(
                command=command,
                machine_id=machine_id,
                pid=pid,
                expected_release=expected_release,
                inner_authorization=inner_authorization,
            )

    assert history is not None and incident_id is not None
    if history.get("history_complete") is not True:
        raise AdapterFailedError("incident-history-incomplete")
    receipt = history.get("recovery_writer_receipt")
    if (
        not isinstance(receipt, Mapping)
        or receipt.get("component") != component
        or type(receipt.get("receipt_row_id")) is not int
        or receipt["receipt_row_id"] <= 0
    ):
        raise AdapterFailedError(f"{component}-recovery-receipt-missing")
    events = history["items"]
    assert isinstance(events, list)
    by_state = {
        item.get("state"): item.get("occurred_at_ms")
        for item in events
        if isinstance(item, Mapping)
        and type(item.get("occurred_at_ms")) is int
    }
    detected_at_ms = by_state.get("detected")
    contained_at_ms = by_state.get("contained")
    if (
        type(detected_at_ms) is not int
        or type(contained_at_ms) is not int
        or detected_at_ms < injection_started_at_ms
        or contained_at_ms < detected_at_ms
    ):
        raise AdapterFailedError(f"{component}-lifecycle-timing-invalid")

    post_rounds = collect_rounds(
        base_url,
        sample_count=5,
        interval_s=1,
        fetch_json=fetch_json,
        clock_ms=clock_ms,
        sleeper=sleeper,
    )
    evidence = build_evidence(
        [*baseline_rounds, *post_rounds],
        expected_release=expected_release,
    )
    evidence.update(
        {
            "scope": "production-fault",
            "mttd_s": (detected_at_ms - injection_started_at_ms) / 1_000,
            "containment_s": (contained_at_ms - detected_at_ms) / 1_000,
            "incidents": [
                {
                    "component": component,
                    "incident_id": incident_id,
                    "state": "verified",
                    "recovery_writer_receipt": dict(receipt),
                }
            ],
        }
    )
    _write_exclusive(evidence_dir / "evidence.json", evidence)
    return evidence


def execute_producer_exit(*, component: str, **kwargs) -> dict[str, object]:
    return execute_producer_fault(
        component=component,
        fault_id=f"{component}-exit",
        primitive="terminate",
        expected_action="sigterm",
        expected_incident_kind="child-nonzero",
        **kwargs,
    )


def execute_reconciliation_stall(**kwargs) -> dict[str, object]:
    return execute_producer_fault(
        component="reconciliation",
        fault_id="reconciliation-stall",
        primitive="stall",
        expected_action="sigstop",
        expected_incident_kind="child-stalled",
        **kwargs,
    )


execute_candidate_exit = partial(execute_producer_exit, component="candidate")


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
    execute.add_argument(
        "--base-url",
        default="https://polyarb-l1.fly.dev",
    )
    execute.add_argument("--timeout-s", type=float, default=120)
    return parser


def _execute(args: argparse.Namespace) -> int:
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
    if not FAULTS[args.fault].execute_supported:
        print(f"adapter-not-implemented: {args.fault}", file=sys.stderr)
        return 2
    try:
        base_url = readonly._validate_base_url(args.base_url)
        adapter = (
            execute_reconciliation_stall
            if args.fault == "reconciliation-stall"
            else partial(
                execute_producer_exit,
                component=FAULTS[args.fault].component,
            )
        )
        evidence = adapter(
            base_url=base_url,
            expected_release=release,
            authorization=args.authorization,
            evidence_dir=args.evidence_dir,
            timeout_s=args.timeout_s,
        )
        print(
            json.dumps(
                {
                    "evidence_dir": str(args.evidence_dir),
                    "fault_id": args.fault,
                    "incident_id": evidence["incidents"][0]["incident_id"],
                    "status": "evidence-ready",
                },
                sort_keys=True,
            )
        )
        return 0
    except (AdapterFailedError, OSError, TypeError, ValueError) as exc:
        if args.evidence_dir.is_dir():
            try:
                _write_exclusive(
                    args.evidence_dir / "failure.json",
                    {
                        "fault_id": args.fault,
                        "reason": str(exc),
                        "status": "failed",
                    },
                )
            except (FileExistsError, OSError, TypeError, ValueError):
                pass
        print(f"adapter-failed: {exc}", file=sys.stderr)
        return 2


def main() -> int:
    args = _parser().parse_args()
    if args.command == "plan":
        print(json.dumps(FAULTS[args.fault].plan(), sort_keys=True))
        return 0
    return _execute(args)


if __name__ == "__main__":
    raise SystemExit(main())
