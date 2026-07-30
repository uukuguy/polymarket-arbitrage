#!/usr/bin/env python3
"""Fail-closed production qualification matrix for M1 perception faults.

The plan command is read-only.  Execute intentionally refuses every fault until
its adapter has an independently reviewed implementation.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import secrets
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from functools import partial
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

from polyarb.perception.fault_auth import fault_hmac_message
from polyarb.safe_artifact import write_exclusive_bytes

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
            "gamma-timeout",
            "neg_risk_discovery_batches",
            "remove scoped Gamma timeout proxy and verify a newer completed batch",
        ),
        _spec(
            "gamma-partial",
            "discovery",
            "coverage:partial-or-rejected-page",
            "neg_risk_discovery_batches",
            "remove scoped partial response and verify coverage quality recovers",
        ),
        _spec(
            "gamma-malformed",
            "discovery",
            "gamma-malformed",
            "neg_risk_discovery_batches",
            "remove scoped malformed-response proxy and verify a completed batch",
        ),
        _spec(
            "gamma-cursor",
            "reconciliation",
            "gamma-cursor",
            "neg_risk_reconciliation_windows",
            "remove scoped cursor loop and verify pages_completed advances",
        ),
        _spec(
            "clob-missing-leg",
            "candidate",
            "clob-missing-leg",
            "neg_risk_candidate_success_receipts",
            "restore the scoped CLOB leg and verify a newer success receipt",
        ),
        _spec(
            "clob-429",
            "candidate",
            "clob-429",
            "neg_risk_candidate_success_receipts",
            "remove scoped 429 proxy and verify a newer success receipt",
        ),
        _spec(
            "clob-latency",
            "candidate",
            "clob-latency",
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
            "sqlite-busy",
            "neg_risk_candidate_success_receipts",
            "release bounded SQLite lock and verify a newer success receipt",
        ),
        _spec(
            "disk-pressure",
            "resource",
            "resource-disk-pressure",
            "neg_risk_resource_decisions",
            "remove bounded filler and verify a newer healthy resource decision",
        ),
        _spec(
            "telegram-failure",
            "notification",
            "telegram-delivery-failed",
            "neg_risk_opportunity_notification_attempts",
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
            "resource-contention",
            "neg_risk_resource_decisions",
            "stop bounded load and verify a newer healthy resource decision",
        ),
    )
}
_UPSTREAM_FAULTS = frozenset(
    {
        "gamma-timeout",
        "gamma-partial",
        "gamma-malformed",
        "gamma-cursor",
        "clob-missing-leg",
        "clob-429",
        "clob-latency",
        "telegram-failure",
    }
)
_RECOVERY_TABLE_BY_COMPONENT = {
    "candidate": "neg_risk_candidate_success_receipts",
    "discovery": "neg_risk_discovery_batches",
    "reconciliation": "neg_risk_reconciliation_windows",
    "notification": "neg_risk_opportunity_notification_attempts",
}
for _upstream_fault_id in _UPSTREAM_FAULTS:
    FAULTS[_upstream_fault_id] = replace(
        FAULTS[_upstream_fault_id],
        execute_supported=True,
    )


def execute_upstream_matrix(
    fault_ids: Sequence[str],
    *,
    executor: Callable[[str], object],
) -> list[object]:
    """Execute serially and freeze immediately when cleanup proof fails."""
    results: list[object] = []
    for fault_id in fault_ids:
        if fault_id not in _UPSTREAM_FAULTS:
            raise AdapterFailedError("unsupported-upstream-fault")
        try:
            results.append(executor(fault_id))
        except BaseException as exc:
            if "cleanup-failed" in str(exc):
                raise AdapterFailedError(
                    f"matrix-frozen:{fault_id}:cleanup-failed"
                ) from exc
            raise
    return results


def _signed_post_json(
    base_url: str,
    path: str,
    payload: Mapping[str, object],
) -> Mapping[str, object]:
    body = json.dumps(
        payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    ordinary_secret = os.getenv("POLYARB_SCAN_SHARED_SECRET", "")
    fault_secret = os.getenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", "")
    if not ordinary_secret or not fault_secret:
        raise AdapterFailedError("control-authority-unavailable")
    timestamp = str(int(time.time()))
    ordinary_nonce = secrets.token_hex(16)
    fault_nonce = secrets.token_hex(16)
    ordinary = b"\n".join(
        (timestamp.encode(), ordinary_nonce.encode(), b"POST", path.encode(), body)
    )
    fault = fault_hmac_message(
        timestamp=timestamp,
        nonce=fault_nonce,
        method="POST",
        path=path,
        body=body,
    )
    headers = {
        "Content-Type": "application/json",
        "X-Perception-Timestamp": timestamp,
        "X-Perception-Nonce": ordinary_nonce,
        "X-Signature": hmac.new(ordinary_secret.encode(), ordinary, hashlib.sha256).hexdigest(),
        "X-Fault-Timestamp": timestamp,
        "X-Fault-Nonce": fault_nonce,
        "X-Fault-Signature": hmac.new(fault_secret.encode(), fault, hashlib.sha256).hexdigest(),
    }
    request = Request(base_url.rstrip("/") + path, data=body, headers=headers, method="POST")
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            raw = response.read(_MAX_HTTP_BYTES + 1)
        if len(raw) > _MAX_HTTP_BYTES:
            raise AdapterFailedError("control-response-oversized")
        value = json.loads(raw)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise AdapterFailedError(f"control-request-failed:{type(exc).__name__}") from exc
    if not isinstance(value, Mapping):
        raise AdapterFailedError("control-response-invalid")
    return value


def _signed_get_json(base_url: str, path: str) -> Mapping[str, object]:
    ordinary_secret = os.getenv("POLYARB_SCAN_SHARED_SECRET", "")
    fault_secret = os.getenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", "")
    if not ordinary_secret or not fault_secret:
        raise AdapterFailedError("control-authority-unavailable")
    timestamp = str(int(time.time()))
    ordinary_nonce = secrets.token_hex(16)
    fault_nonce = secrets.token_hex(16)
    body = b""
    ordinary = b"\n".join(
        (timestamp.encode(), ordinary_nonce.encode(), b"GET", path.encode(), body)
    )
    fault = fault_hmac_message(
        timestamp=timestamp,
        nonce=fault_nonce,
        method="GET",
        path=path,
        body=body,
    )
    request = Request(
        base_url.rstrip("/") + path,
        headers={
            "X-Perception-Timestamp": timestamp,
            "X-Perception-Nonce": ordinary_nonce,
            "X-Signature": hmac.new(
                ordinary_secret.encode(), ordinary, hashlib.sha256
            ).hexdigest(),
            "X-Fault-Timestamp": timestamp,
            "X-Fault-Nonce": fault_nonce,
            "X-Fault-Signature": hmac.new(
                fault_secret.encode(), fault, hashlib.sha256
            ).hexdigest(),
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=10) as response:  # noqa: S310
            raw = response.read(_MAX_HTTP_BYTES + 1)
        if len(raw) > _MAX_HTTP_BYTES:
            raise AdapterFailedError("control-response-oversized")
        value = json.loads(raw)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise AdapterFailedError(f"control-request-failed:{type(exc).__name__}") from exc
    if not isinstance(value, Mapping):
        raise AdapterFailedError("control-response-invalid")
    return value


_MAX_HTTP_BYTES = 1_048_576


class UpstreamHttpTransport:
    """Typed production transport; injectable GET/POST functions enable local E2E."""

    def __init__(
        self,
        *,
        base_url: str,
        expected_release: str,
        timeout_s: float,
        fetch_json: Callable[[str, str], tuple[object, float]] = readonly._fetch_json,
        post_json: Callable[[str, str, Mapping[str, object]], Mapping[str, object]]
        = _signed_post_json,
        control_get_json: Callable[[str, str], Mapping[str, object]]
        = _signed_get_json,
        sleeper: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.base_url = base_url
        self.expected_release = expected_release
        self.timeout_s = timeout_s
        self.fetch_json = fetch_json
        self.post_json = post_json
        self.control_get_json = control_get_json
        self.sleeper = sleeper
        self.monotonic = monotonic
        self._baseline: dict[str, object] | None = None
        self._status: Mapping[str, object] | None = None

    def preflight(self) -> None:
        ordinary = os.getenv("POLYARB_SCAN_SHARED_SECRET", "")
        fault = os.getenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", "")
        if not ordinary or not fault:
            raise AdapterFailedError("control-authority-unavailable")
        if hmac.compare_digest(ordinary, fault):
            raise AdapterFailedError("control-authorities-not-distinct")

    def _get(self, path: str) -> Mapping[str, object]:
        value, _ = self.fetch_json(self.base_url, path)
        if not isinstance(value, Mapping):
            raise AdapterFailedError("readonly-response-invalid")
        return value

    def _wait_status(self, fault_id: str, states: set[str]) -> Mapping[str, object]:
        deadline = self.monotonic() + self.timeout_s
        while self.monotonic() < deadline:
            status = self._get(f"/perception/faults/{fault_id}")
            if status.get("state") in states:
                self._status = status
                return status
            self.sleeper(0.1)
        raise AdapterFailedError("fault-status-timeout")

    def __call__(self, operation: str, payload: Mapping[str, object]) -> Mapping[str, object]:
        if operation == "baseline":
            rounds = readonly.collect_rounds(
                self.base_url,
                sample_count=5,
                interval_s=0,
                fetch_json=self.fetch_json,
                sleeper=self.sleeper,
            )
            evidence = readonly.build_evidence(
                rounds, expected_release=self.expected_release
            )
            if any(
                evidence.get(field) != 0
                for field in (
                    "open_incident_count",
                    "cross_membership_quote_batches",
                    "orphan_collecting_runs",
                )
            ):
                return {"status": "not-green"}
            self._baseline = evidence
            return {"status": "green"}
        if operation == "runtime":
            value = self._get(
                f"/perception/faults/runtime?component={payload['component']}"
            )
            runtime = value.get("runtime")
            return runtime if isinstance(runtime, Mapping) else {}
        if operation == "arm":
            source_intent = payload["intent"]
            assert isinstance(source_intent, Mapping)
            body = {
                "fault_id": source_intent["fault_id"],
                "kind": source_intent["kind"],
                "call_class": source_intent["call_class"],
                "target_key": source_intent["target_key"],
                "parameters": source_intent["parameters"],
                "ttl_ms": min(120_000, max(1_000, int(self.timeout_s * 1_000))),
                "runtime": source_intent["runtime"],
            }
            response = self.post_json(
                self.base_url, "/control/perception/faults/arm", body
            )
            if not isinstance(response, Mapping):
                raise AdapterFailedError("arm-response-invalid")
            return response
        if operation == "admission":
            fault_id = str(payload["fault_id"])
            status = self._get(f"/perception/faults/{fault_id}")
            return {
                "admitted": (
                    status.get("status") == "available"
                    and status.get("fault_id") == fault_id
                    and status.get("state") not in {None, "rejected"}
                )
            }
        if operation == "observe":
            status = self._wait_status(
                str(payload["fault_id"]),
                {"detected", "contained", "cleaned", "recovered"},
            )
            events = status.get("events")
            if not isinstance(events, list):
                raise AdapterFailedError("fault-events-missing")
            injected = [
                {
                    **dict(event.get("evidence", {})),
                    "occurred_at_ms": event.get("occurred_at_ms"),
                    "kind": payload["kind"],
                    "call_class": payload["call_class"],
                    "target_key": payload["target_key"],
                    "runtime": payload["runtime"],
                }
                for event in events
                if isinstance(event, Mapping) and event.get("state") == "injected"
            ]
            detections = [
                dict(event.get("evidence", {}))
                for event in events
                if isinstance(event, Mapping) and event.get("state") == "detected"
            ]
            key = "coverage" if payload["kind"] == "gamma-partial" else "incidents"
            return {"injections": injected, key: detections}
        if operation == "cleanup":
            fault_id = str(payload["fault_id"])
            self.post_json(
                self.base_url,
                "/control/perception/faults/cleanup",
                {"fault_id": fault_id},
            )
            status = self._wait_status(
                fault_id, {"cleaned", "recovered", "verified"}
            )
            events = status.get("events")
            cleaned = next(
                (
                    event for event in reversed(events)
                    if isinstance(event, Mapping) and event.get("state") == "cleaned"
                ),
                None,
            ) if isinstance(events, list) else None
            if not isinstance(cleaned, Mapping):
                raise AdapterFailedError("cleanup-receipt-missing")
            evidence = cleaned.get("evidence")
            if not isinstance(evidence, Mapping):
                raise AdapterFailedError("cleanup-receipt-missing")
            confirmation = next(
                (
                    event
                    for event in reversed(events)
                    if isinstance(event, Mapping)
                    and event.get("action") == "cleanup-confirmed"
                ),
                None,
            )
            confirmation_evidence = (
                confirmation.get("evidence")
                if isinstance(confirmation, Mapping)
                else None
            )
            try:
                if not isinstance(confirmation_evidence, Mapping):
                    raise KeyError("cleanup-confirmed")
                memory_cleared = int(
                    str(confirmation_evidence["memory_cleared_at_ms"])
                )
                persisted = int(
                    str(confirmation_evidence["receipt_commit_confirmed_at_ms"])
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise AdapterFailedError("cleanup-receipt-missing") from exc
            return {
                "status": "cleaned",
                "memory_cleared_at_ms": memory_cleared,
                "receipt_persisted_at_ms": persisted,
            }
        if operation == "recovery":
            cleanup = payload["cleanup"]
            assert isinstance(cleanup, Mapping)
            status = self._wait_status(
                str(payload["fault_id"]), {"recovered", "verified"}
            )
            events = status.get("events")
            recovered = next(
                (
                    event for event in reversed(events)
                    if isinstance(event, Mapping) and event.get("state") == "recovered"
                ),
                None,
            ) if isinstance(events, list) else None
            intent = status.get("intent")
            if not isinstance(recovered, Mapping) or not isinstance(intent, Mapping):
                raise AdapterFailedError("recovery-receipt-missing")
            component = intent.get("runtime", {}).get("component")
            evidence = recovered.get("evidence")
            return {
                "recovery_writer_receipt": {
                    "component": component,
                    "table": _RECOVERY_TABLE_BY_COMPONENT[str(component)],
                    "row_id": (
                        evidence.get("recovery_id")
                        if isinstance(evidence, Mapping)
                        else None
                    ),
                    "occurred_at_ms": recovered.get("occurred_at_ms"),
                }
            }
        if operation == "export":
            envelope = self.control_get_json(
                self.base_url,
                f"/perception/faults/{payload['fault_id']}/export",
            )
            exported_intent = envelope.get("fault_intent")
            if (
                envelope.get("scope") != "production-fault"
                or not isinstance(exported_intent, Mapping)
                or exported_intent.get("fault_id") != payload["fault_id"]
            ):
                raise AdapterFailedError("fault-export-invalid")
            return envelope
        raise AdapterFailedError(f"unsupported-transport-operation:{operation}")


def execute_upstream_fault(
    *,
    fault_id: str,
    release_id: str,
    machine_id: str,
    boot_id: str,
    call_class: str,
    target_key: str,
    parameters: Mapping[str, object],
    evidence_dir: Path,
    transport: Callable[[str, Mapping[str, object]], Mapping[str, object]],
) -> dict[str, object]:
    """Run one typed fault through a dependency-injected control transport.

    The production CLI supplies a doubly-authenticated transport. Tests use a
    local server/SQLite clone. This function never knows any signing secret.
    """
    required = {
        "release-id": release_id,
        "machine-id": machine_id,
        "boot-id": boot_id,
        "call-class": call_class,
        "target-key": target_key,
    }
    for label, value in required.items():
        if not isinstance(value, str) or not value:
            raise AdapterFailedError(f"missing-{label}")
    if fault_id not in _UPSTREAM_FAULTS:
        raise AdapterFailedError("unsupported-upstream-fault")
    if _RELEASE_RE.fullmatch(release_id) is None:
        raise AdapterFailedError("invalid-release-id")
    try:
        UUID(boot_id)
    except (TypeError, ValueError) as exc:
        raise AdapterFailedError("invalid-boot-id") from exc
    if evidence_dir.exists():
        raise AdapterFailedError("evidence-dir-already-exists")
    preflight = getattr(transport, "preflight", None)
    if callable(preflight):
        preflight()

    expected_runtime = {
        "component": FAULTS[fault_id].component,
        "release_id": release_id,
        "machine_id": machine_id,
        "boot_id": boot_id,
    }
    experiment_id = (
        f"{fault_id}-{hashlib.sha256(secrets.token_bytes(32)).hexdigest()[:24]}"
    )
    evidence_dir.mkdir()
    intent = {
        "fault_id": experiment_id,
        "kind": fault_id,
        "call_class": call_class,
        "target_key": target_key,
        "parameters": dict(parameters),
        "runtime": expected_runtime,
    }
    _write_exclusive(evidence_dir / "intent.json", intent)
    baseline = transport("baseline", {})
    if baseline.get("status") != "green":
        raise AdapterFailedError("baseline-not-green")
    runtime = transport("runtime", {"component": FAULTS[fault_id].component})
    if any(runtime.get(key) != value for key, value in expected_runtime.items()):
        raise AdapterFailedError("runtime-identity-mismatch")

    arm_payload = {
        "intent": intent,
    }
    try:
        arm = transport("arm", arm_payload)
        if not isinstance(arm, Mapping):
            raise AdapterFailedError("arm-response-invalid")
        if arm.get("status") != "accepted":
            raise AdapterFailedError("arm-rejected")
        accepted_fault_id = arm.get("fault_id")
        if accepted_fault_id != experiment_id:
            raise AdapterFailedError("arm-response-invalid")
    except BaseException as arm_error:
        cleanup_on_unknown = False
        try:
            admission = transport(
                "admission",
                {"fault_id": experiment_id},
            )
            admitted = admission.get("admitted") is True
        except BaseException:
            admitted = True
            cleanup_on_unknown = True
        if admitted:
            try:
                cleanup = transport(
                    "cleanup",
                    {
                        "fault_id": experiment_id,
                    },
                )
                if cleanup.get("status") != "cleaned":
                    raise AdapterFailedError("cleanup-failed")
            except BaseException as cleanup_error:
                qualifier = (
                    "cleanup-after-unknown-admission-failed"
                    if cleanup_on_unknown
                    else "cleanup-after-admission-failed"
                )
                raise BaseExceptionGroup(
                    f"{qualifier}:cleanup-failed",
                    [arm_error, cleanup_error],
                )
        raise

    original: BaseException | None = None
    observed: Mapping[str, object] | None = None
    try:
        if (
            arm.get("kind") != fault_id
            or arm.get("call_class") != call_class
            or arm.get("target_key") != target_key
            or arm.get("runtime") != expected_runtime
            or not isinstance(arm.get("intent_digest"), str)
            or len(arm["intent_digest"]) != 64
        ):
            raise AdapterFailedError("arm-binding-mismatch")
        observed = transport(
            "observe",
            {
                "fault_id": accepted_fault_id,
                "kind": fault_id,
                "call_class": call_class,
                "target_key": target_key,
                "runtime": expected_runtime,
            },
        )
        injections = observed.get("injections")
        if not isinstance(injections, list) or len(injections) != 1:
            reason = (
                "duplicate-injection"
                if isinstance(injections, list) and len(injections) > 1
                else "missing-injection"
            )
            raise AdapterFailedError(reason)
        injection = injections[0]
        if (
            not isinstance(injection, Mapping)
            or injection.get("kind") != fault_id
            or injection.get("call_class") != call_class
            or injection.get("target_key") != target_key
            or injection.get("runtime") != expected_runtime
            or not isinstance(injection.get("call_id"), str)
            or not isinstance(injection.get("occurred_at_ms"), int)
        ):
            raise AdapterFailedError("injection-identity-mismatch")
        if fault_id == "gamma-partial":
            coverage = observed.get("coverage")
            if not isinstance(coverage, list) or len(coverage) != 1:
                raise AdapterFailedError("coverage-cardinality-invalid")
        else:
            incidents = observed.get("incidents")
            if not isinstance(incidents, list) or len(incidents) != 1:
                raise AdapterFailedError("incident-cardinality-invalid")
    except BaseException as exc:
        original = exc
    cleanup_error: BaseException | None = None
    cleanup: Mapping[str, object] | None = None
    try:
        cleanup = transport(
            "cleanup",
            {
                "fault_id": accepted_fault_id,
            },
        )
        if (
            cleanup.get("status") != "cleaned"
            or not isinstance(cleanup.get("memory_cleared_at_ms"), int)
            or not isinstance(cleanup.get("receipt_persisted_at_ms"), int)
            or cleanup["memory_cleared_at_ms"] > cleanup["receipt_persisted_at_ms"]
        ):
            raise AdapterFailedError("cleanup-failed")
    except BaseException as exc:
        cleanup_error = exc
    if cleanup_error is not None:
        if original is not None:
            raise BaseExceptionGroup(
                "fault-execution-and-cleanup-failed:cleanup-failed",
                [original, cleanup_error],
            )
        raise cleanup_error
    if original is not None:
        raise original
    assert observed is not None and cleanup is not None

    recovery = transport(
        "recovery",
        {"fault_id": accepted_fault_id, "cleanup": dict(cleanup)},
    )
    injection = observed["injections"][0]
    assert isinstance(injection, Mapping)
    recovery_receipt = recovery.get("recovery_writer_receipt")
    if (
        not isinstance(recovery_receipt, Mapping)
        or recovery_receipt.get("component") != expected_runtime["component"]
        or not recovery_receipt.get("table")
        or not recovery_receipt.get("row_id")
        or not isinstance(recovery_receipt.get("occurred_at_ms"), int)
        or recovery_receipt["occurred_at_ms"] <= injection["occurred_at_ms"]
        or recovery_receipt["occurred_at_ms"] <= cleanup["receipt_persisted_at_ms"]
    ):
        raise AdapterFailedError("business-recovery-invalid")
    exported = transport(
        "export",
        {
            "fault_id": accepted_fault_id,
            "recovery": dict(recovery),
            "scope": "production-fault",
        },
    )
    evidence = dict(exported)
    _write_exclusive(evidence_dir / "evidence.json", evidence)
    return evidence


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
    write_exclusive_bytes(path, serialized.encode())


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
    execute.add_argument("--authorization")
    execute.add_argument("--machine-id")
    execute.add_argument("--boot-id")
    execute.add_argument("--call-class")
    execute.add_argument("--target-key")
    execute.add_argument("--parameters-json")
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
    is_upstream = args.fault in _UPSTREAM_FAULTS
    if not FAULTS[args.fault].execute_supported:
        print(f"adapter-not-implemented: {args.fault}", file=sys.stderr)
        return 2
    if is_upstream and args.authorization is not None:
        print("upstream-authorization-argument-forbidden", file=sys.stderr)
        return 2
    if args.evidence_dir.exists():
        print("evidence-dir-already-exists", file=sys.stderr)
        return 2
    if is_upstream and not all(
        (
            args.machine_id,
            args.boot_id,
            args.call_class,
            args.target_key,
            args.parameters_json,
        )
    ):
        print("upstream-execution-requires-exact-target", file=sys.stderr)
        return 2
    try:
        base_url = readonly._validate_base_url(args.base_url)
        if is_upstream:
            parameters = json.loads(args.parameters_json)
            if not isinstance(parameters, Mapping):
                raise AdapterFailedError("parameters-json-invalid")
            transport = UpstreamHttpTransport(
                base_url=base_url,
                expected_release=release,
                timeout_s=args.timeout_s,
            )
            evidence = execute_upstream_fault(
                fault_id=args.fault,
                release_id=release,
                machine_id=args.machine_id,
                boot_id=args.boot_id,
                call_class=args.call_class,
                target_key=args.target_key,
                parameters=parameters,
                evidence_dir=args.evidence_dir,
                transport=transport,
            )
            print(
                json.dumps(
                    {
                        "evidence_dir": str(args.evidence_dir),
                        "fault_id": args.fault,
                        "status": "recovered-evidence-ready",
                        "tail_hash": evidence["fault_history_tail_hash"],
                    },
                    sort_keys=True,
                )
            )
            return 0
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
