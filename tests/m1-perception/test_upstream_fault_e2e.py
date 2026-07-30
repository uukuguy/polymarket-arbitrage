from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path

import pytest

import scripts.perception_chaos as chaos
import scripts.perception_fault_acceptance as acceptance

UPSTREAM_FAULTS = (
    "gamma-timeout",
    "gamma-partial",
    "gamma-malformed",
    "gamma-cursor",
    "clob-missing-leg",
    "clob-429",
    "clob-latency",
    "telegram-failure",
)


@pytest.mark.parametrize("fault_id", UPSTREAM_FAULTS)
def test_all_and_only_typed_upstream_faults_are_executable(fault_id: str) -> None:
    assert chaos.FAULTS[fault_id].execute_supported is True


@pytest.mark.parametrize(
    "missing",
    ("release_id", "machine_id", "boot_id", "call_class", "target_key",
         "ordinary_authorization", "fault_authorization"),
)
def test_preflight_rejects_missing_exact_identity_or_separate_authority(
    missing: str, tmp_path: Path
) -> None:
    values = {
        "fault_id": "gamma-timeout",
        "release_id": "a" * 40,
        "machine_id": "machine-1",
        "boot_id": "12345678-1234-4234-9234-123456789abc",
        "call_class": "gamma-discovery-event-page",
        "target_key": "discovery",
        "parameters": {"delay_ms": 10},
        "ordinary_authorization": "ordinary",
        "fault_authorization": "fault",
        "evidence_dir": tmp_path / "new",
    }
    values[missing] = ""
    calls: list[str] = []

    with pytest.raises(chaos.AdapterFailedError, match=missing.replace("_", "-")):
        chaos.execute_upstream_fault(**values, transport=lambda *_: calls.append("arm"))

    assert calls == []
    assert not values["evidence_dir"].exists()


def test_preflight_rejects_existing_evidence_dir_before_arm(tmp_path: Path) -> None:
    evidence_dir = tmp_path / "owned"
    evidence_dir.mkdir()
    calls: list[str] = []

    with pytest.raises(chaos.AdapterFailedError, match="evidence-dir"):
        chaos.execute_upstream_fault(
            fault_id="gamma-timeout",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id="12345678-1234-4234-9234-123456789abc",
            call_class="gamma-discovery-event-page",
            target_key="discovery",
            parameters={"delay_ms": 10},
            ordinary_authorization="ordinary",
            fault_authorization="fault",
            evidence_dir=evidence_dir,
            transport=lambda *_: calls.append("arm"),
        )

    assert calls == []


@pytest.mark.parametrize(
    "failure",
    (
        TimeoutError("detection timeout"),
        ValueError("malformed response"),
        KeyboardInterrupt(),
        SystemExit(7),
        asyncio.CancelledError(),
    ),
)
def test_cleanup_runs_for_every_base_exception_without_swallowing(
    failure: BaseException, tmp_path: Path
) -> None:
    calls: list[str] = []

    def transport(operation: str, _payload: object) -> dict[str, object]:
        calls.append(operation)
        if operation == "baseline":
            return {"status": "green"}
        if operation == "runtime":
            return {
                "release_id": "a" * 40,
                "machine_id": "machine-1",
                "boot_id": "12345678-1234-4234-9234-123456789abc",
                "component": "discovery",
            }
        if operation == "arm":
            return {
                "status": "accepted",
                "fault_id": "fault-1",
                "kind": "gamma-timeout",
                "call_class": "gamma-discovery-event-page",
                "target_key": "discovery",
                "runtime": {
                    "release_id": "a" * 40,
                    "machine_id": "machine-1",
                    "boot_id": "12345678-1234-4234-9234-123456789abc",
                    "component": "discovery",
                },
                "intent_digest": "d" * 64,
            }
        if operation == "observe":
            raise failure
        if operation == "cleanup":
            return {
                "status": "cleaned",
                "memory_cleared_at_ms": 20,
                "receipt_persisted_at_ms": 21,
            }
        raise AssertionError(operation)

    with pytest.raises(type(failure)):
        chaos.execute_upstream_fault(
            fault_id="gamma-timeout",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id="12345678-1234-4234-9234-123456789abc",
            call_class="gamma-discovery-event-page",
            target_key="discovery",
            parameters={"delay_ms": 10},
            ordinary_authorization="ordinary",
            fault_authorization="fault",
            evidence_dir=tmp_path / type(failure).__name__,
            transport=transport,
        )

    assert calls[-1] == "cleanup"


def test_cleanup_failed_freezes_remaining_matrix() -> None:
    calls: list[str] = []

    def executor(fault_id: str) -> None:
        calls.append(fault_id)
        raise chaos.AdapterFailedError("cleanup-failed:receipt unavailable")

    with pytest.raises(chaos.AdapterFailedError, match="matrix-frozen"):
        chaos.execute_upstream_matrix(
            ("gamma-timeout", "clob-429"), executor=executor
        )

    assert calls == ["gamma-timeout"]


def test_duplicate_injection_is_rejected_after_cleanup(tmp_path: Path) -> None:
    calls: list[str] = []
    runtime = {
        "component": "candidate",
        "release_id": "a" * 40,
        "machine_id": "machine-1",
        "boot_id": "12345678-1234-4234-9234-123456789abc",
    }

    def transport(operation: str, payload: object) -> dict[str, object]:
        calls.append(operation)
        if operation == "baseline":
            return {"status": "green"}
        if operation == "runtime":
            return runtime
        if operation == "arm":
            return {
                "status": "accepted",
                "fault_id": "fault-1",
                "kind": "clob-429",
                "call_class": "clob-candidate-book-batch",
                "target_key": "group-1",
                "runtime": runtime,
                "intent_digest": "d" * 64,
            }
        if operation == "observe":
            injection = {
                "kind": "clob-429",
                "call_class": "clob-candidate-book-batch",
                "target_key": "group-1",
                "runtime": runtime,
                "call_id": "call-1",
                "occurred_at_ms": 10,
            }
            return {
                "injections": [injection, deepcopy(injection)],
                "incidents": [{"incident_id": "incident-1"}],
            }
        if operation == "cleanup":
            return {
                "status": "cleaned",
                "memory_cleared_at_ms": 20,
                "receipt_persisted_at_ms": 21,
            }
        return {"status": "PASS"}

    with pytest.raises(chaos.AdapterFailedError, match="duplicate-injection"):
        chaos.execute_upstream_fault(
            fault_id="clob-429",
            release_id=runtime["release_id"],
            machine_id=runtime["machine_id"],
            boot_id=runtime["boot_id"],
            call_class="clob-candidate-book-batch",
            target_key="group-1",
            parameters={},
            ordinary_authorization="ordinary",
            fault_authorization="fault",
            evidence_dir=tmp_path / "duplicate",
            transport=transport,
        )

    assert calls[-1] == "cleanup"


@pytest.mark.parametrize(
    ("fault_id", "call_class", "target_key", "parameters"),
    (
        ("gamma-timeout", "gamma-discovery-event-page", "discovery", {"delay_ms": 10}),
        ("gamma-partial", "gamma-discovery-event-page", "discovery", {"keep_events": 1}),
        ("gamma-malformed", "gamma-discovery-event-page", "discovery", {}),
        ("gamma-cursor", "gamma-reconciliation-event-page", "reconciliation", {}),
        ("clob-missing-leg", "clob-candidate-book-batch", "group-1", {"leg_index": 0}),
        ("clob-429", "clob-candidate-book-batch", "group-1", {}),
        ("clob-latency", "clob-candidate-book-batch", "group-1", {"delay_ms": 10}),
        ("telegram-failure", "telegram-opportunity-card", "1", {}),
    ),
)
def test_cli_dispatches_each_upstream_fault_only_to_typed_http_transport(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    fault_id: str,
    call_class: str,
    target_key: str,
    parameters: dict[str, int],
) -> None:
    release = "a" * 40
    captured: dict[str, object] = {}
    transport = object()
    monkeypatch.setattr(chaos, "UpstreamHttpTransport", lambda **_kwargs: transport)

    def execute(**kwargs):
        captured.update(kwargs)
        return {"fault_history_tail_hash": "d" * 64}

    monkeypatch.setattr(chaos, "execute_upstream_fault", execute)
    monkeypatch.setattr(
        "sys.argv",
        [
            "perception_chaos.py",
            "execute",
            "--fault",
            fault_id,
            "--expected-release",
            release,
            "--authorization",
            f"fault:{fault_id}:{release}",
            "--ordinary-authorization",
            "ordinary-approval",
            "--fault-authorization",
            "fault-approval",
            "--machine-id",
            "machine-1",
            "--boot-id",
            "12345678-1234-4234-9234-123456789abc",
            "--call-class",
            call_class,
            "--target-key",
            target_key,
            "--parameters-json",
            json.dumps(parameters),
            "--base-url",
            "https://example.test",
            "--evidence-dir",
            str(tmp_path / fault_id),
        ],
    )

    assert chaos.main() == 0
    assert captured["fault_id"] == fault_id
    assert captured["transport"] is transport
    assert captured["parameters"] == parameters


def _production_envelope() -> dict[str, object]:
    runtime = {
        "component": "candidate",
        "release_id": "a" * 40,
        "machine_id": "machine-1",
        "boot_id": "12345678-1234-4234-9234-123456789abc",
    }
    intent = {
        "fault_id": "fault-1",
        "kind": "clob-429",
        "call_class": "clob-candidate-book-batch",
        "target_key": "group-1",
        "parameters": {},
        "nonce_digest": "1" * 64,
        "runtime": runtime,
    }
    events = []
    previous = "0" * 64
    for sequence, (state, evidence, occurred) in enumerate(
        (
            ("authorized", {"reason": "accepted"}, 10),
            (
                "armed",
                {
                    "runtime_identity_digest": acceptance.canonical_digest(runtime),
                    "ownership_digest": "2" * 64,
                },
                11,
            ),
            ("injected", {"call_id": "call-1"}, 12),
            ("detected", {"incident_id": "incident-1"}, 13),
            ("contained", {"containment_id": "contained-1"}, 14),
            ("cleaned", {"cleanup_id": "cleanup-1"}, 15),
            ("recovered", {"recovery_id": "41"}, 16),
        ),
        start=1,
    ):
        event = {
            "fault_id": "fault-1",
            "sequence": sequence,
            "state": state,
            "action": None,
            "occurred_at_ms": occurred,
            "evidence": evidence,
            "previous_hash": previous,
        }
        event["event_hash"] = acceptance.canonical_digest(event)
        previous = event["event_hash"]
        events.append(event)
    return {
        "evidence_schema_version": 2,
        "scope": "production-fault",
        "mode": "candidate",
        "app_id": "polyarb-l1",
        "release_id": "a" * 40,
        "machine_id": "machine-1",
        "boot_id": runtime["boot_id"],
        "fault_intent": intent,
        "fault_intent_digest": acceptance.canonical_digest(intent),
        "target_digest": acceptance.canonical_digest(intent["target_key"]),
        "parameter_digest": acceptance.canonical_digest(intent["parameters"]),
        "nonce_digest": intent["nonce_digest"],
        "fault_history": events,
        "fault_history_tail_hash": previous,
        "recovery_writer_receipt": {
            "table": "neg_risk_candidate_success_receipts",
            "row_id": 41,
            "component": "candidate",
            "occurred_at_ms": 16,
        },
        "open_injection_fault_count": 0,
        "pending_verification_fault_count": 1,
        "source_projection_active": True,
        "open_incident_count": 0,
        "cross_membership_quote_batches": 0,
        "partial_publication_count": 0,
        "orphan_collecting_runs": 0,
        "freshness_gate": True,
        "reconciliation_gate": True,
    }


@pytest.mark.parametrize(
    ("mutator", "reason"),
    (
        (lambda value: value.pop("fault_intent"), "missing-fault-intent"),
        (lambda value: value["fault_history"].append(deepcopy(value["fault_history"][2])),
         "duplicate-injection"),
        (lambda value: value["fault_history"][2].update(evidence={"call_id": "wrong"}),
         "event-hash-mismatch"),
        (lambda value: value["fault_history"].pop(3), "missing-detection"),
        (lambda value: value["fault_history"].pop(5), "missing-cleanup"),
        (lambda value: value["fault_history"].pop(6), "missing-recovery"),
        (lambda value: value["recovery_writer_receipt"].update(
            table="neg_risk_discovery_batches"), "recovery-family-mismatch"),
        (lambda value: value.update(open_injection_fault_count=1), "open-injection-fault"),
    ),
)
def test_candidate_evaluator_names_every_tamper(mutator, reason: str) -> None:
    evidence = _production_envelope()
    mutator(evidence)

    verdict = acceptance.evaluate_fault_envelope(evidence, mode="candidate")

    assert verdict.status == "FAIL"
    assert reason in verdict.reasons


def test_candidate_verdict_is_signed_and_final_mode_requires_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POLYARB_UPSTREAM_FAULT_EVALUATOR_SECRET", "evaluator-only")
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", raising=False)
    evidence = _production_envelope()

    artifact = acceptance.build_candidate_artifact(evidence)

    assert artifact["status"] == "PASS"
    assert artifact["mode"] == "candidate"
    assert artifact["signature"]
    final_before = acceptance.evaluate_fault_envelope(
        evidence, mode="final", candidate_artifact=artifact
    )
    assert final_before.status == "FAIL"
    assert "missing-verified" in final_before.reasons


def test_final_evaluator_has_only_readonly_evidence_and_evaluator_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("POLYARB_UPSTREAM_FAULT_EVALUATOR_SECRET", "evaluator-only")
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", raising=False)
    candidate_source = _production_envelope()
    artifact = acceptance.build_candidate_artifact(candidate_source)
    final_evidence = deepcopy(candidate_source)
    verified = {
        "fault_id": "fault-1",
        "sequence": 8,
        "state": "verified",
        "action": None,
        "occurred_at_ms": 17,
        "evidence": {
            "verdict_id": artifact["verdict_id"],
            "verdict_digest": artifact["artifact_digest"],
        },
        "previous_hash": candidate_source["fault_history_tail_hash"],
    }
    verified["event_hash"] = acceptance.canonical_digest(verified)
    final_evidence["fault_history"].append(verified)
    final_evidence["fault_history_tail_hash"] = verified["event_hash"]
    final_evidence["pending_verification_fault_count"] = 0
    final_evidence["source_projection_active"] = False

    evidence_path = tmp_path / "final-evidence.json"
    artifact_path = tmp_path / "candidate-artifact.json"
    verdict_path = tmp_path / "final-verdict.json"
    evidence_path.write_text(json.dumps(final_evidence))
    artifact_path.write_text(json.dumps(artifact))

    def reject_http_mutation(*_args, **_kwargs):
        raise AssertionError("final evaluator must not have HTTP mutation capability")

    monkeypatch.setattr("urllib.request.urlopen", reject_http_mutation)
    monkeypatch.setattr("http.client.HTTPConnection.request", reject_http_mutation)

    assert acceptance.main(
        [
            "--evidence",
            str(evidence_path),
            "--output",
            str(verdict_path),
            "--require-scope",
            "production-fault",
            "--expected-release",
            "a" * 40,
            "--fault-mode",
            "final",
            "--candidate-artifact",
            str(artifact_path),
        ]
    ) == 0
    assert json.loads(verdict_path.read_text())["status"] == "PASS"
