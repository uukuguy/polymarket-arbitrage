from __future__ import annotations

import asyncio
import hashlib
import json
from copy import deepcopy
from pathlib import Path

import pytest

import polyarb.safe_artifact as safe_artifact
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
EVALUATOR_PRIVATE_KEY = (
    "ed25519-v1:test-key:AQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQE"
)
EVALUATOR_PUBLIC_KEY = (
    "ed25519-v1:test-key:iojj3XQJ8ZX9UtstPLpdcspnCb8dlBIb83SIAbQPb1w"
)


@pytest.mark.parametrize("fault_id", UPSTREAM_FAULTS)
def test_all_and_only_typed_upstream_faults_are_executable(fault_id: str) -> None:
    assert chaos.FAULTS[fault_id].execute_supported is True


def test_exactly_eight_faults_advertise_typed_upstream_execution() -> None:
    assert {
        fault_id
        for fault_id, spec in chaos.FAULTS.items()
        if spec.execute_supported
    } == set(UPSTREAM_FAULTS)


@pytest.mark.parametrize(
    "missing",
    ("release_id", "machine_id", "boot_id", "call_class", "target_key"),
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
            evidence_dir=evidence_dir,
            transport=lambda *_: calls.append("arm"),
        )

    assert calls == []


def test_production_secret_preflight_precedes_get_and_filesystem(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", raising=False)
    calls: list[str] = []
    transport = chaos.UpstreamHttpTransport(
        base_url="https://example.test",
        expected_release="a" * 40,
        timeout_s=1,
        fetch_json=lambda *_args: (calls.append("GET"), 0.0),
    )
    evidence_dir = tmp_path / "must-not-exist"

    with pytest.raises(chaos.AdapterFailedError, match="control-authority-unavailable"):
        chaos.execute_upstream_fault(
            fault_id="gamma-timeout",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id="12345678-1234-4234-9234-123456789abc",
            call_class="gamma-discovery-event-page",
            target_key="discovery",
            parameters={"delay_ms": 10},
            evidence_dir=evidence_dir,
            transport=transport,
        )

    assert calls == []
    assert not evidence_dir.exists()


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

    def transport(operation: str, payload: object) -> dict[str, object]:
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
            assert isinstance(payload, dict)
            intent = payload["intent"]
            assert isinstance(intent, dict)
            return {
                "status": "accepted",
                "fault_id": intent["fault_id"],
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
            evidence_dir=tmp_path / type(failure).__name__,
            transport=transport,
        )

    assert calls[-1] == "cleanup"


def test_ambiguous_arm_status_failure_still_cleans_exact_durable_fault_id(
    tmp_path: Path,
) -> None:
    calls: list[tuple[str, str | None]] = []
    original = TimeoutError("arm response lost")

    def transport(operation: str, payload: object) -> dict[str, object]:
        fault_id = None
        if isinstance(payload, dict):
            raw_fault_id = payload.get("fault_id")
            if isinstance(raw_fault_id, str):
                fault_id = raw_fault_id
            intent = payload.get("intent")
            if isinstance(intent, dict) and isinstance(intent.get("fault_id"), str):
                fault_id = intent["fault_id"]
        calls.append((operation, fault_id))
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
            raise original
        if operation == "admission":
            raise OSError("status unavailable")
        if operation == "cleanup":
            return {
                "status": "cleaned",
                "memory_cleared_at_ms": 20,
                "receipt_persisted_at_ms": 21,
            }
        raise AssertionError(operation)

    evidence_dir = tmp_path / "ambiguous"
    with pytest.raises(TimeoutError) as caught:
        chaos.execute_upstream_fault(
            fault_id="gamma-timeout",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id="12345678-1234-4234-9234-123456789abc",
            call_class="gamma-discovery-event-page",
            target_key="discovery",
            parameters={"delay_ms": 10},
            evidence_dir=evidence_dir,
            transport=transport,
        )

    durable_id = json.loads((evidence_dir / "intent.json").read_text())["fault_id"]
    assert caught.value is original
    assert calls[-2:] == [("admission", durable_id), ("cleanup", durable_id)]


def test_original_and_cleanup_failures_are_preserved_in_order(tmp_path: Path) -> None:
    original = KeyboardInterrupt("observe interrupted")

    def transport(operation: str, payload: object) -> dict[str, object]:
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
            assert isinstance(payload, dict)
            intent = payload["intent"]
            assert isinstance(intent, dict)
            return {
                "status": "accepted",
                "fault_id": intent["fault_id"],
                "kind": "gamma-timeout",
                "call_class": "gamma-discovery-event-page",
                "target_key": "discovery",
                "runtime": intent["runtime"],
                "intent_digest": "d" * 64,
            }
        if operation == "observe":
            raise original
        if operation == "cleanup":
            raise OSError("cleanup transport lost")
        raise AssertionError(operation)

    with pytest.raises(BaseExceptionGroup) as caught:
        chaos.execute_upstream_fault(
            fault_id="gamma-timeout",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id="12345678-1234-4234-9234-123456789abc",
            call_class="gamma-discovery-event-page",
            target_key="discovery",
            parameters={"delay_ms": 10},
            evidence_dir=tmp_path / "grouped",
            transport=transport,
        )

    assert caught.value.exceptions[0] is original
    assert isinstance(caught.value.exceptions[1], OSError)
    assert str(caught.value.exceptions[1]) == "cleanup transport lost"


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
            assert isinstance(payload, dict)
            intent = payload["intent"]
            assert isinstance(intent, dict)
            return {
                "status": "accepted",
                "fault_id": intent["fault_id"],
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
            (
                "injected",
                {
                    "call_id": "call-1",
                    "call_binding_digest": acceptance.fault_call_binding_digest(
                        fault_id="fault-1",
                        kind="clob-429",
                        call_class="clob-candidate-book-batch",
                        target_key="group-1",
                        runtime=runtime,
                        call_id="call-1",
                    ),
                },
                12,
            ),
            ("detected", {"incident_id": "incident-1"}, 13),
            ("contained", {"containment_id": "contained-1"}, 14),
            (
                "cleaned",
                {
                    "cleanup_id": "cleanup-1",
                    "memory_cleared_at_ms": "15",
                    "receipt_persisted_at_ms": "15",
                },
                15,
            ),
            ("cleanup-confirmed", {}, 15),
            ("recovered", {"recovery_id": "41"}, 16),
        ),
        start=1,
    ):
        action = None
        if state == "cleanup-confirmed":
            action = state
            state = None
            evidence = {
                "cleaned_event_hash": previous,
                "cleanup_id": "cleanup-1",
                "memory_cleared_at_ms": 15,
                "receipt_commit_confirmed_at_ms": 15,
            }
        event = {
            "fault_id": "fault-1",
            "sequence": sequence,
            "state": state,
            "action": action,
            "occurred_at_ms": occurred,
            "evidence": evidence,
            "previous_hash": previous,
        }
        event["event_hash"] = acceptance.canonical_digest(event)
        previous = event["event_hash"]
        events.append(event)
    incident_source_history: list[dict[str, object]] = []
    incident_previous = "sha256:" + "0" * 64
    for event_id, state in enumerate(
        ("detected", "classified", "contained", "recovering", "verified"),
        start=1,
    ):
        payload = {
            "evidence_json": "{}",
            "event_id": event_id,
            "incident_id": "incident-1",
            "kind": "clob-429",
            "occurred_at_ms": 20 + event_id,
            "previous_hash": incident_previous,
            "scope": "candidate:group-1",
            "sequence": event_id,
            "state": state,
        }
        event_hash = "sha256:" + hashlib.sha256(
            json.dumps(
                payload,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
            ).encode()
        ).hexdigest()
        incident_source_history.append({**payload, "event_hash": event_hash})
        incident_previous = event_hash
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
        "detection_receipt": {
            "detection_id": "incident-1",
            "kind": "clob-429",
            "call_class": "clob-candidate-book-batch",
            "target_key": "group-1",
            "runtime": runtime,
            "source_kind": "clob-429",
            "source_history": incident_source_history,
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
        (lambda value: value["fault_history"].pop(7), "missing-recovery"),
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


def test_candidate_rejects_wrong_envelope_mode_cross_fault_history_and_release() -> None:
    wrong_mode = _production_envelope()
    wrong_mode["mode"] = "final"
    assert "evidence-mode-mismatch" in acceptance.evaluate_fault_envelope(
        wrong_mode, mode="candidate"
    ).reasons

    cross_fault = _production_envelope()
    previous = "0" * 64
    for event in cross_fault["fault_history"]:
        event["fault_id"] = "fault-other"
        event["previous_hash"] = previous
        event["event_hash"] = acceptance.canonical_digest(event)
        previous = event["event_hash"]
    cross_fault["fault_history_tail_hash"] = previous
    verdict = acceptance.evaluate_fault_envelope(cross_fault, mode="candidate")
    assert "event-fault-id-mismatch" in verdict.reasons

    release = acceptance.evaluate_fault_envelope(
        _production_envelope(),
        mode="candidate",
        expected_release="b" * 40,
    )
    assert "expected-release-mismatch" in release.reasons


def _rehash_history(evidence: dict[str, object]) -> None:
    previous = "0" * 64
    history = evidence["fault_history"]
    assert isinstance(history, list)
    for sequence, event in enumerate(history, start=1):
        assert isinstance(event, dict)
        event["sequence"] = sequence
        event["previous_hash"] = previous
        event["event_hash"] = acceptance.canonical_digest(
            {key: value for key, value in event.items() if key != "event_hash"}
        )
        previous = event["event_hash"]
    evidence["fault_history_tail_hash"] = previous


def test_production_evaluator_rejects_rehashed_weak_or_extended_schema() -> None:
    for mutation, reason in (
        (
            lambda value: value["fault_intent"].update(attacker="extra"),
            "invalid-fault-intent-fields",
        ),
        (
            lambda value: value["fault_intent"]["runtime"].update(attacker="extra"),
            "invalid-runtime-fields",
        ),
        (
            lambda value: value["fault_history"][2].update(action="attacker-action"),
            "invalid-event-state-action",
        ),
        (
            lambda value: value["fault_history"][5].update(
                evidence={"cleanup_id": "cleanup-1"}
            ),
            "invalid-state-evidence-fields",
        ),
        (
            lambda value: value["fault_history"][2].update(
                evidence={"call_id": "call-1"}
            ),
            "invalid-state-evidence-fields",
        ),
    ):
        evidence = _production_envelope()
        mutation(evidence)
        intent = evidence["fault_intent"]
        assert isinstance(intent, dict)
        evidence["fault_intent_digest"] = acceptance.canonical_digest(intent)
        _rehash_history(evidence)
        verdict = acceptance.evaluate_fault_envelope(evidence, mode="candidate")
        assert verdict.status == "FAIL"
        assert reason in verdict.reasons


def test_production_evaluator_rejects_rehashed_attacker_source_history() -> None:
    evidence = _production_envelope()
    evidence["detection_receipt"]["source_history"] = [{"attacker": "yes"}]

    verdict = acceptance.evaluate_fault_envelope(evidence, mode="candidate")

    assert verdict.status == "FAIL"
    assert "detection-source-history-invalid" in verdict.reasons


def test_production_evaluator_rejects_rehashed_arbitrary_cleanup_request() -> None:
    evidence = _production_envelope()
    history = evidence["fault_history"]
    assert isinstance(history, list)
    history.insert(
        5,
        {
            "fault_id": "fault-1",
            "sequence": 0,
            "state": None,
            "action": "cleanup-requested",
            "occurred_at_ms": 14,
            "evidence": {"attacker": "yes"},
            "previous_hash": "",
            "event_hash": "",
        },
    )
    _rehash_history(evidence)

    verdict = acceptance.evaluate_fault_envelope(evidence, mode="candidate")

    assert verdict.status == "FAIL"
    assert "invalid-action-evidence" in verdict.reasons


def test_candidate_binds_call_and_detection_to_exact_intent() -> None:
    call = _production_envelope()
    call["fault_history"][2]["evidence"]["call_binding_digest"] = "0" * 64
    call["fault_history"][2]["event_hash"] = acceptance.canonical_digest(
        call["fault_history"][2]
    )
    assert "injected-call-binding-mismatch" in acceptance.evaluate_fault_envelope(
        call, mode="candidate"
    ).reasons

    detection = _production_envelope()
    detection["detection_receipt"]["target_key"] = "group-other"
    assert "detection-source-binding-mismatch" in acceptance.evaluate_fault_envelope(
        detection, mode="candidate"
    ).reasons


def test_artifact_io_rejects_symlink_and_never_publishes_partial_final(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = tmp_path / "source.json"
    source.write_text("{}")
    linked = tmp_path / "linked.json"
    linked.symlink_to(source)
    with pytest.raises(OSError):
        safe_artifact.read_stable_bytes(linked)

    final = tmp_path / "final.json"
    monkeypatch.setattr(
        safe_artifact.os,
        "link",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("publish failed")),
    )
    with pytest.raises(OSError, match="publish failed"):
        safe_artifact.write_exclusive_bytes(final, b'{"complete":true}\n')
    assert not final.exists()


def test_artifact_write_rejects_parent_directory_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    parent = tmp_path / "trusted"
    moved = tmp_path / "trusted-moved"
    parent.mkdir()
    original_link = safe_artifact.os.link

    def replace_parent_then_link(*args, **kwargs):
        parent.rename(moved)
        parent.mkdir()
        return original_link(*args, **kwargs)

    monkeypatch.setattr(safe_artifact.os, "link", replace_parent_then_link)
    with pytest.raises(ValueError, match="unstable-artifact-parent"):
        safe_artifact.write_exclusive_bytes(parent / "verdict.json", b"{}")
    assert not (parent / "verdict.json").exists()
    assert not (moved / "verdict.json").exists()
    assert list(tmp_path.glob(".final.json.tmp-*")) == []


def test_candidate_verdict_is_signed_and_final_mode_requires_verified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY", EVALUATOR_PRIVATE_KEY
    )
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PUBLIC_KEY", EVALUATOR_PUBLIC_KEY
    )
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
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY", EVALUATOR_PRIVATE_KEY
    )
    monkeypatch.delenv("POLYARB_SCAN_SHARED_SECRET", raising=False)
    monkeypatch.delenv("POLYARB_UPSTREAM_FAULT_CONTROL_SECRET", raising=False)
    candidate_source = _production_envelope()
    artifact = acceptance.build_candidate_artifact(candidate_source)
    monkeypatch.delenv("POLYARB_UPSTREAM_FAULT_EVALUATOR_PRIVATE_KEY")
    monkeypatch.setenv(
        "POLYARB_UPSTREAM_FAULT_EVALUATOR_PUBLIC_KEY", EVALUATOR_PUBLIC_KEY
    )
    final_evidence = deepcopy(candidate_source)
    verified = {
        "fault_id": "fault-1",
        "sequence": 9,
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
    final_evidence["mode"] = "final"
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
