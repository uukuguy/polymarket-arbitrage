from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from polyarb.perception.fault_control import (
    FAULT_CALL_CLASS_BY_KIND,
    FaultCall,
    FaultCallClass,
    FaultController,
    FaultEventState,
    FaultIntent,
    FaultKind,
    FaultRecoveryReceipt,
    FaultRecoveryWriter,
    FaultRuntimeIdentity,
    normalize_evidence,
    normalize_parameters,
    normalize_target,
)

RUNTIME = FaultRuntimeIdentity(
    component="candidate",
    release_id="a" * 40,
    machine_id="machine-1",
    boot_id=UUID("12345678-1234-4678-9234-567812345678"),
)


def intent(**changes: object) -> FaultIntent:
    values = {
        "fault_id": "fault-1",
        "kind": FaultKind.CLOB_LATENCY,
        "call_class": FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH,
        "target_key": "group-1",
        "parameters": {"delay_ms": 10},
        "ttl_ms": 1_000,
        "runtime": RUNTIME,
        "nonce_digest": "b" * 64,
        "accepted_at_ms": 900,
    }
    values.update(changes)
    return FaultIntent(**values)


def test_every_fault_kind_has_exactly_one_call_class() -> None:
    assert set(FAULT_CALL_CLASS_BY_KIND) == set(FaultKind)
    assert all(isinstance(value, FaultCallClass) for value in FAULT_CALL_CLASS_BY_KIND.values())


@pytest.mark.parametrize(
    ("call_class", "target"),
    [
        (FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, ""),
        (FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "x" * 129),
        (FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "https://example.test/group"),
        (FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "token=secret"),
        (FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, "group-1"),
        (FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE, "discovery"),
        (FaultCallClass.TELEGRAM_OPPORTUNITY_CARD, "notification-1"),
    ],
)
def test_target_normalization_rejects_unbounded_or_cross_class_values(
    call_class: FaultCallClass, target: str
) -> None:
    with pytest.raises(ValueError):
        normalize_target(call_class, target)


def test_target_normalization_accepts_only_locked_shapes() -> None:
    assert normalize_target(FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, " discovery ") == "discovery"
    assert (
        normalize_target(FaultCallClass.GAMMA_RECONCILIATION_EVENT_PAGE, "reconciliation")
        == "reconciliation"
    )
    assert normalize_target(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1") == "group-1"
    assert normalize_target(FaultCallClass.TELEGRAM_OPPORTUNITY_CARD, "42") == "42"


@pytest.mark.parametrize(
    ("kind", "parameters"),
    [
        (FaultKind.CLOB_429, {"extra": 1}),
        (FaultKind.CLOB_LATENCY, {"delay_ms": True}),
        (FaultKind.CLOB_LATENCY, {"delay_ms": float("inf")}),
        (FaultKind.CLOB_LATENCY, {"delay_ms": 0}),
        (FaultKind.CLOB_LATENCY, {"delay_ms": 30_001}),
        (FaultKind.GAMMA_PARTIAL, {"keep_events": 100}),
        (FaultKind.CLOB_MISSING_LEG, {"leg_index": -1}),
    ],
)
def test_parameter_normalization_rejects_invalid_values(
    kind: FaultKind, parameters: dict[str, object]
) -> None:
    with pytest.raises(ValueError):
        normalize_parameters(kind, parameters)


@pytest.mark.parametrize("ttl_ms", [999, 120_001, True])
def test_intent_rejects_ttl_outside_locked_bounds(ttl_ms: object) -> None:
    with pytest.raises(ValueError):
        intent(ttl_ms=ttl_ms)


@pytest.mark.parametrize(
    "runtime_values",
    [
        ("candidate", "not-a-release", "machine-1", RUNTIME.boot_id),
        ("candidate", "a" * 40, "https://machine", RUNTIME.boot_id),
        ("candidate", "a" * 40, "machine-1", UUID(int=0)),
    ],
)
def test_runtime_validates_release_machine_and_boot_identity(
    runtime_values: tuple[str, str, str, UUID],
) -> None:
    with pytest.raises(ValueError):
        FaultRuntimeIdentity(*runtime_values)


def test_intent_validates_nonce_digest() -> None:
    with pytest.raises(ValueError):
        intent(nonce_digest="plain-nonce")


def test_intent_rejects_call_class_owned_by_another_component() -> None:
    with pytest.raises(ValueError, match="component-call-class-mismatch"):
        intent(
            kind=FaultKind.GAMMA_TIMEOUT,
            call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            target_key="discovery",
            parameters={"delay_ms": 10},
        )


def test_controller_admits_only_one_active_fault_and_requires_exact_scope() -> None:
    now = [10.0]
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: now[0])
    controller.admit(intent(), claimed_at_ms=1_000)
    with pytest.raises(RuntimeError, match="fault-already-active"):
        controller.admit(intent(fault_id="fault-2"), claimed_at_ms=1_001)

    assert not controller.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-2")
    ).inject
    assert not controller.consume(
        FaultCall(FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE, "discovery")
    ).inject
    decision = controller.consume(FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1"))
    assert decision.inject and decision.fault_id == "fault-1"
    assert not controller.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject


def test_expired_fault_passes_through() -> None:
    now = [10.0]
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: now[0])
    controller.admit(intent(), claimed_at_ms=1_000)
    now[0] = 11.001
    assert not controller.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject


def test_admit_uses_only_ttl_remaining_since_acceptance() -> None:
    now = [10.0]
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: now[0])
    controller.admit(intent(accepted_at_ms=900, ttl_ms=1_000), claimed_at_ms=1_800)
    now[0] = 10.099
    assert controller.consume(FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")).inject

    expired = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    with pytest.raises(ValueError, match="intent-expired"):
        expired.admit(
            intent(accepted_at_ms=900, ttl_ms=1_000),
            claimed_at_ms=1_900,
        )


def test_fault_parameters_are_immutable_private_copies() -> None:
    supplied = {"delay_ms": 10}
    value = intent(parameters=supplied)
    supplied["delay_ms"] = 20
    assert value.parameters["delay_ms"] == 10
    with pytest.raises(TypeError):
        value.parameters["delay_ms"] = 30  # type: ignore[index]

    controller = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    controller.admit(value, claimed_at_ms=1_000)
    decision = controller.consume(FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1"))
    with pytest.raises(TypeError):
        decision.parameters["delay_ms"] = 30  # type: ignore[index]


def test_invalid_controller_input_never_blocks_real_call() -> None:
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    calls = []

    async def real_call() -> str:
        calls.append("called")
        return "real"

    result = asyncio.run(controller.execute(object(), real_call))
    assert result == "real"
    assert calls == ["called"]


def test_control_infrastructure_exception_passes_through_once() -> None:
    clock_calls = [0]

    def clock() -> float:
        clock_calls[0] += 1
        if clock_calls[0] > 1:
            raise OSError("clock unavailable")
        return 10.0

    controller = FaultController(runtime=RUNTIME, monotonic=clock)
    controller.admit(intent(), claimed_at_ms=1_000)
    real_calls = []

    async def real_call() -> str:
        real_calls.append("called")
        return "real"

    result = asyncio.run(
        controller.execute(
            FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1"),
            real_call,
        )
    )
    assert result == "real"
    assert real_calls == ["called"]


def test_control_base_exception_is_not_swallowed() -> None:
    clock_calls = [0]

    def clock() -> float:
        clock_calls[0] += 1
        if clock_calls[0] > 1:
            raise KeyboardInterrupt
        return 10.0

    controller = FaultController(runtime=RUNTIME, monotonic=clock)
    controller.admit(intent(), claimed_at_ms=1_000)
    real_calls = []

    async def real_call() -> str:
        real_calls.append("called")
        return "real"

    with pytest.raises(KeyboardInterrupt):
        asyncio.run(
            controller.execute(
                FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1"),
                real_call,
            )
        )
    assert real_calls == []


@pytest.mark.parametrize(
    ("state", "evidence"),
    [
        (FaultEventState.AUTHORIZED, {"reason": "accepted"}),
        (
            FaultEventState.ARMED,
            {"runtime_identity_digest": "a" * 64, "ownership_digest": "b" * 64},
        ),
        (FaultEventState.INJECTED, {"call_id": "call-1"}),
        (FaultEventState.DETECTED, {"incident_id": "incident-1"}),
        (FaultEventState.CONTAINED, {"containment_id": "containment-1"}),
        (FaultEventState.CLEANED, {"cleanup_id": "cleanup-1"}),
        (FaultEventState.RECOVERED, {"recovery_id": "recovery-1"}),
        (
            FaultEventState.VERIFIED,
            {"verdict_id": "verdict-1", "verdict_digest": "d" * 64},
        ),
        (FaultEventState.REJECTED, {"reason": "nonce-replay"}),
        (FaultEventState.EXPIRED, {"reason": "intent-expired"}),
        (FaultEventState.ABANDONED, {"reason": "runtime-replaced"}),
        (FaultEventState.CLEANUP_FAILED, {"reason": "cleanup-failed"}),
        (FaultEventState.RECOVERY_TIMEOUT, {"reason": "recovery-timeout"}),
        (FaultEventState.EVIDENCE_INVALID, {"reason": "evidence-invalid"}),
        (FaultEventState.ESCALATED, {"reason": "escalated"}),
    ],
)
def test_every_task1_state_accepts_only_its_legitimate_evidence(
    state: FaultEventState,
    evidence: dict[str, str],
) -> None:
    assert dict(normalize_evidence(state, evidence)) == evidence


@pytest.mark.parametrize(
    "value",
    [
        "123456:ABCDEF",
        "https://example.test/id",
        "id?secret=value",
        "header-value",
        "cookie-value",
        "authorization-value",
        "token-value",
        "response-body",
        "client-secret",
    ],
)
def test_evidence_identifier_rejects_sensitive_shapes(value: str) -> None:
    with pytest.raises(ValueError, match="invalid-evidence"):
        normalize_evidence(
            FaultEventState.DETECTED,
            {"incident_id": value},
        )


def test_detected_evidence_requires_exactly_one_incident_or_coverage_id() -> None:
    coverage = {"coverage_id": "coverage-" + "a" * 64}
    assert dict(normalize_evidence(FaultEventState.DETECTED, coverage)) == coverage
    with pytest.raises(ValueError, match="invalid-evidence"):
        normalize_evidence(FaultEventState.DETECTED, {})
    with pytest.raises(ValueError, match="invalid-evidence"):
        normalize_evidence(
            FaultEventState.DETECTED,
            {
                "coverage_id": "coverage-" + "a" * 64,
                "incident_id": "incident-1",
            },
        )
    with pytest.raises(ValueError, match="invalid-evidence"):
        normalize_evidence(
            FaultEventState.DETECTED,
            {"coverage_id": "coverage-not-a-digest"},
        )


def test_recovery_receipt_is_typed_and_redacted() -> None:
    receipt = FaultRecoveryReceipt(
        fault_id="fault-1",
        kind=FaultKind.GAMMA_TIMEOUT,
        call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
        component="discovery",
        runtime=FaultRuntimeIdentity(
            component="discovery",
            release_id="a" * 40,
            machine_id="machine-1",
            boot_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ),
        writer=FaultRecoveryWriter.DISCOVERY_BATCH,
        writer_id=7,
        writer_occurred_at_ms=1_200,
    )

    assert receipt.writer is FaultRecoveryWriter.DISCOVERY_BATCH
    assert receipt.writer_id == 7
    assert "url" not in repr(receipt).lower()
    with pytest.raises(ValueError, match="invalid-recovery-receipt"):
        FaultRecoveryReceipt(
            fault_id="fault-1",
            kind=FaultKind.GAMMA_TIMEOUT,
            call_class=FaultCallClass.GAMMA_DISCOVERY_EVENT_PAGE,
            component="discovery",
            runtime=receipt.runtime,
            writer=FaultRecoveryWriter.DISCOVERY_BATCH,
            writer_id="7",
            writer_occurred_at_ms=1_200,
        )


def test_reason_evidence_is_enumerated_not_free_form() -> None:
    with pytest.raises(ValueError, match="invalid-evidence"):
        normalize_evidence(
            FaultEventState.REJECTED,
            {"reason": "123456:ABCDEF"},
        )


def test_clear_removes_memory_before_receipt_write_and_freezes_on_failure() -> None:
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    controller.admit(intent(), claimed_at_ms=1_000)

    def failing_writer(fault_id: str) -> None:
        assert fault_id == "fault-1"
        assert controller.active is None
        raise OSError("evidence unavailable")

    with pytest.raises(OSError):
        controller.clear("fault-1", receipt_writer=failing_writer)
    assert controller.active is None
    assert controller.frozen
    assert not controller.consume(
        FaultCall(FaultCallClass.CLOB_CANDIDATE_BOOK_BATCH, "group-1")
    ).inject
    with pytest.raises(RuntimeError, match="fault-admission-frozen"):
        controller.admit(intent(fault_id="fault-2"), claimed_at_ms=2_000)


def test_fresh_controller_starts_empty_and_does_not_rearm_persisted_state() -> None:
    first = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    first.admit(intent(), claimed_at_ms=1_000)
    fresh = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    assert fresh.active is None
