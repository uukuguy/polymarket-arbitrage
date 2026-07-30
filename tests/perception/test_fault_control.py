from __future__ import annotations

import asyncio
from uuid import UUID

import pytest

from polyarb.perception.fault_control import (
    FAULT_CALL_CLASS_BY_KIND,
    FaultCall,
    FaultCallClass,
    FaultController,
    FaultIntent,
    FaultKind,
    FaultRuntimeIdentity,
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


def test_invalid_controller_input_never_blocks_real_call() -> None:
    controller = FaultController(runtime=RUNTIME, monotonic=lambda: 10.0)
    calls = []

    async def real_call() -> str:
        calls.append("called")
        return "real"

    result = asyncio.run(controller.execute(object(), real_call))
    assert result == "real"
    assert calls == ["called"]


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
