"""Pure policy and historical replay contracts for runtime evidence."""

from __future__ import annotations

import ast
from datetime import UTC, datetime
from pathlib import Path

import pytest


def test_runtime_boundary_inventory_covers_every_clock_authority() -> None:
    inventory = Path("docs/dev/m1-runtime-boundary-inventory.md").read_text()

    for authority in (
        "Provider request",
        "Worker I/O",
        "Database connect/statement/lock",
        "Durable retry",
        "Circuit probe",
        "Recovery action count",
        "Scheduler cadence",
        "Terminal shutdown",
        "Operator observation",
        "Qualification window",
    ):
        assert authority in inventory
    assert "Wrapping `run_once()`" in inventory
    assert "fault matrix v3 contains 16 cases" in inventory


def test_scheduler_has_no_competing_outer_worker_timeout() -> None:
    source = Path("src/polyarb/control_plane/scheduler.py").read_text()

    assert "wait_for(self._run_worker" not in source
    assert "wait_for(worker.run_once" not in source
    assert "remaining = cycle_deadline - asyncio.get_running_loop().time()" in source


def test_async_transactional_workers_never_claim_on_the_event_loop_thread() -> None:
    root = Path("src/polyarb/control_plane")
    worker_modules = (
        "structure_source.py",
        "structure_worker.py",
        "quote_admission.py",
        "quote_worker.py",
    )
    offenders: list[str] = []
    for module_name in worker_modules:
        tree = ast.parse((root / module_name).read_text())
        for node in ast.walk(tree):
            if not isinstance(node, ast.AsyncFunctionDef) or node.name != "run_once":
                continue
            for descendant in ast.walk(node):
                if (
                    isinstance(descendant, ast.Call)
                    and isinstance(descendant.func, ast.Attribute)
                    and descendant.func.attr == "claim_job"
                ):
                    offenders.append(f"{module_name}:{descendant.lineno}")

    assert offenders == []


def soak_record(
    observed_at: str,
    *,
    expired: int = 0,
    circuits: int = 0,
    machine_states: dict[str, str] | None = None,
    api_status: str = "available",
    successful: int = 10,
) -> dict[str, object]:
    return {
        "observed_at": observed_at,
        "control_api_status": api_status,
        "machine_states": machine_states or {"worker-a": "started"},
        "expired_leases": expired,
        "open_circuit_count": circuits,
        "successful_job_count": successful,
    }


def test_replay_rejects_the_first_expired_lease_without_waiting_for_final_verify() -> None:
    from polyarb.control_plane.runtime_replay import replay_soak_observations

    records = (
        soak_record("2026-08-23T13:41:00Z", expired=0),
        soak_record("2026-08-23T16:22:21Z", expired=1),
        soak_record("2026-08-23T16:27:21Z", expired=0),
    )
    result = replay_soak_observations(records)
    assert result.first_breaking_at == datetime(2026, 8, 23, 16, 22, 21, tzinfo=UTC)
    assert result.reason_codes == ("lease.expired",)


def test_policy_returns_frozen_healthy_result_for_the_baseline() -> None:
    from polyarb.control_plane.runtime_policy import evaluate_soak_observation

    result = evaluate_soak_observation(soak_record("2026-08-23T13:41:00Z"))
    assert result.observed_at == datetime(2026, 8, 23, 13, 41, tzinfo=UTC)
    assert result.severity == "healthy"
    assert result.breaking is False
    assert result.reason_codes == ()
    with pytest.raises(AttributeError):
        setattr(result, "breaking", True)


@pytest.mark.parametrize(
    ("record", "reason"),
    [
        (soak_record("2026-08-23T13:42:00Z", circuits=1), "circuit.open"),
        (
            soak_record("2026-08-23T13:42:00Z", machine_states={"worker-a": "stopped"}),
            "machine.unhealthy",
        ),
        (soak_record("2026-08-23T13:42:00Z", api_status="unavailable"), "api.unavailable"),
    ],
)
def test_policy_classifies_absolute_breaking_facts_against_baseline(record, reason: str) -> None:
    from polyarb.control_plane.runtime_policy import evaluate_soak_observation

    baseline = soak_record("2026-08-23T13:41:00Z")
    result = evaluate_soak_observation(record, baseline=baseline)
    assert result.breaking is True
    assert result.severity == "breaking"
    assert result.reason_codes == (reason,)


def test_policy_classifies_missing_machine_and_success_count_regression() -> None:
    from polyarb.control_plane.runtime_policy import evaluate_soak_observation

    baseline = soak_record("2026-08-23T13:41:00Z", successful=10)
    current = soak_record(
        "2026-08-23T13:42:00Z",
        machine_states={},
        successful=9,
    )
    # An explicit empty machine mapping is malformed rather than a missing
    # sample, so test a valid mapping with a changed identity set.
    current["machine_states"] = {"worker-b": "started"}
    result = evaluate_soak_observation(current, baseline=baseline)
    assert result.reason_codes == ("machine.missing", "progress.regressed")


def test_replay_reports_all_unique_reasons_and_maximum_normalized_gap() -> None:
    from polyarb.control_plane.runtime_replay import replay_soak_observations

    result = replay_soak_observations(
        (
            soak_record("2026-08-23T08:10:00+08:00"),
            soak_record("2026-08-23T00:20:00Z", circuits=1),
            soak_record("2026-08-23T00:30:00Z", api_status="unavailable"),
        ),
        max_gap_seconds=900,
    )
    assert result.sample_count == 3
    assert result.max_gap_seconds == 600
    assert result.reason_codes == ("circuit.open", "api.unavailable")
    assert result.first_breaking_at == datetime(2026, 8, 23, 0, 20, tzinfo=UTC)


@pytest.mark.parametrize(
    "record",
    [
        soak_record("2026-08-23T13:41:00"),
        soak_record("2026-08-23T13:41:00Z", expired=-1),
        soak_record("2026-08-23T13:41:00Z", successful=-1),
        soak_record("2026-08-23T13:41:00Z", circuits=True),
    ],
)
def test_policy_rejects_corrupt_evidence(record) -> None:
    from polyarb.control_plane.runtime_policy import evaluate_soak_observation
    from polyarb.control_plane.soak_evidence import SoakEvidenceError

    with pytest.raises((SoakEvidenceError, ValueError), match="invalid|timezone|non-negative"):
        evaluate_soak_observation(record)


@pytest.mark.parametrize("field", ["expired_leases", "open_circuit_count", "successful_job_count"])
@pytest.mark.parametrize(
    "value",
    [1.0, 1.5, float("nan"), float("inf"), float("-inf"), True, -1],
)
def test_policy_rejects_every_non_exact_integer_counter(field: str, value: object) -> None:
    from polyarb.control_plane.runtime_policy import evaluate_soak_observation
    from polyarb.control_plane.soak_evidence import SoakEvidenceError

    record = soak_record("2026-08-23T13:41:00Z")
    record[field] = value
    with pytest.raises(SoakEvidenceError, match="invalid|non-negative"):
        evaluate_soak_observation(record)


def test_replay_rejects_an_evidence_gap_instead_of_silently_passing() -> None:
    from polyarb.control_plane.runtime_replay import replay_soak_observations

    result = replay_soak_observations(
        (
            soak_record("2026-08-23T00:00:00Z"),
            soak_record("2026-08-23T00:20:00Z"),
        ),
        max_gap_seconds=900,
    )
    assert result.first_breaking_at == datetime(2026, 8, 23, 0, 20, tzinfo=UTC)
    assert result.reason_codes == ("evidence.gap",)
