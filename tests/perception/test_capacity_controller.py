from __future__ import annotations

from pathlib import Path


def test_policy_enters_each_capacity_watermark() -> None:
    from polyarb.perception.capacity_controller import CapacityPolicy

    policy = CapacityPolicy(
        pressure_free_percent=20.0,
        critical_free_percent=12.0,
        exhaustion_free_percent=6.0,
        recovery_hold_ms=30_000,
    )

    assert policy.transition(None, free_percent=19.9, now_ms=1) == "pressure"
    assert policy.transition(None, free_percent=11.9, now_ms=1) == "critical"
    assert (
        policy.transition(None, free_percent=5.9, now_ms=1)
        == "exhaustion-imminent"
    )


def test_policy_does_not_leave_pressure_until_recovery_hold_elapsed() -> None:
    from polyarb.perception.capacity_controller import CapacityPolicy

    policy = CapacityPolicy(20.0, 12.0, 6.0, 30_000)

    assert (
        policy.transition(
            "pressure", previous_state_started_at_ms=0, free_percent=20.1, now_ms=1
        )
        == "pressure"
    )


def test_policy_requires_a_new_recovery_receipt_before_normal() -> None:
    from polyarb.perception.capacity_controller import CapacityPolicy

    policy = CapacityPolicy(20.0, 12.0, 6.0, 30_000)

    assert (
        policy.transition(
            "pressure",
            previous_state_started_at_ms=0,
            free_percent=20.1,
            now_ms=30_000,
        )
        == "pressure"
    )
    assert (
        policy.transition(
            "pressure",
            previous_state_started_at_ms=0,
            last_recovery_receipt_at_ms=30_000,
            free_percent=20.1,
            now_ms=30_000,
        )
        == "normal"
    )


def test_sqlite_runtime_persists_watermark_across_restart(tmp_path: Path) -> None:
    """A capacity episode survives daemon restart instead of silently resetting."""
    from polyarb.storage.sqlite_store import SQLiteStore

    db_path = tmp_path / "state.db"
    store = SQLiteStore(db_path)
    store.init_schema()

    recorded = store.record_capacity_controller_measurement(
        state="pressure",
        free_bytes=7_500,
        free_percent=15.0,
        observed_at_ms=1_000,
    )

    assert recorded == {
        "state": "pressure",
        "state_started_at_ms": 1_000,
        "free_bytes": 7_500,
        "free_percent": 15.0,
        "last_measurement_at_ms": 1_000,
        "last_action": "measured",
        "consecutive_failures": 0,
        "next_attempt_at_ms": 0,
        "last_error_kind": None,
        "last_recovery_receipt_at_ms": None,
    }

    restarted = SQLiteStore(db_path)
    restarted.init_schema()
    assert restarted.capacity_controller_runtime_status() == recorded
