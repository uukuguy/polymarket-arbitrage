from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest


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


def test_quote_priority_defers_capacity_reclaim_without_mutating_history(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.perception.capacity_controller import CapacityController, CapacityPolicy
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    monkeypatch.setattr(
        "polyarb.perception.capacity_controller.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=15, total=100),
    )
    monkeypatch.setattr(
        store,
        "purge_old_snapshots",
        lambda **_kwargs: pytest.fail("Quote priority must skip reclamation"),
    )

    runtime = CapacityController(
        store=store,
        policy=CapacityPolicy(20.0, 12.0, 6.0, 30_000),
        clock_ms=lambda: 1_000,
        retry_delay_ms=5_000,
    ).run_once(quote_priority=True)

    assert runtime["state"] == "pressure"
    assert runtime["last_action"] == "quote-priority"
    assert runtime["next_attempt_at_ms"] == 6_000


def test_pressure_reclaims_one_bounded_history_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.perception.capacity_controller import CapacityController, CapacityPolicy
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    monkeypatch.setattr(
        "polyarb.perception.capacity_controller.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=15, total=100),
    )
    calls: list[dict[str, object]] = []
    quote_calls: list[dict[str, object]] = []

    def reclaim(**kwargs: object) -> tuple[int, list[int]]:
        calls.append(kwargs)
        return (2, [41, 42])

    monkeypatch.setattr(store, "purge_old_snapshots", reclaim)
    monkeypatch.setattr(
        "polyarb.perception.capacity_controller.NegRiskQuoteStore.purge_old_runs",
        lambda _self, **kwargs: quote_calls.append(kwargs) or 1,
    )

    runtime = CapacityController(
        store=store,
        policy=CapacityPolicy(20.0, 12.0, 6.0, 30_000),
        clock_ms=lambda: 1_000,
        retry_delay_ms=5_000,
    ).run_once(quote_priority=False)

    assert calls == [
        {
            "older_than_days": 7,
            "keep_last": 5,
            "max_snapshots_per_run": 10,
        }
    ]
    assert quote_calls == [{"keep_last_per_status": 10, "max_runs": 1}]
    assert runtime["last_action"] == "reclaimed-quote-history"
    assert runtime["last_recovery_receipt_at_ms"] == 1_000


def test_reclaim_failure_becomes_persisted_retry(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.perception.capacity_controller import CapacityController, CapacityPolicy
    from polyarb.storage.sqlite_store import SQLiteStore

    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    monkeypatch.setattr(
        "polyarb.perception.capacity_controller.shutil.disk_usage",
        lambda _path: SimpleNamespace(free=15, total=100),
    )

    def fail_reclaim(**_kwargs: object) -> tuple[int, list[int]]:
        raise sqlite3.OperationalError("database is locked")

    monkeypatch.setattr(store, "purge_old_snapshots", fail_reclaim)
    runtime = CapacityController(
        store=store,
        policy=CapacityPolicy(20.0, 12.0, 6.0, 30_000),
        clock_ms=lambda: 1_000,
        retry_delay_ms=5_000,
    ).run_once(quote_priority=False)

    assert runtime["last_action"] == "reclaim-failed"
    assert runtime["consecutive_failures"] == 1
    assert runtime["next_attempt_at_ms"] == 6_000
    assert runtime["last_error_kind"] == "writer-busy"
