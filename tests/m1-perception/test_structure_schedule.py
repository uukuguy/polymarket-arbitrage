"""Adaptive Structure timeout/cadence policy contracts."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from polyarb.daemon.scheduler import SnapshotScheduler
from polyarb.daemon.structure_schedule import derive_structure_schedule
from polyarb.storage.sqlite_store import SQLiteStore


def _success(
    attempt_id: int,
    duration_ms: int,
    *,
    legacy: bool = False,
) -> dict[str, object]:
    return {
        "id": attempt_id,
        "started_at_ms": 1_000_000,
        "finished_at_ms": 1_000_000 + duration_ms,
        "outcome": "succeeded",
        "failure_kind": None,
        "elapsed_ms": None if legacy else duration_ms,
    }


def test_bootstraps_from_legacy_terminal_durations() -> None:
    attempts = [
        _success(index, duration_s * 1_000, legacy=True)
        for index, duration_s in enumerate(
            (101, 108, 110, 114, 120, 121, 123, 155, 234, 236),
            start=1,
        )
    ]

    decision = derive_structure_schedule(
        attempts,
        configured_timeout_s=240,
        configured_cadence_s=300,
        previous_timeout_s=240,
        previous_cadence_s=300,
        attempts_since_adjustment=3,
    )

    assert decision.success_sample_count == 10
    assert decision.success_p95_s == 236
    assert decision.timeout_s == 266
    assert decision.cadence_s == 326
    assert decision.reason == "success-p95"


def test_timeout_immediately_raises_deadline_and_preserves_non_overlap() -> None:
    attempts = [_success(index, 200_000) for index in range(1, 11)]
    attempts.append(
        {
            "id": 11,
            "started_at_ms": 2_000_000,
            "finished_at_ms": 2_332_000,
            "outcome": "failed",
            "failure_kind": "snapshot-subprocess-timeout",
            "elapsed_ms": 332_000,
        }
    )

    decision = derive_structure_schedule(
        attempts,
        configured_timeout_s=240,
        configured_cadence_s=300,
        previous_timeout_s=240,
        previous_cadence_s=300,
        attempts_since_adjustment=0,
    )

    assert decision.timeout_s == 288
    assert decision.cadence_s == 348
    assert decision.reason == "timeout-backoff"


def test_cooldown_keeps_effective_values_for_non_timeout_sample() -> None:
    attempts = [_success(index, 236_000) for index in range(1, 11)]

    decision = derive_structure_schedule(
        attempts,
        configured_timeout_s=240,
        configured_cadence_s=300,
        previous_timeout_s=250,
        previous_cadence_s=320,
        attempts_since_adjustment=2,
    )

    assert decision.timeout_s == 250
    assert decision.cadence_s == 320
    assert decision.reason == "cooldown"


def test_policy_clamps_timeout_and_cadence() -> None:
    attempts = [_success(index, 700_000) for index in range(1, 11)]

    decision = derive_structure_schedule(
        attempts,
        configured_timeout_s=240,
        configured_cadence_s=300,
        previous_timeout_s=590,
        previous_cadence_s=650,
        attempts_since_adjustment=3,
    )

    assert decision.timeout_s == 600
    assert decision.cadence_s == 790
    assert decision.reason == "success-p95"


def _seed_production_timing_history(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    try:
        for attempt_id, duration_s in enumerate(
            (101, 108, 110, 114, 120, 121, 123, 155, 234, 236),
            start=1,
        ):
            started_at_ms = attempt_id * 1_000_000
            con.execute(
                "INSERT INTO snapshot_attempts("
                "id,started_at_ms,finished_at_ms,outcome,snapshot_id"
                ") VALUES (?,?,?,?,?)",
                (
                    attempt_id,
                    started_at_ms,
                    started_at_ms + duration_s * 1_000,
                    "succeeded",
                    attempt_id,
                ),
            )
        con.execute(
            "INSERT INTO snapshot_attempts("
            "id,started_at_ms,finished_at_ms,outcome,failure_kind,last_stage,elapsed_ms"
            ") VALUES (?,?,?,?,?,?,?)",
            (
                11,
                11_000_000,
                11_332_000,
                "failed",
                "snapshot-subprocess-timeout",
                "gamma-events",
                332_000,
            ),
        )
        con.commit()
    finally:
        con.close()


def test_scheduler_bootstraps_and_persists_effective_schedule(
    daemon_settings_for_test: Any,
) -> None:
    daemon_settings_for_test.scheduler_interval_s = 300
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_production_timing_history(daemon_settings_for_test.db_path)

    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
    )

    assert scheduler.effective_timeout_s == 288
    assert scheduler.effective_cadence_s == 348
    assert store.get_latest_structure_schedule_adjustment() == {
        "source_attempt_id": 11,
        "success_sample_count": 10,
        "success_p95_s": 236,
        "previous_timeout_s": 240,
        "previous_cadence_s": 300,
        "timeout_s": 288,
        "cadence_s": 348,
        "reason": "timeout-backoff",
    }


@pytest.mark.asyncio
async def test_scheduler_caps_adaptive_timeout_at_producer_slot_budget(
    daemon_settings_for_test: Any,
) -> None:
    daemon_settings_for_test.scheduler_interval_s = 300
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_production_timing_history(daemon_settings_for_test.db_path)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
    )

    with patch(
        "polyarb.daemon.scheduler.run_snapshot_in_subprocess",
        new=AsyncMock(return_value=object()),
    ) as run_child:
        await scheduler._run_snapshot()

    assert scheduler.effective_timeout_s == 288
    run_child.assert_awaited_once_with(timeout_s=75.0)


@pytest.mark.asyncio
async def test_scheduler_waits_with_effective_cadence(
    daemon_settings_for_test: Any,
) -> None:
    daemon_settings_for_test.scheduler_interval_s = 300
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_production_timing_history(daemon_settings_for_test.db_path)
    scheduler = SnapshotScheduler(
        settings=daemon_settings_for_test,
        sqlite_store=store,
    )
    scheduler._tick = AsyncMock()  # type: ignore[method-assign]
    scheduler._wait_for_next_tick = AsyncMock(  # type: ignore[method-assign]
        side_effect=(False, True)
    )

    await scheduler.run(asyncio.Event())

    assert scheduler._wait_for_next_tick.await_args_list[1].args[1] == 348


@pytest.mark.asyncio
async def test_failed_structure_step_retries_without_normal_cadence_delay(
    daemon_settings_for_test: Any,
) -> None:
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    scheduler = SnapshotScheduler(daemon_settings_for_test, store)

    async def failed_tick() -> None:
        scheduler._failure_counter = 1

    scheduler._tick = failed_tick  # type: ignore[method-assign]
    scheduler._wait_for_next_tick = AsyncMock(  # type: ignore[method-assign]
        side_effect=(False, True)
    )

    await scheduler.run(asyncio.Event())

    assert scheduler._wait_for_next_tick.await_args_list[1].args[1] == 5


def test_scheduler_restart_does_not_repeat_timeout_backoff(
    daemon_settings_for_test: Any,
) -> None:
    daemon_settings_for_test.scheduler_interval_s = 300
    store = SQLiteStore(daemon_settings_for_test.db_path)
    store.init_schema()
    _seed_production_timing_history(daemon_settings_for_test.db_path)

    first = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)
    second = SnapshotScheduler(settings=daemon_settings_for_test, sqlite_store=store)

    assert first.effective_timeout_s == 288
    assert second.effective_timeout_s == 288
    assert store.count_structure_schedule_adjustments() == 1
