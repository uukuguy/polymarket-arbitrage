from __future__ import annotations

import asyncio
import importlib.util
import sqlite3
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from polyarb.daemon import generation_cleanup_worker


def test_generation_cleanup_worker_module_exists() -> None:
    assert importlib.util.find_spec(
        "polyarb.daemon.generation_cleanup_worker"
    ) is not None


def test_generation_cleanup_worker_class_exists() -> None:
    assert hasattr(generation_cleanup_worker, "StructureGenerationCleanupWorker")


def _settings() -> SimpleNamespace:
    return SimpleNamespace(
        structure_generation_retention_floor=2,
        structure_generation_cleanup_max_rows=500,
        structure_generation_cleanup_active_interval_s=0.05,
        structure_generation_cleanup_idle_interval_s=30.0,
        structure_generation_cleanup_writer_busy_interval_s=5.0,
        structure_generation_cleanup_retry_initial_s=1.0,
        structure_generation_cleanup_retry_max_s=30.0,
        neg_risk_quote_interval_s=120.0,
    )


@pytest.mark.asyncio
async def test_quote_priority_defers_before_taking_producer_lock() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = True
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    lock = asyncio.Lock()
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=lock,
        quote_worker_runtime=runtime,
        clock_ms=lambda: 1_000,
    )

    await worker._tick()

    assert lock.locked() is False
    store.begin_structure_generation_cleanup_attempt.assert_not_called()
    store.cleanup_structure_generation_evidence.assert_not_called()
    store.defer_structure_generation_cleanup_runtime.assert_called_once_with(
        now_ms=1_000,
        next_attempt_at_ms=6_000,
        error_kind="quote-priority",
    )


@pytest.mark.asyncio
async def test_cleanup_tick_runs_one_chunk_and_schedules_active_retry() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    store.begin_structure_generation_cleanup_attempt.return_value = True
    store.cleanup_structure_generation_evidence.return_value = {
        "blocked": False,
        "blocked_reason": None,
        "generation_snapshot_id": 7,
        "phase": "markets",
        "rows_deleted": 500,
        "reclaimed_generation_ids": [],
        "retained_generation_ids": [9, 8],
    }
    store.structure_generation_status.return_value = {
        "reclaimable_generation_count_lower_bound": 1,
    }
    lock = asyncio.Lock()
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=lock,
        quote_worker_runtime=runtime,
        clock_ms=lambda: 1_000,
    )

    await worker._tick()

    assert lock.locked() is False
    store.begin_structure_generation_cleanup_attempt.assert_called_once_with(
        now_ms=1_000
    )
    store.cleanup_structure_generation_evidence.assert_called_once_with(
        retain_generations=2,
        max_rows=500,
        now_ms=1_000,
    )
    store.finish_structure_generation_cleanup_attempt.assert_called_once_with(
        state="idle",
        now_ms=1_000,
        next_attempt_at_ms=1_050,
        generation_snapshot_id=7,
        phase="markets",
        rows_deleted=500,
        error_kind=None,
        increment_failure=False,
    )


@pytest.mark.asyncio
async def test_quote_priority_is_rechecked_after_lock_acquisition() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.side_effect = (False, True)
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    lock = asyncio.Lock()
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=lock,
        quote_worker_runtime=runtime,
        clock_ms=lambda: 2_000,
    )

    await worker._tick()

    assert lock.locked() is False
    store.begin_structure_generation_cleanup_attempt.assert_not_called()
    store.defer_structure_generation_cleanup_runtime.assert_called_once_with(
        now_ms=2_000,
        next_attempt_at_ms=7_000,
        error_kind="quote-priority",
    )


@pytest.mark.asyncio
async def test_writer_busy_is_a_non_failure_backoff() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    store.begin_structure_generation_cleanup_attempt.return_value = True
    store.cleanup_structure_generation_evidence.side_effect = sqlite3.OperationalError(
        "database is locked"
    )
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=runtime,
        clock_ms=lambda: 3_000,
    )

    await worker._tick()

    store.finish_structure_generation_cleanup_attempt.assert_called_once_with(
        state="backoff",
        now_ms=3_000,
        next_attempt_at_ms=8_000,
        generation_snapshot_id=None,
        phase=None,
        rows_deleted=0,
        error_kind="writer-busy",
        increment_failure=False,
    )


@pytest.mark.asyncio
async def test_authenticated_block_is_durable_and_fail_closed() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    store.begin_structure_generation_cleanup_attempt.return_value = True
    store.cleanup_structure_generation_evidence.return_value = {
        "blocked": True,
        "blocked_reason": "comparison-receipt-digest-mismatch",
        "generation_snapshot_id": 7,
        "phase": "events",
        "rows_deleted": 0,
        "reclaimed_generation_ids": [],
        "retained_generation_ids": [9, 8],
    }
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=runtime,
        clock_ms=lambda: 3_000,
    )

    await worker._tick()

    store.finish_structure_generation_cleanup_attempt.assert_called_once_with(
        state="blocked",
        now_ms=3_000,
        next_attempt_at_ms=4_000,
        generation_snapshot_id=7,
        phase="events",
        rows_deleted=0,
        error_kind="comparison-receipt-digest-mismatch",
        increment_failure=True,
    )


@pytest.mark.asyncio
async def test_unexpected_error_uses_capped_exponential_backoff() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    store.begin_structure_generation_cleanup_attempt.return_value = True
    store.cleanup_structure_generation_evidence.side_effect = RuntimeError("boom")
    store.structure_generation_cleanup_runtime_status.return_value = {
        "consecutive_failures": 2,
    }
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        quote_worker_runtime=runtime,
        clock_ms=lambda: 3_000,
    )

    await worker._tick()

    store.finish_structure_generation_cleanup_attempt.assert_called_once_with(
        state="backoff",
        now_ms=3_000,
        next_attempt_at_ms=7_000,
        generation_snapshot_id=None,
        phase=None,
        rows_deleted=0,
        error_kind="RuntimeError",
        increment_failure=True,
    )


@pytest.mark.asyncio
async def test_cancellation_waits_for_sqlite_thread_before_releasing_lock() -> None:
    runtime = MagicMock()
    runtime.pipeline_active.return_value = False
    runtime.pipeline_due.return_value = False
    store = MagicMock()
    store.begin_structure_generation_cleanup_attempt.return_value = True
    started = threading.Event()
    release = threading.Event()

    def cleanup(**_kwargs):
        started.set()
        release.wait(timeout=5)
        return {
            "blocked": False,
            "blocked_reason": None,
            "generation_snapshot_id": None,
            "phase": None,
            "rows_deleted": 0,
            "reclaimed_generation_ids": [],
            "retained_generation_ids": [],
        }

    store.cleanup_structure_generation_evidence.side_effect = cleanup
    store.structure_generation_status.return_value = {
        "reclaimable_generation_count_lower_bound": 0,
    }
    lock = asyncio.Lock()
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=lock,
        quote_worker_runtime=runtime,
        clock_ms=lambda: 4_000,
    )
    tick = asyncio.create_task(worker._tick())
    assert await asyncio.to_thread(started.wait, 2)

    tick.cancel()
    await asyncio.sleep(0)
    assert lock.locked() is True
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await tick
    assert lock.locked() is False
    store.finish_structure_generation_cleanup_attempt.assert_called_once()


@pytest.mark.asyncio
async def test_run_recovers_orphaned_owner_before_observing_stop() -> None:
    store = MagicMock()
    worker = generation_cleanup_worker.StructureGenerationCleanupWorker(
        settings=_settings(),
        sqlite_store=store,
        producer_lock=asyncio.Lock(),
        clock_ms=lambda: 5_000,
    )
    stop_event = asyncio.Event()
    stop_event.set()

    await worker.run(stop_event)

    store.recover_structure_generation_cleanup_runtime.assert_called_once_with(
        now_ms=5_000,
        retry_delay_ms=1_000,
    )
