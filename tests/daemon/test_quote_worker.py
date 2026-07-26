from __future__ import annotations

import asyncio
import importlib.util
import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult


def test_quote_worker_module_exists() -> None:
    assert importlib.util.find_spec("polyarb.daemon.quote_worker") is not None


def _result(run_id: int) -> QuoteCollectionResult:
    return QuoteCollectionResult(
        run_id=run_id,
        status="complete",
        universe_snapshot_id=7,
        requested_token_count=12,
        successful_response_count=12,
        quote_taken_at_ms=1_700_000_000_000,
        elapsed_ms=25,
    )


async def test_worker_collects_immediately_then_waits_for_interval() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    calls: list[int] = []
    delays: list[float] = []

    async def collect_once() -> QuoteCollectionResult:
        calls.append(len(calls) + 1)
        return _result(calls[-1])

    async def wait_for_stop(_stop: asyncio.Event, delay_s: float) -> bool:
        delays.append(delay_s)
        return len(delays) == 2

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert calls == [1, 2]
    assert delays == pytest.approx([120, 120], abs=0.05)
    snapshot = worker.runtime.snapshot()
    assert snapshot.state == "stopped"
    assert snapshot.attempt_count == 2
    assert snapshot.success_count == 2
    assert snapshot.failure_count == 0
    assert snapshot.last_run_id == 2
    assert snapshot.last_requested_token_count == 12
    assert snapshot.last_successful_response_count == 12
    assert snapshot.last_elapsed_ms == 25


async def test_worker_never_overlaps_collections() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    active = 0
    maximum_active = 0
    calls = 0

    async def collect_once() -> QuoteCollectionResult:
        nonlocal active, maximum_active, calls
        calls += 1
        active += 1
        maximum_active = max(maximum_active, active)
        await asyncio.sleep(0)
        active -= 1
        return _result(calls)

    waits = 0

    async def wait_for_stop(_stop: asyncio.Event, _delay_s: float) -> bool:
        nonlocal waits
        waits += 1
        return waits == 3

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert calls == 3
    assert maximum_active == 1


@pytest.mark.parametrize(
    ("finished_at", "expected_delay"),
    ((175.0, 45.0), (250.0, 0.0)),
)
async def test_worker_interval_is_start_to_start_without_negative_wait(
    finished_at: float,
    expected_delay: float,
) -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    clock = iter((100.0, finished_at))
    delays: list[float] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(1)

    async def wait_for_stop(_stop: asyncio.Event, delay_s: float) -> bool:
        delays.append(delay_s)
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        wait_for_stop=wait_for_stop,
        monotonic=lambda: next(clock),
    )

    await worker.run(asyncio.Event())

    assert delays == [expected_delay]
    assert worker.runtime.snapshot().attempt_count == 1


async def test_worker_failure_is_recorded_and_next_attempt_can_succeed() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    calls = 0

    async def collect_once() -> QuoteCollectionResult:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("secret-bearing detail must not enter runtime state")
        return _result(9)

    waits = 0

    async def wait_for_stop(_stop: asyncio.Event, _delay_s: float) -> bool:
        nonlocal waits
        waits += 1
        return waits == 2

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    snapshot = worker.runtime.snapshot()
    assert snapshot.attempt_count == 2
    assert snapshot.success_count == 1
    assert snapshot.failure_count == 1
    assert snapshot.consecutive_failures == 0
    assert snapshot.last_error_kind is None
    assert snapshot.last_run_id == 9


async def test_worker_publishes_projection_only_after_certification() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = SimpleNamespace(run_id=7)
    published_during_certification: list[object | None] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        published_during_certification.append(worker.runtime.certified_projection())
        return projection

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert published_during_certification == [None]
    assert worker.runtime.certified_projection() is projection
    assert worker.runtime.snapshot().success_count == 1


async def test_failed_certification_preserves_previous_projection() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    previous = SimpleNamespace(run_id=6)
    wrong_run = SimpleNamespace(run_id=999)

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return wrong_run

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )
    worker.runtime.publish_certified_projection(previous)

    await worker.run(asyncio.Event())

    assert worker.runtime.certified_projection() is previous
    snapshot = worker.runtime.snapshot()
    assert snapshot.success_count == 0
    assert snapshot.failure_count == 1
    assert snapshot.last_error_kind == "QuoteProjectionIntegrityError"


async def test_worker_cancellation_propagates_without_failure_count() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    entered = asyncio.Event()

    async def collect_once() -> QuoteCollectionResult:
        entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    worker = QuoteWorker(collect_once=collect_once, interval_s=120)
    task = asyncio.create_task(worker.run(asyncio.Event()))
    await asyncio.wait_for(entered.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    snapshot = worker.runtime.snapshot()
    assert snapshot.state == "stopped"
    assert snapshot.attempt_count == 1
    assert snapshot.failure_count == 0
    assert snapshot.last_error_kind is None


def test_builder_is_disabled_by_default_and_honors_interval(tmp_path) -> None:
    from polyarb.daemon.quote_worker import build_production_quote_worker

    disabled = Settings(
        db_path=tmp_path / "disabled.db",
        neg_risk_quote_worker_enabled=False,
    )
    enabled = Settings(
        db_path=tmp_path / "enabled.db",
        neg_risk_quote_worker_enabled=True,
        neg_risk_quote_interval_s=77,
    )

    assert build_production_quote_worker(disabled) is None
    worker = build_production_quote_worker(enabled)
    assert worker is not None
    assert worker.interval_s == 77
