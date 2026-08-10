from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import sqlite3
import time
import weakref
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult
from polyarb.routing.neg_risk_quote_store import NegRiskQuoteStore


@dataclass
class _ProjectionFixture:
    run_id: int
    universe_snapshot_id: int = 70
    universe_taken_at_ms: int = 1_700_000_000_000
    quoted_at_ms: int = 1_700_000_000_100
    requested_token_count: int = 12
    successful_response_count: int = 12
    universe_hash: str = "hash-7"
    source_truth_hash: str = "truth-7"
    retained_payload: list[object] = field(default_factory=list)


def test_quote_worker_module_exists() -> None:
    assert importlib.util.find_spec("polyarb.daemon.quote_worker") is not None


def test_durable_feed_loader_rejects_stale_run_before_large_projection_read(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A stuck Quote child must not make the HTTP parent rescan 40k quotes forever."""
    from polyarb.daemon import quote_worker
    from polyarb.routing.opportunity_scanner import StaleQuoteRunError

    class _QuoteStore:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def latest_complete_projection_metadata(self):
            return SimpleNamespace(quoted_at_ms=1_000)

        def latest_complete_projection(self):
            raise AssertionError("stale feed must not load the full projection")

    monkeypatch.setattr(quote_worker, "NegRiskQuoteStore", _QuoteStore)

    with pytest.raises(StaleQuoteRunError, match="quote age"):
        quote_worker.load_certified_quote_feed(
            Settings(),
            now_s=lambda: 302.0,
        )


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
        release_projection_memory=lambda: None,
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


async def test_worker_waits_for_shared_producer_slot() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    producer_lock = asyncio.Lock()
    await producer_lock.acquire()
    calls = 0

    async def collect_once() -> QuoteCollectionResult:
        nonlocal calls
        calls += 1
        return _result(1)

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        producer_lock=producer_lock,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )
    running = asyncio.create_task(worker.run(asyncio.Event()))
    await asyncio.sleep(0)
    assert calls == 0

    producer_lock.release()
    await running

    assert calls == 1


async def test_worker_keeps_shared_producer_slot_through_certification() -> None:
    """Structure writers cannot interleave with a certified Quote publication."""
    from polyarb.daemon.quote_worker import QuoteWorker

    producer_lock = asyncio.Lock()
    projection = _ProjectionFixture(run_id=1)
    contender_entered = asyncio.Event()
    contenders: list[asyncio.Task[None]] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(1)

    async def certify_projection(_result: QuoteCollectionResult) -> _ProjectionFixture:
        contenders.append(asyncio.create_task(_acquire_contender()))
        await asyncio.sleep(0)
        assert not contender_entered.is_set(), "producer slot released before certification"
        return projection

    async def _acquire_contender() -> None:
        async with producer_lock:
            contender_entered.set()

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        producer_lock=producer_lock,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())
    await contenders[0]

    assert contender_entered.is_set()


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


async def test_subprocess_failure_retries_without_cadence_sleep() -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        QuoteWorker,
    )

    attempts = 0
    delays: list[float] = []

    async def collect_once() -> QuoteCollectionResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise QuoteCollectionSubprocessError("timeout")
        return _result(9)

    async def wait_for_stop(_stop: asyncio.Event, delay_s: float) -> bool:
        delays.append(delay_s)
        return attempts >= 2

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert attempts == 2
    assert delays[0] == 0.0
    assert worker.runtime.snapshot().success_count == 1


async def test_supervised_worker_exits_after_bounded_consecutive_timeouts() -> None:
    """The outer supervisor, not an endless inner loop, owns P1 recovery."""
    from polyarb.daemon.quote_worker import QuoteCollectionSubprocessError, QuoteWorker

    attempts = 0

    async def collect_once() -> QuoteCollectionResult:
        nonlocal attempts
        attempts += 1
        raise QuoteCollectionSubprocessError("timeout", attempt_id=attempts)

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        stop_after_consecutive_timeouts=3,
    )

    await worker.run(asyncio.Event())

    snapshot = worker.runtime.snapshot()
    assert attempts == 3
    assert snapshot.consecutive_failures == 3
    assert snapshot.state == "stopped"


async def test_supervised_quote_worker_publishes_child_progress_each_cycle() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    progress: list[str] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(1)

    async def on_cycle_started() -> None:
        progress.append("progress")

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        interval_s=120,
        on_cycle_started=on_cycle_started,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert progress == ["progress"]


async def test_worker_releases_orphaned_collection_before_first_admission() -> None:
    """A restarted sole Quote worker must not wait for a dead predecessor lease."""
    from polyarb.daemon.quote_worker import QuoteWorker

    events: list[str] = []

    async def cleanup_collecting_runs() -> int:
        events.append("cleanup")
        return 1

    async def collect_once() -> QuoteCollectionResult:
        events.append("collect")
        return _result(1)

    async def stop_after_first_attempt(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        recover_orphaned_collecting_runs=cleanup_collecting_runs,
        interval_s=120,
        wait_for_stop=stop_after_first_attempt,
    )

    await worker.run(asyncio.Event())

    assert events == ["cleanup", "collect"]


async def test_timeout_incident_receives_failed_attempt_identity() -> None:
    """A timeout must identify its failed run, never the prior successful run."""
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        QuoteWorker,
        QuoteWorkerRuntime,
    )

    attempts = 0
    recorded: list[tuple[int | None, int | None, int | None]] = []

    async def collect_once() -> QuoteCollectionResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise QuoteCollectionSubprocessError(
                "timeout",
                attempt_id=353,
            )
        return _result(2156)

    async def record_timeout(error, _runtime) -> None:
        recorded.append(
            (error.attempt_id, error.run_id, error.requested_token_count)
        )

    async def wait_for_stop(_stop: asyncio.Event, _delay_s: float) -> bool:
        return attempts >= 2

    runtime = QuoteWorkerRuntime()
    runtime.mark_success(_result(2154))
    worker = QuoteWorker(
        collect_once=collect_once,
        record_timeout_incident=record_timeout,
        interval_s=120,
        runtime=runtime,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert recorded == [(353, None, None)]


async def test_certification_failure_records_pipeline_incident() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker
    from polyarb.routing.opportunity_scanner import StaleUniverseError

    recorded: list[tuple[str, int | None]] = []
    delays: list[float] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(2156)

    async def certify_projection(_result: QuoteCollectionResult):
        raise StaleUniverseError("universe age 50401.0s exceeds 50400.0s")

    async def record_pipeline_failure(error, _runtime, result) -> None:
        recorded.append((type(error).__name__, None if result is None else result.run_id))

    async def wait_for_stop(_stop: asyncio.Event, _delay_s: float) -> bool:
        delays.append(_delay_s)
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        record_pipeline_failure_incident=record_pipeline_failure,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert recorded == [("StaleUniverseError", 2156)]
    assert delays == pytest.approx([300.0], abs=0.1)


async def test_failed_quote_payload_is_reclaimed_before_immediate_retry() -> None:
    from polyarb.daemon.quote_worker import QuoteCollectionSubprocessError, QuoteWorker

    attempts = 0
    events: list[str] = []

    async def collect_once() -> QuoteCollectionResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise QuoteCollectionSubprocessError("timeout")
        return _result(9)

    async def reclaim_failed_payloads() -> int:
        events.append("reclaim")
        return 1

    async def wait_for_stop(_stop: asyncio.Event, _delay_s: float) -> bool:
        events.append("wait")
        return attempts >= 2

    worker = QuoteWorker(
        collect_once=collect_once,
        reclaim_failed_payloads=reclaim_failed_payloads,
        interval_s=120,
        wait_for_stop=wait_for_stop,
    )

    await worker.run(asyncio.Event())

    assert attempts == 2
    assert events == ["reclaim", "wait", "wait"]


async def test_worker_publishes_projection_only_after_certification() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = _ProjectionFixture(run_id=7)
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
    published = worker.runtime.certified_projection()
    assert published is not projection
    assert published is not None
    assert published.run_id == projection.run_id
    assert worker.runtime.snapshot().success_count == 1


async def test_worker_atomically_publishes_projection_and_precomputed_scan() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = _ProjectionFixture(run_id=7)
    opportunity_scan = SimpleNamespace(
        quote_run_id=7,
        source_snapshot_id=70,
        universe_hash="hash-7",
    )
    observed_during_prepare: list[object | None] = []
    completed_after_publish: list[int] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return projection

    async def prepare_opportunities(_projection):
        observed_during_prepare.append(worker.runtime.certified_feed())
        return opportunity_scan

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    async def complete_attempt(result: QuoteCollectionResult) -> None:
        assert worker.runtime.certified_feed() is not None
        completed_after_publish.append(result.run_id)

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        complete_attempt=complete_attempt,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert observed_during_prepare == [None]
    feed = worker.runtime.certified_feed()
    assert feed is not None
    assert feed.projection is not projection
    assert feed.projection.run_id == projection.run_id
    assert feed.opportunity_scan is opportunity_scan
    assert completed_after_publish == [7]


async def test_watcher_failure_keeps_certified_feed_publishable() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = _ProjectionFixture(run_id=7)
    opportunity_scan = SimpleNamespace(
        quote_run_id=7,
        source_snapshot_id=70,
        universe_hash="hash-7",
    )
    observed_before_watcher: list[object | None] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return projection

    async def prepare_opportunities(_projection):
        return opportunity_scan

    async def reconcile_global(_projection) -> None:
        observed_before_watcher.append(worker.runtime.certified_feed())
        raise OSError("telegram unavailable")

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        reconcile_global_projection=reconcile_global,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert observed_before_watcher == [worker.runtime.certified_feed()]
    feed = worker.runtime.certified_feed()
    assert feed is not None
    assert feed.projection.run_id == 7
    assert feed.opportunity_scan is opportunity_scan
    assert worker.runtime.snapshot().success_count == 1
    assert worker.runtime.snapshot().failure_count == 0


async def test_mismatched_precomputed_scan_preserves_previous_feed() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    previous_projection = _ProjectionFixture(
        run_id=6,
        universe_hash="hash-6",
        source_truth_hash="truth-6",
    )
    previous_scan = SimpleNamespace(quote_run_id=6)
    projection = _ProjectionFixture(run_id=7)
    mismatched_scan = SimpleNamespace(
        quote_run_id=7,
        source_snapshot_id=999,
        universe_hash="hash-7",
    )

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return projection

    async def prepare_opportunities(_projection):
        return mismatched_scan

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )
    worker.runtime.publish_certified_feed(
        previous_projection,
        previous_scan,
    )

    await worker.run(asyncio.Event())

    feed = worker.runtime.certified_feed()
    assert feed is not None
    assert feed.projection is not previous_projection
    assert feed.projection.run_id == previous_projection.run_id
    assert feed.opportunity_scan is previous_scan
    snapshot = worker.runtime.snapshot()
    assert snapshot.failure_count == 1
    assert snapshot.last_error_kind == "QuoteProjectionIntegrityError"


async def test_failed_certification_preserves_previous_projection() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    previous = _ProjectionFixture(run_id=6)
    wrong_run = _ProjectionFixture(run_id=999)
    failed_attempts: list[tuple[int, str]] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return wrong_run

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    async def fail_attempt(result: QuoteCollectionResult, reason: str) -> None:
        failed_attempts.append((result.run_id, reason))

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        fail_attempt=fail_attempt,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )
    worker.runtime.publish_certified_projection(previous)

    await worker.run(asyncio.Event())

    published = worker.runtime.certified_projection()
    assert published is not previous
    assert published is not None
    assert published.run_id == previous.run_id
    snapshot = worker.runtime.snapshot()
    assert snapshot.success_count == 0
    assert snapshot.failure_count == 1
    assert snapshot.last_error_kind == "QuoteProjectionIntegrityError"
    assert failed_attempts == [(7, "QuoteProjectionIntegrityError")]


async def test_worker_releases_full_projection_before_interval_wait() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection_ref: weakref.ReferenceType[_ProjectionFixture] | None = None

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        nonlocal projection_ref
        projection = _ProjectionFixture(
            run_id=7,
            retained_payload=[bytearray(4 * 1024 * 1024)],
        )
        projection_ref = weakref.ref(projection)
        return projection

    async def prepare_opportunities(_projection):
        return SimpleNamespace(
            quote_run_id=7,
            source_snapshot_id=70,
            universe_hash="hash-7",
        )

    release_calls = 0

    def release_projection_memory() -> None:
        nonlocal release_calls
        release_calls += 1
        assert projection_ref is not None
        assert projection_ref() is None

    async def stop_after_release(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
        interval_s=120,
        wait_for_stop=stop_after_release,
        release_projection_memory=release_projection_memory,
    )

    await worker.run(asyncio.Event())

    feed = worker.runtime.certified_feed()
    assert feed is not None
    assert feed.projection.run_id == 7
    assert not hasattr(feed.projection, "retained_payload")
    assert release_calls == 1


async def test_worker_purges_old_runs_after_feed_publication() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = _ProjectionFixture(run_id=7)
    events: list[str] = []

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return projection

    async def cleanup_old_runs() -> int:
        assert worker.runtime.certified_projection() is not None
        assert worker.runtime.snapshot().state == "pass"
        assert worker.runtime.snapshot().last_run_id == 7
        events.append("cleanup")
        return 20

    async def reconcile_global_projection(_projection) -> None:
        events.append("reconcile")

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        cleanup_old_runs=cleanup_old_runs,
        reconcile_global_projection=reconcile_global_projection,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert events == ["cleanup", "reconcile"]
    assert worker.runtime.snapshot().success_count == 1


async def test_quote_history_cleanup_failure_does_not_unpublish_fresh_feed() -> None:
    from polyarb.daemon.quote_worker import QuoteWorker

    projection = _ProjectionFixture(run_id=7)

    async def cleanup_old_runs() -> int:
        raise OSError("cleanup unavailable")

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=lambda: asyncio.sleep(0, result=_result(7)),
        certify_projection=lambda _result: asyncio.sleep(0, result=projection),
        cleanup_old_runs=cleanup_old_runs,
        interval_s=120,
        wait_for_stop=stop_after_once,
    )

    await worker.run(asyncio.Event())

    assert worker.runtime.certified_projection() is not None
    snapshot = worker.runtime.snapshot()
    assert snapshot.success_count == 1
    assert snapshot.cleanup_failure_count == 1
    assert snapshot.cleanup_consecutive_failures == 1
    assert snapshot.last_cleanup_error_kind == "OSError"


def test_projection_memory_release_runs_gc_then_linux_malloc_trim(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.daemon import quote_worker

    events: list[object] = []

    class FakeTrim:
        argtypes: list[object] = []
        restype: object = None

        def __call__(self, pad: int) -> int:
            events.append(("trim", pad))
            return 1

    class FakeLibc:
        malloc_trim = FakeTrim()

    monkeypatch.setattr(quote_worker.gc, "collect", lambda: events.append("gc"))
    monkeypatch.setattr(quote_worker.sys, "platform", "linux")
    monkeypatch.setattr(
        quote_worker.ctypes,
        "CDLL",
        lambda _name: events.append("cdll") or FakeLibc(),
    )

    quote_worker._release_projection_memory()

    assert events == ["gc", "cdll", ("trim", 0)]


def test_projection_memory_release_is_fail_soft(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.daemon import quote_worker

    def fail_gc() -> None:
        raise RuntimeError("injected gc failure")

    monkeypatch.setattr(quote_worker.gc, "collect", fail_gc)

    quote_worker._release_projection_memory()


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


class _FakeProcess:
    def __init__(
        self,
        *,
        stdout: bytes = b"",
        stderr: bytes = b"",
        returncode: int = 0,
        block: bool = False,
    ) -> None:
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = returncode
        self.block = block
        self.terminated = False
        self.killed = False
        self.wait_called = False
        self.terminated_at: float | None = None

    async def communicate(self):
        if self.block and not self.killed:
            await asyncio.Event().wait()
        return self.stdout, self.stderr

    def terminate(self) -> None:
        self.terminated = True
        self.terminated_at = time.monotonic()

    def kill(self) -> None:
        self.killed = True

    async def wait(self) -> int:
        self.wait_called = True
        return self.returncode


def _subprocess_payload(**overrides) -> bytes:
    payload = {
        "elapsed_ms": 25,
        "quote_taken_at_ms": 1_700_000_000_000,
        "requested_token_count": 12,
        "run_id": 7,
        "status": "complete",
        "successful_response_count": 11,
        "universe_snapshot_id": 70,
        "universe_hash": "a" * 64,
        "attempt_id": 1,
        "universe_ms": 10,
        "admission_ms": 2,
        "fetch_ms": 8,
        "transform_ms": 3,
        "persist_ms": 2,
        "structure_receipt_digest": "b" * 64,
    }
    payload.update(overrides)
    return json.dumps(payload).encode()


async def test_isolated_collection_parses_one_bounded_result(tmp_path) -> None:
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    process = _FakeProcess(stdout=_subprocess_payload())
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def spawn(*args, **kwargs):
        calls.append((args, kwargs))
        return process

    settings = Settings(db_path=tmp_path / "state.db")
    result = await collect_quotes_in_subprocess(settings, spawn=spawn)

    assert result == _result(7).__class__(
        run_id=7,
        status="complete",
        universe_snapshot_id=70,
        requested_token_count=12,
        successful_response_count=11,
        quote_taken_at_ms=1_700_000_000_000,
        elapsed_ms=25,
        universe_hash="a" * 64,
        attempt_id=1,
        universe_ms=10,
        admission_ms=2,
        fetch_ms=8,
        transform_ms=3,
        persist_ms=2,
        structure_receipt_digest="b" * 64,
    )
    args, kwargs = calls[0]
    assert args[1:4] == ("-m", "polyarb.cli_arbitrage", "collect-neg-risk-quotes")
    assert args[-4:] == ("--db-path", str(settings.db_path), "--attempt-id", "1")
    assert kwargs["stdout"] == asyncio.subprocess.PIPE
    assert kwargs["stderr"] == asyncio.subprocess.PIPE


@pytest.mark.parametrize(
    ("process", "reason"),
    (
        (_FakeProcess(returncode=2, stderr=b"private detail"), "failed"),
        (_FakeProcess(returncode=75), "failed"),
        (_FakeProcess(returncode=75, stdout=b"not-json"), "failed"),
        (
            _FakeProcess(
                returncode=75,
                stdout=json.dumps(
                    {
                        "attempt_id": 999,
                        "elapsed_ms": 100_000,
                        "outcome": "failed",
                        "reason": "fetch-timeout",
                        "stage": "fetch",
                    }
                ).encode(),
            ),
            "failed",
        ),
        (_FakeProcess(stdout=b"not-json"), "invalid-json"),
        (
            _FakeProcess(
                stdout=_subprocess_payload(universe_snapshot_id=0),
            ),
            "invalid-json",
        ),
    ),
)
async def test_isolated_collection_fails_closed_on_invalid_child_result(
    tmp_path,
    process,
    reason,
) -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(
        QuoteCollectionSubprocessError,
        match=f"quote-collection-subprocess-{reason}",
    ):
        await collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
        )
    attempt = NegRiskQuoteStore(tmp_path / "state.db").latest_collection_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "failed"


async def test_failed_child_retains_bounded_stderr_diagnostic(tmp_path) -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )

    async def spawn(*_args, **_kwargs):
        return _FakeProcess(returncode=2, stderr=b"Traceback\\nQuoteUniverseUnavailableError")

    with pytest.raises(QuoteCollectionSubprocessError) as captured:
        await collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"), spawn=spawn
        )

    assert captured.value.reason == "failed"
    assert captured.value.diagnostic == "Traceback\\nQuoteUniverseUnavailableError"


async def test_isolated_collection_hard_timeout_kills_child_and_releases_run(tmp_path) -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )

    db_path = tmp_path / "state.db"

    class RenewingHungProcess(_FakeProcess):
        def __init__(self) -> None:
            super().__init__(block=True)
            self.renewals = 0
            self.exited = False

        async def communicate(self):
            with sqlite3.connect(db_path) as con:
                con.execute(
                    "INSERT INTO neg_risk_quote_runs("
                    "universe_snapshot_id,universe_taken_at_ms,quoted_at_ms,"
                    "requested_token_count,successful_response_count,lease_expires_at_ms,status"
                    ") VALUES (999,0,0,0,0,9999999999999,'collecting')"
                )
                run_id = int(con.execute("SELECT last_insert_rowid()").fetchone()[0])
                attempt_id = int(
                    con.execute(
                        "SELECT MAX(id) FROM neg_risk_quote_attempts"
                    ).fetchone()[0]
                )
                con.execute(
                    "UPDATE neg_risk_quote_attempts SET quote_run_id=?,"
                    "quote_run_identity=?,target_count=? WHERE id=?",
                    (run_id, run_id, 40_495, attempt_id),
                )
            while not self.killed:
                with sqlite3.connect(db_path) as con:
                    con.execute(
                        "UPDATE neg_risk_quote_runs SET lease_expires_at_ms="
                        "lease_expires_at_ms+1 WHERE status='collecting'"
                    )
                self.renewals += 1
                await asyncio.sleep(0.002)
            self.exited = True
            return self.stdout, self.stderr

        async def wait(self) -> int:
            self.wait_called = True
            while not self.exited:
                await asyncio.sleep(0)
            return self.returncode

    process = RenewingHungProcess()

    async def spawn(*_args, **_kwargs):
        return process

    started = time.monotonic()
    with pytest.raises(QuoteCollectionSubprocessError, match="subprocess-timeout") as captured:
        await collect_quotes_in_subprocess(
            Settings(
                db_path=tmp_path / "state.db",
                neg_risk_quote_child_hard_limit_s=0.08,
                neg_risk_quote_fetch_timeout_s=0.05,
                neg_risk_quote_shutdown_reserve_s=0.02,
            ),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )

    assert time.monotonic() - started < 0.2
    assert process.terminated is True
    assert process.killed is True
    assert process.exited is True
    assert process.renewals > 0
    assert captured.value.attempt_id is not None
    assert captured.value.run_id is None
    assert captured.value.requested_token_count is None
    attempt = NegRiskQuoteStore(db_path).latest_collection_attempt()
    assert attempt is not None
    assert (attempt["phase"], attempt["outcome"], attempt["failure_kind"]) == (
        "failed",
        "failed",
        "child-hard-timeout",
    )
    with sqlite3.connect(db_path) as con:
        assert con.execute(
            "SELECT COUNT(*) FROM neg_risk_quote_runs WHERE status='collecting'"
        ).fetchone() == (0,)
        assert con.execute(
            "SELECT failure_reason FROM neg_risk_quote_runs"
        ).fetchone() == ("collector-hard-timeout",)


async def test_cli_fetch_timeout_envelope_drives_parent_timeout_retry(
    tmp_path,
    monkeypatch,
) -> None:
    from typer.testing import CliRunner

    from polyarb import cli_arbitrage as cli_module
    from polyarb.cli_arbitrage import app
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )
    from polyarb.routing.neg_risk_quote_collector import (
        QUOTE_FETCH_TIMEOUT_EXIT_CODE,
        QuoteFetchTimeoutError,
    )

    db_path = tmp_path / "state.db"

    async def fetch_timeout(**_kwargs):
        raise QuoteFetchTimeoutError()

    monkeypatch.setattr(cli_module, "collect_neg_risk_quotes", fetch_timeout)
    child = await asyncio.to_thread(
        CliRunner().invoke,
        app,
        [
            "collect-neg-risk-quotes",
            "--db-path",
            str(db_path),
            "--attempt-id",
            "1",
        ],
    )
    assert child.exit_code == QUOTE_FETCH_TIMEOUT_EXIT_CODE
    envelope = json.loads(child.stdout)
    assert envelope == {
        "attempt_id": 1,
        "elapsed_ms": envelope["elapsed_ms"],
        "outcome": "failed",
        "reason": "fetch-timeout",
        "stage": "fetch",
    }
    assert isinstance(envelope["elapsed_ms"], int)
    assert envelope["elapsed_ms"] >= 0

    process = _FakeProcess(
        returncode=child.exit_code,
        stdout=child.stdout.encode(),
    )

    async def spawn(*_args, **_kwargs):
        return process

    with pytest.raises(QuoteCollectionSubprocessError) as captured:
        await collect_quotes_in_subprocess(
            Settings(db_path=db_path),
            spawn=spawn,
        )

    assert captured.value.reason == "timeout"
    attempt = NegRiskQuoteStore(db_path).latest_collection_attempt()
    assert attempt is not None
    assert (attempt["outcome"], attempt["failure_kind"]) == (
        "failed",
        "child-fetch-timeout",
    )


async def test_hard_timeout_explicitly_waits_if_killed_child_pipes_never_close(
    tmp_path,
) -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )

    class WedgedPipesProcess(_FakeProcess):
        async def communicate(self):
            await asyncio.Event().wait()

        async def wait(self) -> int:
            self.wait_called = True
            await asyncio.sleep(0.04)
            return self.returncode

    process = WedgedPipesProcess(block=True)

    async def spawn(*_args, **_kwargs):
        return process

    started = time.monotonic()
    with pytest.raises(QuoteCollectionSubprocessError, match="subprocess-timeout"):
        await collect_quotes_in_subprocess(
            Settings(
                db_path=tmp_path / "state.db",
                neg_risk_quote_child_hard_limit_s=0.08,
                neg_risk_quote_fetch_timeout_s=0.05,
                neg_risk_quote_shutdown_reserve_s=0.02,
            ),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )

    assert time.monotonic() - started < 0.25
    assert process.terminated_at is not None
    assert time.monotonic() - process.terminated_at < 0.08
    assert process.killed is True
    assert process.wait_called is True
    attempt = NegRiskQuoteStore(tmp_path / "state.db").latest_collection_attempt()
    assert attempt is not None
    assert attempt["outcome"] == "failed"
    await asyncio.sleep(0.05)


async def test_hard_timeout_terminalizes_attempt_when_run_cleanup_fails(
    tmp_path,
    monkeypatch,
) -> None:
    from polyarb.daemon.quote_worker import (
        QuoteCollectionSubprocessError,
        collect_quotes_in_subprocess,
    )

    process = _FakeProcess(block=True)

    async def spawn(*_args, **_kwargs):
        return process

    def fail_cleanup(*_args, **_kwargs):
        raise sqlite3.OperationalError("cleanup unavailable")

    monkeypatch.setattr(NegRiskQuoteStore, "fail_collecting_runs", fail_cleanup)

    with pytest.raises(QuoteCollectionSubprocessError) as captured:
        await collect_quotes_in_subprocess(
            Settings(
                db_path=tmp_path / "state.db",
                neg_risk_quote_child_hard_limit_s=0.08,
                neg_risk_quote_fetch_timeout_s=0.05,
                neg_risk_quote_shutdown_reserve_s=0.02,
            ),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )

    assert captured.value.reason == "timeout"
    attempt = NegRiskQuoteStore(tmp_path / "state.db").latest_collection_attempt()
    assert attempt is not None
    assert (attempt["outcome"], attempt["failure_kind"]) == (
        "failed",
        "child-hard-timeout",
    )


async def test_isolated_collection_spawn_failure_terminalizes_attempt(tmp_path) -> None:
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    async def spawn(*_args, **_kwargs):
        raise OSError("spawn unavailable")

    with pytest.raises(OSError, match="spawn unavailable"):
        await collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
        )

    attempt = NegRiskQuoteStore(tmp_path / "state.db").latest_collection_attempt()
    assert attempt is not None
    assert (attempt["outcome"], attempt["failure_kind"]) == ("failed", "spawn-failed")


async def test_isolated_collection_cancellation_terminates_then_kills_child(
    tmp_path,
) -> None:
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    process = _FakeProcess(block=True)
    spawned = asyncio.Event()

    async def spawn(*_args, **_kwargs):
        spawned.set()
        return process

    task = asyncio.create_task(
        collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.wait_for(spawned.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated is True
    assert process.killed is True


async def test_cancellation_preserves_cancelled_error_when_reap_and_run_cleanup_fail(
    tmp_path,
    monkeypatch,
) -> None:
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    class NeverReapedProcess(_FakeProcess):
        async def communicate(self):
            await asyncio.Event().wait()

        async def wait(self) -> int:
            self.wait_called = True
            await asyncio.sleep(0.04)
            return self.returncode

    process = NeverReapedProcess(block=True)
    spawned = asyncio.Event()

    async def spawn(*_args, **_kwargs):
        spawned.set()
        return process

    def fail_run_cleanup(*_args, **_kwargs):
        raise sqlite3.OperationalError("run cleanup unavailable")

    monkeypatch.setattr(NegRiskQuoteStore, "fail_collecting_runs", fail_run_cleanup)
    task = asyncio.create_task(
        collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.wait_for(spawned.wait(), timeout=1)
    started = time.monotonic()
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert time.monotonic() - started < 0.08
    assert process.terminated is True
    assert process.killed is True
    assert process.wait_called is True
    attempt = NegRiskQuoteStore(tmp_path / "state.db").latest_collection_attempt()
    assert attempt is not None
    assert (attempt["outcome"], attempt["failure_kind"]) == (
        "failed",
        "parent-cancelled",
    )
    await asyncio.sleep(0.05)
