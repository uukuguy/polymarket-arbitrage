from __future__ import annotations

import asyncio
import importlib.util
import json
import os
import weakref
from dataclasses import dataclass, field
from types import SimpleNamespace

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_collector import QuoteCollectionResult


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

    async def collect_once() -> QuoteCollectionResult:
        return _result(7)

    async def certify_projection(_result: QuoteCollectionResult):
        return projection

    async def prepare_opportunities(_projection):
        observed_during_prepare.append(worker.runtime.certified_feed())
        return opportunity_scan

    async def stop_after_once(_stop: asyncio.Event, _delay_s: float) -> bool:
        return True

    worker = QuoteWorker(
        collect_once=collect_once,
        certify_projection=certify_projection,
        prepare_opportunities=prepare_opportunities,
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

    published = worker.runtime.certified_projection()
    assert published is not previous
    assert published is not None
    assert published.run_id == previous.run_id
    snapshot = worker.runtime.snapshot()
    assert snapshot.success_count == 0
    assert snapshot.failure_count == 1
    assert snapshot.last_error_kind == "QuoteProjectionIntegrityError"


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

    async def communicate(self):
        if self.block and not self.killed:
            await asyncio.Event().wait()
        return self.stdout, self.stderr

    def terminate(self) -> None:
        self.terminated = True

    def kill(self) -> None:
        self.killed = True


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
    )
    args, kwargs = calls[0]
    assert args[1:4] == ("-m", "polyarb.cli_arbitrage", "collect-neg-risk-quotes")
    assert args[-2:] == ("--db-path", str(settings.db_path))
    assert kwargs["stdout"] == asyncio.subprocess.PIPE
    assert kwargs["stderr"] == asyncio.subprocess.PIPE


@pytest.mark.parametrize(
    ("process", "reason"),
    (
        (_FakeProcess(returncode=2, stderr=b"private detail"), "failed"),
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


async def test_isolated_collection_cancellation_terminates_then_kills_child(
    tmp_path,
) -> None:
    from polyarb.daemon.quote_worker import collect_quotes_in_subprocess

    process = _FakeProcess(block=True)

    async def spawn(*_args, **_kwargs):
        return process

    task = asyncio.create_task(
        collect_quotes_in_subprocess(
            Settings(db_path=tmp_path / "state.db"),
            spawn=spawn,
            terminate_timeout_s=0.01,
        )
    )
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert process.terminated is True
    assert process.killed is True
