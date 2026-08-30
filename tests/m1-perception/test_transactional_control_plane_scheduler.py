from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime
from time import monotonic

import pytest

from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler


def test_claim_worker_job_keeps_the_event_loop_live_during_sync_database_io() -> None:
    from polyarb.control_plane import service_lifecycle

    started = threading.Event()
    release = threading.Event()
    loop_advanced = asyncio.Event()
    expected = object()

    class _BlockingClaimStore:
        def claim_job(self, **kwargs: object) -> object:
            assert kwargs["worker_id"] == "worker-a"
            assert kwargs["job_types"] == ("structure-fetch",)
            started.set()
            if not release.wait(timeout=1):
                raise AssertionError("claim release was not signalled")
            return expected

    async def run() -> None:
        claim = asyncio.create_task(
            service_lifecycle.claim_worker_job(
                _BlockingClaimStore(),
                worker_id="worker-a",
                job_types=("structure-fetch",),
                lease_seconds=120,
                now=datetime(2030, 1, 1, tzinfo=UTC),
            )
        )
        assert await asyncio.to_thread(started.wait, 1)

        async def advance_loop() -> None:
            await asyncio.sleep(0)
            loop_advanced.set()

        ticker = asyncio.create_task(advance_loop())
        await asyncio.wait_for(loop_advanced.wait(), timeout=0.1)
        assert not claim.done()
        release.set()
        assert await claim is expected
        await ticker

    try:
        asyncio.run(run())
    finally:
        release.set()


class _AsyncWorker:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0
        self._lease_seconds = 30

    async def run_once(self):
        self.calls += 1
        return type("Result", (), {"job_key": self.name, "outcome": "succeeded"})()


class _SyncWorker:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0
        self._lease_seconds = 30

    def run_once(self):
        self.calls += 1
        return type("Result", (), {"job_key": self.name, "outcome": "certified"})()


class _DelayedWorker:
    def __init__(self, name: str) -> None:
        self.name = name

    async def run_once(self):
        await asyncio.sleep(0.01)
        return type("Result", (), {"job_key": self.name, "outcome": "succeeded"})()


class _StaleLeaseWorker:
    def run_once(self):
        raise StaleLeaseError("lease is no longer current for quote-batch:test")


def test_service_lifecycle_rejects_a_worker_without_declared_grace_policy() -> None:
    from polyarb.control_plane.service_lifecycle import terminal_grace_seconds

    with pytest.raises(ValueError, match="declared terminal grace"):
        terminal_grace_seconds("unregistered-worker", _AsyncWorker("unknown"))

    worker_without_lease = type(
        "WorkerWithoutLease",
        (),
        {"run_once": lambda self: None},
    )()
    with pytest.raises(ValueError, match="lease must be a positive integer"):
        terminal_grace_seconds("quote-batch", worker_without_lease)


def test_blocking_io_timeout_detaches_instead_of_waiting_for_executor_shutdown() -> None:
    from polyarb.control_plane.blocking_bridge import run_blocking_call_with_timeout

    started = threading.Event()
    release = threading.Event()

    def blocked() -> None:
        started.set()
        release.wait()

    async def run() -> None:
        with pytest.raises(TimeoutError, match="blocking I/O deadline"):
            await run_blocking_call_with_timeout(
                blocked,
                timeout_seconds=0.05,
                thread_name="test:timed-blocking-call",
            )

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        asyncio.run(run())
        assert started.is_set()
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_grace_expiry_prevents_a_cancellation_handler_from_starting_new_terminal_io() -> None:
    from polyarb.control_plane.blocking_bridge import run_blocking_call

    started = threading.Event()
    release = threading.Event()
    terminal_started = threading.Event()

    def blocked() -> None:
        started.set()
        release.wait()

    def terminal() -> None:
        terminal_started.set()
        release.wait()

    async def owner() -> None:
        try:
            await run_blocking_call(blocked, thread_name="test:cancelled-call")
        except asyncio.CancelledError:
            await run_blocking_call(
                terminal,
                point_of_no_return=True,
                thread_name="test:late-terminal-call",
            )
            raise

    async def run() -> None:
        task = asyncio.create_task(owner())
        while not started.is_set():
            await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(task, timeout=0.2)

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    try:
        asyncio.run(run())
        assert not terminal_started.is_set()
    finally:
        release.set()
        safety_release.cancel()


def test_bounded_tick_runs_only_configured_number_of_turns() -> None:
    admitter = _AsyncWorker("structure-source-admit")
    source = _AsyncWorker("structure-source")
    materializer = _AsyncWorker("structure-source-materialize")
    structure = _AsyncWorker("structure-range")
    quote = _AsyncWorker("quote-batch")
    structure_certifier = _SyncWorker("structure-certify")
    quote_admitter = _AsyncWorker("quote-admit")
    quote_certifier = _SyncWorker("quote-certify")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=admitter,
        structure_source_worker=source,
        structure_source_materializer=materializer,
        structure_worker=structure,
        quote_worker=quote,
        structure_certifier=structure_certifier,
        quote_admitter=quote_admitter,
        quote_certifier=quote_certifier,
        max_turns=2,
    )

    result = asyncio.run(scheduler.run_tick())

    assert result == {
        "status": "ok",
        "turns": [
            {
                "worker": "structure-source-admit",
                "job_key": "structure-source-admit",
                "outcome": "succeeded",
            },
            {"worker": "structure-source", "job_key": "structure-source", "outcome": "succeeded"},
        ],
    }
    assert (
        admitter.calls,
        source.calls,
        materializer.calls,
        structure.calls,
        structure_certifier.calls,
        quote.calls,
        quote_certifier.calls,
    ) == (1, 1, 0, 0, 0, 0, 0)
    assert asyncio.run(scheduler.run_tick())["turns"] == [
        {
            "worker": "structure-source-materialize",
            "job_key": "structure-source-materialize",
            "outcome": "succeeded",
        },
        {"worker": "structure-range", "job_key": "structure-range", "outcome": "succeeded"},
    ]
    assert asyncio.run(scheduler.run_tick())["turns"] == [
        {"worker": "structure-certify", "job_key": "structure-certify", "outcome": "certified"},
        {"worker": "quote-admit", "job_key": "quote-admit", "outcome": "succeeded"},
    ]
    assert asyncio.run(scheduler.run_tick())["turns"] == [
        {"worker": "quote-batch", "job_key": "quote-batch", "outcome": "succeeded"},
        {"worker": "quote-certify", "job_key": "quote-certify", "outcome": "certified"},
    ]


def test_coordinator_places_quote_refresh_before_quote_admission() -> None:
    idle = _AsyncWorker("idle")
    refresh = _AsyncWorker("recurring-refresh")
    quote_admit = _AsyncWorker("quote-admit")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=idle,
        quote_refresh_admitter=refresh,
        structure_source_worker=idle,
        structure_source_materializer=idle,
        structure_worker=idle,
        structure_certifier=idle,
        quote_admitter=quote_admit,
        quote_worker=idle,
        quote_certifier=idle,
        max_turns=7,
    )

    result = asyncio.run(scheduler.run_tick())

    workers = [turn["worker"] for turn in result["turns"]]
    assert workers.index("quote-refresh-admit") < workers.index("quote-admit")
    refresh_turn = next(
        turn for turn in result["turns"] if turn["worker"] == "quote-refresh-admit"
    )
    assert refresh_turn == {
        "worker": "quote-refresh-admit",
        "job_key": "recurring-refresh",
        "outcome": "succeeded",
    }


def test_range_budget_appends_only_serial_structure_range_turns() -> None:
    structure = _AsyncWorker("structure-range")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=structure,
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=8,
        structure_range_turns=3,
    )

    turns = asyncio.run(scheduler.run_tick())["turns"]

    assert [turn["worker"] for turn in turns] == [
        "structure-source-admit",
        "structure-source",
        "structure-source-materialize",
        "structure-range",
        "structure-certify",
        "quote-admit",
        "quote-batch",
        "quote-certify",
        "structure-range",
        "structure-range",
        "structure-range",
    ]
    assert structure.calls == 4


def test_materializer_budget_is_serial_and_independent_from_range_budget() -> None:
    materializer = _AsyncWorker("structure-source-materialize")
    structure = _AsyncWorker("structure-range")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=materializer,
        structure_worker=structure,
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=8,
        structure_materializer_turns=3,
        structure_range_turns=2,
    )

    turns = asyncio.run(scheduler.run_tick())["turns"]

    assert [turn["worker"] for turn in turns] == [
        "structure-source-admit",
        "structure-source",
        "structure-source-materialize",
        "structure-range",
        "structure-certify",
        "quote-admit",
        "quote-batch",
        "quote-certify",
        "structure-source-materialize",
        "structure-source-materialize",
        "structure-source-materialize",
        "structure-range",
        "structure-range",
    ]
    assert materializer.calls == 4
    assert structure.calls == 3


def test_coordinator_excludes_dedicated_range_and_quote_batch_workers() -> None:
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=8,
        include_structure_range=False,
        include_quote_batch=False,
    )

    turns = asyncio.run(scheduler.run_tick())["turns"]

    assert [turn["worker"] for turn in turns] == [
        "structure-source-admit",
        "structure-source",
        "structure-source-materialize",
        "structure-certify",
        "quote-admit",
        "quote-certify",
    ]


def test_coordinator_rejects_extra_range_turn_budget() -> None:
    with pytest.raises(ValueError, match="excluded"):
        TransactionalControlPlaneScheduler(
            structure_source_admitter=_AsyncWorker("structure-source-admit"),
            structure_source_worker=_AsyncWorker("structure-source"),
            structure_source_materializer=_AsyncWorker("structure-source-materialize"),
            structure_worker=_AsyncWorker("structure-range"),
            structure_certifier=_SyncWorker("structure-certify"),
            quote_admitter=_AsyncWorker("quote-admit"),
            quote_worker=_AsyncWorker("quote-batch"),
            quote_certifier=_SyncWorker("quote-certify"),
            max_turns=1,
            structure_range_turns=1,
            include_structure_range=False,
        )


def test_role_loop_runs_only_its_named_worker_for_its_bound() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    worker = _AsyncWorker("quote-batch")
    loop = TransactionalWorkerLoop(worker_name="quote-batch", worker=worker, turns_per_tick=2)

    assert asyncio.run(loop.run_tick()) == {
        "status": "ok",
        "turns": [
            {
                "worker": "quote-batch",
                "job_key": "quote-batch",
                "outcome": "succeeded",
            },
            {
                "worker": "quote-batch",
                "job_key": "quote-batch",
                "outcome": "succeeded",
            },
        ],
    }
    assert worker.calls == 2


def test_role_loop_does_not_own_a_competing_worker_timeout() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    loop = TransactionalWorkerLoop(
        worker_name="structure-range",
        worker=_DelayedWorker("structure-range"),
        turns_per_tick=1,
    )

    assert asyncio.run(loop.run_tick()) == {
        "status": "ok",
        "turns": [
            {
                "worker": "structure-range",
                "job_key": "structure-range",
                "outcome": "succeeded",
            },
        ],
    }


def test_role_loop_records_stale_lease_without_stopping_its_tick() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    loop = TransactionalWorkerLoop(
        worker_name="quote-batch", worker=_StaleLeaseWorker(), turns_per_tick=2
    )

    assert asyncio.run(loop.run_tick()) == {
        "status": "ok",
        "turns": [
            {"worker": "quote-batch", "job_key": None, "outcome": "stale-lease"},
            {"worker": "quote-batch", "job_key": None, "outcome": "stale-lease"},
        ],
    }


def test_role_loop_records_retryable_lane_failure_without_stopping_service() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    class _RetryableWorker:
        _lease_seconds = 30

        def __init__(self) -> None:
            self.calls = 0

        async def run_once(self):
            self.calls += 1
            if self.calls == 1:
                raise TimeoutError("provider unavailable")
            return type("Result", (), {"job_key": "quote:next", "outcome": "succeeded"})()

    worker = _RetryableWorker()
    loop = TransactionalWorkerLoop(
        worker_name="quote-batch", worker=worker, turns_per_tick=2
    )

    assert asyncio.run(loop.run_tick()) == {
        "status": "ok",
        "turns": [
            {
                "worker": "quote-batch",
                "job_key": None,
                "outcome": "failed",
                "error_class": "TimeoutError",
            },
            {
                "worker": "quote-batch",
                "job_key": "quote:next",
                "outcome": "succeeded",
            },
        ],
    }


def test_role_service_cancels_current_async_attempt_when_stop_is_requested() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    started = asyncio.Event()
    finalized = asyncio.Event()
    stop_event = asyncio.Event()

    class _InterruptibleWorker:
        _lease_seconds = 30

        async def run_once(self):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                finalized.set()
                raise

    loop = TransactionalWorkerLoop(
        worker_name="quote-batch",
        worker=_InterruptibleWorker(),
        turns_per_tick=1,
    )

    async def run() -> dict[str, object]:
        service = asyncio.create_task(
            loop.run_until_stopped(stop_event=stop_event, interval_seconds=60)
        )
        await started.wait()
        stop_event.set()
        result = await service
        assert finalized.is_set()
        return result

    assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}


def test_quote_pool_service_stop_cancels_and_drains_every_active_lane() -> None:
    from polyarb.control_plane.quote_worker import TransactionalQuoteBatchPool
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    started = [asyncio.Event() for _ in range(4)]
    interrupted = [asyncio.Event() for _ in range(4)]
    stop_event = asyncio.Event()

    class _InterruptibleLane:
        _lease_seconds = 30

        def __init__(self, ordinal: int) -> None:
            self._ordinal = ordinal

        async def run_once(self):
            started[self._ordinal].set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Production lanes write finish_interrupted here before
                # re-raising; the event is the deterministic test surrogate.
                interrupted[self._ordinal].set()
                raise

    pool = TransactionalQuoteBatchPool(
        lanes=tuple(_InterruptibleLane(ordinal) for ordinal in range(4))
    )
    loop = TransactionalWorkerLoop(
        worker_name="quote-batch", worker=pool, turns_per_tick=1
    )

    async def run() -> dict[str, object]:
        service = asyncio.create_task(
            loop.run_until_stopped(stop_event=stop_event, interval_seconds=60)
        )
        await asyncio.gather(*(event.wait() for event in started))
        stop_event.set()
        result = await service
        assert all(event.is_set() for event in interrupted)
        return result

    assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}


def test_scheduler_records_stale_lease_then_runs_later_workers() -> None:
    quote_admitter = _AsyncWorker("quote-admit")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_StaleLeaseWorker(),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=quote_admitter,
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=6,
        include_structure_range=False,
        include_quote_batch=False,
    )

    turns = asyncio.run(scheduler.run_tick())["turns"]

    assert turns[0] == {
        "worker": "structure-source-admit",
        "job_key": None,
        "outcome": "stale-lease",
    }
    assert turns[4] == {
        "worker": "quote-admit",
        "job_key": "quote-admit",
        "outcome": "succeeded",
    }
    assert quote_admitter.calls == 1


def test_scheduler_service_emits_tick_then_stops_without_sleeping() -> None:
    admitter = _AsyncWorker("structure-source-admit")
    source = _AsyncWorker("structure-source")
    structure = _AsyncWorker("structure-range")
    stop_event = asyncio.Event()
    outcomes: list[dict[str, object]] = []
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=admitter,
        structure_source_worker=source,
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=structure,
        structure_certifier=_SyncWorker("structure-certify"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=1,
    )

    async def on_tick(outcome: dict[str, object]) -> None:
        outcomes.append(outcome)
        stop_event.set()

    assert asyncio.run(
        scheduler.run_until_stopped(stop_event=stop_event, interval_seconds=60, on_tick=on_tick)
    ) == {"status": "stopped", "ticks": 1}
    assert outcomes == [
        {
            "status": "ok",
            "turns": [
                {
                    "worker": "structure-source-admit",
                    "job_key": "structure-source-admit",
                    "outcome": "succeeded",
                }
            ],
        }
    ]


def test_scheduler_recomputes_cadence_after_slow_observer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import scheduler as scheduler_module

    stop_event = asyncio.Event()
    emitted = 0
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=1,
    )
    real_wait_for = asyncio.wait_for

    async def reject_stale_cadence_wait(awaitable, *, timeout):
        awaitable.close()
        raise AssertionError(f"scheduler reused stale cadence remainder: {timeout}")

    async def on_tick(_outcome: dict[str, object]) -> None:
        nonlocal emitted
        emitted += 1
        await asyncio.sleep(0.02)
        if emitted == 2:
            stop_event.set()

    async def run() -> dict[str, object]:
        monkeypatch.setattr(scheduler_module.asyncio, "wait_for", reject_stale_cadence_wait)
        try:
            return await scheduler.run_until_stopped(
                stop_event=stop_event,
                interval_seconds=0.001,
                on_tick=on_tick,
            )
        finally:
            monkeypatch.setattr(scheduler_module.asyncio, "wait_for", real_wait_for)

    assert asyncio.run(run()) == {"status": "stopped", "ticks": 2}


def test_scheduler_does_not_own_a_competing_worker_timeout() -> None:
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_DelayedWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=1,
    )

    result = asyncio.run(scheduler.run_tick())

    assert result["turns"][0] == {
        "worker": "structure-source-admit",
        "job_key": "structure-source-admit",
        "outcome": "succeeded",
    }


def test_scheduler_runs_sync_worker_off_the_event_loop() -> None:
    released = threading.Event()

    class _BlockingSyncWorker:
        def run_once(self):
            assert released.wait(timeout=1)
            return type("Result", (), {"job_key": "structure-certify", "outcome": "certified"})()

    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_BlockingSyncWorker(),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=1,
    )
    scheduler._next_worker = 4

    async def run() -> dict[str, object]:
        asyncio.get_running_loop().call_later(0.01, released.set)
        return await scheduler.run_tick()

    result = asyncio.run(run())
    assert result["turns"] == [
        {
            "worker": "structure-certify",
            "job_key": "structure-certify",
            "outcome": "certified",
        }
    ]


def test_slow_certifier_does_not_block_sibling_job_type_lane() -> None:
    sibling_finished = threading.Event()

    class _BlockingCertifier:
        def run_once(self):
            assert sibling_finished.wait(timeout=1)
            return type("Result", (), {"job_key": "structure-certify", "outcome": "certified"})()

    class _SiblingWorker:
        async def run_once(self):
            sibling_finished.set()
            return type("Result", (), {"job_key": "quote-admit", "outcome": "idle"})()

    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_AsyncWorker("structure-source-admit"),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_BlockingCertifier(),
        quote_admitter=_SiblingWorker(),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=6,
    )

    turns = asyncio.run(scheduler.run_tick())["turns"]

    assert sibling_finished.is_set()
    assert turns[4]["outcome"] == "certified"
    assert turns[5]["worker"] == "quote-admit"


def test_scheduler_service_repeats_fast_lane_while_slow_lane_is_still_running() -> None:
    slow_started = asyncio.Event()
    release_slow = asyncio.Event()
    fast_calls = 0
    stop_event = asyncio.Event()

    class _SlowWorker:
        async def run_once(self):
            slow_started.set()
            await release_slow.wait()
            return type("Result", (), {"job_key": "slow", "outcome": "succeeded"})()

    class _FastWorker:
        async def run_once(self):
            nonlocal fast_calls
            fast_calls += 1
            if fast_calls == 2:
                stop_event.set()
                release_slow.set()
            return type("Result", (), {"job_key": "fast", "outcome": "succeeded"})()

    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_SlowWorker(),
        structure_source_worker=_FastWorker(),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=8,
    )

    async def run() -> dict[str, object]:
        result = await scheduler.run_until_stopped(
            stop_event=stop_event,
            interval_seconds=0.01,
        )
        assert slow_started.is_set()
        return result

    result = asyncio.run(run())

    assert result["status"] == "stopped"
    assert fast_calls == 2


def test_scheduler_service_isolates_lane_failure() -> None:
    stop_event = asyncio.Event()
    outcomes: list[dict[str, object]] = []

    class _FailedWorker:
        async def run_once(self):
            raise RuntimeError("lane-boom")

    class _StoppingWorker:
        async def run_once(self):
            stop_event.set()
            return type("Result", (), {"job_key": "healthy", "outcome": "succeeded"})()

    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_FailedWorker(),
        structure_source_worker=_StoppingWorker(),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=2,
    )

    async def on_tick(outcome: dict[str, object]) -> None:
        outcomes.append(outcome)

    result = asyncio.run(
        scheduler.run_until_stopped(
            stop_event=stop_event,
            interval_seconds=60,
            on_tick=on_tick,
        )
    )

    assert result == {"status": "stopped", "ticks": 1}
    turns = [turn for outcome in outcomes for turn in outcome["turns"]]
    assert {turn["worker"] for turn in turns} == {
        "structure-source-admit",
        "structure-source",
    }
    assert any(
        turn["outcome"] == "failed" and turn["error_class"] == "RuntimeError" for turn in turns
    )


def test_role_service_terminal_grace_detaches_non_cooperative_sync_turn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import service_lifecycle
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    started = threading.Event()
    release = threading.Event()
    stop_event = asyncio.Event()
    outcomes: list[dict[str, object]] = []

    class _NonCooperativeSyncWorker:
        _lease_seconds = 3

        def __init__(self) -> None:
            self.stop_requested = False

        def request_stop(self) -> None:
            self.stop_requested = True

        def run_once(self):
            started.set()
            release.wait()
            return type("Result", (), {"job_key": "held-lease", "outcome": "late"})()

    worker = _NonCooperativeSyncWorker()
    loop = TransactionalWorkerLoop(worker_name="quote-batch", worker=worker, turns_per_tick=1)
    monkeypatch.setattr(service_lifecycle, "terminal_grace_seconds", lambda *_args, **_kw: 0.05)

    async def record_outcome(outcome: dict[str, object]) -> None:
        outcomes.append(outcome)

    async def run() -> dict[str, object]:
        service = asyncio.create_task(
            loop.run_until_stopped(
                stop_event=stop_event,
                interval_seconds=60,
                on_tick=record_outcome,
            )
        )
        assert await asyncio.to_thread(started.wait, 1)
        stop_event.set()
        return await asyncio.wait_for(service, timeout=0.5)

    try:
        assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}
        assert worker.stop_requested is True
        assert outcomes == [
            {
                "status": "ok",
                "turns": [
                    {
                        "worker": "quote-batch",
                        "job_key": None,
                        "outcome": "service-stop-grace-expired",
                    }
                ],
            }
        ]
    finally:
        release.set()


def test_scheduler_terminal_grace_reports_stalled_async_terminal_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import service_lifecycle

    started = asyncio.Event()
    stop_event = asyncio.Event()
    outcomes: list[dict[str, object]] = []

    class _StalledTerminalWorker:
        _lease_seconds = 3

        async def run_once(self):
            started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                # Simulate a terminal database path that fails to respect the
                # first cancellation; the grace boundary must still win.
                await asyncio.Event().wait()

    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_StalledTerminalWorker(),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=_AsyncWorker("structure-source-materialize"),
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=1,
    )
    monkeypatch.setattr(service_lifecycle, "terminal_grace_seconds", lambda *_args, **_kw: 0.05)

    async def record_outcome(outcome: dict[str, object]) -> None:
        outcomes.append(outcome)

    async def run() -> dict[str, object]:
        service = asyncio.create_task(
            scheduler.run_until_stopped(
                stop_event=stop_event,
                interval_seconds=60,
                on_tick=record_outcome,
            )
        )
        await started.wait()
        stop_event.set()
        return await asyncio.wait_for(service, timeout=0.5)

    assert asyncio.run(run()) == {"status": "stopped", "ticks": 1}
    assert outcomes[-1]["turns"] == [
        {
            "worker": "structure-source-admit",
            "job_key": None,
            "outcome": "service-stop-grace-expired",
        }
    ]


def test_role_service_grace_is_not_defeated_by_asyncio_executor_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane import service_lifecycle, structure_source
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    started = threading.Event()
    release = threading.Event()
    stop_event = asyncio.Event()

    class _AsyncWorkerUsingBlockingBridge:
        _lease_seconds = 3

        async def run_once(self):
            started.set()
            await structure_source._to_thread(release.wait)
            return type("Result", (), {"job_key": "late", "outcome": "late"})()

    loop = TransactionalWorkerLoop(
        worker_name="structure-source",
        worker=_AsyncWorkerUsingBlockingBridge(),
        turns_per_tick=1,
    )
    monkeypatch.setattr(service_lifecycle, "terminal_grace_seconds", lambda *_a, **_k: 0.05)

    async def run() -> dict[str, object]:
        service = asyncio.create_task(
            loop.run_until_stopped(stop_event=stop_event, interval_seconds=60)
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop_event.set()
        return await service

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        assert asyncio.run(run()) == {"status": "stopped", "ticks": 0}
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()
