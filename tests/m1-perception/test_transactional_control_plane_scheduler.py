from __future__ import annotations

import asyncio
import threading

import pytest

from polyarb.control_plane.postgres import StaleLeaseError
from polyarb.control_plane.scheduler import TransactionalControlPlaneScheduler


class _AsyncWorker:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

    async def run_once(self):
        self.calls += 1
        return type("Result", (), {"job_key": self.name, "outcome": "succeeded"})()


class _SyncWorker:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0

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


def test_role_service_cancels_current_async_attempt_when_stop_is_requested() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    started = asyncio.Event()
    finalized = asyncio.Event()
    stop_event = asyncio.Event()

    class _InterruptibleWorker:
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
