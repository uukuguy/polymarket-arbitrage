from __future__ import annotations

import asyncio

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


class _HangingWorker:
    async def run_once(self):
        await asyncio.Event().wait()


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
    loop = TransactionalWorkerLoop(
        worker_name="quote-batch", worker=worker, turns_per_tick=2
    )

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


def test_role_loop_timeout_records_each_bound_turn() -> None:
    from polyarb.control_plane.worker_loop import TransactionalWorkerLoop

    loop = TransactionalWorkerLoop(
        worker_name="structure-range",
        worker=_HangingWorker(),
        turns_per_tick=2,
        turn_timeout_seconds=0.001,
    )

    assert asyncio.run(loop.run_tick()) == {
        "status": "ok",
        "turns": [
            {"worker": "structure-range", "job_key": None, "outcome": "timed-out"},
            {"worker": "structure-range", "job_key": None, "outcome": "timed-out"},
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


def test_timed_out_turn_does_not_freeze_later_workers() -> None:
    healthy = _AsyncWorker("structure-source-materialize")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=_HangingWorker(),
        structure_source_worker=_AsyncWorker("structure-source"),
        structure_source_materializer=healthy,
        structure_worker=_AsyncWorker("structure-range"),
        structure_certifier=_SyncWorker("structure-certify"),
        quote_admitter=_AsyncWorker("quote-admit"),
        quote_worker=_AsyncWorker("quote-batch"),
        quote_certifier=_SyncWorker("quote-certify"),
        max_turns=3,
        turn_timeout_seconds=0.001,
    )

    result = asyncio.run(scheduler.run_tick())

    assert result["turns"][0] == {
        "worker": "structure-source-admit",
        "job_key": None,
        "outcome": "timed-out",
    }
    assert healthy.calls == 1
