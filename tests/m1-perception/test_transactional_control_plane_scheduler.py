from __future__ import annotations

import asyncio

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


def test_bounded_tick_runs_only_configured_number_of_turns() -> None:
    admitter = _AsyncWorker("structure-source-admit")
    source = _AsyncWorker("structure-source")
    materializer = _AsyncWorker("structure-source-materialize")
    structure = _AsyncWorker("structure-range")
    quote = _AsyncWorker("quote-batch")
    structure_certifier = _SyncWorker("structure-certify")
    quote_certifier = _SyncWorker("quote-certify")
    scheduler = TransactionalControlPlaneScheduler(
        structure_source_admitter=admitter,
        structure_source_worker=source,
        structure_source_materializer=materializer,
        structure_worker=structure,
        quote_worker=quote,
        structure_certifier=structure_certifier,
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
