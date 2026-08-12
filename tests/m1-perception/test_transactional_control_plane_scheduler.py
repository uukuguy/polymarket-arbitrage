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
    structure = _AsyncWorker("structure-range")
    quote = _AsyncWorker("quote-batch")
    structure_certifier = _SyncWorker("structure-certify")
    quote_certifier = _SyncWorker("quote-certify")
    scheduler = TransactionalControlPlaneScheduler(
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
            {"worker": "structure-range", "job_key": "structure-range", "outcome": "succeeded"},
            {"worker": "structure-certify", "job_key": "structure-certify", "outcome": "certified"},
        ],
    }
    assert (
        structure.calls,
        structure_certifier.calls,
        quote.calls,
        quote_certifier.calls,
    ) == (1, 1, 0, 0)
    assert asyncio.run(scheduler.run_tick())["turns"] == [
        {"worker": "quote-batch", "job_key": "quote-batch", "outcome": "succeeded"},
        {"worker": "quote-certify", "job_key": "quote-certify", "outcome": "certified"},
    ]
