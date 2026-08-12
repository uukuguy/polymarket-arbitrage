"""Bounded, explicitly invoked turns for transactional M1 workers."""

from __future__ import annotations

import asyncio
import inspect
from typing import Any, Protocol


class _Worker(Protocol):
    def run_once(self) -> Any: ...


class TransactionalControlPlaneScheduler:
    """Run a small ordered set of fenced worker turns without overlapping locally."""

    def __init__(
        self,
        *,
        structure_worker: _Worker,
        structure_certifier: _Worker,
        quote_worker: _Worker,
        quote_certifier: _Worker,
        max_turns: int,
    ) -> None:
        if max_turns <= 0:
            raise ValueError("max_turns must be positive")
        self._workers = (
            ("structure-range", structure_worker),
            ("structure-certify", structure_certifier),
            ("quote-batch", quote_worker),
            ("quote-certify", quote_certifier),
        )
        self._max_turns = max_turns
        self._running = asyncio.Lock()
        self._next_worker = 0

    async def run_tick(self) -> dict[str, object]:
        if self._running.locked():
            return {"status": "skipped-overlapping", "turns": []}
        async with self._running:
            turns: list[dict[str, object]] = []
            turn_count = min(self._max_turns, len(self._workers))
            workers = tuple(
                self._workers[(self._next_worker + offset) % len(self._workers)]
                for offset in range(turn_count)
            )
            for name, worker in workers:
                result = worker.run_once()
                if inspect.isawaitable(result):
                    result = await result
                turns.append(
                    {
                        "worker": name,
                        "job_key": result.job_key,
                        "outcome": result.outcome,
                    }
                )
            self._next_worker = (self._next_worker + turn_count) % len(self._workers)
            return {"status": "ok", "turns": turns}
