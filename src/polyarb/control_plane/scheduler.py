"""Bounded, explicitly invoked turns for transactional M1 workers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Protocol


class _Worker(Protocol):
    def run_once(self) -> Any: ...


class TransactionalControlPlaneScheduler:
    """Run a small ordered set of fenced worker turns without overlapping locally."""

    def __init__(
        self,
        *,
        structure_source_admitter: _Worker,
        structure_source_worker: _Worker,
        structure_source_materializer: _Worker,
        structure_worker: _Worker,
        structure_certifier: _Worker,
        quote_worker: _Worker,
        quote_certifier: _Worker,
        max_turns: int,
    ) -> None:
        if max_turns <= 0:
            raise ValueError("max_turns must be positive")
        self._workers = (
            ("structure-source-admit", structure_source_admitter),
            ("structure-source", structure_source_worker),
            ("structure-source-materialize", structure_source_materializer),
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

    async def aclose(self) -> None:
        """Close optional long-lived worker transports after a service stops."""
        closed: set[int] = set()
        for _name, worker in self._workers:
            if id(worker) in closed:
                continue
            closed.add(id(worker))
            closer = getattr(worker, "aclose", None)
            if closer is None:
                continue
            result = closer()
            if inspect.isawaitable(result):
                await result

    async def run_until_stopped(
        self,
        *,
        stop_event: asyncio.Event,
        interval_seconds: float,
        on_tick: Callable[[dict[str, object]], Awaitable[None]] | None = None,
    ) -> dict[str, object]:
        """Run bounded ticks at a fixed cadence until a caller-owned stop signal.

        The loop owns no durable coordination; worker leases remain the only
        cross-process ownership authority. The callback makes every completed
        tick observable to the service wrapper without coupling it to logging.
        """
        if interval_seconds <= 0:
            raise ValueError("interval_seconds must be positive")
        ticks = 0
        while not stop_event.is_set():
            outcome = await self.run_tick()
            ticks += 1
            if on_tick is not None:
                await on_tick(outcome)
            if stop_event.is_set():
                break
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                continue
        return {"status": "stopped", "ticks": ticks}
