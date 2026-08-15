"""Bounded role-local service loop for one lease-fenced transactional worker."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, Protocol


class _Worker(Protocol):
    def run_once(self) -> Any: ...


class TransactionalWorkerLoop:
    """Run only one named worker role without creating cross-process ownership."""

    def __init__(
        self,
        *,
        worker_name: str,
        worker: _Worker,
        turns_per_tick: int,
        turn_timeout_seconds: float = 105,
    ) -> None:
        if not worker_name or turns_per_tick <= 0 or turn_timeout_seconds <= 0:
            raise ValueError("worker loop bounds are invalid")
        self._worker_name = worker_name
        self._worker = worker
        self._turns_per_tick = turns_per_tick
        self._turn_timeout_seconds = turn_timeout_seconds
        self._running = asyncio.Lock()

    async def run_tick(self) -> dict[str, object]:
        if self._running.locked():
            return {"status": "skipped-overlapping", "turns": []}
        async with self._running:
            turns = [await self._run_turn() for _ in range(self._turns_per_tick)]
            return {"status": "ok", "turns": turns}

    async def _run_turn(self) -> dict[str, object]:
        result = self._worker.run_once()
        if inspect.isawaitable(result):
            try:
                result = await asyncio.wait_for(result, timeout=self._turn_timeout_seconds)
            except TimeoutError:
                return {"worker": self._worker_name, "job_key": None, "outcome": "timed-out"}
        return {
            "worker": self._worker_name,
            "job_key": result.job_key,
            "outcome": result.outcome,
        }

    async def run_until_stopped(
        self,
        *,
        stop_event: asyncio.Event,
        interval_seconds: float,
        on_tick: Callable[[dict[str, object]], Awaitable[None]] | None = None,
    ) -> dict[str, object]:
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

    async def aclose(self) -> None:
        closer = getattr(self._worker, "aclose", None)
        if closer is None:
            return
        result = closer()
        if inspect.isawaitable(result):
            await result
