"""Bounded role-local service loop for one lease-fenced transactional worker."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from .postgres import StaleLeaseError
from .service_lifecycle import drain_worker_task, run_worker


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
    ) -> None:
        if not worker_name or turns_per_tick <= 0:
            raise ValueError("worker loop bounds are invalid")
        self._worker_name = worker_name
        self._worker = worker
        self._turns_per_tick = turns_per_tick
        self._running = asyncio.Lock()

    async def run_tick(self) -> dict[str, object]:
        if self._running.locked():
            return {"status": "skipped-overlapping", "turns": []}
        async with self._running:
            turns = [await self._run_turn() for _ in range(self._turns_per_tick)]
            return {"status": "ok", "turns": turns}

    async def _run_turn(self) -> dict[str, object]:
        try:
            result = await run_worker(self._worker)
        except StaleLeaseError:
            return {"worker": self._worker_name, "job_key": None, "outcome": "stale-lease"}
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
            tick_task = asyncio.create_task(
                self.run_tick(), name=f"worker-loop-tick:{self._worker_name}"
            )
            stop_task = asyncio.create_task(stop_event.wait())
            await asyncio.wait(
                (tick_task, stop_task),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if not stop_task.done():
                stop_task.cancel()
            await asyncio.gather(stop_task, return_exceptions=True)
            if stop_event.is_set() and not tick_task.done():
                closed = await drain_worker_task(
                    tick_task,
                    worker_name=self._worker_name,
                    worker=self._worker,
                )
                if not closed and on_tick is not None:
                    await on_tick(
                        {
                            "status": "ok",
                            "turns": [
                                {
                                    "worker": self._worker_name,
                                    "job_key": None,
                                    "outcome": "service-stop-grace-expired",
                                }
                            ],
                        }
                    )
                break
            try:
                outcome = tick_task.result()
            except asyncio.CancelledError:
                break
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
        if isinstance(result, Awaitable):
            await result
