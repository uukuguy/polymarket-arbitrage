"""Bounded, explicitly invoked turns for transactional M1 workers."""

from __future__ import annotations

import asyncio
from collections import defaultdict
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from .postgres import StaleLeaseError
from .runtime_deadlines import RUNTIME_JOB_ORDER
from .service_lifecycle import drain_worker_task, run_worker


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
        quote_admitter: _Worker,
        quote_worker: _Worker,
        quote_certifier: _Worker,
        opportunity_certifier: _Worker | None = None,
        max_turns: int,
        structure_materializer_turns: int = 0,
        structure_range_turns: int = 0,
        include_structure_range: bool = True,
        include_quote_batch: bool = True,
    ) -> None:
        if max_turns <= 0 or structure_materializer_turns < 0 or structure_range_turns < 0:
            raise ValueError("scheduler bounds are invalid")
        if not include_structure_range and structure_range_turns:
            raise ValueError("structure range turns are excluded from this scheduler role")
        runtime_workers: dict[str, tuple[str, _Worker] | None] = {
            "structure-fetch": ("structure-source", structure_source_worker),
            "structure-materialize": (
                "structure-source-materialize",
                structure_source_materializer,
            ),
            "structure-normalize": (
                ("structure-range", structure_worker) if include_structure_range else None
            ),
            "structure-certify": ("structure-certify", structure_certifier),
            "quote-admit": ("quote-admit", quote_admitter),
            "quote-batch": (("quote-batch", quote_worker) if include_quote_batch else None),
            "quote-certify": ("quote-certify", quote_certifier),
            "opportunity-certify": (
                None
                if opportunity_certifier is None
                else ("opportunity-certify", opportunity_certifier)
            ),
        }
        workers: list[tuple[str, _Worker]] = [("structure-source-admit", structure_source_admitter)]
        workers.extend(
            worker
            for job_type in RUNTIME_JOB_ORDER
            if (worker := runtime_workers[job_type]) is not None
        )
        self._workers = tuple(workers)
        self._max_turns = max_turns
        self._structure_materializer_worker = (
            "structure-source-materialize",
            structure_source_materializer,
        )
        self._structure_materializer_turns = structure_materializer_turns
        self._structure_range_worker = ("structure-range", structure_worker)
        self._structure_range_turns = structure_range_turns
        self._running = asyncio.Lock()
        self._next_worker = 0

    async def run_tick(self) -> dict[str, object]:
        if self._running.locked():
            return {"status": "skipped-overlapping", "turns": []}
        async with self._running:
            turns: list[dict[str, object]] = []
            turn_count = min(self._max_turns, len(self._workers))
            base_workers = tuple(
                self._workers[(self._next_worker + offset) % len(self._workers)]
                for offset in range(turn_count)
            )
            workers = (
                base_workers
                + (self._structure_materializer_worker,) * self._structure_materializer_turns
                + (self._structure_range_worker,) * self._structure_range_turns
            )
            lanes: dict[str, list[tuple[int, _Worker]]] = defaultdict(list)
            for index, (name, worker) in enumerate(workers):
                lanes[name].append((index, worker))
            completed = await asyncio.gather(
                *(self._run_lane(name, lane) for name, lane in lanes.items())
            )
            indexed_turns = [item for lane in completed for item in lane]
            turns.extend(turn for _index, turn in sorted(indexed_turns))
            self._next_worker = (self._next_worker + turn_count) % len(self._workers)
            return {"status": "ok", "turns": turns}

    async def _run_lane(
        self,
        name: str,
        lane: list[tuple[int, _Worker]],
    ) -> list[tuple[int, dict[str, object]]]:
        """Run one job-type lane serially while sibling lanes remain independent."""
        turns: list[tuple[int, dict[str, object]]] = []
        for index, worker in lane:
            try:
                result = await self._run_worker(worker)
                turn = {
                    "worker": name,
                    "job_key": result.job_key,
                    "outcome": result.outcome,
                }
            except StaleLeaseError:
                turn = {"worker": name, "job_key": None, "outcome": "stale-lease"}
            except Exception as error:
                # A failed job lane is already responsible for its durable
                # attempt transition.  Keep the coordinator alive and, most
                # importantly, never cancel unrelated in-flight lanes.
                turn = {
                    "worker": name,
                    "job_key": None,
                    "outcome": "failed",
                    "error_class": type(error).__name__,
                }
            turns.append((index, turn))
        return turns

    @staticmethod
    async def _run_worker(worker: _Worker) -> Any:
        """Keep blocking workers off-loop without imposing a second deadline."""
        return await run_worker(worker)

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
            if isinstance(result, Awaitable):
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
        active: dict[str, asyncio.Task[list[tuple[int, dict[str, object]]]]] = {}
        active_workers: dict[str, _Worker] = {}

        async def emit_completed() -> None:
            for name, task in tuple(active.items()):
                if not task.done():
                    continue
                del active[name]
                active_workers.pop(name, None)
                if task.cancelled():
                    lane_turns = [
                        (
                            0,
                            {
                                "worker": name,
                                "job_key": None,
                                "outcome": "service-stopped",
                            },
                        )
                    ]
                else:
                    lane_turns = task.result()
                if on_tick is not None:
                    await on_tick(
                        {
                            "status": "ok",
                            "turns": [turn for _index, turn in sorted(lane_turns)],
                        }
                    )

        try:
            while not stop_event.is_set():
                turn_count = min(self._max_turns, len(self._workers))
                workers = tuple(
                    self._workers[(self._next_worker + offset) % len(self._workers)]
                    for offset in range(turn_count)
                )
                workers += (
                    self._structure_materializer_worker,
                ) * self._structure_materializer_turns + (
                    self._structure_range_worker,
                ) * self._structure_range_turns
                self._next_worker = (self._next_worker + turn_count) % len(self._workers)
                lanes: dict[str, list[tuple[int, _Worker]]] = defaultdict(list)
                for index, (name, worker) in enumerate(workers):
                    lanes[name].append((index, worker))
                for name, lane in lanes.items():
                    if name not in active:
                        active[name] = asyncio.create_task(
                            self._run_lane(name, lane),
                            name=f"control-plane-lane:{name}",
                        )
                        active_workers[name] = lane[0][1]
                ticks += 1

                cycle_deadline = asyncio.get_running_loop().time() + interval_seconds
                while not stop_event.is_set():
                    remaining = cycle_deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    stop_task = asyncio.create_task(stop_event.wait())
                    try:
                        await asyncio.wait(
                            (*active.values(), stop_task),
                            timeout=remaining,
                            return_when=asyncio.FIRST_COMPLETED,
                        )
                    finally:
                        if not stop_task.done():
                            stop_task.cancel()
                            await asyncio.gather(stop_task, return_exceptions=True)
                    await emit_completed()
                    if not active and not stop_event.is_set():
                        # Observer work is part of this cadence turn. Re-read
                        # the monotonic clock after it completes instead of
                        # sleeping the pre-observer remainder a second time.
                        remaining = cycle_deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            break
                        try:
                            await asyncio.wait_for(stop_event.wait(), timeout=remaining)
                        except TimeoutError:
                            pass
                        break
        finally:
            # Stop new claims first, then drain every independent lane against
            # the terminal grace derived from that worker's durable policy.
            drained = await asyncio.gather(
                *(
                    drain_worker_task(
                        task,
                        worker_name=name,
                        worker=active_workers[name],
                    )
                    for name, task in active.items()
                )
            )
            for (name, task), closed in zip(tuple(active.items()), drained, strict=True):
                if closed:
                    continue
                del active[name]
                active_workers.pop(name, None)
                if on_tick is not None:
                    await on_tick(
                        {
                            "status": "ok",
                            "turns": [
                                {
                                    "worker": name,
                                    "job_key": None,
                                    "outcome": "service-stop-grace-expired",
                                }
                            ],
                        }
                    )
            await emit_completed()
        return {"status": "stopped", "ticks": ticks}
