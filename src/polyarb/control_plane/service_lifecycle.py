"""Shared bounded service-stop mechanics for transactional workers."""

from __future__ import annotations

import asyncio
import inspect
import threading
from collections.abc import Awaitable
from typing import Any, Protocol

from .runtime_deadlines import runtime_policy


class Worker(Protocol):
    def run_once(self) -> Any: ...


_WORKER_JOB_TYPES = {
    "structure-source": "structure-fetch",
    "structure-source-materialize": "structure-materialize",
    "structure-range": "structure-normalize",
    "structure-certify": "structure-certify",
    "quote-admit": "quote-admit",
    "quote-batch": "quote-batch",
    "quote-certify": "quote-certify",
    "opportunity-certify": "opportunity-certify",
}
_UNFENCED_ADMISSION_GRACE_SECONDS = 3


def terminal_grace_seconds(worker_name: str, worker: Worker) -> float:
    """Resolve stop grace from the same policy as the worker's durable lease."""
    job_type = _WORKER_JOB_TYPES.get(worker_name)
    if job_type is None:
        return float(_UNFENCED_ADMISSION_GRACE_SECONDS)
    lease_seconds = getattr(worker, "_lease_seconds", 3)
    if not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError(f"{worker_name} worker lease must be a positive integer")
    return float(runtime_policy(job_type, lease_seconds).terminal_grace_seconds)


async def run_worker(worker: Worker) -> Any:
    """Run async work on-loop and blocking work in a non-joining daemon thread.

    A normal executor is deliberately unsuitable here: ``asyncio.run`` joins
    its default executor during shutdown, so one non-cooperative sync call can
    defeat the service's terminal grace.  The daemon is only the last-resort
    isolation boundary; every production sync worker still has client bounds,
    cooperative stop checks, durable checkpoints, and lease fencing.
    """
    if inspect.iscoroutinefunction(worker.run_once):
        return await worker.run_once()

    loop = asyncio.get_running_loop()
    bridge: asyncio.Future[Any] = loop.create_future()

    def publish_result(result: Any = None, error: BaseException | None = None) -> None:
        if bridge.done():
            return
        if error is None:
            bridge.set_result(result)
        else:
            bridge.set_exception(error)

    def schedule_result(result: Any = None, error: BaseException | None = None) -> None:
        try:
            loop.call_soon_threadsafe(publish_result, result, error)
        except RuntimeError:
            # The service already returned after grace and its loop is closed.
            # The durable lease fence, not this in-memory result, is authority.
            pass

    def invoke() -> None:
        try:
            result = worker.run_once()
            if isinstance(result, Awaitable):
                result = asyncio.run(result)
        except BaseException as error:
            schedule_result(None, error)
        else:
            schedule_result(result, None)

    threading.Thread(
        target=invoke,
        name=f"transactional-worker:{type(worker).__name__}",
        daemon=True,
    ).start()
    return await bridge


async def drain_worker_task(
    task: asyncio.Task[Any],
    *,
    worker_name: str,
    worker: Worker,
) -> bool:
    """Stop one in-flight turn and return whether it closed within its grace."""
    request_stop = getattr(worker, "request_stop", None)
    if callable(request_stop):
        request_stop()
    if inspect.iscoroutinefunction(worker.run_once):
        task.cancel()

    done, _pending = await asyncio.wait(
        (task,), timeout=terminal_grace_seconds(worker_name, worker)
    )
    if done:
        await asyncio.gather(task, return_exceptions=True)
        return True

    # A sync call cannot be killed safely inside CPython.  Detach only after
    # its centrally-derived grace; its still-current lease and checkpoints are
    # the recoverable fact, and fencing prevents a late terminal commit after
    # takeover.  Cancelling the bridge lets the service event loop terminate.
    task.cancel()
    await asyncio.sleep(0)
    if not task.done():
        task.add_done_callback(_consume_task_result)
    return False


def _consume_task_result(task: asyncio.Task[Any]) -> None:
    if task.cancelled():
        return
    task.exception()


__all__ = ["drain_worker_task", "run_worker", "terminal_grace_seconds"]
