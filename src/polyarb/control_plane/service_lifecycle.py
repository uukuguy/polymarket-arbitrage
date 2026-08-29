"""Shared bounded service-stop mechanics for transactional workers."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Sequence
from datetime import datetime
from typing import Any, Protocol

from .blocking_bridge import run_blocking_call
from .models import JobLease
from .runtime_deadlines import runtime_policy


class Worker(Protocol):
    def run_once(self) -> Any: ...


class ClaimStore(Protocol):
    def claim_job(
        self,
        *,
        worker_id: str,
        job_types: Sequence[str],
        lease_seconds: int,
        now: datetime,
    ) -> JobLease | None: ...


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


def terminal_grace_seconds(worker_name: str, worker: Worker) -> float:
    """Resolve stop grace from the same policy as the worker's durable lease."""
    job_type = _WORKER_JOB_TYPES.get(worker_name)
    if job_type is None:
        declared_grace = getattr(worker, "_terminal_grace_seconds", None)
        if not isinstance(declared_grace, int | float) or declared_grace <= 0:
            raise ValueError(f"{worker_name} worker has no declared terminal grace policy")
        return float(declared_grace)
    lease_seconds = getattr(worker, "_lease_seconds", None)
    if not isinstance(lease_seconds, int) or lease_seconds <= 0:
        raise ValueError(f"{worker_name} worker lease must be a positive integer")
    return float(runtime_policy(job_type, lease_seconds).terminal_grace_seconds)


async def claim_worker_job(
    store: ClaimStore,
    *,
    worker_id: str,
    job_types: Sequence[str],
    lease_seconds: int,
    now: datetime,
) -> JobLease | None:
    """Claim through the shared bridge without inventing another DB deadline."""
    return await run_blocking_call(
        store.claim_job,
        worker_id=worker_id,
        job_types=job_types,
        lease_seconds=lease_seconds,
        now=now,
        thread_name=f"transactional-claim:{','.join(job_types)}",
    )


async def _await_worker_result(result: Awaitable[Any]) -> Any:
    return await result


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

    def invoke() -> Any:
        return_value = worker.run_once()
        if isinstance(return_value, Awaitable):
            return asyncio.run(_await_worker_result(return_value))
        return return_value

    return await run_blocking_call(
        invoke,
        thread_name=f"transactional-worker:{type(worker).__name__}",
    )


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
    # First cancellation requests the worker's safe terminal path. Blocking
    # bridges and async workers both receive exactly the same grace window.
    task.cancel()

    done, _pending = await asyncio.wait(
        (task,), timeout=terminal_grace_seconds(worker_name, worker)
    )
    if done:
        await asyncio.gather(task, return_exceptions=True)
        return True

    # The second cancellation is the one authoritative grace-expiry signal.
    # Blocking calls detach their daemon bridge; async cleanup may no longer
    # begin another terminal I/O call. Durable lease fencing owns late work.
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    return False


__all__ = [
    "claim_worker_job",
    "drain_worker_task",
    "run_worker",
    "terminal_grace_seconds",
]
