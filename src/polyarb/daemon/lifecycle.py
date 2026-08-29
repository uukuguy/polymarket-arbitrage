"""Process-level lifecycle policies shared by daemon entrypoints.

HTTP readiness is one interruptible startup operation. Fly owns the outer
termination window, and daemons must finish their cooperative drain before
that window ends so the platform retains time to reap the VM and flush logs.
Keep these values and algorithms here instead of scattering loop counts or
local ``wait_for`` literals through daemon implementations.
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from typing import Any

DAEMON_HTTP_STARTUP_BUDGET_SECONDS = 10.0
DAEMON_HTTP_STARTUP_POLL_SECONDS = 0.05
PLATFORM_TERMINATION_WINDOW_SECONDS = 40.0
DAEMON_TASK_DRAIN_BUDGET_SECONDS = 30.0

if not 0 < DAEMON_HTTP_STARTUP_POLL_SECONDS < DAEMON_HTTP_STARTUP_BUDGET_SECONDS:
    raise RuntimeError("daemon HTTP startup polling must fit inside its deadline")
if not 0 < DAEMON_TASK_DRAIN_BUDGET_SECONDS < PLATFORM_TERMINATION_WINDOW_SECONDS:
    raise RuntimeError("daemon drain budget must fit inside the platform termination window")


async def wait_for_http_server_startup(
    server: Any,
    server_task: asyncio.Task[Any],
    stop_event: asyncio.Event,
    *,
    timeout_s: float = DAEMON_HTTP_STARTUP_BUDGET_SECONDS,
) -> bool:
    """Wait for one HTTP startup commit point or a cooperative stop.

    ``False`` means a signal won before readiness and is therefore a clean
    interruption, not a startup defect. Server exit and deadline expiry remain
    typed startup failures. The monotonic deadline is checked before every
    bounded sleep, so polling cadence cannot multiply the startup budget.
    """

    if timeout_s <= 0:
        raise ValueError("HTTP startup timeout must be positive")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while True:
        if stop_event.is_set():
            return False
        if server.started is True:
            return True
        if server_task.done():
            if server_task.cancelled():
                raise RuntimeError("http-server-startup-failed:cancelled")
            error = server_task.exception()
            detail = "exited" if error is None else type(error).__name__
            raise RuntimeError(f"http-server-startup-failed:{detail}") from error
        remaining_s = deadline - loop.time()
        if remaining_s <= 0:
            raise RuntimeError("http-server-startup-failed:readiness-timeout")
        await asyncio.sleep(min(DAEMON_HTTP_STARTUP_POLL_SECONDS, remaining_s))


async def wait_for_daemon_stop_or_task_exit(
    *,
    stop_event: asyncio.Event,
    tasks: Iterable[asyncio.Task[Any]],
) -> tuple[asyncio.Task[Any], BaseException] | None:
    """Supervise all long-lived daemon tasks until a process stop wins.

    A signal is the normal result (``None``). Any clean, failed or cancelled
    task exit while no stop is pending returns the exact task and error so the
    caller can stop siblings, drain them and exit nonzero. The temporary stop
    waiter is always reaped.
    """

    owned_tasks = tuple(tasks)
    if not owned_tasks:
        raise ValueError("daemon supervision requires at least one task")
    stop_task = asyncio.create_task(stop_event.wait(), name="daemon-stop-wait")
    try:
        await asyncio.wait(
            (stop_task, *owned_tasks),
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_event.is_set():
            return None
        exited_task = next(task for task in owned_tasks if task.done())
        if exited_task.cancelled():
            error: BaseException = RuntimeError("daemon-task-exited:cancelled")
        else:
            task_error = exited_task.exception()
            error = RuntimeError("daemon-task-exited:clean") if task_error is None else task_error
        return exited_task, error
    finally:
        if not stop_task.done():
            stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
