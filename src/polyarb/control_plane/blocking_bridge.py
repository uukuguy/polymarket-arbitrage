"""Cancellation-aware bridge for blocking control-plane calls."""

from __future__ import annotations

import asyncio
import threading
from collections.abc import Callable
from typing import Any


def _daemon_call_bridge[Result](
    call: Callable[..., Result],
    *args: Any,
    thread_name: str,
    **kwargs: Any,
) -> asyncio.Future[Result]:
    """Start one blocking call without enrolling it in loop shutdown."""
    loop = asyncio.get_running_loop()
    bridge: asyncio.Future[Result] = loop.create_future()

    def publish_result(
        result: Result | None = None,
        error: BaseException | None = None,
    ) -> None:
        if bridge.done():
            return
        if error is None:
            bridge.set_result(result)  # type: ignore[arg-type]
        else:
            bridge.set_exception(error)

    def schedule_result(
        result: Result | None = None,
        error: BaseException | None = None,
    ) -> None:
        try:
            loop.call_soon_threadsafe(publish_result, result, error)
        except RuntimeError:
            # The service detached after its centrally-derived grace and the
            # loop is already closed. Durable fencing owns any late result.
            pass

    def invoke() -> None:
        try:
            result = call(*args, **kwargs)
        except BaseException as error:
            schedule_result(None, error)
        else:
            schedule_result(result, None)

    threading.Thread(target=invoke, name=thread_name, daemon=True).start()
    return bridge


def _consume_future_result(future: asyncio.Future[Any]) -> None:
    if future.cancelled():
        return
    future.exception()


async def run_blocking_call[Result](
    call: Callable[..., Result],
    *args: Any,
    point_of_no_return: bool = False,
    thread_name: str = "control-plane:blocking-call",
    **kwargs: Any,
) -> Result:
    """Run blocking I/O under the service's single cancellation authority.

    The first cancellation drains the call inside the worker's central
    terminal grace. Nonterminal work then propagates cancellation; a terminal
    point-of-no-return returns its durable result. The second cancellation,
    issued when grace expires, always detaches. A daemon bridge is required
    because ``asyncio.run`` otherwise joins its default executor and silently
    defeats the grace decision.
    """
    owner = asyncio.current_task()
    if owner is not None and owner.cancelling() > 1:
        raise asyncio.CancelledError
    bridge = _daemon_call_bridge(call, *args, thread_name=thread_name, **kwargs)
    try:
        return await asyncio.shield(bridge)
    except asyncio.CancelledError as cancellation:
        if owner is not None and owner.cancelling() > 1:
            # The worker entered this call while handling the first stop
            # request. This cancellation is therefore grace expiry.
            bridge.add_done_callback(_consume_future_result)
            raise
        try:
            result = await asyncio.shield(bridge)
        except asyncio.CancelledError:
            bridge.add_done_callback(_consume_future_result)
            raise
        except BaseException as error:
            if point_of_no_return:
                raise error from cancellation
            raise cancellation from error
        if point_of_no_return:
            if owner is not None and owner.cancelling():
                owner.uncancel()
            return result
        raise cancellation


async def run_blocking_call_with_timeout[Result](
    call: Callable[..., Result],
    *args: Any,
    timeout_seconds: float,
    thread_name: str = "control-plane:timed-blocking-call",
    **kwargs: Any,
) -> Result:
    """Detach nonterminal blocking I/O at its authoritative I/O deadline."""
    if timeout_seconds <= 0:
        raise ValueError("blocking I/O timeout must be positive")
    bridge = _daemon_call_bridge(call, *args, thread_name=thread_name, **kwargs)
    try:
        async with asyncio.timeout(timeout_seconds):
            return await asyncio.shield(bridge)
    except TimeoutError as error:
        bridge.add_done_callback(_consume_future_result)
        raise TimeoutError("blocking I/O deadline exceeded") from error
    except asyncio.CancelledError:
        bridge.add_done_callback(_consume_future_result)
        raise


async def run_blocking_call_until_stopped[Result](
    call: Callable[..., Result],
    *args: Any,
    stop_event: asyncio.Event,
    grace_seconds: float,
    point_of_no_return: bool = False,
    request_stop: Callable[[], None] | None = None,
    thread_name: str = "control-plane:stop-aware-call",
    **kwargs: Any,
) -> tuple[bool, Result | None]:
    """Race one blocking call against service stop under one grace authority."""
    if grace_seconds < 0:
        raise ValueError("blocking call grace cannot be negative")
    if stop_event.is_set():
        return False, None
    call_task = asyncio.create_task(
        run_blocking_call(
            call,
            *args,
            point_of_no_return=point_of_no_return,
            thread_name=thread_name,
            **kwargs,
        )
    )
    stop_task = asyncio.create_task(stop_event.wait())
    done, _ = await asyncio.wait((call_task, stop_task), return_when=asyncio.FIRST_COMPLETED)
    if call_task in done:
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return True, call_task.result()

    if request_stop is not None:
        try:
            request_stop()
        except Exception:
            # A best-effort cooperative hint cannot defeat the authoritative
            # cancellation and grace-expiry path below.
            pass
    call_task.cancel()
    done, _ = await asyncio.wait((call_task,), timeout=grace_seconds)
    if not done:
        # This is the one and only grace-expiry cancellation. The bridge will
        # detach its daemon thread and the durable transaction/fence remains
        # authoritative.
        call_task.cancel()
    await asyncio.gather(call_task, return_exceptions=True)
    return False, None


__all__ = [
    "run_blocking_call",
    "run_blocking_call_until_stopped",
    "run_blocking_call_with_timeout",
]
