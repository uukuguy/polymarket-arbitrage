"""Database-independent runtime checks for the formal transactional lane."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class RuntimeObservation:
    """One bounded view of the external runtime facts."""

    healthy: bool
    failures: tuple[str, ...]


def assess_runtime(
    *,
    machine_states: Mapping[str, str],
    expected_machine_ids: Sequence[str],
    control_api_payload: Mapping[str, object] | None = None,
    control_api_error: BaseException | None = None,
    machine_error: BaseException | None = None,
) -> RuntimeObservation:
    """Fail closed when the control read path or an exact Machine is unavailable."""
    failures: list[str] = []
    if control_api_error is not None:
        failures.append(f"control-api:{type(control_api_error).__name__}")
    elif control_api_payload is None or control_api_payload.get("status") != "available":
        failures.append("control-api:unavailable")
    if machine_error is not None:
        failures.append(f"fly-machines:{type(machine_error).__name__}")
    else:
        for machine_id in expected_machine_ids:
            state = machine_states.get(machine_id)
            if state != "started":
                failures.append(f"machine:{machine_id}:{state or 'missing'}")
    return RuntimeObservation(healthy=not failures, failures=tuple(failures))


def render_alert(observation: RuntimeObservation, *, recovered: bool) -> str:
    """Render a credential-free Telegram body suitable for incident handoff."""
    if recovered:
        return "M1 runtime recovered: control API and expected business Machines are healthy."
    return "M1 runtime incident: " + "; ".join(observation.failures)


async def run_watchdog_service(
    *,
    observe: Callable[[], RuntimeObservation],
    send: Callable[[str], Awaitable[None]],
    on_check: Callable[[RuntimeObservation], Awaitable[None]] | None = None,
    interval_seconds: float,
    stop_event: asyncio.Event,
    wait: Callable[[float], Awaitable[None]] | None = None,
) -> dict[str, object]:
    """Page on a health transition; repeated identical failures do not storm chat."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")

    async def wait_for_next_tick(seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        except TimeoutError:
            return

    sleep = wait or wait_for_next_tick
    previous_healthy: bool | None = None
    checks = 0
    alerts = 0
    while not stop_event.is_set():
        observation = await asyncio.to_thread(observe)
        checks += 1
        if on_check is not None:
            await on_check(observation)
        if previous_healthy is None or observation.healthy != previous_healthy:
            await send(render_alert(observation, recovered=observation.healthy))
            alerts += 1
        previous_healthy = observation.healthy
        await sleep(interval_seconds)
    return {"status": "stopped", "checks": checks, "alerts": alerts}
