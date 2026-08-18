"""Database-independent runtime checks for the formal transactional lane."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta


@dataclass(frozen=True, slots=True)
class RuntimeObservation:
    """One bounded view of the external runtime facts."""

    healthy: bool
    failures: tuple[str, ...]


class RestartEventGate:
    """Turn fresh Fly restart-counter changes into one observable incident.

    A Machine can have state ``started`` while Fly is repeatedly restarting its
    main process.  The first read establishes the current counter as a baseline;
    each later increase is unhealthy for one watchdog tick, then a normal next
    tick produces the regular recovery transition.
    """

    def __init__(self) -> None:
        self._previous_counts: dict[str, int] | None = None

    def apply(
        self, observation: RuntimeObservation, restart_counts: Mapping[str, int]
    ) -> RuntimeObservation:
        current = dict(restart_counts)
        if self._previous_counts is None:
            self._previous_counts = current
            return observation
        failures = list(observation.failures)
        for machine_id, current_count in current.items():
            previous_count = self._previous_counts.get(machine_id, current_count)
            if current_count > previous_count:
                failures.append(
                    f"machine:{machine_id}:restart-count:{previous_count}->{current_count}"
                )
        self._previous_counts = current
        return RuntimeObservation(healthy=not failures, failures=tuple(failures))


class ProgressGate:
    """Page when durable work is pending but successful-job progress stalls."""

    def __init__(self, *, max_stall: timedelta) -> None:
        if max_stall.total_seconds() <= 0:
            raise ValueError("max_stall must be positive")
        self._max_stall = max_stall
        self._last_succeeded: int | None = None
        self._last_progress_at: datetime | None = None
        self._was_pending = False

    def apply(
        self,
        observation: RuntimeObservation,
        control_api_payload: Mapping[str, object] | None,
        *,
        now: datetime,
    ) -> RuntimeObservation:
        if control_api_payload is None:
            return observation
        job_counts = control_api_payload.get("job_counts")
        if not isinstance(job_counts, Mapping):
            return RuntimeObservation(
                healthy=False,
                failures=(*observation.failures, "control-api:invalid-job-counts"),
            )
        succeeded = job_counts.get("succeeded")
        runnable = job_counts.get("runnable", 0)
        leased = job_counts.get("leased", 0)
        counts = (succeeded, runnable, leased)
        if not all(isinstance(value, int) and value >= 0 for value in counts):
            return RuntimeObservation(
                healthy=False,
                failures=(*observation.failures, "control-api:invalid-job-counts"),
            )
        pending = runnable + leased > 0
        started_pending_work = pending and not self._was_pending
        if (
            self._last_succeeded is None
            or succeeded > self._last_succeeded
            or started_pending_work
        ):
            self._last_succeeded = succeeded
            self._last_progress_at = now
        self._was_pending = pending
        if not pending or self._last_progress_at is None:
            return observation
        elapsed = now - self._last_progress_at
        if elapsed <= self._max_stall:
            return observation
        return RuntimeObservation(
            healthy=False,
            failures=(
                *observation.failures,
                f"control-api:job-progress-stalled:{int(elapsed.total_seconds())}s",
            ),
        )


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
