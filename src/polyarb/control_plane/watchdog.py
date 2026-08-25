"""Database-independent runtime checks for the formal transactional lane."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from .alert_delivery import (
    DEFAULT_RUNTIME_DASHBOARD_URL,
    render_runtime_incident_message,
    runtime_incident_transition_payload,
)


@dataclass(frozen=True, slots=True)
class RuntimeObservation:
    """One bounded view of the external runtime facts."""

    healthy: bool
    failures: tuple[str, ...]
    incident_id: str = "runtime-watchdog"
    component: str = "runtime-watchdog"
    source: str = "independent-runtime-watchdog"
    job_key: str | None = None
    stage: str | None = None
    action: str = "restart-machine"
    qualification_impact: str = "unknown"


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
        assert isinstance(succeeded, int)
        assert isinstance(runnable, int)
        assert isinstance(leased, int)
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


class SoakEvidenceGate:
    """Fail closed when the independent sampler stops appending evidence."""

    def __init__(self, *, max_age: timedelta, expected_run_id: str | None = None) -> None:
        if max_age.total_seconds() <= 0:
            raise ValueError("max_age must be positive")
        if expected_run_id is not None and not expected_run_id:
            raise ValueError("expected_run_id must be non-empty when provided")
        self._max_age = max_age
        self._expected_run_id = expected_run_id

    def apply(
        self,
        observation: RuntimeObservation,
        control_api_payload: Mapping[str, object] | None,
        *,
        now: datetime,
    ) -> RuntimeObservation:
        if control_api_payload is None:
            return observation
        evidence = control_api_payload.get("soak_evidence")
        run_id = evidence.get("latest_run_id") if isinstance(evidence, Mapping) else None
        observed_at = evidence.get("latest_observed_at") if isinstance(evidence, Mapping) else None
        if self._expected_run_id is not None and run_id != self._expected_run_id:
            observed = run_id if isinstance(run_id, str) and run_id else "missing"
            return RuntimeObservation(
                healthy=False,
                failures=(*observation.failures, f"evidence:unexpected-run:{observed}"),
            )
        if not isinstance(observed_at, str):
            return RuntimeObservation(
                healthy=False,
                failures=(*observation.failures, "control-api:invalid-soak-evidence"),
            )
        try:
            timestamp = datetime.fromisoformat(observed_at).astimezone(UTC)
        except ValueError:
            return RuntimeObservation(
                healthy=False,
                failures=(*observation.failures, "control-api:invalid-soak-evidence"),
            )
        age = now.astimezone(UTC) - timestamp
        if age <= self._max_age:
            return observation
        return RuntimeObservation(
            healthy=False,
            failures=(*observation.failures, f"evidence:sample-stale:{int(age.total_seconds())}s"),
        )


class CloudUsageGate:
    """Fail closed when active work has no recent metered cloud-input fact."""

    def __init__(self, *, max_age: timedelta) -> None:
        self._max_age = max_age

    def apply(
        self,
        observation: RuntimeObservation,
        payload: Mapping[str, object] | None,
        *,
        now: datetime,
    ) -> RuntimeObservation:
        if payload is None:
            return observation
        counts = payload.get("job_counts")
        active = isinstance(counts, Mapping) and any(
            int(counts.get(key, 0)) > 0 for key in ("runnable", "leased", "succeeded")
        )
        if not active:
            return observation
        usage = payload.get("cloud_usage")
        latest = usage.get("latest_observation") if isinstance(usage, Mapping) else None
        observed_at = latest.get("observed_at") if isinstance(latest, Mapping) else None
        if not isinstance(observed_at, str):
            return RuntimeObservation(
                False, (*observation.failures, "cloud-usage:observation-missing")
            )
        try:
            age = now.astimezone(UTC) - datetime.fromisoformat(observed_at).astimezone(UTC)
        except ValueError:
            return RuntimeObservation(
                False, (*observation.failures, "cloud-usage:observation-invalid")
            )
        return (
            observation
            if age <= self._max_age
            else RuntimeObservation(
                False,
                (
                    *observation.failures,
                    f"cloud-usage:observation-stale:{int(age.total_seconds())}s",
                ),
            )
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
    persist_transition: Callable[..., Awaitable[object]] | None = None,
    on_check: Callable[[RuntimeObservation], Awaitable[None]] | None = None,
    interval_seconds: float,
    stop_event: asyncio.Event,
    wait: Callable[[float], Awaitable[None]] | None = None,
    now: Callable[[], datetime] | None = None,
    dashboard_url: str = DEFAULT_RUNTIME_DASHBOARD_URL,
) -> dict[str, object]:
    """Page on a health transition; repeated identical failures do not storm chat."""
    if interval_seconds <= 0:
        raise ValueError("interval_seconds must be positive")
    if not dashboard_url:
        raise ValueError("dashboard_url must be non-empty")

    async def wait_for_next_tick(seconds: float) -> None:
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=seconds)
        except TimeoutError:
            return

    sleep = wait or wait_for_next_tick
    clock = now or (lambda: datetime.now(UTC))
    previous_healthy: bool | None = None
    break_glass_sent = False
    checks = 0
    alerts = 0
    while not stop_event.is_set():
        tick_at = clock().astimezone(UTC)
        observation = await asyncio.to_thread(observe)
        checks += 1
        if on_check is not None:
            await on_check(observation)
        if persist_transition is not None:
            proposal = _runtime_transition_payload(
                observation,
                transition="recovered" if observation.healthy else "detected",
                occurred_at=tick_at,
                dashboard_url=dashboard_url,
            )
            try:
                await persist_transition(proposal)
                break_glass_sent = False
            except (OSError, ValueError):
                if not break_glass_sent:
                    await send(
                        "M1 runtime event writer unavailable; Telegram is in "
                        "break-glass mode and the dashboard ledger may be stale."
                    )
                    break_glass_sent = True
                    alerts += 1
        elif previous_healthy is None or observation.healthy != previous_healthy:
            payload = _runtime_transition_payload(
                observation,
                transition="recovered" if observation.healthy else "detected",
                occurred_at=tick_at,
                dashboard_url=dashboard_url,
            )
            await send(render_runtime_incident_message(payload))
            alerts += 1
        previous_healthy = observation.healthy
        await sleep(interval_seconds)
    return {"status": "stopped", "checks": checks, "alerts": alerts}


def _runtime_transition_payload(
    observation: RuntimeObservation,
    *,
    transition: str,
    occurred_at: datetime,
    dashboard_url: str,
) -> dict[str, object]:
    reason = observation.failures[0] if observation.failures else "runtime-healthy"
    return runtime_incident_transition_payload(
        transition=transition,
        incident_id=observation.incident_id,
        incident_key=f"runtime-watchdog:{observation.source}",
        component=observation.component,
        source=observation.source,
        job_key=observation.job_key,
        stage=observation.stage,
        reason=reason,
        action="none" if transition == "recovered" else observation.action,
        qualification_impact=observation.qualification_impact,
        dashboard_url=dashboard_url,
        occurred_at=occurred_at.astimezone(UTC),
    )
