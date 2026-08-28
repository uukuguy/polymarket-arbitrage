"""Independent runtime watchdog contracts for the transactional control plane."""

from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from time import monotonic


def test_watchdog_stop_detaches_a_stalled_read_only_observation() -> None:
    from polyarb.control_plane.watchdog import run_watchdog_service

    started = threading.Event()
    release = threading.Event()
    stop = asyncio.Event()

    def observe():
        started.set()
        release.wait()
        raise AssertionError("detached observation must not re-enter the stopped service")

    async def send(_text: str) -> None:
        raise AssertionError("no incomplete observation may page")

    async def run() -> dict[str, object]:
        task = asyncio.create_task(
            run_watchdog_service(
                observe=observe,
                send=send,
                interval_seconds=30,
                stop_event=stop,
            )
        )
        while not started.is_set():
            await asyncio.sleep(0)
        stop.set()
        return await task

    safety_release = threading.Timer(0.4, release.set)
    safety_release.start()
    before = monotonic()
    try:
        assert asyncio.run(run()) == {"status": "stopped", "checks": 0, "alerts": 0}
        assert monotonic() - before < 0.2
    finally:
        release.set()
        safety_release.cancel()


def test_watchdog_classifies_database_backed_api_timeout_and_stopped_machine() -> None:
    from polyarb.control_plane.watchdog import assess_runtime

    observation = assess_runtime(
        machine_states={"coordinator": "stopped", "quote": "started"},
        expected_machine_ids=("coordinator", "quote"),
        control_api_error=TimeoutError(),
    )

    assert observation.healthy is False
    assert observation.failures == (
        "control-api:TimeoutError",
        "machine:coordinator:stopped",
    )


def test_watchdog_requires_available_control_api_and_all_expected_machines_started() -> None:
    from polyarb.control_plane.watchdog import assess_runtime

    observation = assess_runtime(
        machine_states={"coordinator": "started", "quote": "started"},
        expected_machine_ids=("coordinator", "quote"),
        control_api_payload={"status": "available", "job_counts": {"succeeded": 9}},
    )

    assert observation.healthy is True
    assert observation.failures == ()


def test_watchdog_pages_once_for_a_new_non_requested_machine_restart() -> None:
    from polyarb.control_plane.watchdog import RestartEventGate, RuntimeObservation

    gate = RestartEventGate()
    healthy = RuntimeObservation(healthy=True, failures=())

    assert gate.apply(healthy, {"evidence/sampler": 4}) == healthy

    restarted = gate.apply(healthy, {"evidence/sampler": 5})

    assert restarted.healthy is False
    assert restarted.failures == ("machine:evidence/sampler:restart-count:4->5",)
    assert gate.apply(healthy, {"evidence/sampler": 5}) == healthy


def test_watchdog_pages_when_runnable_work_stops_making_durable_progress() -> None:
    from polyarb.control_plane.watchdog import ProgressGate, RuntimeObservation

    gate = ProgressGate(max_stall=timedelta(minutes=5))
    healthy = RuntimeObservation(healthy=True, failures=())
    started = datetime(2026, 8, 18, 14, 0, tzinfo=UTC)
    pending = {"job_counts": {"runnable": 2, "leased": 1, "succeeded": 100}}

    assert gate.apply(healthy, pending, now=started) == healthy
    assert gate.apply(healthy, pending, now=started + timedelta(minutes=4)) == healthy

    stalled = gate.apply(healthy, pending, now=started + timedelta(minutes=5, seconds=1))

    assert stalled.healthy is False
    assert stalled.failures == ("control-api:job-progress-stalled:301s",)


def test_watchdog_pages_when_latest_cloud_evidence_sample_is_stale() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, SoakEvidenceGate

    gate = SoakEvidenceGate(max_age=timedelta(minutes=15))
    observation = gate.apply(
        RuntimeObservation(healthy=True, failures=()),
        {"soak_evidence": {"latest_observed_at": "2026-08-18T14:00:00+00:00"}},
        now=datetime(2026, 8, 18, 14, 15, 1, tzinfo=UTC),
    )

    assert observation.healthy is False
    assert observation.failures == ("evidence:sample-stale:901s",)


def test_watchdog_rejects_active_collection_without_fresh_cloud_usage() -> None:
    from polyarb.control_plane.watchdog import CloudUsageGate, RuntimeObservation

    gate = CloudUsageGate(max_age=timedelta(minutes=15))
    now = datetime(2026, 8, 18, 14, 15, 1, tzinfo=UTC)
    missing = gate.apply(
        RuntimeObservation(True, ()),
        {"job_counts": {"succeeded": 1}, "cloud_usage": {}},
        now=now,
    )
    stale = gate.apply(
        RuntimeObservation(True, ()),
        {
            "job_counts": {"succeeded": 1},
            "cloud_usage": {"latest_observation": {"observed_at": "2026-08-18T14:00:00+00:00"}},
        },
        now=now,
    )

    assert missing.failures == ("cloud-usage:observation-missing",)
    assert stale.failures == ("cloud-usage:observation-stale:901s",)


def test_watchdog_rejects_a_fresh_sample_from_the_wrong_formal_run() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, SoakEvidenceGate

    gate = SoakEvidenceGate(max_age=timedelta(minutes=15), expected_run_id="m1-formal-required")
    observation = gate.apply(
        RuntimeObservation(healthy=True, failures=()),
        {
            "soak_evidence": {
                "latest_run_id": "m1-formal-other",
                "latest_observed_at": "2026-08-18T14:15:00+00:00",
            }
        },
        now=datetime(2026, 8, 18, 14, 15, 1, tzinfo=UTC),
    )

    assert observation.healthy is False
    assert observation.failures == ("evidence:unexpected-run:m1-formal-other",)


def test_watchdog_message_is_actionable_without_leaking_secrets() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, render_alert

    alert = render_alert(
        RuntimeObservation(
            healthy=False,
            failures=("control-api:HTTPError", "machine:quote:stopped"),
        ),
        recovered=False,
    )

    assert "M1 runtime incident" in alert
    assert "control-api:HTTPError" in alert
    assert "machine:quote:stopped" in alert
    assert "token" not in alert.lower()


def test_watchdog_service_pages_once_then_sends_recovery_on_state_transition() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    observations = iter(
        (
            RuntimeObservation(healthy=False, failures=("control-api:TimeoutError",)),
            RuntimeObservation(healthy=False, failures=("control-api:TimeoutError",)),
            RuntimeObservation(healthy=True, failures=()),
        )
    )
    delivered: list[str] = []
    stop = asyncio.Event()

    def observe() -> RuntimeObservation:
        return next(observations)

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        if len(delivered) == 2:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=observe,
            send=send,
            interval_seconds=60,
            stop_event=stop,
            wait=wait,
        )
    )

    assert result == {"status": "stopped", "checks": 3, "alerts": 2}
    assert delivered[0].startswith("M1 runtime DETECTED")
    assert delivered[1].startswith("M1 runtime RECOVERED")


def test_watchdog_service_emits_a_heartbeat_for_every_completed_check() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    observations = iter(
        (
            RuntimeObservation(healthy=False, failures=("control-api:TimeoutError",)),
            RuntimeObservation(healthy=False, failures=("control-api:TimeoutError",)),
        )
    )
    heartbeats: list[RuntimeObservation] = []
    stop = asyncio.Event()

    async def send(_text: str) -> None:
        return None

    async def on_check(observation: RuntimeObservation) -> None:
        heartbeats.append(observation)

    async def wait(_seconds: float) -> None:
        if len(heartbeats) == 2:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=lambda: next(observations),
            send=send,
            on_check=on_check,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
        )
    )

    assert result == {"status": "stopped", "checks": 2, "alerts": 1}
    assert [heartbeat.healthy for heartbeat in heartbeats] == [False, False]


def test_watchdog_persists_each_state_transition_before_telegram() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    observations = iter(
        (
            RuntimeObservation(healthy=False, failures=("machine:evidence/restart",)),
            RuntimeObservation(healthy=True, failures=()),
        )
    )
    delivered: list[str] = []
    persisted: list[dict[str, object]] = []
    stop = asyncio.Event()

    async def persist(payload: dict[str, object]) -> dict[str, object]:
        persisted.append(payload)
        return {"transition_payload": payload}

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        if len(persisted) == 2:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=lambda: next(observations),
            send=send,
            persist_transition=persist,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
        )
    )

    assert result == {"status": "stopped", "checks": 2, "alerts": 0}
    assert [payload["transition"] for payload in persisted] == ["detected", "recovered"]
    assert delivered == []


def test_runtime_transition_watchdog_persists_normalized_payload_before_telegram() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    observations = iter(
        (
            RuntimeObservation(
                healthy=False,
                failures=("control-api:TimeoutError",),
                incident_id="runtime-watchdog-incident-a",
                job_key="quote:batch:42",
                stage="quote-fetch",
                action="restart-machine",
                qualification_impact="invalidated",
            ),
            RuntimeObservation(
                healthy=False,
                failures=("control-api:TimeoutError",),
                incident_id="runtime-watchdog-incident-a",
                job_key="quote:batch:42",
                stage="quote-fetch",
                action="restart-machine",
                qualification_impact="invalidated",
            ),
        )
    )
    now = datetime(2030, 1, 1, tzinfo=UTC)
    proposals: list[dict[str, object]] = []
    delivered: list[str] = []
    stop = asyncio.Event()

    async def persist(payload: dict[str, object]) -> dict[str, object]:
        proposals.append(payload)
        return {"status": "noop"}

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        if len(proposals) == 2:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=lambda: next(observations),
            send=send,
            persist_transition=persist,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
            now=lambda: now,
            dashboard_url="https://dashboard.example/control-plane",
        )
    )

    assert result == {"status": "stopped", "checks": 2, "alerts": 0}
    assert proposals[0] == {
        "schema_version": "m1-runtime-incident-transition-v1",
        "transition": "detected",
        "incident_id": "runtime-watchdog-incident-a",
        "incident_key": "runtime-watchdog:independent-runtime-watchdog",
        "component": "runtime-watchdog",
        "source": "independent-runtime-watchdog",
        "job_key": "quote:batch:42",
        "stage": "quote-fetch",
        "reason": "control-api:TimeoutError",
        "action": "restart-machine",
        "qualification_impact": "invalidated",
        "dashboard_url": "https://dashboard.example/control-plane",
        "occurred_at": "2030-01-01T00:00:00+00:00",
    }
    assert proposals[1] == proposals[0]
    assert delivered == []


def test_runtime_transition_watchdog_uses_persisted_duplicate_result_to_skip_delivery() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    observations = iter(
        (
            RuntimeObservation(
                healthy=False,
                failures=("control-api:TimeoutError",),
                incident_id="runtime-watchdog-incident-a",
            ),
        )
    )
    persisted: list[dict[str, object]] = []
    delivered: list[str] = []
    stop = asyncio.Event()

    async def persist(payload: dict[str, object]) -> dict[str, object]:
        persisted.append(payload)
        return {"status": "duplicate"}

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=lambda: next(observations),
            send=send,
            persist_transition=persist,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
            now=lambda: datetime(2030, 1, 1, tzinfo=UTC),
            dashboard_url="https://dashboard.example/control-plane",
        )
    )

    assert result == {"status": "stopped", "checks": 1, "alerts": 0}
    assert len(persisted) == 1
    assert delivered == []


def test_runtime_transition_watchdog_delegates_reminder_timing_to_writer() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    clock = iter(
        (
            datetime(2030, 1, 1, 0, 0, tzinfo=UTC),
            datetime(2030, 1, 1, 0, 14, tzinfo=UTC),
            datetime(2030, 1, 1, 0, 15, tzinfo=UTC),
            datetime(2030, 1, 1, 1, 14, tzinfo=UTC),
            datetime(2030, 1, 1, 1, 15, tzinfo=UTC),
        )
    )
    observation = RuntimeObservation(
        healthy=False,
        failures=("control-api:job-progress-stalled:901s",),
        incident_id="runtime-watchdog-incident-a",
        job_key="structure:bundle:7",
        stage="structure-certify",
        action="restart-machine",
        qualification_impact="delayed",
    )
    persisted: list[dict[str, object]] = []
    delivered: list[str] = []
    stop = asyncio.Event()

    async def persist(payload: dict[str, object]) -> dict[str, object]:
        persisted.append(payload)
        return {"status": "noop"}

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        if len(persisted) == 5:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=lambda: observation,
            send=send,
            persist_transition=persist,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
            now=lambda: next(clock),
            dashboard_url="https://dashboard.example/control-plane",
        )
    )

    assert result == {"status": "stopped", "checks": 5, "alerts": 0}
    assert [payload["transition"] for payload in persisted] == [
        "detected",
        "detected",
        "detected",
        "detected",
        "detected",
    ]
    assert delivered == []


def test_runtime_transition_watchdog_break_glass_pages_once_when_writer_fails() -> None:
    from polyarb.control_plane.watchdog import RuntimeObservation, run_watchdog_service

    delivered: list[str] = []
    checks = 0
    stop = asyncio.Event()

    def observe() -> RuntimeObservation:
        nonlocal checks
        checks += 1
        return RuntimeObservation(healthy=False, failures=("control-api:TimeoutError",))

    async def persist(_payload: dict[str, object]) -> dict[str, object]:
        raise OSError("writer-down")

    async def send(text: str) -> None:
        delivered.append(text)

    async def wait(_seconds: float) -> None:
        if checks == 3:
            stop.set()

    result = asyncio.run(
        run_watchdog_service(
            observe=observe,
            send=send,
            persist_transition=persist,
            interval_seconds=30,
            stop_event=stop,
            wait=wait,
        )
    )

    assert result == {"status": "stopped", "checks": 3, "alerts": 1}
    assert delivered == [
        "M1 runtime event writer unavailable; Telegram is in break-glass mode "
        "and the dashboard ledger may be stale."
    ]
