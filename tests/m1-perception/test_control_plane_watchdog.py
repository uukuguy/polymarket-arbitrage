"""Independent runtime watchdog contracts for the transactional control plane."""

from __future__ import annotations

import asyncio


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
    assert delivered[0].startswith("M1 runtime incident:")
    assert delivered[1].startswith("M1 runtime recovered:")


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
