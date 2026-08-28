from __future__ import annotations

import asyncio
import json
import subprocess
import time
from pathlib import Path


def test_soak_start_and_sample_are_read_only_and_need_no_dsn(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {
            "status": "available",
            "expired_leases": 6,
            "open_circuit_count": 74,
            "queue_health": {"quote-batch": {"unfinished": 0}},
            "job_counts": {"succeeded": 100},
        },
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_fly_machine_states",
        lambda machine_ids, *, app: {machine_id: "started" for machine_id in machine_ids},
    )
    evidence = tmp_path / "soak.jsonl"
    args = [
        "--output",
        str(evidence),
        "--control-api-url",
        "https://control.example/perception/control-plane",
        "--machine-id",
        "machine-a",
        "--json",
    ]

    assert cli_control_plane.main(["soak-start", *args]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "baseline-recorded"
    assert cli_control_plane.main(["soak-sample", *args]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "sample-recorded"


def test_local_soak_fly_read_has_an_operation_specific_subprocess_bound(monkeypatch) -> None:
    from polyarb import cli_control_plane

    observed = {}

    def run(argv, **kwargs):
        observed.update({"argv": argv, **kwargs})
        return subprocess.CompletedProcess(
            argv,
            0,
            stdout=json.dumps([{"id": "machine-a", "state": "started"}]),
            stderr="",
        )

    monkeypatch.setattr(cli_control_plane.subprocess, "run", run)

    assert cli_control_plane._read_fly_machine_states(
        ("machine-a",), app="polyarb-control-worker"
    ) == {"machine-a": "started"}
    assert observed["timeout"] == cli_control_plane._FLY_CLI_READ_TIMEOUT_SECONDS


def test_soak_verify_is_local_and_fail_closed(tmp_path: Path, capsys) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.soak_evidence import append_record, create_record

    evidence = tmp_path / "soak.jsonl"
    for index, observed_at in enumerate(("2030-01-01T00:00:00+00:00", "2030-01-02T00:00:00+00:00")):
        append_record(
            evidence,
            create_record(
                observed_at=observed_at,
                control_api_url="https://control.example/perception/control-plane",
                machine_states={"machine-a": "started"},
                control_snapshot={
                    "status": "available",
                    "expired_leases": 6,
                    "open_circuit_count": 74,
                    "queue_health": {},
                    "job_counts": {"succeeded": 100 + index},
                },
            ),
            exclusive=index == 0,
        )

    assert (
        cli_control_plane.main(
            [
                "soak-verify",
                "--evidence",
                str(evidence),
                "--max-gap-seconds",
                "90000",
                "--json",
            ]
        )
        == 0
    )
    assert json.loads(capsys.readouterr().out)["status"] == "PASS"


def test_soak_verify_reports_the_safe_gate_reason(tmp_path: Path, capsys) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.soak_evidence import append_record, create_record

    evidence = tmp_path / "short-soak.jsonl"
    for index, observed_at in enumerate(("2030-01-01T00:00:00+00:00", "2030-01-01T00:05:00+00:00")):
        append_record(
            evidence,
            create_record(
                observed_at=observed_at,
                control_api_url="https://control.example/perception/control-plane",
                machine_states={"machine-a": "started"},
                control_snapshot={
                    "status": "available",
                    "expired_leases": 6,
                    "open_circuit_count": 74,
                    "queue_health": {},
                    "job_counts": {"succeeded": 100 + index},
                },
            ),
            exclusive=index == 0,
        )

    assert cli_control_plane.main(["soak-verify", "--evidence", str(evidence), "--json"]) == 1
    assert "soak must cover at least 86400 seconds" in capsys.readouterr().err


def test_cloud_soak_commands_persist_only_canonical_remote_observations(
    monkeypatch, capsys
) -> None:
    from polyarb import cli_control_plane

    class ControlPlane:
        def __init__(self) -> None:
            self.records: list[dict[str, object]] = []

        def start_soak_run(self, *, run_id: str, baseline_record: dict[str, object]) -> None:
            assert run_id == "formal-cloud-v1"
            self.records.append(baseline_record)

        def append_soak_observation(self, *, run_id: str, record: dict[str, object]) -> None:
            assert run_id == "formal-cloud-v1"
            self.records.append(record)

        def read_soak_observations(self, run_id: str) -> tuple[dict[str, object], ...]:
            assert run_id == "formal-cloud-v1"
            return tuple(self.records)

    control_plane = ControlPlane()
    monkeypatch.setattr(cli_control_plane, "_control_plane_from_env", lambda: control_plane)
    monkeypatch.setattr(
        cli_control_plane,
        "_read_soak_control_snapshot",
        lambda _url: {
            "status": "available",
            "expired_leases": 0,
            "open_circuit_count": 0,
            "queue_health": {},
            "job_counts": {"succeeded": 1},
        },
    )
    monkeypatch.setattr(
        cli_control_plane,
        "_read_cloud_fly_machine_states",
        lambda machine_ids, *, app, token: {machine_id: "started" for machine_id in machine_ids},
    )
    monkeypatch.setenv("POLYARB_FLY_API_TOKEN", "read-token")
    shared = [
        "--run-id",
        "formal-cloud-v1",
        "--control-api-url",
        "https://control.example/perception/control-plane",
        "--fly-app",
        "polyarb-control-worker",
        "--machine-id",
        "machine-a",
        "--json",
    ]

    assert cli_control_plane.main(["cloud-soak-start", *shared]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "cloud-baseline-recorded"
    assert cli_control_plane.main(["cloud-soak-sample", *shared]) == 0
    assert json.loads(capsys.readouterr().out)["status"] == "cloud-sample-recorded"
    assert len(control_plane.records) == 2


def test_cloud_soak_service_requires_explicit_enable_before_connect(monkeypatch, capsys) -> None:
    from polyarb import cli_control_plane

    monkeypatch.setattr(
        cli_control_plane,
        "_control_plane_from_env",
        lambda: (_ for _ in ()).throw(AssertionError("must not connect")),
    )

    assert (
        cli_control_plane.main(
            [
                "cloud-soak-serve",
                "--run-id",
                "formal-cloud-v1",
                "--control-api-url",
                "https://control.example/perception/control-plane",
                "--fly-app",
                "polyarb-control-worker",
                "--machine-id",
                "machine-a",
                "--json",
            ]
        )
        == 2
    )
    assert "--enable is required" in capsys.readouterr().err


def test_cloud_soak_service_stops_without_synthesizing_a_sample() -> None:
    from polyarb import cli_control_plane

    stopped = asyncio.Event()
    stopped.set()
    args = cli_control_plane._parser().parse_args(
        [
            "cloud-soak-serve",
            "--enable",
            "--run-id",
            "formal-cloud-v1",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "polyarb-control-worker",
            "--machine-id",
            "machine-a",
        ]
    )

    assert asyncio.run(
        cli_control_plane._run_cloud_soak_service(object(), args, stop_event=stopped)
    ) == {"status": "stopped", "samples": 0}


def test_cloud_soak_stop_wins_before_a_late_sample_can_write(monkeypatch) -> None:
    from polyarb import cli_control_plane

    class ControlPlane:
        def __init__(self) -> None:
            self.writes = 0

        def append_soak_observation(self, **_kwargs) -> None:
            self.writes += 1

    control_plane = ControlPlane()
    args = cli_control_plane._parser().parse_args(
        [
            "cloud-soak-serve",
            "--enable",
            "--run-id",
            "formal-cloud-v1",
            "--control-api-url",
            "https://control.example/perception/control-plane",
            "--fly-app",
            "polyarb-control-worker",
            "--machine-id",
            "machine-a",
        ]
    )

    def blocked_sample(_control_plane, _args, *, baseline, stop_requested):
        assert baseline is False
        time.sleep(0.05)
        if stop_requested():
            return None
        raise AssertionError("stop must be visible before the write boundary")

    monkeypatch.setattr(cli_control_plane, "_record_cloud_soak_observation", blocked_sample)

    async def scenario():
        stop = asyncio.Event()

        async def stop_soon() -> None:
            await asyncio.sleep(0.005)
            stop.set()

        asyncio.create_task(stop_soon())
        return await cli_control_plane._run_cloud_soak_service(
            control_plane,
            args,
            stop_event=stop,
            grace_seconds=0.1,
        )

    assert asyncio.run(scenario()) == {"status": "stopped", "samples": 0}
    assert control_plane.writes == 0
