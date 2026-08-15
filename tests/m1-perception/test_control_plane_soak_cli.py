from __future__ import annotations

import json
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


def test_soak_verify_is_local_and_fail_closed(tmp_path: Path, capsys) -> None:
    from polyarb import cli_control_plane
    from polyarb.control_plane.soak_evidence import append_record, create_record

    evidence = tmp_path / "soak.jsonl"
    for index, observed_at in enumerate(
        ("2030-01-01T00:00:00+00:00", "2030-01-02T00:00:00+00:00")
    ):
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
