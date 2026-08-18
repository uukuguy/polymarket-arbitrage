from __future__ import annotations

from pathlib import Path

import pytest


def _record(
    at: str,
    *,
    state: str = "started",
    circuits: int = 74,
    successful_job_count: int = 100,
) -> dict[str, object]:
    from polyarb.control_plane.soak_evidence import create_record

    return create_record(
        observed_at=at,
        control_api_url="https://control.example/perception/control-plane",
        machine_states={"machine-a": state},
        control_snapshot={
            "status": "available",
            "expired_leases": 6,
            "open_circuit_count": circuits,
            "queue_health": {},
            "job_counts": {"succeeded": successful_job_count},
        },
    )


def test_v2_soak_requires_monotonic_successful_job_progress() -> None:
    from polyarb.control_plane.soak_evidence import SoakEvidenceError, verify_soak

    assert verify_soak(
        [
            _record("2030-01-01T00:00:00+00:00", successful_job_count=100),
            _record("2030-01-02T00:00:00+00:00", successful_job_count=101),
        ],
        max_gap_seconds=90_000,
    )["successful_job_count"] == 101

    with pytest.raises(SoakEvidenceError, match="successful job count"):
        verify_soak(
            [
                _record("2030-01-01T00:00:00+00:00", successful_job_count=100),
                _record("2030-01-02T00:00:00+00:00", successful_job_count=100),
            ],
            max_gap_seconds=90_000,
        )

    with pytest.raises(SoakEvidenceError, match="successful job count"):
        verify_soak(
            [
                _record("2030-01-01T00:00:00+00:00", successful_job_count=100),
                _record("2030-01-02T00:00:00+00:00", successful_job_count=99),
            ],
            max_gap_seconds=90_000,
        )


def test_soak_record_round_trip_and_verifies_24_hours(tmp_path: Path) -> None:
    from polyarb.control_plane.soak_evidence import append_record, read_records, verify_soak

    path = tmp_path / "soak.jsonl"
    append_record(
        path,
        _record("2030-01-01T00:00:00+00:00", successful_job_count=100),
        exclusive=True,
    )
    append_record(path, _record("2030-01-01T12:00:00+00:00", successful_job_count=101))
    append_record(path, _record("2030-01-02T00:00:00+00:00", successful_job_count=102))

    assert verify_soak(read_records(path), max_gap_seconds=43_201) == {
        "status": "PASS",
        "duration_seconds": 86_400,
        "scheduler_ticks": 3,
        "machine_count": 1,
        "successful_job_count": 102,
    }


@pytest.mark.parametrize(
    "records, reason",
    [
        (
            lambda: [
                _record("2030-01-01T00:00:00+00:00"),
                {**_record("2030-01-02T00:00:00+00:00"), "snapshot_sha256": "0" * 64},
            ],
            "digest",
        ),
        (
            lambda: [
                _record("2030-01-01T00:00:00+00:00"),
                _record("2030-01-02T00:00:00+00:00", state="stopped"),
            ],
            "started",
        ),
        (
            lambda: [
                _record("2030-01-01T00:00:00+00:00"),
                _record("2029-12-31T23:59:59+00:00"),
            ],
            "increasing",
        ),
        (
            lambda: [
                _record("2030-01-01T00:00:00+00:00"),
                _record("2030-01-02T00:00:00+00:00", circuits=75),
            ],
            "circuit",
        ),
    ],
)
def test_soak_verifier_rejects_tampered_or_unhealthy_records(records, reason: str) -> None:
    from polyarb.control_plane.soak_evidence import SoakEvidenceError, verify_soak

    with pytest.raises(SoakEvidenceError, match=reason):
        verify_soak(records(), max_gap_seconds=90_000)


def test_soak_verifier_rejects_short_or_gapped_window() -> None:
    from polyarb.control_plane.soak_evidence import SoakEvidenceError, verify_soak

    with pytest.raises(SoakEvidenceError, match="86400 seconds"):
        verify_soak(
            [_record("2030-01-01T00:00:00+00:00"), _record("2030-01-01T23:59:59+00:00")],
            max_gap_seconds=90_000,
        )
    with pytest.raises(SoakEvidenceError, match="gap"):
        verify_soak(
            [_record("2030-01-01T00:00:00+00:00"), _record("2030-01-02T00:00:00+00:00")]
        )
