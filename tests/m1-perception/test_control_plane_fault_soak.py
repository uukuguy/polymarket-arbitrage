"""Offline cloud fault/soak acceptance contract for transactional M1."""

from __future__ import annotations

import pytest


def _evidence() -> dict[str, object]:
    return {
        "takeovers": [
            {
                "worker": "structure",
                "crash_boundary": "r2-upload-before-receipt",
                "lease_reclaimed_seconds": 90,
                "lease_reclaim_sla_seconds": 120,
                "old_certified_truth_available": True,
                "control_api_readable": True,
                "circuit": {
                    "job_key": "structure:window-a:fetch:events:0",
                    "opened_after_failures": 3,
                    "probe_delays_seconds": [15, 30, 60],
                    "replacement_worker": "structure-replacement-a",
                    "recovery_event_kind": "recovered",
                    "incident_resolved": True,
                    "delivery_receipts": ["dashboard", "telegram"],
                },
            },
            {
                "worker": "quote",
                "crash_boundary": "r2-upload-before-receipt",
                "lease_reclaimed_seconds": 100,
                "lease_reclaim_sla_seconds": 120,
                "old_certified_truth_available": True,
                "control_api_readable": True,
                "circuit": {
                    "job_key": "quote:generation-a:batch:0000",
                    "opened_after_failures": 3,
                    "probe_delays_seconds": [15, 30, 60],
                    "replacement_worker": "quote-replacement-a",
                    "recovery_event_kind": "recovered",
                    "incident_resolved": True,
                    "delivery_receipts": ["dashboard", "telegram"],
                },
            },
        ],
        "soak": {
            "duration_seconds": 86_400,
            "scheduler_ticks": 5_760,
            "control_api_readable": True,
            "manual_unlocks": 0,
            "silent_stops": 0,
            "permanent_degradations": 0,
            "circuit_recoveries": 2,
        },
    }


def test_fault_soak_verdict_requires_both_takeovers_and_uninterrupted_24h_soak() -> None:
    from polyarb.control_plane.fault_soak import verify_fault_soak

    assert verify_fault_soak(_evidence()) == {
        "status": "PASS",
        "takeover_workers": ["quote", "structure"],
        "minimum_soak_seconds": 86_400,
        "observed_soak_seconds": 86_400,
        "manual_unlocks": 0,
        "circuit_recovery_workers": ["quote", "structure"],
    }


@pytest.mark.parametrize(
    "mutate,reason",
    [
        (
            lambda evidence: evidence["takeovers"].pop(),  # type: ignore[union-attr]
            "requires Structure and Quote",
        ),
        (
            lambda evidence: evidence["soak"].update({"duration_seconds": 3_600}),  # type: ignore[union-attr]
            "24 hours",
        ),
        (
            lambda evidence: evidence["takeovers"][0].update(  # type: ignore[index]
                {"old_certified_truth_available": False}
            ),
            "old certified truth",
        ),
        (
            lambda evidence: evidence["takeovers"][0]["circuit"].update(  # type: ignore[index]
                {"probe_delays_seconds": [15, 30, 120]}
            ),
            "circuit probes",
        ),
        (
            lambda evidence: evidence["takeovers"][1]["circuit"].update(  # type: ignore[index]
                {"incident_resolved": False}
            ),
            "incident resolved",
        ),
    ],
)
def test_fault_soak_verdict_rejects_missing_takeover_short_soak_or_lost_truth(
    mutate, reason: str
) -> None:
    from polyarb.control_plane.fault_soak import FaultSoakError, verify_fault_soak

    evidence = _evidence()
    mutate(evidence)
    with pytest.raises(FaultSoakError, match=reason):
        verify_fault_soak(evidence)
