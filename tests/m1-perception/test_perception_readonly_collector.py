import json
from pathlib import Path

import pytest

import scripts.perception_fault_readonly as collector


def _round(
    sequence: int,
    *,
    release_id: str = "a" * 40,
    machine_id: str = "machine-1",
    boot_id: str = "12345678-1234-4234-9234-123456789abc",
) -> dict[str, object]:
    return {
        "observed_at_ms": 1_000 + sequence * 100,
        "latencies_s": [0.1 + sequence / 100],
        "health": {
            "serviceId": "polyarb-l1",
            "releaseId": release_id,
            "machineId": machine_id,
            "bootId": boot_id,
            "qualificationPolicy": {
                "candidateQuoteHardStaleS": 90,
                "candidateLowerLaneMaxWaitS": 120,
                "discoveryCandidateMaxWaitS": 60,
            },
        },
        "discovery": {
            "status": "available",
            "discovery": {
                "oldest_visit_age_ms": 3_600_000,
                "coverage": {
                    "by_minutes": {
                        "15": {
                            "liquidity_weighted_fraction": 0.95,
                        }
                    }
                },
                "admission_proof": {
                    "effective_start_bound_ms": 45_000,
                },
            },
        },
        "reconciliation": {
            "status": "available",
            "reconciliation": {
                "status": "open",
                "started_at_ms": 500,
                "checkpoint_at_ms": 600 + sequence,
                "duration_ms": 100 + sequence,
            },
        },
        "resources": {
            "status": "available",
            "items": [
                {
                    "sample": {
                        "candidate_quote_p95_ms": 15_000,
                    }
                }
            ],
        },
        "incidents": {
            "status": "available",
            "items": [],
            "open_count": 0,
        },
        "qualification": {
            "status": "available",
            "cross_membership_quote_batches": 0,
            "orphan_collecting_runs": 0,
        },
    }


def test_collector_exports_bounded_evidence_builder() -> None:
    assert callable(getattr(collector, "build_evidence", None))


def test_builder_binds_identity_and_derives_only_observed_metrics() -> None:
    assert callable(getattr(collector, "build_evidence", None))
    rounds = [_round(sequence) for sequence in range(5)]

    evidence = collector.build_evidence(rounds, expected_release="a" * 40)

    assert evidence["scope"] == "production-readonly"
    assert evidence["release_id"] == "a" * 40
    assert evidence["machine_id"] == "machine-1"
    assert evidence["boot_id"] == "12345678-1234-4234-9234-123456789abc"
    assert evidence["sample_count"] == 5
    assert evidence["candidate_quote_p95_s"] == 15
    assert evidence["candidate_stale_before_s"] == 90
    assert evidence["normal_quote_stale_before_s"] == 120
    assert evidence["liquidity_weighted_active_known_coverage"] == 0.95
    assert evidence["coverage_window_s"] == 900
    assert evidence["oldest_known_group_visit_s"] == 3_600
    assert evidence["promotion_to_watch_s"] == 45
    assert evidence["reconciliation_complete"] is False
    assert evidence["reconciliation_advancing"] is True
    assert evidence["open_incident_count"] == 0
    assert "reconciliation_closure_s" not in evidence
    assert "mttd_s" not in evidence
    assert "containment_s" not in evidence
    assert evidence["cross_membership_quote_batches"] == 0
    assert evidence["orphan_collecting_runs"] == 0


def test_builder_rejects_identity_change_mid_window() -> None:
    assert callable(getattr(collector, "build_evidence", None))
    rounds = [_round(sequence) for sequence in range(5)]
    rounds[-1] = _round(4, boot_id="87654321-4321-4321-8321-cba987654321")

    try:
        collector.build_evidence(rounds, expected_release="a" * 40)
    except ValueError as exc:
        assert str(exc) == "runtime-identity-changed"
    else:
        raise AssertionError("identity change must fail closed")


def test_collect_rounds_uses_only_bounded_get_surfaces() -> None:
    assert callable(getattr(collector, "collect_rounds", None))
    template = _round(0)
    by_path = {
        "/healthz": template["health"],
        "/perception/discovery": template["discovery"],
        "/perception/reconciliation": template["reconciliation"],
        "/perception/resources?limit=1": template["resources"],
        "/perception/incidents?limit=100": template["incidents"],
        "/perception/qualification": template["qualification"],
    }
    calls: list[str] = []

    def fetch_json(base_url: str, path: str) -> tuple[object, float]:
        assert base_url == "https://example.test"
        calls.append(path)
        return by_path[path], 0.1

    rounds = collector.collect_rounds(
        "https://example.test",
        sample_count=5,
        interval_s=0,
        fetch_json=fetch_json,
        clock_ms=iter(range(1_000, 1_500, 100)).__next__,
        sleeper=lambda _: None,
    )

    assert len(rounds) == 5
    assert calls == list(by_path) * 5
    assert all(len(sample["latencies_s"]) == 6 for sample in rounds)


def test_cli_rejects_non_https_remote_url(tmp_path: Path) -> None:
    assert callable(getattr(collector, "main", None))

    result = collector.main(
        [
            "--base-url",
            "http://example.test",
            "--expected-release",
            "a" * 40,
            "--output",
            str(tmp_path / "evidence.json"),
        ]
    )

    assert result == 2
    assert not (tmp_path / "evidence.json").exists()


def test_cli_writes_exclusive_canonical_evidence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert callable(getattr(collector, "main", None))
    evidence_path = tmp_path / "evidence.json"
    rounds = [_round(sequence) for sequence in range(5)]
    monkeypatch.setattr(collector, "collect_rounds", lambda *args, **kwargs: rounds)

    first = collector.main(
        [
            "--base-url",
            "https://example.test",
            "--expected-release",
            "a" * 40,
            "--output",
            str(evidence_path),
        ]
    )
    original = evidence_path.read_text()
    second = collector.main(
        [
            "--base-url",
            "https://example.test",
            "--expected-release",
            "a" * 40,
            "--output",
            str(evidence_path),
        ]
    )

    assert first == 0
    assert second == 2
    assert evidence_path.read_text() == original
    assert json.loads(original)["scope"] == "production-readonly"
    assert original.endswith("\n")
