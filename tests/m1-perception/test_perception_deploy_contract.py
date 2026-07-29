import json
import subprocess
from pathlib import Path

import pytest

from scripts.perception_fault_acceptance import evaluate

ROOT = Path(__file__).parents[2]
FIXTURE = ROOT / "tests/fixtures/perception-fault-acceptance-pass.json"


def production_evidence(**overrides: object) -> dict[str, object]:
    evidence = json.loads(FIXTURE.read_text())
    evidence.update(
        {
            "evidence_schema_version": 1,
            "scope": "production-readonly",
            "app_id": "polyarb-l1",
            "release_id": "a" * 40,
            "machine_id": "machine-01HXYZ",
            "boot_id": "12345678-1234-4234-9234-123456789abc",
            "window_started_at_ms": 1_000,
            "window_ended_at_ms": 2_000,
            "sample_count": 5,
        }
    )
    evidence.update(overrides)
    return evidence


def test_local_conformance_cannot_pass_production_scope() -> None:
    evidence = json.loads(FIXTURE.read_text())

    verdict = evaluate(
        evidence,
        required_scope="production-readonly",
        expected_release="a" * 40,
    )

    assert verdict.status == "FAIL"
    assert "scope-mismatch" in verdict.reasons


def test_valid_production_identity_can_reach_sla_verdict() -> None:
    verdict = evaluate(
        production_evidence(),
        required_scope="production-readonly",
        expected_release="a" * 40,
    )

    assert verdict.status == "PASS"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("release_id", "dev", "invalid-release-id"),
        ("machine_id", "local", "invalid-machine-id"),
        ("boot_id", "not-a-uuid", "invalid-boot-id"),
        ("window_started_at_ms", 2_001, "invalid-evidence-window"),
        ("sample_count", 0, "invalid-sample-count"),
    ],
)
def test_production_identity_fails_closed(
    field: str,
    value: object,
    reason: str,
) -> None:
    verdict = evaluate(
        production_evidence(**{field: value}),
        required_scope="production-readonly",
        expected_release="a" * 40,
    )

    assert verdict.status == "FAIL"
    assert reason in verdict.reasons


def test_expected_release_mismatch_fails_closed() -> None:
    verdict = evaluate(
        production_evidence(),
        required_scope="production-readonly",
        expected_release="b" * 40,
    )

    assert verdict.status == "FAIL"
    assert "release-mismatch" in verdict.reasons


def test_prod_readonly_make_contract_is_non_mutating() -> None:
    result = subprocess.run(
        ["make", "-n", "qualify-perception-prod-readonly"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "perception_fault_readonly.py" in result.stdout
    assert "--require-scope production-readonly" in result.stdout
    for forbidden in ("curl -X POST", "/control/", "fly deploy", "queue-"):
        assert forbidden not in result.stdout
