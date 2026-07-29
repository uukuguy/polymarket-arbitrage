import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

import scripts.perception_fault_acceptance as acceptance

ROOT = Path(__file__).parents[2]
EVALUATOR = ROOT / "scripts/perception_fault_acceptance.py"
FIXTURE = ROOT / "tests/fixtures/perception-fault-acceptance-pass.json"


def fixture_evidence(**overrides: object) -> dict[str, object]:
    evidence: dict[str, object] = {
        "evidence_schema_version": 1,
        "scope": "local-conformance",
        "app_id": "polyarb-l1",
        "http_p95_s": 1.0,
        "candidate_quote_p95_s": 15,
        "candidate_stale_before_s": 60,
        "normal_quote_stale_before_s": 100,
        "liquidity_weighted_active_known_coverage": 0.95,
        "coverage_window_s": 900,
        "oldest_known_group_visit_s": 3_600,
        "promotion_to_watch_s": 30,
        "reconciliation_complete": True,
        "reconciliation_closure_s": 3_600,
        "reconciliation_advancing": False,
        "mttd_s": 10,
        "containment_s": 20,
        "cross_membership_quote_batches": 0,
        "orphan_collecting_runs": 0,
        "open_incident_count": 0,
        "incidents": [
            {
                "component": "candidate",
                "incident_id": 7,
                "state": "verified",
                "recovery_writer_receipt": {
                    "component": "candidate",
                    "receipt_row_id": 41,
                },
            }
        ],
    }
    evidence.update(overrides)
    return evidence


def test_acceptance_evaluator_is_available_at_operator_entrypoint() -> None:
    assert EVALUATOR.is_file()


def test_acceptance_evaluator_exports_evaluate() -> None:
    assert callable(getattr(acceptance, "evaluate", None))


def test_verdict_passes_complete_evidence_within_every_sla() -> None:
    verdict = acceptance.evaluate(fixture_evidence())

    assert verdict.status == "PASS"
    assert verdict.reasons == ()

def test_production_fault_scope_requires_and_accepts_exact_runtime_identity() -> None:
    release = "a" * 40
    evidence = fixture_evidence(
        scope="production-fault",
        incidents=[
            {
                "component": "candidate",
                "incident_id": "b" * 32,
                "state": "verified",
                "recovery_writer_receipt": {
                    "component": "candidate",
                    "receipt_row_id": 41,
                },
            }
        ],
        release_id=release,
        machine_id="85e647c4eed598",
        boot_id="6d62de9e-4587-4c4a-bb4f-cf4f261ac0c2",
        window_started_at_ms=1_000,
        window_ended_at_ms=2_000,
        sample_count=5,
    )

    verdict = acceptance.evaluate(
        evidence,
        required_scope="production-fault",
        expected_release=release,
    )

    assert verdict.status == "PASS"
    assert verdict.reasons == ()

    mismatch = acceptance.evaluate(
        evidence,
        required_scope="production-fault",
        expected_release="b" * 40,
    )
    assert mismatch.status == "FAIL"
    assert "release-mismatch" in mismatch.reasons


def test_verdict_rejects_incident_without_recovery_writer_receipt() -> None:
    evidence = fixture_evidence(
        incidents=[
            {
                "component": "candidate",
                "incident_id": 7,
                "state": "verified",
                "recovery_writer_receipt": None,
            }
        ]
    )

    assert callable(getattr(acceptance, "evaluate", None))
    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "missing-recovery-writer-evidence" in verdict.reasons


def test_verdict_rejects_background_pass_with_hot_sla_violation() -> None:
    evidence = fixture_evidence(
        candidate_quote_p95_s=31,
        reconciliation_complete=True,
    )

    assert callable(getattr(acceptance, "evaluate", None))
    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "candidate-quote-p95" in verdict.reasons


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("http_p95_s", 2.01, "http-p95"),
        ("candidate_stale_before_s", 90.01, "candidate-stale"),
        ("normal_quote_stale_before_s", 120.01, "normal-stale"),
        (
            "liquidity_weighted_active_known_coverage",
            0.899,
            "active-known-coverage",
        ),
        ("coverage_window_s", 900.01, "coverage-window"),
        ("oldest_known_group_visit_s", 21_600.01, "oldest-known-group-visit"),
        ("promotion_to_watch_s", 60.01, "promotion-to-watch"),
        ("reconciliation_closure_s", 86_400.01, "reconciliation-closure"),
        ("mttd_s", 30.01, "mttd"),
        ("containment_s", 60.01, "containment"),
        ("cross_membership_quote_batches", 1, "cross-membership-quote"),
        ("orphan_collecting_runs", 1, "orphan-collecting-run"),
        ("open_incident_count", 1, "open-incident"),
    ],
)
def test_verdict_rejects_each_sla_violation(
    field: str,
    value: object,
    reason: str,
) -> None:
    verdict = acceptance.evaluate(fixture_evidence(**{field: value}))

    assert verdict.status == "FAIL"
    assert reason in verdict.reasons


def test_reconciliation_can_be_incomplete_only_while_checkpoint_advances() -> None:
    stalled = acceptance.evaluate(
        fixture_evidence(
            reconciliation_complete=False,
            reconciliation_advancing=False,
        )
    )
    advancing = acceptance.evaluate(
        fixture_evidence(
            reconciliation_complete=False,
            reconciliation_advancing=True,
        )
    )

    assert stalled.status == "FAIL"
    assert "reconciliation-not-advancing" in stalled.reasons
    assert "reconciliation-closure" not in advancing.reasons


def test_advancing_reconciliation_does_not_require_closure_duration() -> None:
    evidence = fixture_evidence(
        reconciliation_complete=False,
        reconciliation_advancing=True,
    )
    del evidence["reconciliation_closure_s"]

    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "PASS"


def test_verified_incident_requires_component_specific_receipt_identity() -> None:
    evidence = fixture_evidence(
        incidents=[
            {
                "component": "candidate",
                "incident_id": 7,
                "state": "verified",
                "recovery_writer_receipt": {
                    "component": "candidate",
                    "receipt_row_id": None,
                },
            }
        ]
    )

    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "missing-recovery-writer-evidence" in verdict.reasons


def test_verified_incident_receipt_must_match_incident_component() -> None:
    evidence = fixture_evidence()
    incidents = evidence["incidents"]
    assert isinstance(incidents, list)
    incident = incidents[0]
    assert isinstance(incident, dict)
    receipt = incident["recovery_writer_receipt"]
    assert isinstance(receipt, dict)
    receipt["component"] = "discovery"

    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "missing-recovery-writer-evidence" in verdict.reasons


def test_malformed_incident_entry_fails_closed() -> None:
    verdict = acceptance.evaluate(fixture_evidence(incidents=["not-an-object"]))

    assert verdict.status == "FAIL"
    assert "invalid-incidents" in verdict.reasons


@pytest.mark.parametrize("incident_id", ["", "A" * 32, "g" * 32, "a" * 31])
def test_malformed_durable_incident_identity_fails_closed(
    incident_id: str,
) -> None:
    evidence = fixture_evidence()
    incidents = evidence["incidents"]
    assert isinstance(incidents, list)
    assert isinstance(incidents[0], dict)
    incidents[0]["incident_id"] = incident_id

    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "invalid-incidents" in verdict.reasons


def test_missing_required_metric_fails_closed() -> None:
    evidence = fixture_evidence()
    del evidence["http_p95_s"]

    verdict = acceptance.evaluate(evidence)

    assert verdict.status == "FAIL"
    assert "missing-http-p95-s" in verdict.reasons


def _run_cli(
    evidence_path: Path,
    output_path: Path,
    *,
    required_scope: str = "local-conformance",
    expected_release: str | None = None,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(EVALUATOR),
        "--evidence",
        str(evidence_path),
        "--output",
        str(output_path),
        "--require-scope",
        required_scope,
    ]
    if expected_release is not None:
        command.extend(["--expected-release", expected_release])
    return subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )


def test_cli_writes_digest_bound_canonical_pass_verdict(tmp_path: Path) -> None:
    evidence = fixture_evidence()
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text(json.dumps(evidence))

    result = _run_cli(evidence_path, output_path)

    canonical = json.dumps(
        evidence,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    expected = {
        "evidence_sha256": f"sha256:{hashlib.sha256(canonical).hexdigest()}",
        "reasons": [],
        "schema_version": 1,
        "status": "PASS",
    }
    assert result.returncode == 0, result.stderr
    assert json.loads(output_path.read_text()) == expected
    assert output_path.read_text().endswith("\n")


def test_cli_returns_one_for_fail_verdict(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text(json.dumps(fixture_evidence(http_p95_s=3)))

    result = _run_cli(evidence_path, output_path)

    assert result.returncode == 1
    assert json.loads(output_path.read_text())["status"] == "FAIL"
    assert "http-p95" in json.loads(output_path.read_text())["reasons"]


def test_cli_rejects_local_fixture_for_production_scope(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text(FIXTURE.read_text())

    result = _run_cli(
        evidence_path,
        output_path,
        required_scope="production-readonly",
        expected_release="a" * 40,
    )

    assert result.returncode == 1
    assert "scope-mismatch" in json.loads(output_path.read_text())["reasons"]


def test_cli_refuses_to_overwrite_verdict(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text(json.dumps(fixture_evidence()))
    output_path.write_text("operator-owned\n")

    result = _run_cli(evidence_path, output_path)

    assert result.returncode == 2
    assert output_path.read_text() == "operator-owned\n"
    assert "already exists" in result.stderr


def test_cli_rejects_invalid_json_without_creating_verdict(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text("{")

    result = _run_cli(evidence_path, output_path)

    assert result.returncode == 2
    assert not output_path.exists()
    assert "invalid evidence" in result.stderr


def test_cli_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    evidence_path = tmp_path / "evidence.json"
    output_path = tmp_path / "verdict.json"
    evidence_path.write_text('{"http_p95_s":1,"http_p95_s":3}')

    result = _run_cli(evidence_path, output_path)

    assert result.returncode == 2
    assert not output_path.exists()
    assert "duplicate JSON key" in result.stderr


def test_make_exposes_deterministic_local_qualification() -> None:
    result = subprocess.run(
        ["make", "-n", "qualify-perception-local"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "test_perception_fault_acceptance.py" in result.stdout
    assert "perception_fault_acceptance.py" in result.stdout
    assert "perception-fault-acceptance-pass.json" in result.stdout
    assert "--require-scope local-conformance" in result.stdout
