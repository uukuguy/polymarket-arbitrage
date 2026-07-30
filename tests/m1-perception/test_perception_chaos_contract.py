import json
import subprocess
import sys
from pathlib import Path

import pytest

import scripts.perception_chaos as chaos
import scripts.perception_fault_acceptance as acceptance
from polyarb.config import Settings

ROOT = Path(__file__).parents[2]
SCRIPT = ROOT / "scripts/perception_chaos.py"
FAULT_IDS = (
    "gamma-timeout",
    "gamma-partial",
    "gamma-malformed",
    "gamma-cursor",
    "clob-missing-leg",
    "clob-429",
    "clob-latency",
    "candidate-exit",
    "discovery-exit",
    "reconciliation-stall",
    "sqlite-busy",
    "disk-pressure",
    "telegram-failure",
    "daemon-restart",
    "deploy-interrupt",
    "contention",
)


@pytest.mark.parametrize("fault_id", FAULT_IDS)
def test_every_fault_has_a_complete_readonly_plan(fault_id: str) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", fault_id],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    plan = json.loads(result.stdout)
    assert plan["fault_id"] == fault_id
    assert plan["component"]
    assert plan["expected_incident_kind"]
    assert plan["recovery_writer"]
    assert plan["cleanup"]
    assert plan["required_tools"] == ["python"]
    assert plan["image_check"] == "make chaos-l2-fly-image-check"
    assert plan["execute_supported"] is (
        fault_id in {
            "gamma-timeout",
            "gamma-partial",
            "gamma-malformed",
            "gamma-cursor",
            "clob-missing-leg",
            "clob-429",
            "clob-latency",
            "telegram-failure",
        }
    )


def test_execute_fails_before_mutation_without_exact_upstream_target(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "execute",
            "--fault",
            "gamma-timeout",
            "--expected-release",
            "a" * 40,
            "--authorization",
            f"fault:gamma-timeout:{'a' * 40}",
            "--evidence-dir",
            str(tmp_path / "evidence"),
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    assert "upstream-execution-requires-exact-target" in result.stderr
    assert not (tmp_path / "evidence").exists()


def test_candidate_exit_plan_matches_sigterm_supervisor_outcome() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "candidate-exit"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == "child-nonzero"
    assert plan["execute_supported"] is False
    assert plan["legacy_execute_supported"] is True


def test_discovery_exit_plan_matches_sigterm_supervisor_outcome() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "discovery-exit"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0
    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == "child-nonzero"
    assert plan["execute_supported"] is False
    assert plan["legacy_execute_supported"] is True


def test_reconciliation_stall_uses_durable_early_detection_policy() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "reconciliation-stall"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["execute_supported"] is False
    assert plan["legacy_execute_supported"] is True
    assert plan["expected_incident_kind"] == "child-stalled"
    assert Settings().producer_stall_detection_s <= 30
    assert (
        Settings().producer_stall_detection_s
        < Settings().producer_stall_timeout_s
    )
    assert Settings().producer_stall_timeout_s > 30


@pytest.mark.parametrize(
    ("fault_id", "expected_kind"),
    [
        ("gamma-timeout", "gamma-timeout"),
        ("gamma-malformed", "gamma-malformed"),
        ("gamma-cursor", "gamma-cursor"),
    ],
)
def test_gamma_exception_plans_name_the_durable_runner_incident(
    fault_id: str,
    expected_kind: str,
) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", fault_id],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == expected_kind
    assert plan["execute_supported"] is True


def test_gamma_partial_remains_a_coverage_fact_not_a_failure_incident() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "gamma-partial"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"].startswith("coverage:")
    assert plan["execute_supported"] is True


@pytest.mark.parametrize(
    ("fault_id", "expected_kind"),
    [
        ("clob-missing-leg", "clob-missing-leg"),
        ("clob-429", "clob-429"),
        ("clob-latency", "clob-latency"),
    ],
)
def test_clob_plans_name_group_scoped_durable_incidents(
    fault_id: str,
    expected_kind: str,
) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", fault_id],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == expected_kind
    assert plan["execute_supported"] is True


def test_sqlite_busy_plan_names_group_scoped_durable_incident() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "sqlite-busy"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == "sqlite-busy"
    assert plan["execute_supported"] is False


@pytest.mark.parametrize(
    ("fault_id", "expected_kind"),
    [
        ("disk-pressure", "resource-disk-pressure"),
        ("contention", "resource-contention"),
    ],
)
def test_resource_plans_name_sensor_backed_durable_incidents(
    fault_id: str,
    expected_kind: str,
) -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", fault_id],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == expected_kind
    assert plan["execute_supported"] is False


def test_telegram_plan_names_durable_delivery_incident_and_writer() -> None:
    result = subprocess.run(
        [sys.executable, str(SCRIPT), "plan", "--fault", "telegram-failure"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    plan = json.loads(result.stdout)
    assert plan["expected_incident_kind"] == "telegram-delivery-failed"
    assert plan["recovery_writer"] == "neg_risk_opportunity_notification_attempts"
    assert plan["execute_supported"] is True


def test_reconciliation_stall_adapter_resumes_exact_worker_before_verification(
    tmp_path: Path,
) -> None:
    release = "a" * 40
    commands: list[tuple[str, ...]] = []

    def command(argv):
        argv = tuple(argv)
        commands.append(argv)
        if argv[0] == "make":
            return subprocess.CompletedProcess(argv, 0, "PASS\n", "")
        remote = argv[-1]
        action = (
            "locate"
            if " locate " in f" {remote} "
            else "sigcont"
            if " resume " in f" {remote} "
            else "sigstop"
        )
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "action": action,
                    "component": "reconciliation",
                    "pid": 43,
                    "ppid": 1,
                }
            )
            + "\n",
            "",
        )

    history_reads = 0

    def fetch_json(_base_url: str, path: str):
        nonlocal history_reads
        if path.startswith("/perception/incidents/recent"):
            return {
                "status": "available",
                "items": [
                    {
                        "incident_id": "d" * 32,
                        "kind": "child-stalled",
                        "state": "recovering",
                    }
                ],
            }, 0.1
        history_reads += 1
        state = "recovering" if history_reads == 1 else "verified"
        return {
            "status": "available",
            "history_complete": True,
            "recovery_writer_receipt": (
                None
                if state == "recovering"
                else {
                    "component": "reconciliation",
                    "receipt_row_id": 99,
                }
            ),
            "items": [
                {"state": "detected", "occurred_at_ms": 1_100},
                {"state": "contained", "occurred_at_ms": 1_200},
                {"state": state, "occurred_at_ms": 1_300},
            ],
        }, 0.1

    rounds = iter([[{}] * 5, [{}] * 5])
    now = [0.0]
    evidence = chaos.execute_reconciliation_stall(
        base_url="https://example.test",
        expected_release=release,
        authorization=f"fault:reconciliation-stall:{release}",
        evidence_dir=tmp_path / "reconciliation-stall",
        timeout_s=10,
        command=command,
        fetch_json=fetch_json,
        collect_rounds=lambda *_args, **_kwargs: next(rounds),
        build_evidence=lambda samples, **_kwargs: {
            "machine_id": "machine-1",
            "boot_id": "12345678-1234-4234-9234-123456789abc",
            "sample_count": len(samples),
            "open_incident_count": 0,
            "cross_membership_quote_batches": 0,
            "orphan_collecting_runs": 0,
            "incidents": [],
        },
        clock_ms=lambda: 1_000,
        monotonic=lambda: now[0],
        sleeper=lambda seconds: now.__setitem__(0, now[0] + seconds),
    )

    remote_commands = [argv[-1] for argv in commands if argv[0] == "flyctl"]
    assert " stall " in f" {remote_commands[1]} "
    assert " resume " in f" {remote_commands[2]} "
    assert evidence["incidents"][0]["recovery_writer_receipt"]["receipt_row_id"] == 99


def test_candidate_exit_adapter_preserves_complete_release_bound_evidence(
    tmp_path: Path,
) -> None:
    release = "a" * 40
    commands: list[tuple[str, ...]] = []

    def command(argv):
        argv = tuple(argv)
        commands.append(argv)
        if argv[0] == "make":
            return subprocess.CompletedProcess(argv, 0, "PASS\n", "")
        if "locate --component candidate" in argv[-1]:
            return subprocess.CompletedProcess(
                argv,
                0,
                '{"action":"locate","component":"candidate","pid":41,"ppid":1}\n',
                "",
            )
        return subprocess.CompletedProcess(
            argv,
            0,
            '{"action":"sigterm","component":"candidate","pid":41,"ppid":1}\n',
            "",
        )

    history = {
        "status": "available",
        "incident_id": "b" * 32,
        "history_complete": True,
        "recovery_writer_receipt": {
            "component": "candidate",
            "receipt_row_id": 77,
        },
        "items": [
            {"state": "detected", "occurred_at_ms": 1_100},
            {"state": "classified", "occurred_at_ms": 1_150},
            {"state": "contained", "occurred_at_ms": 1_200},
            {"state": "recovering", "occurred_at_ms": 1_250},
            {"state": "verified", "occurred_at_ms": 1_300},
        ],
    }

    def fetch_json(_base_url: str, path: str):
        if path.startswith("/perception/incidents/recent"):
            return (
                {
                    "status": "available",
                    "items": [
                        {
                            "incident_id": "b" * 32,
                            "kind": "child-nonzero",
                            "state": "verified",
                        }
                    ],
                },
                0.1,
            )
        assert path == f"/perception/incidents/{'b' * 32}/history"
        return history, 0.1

    rounds = iter(
        [
            [{"sample": "pre"}] * 5,
            [{"sample": "post"}] * 5,
        ]
    )

    def collect_rounds(*_args, **_kwargs):
        return next(rounds)

    def build_evidence(samples, *, expected_release):
        assert expected_release == release
        return {
            "evidence_schema_version": 1,
            "scope": "production-readonly",
            "app_id": "polyarb-l1",
            "release_id": release,
            "machine_id": "machine-1",
            "boot_id": "12345678-1234-4234-9234-123456789abc",
            "window_started_at_ms": 900,
            "window_ended_at_ms": 1_400,
            "sample_count": len(samples),
            "http_p95_s": 0.1,
            "candidate_quote_p95_s": 15,
            "candidate_stale_before_s": 90,
            "normal_quote_stale_before_s": 120,
            "liquidity_weighted_active_known_coverage": 0.95,
            "coverage_window_s": 900,
            "oldest_known_group_visit_s": 3_600,
            "promotion_to_watch_s": 30,
            "reconciliation_complete": True,
            "reconciliation_advancing": False,
            "reconciliation_closure_s": 3_600,
            "cross_membership_quote_batches": 0,
            "orphan_collecting_runs": 0,
            "open_incident_count": 0,
            "incidents": [],
        }

    evidence_dir = tmp_path / "candidate-exit"
    evidence = chaos.execute_candidate_exit(
        base_url="https://example.test",
        expected_release=release,
        authorization=f"fault:candidate-exit:{release}",
        evidence_dir=evidence_dir,
        timeout_s=10,
        command=command,
        fetch_json=fetch_json,
        collect_rounds=collect_rounds,
        build_evidence=build_evidence,
        clock_ms=lambda: 1_000,
        monotonic=lambda: 0,
        sleeper=lambda _seconds: None,
    )

    assert json.loads((evidence_dir / "intent.json").read_text())["pid"] == 41
    assert json.loads((evidence_dir / "evidence.json").read_text()) == evidence
    assert evidence["mttd_s"] == 0.1
    assert evidence["containment_s"] == 0.1
    assert acceptance.evaluate(
        evidence,
        required_scope="production-fault",
        expected_release=release,
    ).status == "PASS"
    assert commands[0] == ("make", "chaos-l2-fly-image-check", "required=python")
    assert all(isinstance(command_argv, tuple) for command_argv in commands)


def test_candidate_exit_adapter_refuses_dirty_baseline_before_mutation(
    tmp_path: Path,
) -> None:
    commands: list[tuple[str, ...]] = []

    def command(argv):
        commands.append(tuple(argv))
        return subprocess.CompletedProcess(argv, 0, "PASS\n", "")

    with pytest.raises(chaos.AdapterFailedError, match="baseline-open-incident"):
        chaos.execute_candidate_exit(
            base_url="https://example.test",
            expected_release="a" * 40,
            authorization=f"fault:candidate-exit:{'a' * 40}",
            evidence_dir=tmp_path / "must-not-exist",
            timeout_s=10,
            command=command,
            collect_rounds=lambda *_args, **_kwargs: [{}] * 5,
            build_evidence=lambda *_args, **_kwargs: {
                "machine_id": "machine-1",
                "boot_id": "12345678-1234-4234-9234-123456789abc",
                "open_incident_count": 1,
                "cross_membership_quote_batches": 0,
                "orphan_collecting_runs": 0,
            },
            sleeper=lambda _seconds: None,
        )

    assert commands == [("make", "chaos-l2-fly-image-check", "required=python")]
    assert not (tmp_path / "must-not-exist").exists()


def test_discovery_exit_adapter_binds_discovery_worker_and_receipt(
    tmp_path: Path,
) -> None:
    release = "a" * 40
    commands: list[tuple[str, ...]] = []

    def command(argv):
        argv = tuple(argv)
        commands.append(argv)
        if argv[0] == "make":
            return subprocess.CompletedProcess(argv, 0, "PASS\n", "")
        action = "locate" if " locate " in f" {argv[-1]} " else "sigterm"
        return subprocess.CompletedProcess(
            argv,
            0,
            json.dumps(
                {
                    "action": action,
                    "component": "discovery",
                    "pid": 42,
                    "ppid": 1,
                }
            )
            + "\n",
            "",
        )

    def fetch_json(_base_url: str, path: str):
        if path.startswith("/perception/incidents/recent"):
            assert "scope=discovery" in path
            return {
                "status": "available",
                "items": [
                    {
                        "incident_id": "c" * 32,
                        "kind": "child-nonzero",
                        "state": "verified",
                    }
                ],
            }, 0.1
        return {
            "status": "available",
            "history_complete": True,
            "recovery_writer_receipt": {
                "component": "discovery",
                "receipt_row_id": 88,
            },
            "items": [
                {"state": "detected", "occurred_at_ms": 1_100},
                {"state": "contained", "occurred_at_ms": 1_200},
                {"state": "verified", "occurred_at_ms": 1_300},
            ],
        }, 0.1

    rounds = iter([[{}] * 5, [{}] * 5])

    evidence = chaos.execute_producer_exit(
        component="discovery",
        base_url="https://example.test",
        expected_release=release,
        authorization=f"fault:discovery-exit:{release}",
        evidence_dir=tmp_path / "discovery-exit",
        timeout_s=10,
        command=command,
        fetch_json=fetch_json,
        collect_rounds=lambda *_args, **_kwargs: next(rounds),
        build_evidence=lambda samples, **_kwargs: {
            "machine_id": "machine-1",
            "boot_id": "12345678-1234-4234-9234-123456789abc",
            "sample_count": len(samples),
            "open_incident_count": 0,
            "cross_membership_quote_batches": 0,
            "orphan_collecting_runs": 0,
            "incidents": [],
        },
        clock_ms=lambda: 1_000,
        monotonic=lambda: 0,
        sleeper=lambda _seconds: None,
    )

    assert evidence["incidents"] == [
        {
            "component": "discovery",
            "incident_id": "c" * 32,
            "state": "verified",
            "recovery_writer_receipt": {
                "component": "discovery",
                "receipt_row_id": 88,
            },
        }
    ]
    assert any("locate --component discovery" in argv[-1] for argv in commands)
    assert any(
        f"--authorization fault:discovery-exit:{release}:42" in argv[-1]
        for argv in commands
    )


@pytest.mark.parametrize("fault_id", FAULT_IDS)
def test_every_fault_has_a_plan_only_make_entry(fault_id: str) -> None:
    target = f"chaos-perception-{fault_id}"
    result = subprocess.run(
        ["make", "-s", target],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout)["fault_id"] == fault_id

    help_result = subprocess.run(
        ["make", "-s", "help"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert help_result.returncode == 0, help_result.stderr
    assert target in help_result.stdout


def test_make_exposes_release_bound_recovery_verifier() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "verify-perception-recovery",
            "evidence=evidence.json",
            "output=verdict.json",
            f"expected_release={'a' * 40}",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert "perception_fault_acceptance.py" in result.stdout
    assert "--require-scope production-fault" in result.stdout
    assert f'--expected-release \"{"a" * 40}\"' in result.stdout


def test_candidate_make_execute_binds_canonical_https_origin() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "chaos-perception-candidate-exit",
            "mode=execute",
            f"expected_release={'a' * 40}",
            f"authorization=fault:candidate-exit:{'a' * 40}",
            "evidence_dir=output/candidate-exit",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert '--base-url "https://polyarb-l1.fly.dev"' in result.stdout
    assert '--timeout-s "120"' in result.stdout


def test_upstream_make_execute_passes_exact_runtime_and_separate_authorities() -> None:
    release = "a" * 40
    boot_id = "12345678-1234-4234-9234-123456789abc"
    result = subprocess.run(
        [
            "make",
            "-n",
            "chaos-perception-gamma-timeout",
            "mode=execute",
            f"expected_release={release}",
            f"authorization=fault:gamma-timeout:{release}",
            "ordinary_authorization=ordinary-approval",
            "fault_authorization=fault-approval",
            "machine_id=machine-1",
            f"boot_id={boot_id}",
            "call_class=gamma-discovery-event-page",
            "target_key=discovery",
            'parameters_json={"delay_ms":10}',
            "evidence_dir=output/gamma-timeout",
        ],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    for expected in (
        '--ordinary-authorization "ordinary-approval"',
        '--fault-authorization "fault-approval"',
        '--machine-id "machine-1"',
        f'--boot-id "{boot_id}"',
        '--call-class "gamma-discovery-event-page"',
        '--target-key "discovery"',
        '--parameters-json "{"delay_ms":10}"',
    ):
        assert expected in result.stdout
