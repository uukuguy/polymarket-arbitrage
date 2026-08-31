"""Makefile contract tests + CLI invocation smoke.

Plan 01-5 T5 — covers two responsibilities:

  1. Makefile contract — every snapshot target in the Makefile must dry-run cleanly
     via ``make -n``. This catches recipe drift (e.g. a future commit accidentally
     dropping the ``--full`` flag) before the user runs them against live APIs.

  2. CLI invocation smoke — typer.testing.CliRunner exercises the in-process
     CLI (``polyarb.snapshot.cli:app``) so we know the orchestrator → CLI →
     stdout/stderr → exit-code path is wired correctly under mocks.

Critical: ``make snapshot-markets`` is NEVER actually executed (would hit live
APIs and take 10-20 minutes). Legacy live/API targets stay on ``make -n``;
qualification targets execute through a fake ``uv`` binary so Make guard and
argv contracts are tested without touching the real CLI or database.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from polyarb.snapshot.cli import app

PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
runner = CliRunner(mix_stderr=False)


def _fake_uv_env(tmp_path: Path) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "uv-argv.jsonl"
    fake_uv = bin_dir / "uv"
    fake_uv.write_text(
        "#!/usr/bin/env python3\n"
        "from __future__ import annotations\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "log = os.environ['FAKE_UV_LOG']\n"
        "with open(log, 'a', encoding='utf-8') as handle:\n"
        "    handle.write(json.dumps({'argv': sys.argv[1:]}, separators=(',', ':')) + '\\n')\n"
        "print(json.dumps({'fake_uv': sys.argv[1:]}, separators=(',', ':')))\n"
    )
    fake_uv.chmod(0o755)
    env = os.environ.copy()
    env["FAKE_UV_LOG"] = str(log_path)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    return env, log_path


def _fake_uv_calls(log_path: Path) -> list[list[str]]:
    if not log_path.exists():
        return []
    return [json.loads(line)["argv"] for line in log_path.read_text().splitlines() if line]


def test_quote_refresh_admit_make_target_is_bounded_and_explicit(tmp_path: Path) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_SUPABASE_DB_DSN"] = "postgresql://operator@example.test/control"

    result = subprocess.run(
        ["make", "quote-refresh-admit-once", "enable=1"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "quote-refresh-admit-once",
            "--enable",
            "--json",
        ]
    ]


def _fake_curl_env(
    tmp_path: Path, *, body: str, status: str = "200"
) -> tuple[dict[str, str], Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "curl-argv.jsonl"
    fake_curl = bin_dir / "curl"
    fake_curl.write_text(
        "#!/usr/bin/env python3\n"
        "from __future__ import annotations\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['FAKE_CURL_LOG'], 'a', encoding='utf-8') as handle:\n"
        "    handle.write(json.dumps({'argv': args}, separators=(',', ':')) + '\\n')\n"
        "body_path = None\n"
        "for index, arg in enumerate(args):\n"
        "    if arg in ('-o', '--output') and index + 1 < len(args):\n"
        "        body_path = args[index + 1]\n"
        "if body_path is not None:\n"
        "    with open(body_path, 'w', encoding='utf-8') as handle:\n"
        "        handle.write(os.environ.get('FAKE_CURL_BODY', ''))\n"
        "print(os.environ.get('FAKE_CURL_STATUS', '200'), end='')\n"
        "sys.exit(int(os.environ.get('FAKE_CURL_EXIT', '0')))\n"
    )
    fake_curl.chmod(0o755)
    env = os.environ.copy()
    env["FAKE_CURL_LOG"] = str(log_path)
    env["FAKE_CURL_BODY"] = body
    env["FAKE_CURL_STATUS"] = status
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"
    return env, log_path


def _fake_json_formatter(env: dict[str, str], tmp_path: Path) -> Path:
    """Install a formatter sentinel so curl failure cannot be hidden by a pipe."""
    bin_dir = Path(env["PATH"].split(os.pathsep, 1)[0])
    log_path = tmp_path / "json-formatter-argv.jsonl"
    fake_python = bin_dir / "python"
    fake_python.write_text(
        "#!/usr/bin/env python3\n"
        "from __future__ import annotations\n"
        "import json\n"
        "import os\n"
        "import sys\n"
        "with open(os.environ['FAKE_JSON_FORMATTER_LOG'], 'a', encoding='utf-8') as handle:\n"
        "    handle.write(json.dumps(sys.argv[1:]) + '\\n')\n"
        "print('{}')\n"
    )
    fake_python.chmod(0o755)
    env["FAKE_JSON_FORMATTER_LOG"] = str(log_path)
    return log_path


def _fake_curl_calls(log_path: Path) -> list[list[str]]:
    if not log_path.exists():
        return []
    return [json.loads(line)["argv"] for line in log_path.read_text().splitlines() if line]


# =============================================================================
# Makefile contract — most legacy targets dry-run; qualification targets execute
# with a fake uv binary so the Make guard/argv path is tested without live APIs.
# =============================================================================


def test_make_help_lists_snapshot_markets() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    # Both subset and full mode targets must appear in the help listing.
    assert "snapshot-markets:" in result.stdout
    assert "snapshot-markets-full:" in result.stdout


def test_make_runtime_policy_replay_is_read_only() -> None:
    """The historical replay target must stay a DSN-scoped, read-only command."""
    result = subprocess.run(
        ["make", "-n", "runtime-policy-replay", "run_id=run-a"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert (
        'uv run python -m polyarb.cli_control_plane runtime-policy-replay --run-id "run-a" --json'
    ) in result.stdout

    recipe = result.stdout.lower()
    for forbidden in ("flyctl", "deploy", "machines", "curl --request post", "curl --request put"):
        assert forbidden not in recipe, f"runtime replay must not mutate cloud state: {forbidden}"


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        (
            "runtime-controller-status",
            "uv run python -m polyarb.cli_control_plane runtime-controller-status",
        ),
        (
            "runtime-observe-verify",
            "uv run python -m polyarb.cli_control_plane runtime-observe-verify",
        ),
        (
            "runtime-reconcile-once",
            "uv run python -m polyarb.cli_control_plane runtime-reconcile-once --enable",
        ),
        (
            "runtime-reconcile-serve",
            "uv run python -m polyarb.cli_control_plane runtime-reconcile-serve --enable",
        ),
        (
            "runtime-reconcile-until",
            "uv run python -m polyarb.cli_control_plane runtime-reconcile-until --enable",
        ),
    ],
)
def test_make_runtime_controller_targets_are_wired(target: str, expected: str) -> None:
    result = subprocess.run(
        ["make", "-n", target, "enable=1"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout
    assert "flyctl" not in result.stdout.lower()
    assert "deploy" not in result.stdout.lower()


def test_make_runtime_mutation_target_has_enable_guard() -> None:
    for target in (
        "runtime-reconcile-once",
        "runtime-reconcile-until",
        "runtime-reconcile-serve",
    ):
        result = subprocess.run(
            ["make", target],
            capture_output=True,
            text=True,
            cwd=PROJECT_ROOT,
            timeout=5,
        )
        assert result.returncode == 2
        assert "enable=1" in result.stderr


def test_make_runtime_reconcile_once_forwards_exact_target_selector(
    tmp_path: Path,
) -> None:
    uv = tmp_path / "uv"
    uv.write_text('#!/bin/sh\nprintf \'%s\\n\' "$*"\n')
    uv.chmod(0o755)
    result = subprocess.run(
        [
            "make",
            "-s",
            "runtime-reconcile-once",
            "enable=1",
            "target_type=circuit",
            "target_id=structure-source:window:fetch:events:162",
            "expected_action=probe-circuit",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env={**os.environ, "PATH": f"{tmp_path}:{os.environ['PATH']}"},
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "--target-type circuit" in result.stdout
    assert "--target-id structure-source:window:fetch:events:162" in result.stdout
    assert "--expected-action probe-circuit" in result.stdout


def test_make_render_machine_update_uses_local_contract_translator() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-render-machine-update",
            "current_machine=/tmp/current.json",
            "fly_config=/tmp/fly.toml",
            "expected_app=polyarb-runtime-controller-m1",
            "machine_id=6e82036dce4958",
            "target_image=registry.fly.io/example:new",
            "update_env_from_fly=POLYARB_QUALIFICATION_RELEASE_ID",
            "output=/tmp/update.json",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "python -m polyarb.control_plane.fly_machine_update render" in result.stdout
    assert '--current-machine "/tmp/current.json"' in result.stdout
    assert '--fly-config "/tmp/fly.toml"' in result.stdout
    assert '--expected-app "polyarb-runtime-controller-m1"' in result.stdout
    assert '--expected-machine-id "6e82036dce4958"' in result.stdout
    assert '--target-image "registry.fly.io/example:new"' in result.stdout
    assert '--update-env-from-fly "POLYARB_QUALIFICATION_RELEASE_ID"' in result.stdout
    assert '--output "/tmp/update.json"' in result.stdout
    assert "flyctl" not in result.stdout.lower()
    assert "curl" not in result.stdout.lower()


def test_make_verify_machine_update_uses_local_redacted_verifier() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-verify-machine-update",
            "updated_machine=/tmp/updated.json",
            "update_payload=/tmp/update.json",
            "machine_id=6e82036dce4958",
            "region=ams",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "python -m polyarb.control_plane.fly_machine_update verify" in result.stdout
    assert '--updated-machine "/tmp/updated.json"' in result.stdout
    assert '--update-payload "/tmp/update.json"' in result.stdout
    assert '--expected-machine-id "6e82036dce4958"' in result.stdout
    assert '--expected-region "ams"' in result.stdout
    assert "flyctl" not in result.stdout.lower()
    assert "curl" not in result.stdout.lower()


def test_make_runtime_maintenance_uses_process_replacement_renderer() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-render-runtime-maintenance",
            "current_machine=/tmp/current.json",
            "expected_app=polyarb-runtime-controller-m1",
            "machine_id=6e82036dce4958",
            "target_type=circuit",
            "target_id=structure:window:normalize:event_tags:177",
            "expected_action=probe-circuit",
            "controller_id=maintenance-a",
            "output=/tmp/maintenance.json",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "render-runtime-maintenance" in result.stdout
    assert '--target-id "structure:window:normalize:event_tags:177"' in result.stdout
    assert "flyctl" not in result.stdout.lower()
    assert "ssh" not in result.stdout.lower()


def test_make_runtime_restore_uses_saved_baseline_renderer() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-render-runtime-restore",
            "baseline_machine=/tmp/baseline.json",
            "maintenance_machine=/tmp/maintenance-machine.json",
            "maintenance_payload=/tmp/maintenance.json",
            "machine_id=6e82036dce4958",
            "output=/tmp/restore.json",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "render-runtime-restore" in result.stdout
    assert '--baseline-machine "/tmp/baseline.json"' in result.stdout
    assert "flyctl" not in result.stdout.lower()
    assert "ssh" not in result.stdout.lower()


def test_make_runtime_maintenance_outcome_requires_before_and_after_artifacts() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-verify-runtime-maintenance-outcome",
            "before_status=/tmp/before.json",
            "after_status=/tmp/after.json",
            "maintenance_payload=/tmp/maintenance.json",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "verify-runtime-maintenance-outcome" in result.stdout
    assert '--before-status "/tmp/before.json"' in result.stdout
    assert '--after-status "/tmp/after.json"' in result.stdout
    assert "flyctl" not in result.stdout.lower()


def test_make_runtime_status_is_read_only_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "runtime-controller-status"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    recipe = result.stdout.lower()
    assert "--enable" not in recipe
    assert "claim_controller" not in recipe


def test_make_runtime_observe_verify_is_read_only_and_bounded() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "runtime-observe-verify",
            "minimum_seconds=1800",
            "max_gap_seconds=90",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    recipe = result.stdout.lower()
    assert "runtime-observe-verify" in recipe
    assert '--minimum-seconds "1800"' in recipe
    assert '--max-gap-seconds "90"' in recipe
    for forbidden in ("--enable", "flyctl", "deploy", "curl --request"):
        assert forbidden not in recipe


def test_make_render_rollout_exposes_exact_six_app_topology() -> None:
    release_id = "0123456789abcdef0123456789abcdef01234567"
    result = subprocess.run(
        [
            "make",
            "-n",
            "control-plane-render-rollout",
            "enable=1",
            "api_app=api-app",
            "worker_app=worker-app",
            "alert_app=alert-app",
            "runtime_event_writer_app=writer-app",
            "runtime_controller_app=controller-app",
            "qualification_worker_app=qualification-app",
            f"release_id={release_id}",
            "runtime_recovery_allowed_targets=worker-app/machine-a",
            "expected_database=control",
            "output_dir=/tmp/runtime-rollout-contract",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert '--runtime-controller-app "controller-app"' in result.stdout
    assert '--qualification-worker-app "qualification-app"' in result.stdout
    assert f'--release-id "{release_id}"' in result.stdout
    assert '--runtime-recovery-allowed-target "worker-app/machine-a"' in result.stdout


def test_make_render_rollout_rejects_missing_release_id_before_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)

    result = subprocess.run(
        [
            "make",
            "control-plane-render-rollout",
            "enable=1",
            "api_app=api-app",
            "worker_app=worker-app",
            "alert_app=alert-app",
            "runtime_event_writer_app=writer-app",
            "runtime_controller_app=controller-app",
            "qualification_worker_app=qualification-app",
            "expected_database=control",
            "output_dir=/tmp/runtime-rollout-contract",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "release_id=<40-char-lowercase-git-sha>" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_render_rollout_forwards_exact_release_id_to_cli(tmp_path: Path) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    release_id = "0123456789abcdef0123456789abcdef01234567"

    result = subprocess.run(
        [
            "make",
            "control-plane-render-rollout",
            "enable=1",
            "api_app=api-app",
            "worker_app=worker-app",
            "alert_app=alert-app",
            "runtime_event_writer_app=writer-app",
            "runtime_controller_app=controller-app",
            "qualification_worker_app=qualification-app",
            f"release_id={release_id}",
            "expected_database=control",
            "output_dir=/tmp/runtime-rollout-contract",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "render-rollout",
            "--enable",
            "--api-app",
            "api-app",
            "--worker-app",
            "worker-app",
            "--alert-app",
            "alert-app",
            "--runtime-event-writer-app",
            "writer-app",
            "--runtime-controller-app",
            "controller-app",
            "--qualification-worker-app",
            "qualification-app",
            "--release-id",
            release_id,
            "--expected-database",
            "control",
            "--output-dir",
            "/tmp/runtime-rollout-contract",
            "--json",
        ]
    ]


def test_make_help_lists_control_plane_db_role_targets() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "control-plane-db-role-preflight:" in result.stdout
    assert "control-plane-db-role-provision:" in result.stdout
    assert "control-plane-db-role-verify:" in result.stdout
    assert "control-plane-db-role-disable:" in result.stdout
    assert "control-plane-fly-topology-audit:" in result.stdout


def test_make_fly_topology_audit_exposes_exact_read_only_argv(tmp_path: Path) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    result = subprocess.run(
        [
            "make",
            "control-plane-fly-topology-audit",
            "targets=writer-app/28654e35a73d08",
            "required_secrets=writer-app/POLYARB_SUPABASE_DB_DSN",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.fly_topology_audit",
            "--target",
            "writer-app/28654e35a73d08",
            "--required-secret",
            "writer-app/POLYARB_SUPABASE_DB_DSN",
            "--json",
        ]
    ]
    recipe = result.stdout.lower()
    for forbidden in ("deploy", "machine stop", "machine start", "secrets set", "secrets unset"):
        assert forbidden not in recipe


@pytest.mark.parametrize(
    ("target", "make_args", "expected_argv"),
    [
        (
            "control-plane-db-role-preflight",
            ("expected_database=control",),
            [
                "run",
                "python",
                "-m",
                "polyarb.control_plane.db_role_admin",
                "preflight",
                "--expected-database",
                "control",
                "--json",
            ],
        ),
        (
            "control-plane-db-role-provision",
            ("enable=1", "expected_database=control"),
            [
                "run",
                "python",
                "-m",
                "polyarb.control_plane.db_role_admin",
                "provision",
                "--enable",
                "--expected-database",
                "control",
                "--json",
            ],
        ),
        (
            "control-plane-db-role-verify",
            ("profile=runtime-controller", "expected_database=control"),
            [
                "run",
                "python",
                "-m",
                "polyarb.control_plane.db_role_admin",
                "verify",
                "--profile",
                "runtime-controller",
                "--expected-database",
                "control",
                "--json",
            ],
        ),
        (
            "control-plane-db-role-disable",
            ("enable=1", "expected_database=control"),
            [
                "run",
                "python",
                "-m",
                "polyarb.control_plane.db_role_admin",
                "disable",
                "--enable",
                "--expected-database",
                "control",
                "--json",
            ],
        ),
    ],
)
def test_make_control_plane_db_role_targets_execute_fake_uv_only(
    tmp_path: Path,
    target: str,
    make_args: tuple[str, ...],
    expected_argv: list[str],
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    if target != "control-plane-db-role-verify":
        env["POLYARB_CONTROL_PLANE_DB_ADMIN_DSN"] = "postgresql://admin@example.test/control"
    result = subprocess.run(
        ["make", target, *make_args],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [expected_argv]


@pytest.mark.parametrize(
    "target",
    ("control-plane-db-role-provision", "control-plane-db-role-disable"),
)
def test_make_control_plane_db_role_mutations_require_enable_before_fake_uv(
    tmp_path: Path,
    target: str,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    result = subprocess.run(
        ["make", target, "expected_database=control"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "enable=1" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_help_lists_runtime_fault_matrix() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "runtime-fault-matrix:" in result.stdout
    assert "local deterministic self-healing fault matrix" in result.stdout


def test_make_runtime_fault_matrix_requires_test_dsn_before_fake_uv(tmp_path: Path) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        ["make", "runtime-fault-matrix"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_runtime_fault_matrix_executes_fake_uv_with_explicit_test_dsn(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    result = subprocess.run(
        ["make", "runtime-fault-matrix"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "runtime-fault-matrix",
            "--json",
        ]
    ]


def test_make_help_lists_stale_owner_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-stale-owner:" in result.stdout
    assert "isolated migrated loopback databases" in result.stdout


def test_make_help_lists_progress_stall_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-progress-stall:" in result.stdout
    assert "live-lease progress-stall" in result.stdout


def test_make_help_lists_retry_budget_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-retry-budget:" in result.stdout
    assert "retry-budget circuit" in result.stdout


def test_make_help_lists_heartbeat_outage_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-heartbeat-outage:" in result.stdout
    assert "live-attempt heartbeat" in result.stdout


def test_make_help_lists_worker_exit_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-worker-exit:" in result.stdout
    assert "expired-worker reclaim" in result.stdout


def test_make_help_lists_source_receipt_gap_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-source-receipt-gap:" in result.stdout
    assert "source receipt barrier" in result.stdout


def test_make_help_lists_quote_batch_incomplete_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-quote-batch-incomplete:" in result.stdout
    assert "Quote batch barrier" in result.stdout


def test_make_help_lists_quote_admission_missing_shard_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-quote-admission-missing-shard:" in result.stdout
    assert "missing Structure shard" in result.stdout


def test_make_help_lists_normalization_payload_corrupt_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-normalization-payload-corrupt:" in result.stdout
    assert "schema-invalid Structure shard" in result.stdout


def test_make_help_lists_structure_parity_mismatch_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-structure-parity-mismatch:" in result.stdout
    assert "frozen Structure count conflict" in result.stdout


def test_make_help_lists_publication_pointer_conflict_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-publication-pointer-conflict:" in result.stdout
    assert "stale Structure, Quote, and Opportunity publishers" in result.stdout


def test_make_help_lists_r2_read_timeout_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-r2-read-timeout:" in result.stdout
    assert "six R2-reading production nodes" in result.stdout


def test_make_help_lists_r2_write_timeout_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-r2-write-timeout:" in result.stdout
    assert "seven R2-writing production nodes" in result.stdout


def test_make_help_lists_stale_quote_pointer_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-stale-quote-pointer:" in result.stdout
    assert "stale Quote authority blocks Opportunity publication" in result.stdout


def test_make_help_lists_clob_missing_leg_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-clob-missing-leg:" in result.stdout
    assert "omitted CLOB coverage cannot publish a Quote batch" in result.stdout


def test_make_help_lists_clob_429_commissioning_harness() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-clob-429:" in result.stdout
    assert "body-free typed CLOB 429" in result.stdout


def test_make_help_lists_gamma_provider_commissioning_harnesses() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-gamma-timeout:" in result.stdout
    assert "Gamma timeout resets transport" in result.stdout
    assert "m1-production-commissioning-gamma-malformed-page:" in result.stdout
    assert "body-free malformed Gamma page" in result.stdout


def test_make_help_lists_complete_commissioning_bundle() -> None:
    result = subprocess.run(
        ["make", "help"], capture_output=True, text=True, cwd=PROJECT_ROOT, timeout=10
    )

    assert result.returncode == 0, result.stderr
    assert "m1-production-commissioning-complete:" in result.stdout
    assert "66 exact-image attack proofs" in result.stdout


def test_make_stale_owner_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-stale-owner",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_progress_stall_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-progress-stall",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_retry_budget_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-retry-budget",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_heartbeat_outage_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-heartbeat-outage",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_worker_exit_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-worker-exit",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_source_receipt_gap_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-source-receipt-gap",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_quote_batch_incomplete_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-quote-batch-incomplete",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_quote_admission_missing_shard_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-quote-admission-missing-shard",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_normalization_payload_corrupt_commissioning_requires_test_dsn_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env.pop("POLYARB_CONTROL_PLANE_TEST_DSN", None)
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-normalization-payload-corrupt",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "POLYARB_CONTROL_PLANE_TEST_DSN" in result.stderr
    assert _fake_uv_calls(log_path) == []


@pytest.mark.parametrize(
    ("make_vars", "expected_error"),
    [
        ((), "evidence_root=<dir>"),
        (("evidence_root=evidence",), "expected_release is required"),
        (
            ("evidence_root=evidence", f"expected_release={'a' * 40}"),
            "expected_config is required",
        ),
    ],
)
def test_make_stale_owner_commissioning_requires_identity_before_fake_uv(
    tmp_path: Path,
    make_vars: tuple[str, ...],
    expected_error: str,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    result = subprocess.run(
        ["make", "m1-production-commissioning-stale-owner", *make_vars],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert expected_error in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_stale_owner_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-stale-owner",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "stale-owner",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_progress_stall_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-progress-stall",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "progress-stall",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_retry_budget_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-retry-budget",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "retry-budget",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_heartbeat_outage_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-heartbeat-outage",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "heartbeat-outage",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_worker_exit_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-worker-exit",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "worker-exit",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_source_receipt_gap_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-source-receipt-gap",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "source-receipt-gap",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_quote_batch_incomplete_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-quote-batch-incomplete",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "quote-batch-incomplete",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_quote_admission_missing_shard_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-quote-admission-missing-shard",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "quote-admission-missing-shard",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_normalization_payload_corrupt_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-normalization-payload-corrupt",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "normalization-payload-corrupt",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_structure_parity_mismatch_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-structure-parity-mismatch",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "structure-parity-mismatch",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_publication_pointer_conflict_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-publication-pointer-conflict",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "publication-pointer-conflict",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_r2_read_timeout_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-r2-read-timeout",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "r2-read-timeout",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_r2_write_timeout_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-r2-write-timeout",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "r2-write-timeout",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_stale_quote_pointer_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-stale-quote-pointer",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "stale-quote-pointer",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_clob_missing_leg_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-clob-missing-leg",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "clob-missing-leg",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_clob_429_commissioning_executes_exact_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-clob-429",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "clob-429",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


@pytest.mark.parametrize(
    "command",
    ("gamma-timeout", "gamma-malformed-page"),
)
def test_make_gamma_provider_commissioning_executes_exact_harness_command(
    tmp_path: Path,
    command: str,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            f"m1-production-commissioning-{command}",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            command,
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_complete_commissioning_executes_exact_resumable_harness_command(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    release = "a" * 40
    config = f"sha256:{'b' * 64}"
    result = subprocess.run(
        [
            "make",
            "m1-production-commissioning-complete",
            "evidence_root=evidence",
            f"expected_release={release}",
            f"expected_config={config}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.control_plane.production_commissioning_harness",
            "complete",
            "--root",
            "evidence",
            "--release-id",
            release,
            "--config-id",
            config,
            "--json",
        ]
    ]


def test_make_complete_commissioning_declares_control_plane_runtime_identity() -> None:
    env = os.environ.copy()
    env["POLYARB_CONTROL_PLANE_TEST_DSN"] = "postgresql://localhost/test"
    result = subprocess.run(
        [
            "make",
            "-n",
            "m1-production-commissioning-complete",
            "evidence_root=evidence",
            f"expected_release={'a' * 40}",
            f"expected_config=sha256:{'b' * 64}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "POLYARB_RUNTIME_ROLE=control-plane uv run python" in result.stdout


def test_make_help_lists_rolling_qualification_targets() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "qualification-status:" in result.stdout
    assert "qualification-certificates:" in result.stdout
    assert "qualification-serve:" in result.stdout
    assert "immutable qualification certificates" in result.stdout
    assert "requires enable=1" in result.stdout


@pytest.mark.parametrize(
    ("make_args", "expected_argv"),
    [
        (
            ("qualification-status",),
            [
                "run",
                "python",
                "-m",
                "polyarb.cli_control_plane",
                "qualification-status",
                "--json",
            ],
        ),
        (
            ("qualification-certificates",),
            [
                "run",
                "python",
                "-m",
                "polyarb.cli_control_plane",
                "qualification-certificates",
                "--limit",
                "20",
                "--json",
            ],
        ),
        (
            ("qualification-certificates", "limit=7"),
            [
                "run",
                "python",
                "-m",
                "polyarb.cli_control_plane",
                "qualification-certificates",
                "--limit",
                "7",
                "--json",
            ],
        ),
    ],
)
def test_make_qualification_read_targets_execute_fake_uv_only(
    tmp_path: Path, make_args: tuple[str, ...], expected_argv: list[str]
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    result = subprocess.run(
        ["make", *make_args],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [expected_argv]


def test_make_qualification_serve_requires_enable_before_cli(tmp_path: Path) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    result = subprocess.run(
        ["make", "qualification-serve"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "enable=1" in result.stderr
    assert _fake_uv_calls(log_path) == []


@pytest.mark.parametrize(
    ("make_args", "expected_interval"),
    [
        (("qualification-serve", "enable=1"), "30"),
        (("qualification-serve", "enable=1", "interval_seconds=5"), "5"),
    ],
)
def test_make_qualification_serve_executes_fake_uv_after_enable_guard(
    tmp_path: Path, make_args: tuple[str, ...], expected_interval: str
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    result = subprocess.run(
        ["make", *make_args],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [
        [
            "run",
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "qualification-serve",
            "--enable",
            "--interval-seconds",
            expected_interval,
            "--json",
        ]
    ]


@pytest.mark.parametrize(
    ("make_args", "expected_argv"),
    [
        (
            ("control-plane-alert-serve", "enable=1"),
            [
                "run",
                "python",
                "-m",
                "polyarb.cli_control_plane",
                "alert-serve",
                "--enable",
                "--worker-id",
                "control-plane-alert-service",
                "--interval-seconds",
                "15",
                "--json",
            ],
        ),
        (
            (
                "control-plane-alert-serve",
                "enable=1",
                "acceptance_run_id=run-a",
                "interval_seconds=5",
            ),
            [
                "run",
                "python",
                "-m",
                "polyarb.cli_control_plane",
                "alert-serve",
                "--enable",
                "--worker-id",
                "control-plane-alert-service",
                "--acceptance-run-id",
                "run-a",
                "--interval-seconds",
                "5",
                "--json",
            ],
        ),
    ],
)
def test_make_control_plane_alert_serve_executes_fake_uv_with_optional_acceptance_scope(
    tmp_path: Path, make_args: tuple[str, ...], expected_argv: list[str]
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_SUPABASE_DB_DSN"] = "postgresql://operator@example.test/control"
    result = subprocess.run(
        ["make", *make_args],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert _fake_uv_calls(log_path) == [expected_argv]


def test_make_control_plane_alert_serve_requires_enable_before_fake_uv(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_uv_env(tmp_path)
    env["POLYARB_SUPABASE_DB_DSN"] = "postgresql://operator@example.test/control"
    result = subprocess.run(
        ["make", "control-plane-alert-serve"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "enable=1" in result.stderr
    assert _fake_uv_calls(log_path) == []


def test_make_help_lists_control_plane_dashboard_smoke() -> None:
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )

    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "smoke-control-plane-dashboard:" in result.stdout
    assert "authenticated /control-plane" in result.stdout
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "auth_header_file" in makefile
    assert "auth_header=" not in makefile


def test_smoke_control_plane_dashboard_requires_authenticated_input_before_curl(
    tmp_path: Path,
) -> None:
    env, log_path = _fake_curl_env(
        tmp_path,
        body="<main><h1>auth page</h1></main>",
    )
    result = subprocess.run(
        ["make", "smoke-control-plane-dashboard", "dashboard_url=http://dashboard.test"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 2
    assert "cookie_file=<cookie.jar> or auth_header_file=<header-file>" in result.stderr
    assert _fake_curl_calls(log_path) == []


def test_smoke_control_plane_dashboard_rejects_auth_or_empty_200_page(
    tmp_path: Path,
) -> None:
    cookie_file = tmp_path / "session.cookie"
    cookie_file.write_text("session=fixture\n")
    env, log_path = _fake_curl_env(
        tmp_path,
        body="<main><h1>Login</h1><p>Vercel Authentication</p></main>",
    )
    result = subprocess.run(
        [
            "make",
            "smoke-control-plane-dashboard",
            "dashboard_url=http://dashboard.test",
            f"cookie_file={cookie_file}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode != 0
    assert "auth/login page returned HTTP 200" in result.stderr
    calls = _fake_curl_calls(log_path)
    assert len(calls) == 1
    joined = " ".join(calls[0])
    assert "--request GET" in joined
    assert "--cookie" in joined
    assert "http://dashboard.test/control-plane" in joined


def test_smoke_control_plane_dashboard_accepts_authenticated_operator_body(
    tmp_path: Path,
) -> None:
    header_file = tmp_path / "headers.txt"
    header_file.write_text("Authorization: Bearer fixture\n")
    env, _log_path = _fake_curl_env(
        tmp_path,
        body=(
            "<main><h2>Runtime overview</h2><h2>Active tasks</h2>"
            "<h2>Incident timeline</h2><h2>Rolling qualification</h2></main>"
        ),
    )
    result = subprocess.run(
        [
            "make",
            "smoke-control-plane-dashboard",
            "dashboard_url=http://dashboard.test",
            f"auth_header_file={header_file}",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        env=env,
        timeout=5,
    )

    assert result.returncode == 0, result.stdout + result.stderr
    assert "/control-plane: authenticated operator panels OK" in result.stdout
    assert "Bearer fixture" not in result.stdout
    assert "Bearer fixture" not in result.stderr


def test_make_control_plane_preflight_help_and_dry_run_require_revision_022() -> None:
    help_result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert help_result.returncode == 0, f"make help failed: {help_result.stderr}"
    assert "control-plane-preflight:" in help_result.stdout
    assert "named 022 database" in help_result.stdout
    assert "named 014 database" not in help_result.stdout

    dry_run = subprocess.run(
        ["make", "-n", "control-plane-preflight", "expected_database=control_plane_staging"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert dry_run.returncode == 0, f"make -n failed: {dry_run.stderr}"
    assert (
        "uv run python -m polyarb.cli_control_plane preflight "
        '--expected-database "control_plane_staging" --json'
    ) in dry_run.stdout


def test_make_snapshot_markets_dry_run_recipe() -> None:
    """The subset target must invoke ``python -m polyarb.snapshot`` WITHOUT --full."""
    result = subprocess.run(
        ["make", "-n", "snapshot-markets"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "python -m polyarb.snapshot" in result.stdout
    # subset target MUST NOT include --full (that would silently switch to full mode).
    # We grep the recipe lines (skip echo'd "make[1]:" diagnostics).
    recipe_lines = [ln for ln in result.stdout.splitlines() if "polyarb.snapshot" in ln]
    assert recipe_lines, "no recipe line found"
    for ln in recipe_lines:
        assert "--full" not in ln, f"snapshot-markets recipe must not include --full: {ln!r}"


def test_make_snapshot_markets_full_dry_run_recipe() -> None:
    """The full target must invoke ``uv run python -m polyarb.snapshot snapshot --full``."""
    result = subprocess.run(
        ["make", "-n", "snapshot-markets-full"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    # Makefile uses 'uv run python -m polyarb.snapshot snapshot --full' (CLAUDE.md §7 toolchain)
    assert "uv run python -m polyarb.snapshot snapshot --full" in result.stdout


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("sync-structure-local", "polyarb.snapshot structure-sync"),
        ("archive-markets-local", "--product archive"),
    ],
)
def test_make_explicit_data_product_targets_are_wired(target: str, expected: str) -> None:
    """Operators must not need to reconstruct product-selection flags by hand."""
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert expected in result.stdout


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("structure-generation-status", "structure-generation-status"),
        ("structure-generation-backfill", "structure-generation-backfill"),
        ("structure-generation-compare", "structure-generation-compare"),
        (
            "structure-generation-drift-compare",
            "structure-generation-drift-compare",
        ),
        ("structure-generation-cleanup", "structure-generation-cleanup"),
    ],
)
def test_make_structure_generation_operator_surfaces_are_wired(target: str, expected: str) -> None:
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout


def test_make_structure_generation_backfill_exposes_bounded_batch_controls() -> None:
    result = subprocess.run(
        [
            "make",
            "-n",
            "structure-generation-backfill",
            "max_rows=500",
            "max_chunks=100",
            "max_elapsed_seconds=60",
        ],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert '--max-rows "500"' in result.stdout
    assert '--max-chunks "100"' in result.stdout
    assert '--max-elapsed-seconds "60"' in result.stdout


def test_structure_generation_status_cli_prints_stable_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from polyarb.snapshot import cli as cli_module
    from polyarb.storage import sqlite_store as store_module

    class FakeStore:
        def __init__(self, _path):
            pass

        def init_schema(self) -> None:
            raise AssertionError("read-only status must not initialize schema")

        def structure_generation_status(
            self, *, retain_generations: int, pressure_probe_limit: int
        ):
            assert retain_generations == 2
            assert pressure_probe_limit == 8
            return {"pointer_snapshot_id": 7, "retention_floor": 2}

    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=tmp_path / "state.db"),
    )
    monkeypatch.setattr(store_module, "SQLiteStore", FakeStore)
    result = runner.invoke(app, ["structure-generation-status"])
    assert result.exit_code == 0, result.stderr
    assert json.loads(result.stdout) == {
        "pointer_snapshot_id": 7,
        "retention_floor": 2,
    }


def test_structure_generation_status_does_not_create_missing_database_parent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from polyarb.snapshot import cli as cli_module

    path = tmp_path / "must-not-exist" / "state.db"
    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=path),
    )
    result = runner.invoke(app, ["structure-generation-status"])
    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "available": False,
        "error": "structure-generation-status-unavailable",
    }
    assert not path.parent.exists()


def test_structure_generation_compare_cli_fails_with_stable_json_when_unavailable(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from polyarb.snapshot import cli as cli_module
    from polyarb.storage.sqlite_store import SQLiteStore

    path = tmp_path / "empty.db"
    SQLiteStore(path).init_schema()
    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=path),
    )
    result = runner.invoke(app, ["structure-generation-compare"])
    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "matches": False,
        "mismatch_reasons": ["legacy-structure-unavailable"],
    }


def test_structure_generation_drift_compare_is_read_only_and_unavailable_json(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from polyarb.snapshot import cli as cli_module
    from polyarb.storage.sqlite_store import SQLiteStore

    path = tmp_path / "empty-drift.db"
    SQLiteStore(path).init_schema()
    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=path),
    )
    before = path.read_bytes()
    result = runner.invoke(app, ["structure-generation-drift-compare"])
    assert result.exit_code == 1
    assert json.loads(result.stdout) == {
        "authorization_mode": "unavailable",
        "authorized": False,
        "available": False,
        "reason": "structure-drift-current-unavailable",
    }
    assert path.read_bytes() == before


def test_structure_generation_cleanup_cli_is_bounded_and_idempotent_on_empty_db(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from polyarb.snapshot import cli as cli_module

    monkeypatch.setattr(
        cli_module,
        "load_settings",
        lambda: SimpleNamespace(db_path=tmp_path / "empty.db"),
    )
    result = runner.invoke(
        app,
        ["structure-generation-cleanup", "--max-rows", "1"],
    )
    assert result.exit_code == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["rows_deleted"] == 0
    assert payload["reclaimed_generation_ids"] == []


def test_makefile_phony_declaration_present() -> None:
    """Core snapshot recipe names must be declared .PHONY so a file by that name
    in the project root can't shadow the recipe.

    Looks at every .PHONY line and verifies the contract targets show up at
    least once across all of them — the actual grouping (one big line vs many
    small lines) is an implementation detail.
    """
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {
        "snapshot-markets",
        "snapshot-markets-full",
        "sync-structure-local",
        "archive-markets-local",
        "qualification-status",
        "qualification-certificates",
        "qualification-serve",
    }
    missing = required - phony_targets
    assert not missing, f"missing .PHONY declarations for: {missing}"


def test_dashboard_smoke_uses_canonical_production_project_url() -> None:
    """The default smoke target must not regress to the dead short alias."""
    result = subprocess.run(
        ["make", "-n", "smoke-l2-dashboard"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "https://polymarket-arbitrage-jiangwen-su-s-projects.vercel.app" in result.stdout
    assert "https://polymarket-arbitrage.vercel.app" not in result.stdout


# =============================================================================
# Phase 02 Plan 01: triple-check contract — dry-run only
# =============================================================================


def test_make_triple_check_dry_run_recipe() -> None:
    """Phase 02 Plan 01: make triple-check must invoke test_makefile_triple_check.sh.

    Dry-run verifies the recipe is wired correctly without actually executing
    the full snapshot pipeline (L11/S5 silent failure gate — see LEARNINGS).
    """
    result = subprocess.run(
        ["make", "-n", "triple-check"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n triple-check failed: {result.stderr}"
    assert "tests/m1-perception/test_makefile_triple_check.sh" in result.stdout, (
        f"triple-check recipe must invoke test_makefile_triple_check.sh, got: {result.stdout!r}"
    )


# =============================================================================
# CLI smoke — typer.testing.CliRunner with mocked clients
# =============================================================================


def _build_yaml(tmp_path: Path, db: Path, parquet: Path) -> Path:
    yaml_path = tmp_path / "test.yaml"
    yaml_path.write_text(
        f"db_path: {db}\n"
        f"parquet_root: {parquet}\n"
        f"liquidity_threshold_usd: 100.0\n"
        f"retry_attempts: 1\n"
        f"retry_min_wait_s: 0.001\n"
        f"retry_max_wait_s: 0.005\n"
        f"http_timeout_s: 2.0\n"
    )
    return yaml_path


def test_cli_help_shows_all_commands() -> None:
    """Snapshot CLI shows 'snapshot' and 'snapshots-purge' commands in --help."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0, f"--help failed: {result.stderr}"
    assert "snapshot" in result.stdout
    assert "snapshots-purge" in result.stdout


def test_cli_default_subset_mode(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
    # Exit code 0 (valid) is expected with our clean fixture; allow 1 for safety.
    assert result.exit_code in (0, 1), f"unexpected exit: {result.exit_code} stderr={result.stderr}"
    assert "mode=subset" in result.stdout
    assert ("OK" in result.stdout) or ("DEGRADED" in result.stdout) or ("FAILED" in result.stdout)
    # SQLite was created at the configured path.
    assert tmp_db_path.exists()


def test_cli_full_flag_sets_full_mode(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--full", "--config", str(yaml_path)])
    assert result.exit_code in (0, 1)
    assert "mode=full" in result.stdout


def test_cli_summary_format_matches_spec(
    tmp_path: Path,
    tmp_db_path: Path,
    tmp_parquet_root: Path,
    mocked_gamma_orchestrator,
    mocked_clob,
) -> None:
    """D-F1: summary line is single-line cron-grep friendly."""
    yaml_path = _build_yaml(tmp_path, tmp_db_path, tmp_parquet_root)
    result = runner.invoke(app, ["snapshot", "--config", str(yaml_path)])
    summary_re = re.compile(
        r"^(OK|DEGRADED|FAILED) \| \d+ markets \| mode=(subset|full)"
        r" \| \d+ issues \| -> .+\.parquet$"
    )
    summary_lines = [ln for ln in result.stdout.splitlines() if summary_re.match(ln)]
    assert summary_lines, f"no summary line matched, stdout={result.stdout!r}"


# Removed bare-invocation test: typer's no_args_is_help only fires for top-level
# Typer apps with multiple commands; with a single @app.command() typer treats
# bare invocation as "run the only command with no args" which triggers a real
# pipeline run + live network. The --help test below covers the help-text contract.


# =============================================================================
# Phase 1.1 plan-02 — translation Makefile targets
# =============================================================================


def test_make_translate_pending_dry_run() -> None:
    """`make -n translate-pending` resolves to the cli_translation entry."""
    result = subprocess.run(
        ["make", "-n", "translate-pending"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translate-pending" in result.stdout
    # Without FORCE=1 the recipe must NOT include --force-full
    recipe_lines = [ln for ln in result.stdout.splitlines() if "cli_translation" in ln]
    for ln in recipe_lines:
        assert "--force-full" not in ln, (
            f"translate-pending without FORCE=1 must not pass --force-full: {ln!r}"
        )


def test_make_translate_pending_force_full_dry_run() -> None:
    """`make -n translate-pending FORCE=1` adds --force-full to the recipe."""
    result = subprocess.run(
        ["make", "-n", "translate-pending", "FORCE=1"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translate-pending" in result.stdout
    assert "--force-full" in result.stdout


def test_make_translate_pending_sample_dry_run() -> None:
    """`make -n translate-pending-sample` includes --limit 50."""
    result = subprocess.run(
        ["make", "-n", "translate-pending-sample"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--limit 50" in result.stdout


def test_make_translation_stats_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "translation-stats"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "polyarb.cli_translation translation-stats" in result.stdout


def test_make_help_lists_translation_targets() -> None:
    """make help must surface all 3 translation targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "translate-pending:" in result.stdout
    assert "translate-pending-sample:" in result.stdout
    assert "translation-stats:" in result.stdout


def test_makefile_translation_targets_phony() -> None:
    """All 3 translation targets must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {"translate-pending", "translate-pending-sample", "translation-stats"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for translation targets: {missing}"


# =============================================================================
# Phase 1.1 plan-03 — observation Makefile targets (8 targets)
# =============================================================================


_OBSERVATION_TARGETS = [
    "scan-thick-but-slippery",
    "scan-near-end",
    "scan-ghost-suspicious",
    "scan-coin-flip",
    "scan-neg-risk-incomplete",
    "scan-by-tag",
    "list-recipes",
    "scans-purge",
]


@pytest.mark.parametrize("target", _OBSERVATION_TARGETS)
def test_make_observation_target_dry_run(target: str) -> None:
    """Each observation target must dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_scan_generic_dry_run() -> None:
    """`make -n scan name=thick-but-slippery` resolves to the cli scan command."""
    result = subprocess.run(
        ["make", "-n", "scan", "name=thick-but-slippery"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n scan failed: {result.stderr}"
    assert "polyarb.cli_observation scan" in result.stdout
    assert "--name thick-but-slippery" in result.stdout


def test_make_help_lists_observation_targets() -> None:
    """make help must surface all 8 observation targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    expected = [
        "scan-thick-but-slippery:",
        "scan-near-end:",
        "scan-ghost-suspicious:",
        "scan-coin-flip:",
        "scan-neg-risk-incomplete:",
        "scan-by-tag:",
        "list-recipes:",
        "scans-purge:",
    ]
    for target in expected:
        assert target in result.stdout, f"missing in `make help`: {target}"


def test_makefile_observation_targets_phony() -> None:
    """All 8 observation targets + the generic `scan` target must be .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {*_OBSERVATION_TARGETS, "scan"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for observation targets: {missing}"


def test_makefile_scan_by_tag_replaces_by_category() -> None:
    """Amendment 01: there is NO scan-by-category target; it was renamed to scan-by-tag."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "scan-by-tag:" in makefile
    # Recipe lines (target definitions, not comments) should not reference
    # the old `by-category` recipe name.
    recipe_lines = [
        ln for ln in makefile.splitlines() if ln.startswith("scan-") and ln.endswith(":")
    ]
    for ln in recipe_lines:
        assert "by-category" not in ln, f"stale by-category target should be by-tag: {ln!r}"


# =============================================================================
# Phase 1.1 plan-04 — compare-snapshots + track-market Makefile targets (2 targets)
# =============================================================================


_PLAN04_TARGETS = ["compare-snapshots", "track-market"]


@pytest.mark.parametrize("target", _PLAN04_TARGETS)
def test_make_plan04_target_dry_run(target: str) -> None:
    """Each plan-04 target must dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", target],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_compare_snapshots_with_args() -> None:
    """`make -n compare-snapshots from=1 to=2` passes --from 1 --to 2."""
    result = subprocess.run(
        ["make", "-n", "compare-snapshots", "from=1", "to=2"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--from" in result.stdout
    assert "--to" in result.stdout


def test_make_track_market_with_slug() -> None:
    """`make -n track-market slug=will-x-happen` passes --slug will-x-happen."""
    result = subprocess.run(
        ["make", "-n", "track-market", "slug=will-x-happen"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--slug will-x-happen" in result.stdout


def test_make_help_lists_plan04_targets() -> None:
    """make help must surface both plan-04 targets."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "compare-snapshots:" in result.stdout
    assert "track-market:" in result.stdout


def test_makefile_plan04_targets_phony() -> None:
    """Both plan-04 targets must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    missing = set(_PLAN04_TARGETS) - phony_targets
    assert not missing, f"missing .PHONY for plan-04 targets: {missing}"


# =============================================================================
# Phase 1.1 plan-05 — show-market + watchlist + watchlist-alerts (3 targets)
# =============================================================================


_PLAN05_TARGETS = ["show-market", "watchlist", "watchlist-alerts"]


@pytest.mark.parametrize("target", _PLAN05_TARGETS)
def test_make_plan05_target_dry_run(target: str) -> None:
    result = subprocess.run(
        ["make", "-n", target, "slug=test"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n {target} failed: {result.stderr}"
    assert "polyarb.cli_observation" in result.stdout


def test_make_show_market_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "show-market", "slug=will-x-happen"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "--slug will-x-happen" in result.stdout


def test_make_watchlist_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "watchlist"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "watchlist" in result.stdout


def test_make_watchlist_alerts_dry_run() -> None:
    result = subprocess.run(
        ["make", "-n", "watchlist-alerts"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n failed: {result.stderr}"
    assert "watchlist-alerts" in result.stdout


def test_makefile_plan05_targets_phony() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    missing = set(_PLAN05_TARGETS) - phony_targets
    assert not missing, f"missing .PHONY for plan-05 targets: {missing}"


# =============================================================================
# Phase 02 Plan 02 — daemon targets (daemon-run-local + smoke-health-local)
# =============================================================================


def test_make_daemon_run_local_dry_run_recipe() -> None:
    """`make -n daemon-run-local` resolves to python -m polyarb.daemon.main."""
    result = subprocess.run(
        ["make", "-n", "daemon-run-local"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n daemon-run-local failed: {result.stderr}"
    assert "polyarb.daemon.main" in result.stdout, (
        f"daemon-run-local recipe must invoke polyarb.daemon.main, got: {result.stdout!r}"
    )


def test_make_smoke_health_local_dry_run_recipe() -> None:
    """`make -n smoke-health-local` resolves to a curl /health call."""
    result = subprocess.run(
        ["make", "-n", "smoke-health-local"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n smoke-health-local failed: {result.stderr}"
    assert "127.0.0.1:$PORT/health" in result.stdout or "127.0.0.1:19080/health" in result.stdout, (
        f"smoke-health-local recipe must target /health on localhost, got: {result.stdout!r}"
    )


def test_make_help_lists_daemon_targets() -> None:
    """make help must surface daemon-run-local and smoke-health-local."""
    result = subprocess.run(
        ["make", "help"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=10,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "daemon-run-local:" in result.stdout, "daemon-run-local missing from make help"
    assert "smoke-health-local:" in result.stdout, "smoke-health-local missing from make help"


# =============================================================================
# Phase 02 Plan 03 — Supabase migrate + reconcile + r2-list Makefile targets
# =============================================================================


def test_make_supabase_migrate_dry_run() -> None:
    """`make -n supabase-migrate` resolves to alembic upgrade head."""
    result = subprocess.run(
        ["make", "-n", "supabase-migrate"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
        env={**__import__("os").environ, "POLYARB_SUPABASE_DB_DSN": "postgresql://dummy"},
    )
    assert result.returncode == 0, f"make -n supabase-migrate failed: {result.stderr}"
    assert "alembic upgrade head" in result.stdout, (
        f"supabase-migrate recipe must invoke 'alembic upgrade head', got: {result.stdout!r}"
    )


def test_make_supabase_reconcile_dry_run() -> None:
    """`make -n supabase-reconcile` resolves to scripts/supabase_seed.py reconcile."""
    result = subprocess.run(
        ["make", "-n", "supabase-reconcile"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n supabase-reconcile failed: {result.stderr}"
    assert "scripts/supabase_seed.py" in result.stdout, (
        f"supabase-reconcile recipe must invoke scripts/supabase_seed.py, got: {result.stdout!r}"
    )


def test_make_r2_list_dry_run() -> None:
    """`make -n r2-list` resolves to boto3 R2 list operation."""
    result = subprocess.run(
        ["make", "-n", "r2-list"],
        capture_output=True,
        text=True,
        cwd=PROJECT_ROOT,
        timeout=5,
        env={
            **__import__("os").environ,
            "POLYARB_R2_ENDPOINT": "https://test.r2.cloudflarestorage.com",
        },
    )
    assert result.returncode == 0, f"make -n r2-list failed: {result.stderr}"
    assert "boto3" in result.stdout, f"r2-list recipe must invoke boto3, got: {result.stdout!r}"


def test_makefile_daemon_targets_phony() -> None:
    """daemon-run-local, smoke-health-local, tail-logs-local must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    required = {"daemon-run-local", "smoke-health-local", "tail-logs-local"}
    missing = required - phony_targets
    assert not missing, f"missing .PHONY for daemon targets: {missing}"


# =============================================================================
# Phase 02 Plan 04 — docker + deploy Makefile targets
# =============================================================================


def test_make_docker_build_dry_run() -> None:
    """`make -n docker-build` resolves to a docker build command."""
    result = subprocess.run(
        ["make", "-n", "docker-build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n docker-build failed: {result.stderr}"
    assert "docker --context orbstack build" in result.stdout, (
        f"docker-build recipe must bind OrbStack, got: {result.stdout!r}"
    )


def test_docker_targets_are_uniformly_bound_to_orbstack_without_global_mutation() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    assert "docker context use" not in makefile
    assert "colima" not in makefile.lower()

    build = subprocess.run(
        ["make", "-n", "docker-build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    status = subprocess.run(
        ["make", "-n", "docker-context-status"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )

    assert build.returncode == 0, build.stderr
    assert "docker --context orbstack build" in build.stdout
    assert status.returncode == 0, status.stderr
    assert "docker context show" in status.stdout
    assert "docker --context orbstack system df" in status.stdout
    assert "orb df -h /var/lib/docker" in status.stdout


def test_make_deploy_dry_run() -> None:
    """`make -n deploy` resolves to a flyctl deploy command."""
    result = subprocess.run(
        ["make", "-n", "deploy"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=False,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n deploy failed: {result.stderr}"
    assert "flyctl deploy" in result.stdout, (
        f"deploy recipe must invoke 'flyctl deploy', got: {result.stdout!r}"
    )


# Plan 02-09: memory-budget-test + docker-smoke-256mb dry-run contract
def test_make_memory_budget_test_dry_run() -> None:
    """`make memory-budget-test` recipe must include both calibration + budget tests."""
    result = subprocess.run(
        ["make", "-n", "memory-budget-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "test_streaming_memory_budget" in result.stdout
    assert "test_streaming_memory_calibration" in result.stdout


def test_make_docker_smoke_256mb_dry_run() -> None:
    """`make docker-smoke-256mb` recipe must enforce --memory=256m and prod $1k threshold."""
    result = subprocess.run(
        ["make", "-n", "docker-smoke-256mb"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "--memory=256m" in result.stdout
    assert "POLYARB_LIQUIDITY_THRESHOLD_USD=1000.0" in result.stdout


# =============================================================================
# Phase 02 Plan 05 — observability targets (sentry-test + alerts-test + logs-tail-axiom)
# =============================================================================


def test_make_sentry_test_dry_run() -> None:
    """`make -n sentry-test` resolves to init_sentry + capture_message under uv."""
    result = subprocess.run(
        ["make", "-n", "sentry-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n sentry-test failed: {result.stderr}"
    assert "init_sentry" in result.stdout, (
        f"sentry-test recipe must call init_sentry, got: {result.stdout!r}"
    )
    assert "capture_message" in result.stdout, (
        f"sentry-test recipe must call capture_message, got: {result.stdout!r}"
    )


def test_make_alerts_test_dry_run() -> None:
    """`make -n alerts-test` resolves to send_paused_alert under uv."""
    result = subprocess.run(
        ["make", "-n", "alerts-test"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n alerts-test failed: {result.stderr}"
    assert "send_paused_alert" in result.stdout, (
        f"alerts-test recipe must call send_paused_alert, got: {result.stdout!r}"
    )


def test_make_logs_tail_axiom_dry_run() -> None:
    """`make -n logs-tail-axiom` prints the Axiom dataset URL (no-op convenience)."""
    result = subprocess.run(
        ["make", "-n", "logs-tail-axiom"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n logs-tail-axiom failed: {result.stderr}"
    assert "axiom.co" in result.stdout, (
        f"logs-tail-axiom recipe should print axiom URL, got: {result.stdout!r}"
    )


def test_makefile_phase02_plan05_targets_phony() -> None:
    """sentry-test / alerts-test / logs-tail-axiom must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"sentry-test", "alerts-test", "logs-tail-axiom"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-05 targets: {missing}"


# =============================================================================
# Phase 02 Plan 02-06 — Dashboard Makefile contract
# =============================================================================


def test_make_dashboard_dev_dry_run() -> None:
    """`make -n dashboard-dev` resolves to `cd dashboard && pnpm run dev`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-dev"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-dev failed: {result.stderr}"
    assert "pnpm run dev" in result.stdout
    assert "cd dashboard" in result.stdout


def test_make_dashboard_build_dry_run() -> None:
    """`make -n dashboard-build` resolves to `cd dashboard && pnpm run build`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-build"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-build failed: {result.stderr}"
    assert "pnpm run build" in result.stdout
    assert "cd dashboard" in result.stdout


def test_make_dashboard_typecheck_dry_run() -> None:
    """`make -n dashboard-typecheck` resolves to `pnpm tsc --noEmit`."""
    result = subprocess.run(
        ["make", "-n", "dashboard-typecheck"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-typecheck failed: {result.stderr}"
    assert "tsc --noEmit" in result.stdout


def test_make_dashboard_deploy_dry_run() -> None:
    """`make -n dashboard-deploy` invokes vercel."""
    result = subprocess.run(
        ["make", "-n", "dashboard-deploy"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n dashboard-deploy failed: {result.stderr}"
    assert "vercel" in result.stdout


def test_makefile_phase02_plan06_targets_phony() -> None:
    """dashboard-{dev,build,typecheck,deploy} must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"dashboard-dev", "dashboard-build", "dashboard-typecheck", "dashboard-deploy"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-06 targets: {missing}"


# =============================================================================
# Phase 02 Plan 07 — soak monitoring Makefile targets
# =============================================================================


def test_make_soak_status_dry_run() -> None:
    """`make -n soak-status` must invoke soak_monitor.py status."""
    result = subprocess.run(
        ["make", "-n", "soak-status"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-status failed: {result.stderr}"
    assert "soak_monitor.py status" in result.stdout, (
        f"soak-status recipe must call soak_monitor.py status, got: {result.stdout!r}"
    )


def test_make_soak_export_dry_run() -> None:
    """`make -n soak-export` must invoke soak_monitor.py export --days 7."""
    result = subprocess.run(
        ["make", "-n", "soak-export"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-export failed: {result.stderr}"
    assert "soak_monitor.py export" in result.stdout, (
        f"soak-export recipe must call soak_monitor.py export, got: {result.stdout!r}"
    )
    assert "--days 7" in result.stdout, (
        f"soak-export recipe must include --days 7, got: {result.stdout!r}"
    )


def test_make_soak_fault_inject_dry_run() -> None:
    """`make -n soak-fault-inject` must exist and dry-run cleanly."""
    result = subprocess.run(
        ["make", "-n", "soak-fault-inject"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make -n soak-fault-inject failed: {result.stderr}"


def test_makefile_phase02_plan07_targets_phony() -> None:
    """soak-status / soak-export / soak-fault-inject must be declared .PHONY."""
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    phony_targets: set[str] = set()
    for line in makefile.splitlines():
        stripped = line.strip()
        if stripped.startswith(".PHONY:"):
            phony_targets.update(stripped[len(".PHONY:") :].split())
    expected = {"soak-status", "soak-export", "soak-fault-inject"}
    missing = expected - phony_targets
    assert not missing, f"missing .PHONY for phase-02 plan-07 soak targets: {missing}"


# =============================================================================
# Quick 260717 — agent worktree lifecycle repair
# =============================================================================


def test_makefile_exposes_safe_worktree_lifecycle_targets() -> None:
    result = subprocess.run(
        ["make", "help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )
    assert result.returncode == 0, f"make help failed: {result.stderr}"
    assert "cleanup-worktrees:" in result.stdout
    assert "patch-gsd-worktree-cleanup:" in result.stdout

    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "--apply" in makefile
    assert "--discard-unmerged" in makefile


def test_make_help_exposes_control_plane_production_smoke() -> None:
    result = subprocess.run(
        ["make", "help"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "smoke-control-plane-prod:" in result.stdout


def test_control_plane_opportunities_is_current_read_only_business_entrypoint() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text(encoding="utf-8")
    match = re.search(
        r"(?m)^control-plane-opportunities:\n(?P<recipe>(?:\t.*\n)+)", makefile
    )
    assert match is not None
    recipe = match.group("recipe")

    assert "https://polyarb-control-api.fly.dev/perception/opportunities" in recipe
    assert "export limit after_group_id" in makefile
    assert "--get" in recipe
    assert '--data-urlencode "limit=$${limit:-50}"' in recipe
    assert '--data-urlencode "after_group_id=$$after_group_id"' in recipe
    assert "$(limit)" not in recipe
    assert "$(after_group_id)" not in recipe
    assert "curl --disable" in recipe
    assert "--connect-timeout 3" in recipe
    assert "--max-time 10" in recipe
    assert "-f" in recipe
    assert "python -m json.tool" in recipe
    assert not any(
        re.search(rf"\b{token}\b", recipe.lower())
        for token in (
            "flyctl",
            "deploy",
            "post",
            "secret",
            "dsn",
            "sqlite",
            "wallet",
            "order",
            "trade",
        )
    )

    result = subprocess.run(
        ["make", "help"], cwd=PROJECT_ROOT, text=True, capture_output=True, timeout=5
    )
    assert result.returncode == 0, result.stderr
    assert "control-plane-opportunities:" in result.stdout


def test_control_plane_opportunities_encodes_untrusted_make_values_without_shell_execution(
    tmp_path: Path,
) -> None:
    env, curl_log = _fake_curl_env(tmp_path, body='{"status":"available"}')
    formatter_log = _fake_json_formatter(env, tmp_path)
    shell_escape = tmp_path / "must-not-exist"
    limit = f'50; touch {shell_escape}; #&"'
    after_group_id = f"group&next#'; touch {shell_escape};"

    result = subprocess.run(
        [
            "make",
            "control-plane-opportunities",
            f"limit={limit}",
            f"after_group_id={after_group_id}",
        ],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert not shell_escape.exists()
    curl_args = _fake_curl_calls(curl_log)[0]
    assert curl_args.count("--data-urlencode") == 2
    assert curl_args[curl_args.index("--data-urlencode") + 1] == f"limit={limit}"
    after_index = curl_args.index("--data-urlencode", curl_args.index("--data-urlencode") + 1)
    assert curl_args[after_index + 1] == f"after_group_id={after_group_id}"
    assert curl_args[-1] == "https://polyarb-control-api.fly.dev/perception/opportunities"
    assert formatter_log.exists()


def test_control_plane_opportunities_preserves_curl_failure_without_formatting(
    tmp_path: Path,
) -> None:
    env, curl_log = _fake_curl_env(tmp_path, body='{"error":"unavailable"}')
    env["FAKE_CURL_EXIT"] = "22"
    formatter_log = _fake_json_formatter(env, tmp_path)

    result = subprocess.run(
        ["make", "control-plane-opportunities"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode != 0
    assert "Error 22" in result.stderr
    assert len(_fake_curl_calls(curl_log)) == 1
    assert not formatter_log.exists()


def test_retired_market_truth_production_smoke_fails_loud_without_network_recipe() -> None:
    result = subprocess.run(
        ["make", "-n", "smoke-market-truth-prod"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "RETIRED: polyarb-l1 no longer exists." in result.stdout
    assert "make smoke-control-plane-prod" in result.stdout
    assert "make control-plane-status" in result.stdout
    assert "fly.dev" not in result.stdout


def test_l1_deploy_binds_exact_source_sha_and_scales_noninteractively() -> None:
    result = subprocess.run(
        ["make", "-n", "deploy"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=5,
    )

    assert result.returncode == 0, result.stderr
    assert "git rev-parse HEAD" in result.stdout
    assert '--env POLYARB_RELEASE_ID="$RELEASE_ID"' in result.stdout
    assert "--ha=false --max-concurrent 1" in result.stdout
    assert "flyctl scale count app=1 cron=1 -a polyarb-l1 --yes" in result.stdout


def test_runtime_image_build_binds_exact_revision_and_is_build_only(
    tmp_path: Path,
) -> None:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log_path = tmp_path / "flyctl-argv.json"
    fake_flyctl = bin_dir / "flyctl"
    fake_flyctl.write_text(
        "#!/usr/bin/env python3\n"
        "import json, os, sys\n"
        "with open(os.environ['FAKE_FLYCTL_LOG'], 'w', encoding='utf-8') as handle:\n"
        "    json.dump({'argv': sys.argv[1:], 'token': os.environ.get('FLY_API_TOKEN'), "
        "'docker_context': os.environ.get('DOCKER_CONTEXT')}, handle)\n"
    )
    fake_flyctl.chmod(0o755)
    env = os.environ.copy()
    env["FAKE_FLYCTL_LOG"] = str(log_path)
    env["PATH"] = f"{bin_dir}{os.pathsep}{env.get('PATH', '')}"

    result = subprocess.run(
        ["make", "runtime-image-build", "image_tag=test-runtime-exact"],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr
    invocation = json.loads(log_path.read_text(encoding="utf-8"))
    argv = invocation["argv"]
    release_id = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    assert argv[0] == "deploy"
    assert "--build-only" in argv
    assert "--push" in argv
    assert argv[argv.index("--image-label") + 1] == "test-runtime-exact"
    assert argv[argv.index("--label") + 1] == (
        f"org.opencontainers.image.revision={release_id}"
    )
    assert "--env" not in argv
    assert invocation["token"] == ""
    assert invocation["docker_context"] == "orbstack"


def test_runtime_image_build_treats_fly_config_as_release_input() -> None:
    makefile = (PROJECT_ROOT / "Makefile").read_text()
    recipe = makefile.split("runtime-image-build:", 1)[1].split("\n## deploy:", 1)[0]

    assert "git diff --quiet -- fly.toml Dockerfile" in recipe
    assert "git diff --cached --quiet -- fly.toml Dockerfile" in recipe
    assert "git ls-files --others --exclude-standard -- fly.toml Dockerfile" in recipe
