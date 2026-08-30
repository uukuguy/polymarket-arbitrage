"""Contracts for translating rendered Fly config into a Machines API update."""

from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path

import pytest


def _machine() -> dict[str, object]:
    return {
        "id": "6e82036dce4958",
        "instance_id": "01M14S7MT04KBYMPZ7KHA8C1N4",
        "region": "ams",
        "config": {
            "image": "registry.fly.io/example:old",
            "env": {"MODE": "observe-only", "ALLOWED": ""},
            "init": {"cmd": ["python", "-m", "example"]},
            "guest": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 256},
            "restart": {"policy": "always"},
            "metadata": {"fly_process_group": "controller"},
        },
    }


def _fly_config(path: Path) -> Path:
    path.write_text(
        "\n".join(
            (
                'app = "polyarb-runtime-controller-m1"',
                'kill_signal = "SIGTERM"',
                "kill_timeout = 40",
                "",
            )
        )
    )
    return path


def test_machine_update_maps_toml_lifecycle_to_api_stop_config_and_preserves_rest(
    tmp_path: Path,
) -> None:
    from polyarb.control_plane.fly_machine_update import render_machine_update_payload

    current = _machine()
    original = deepcopy(current)
    target_image = "registry.fly.io/example:new@sha256:abc"

    payload, proof = render_machine_update_payload(
        current_machine=current,
        fly_config_path=_fly_config(tmp_path / "fly.toml"),
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_image=target_image,
    )

    assert current == original
    assert payload["current_version"] == current["instance_id"]
    assert payload["config"]["image"] == target_image
    assert payload["config"]["stop_config"] == {
        "signal": "SIGTERM",
        "timeout": "40s",
    }
    assert "kill_signal" not in payload["config"]
    assert "kill_timeout" not in payload["config"]
    expected_preserved = deepcopy(current["config"])
    assert isinstance(expected_preserved, dict)
    expected_preserved.pop("image")
    actual_preserved = deepcopy(payload["config"])
    actual_preserved.pop("image")
    actual_preserved.pop("stop_config")
    assert actual_preserved == expected_preserved
    assert proof == {
        "app": "polyarb-runtime-controller-m1",
        "machine_id": "6e82036dce4958",
        "current_version": "01M14S7MT04KBYMPZ7KHA8C1N4",
        "kill_signal": "SIGTERM",
        "kill_timeout_seconds": 40,
        "preserved_config_sha256": proof["preserved_config_sha256"],
        "target_image": target_image,
        "updated_env_keys": [],
    }
    assert len(proof["preserved_config_sha256"]) == 64


def test_machine_update_allows_one_explicit_full_guest_shape_change(tmp_path: Path) -> None:
    from polyarb.control_plane.fly_machine_update import render_machine_update_payload

    current = _machine()
    original = deepcopy(current)

    payload, proof = render_machine_update_payload(
        current_machine=current,
        fly_config_path=_fly_config(tmp_path / "fly.toml"),
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_image="registry.fly.io/example:new",
        target_cpu_kind="shared",
        target_cpus=2,
        target_memory_mb=256,
    )

    assert current == original
    assert payload["config"]["guest"] == {
        "cpu_kind": "shared",
        "cpus": 2,
        "memory_mb": 256,
    }
    assert proof["resource_change"] == {
        "from": {"cpu_kind": "shared", "cpus": 1, "memory_mb": 256},
        "to": {"cpu_kind": "shared", "cpus": 2, "memory_mb": 256},
    }


@pytest.mark.parametrize(
    ("target_cpu_kind", "target_cpus", "target_memory_mb"),
    (
        ("shared", None, None),
        (None, 2, None),
        (None, None, 256),
        ("shared", 0, 256),
        ("", 2, 256),
    ),
)
def test_machine_update_rejects_partial_or_invalid_guest_shape_change(
    tmp_path: Path,
    target_cpu_kind: str | None,
    target_cpus: int | None,
    target_memory_mb: int | None,
) -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_machine_update_payload,
    )

    with pytest.raises(FlyMachineUpdateContractError):
        render_machine_update_payload(
            current_machine=_machine(),
            fly_config_path=_fly_config(tmp_path / "fly.toml"),
            expected_app="polyarb-runtime-controller-m1",
            expected_machine_id="6e82036dce4958",
            target_image="registry.fly.io/example:new",
            target_cpu_kind=target_cpu_kind,
            target_cpus=target_cpus,
            target_memory_mb=target_memory_mb,
        )


@pytest.mark.parametrize(
    "fly_config",
    (
        'app = "polyarb-runtime-controller-m1"\nkill_signal = "SIGTERM"\n',
        'app = "polyarb-runtime-controller-m1"\nkill_timeout = 40\n',
        ('app = "polyarb-runtime-controller-m1"\nkill_signal = "SIGINT"\nkill_timeout = 40\n'),
        ('app = "polyarb-runtime-controller-m1"\nkill_signal = "SIGTERM"\nkill_timeout = 30\n'),
    ),
)
def test_machine_update_rejects_missing_or_nonformal_shutdown_contract(
    tmp_path: Path, fly_config: str
) -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_machine_update_payload,
    )

    config_path = tmp_path / "fly.toml"
    config_path.write_text(fly_config)

    with pytest.raises(FlyMachineUpdateContractError):
        render_machine_update_payload(
            current_machine=_machine(),
            fly_config_path=config_path,
            expected_app="polyarb-runtime-controller-m1",
            expected_machine_id="6e82036dce4958",
            target_image="registry.fly.io/example:new@sha256:abc",
        )


def test_machine_update_cli_writes_payload_without_emitting_machine_environment(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from polyarb.control_plane.fly_machine_update import main

    machine_path = tmp_path / "machine.json"
    machine_path.write_text(json.dumps(_machine()))
    output_path = tmp_path / "update.json"

    assert (
        main(
            [
                "render",
                "--current-machine",
                str(machine_path),
                "--fly-config",
                str(_fly_config(tmp_path / "fly.toml")),
                "--expected-app",
                "polyarb-runtime-controller-m1",
                "--expected-machine-id",
                "6e82036dce4958",
                "--target-image",
                "registry.fly.io/example:new@sha256:abc",
                "--output",
                str(output_path),
                "--json",
            ]
        )
        == 0
    )

    emitted = capsys.readouterr().out
    assert "MODE" not in emitted
    assert "ALLOWED" not in emitted
    assert "observe-only" not in emitted
    result = json.loads(emitted)
    assert result["status"] == "rendered"
    assert result["output"] == str(output_path)
    payload = json.loads(output_path.read_text())
    assert payload["config"]["env"] == {"MODE": "observe-only", "ALLOWED": ""}

    with pytest.raises(FileExistsError):
        main(
            [
                "render",
                "--current-machine",
                str(machine_path),
                "--fly-config",
                str(tmp_path / "fly.toml"),
                "--expected-app",
                "polyarb-runtime-controller-m1",
                "--expected-machine-id",
                "6e82036dce4958",
                "--target-image",
                "registry.fly.io/example:new@sha256:abc",
                "--output",
                str(output_path),
            ]
        )


def test_machine_update_verifier_requires_exact_remote_config_except_resolved_image(
    tmp_path: Path,
) -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_machine_update_payload,
        verify_machine_update_response,
    )

    payload, _ = render_machine_update_payload(
        current_machine=_machine(),
        fly_config_path=_fly_config(tmp_path / "fly.toml"),
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_image="registry.fly.io/example:new",
    )
    updated = {
        "id": "6e82036dce4958",
        "instance_id": "01M14S8NEWVERSION0000000000",
        "region": "ams",
        "state": "started",
        "config": deepcopy(payload["config"]),
    }
    updated["config"]["image"] = "registry.fly.io/example:new@sha256:abc"

    proof = verify_machine_update_response(
        updated_machine=updated,
        update_payload=payload,
        expected_machine_id="6e82036dce4958",
        expected_region="ams",
    )

    assert proof["status"] == "verified"
    assert proof["new_version"] == "01M14S8NEWVERSION0000000000"
    assert proof["kill_signal"] == "SIGTERM"
    assert proof["kill_timeout_seconds"] == 40
    assert "env" not in proof

    broken = deepcopy(updated)
    broken["config"]["restart"] = {"policy": "no"}
    with pytest.raises(FlyMachineUpdateContractError):
        verify_machine_update_response(
            updated_machine=broken,
            update_payload=payload,
            expected_machine_id="6e82036dce4958",
            expected_region="ams",
        )


def test_machine_update_allows_only_explicit_release_and_database_budget_overlay(
    tmp_path: Path,
) -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_machine_update_payload,
    )

    current = _machine()
    current_config = current["config"]
    assert isinstance(current_config, dict)
    current_env = current_config["env"]
    assert isinstance(current_env, dict)
    current_env["POLYARB_QUALIFICATION_RELEASE_ID"] = "old-release"
    current_env["POLYARB_QUALIFICATION_CONFIG_ID"] = "sha256:old-config"
    current_env["POLYARB_DB_POOL_MAX_SIZE"] = "32"
    config_path = tmp_path / "qualification.toml"
    config_path.write_text(
        "\n".join(
            (
                'app = "polyarb-qualification-worker-m1"',
                'kill_signal = "SIGTERM"',
                "kill_timeout = 40",
                "[env]",
                'POLYARB_QUALIFICATION_RELEASE_ID = "new-release"',
                'POLYARB_QUALIFICATION_CONFIG_ID = "sha256:new-config"',
                'POLYARB_DB_POOL_MAX_SIZE = "1"',
                'MODE = "must-not-overlay"',
                "",
            )
        )
    )

    payload, proof = render_machine_update_payload(
        current_machine=current,
        fly_config_path=config_path,
        expected_app="polyarb-qualification-worker-m1",
        expected_machine_id="6e82036dce4958",
        target_image="registry.fly.io/example:new",
        update_env_from_fly=(
            "POLYARB_QUALIFICATION_RELEASE_ID",
            "POLYARB_QUALIFICATION_CONFIG_ID",
            "POLYARB_DB_POOL_MAX_SIZE",
        ),
    )

    assert payload["config"]["env"] == {
        "MODE": "observe-only",
        "ALLOWED": "",
        "POLYARB_QUALIFICATION_CONFIG_ID": "sha256:new-config",
        "POLYARB_QUALIFICATION_RELEASE_ID": "new-release",
        "POLYARB_DB_POOL_MAX_SIZE": "1",
    }
    assert proof["updated_env_keys"] == [
        "POLYARB_DB_POOL_MAX_SIZE",
        "POLYARB_QUALIFICATION_CONFIG_ID",
        "POLYARB_QUALIFICATION_RELEASE_ID",
    ]
    assert "new-release" not in json.dumps(proof)
    assert "new-config" not in json.dumps(proof)

    with pytest.raises(FlyMachineUpdateContractError):
        render_machine_update_payload(
            current_machine=current,
            fly_config_path=config_path,
            expected_app="polyarb-qualification-worker-m1",
            expected_machine_id="6e82036dce4958",
            target_image="registry.fly.io/example:new",
            update_env_from_fly=("MODE",),
        )


def test_runtime_maintenance_replaces_daemon_without_starting_a_second_python() -> None:
    from polyarb.control_plane.fly_machine_update import (
        render_runtime_recovery_maintenance_payload,
    )

    current = _machine()
    current_config = current["config"]
    assert isinstance(current_config, dict)
    current_config["env"] = {
        "POLYARB_RUNTIME_RECOVERY_MODE": "observe-only",
        "POLYARB_DB_POOL_MAX_SIZE": "1",
    }
    current_config["init"] = {
        "cmd": [
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "runtime-reconcile-serve",
            "--enable",
            "--interval-seconds",
            "30",
            "--json",
        ]
    }

    payload, proof = render_runtime_recovery_maintenance_payload(
        current_machine=current,
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_type="circuit",
        target_id="structure:window:normalize:event_tags:177",
        expected_action="probe-circuit",
        controller_id="m1-runtime-reconciler-maintenance-test",
    )

    assert payload["current_version"] == current["instance_id"]
    config = payload["config"]
    assert config["guest"]["memory_mb"] == 256
    assert config["restart"] == {"policy": "no"}
    assert config["env"]["POLYARB_RUNTIME_RECOVERY_MODE"] == "execute"
    assert config["init"]["cmd"] == [
        "python",
        "-m",
        "polyarb.cli_control_plane",
        "runtime-reconcile-until",
        "--enable",
        "--controller-id",
        "m1-runtime-reconciler-maintenance-test",
        "--owner-id",
        "m1-runtime-reconciler-maintenance-test",
        "--target-type",
        "circuit",
        "--target-id",
        "structure:window:normalize:event_tags:177",
        "--expected-action",
        "probe-circuit",
        "--max-wait-seconds",
        "45",
        "--retry-interval-seconds",
        "1",
        "--json",
    ]
    assert proof["execution_model"] == "replace-resident-process"
    assert proof["restore_required"] is True
    assert "env" not in proof


def test_runtime_maintenance_rejects_non_controller_or_non_observe_baseline() -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_runtime_recovery_maintenance_payload,
    )

    current = _machine()
    with pytest.raises(FlyMachineUpdateContractError, match="runtime controller app"):
        render_runtime_recovery_maintenance_payload(
            current_machine=current,
            expected_app="polyarb-control-worker-m1",
            expected_machine_id="6e82036dce4958",
            target_type="circuit",
            target_id="structure:window:normalize:event_tags:177",
            expected_action="probe-circuit",
            controller_id="maintenance-a",
        )

    config = current["config"]
    assert isinstance(config, dict)
    config["env"] = {"POLYARB_RUNTIME_RECOVERY_MODE": "execute"}
    with pytest.raises(FlyMachineUpdateContractError, match="observe-only"):
        render_runtime_recovery_maintenance_payload(
            current_machine=current,
            expected_app="polyarb-runtime-controller-m1",
            expected_machine_id="6e82036dce4958",
            target_type="circuit",
            target_id="structure:window:normalize:event_tags:177",
            expected_action="probe-circuit",
            controller_id="maintenance-a",
        )


def test_runtime_restore_requires_exact_maintenance_config_and_restores_baseline() -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_runtime_recovery_maintenance_payload,
        render_runtime_recovery_restore_payload,
    )

    baseline = _machine()
    baseline_config = baseline["config"]
    assert isinstance(baseline_config, dict)
    baseline_config["env"] = {
        "POLYARB_RUNTIME_RECOVERY_MODE": "observe-only",
        "POLYARB_DB_POOL_MAX_SIZE": "1",
    }
    baseline_config["init"] = {
        "cmd": [
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "runtime-reconcile-serve",
            "--enable",
            "--interval-seconds",
            "30",
            "--json",
        ]
    }
    maintenance_payload, _ = render_runtime_recovery_maintenance_payload(
        current_machine=baseline,
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_type="circuit",
        target_id="structure:window:normalize:event_tags:177",
        expected_action="probe-circuit",
        controller_id="maintenance-a",
    )
    maintenance = {
        "id": baseline["id"],
        "instance_id": "01M14S8MAINTENANCE000000000",
        "region": baseline["region"],
        "state": "stopped",
        "config": deepcopy(maintenance_payload["config"]),
    }

    restore, proof = render_runtime_recovery_restore_payload(
        baseline_machine=baseline,
        maintenance_machine=maintenance,
        maintenance_payload=maintenance_payload,
        expected_machine_id="6e82036dce4958",
    )

    assert restore["current_version"] == maintenance["instance_id"]
    assert restore["config"] == baseline["config"]
    assert proof["status"] == "restore-rendered"
    assert proof["baseline_version"] == baseline["instance_id"]
    assert proof["maintenance_version"] == maintenance["instance_id"]

    drifted = deepcopy(maintenance)
    drifted["config"]["guest"]["memory_mb"] = 512
    with pytest.raises(FlyMachineUpdateContractError, match="maintenance Machine config"):
        render_runtime_recovery_restore_payload(
            baseline_machine=baseline,
            maintenance_machine=drifted,
            maintenance_payload=maintenance_payload,
            expected_machine_id="6e82036dce4958",
        )


def test_runtime_maintenance_outcome_requires_new_succeeded_exact_action() -> None:
    from polyarb.control_plane.fly_machine_update import (
        FlyMachineUpdateContractError,
        render_runtime_recovery_maintenance_payload,
        verify_runtime_recovery_maintenance_outcome,
    )

    baseline = _machine()
    config = baseline["config"]
    assert isinstance(config, dict)
    config["env"] = {"POLYARB_RUNTIME_RECOVERY_MODE": "observe-only"}
    config["init"] = {"cmd": list((
        "python", "-m", "polyarb.cli_control_plane", "runtime-reconcile-serve",
        "--enable", "--interval-seconds", "30", "--json",
    ))}
    payload, _ = render_runtime_recovery_maintenance_payload(
        current_machine=baseline,
        expected_app="polyarb-runtime-controller-m1",
        expected_machine_id="6e82036dce4958",
        target_type="circuit",
        target_id="structure:window:normalize:event_tags:177",
        expected_action="probe-circuit",
        controller_id="maintenance-unique-a",
    )
    old = {
        "recovery_actions": {"items": [{
            "action_id": "old-action", "target_id": "structure:window:normalize:event_tags:177",
            "action_type": "probe-circuit", "state": "completed", "result_code": "succeeded",
        }]}
    }
    new_action = {
        "action_id": "new-action", "target_id": "structure:window:normalize:event_tags:177",
        "action_type": "probe-circuit", "state": "completed", "result_code": "succeeded",
    }

    proof = verify_runtime_recovery_maintenance_outcome(
        before_status=old,
        after_status={
            "recovery_actions": {
                "items": [new_action, *old["recovery_actions"]["items"]]
            }
        },
        maintenance_payload=payload,
    )
    assert proof == {
        "status": "maintenance-outcome-verified",
        "action_id": "new-action",
        "action_type": "probe-circuit",
        "target_id_sha256": proof["target_id_sha256"],
        "result_code": "succeeded",
    }

    with pytest.raises(FlyMachineUpdateContractError, match="new succeeded exact action"):
        verify_runtime_recovery_maintenance_outcome(
            before_status=old,
            after_status=old,
            maintenance_payload=payload,
        )
