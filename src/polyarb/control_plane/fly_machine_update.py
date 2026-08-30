"""Fail-closed Fly TOML to Machines API update translation.

Fly's application config names the lifecycle fields ``kill_signal`` and
``kill_timeout``.  The Machines API represents the same contract as
``config.stop_config.signal`` and ``config.stop_config.timeout``.  Keeping the
translation here prevents rollout scripts from silently posting unknown JSON
fields while preserving the fresh remote Machine config verbatim.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tomllib
from collections.abc import Mapping, Sequence
from copy import deepcopy
from pathlib import Path
from typing import Any


class FlyMachineUpdateContractError(ValueError):
    """A local artifact cannot produce an unambiguous safe Machine update."""


_FORMAL_KILL_SIGNAL = "SIGTERM"
_FORMAL_KILL_TIMEOUT_SECONDS = 40
_ALLOWED_ENV_UPDATES = frozenset(
    {
        "POLYARB_DB_POOL_MAX_SIZE",
        "POLYARB_QUALIFICATION_CONFIG_ID",
        "POLYARB_QUALIFICATION_RELEASE_ID",
    }
)
_RUNTIME_CONTROLLER_APP_PREFIX = "polyarb-runtime-controller-"
_RUNTIME_DAEMON_COMMAND = (
    "python",
    "-m",
    "polyarb.cli_control_plane",
    "runtime-reconcile-serve",
    "--enable",
    "--interval-seconds",
    "30",
    "--json",
)
_RUNTIME_RECOVERY_ACTIONS = frozenset(
    {"restart-worker", "reclaim-job", "retry-job", "probe-circuit", "page-operator"}
)


def render_machine_update_payload(
    *,
    current_machine: Mapping[str, object],
    fly_config_path: Path,
    expected_app: str,
    expected_machine_id: str,
    target_image: str,
    update_env_from_fly: Sequence[str] = (),
    target_cpu_kind: str | None = None,
    target_cpus: int | None = None,
    target_memory_mb: int | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a full optimistic Machines API update and redacted proof.

    ``current_machine`` must be a fresh GET response.  Every existing config
    field is copied; only the image and lifecycle stop contract may change by
    default.  A complete, explicit guest triple permits one auditable resource
    shape change without opening an arbitrary config-patch surface.
    """
    if not expected_app or not expected_machine_id or not target_image:
        raise FlyMachineUpdateContractError(
            "expected app, Machine ID and target image must be non-empty"
        )
    machine_id = _required_string(current_machine, "id", "Machine response")
    if machine_id != expected_machine_id:
        raise FlyMachineUpdateContractError("fresh Machine response has the wrong ID")
    current_version = _required_string(current_machine, "instance_id", "Machine response")
    current_config = _required_mapping(current_machine, "config", "Machine response")

    with fly_config_path.open("rb") as handle:
        fly_config = tomllib.load(handle)
    if fly_config.get("app") != expected_app:
        raise FlyMachineUpdateContractError("rendered Fly config has the wrong app identity")
    if fly_config.get("kill_signal") != _FORMAL_KILL_SIGNAL:
        raise FlyMachineUpdateContractError("formal Fly config must declare SIGTERM")
    if fly_config.get("kill_timeout") != _FORMAL_KILL_TIMEOUT_SECONDS:
        raise FlyMachineUpdateContractError("formal Fly config must declare a 40-second drain")

    updated_env_keys = tuple(sorted(set(update_env_from_fly)))
    if len(updated_env_keys) != len(update_env_from_fly):
        raise FlyMachineUpdateContractError("Machine env update keys must be unique")
    if any(key not in _ALLOWED_ENV_UPDATES for key in updated_env_keys):
        raise FlyMachineUpdateContractError("Machine env update key is outside rollout policy")

    guest_values = (target_cpu_kind, target_cpus, target_memory_mb)
    guest_change_requested = any(value is not None for value in guest_values)
    if guest_change_requested and not all(value is not None for value in guest_values):
        raise FlyMachineUpdateContractError("resource shape update requires all guest fields")
    if guest_change_requested and (
        not isinstance(target_cpu_kind, str)
        or not target_cpu_kind
        or type(target_cpus) is not int
        or target_cpus <= 0
        or type(target_memory_mb) is not int
        or target_memory_mb <= 0
    ):
        raise FlyMachineUpdateContractError("resource shape fields must be non-empty and positive")

    candidate_config: dict[str, Any] = deepcopy(dict(current_config))
    candidate_config.pop("kill_signal", None)
    candidate_config.pop("kill_timeout", None)
    candidate_config["image"] = target_image
    candidate_config["stop_config"] = {
        "signal": _FORMAL_KILL_SIGNAL,
        "timeout": f"{_FORMAL_KILL_TIMEOUT_SECONDS}s",
    }
    if updated_env_keys:
        current_env = dict(_required_mapping(candidate_config, "env", "Machine config"))
        rendered_env = _required_mapping(fly_config, "env", "rendered Fly config")
        for key in updated_env_keys:
            current_env[key] = _required_string(rendered_env, key, "rendered Fly config env")
        candidate_config["env"] = current_env
    resource_change: dict[str, object] | None = None
    if guest_change_requested:
        current_guest = dict(_required_mapping(candidate_config, "guest", "Machine config"))
        expected_guest_keys = {"cpu_kind", "cpus", "memory_mb"}
        if set(current_guest) != expected_guest_keys:
            raise FlyMachineUpdateContractError(
                "resource shape update requires the canonical three-field guest config"
            )
        target_guest = {
            "cpu_kind": target_cpu_kind,
            "cpus": target_cpus,
            "memory_mb": target_memory_mb,
        }
        candidate_config["guest"] = target_guest
        resource_change = {"from": current_guest, "to": target_guest}

    preserved_current = deepcopy(dict(current_config))
    preserved_current.pop("image", None)
    preserved_current.pop("stop_config", None)
    preserved_current.pop("kill_signal", None)
    preserved_current.pop("kill_timeout", None)
    preserved_candidate = deepcopy(candidate_config)
    preserved_candidate.pop("image", None)
    preserved_candidate.pop("stop_config", None)
    if guest_change_requested:
        preserved_current.pop("guest", None)
        preserved_candidate.pop("guest", None)
    for preserved in (preserved_current, preserved_candidate):
        if updated_env_keys:
            preserved_env = dict(_required_mapping(preserved, "env", "preserved config"))
            for key in updated_env_keys:
                preserved_env.pop(key, None)
            preserved["env"] = preserved_env
    if preserved_candidate != preserved_current:
        raise FlyMachineUpdateContractError("candidate changed non-release Machine config")
    preserved_hash = hashlib.sha256(
        json.dumps(
            preserved_candidate,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
    ).hexdigest()

    payload: dict[str, Any] = {
        "config": candidate_config,
        "current_version": current_version,
    }
    proof: dict[str, Any] = {
        "app": expected_app,
        "machine_id": machine_id,
        "current_version": current_version,
        "kill_signal": _FORMAL_KILL_SIGNAL,
        "kill_timeout_seconds": _FORMAL_KILL_TIMEOUT_SECONDS,
        "preserved_config_sha256": preserved_hash,
        "target_image": target_image,
        "updated_env_keys": list(updated_env_keys),
    }
    if resource_change is not None:
        proof["resource_change"] = resource_change
    return payload, proof


def verify_machine_update_response(
    *,
    updated_machine: Mapping[str, object],
    update_payload: Mapping[str, object],
    expected_machine_id: str,
    expected_region: str,
) -> dict[str, Any]:
    """Verify one fresh Machine GET against the exact rendered update."""
    machine_id = _required_string(updated_machine, "id", "updated Machine response")
    if machine_id != expected_machine_id:
        raise FlyMachineUpdateContractError("updated Machine response has the wrong ID")
    if _required_string(updated_machine, "region", "updated Machine response") != expected_region:
        raise FlyMachineUpdateContractError("updated Machine response changed region")
    if updated_machine.get("state") != "started":
        raise FlyMachineUpdateContractError("updated Machine is not started")
    old_version = _required_string(update_payload, "current_version", "update payload")
    new_version = _required_string(updated_machine, "instance_id", "updated Machine response")
    if new_version == old_version:
        raise FlyMachineUpdateContractError("Machine update did not create a new version")

    expected_config = dict(_required_mapping(update_payload, "config", "update payload"))
    actual_config = dict(_required_mapping(updated_machine, "config", "updated Machine response"))
    expected_image = _required_string(expected_config, "image", "update config")
    actual_image = _required_string(actual_config, "image", "updated Machine config")
    if actual_image != expected_image and not actual_image.startswith(f"{expected_image}@sha256:"):
        raise FlyMachineUpdateContractError("updated Machine resolved an unexpected image")
    expected_config.pop("image")
    actual_config.pop("image")
    if actual_config != expected_config:
        raise FlyMachineUpdateContractError("updated Machine config differs from rendered payload")
    stop_config = _required_mapping(actual_config, "stop_config", "updated Machine config")
    if stop_config != {"signal": _FORMAL_KILL_SIGNAL, "timeout": "40s"}:
        raise FlyMachineUpdateContractError("updated Machine lacks the formal stop contract")
    return {
        "status": "verified",
        "machine_id": machine_id,
        "old_version": old_version,
        "new_version": new_version,
        "region": expected_region,
        "kill_signal": _FORMAL_KILL_SIGNAL,
        "kill_timeout_seconds": _FORMAL_KILL_TIMEOUT_SECONDS,
        "resolved_image": actual_image,
    }


def render_runtime_recovery_maintenance_payload(
    *,
    current_machine: Mapping[str, object],
    expected_app: str,
    expected_machine_id: str,
    target_type: str,
    target_id: str,
    expected_action: str,
    controller_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Replace the resident controller with one exact recovery process.

    This is deliberately a Machine *replacement*, not an SSH command.  A
    256-MB controller cannot safely host a second Python interpreter beside
    the resident daemon.  The returned config keeps the same VM shape and
    authority, disables restart, and executes at most one fenced action.
    """
    if not expected_app.startswith(_RUNTIME_CONTROLLER_APP_PREFIX):
        raise FlyMachineUpdateContractError("maintenance requires a runtime controller app")
    machine_id = _required_string(current_machine, "id", "Machine response")
    if machine_id != expected_machine_id:
        raise FlyMachineUpdateContractError("fresh Machine response has the wrong ID")
    current_version = _required_string(current_machine, "instance_id", "Machine response")
    current_config = _required_mapping(current_machine, "config", "Machine response")
    current_env = _required_mapping(current_config, "env", "Machine config")
    if current_env.get("POLYARB_RUNTIME_RECOVERY_MODE") != "observe-only":
        raise FlyMachineUpdateContractError(
            "runtime maintenance baseline must be observe-only"
        )
    current_init = _required_mapping(current_config, "init", "Machine config")
    current_command = current_init.get("cmd")
    if (
        tuple(current_command) if isinstance(current_command, list) else ()
    ) != _RUNTIME_DAEMON_COMMAND:
        raise FlyMachineUpdateContractError(
            "runtime maintenance baseline has an unexpected resident command"
        )
    if current_config.get("restart") != {"policy": "always"}:
        raise FlyMachineUpdateContractError(
            "runtime maintenance baseline must use always restart policy"
        )
    if target_type not in {"job", "circuit"}:
        raise FlyMachineUpdateContractError("runtime recovery target type is invalid")
    for owner, value in (
        ("target ID", target_id),
        ("controller ID", controller_id),
    ):
        if not value or len(value) > 512 or any(ord(character) < 32 for character in value):
            raise FlyMachineUpdateContractError(f"runtime recovery {owner} is invalid")
    if expected_action not in _RUNTIME_RECOVERY_ACTIONS:
        raise FlyMachineUpdateContractError("runtime recovery action is invalid")

    candidate_config: dict[str, Any] = deepcopy(dict(current_config))
    candidate_env = dict(current_env)
    candidate_env["POLYARB_RUNTIME_RECOVERY_MODE"] = "execute"
    candidate_config["env"] = candidate_env
    candidate_config["init"] = {
        "cmd": [
            "python",
            "-m",
            "polyarb.cli_control_plane",
            "runtime-reconcile-until",
            "--enable",
            "--controller-id",
            controller_id,
            "--owner-id",
            controller_id,
            "--target-type",
            target_type,
            "--target-id",
            target_id,
            "--expected-action",
            expected_action,
            "--max-wait-seconds",
            "45",
            "--retry-interval-seconds",
            "1",
            "--json",
        ]
    }
    candidate_config["restart"] = {"policy": "no"}
    payload: dict[str, Any] = {
        "config": candidate_config,
        "current_version": current_version,
    }
    return payload, {
        "status": "maintenance-rendered",
        "app": expected_app,
        "machine_id": machine_id,
        "current_version": current_version,
        "execution_model": "replace-resident-process",
        "target_type": target_type,
        "target_id_sha256": hashlib.sha256(target_id.encode()).hexdigest(),
        "expected_action": expected_action,
        "restore_required": True,
        "candidate_config_sha256": _mapping_sha256(candidate_config),
    }


def render_runtime_recovery_restore_payload(
    *,
    baseline_machine: Mapping[str, object],
    maintenance_machine: Mapping[str, object],
    maintenance_payload: Mapping[str, object],
    expected_machine_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Restore an exact saved controller config after one maintenance turn."""
    baseline_id = _required_string(baseline_machine, "id", "baseline Machine")
    maintenance_id = _required_string(maintenance_machine, "id", "maintenance Machine")
    if baseline_id != expected_machine_id or maintenance_id != expected_machine_id:
        raise FlyMachineUpdateContractError("runtime restore Machine ID changed")
    if baseline_machine.get("region") != maintenance_machine.get("region"):
        raise FlyMachineUpdateContractError("runtime restore Machine region changed")
    baseline_version = _required_string(
        baseline_machine, "instance_id", "baseline Machine"
    )
    maintenance_version = _required_string(
        maintenance_machine, "instance_id", "maintenance Machine"
    )
    if maintenance_version == baseline_version:
        raise FlyMachineUpdateContractError("runtime maintenance did not create a new version")
    expected_maintenance_config = dict(
        _required_mapping(maintenance_payload, "config", "maintenance payload")
    )
    actual_maintenance_config = dict(
        _required_mapping(maintenance_machine, "config", "maintenance Machine")
    )
    if not _configs_match_allowing_image_resolution(
        expected_maintenance_config, actual_maintenance_config
    ):
        raise FlyMachineUpdateContractError(
            "maintenance Machine config differs from rendered payload"
        )
    if maintenance_machine.get("state") not in {"stopped", "started"}:
        raise FlyMachineUpdateContractError("maintenance Machine has an invalid state")
    baseline_config = deepcopy(
        dict(_required_mapping(baseline_machine, "config", "baseline Machine"))
    )
    baseline_env = _required_mapping(baseline_config, "env", "baseline Machine config")
    if baseline_env.get("POLYARB_RUNTIME_RECOVERY_MODE") != "observe-only":
        raise FlyMachineUpdateContractError("runtime restore baseline must be observe-only")
    baseline_init = _required_mapping(baseline_config, "init", "baseline Machine config")
    baseline_command = baseline_init.get("cmd")
    if (
        tuple(baseline_command) if isinstance(baseline_command, list) else ()
    ) != _RUNTIME_DAEMON_COMMAND:
        raise FlyMachineUpdateContractError("runtime restore baseline command is invalid")
    if baseline_config.get("restart") != {"policy": "always"}:
        raise FlyMachineUpdateContractError("runtime restore baseline restart policy is invalid")
    payload: dict[str, Any] = {
        "config": baseline_config,
        "current_version": maintenance_version,
    }
    return payload, {
        "status": "restore-rendered",
        "machine_id": expected_machine_id,
        "baseline_version": baseline_version,
        "maintenance_version": maintenance_version,
        "baseline_config_sha256": _mapping_sha256(baseline_config),
    }


def verify_runtime_recovery_maintenance_outcome(
    *,
    before_status: Mapping[str, object],
    after_status: Mapping[str, object],
    maintenance_payload: Mapping[str, object],
) -> dict[str, Any]:
    """Require a new completed exact action; process exit alone is not success."""
    config = _required_mapping(maintenance_payload, "config", "maintenance payload")
    init = _required_mapping(config, "init", "maintenance config")
    raw_command = init.get("cmd")
    if not isinstance(raw_command, list) or any(
        not isinstance(argument, str) for argument in raw_command
    ):
        raise FlyMachineUpdateContractError("maintenance command is invalid")
    command = tuple(raw_command)
    if command[:4] != (
        "python",
        "-m",
        "polyarb.cli_control_plane",
        "runtime-reconcile-until",
    ):
        raise FlyMachineUpdateContractError("maintenance command is not runtime recovery")
    target_id = _command_option(command, "--target-id")
    action_type = _command_option(command, "--expected-action")
    before_ids = {
        _required_string(action, "action_id", "recovery action")
        for action in _status_recovery_actions(before_status)
    }
    matches = [
        action
        for action in _status_recovery_actions(after_status)
        if action.get("action_id") not in before_ids
        and action.get("target_id") == target_id
        and action.get("action_type") == action_type
        and action.get("state") == "completed"
        and action.get("result_code") == "succeeded"
    ]
    if len(matches) != 1:
        raise FlyMachineUpdateContractError(
            "runtime maintenance produced no unique new succeeded exact action"
        )
    action_id = _required_string(matches[0], "action_id", "recovery action")
    return {
        "status": "maintenance-outcome-verified",
        "action_id": action_id,
        "action_type": action_type,
        "target_id_sha256": hashlib.sha256(target_id.encode()).hexdigest(),
        "result_code": "succeeded",
    }


def _command_option(command: Sequence[str], option: str) -> str:
    if command.count(option) != 1:
        raise FlyMachineUpdateContractError(f"maintenance command requires one {option}")
    index = command.index(option)
    if index + 1 >= len(command) or not command[index + 1]:
        raise FlyMachineUpdateContractError(f"maintenance command requires a value for {option}")
    return command[index + 1]


def _status_recovery_actions(status: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    recovery_actions = _required_mapping(status, "recovery_actions", "control-plane status")
    items = recovery_actions.get("items")
    if not isinstance(items, list) or any(not isinstance(item, Mapping) for item in items):
        raise FlyMachineUpdateContractError("control-plane recovery actions must be a list")
    return tuple(items)


def _configs_match_allowing_image_resolution(
    expected: Mapping[str, object], actual: Mapping[str, object]
) -> bool:
    expected_copy = deepcopy(dict(expected))
    actual_copy = deepcopy(dict(actual))
    expected_image = expected_copy.pop("image", None)
    actual_image = actual_copy.pop("image", None)
    if not isinstance(expected_image, str) or not isinstance(actual_image, str):
        return False
    image_matches = actual_image == expected_image or actual_image.startswith(
        f"{expected_image}@sha256:"
    )
    return image_matches and actual_copy == expected_copy


def _mapping_sha256(value: Mapping[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _required_string(source: Mapping[str, object], key: str, owner: str) -> str:
    value = source.get(key)
    if not isinstance(value, str) or not value:
        raise FlyMachineUpdateContractError(f"{owner} requires non-empty {key}")
    return value


def _required_mapping(source: Mapping[str, object], key: str, owner: str) -> Mapping[str, object]:
    value = source.get(key)
    if not isinstance(value, Mapping):
        raise FlyMachineUpdateContractError(f"{owner} requires object {key}")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subcommands = parser.add_subparsers(dest="command", required=True)
    render = subcommands.add_parser("render", help="render one full Machines API update payload")
    render.add_argument("--current-machine", type=Path, required=True)
    render.add_argument("--fly-config", type=Path, required=True)
    render.add_argument("--expected-app", required=True)
    render.add_argument("--expected-machine-id", required=True)
    render.add_argument("--target-image", required=True)
    render.add_argument("--update-env-from-fly", action="append", default=[])
    render.add_argument("--target-cpu-kind")
    render.add_argument("--target-cpus", type=int)
    render.add_argument("--target-memory-mb", type=int)
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--json", action="store_true")
    maintenance = subcommands.add_parser(
        "render-runtime-maintenance",
        help="render one exact process-replacement runtime recovery update",
    )
    maintenance.add_argument("--current-machine", type=Path, required=True)
    maintenance.add_argument("--expected-app", required=True)
    maintenance.add_argument("--expected-machine-id", required=True)
    maintenance.add_argument("--target-type", choices=("job", "circuit"), required=True)
    maintenance.add_argument("--target-id", required=True)
    maintenance.add_argument("--expected-action", required=True)
    maintenance.add_argument("--controller-id", required=True)
    maintenance.add_argument("--output", type=Path, required=True)
    maintenance.add_argument("--json", action="store_true")
    restore = subcommands.add_parser(
        "render-runtime-restore",
        help="render an exact controller restore after runtime maintenance",
    )
    restore.add_argument("--baseline-machine", type=Path, required=True)
    restore.add_argument("--maintenance-machine", type=Path, required=True)
    restore.add_argument("--maintenance-payload", type=Path, required=True)
    restore.add_argument("--expected-machine-id", required=True)
    restore.add_argument("--output", type=Path, required=True)
    restore.add_argument("--json", action="store_true")
    outcome = subcommands.add_parser(
        "verify-runtime-maintenance-outcome",
        help="verify that maintenance created one new succeeded exact action",
    )
    outcome.add_argument("--before-status", type=Path, required=True)
    outcome.add_argument("--after-status", type=Path, required=True)
    outcome.add_argument("--maintenance-payload", type=Path, required=True)
    outcome.add_argument("--json", action="store_true")
    verify = subcommands.add_parser(
        "verify", help="verify a fresh Machine GET against one rendered update payload"
    )
    verify.add_argument("--updated-machine", type=Path, required=True)
    verify.add_argument("--update-payload", type=Path, required=True)
    verify.add_argument("--expected-machine-id", required=True)
    verify.add_argument("--expected-region", required=True)
    verify.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "render":
        current_machine = _read_json_mapping(args.current_machine, "current Machine artifact")
        payload, proof = render_machine_update_payload(
            current_machine=current_machine,
            fly_config_path=args.fly_config,
            expected_app=args.expected_app,
            expected_machine_id=args.expected_machine_id,
            target_image=args.target_image,
            update_env_from_fly=args.update_env_from_fly,
            target_cpu_kind=args.target_cpu_kind,
            target_cpus=args.target_cpus,
            target_memory_mb=args.target_memory_mb,
        )
        with args.output.open("x") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
        result = {"status": "rendered", "output": str(args.output), **proof}
    elif args.command == "render-runtime-maintenance":
        payload, proof = render_runtime_recovery_maintenance_payload(
            current_machine=_read_json_mapping(
                args.current_machine, "current Machine artifact"
            ),
            expected_app=args.expected_app,
            expected_machine_id=args.expected_machine_id,
            target_type=args.target_type,
            target_id=args.target_id,
            expected_action=args.expected_action,
            controller_id=args.controller_id,
        )
        _write_new_json(args.output, payload)
        result = {"output": str(args.output), **proof}
    elif args.command == "render-runtime-restore":
        payload, proof = render_runtime_recovery_restore_payload(
            baseline_machine=_read_json_mapping(
                args.baseline_machine, "baseline Machine artifact"
            ),
            maintenance_machine=_read_json_mapping(
                args.maintenance_machine, "maintenance Machine artifact"
            ),
            maintenance_payload=_read_json_mapping(
                args.maintenance_payload, "maintenance payload artifact"
            ),
            expected_machine_id=args.expected_machine_id,
        )
        _write_new_json(args.output, payload)
        result = {"output": str(args.output), **proof}
    elif args.command == "verify-runtime-maintenance-outcome":
        result = verify_runtime_recovery_maintenance_outcome(
            before_status=_read_json_mapping(args.before_status, "before status artifact"),
            after_status=_read_json_mapping(args.after_status, "after status artifact"),
            maintenance_payload=_read_json_mapping(
                args.maintenance_payload, "maintenance payload artifact"
            ),
        )
    elif args.command == "verify":
        result = verify_machine_update_response(
            updated_machine=_read_json_mapping(args.updated_machine, "updated Machine artifact"),
            update_payload=_read_json_mapping(args.update_payload, "update payload artifact"),
            expected_machine_id=args.expected_machine_id,
            expected_region=args.expected_region,
        )
    else:  # pragma: no cover - argparse owns the command set
        raise AssertionError(f"unsupported command: {args.command}")
    if args.json:
        print(json.dumps(result, sort_keys=True))
    elif args.command == "render":
        print(f"rendered Machines API update: {args.output}")
    else:
        print(f"verified Machines API update: {args.expected_machine_id}")
    return 0


def _read_json_mapping(path: Path, owner: str) -> Mapping[str, object]:
    payload = json.loads(path.read_text())
    if not isinstance(payload, Mapping):
        raise FlyMachineUpdateContractError(f"{owner} must be a JSON object")
    return payload


def _write_new_json(path: Path, payload: Mapping[str, object]) -> None:
    with path.open("x") as handle:
        json.dump(payload, handle, sort_keys=True, indent=2)
        handle.write("\n")


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
