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


def render_machine_update_payload(
    *,
    current_machine: Mapping[str, object],
    fly_config_path: Path,
    expected_app: str,
    expected_machine_id: str,
    target_image: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a full optimistic Machines API update and redacted proof.

    ``current_machine`` must be a fresh GET response.  Every existing config
    field is copied; only the image and lifecycle stop contract may change.
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

    candidate_config: dict[str, Any] = deepcopy(dict(current_config))
    candidate_config.pop("kill_signal", None)
    candidate_config.pop("kill_timeout", None)
    candidate_config["image"] = target_image
    candidate_config["stop_config"] = {
        "signal": _FORMAL_KILL_SIGNAL,
        "timeout": f"{_FORMAL_KILL_TIMEOUT_SECONDS}s",
    }

    preserved_current = deepcopy(dict(current_config))
    preserved_current.pop("image", None)
    preserved_current.pop("stop_config", None)
    preserved_current.pop("kill_signal", None)
    preserved_current.pop("kill_timeout", None)
    preserved_candidate = deepcopy(candidate_config)
    preserved_candidate.pop("image", None)
    preserved_candidate.pop("stop_config", None)
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
    }
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
    render.add_argument("--output", type=Path, required=True)
    render.add_argument("--json", action="store_true")
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
        )
        with args.output.open("x") as handle:
            json.dump(payload, handle, sort_keys=True, indent=2)
            handle.write("\n")
        result = {"status": "rendered", "output": str(args.output), **proof}
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
