"""Fail-closed, credential-free Fly topology and secret-name audit."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from typing import NoReturn

FlyRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]

_APP_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")
_MACHINE_RE = re.compile(r"^[a-f0-9]{14}$")
_SAFE_VALUE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._/@:+-]{0,511}$")
_ENV_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]{0,127}$")
_CREDENTIAL_KEY_RE = re.compile(
    r"(?:^|_)(?:DSN|PASSWORD|PASSWD|TOKEN|SECRET|PRIVATE_KEY|API_KEY|ACCESS_KEY)(?:_|$)"
)


class FlyTopologyAuditError(RuntimeError):
    """Sanitized audit refusal containing no provider detail."""

    def __init__(self, reason_code: str, app: str, object_identifier: str) -> None:
        self.reason_code = reason_code
        self.app = app
        self.object_identifier = object_identifier
        super().__init__(f"{reason_code}: {app}:{object_identifier}")

    def as_mapping(self) -> Mapping[str, object]:
        return {
            "status": "fail",
            "reason_code": self.reason_code,
            "app": self.app,
            "object": self.object_identifier,
        }


def run_flyctl_read_only(argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
    """Capture one allowlisted Fly read without leaking its raw output."""

    command = tuple(argv)
    allowed = (
        len(command) == 5
        and command[:2] == ("flyctl", "status")
        and command[2] == "-a"
        and command[4:] == ("--json",)
    ) or (
        len(command) == 6
        and command[:3] == ("flyctl", "secrets", "list")
        and command[3] == "-a"
        and command[5:] == ("--json",)
    )
    if not allowed:
        raise FlyTopologyAuditError(
            "fly-topology-audit.command-not-allowlisted",
            "local",
            "flyctl",
        )
    child_env = os.environ.copy()
    child_env["FLY_API_TOKEN"] = ""
    return subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
        env=child_env,
    )


def audit_fly_topology(
    *,
    targets: Sequence[str],
    required_secrets: Sequence[str],
    runner: FlyRunner = run_flyctl_read_only,
) -> Mapping[str, object]:
    expected = _parse_scoped_values(targets, value_name="machine")
    required = _parse_scoped_values(required_secrets, value_name="secret", allow_empty=True)
    if not expected:
        _fail("fly-topology-audit.target-missing", "local", "target")
    if set(required) - set(expected):
        _fail("fly-topology-audit.secret-app-not-targeted", "local", "required-secret")

    apps: list[Mapping[str, object]] = []
    for app in sorted(expected):
        status_payload = _provider_json(
            runner,
            ("flyctl", "status", "-a", app, "--json"),
            app=app,
            operation="status",
        )
        machines = _parse_exact_machines(
            status_payload,
            app=app,
            expected_ids=frozenset(expected[app]),
        )
        presence: dict[str, bool] = {}
        if required.get(app):
            secrets_payload = _provider_json(
                runner,
                ("flyctl", "secrets", "list", "-a", app, "--json"),
                app=app,
                operation="secrets-list",
            )
            provider_secret_names = _parse_secret_names(secrets_payload, app=app)
            for secret_name in sorted(required[app]):
                present = secret_name in provider_secret_names
                presence[secret_name] = present
                if not present:
                    _fail(
                        "fly-topology-audit.required-secret-missing",
                        app,
                        secret_name,
                    )
        apps.append(
            {
                "app": app,
                "machines": machines,
                "required_secret_presence": presence,
            }
        )
    return {"status": "pass", "apps": apps}


def _parse_scoped_values(
    values: Sequence[str],
    *,
    value_name: str,
    allow_empty: bool = False,
) -> dict[str, set[str]]:
    parsed: dict[str, set[str]] = defaultdict(set)
    for item in values:
        if item.count("/") != 1:
            _fail("fly-topology-audit.argument-invalid", "local", value_name)
        app, value = item.split("/", 1)
        if not _APP_RE.fullmatch(app):
            _fail("fly-topology-audit.argument-invalid", "local", value_name)
        valid_value = (
            bool(_MACHINE_RE.fullmatch(value))
            if value_name == "machine"
            else bool(_ENV_KEY_RE.fullmatch(value))
        )
        if not valid_value or value in parsed[app]:
            _fail("fly-topology-audit.argument-invalid", app, value_name)
        parsed[app].add(value)
    if not values and not allow_empty:
        _fail("fly-topology-audit.argument-invalid", "local", value_name)
    return dict(parsed)


def _provider_json(
    runner: FlyRunner,
    argv: Sequence[str],
    *,
    app: str,
    operation: str,
) -> object:
    try:
        completed = runner(argv)
    except FlyTopologyAuditError:
        raise
    except Exception:
        _fail("fly-topology-audit.provider-failure", app, operation)
    if completed.returncode != 0:
        _fail("fly-topology-audit.provider-failure", app, operation)
    try:
        return json.loads(completed.stdout)
    except (TypeError, json.JSONDecodeError):
        _fail("fly-topology-audit.provider-shape-invalid", app, operation)


def _parse_exact_machines(
    payload: object,
    *,
    app: str,
    expected_ids: frozenset[str],
) -> list[Mapping[str, object]]:
    if not isinstance(payload, Mapping):
        _fail("fly-topology-audit.provider-shape-invalid", app, "status")
    provider_app = payload.get("Name", payload.get("name"))
    if provider_app is not None and provider_app != app:
        _fail("fly-topology-audit.topology-mismatch", app, "app")
    raw_machines = payload.get("Machines", payload.get("machines"))
    if not isinstance(raw_machines, list):
        _fail("fly-topology-audit.provider-shape-invalid", app, "status")
    raw_ids = {
        machine.get("id")
        for machine in raw_machines
        if isinstance(machine, Mapping) and isinstance(machine.get("id"), str)
    }
    if raw_ids != expected_ids or len(raw_machines) != len(expected_ids):
        _fail("fly-topology-audit.topology-mismatch", app, "machine-set")

    machines: list[Mapping[str, object]] = []
    for machine in raw_machines:
        if not isinstance(machine, Mapping):
            _fail("fly-topology-audit.provider-shape-invalid", app, "machine")
        machine_id = machine.get("id")
        state = machine.get("state")
        config = machine.get("config")
        if (
            not isinstance(machine_id, str)
            or not _MACHINE_RE.fullmatch(machine_id)
            or not isinstance(state, str)
            or not _SAFE_VALUE_RE.fullmatch(state)
            or not isinstance(config, Mapping)
        ):
            _fail("fly-topology-audit.provider-shape-invalid", app, "machine")
        image = config.get("image")
        metadata = config.get("metadata")
        env = config.get("env", {})
        process_group = (
            metadata.get("fly_process_group") if isinstance(metadata, Mapping) else None
        )
        if (
            not isinstance(image, str)
            or not _SAFE_VALUE_RE.fullmatch(image)
            or not isinstance(process_group, str)
            or not _SAFE_VALUE_RE.fullmatch(process_group)
            or not isinstance(env, Mapping)
        ):
            _fail("fly-topology-audit.provider-shape-invalid", app, "machine-config")
        env_keys: list[str] = []
        for key in env:
            if not isinstance(key, str) or not _ENV_KEY_RE.fullmatch(key):
                _fail("fly-topology-audit.provider-shape-invalid", app, "env-key")
            if _CREDENTIAL_KEY_RE.search(key):
                _fail(
                    "fly-topology-audit.credential-in-ordinary-env",
                    app,
                    key,
                )
            env_keys.append(key)
        machines.append(
            {
                "machine_id": machine_id,
                "state": state,
                "image": image,
                "process_group": process_group,
                "env_keys": sorted(env_keys),
            }
        )
    return sorted(machines, key=lambda item: str(item["machine_id"]))


def _parse_secret_names(payload: object, *, app: str) -> frozenset[str]:
    rows = payload
    if isinstance(payload, Mapping):
        rows = payload.get("Secrets", payload.get("secrets"))
    if not isinstance(rows, list):
        _fail("fly-topology-audit.provider-shape-invalid", app, "secrets-list")
    names: set[str] = set()
    for row in rows:
        if not isinstance(row, Mapping):
            _fail("fly-topology-audit.provider-shape-invalid", app, "secrets-list")
        name = row.get("Name", row.get("name"))
        if not isinstance(name, str) or not _ENV_KEY_RE.fullmatch(name):
            _fail("fly-topology-audit.provider-shape-invalid", app, "secret-name")
        names.add(name)
    return frozenset(names)


def _fail(reason_code: str, app: str, object_identifier: str) -> NoReturn:
    raise FlyTopologyAuditError(reason_code, app, object_identifier)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", action="append", required=True)
    parser.add_argument("--required-secret", action="append", default=[])
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = audit_fly_topology(
            targets=tuple(args.target),
            required_secrets=tuple(args.required_secret),
        )
    except FlyTopologyAuditError as exc:
        print(json.dumps(exc.as_mapping(), sort_keys=True), file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


__all__ = [
    "FlyTopologyAuditError",
    "audit_fly_topology",
    "main",
    "run_flyctl_read_only",
]


if __name__ == "__main__":
    raise SystemExit(main())
