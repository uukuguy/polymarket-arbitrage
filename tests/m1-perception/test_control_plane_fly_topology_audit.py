"""Credential-free contracts for the allowlisted Fly topology audit."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Sequence

import pytest


class FakeFlyRunner:
    def __init__(
        self,
        *,
        status_payload: object,
        secrets_payload: object | None = None,
        returncode: int = 0,
        stderr: str = "",
    ) -> None:
        self.status_payload = status_payload
        self.secrets_payload = [] if secrets_payload is None else secrets_payload
        self.returncode = returncode
        self.stderr = stderr
        self.calls: list[tuple[str, ...]] = []

    def __call__(self, argv: Sequence[str]) -> subprocess.CompletedProcess[str]:
        call = tuple(argv)
        self.calls.append(call)
        payload = self.secrets_payload if "secrets" in call else self.status_payload
        return subprocess.CompletedProcess(
            call,
            self.returncode,
            stdout=json.dumps(payload),
            stderr=self.stderr,
        )


def _status_payload(*, env: dict[str, str] | None = None) -> dict[str, object]:
    return {
        "Name": "writer-app",
        "Machines": [
            {
                "id": "28654e35a73d08",
                "state": "started",
                "config": {
                    "image": "registry.fly.io/writer@sha256:" + "a" * 64,
                    "metadata": {"fly_process_group": "runtime-event-writer"},
                    "env": env
                    or {
                        "FLY_PROCESS_GROUP": "runtime-event-writer",
                        "POLYARB_RUNTIME_ROLE": "runtime-event-writer",
                        "PRIMARY_REGION": "nrt",
                    },
                },
            }
        ],
    }


def test_fly_topology_audit_emits_only_allowlisted_fields() -> None:
    from polyarb.control_plane.fly_topology_audit import audit_fly_topology

    sentinel = "postgresql://writer:must-never-appear@example.test/postgres"
    payload = _status_payload()
    payload["ignored_provider_body"] = sentinel
    runner = FakeFlyRunner(
        status_payload=payload,
        secrets_payload=[
            {"Name": "POLYARB_SUPABASE_DB_DSN", "Digest": sentinel},
            {"Name": "POLYARB_RUNTIME_EVENT_WRITER_TOKEN", "Digest": "sha256:ignored"},
            {"Name": "UNREQUESTED_SECRET", "Digest": sentinel},
        ],
    )

    result = audit_fly_topology(
        targets=("writer-app/28654e35a73d08",),
        required_secrets=(
            "writer-app/POLYARB_SUPABASE_DB_DSN",
            "writer-app/POLYARB_RUNTIME_EVENT_WRITER_TOKEN",
        ),
        runner=runner,
    )

    rendered = json.dumps(result, sort_keys=True)
    assert result == {
        "status": "pass",
        "apps": [
            {
                "app": "writer-app",
                "machines": [
                    {
                        "machine_id": "28654e35a73d08",
                        "state": "started",
                        "image": "registry.fly.io/writer@sha256:" + "a" * 64,
                        "process_group": "runtime-event-writer",
                        "env_keys": [
                            "FLY_PROCESS_GROUP",
                            "POLYARB_RUNTIME_ROLE",
                            "PRIMARY_REGION",
                        ],
                    }
                ],
                "required_secret_presence": {
                    "POLYARB_RUNTIME_EVENT_WRITER_TOKEN": True,
                    "POLYARB_SUPABASE_DB_DSN": True,
                },
            }
        ],
    }
    assert sentinel not in rendered
    assert "UNREQUESTED_SECRET" not in rendered
    assert runner.calls == [
        ("flyctl", "status", "-a", "writer-app", "--json"),
        ("flyctl", "secrets", "list", "-a", "writer-app", "--json"),
    ]


def test_fly_topology_audit_rejects_password_bearing_ordinary_env_without_value() -> None:
    from polyarb.control_plane.fly_topology_audit import (
        FlyTopologyAuditError,
        audit_fly_topology,
    )

    sentinel = "postgresql://writer:must-never-appear@example.test/postgres"
    runner = FakeFlyRunner(
        status_payload=_status_payload(env={"POLYARB_SUPABASE_DB_DSN_V2": sentinel})
    )

    with pytest.raises(FlyTopologyAuditError) as exc_info:
        audit_fly_topology(
            targets=("writer-app/28654e35a73d08",),
            required_secrets=(),
            runner=runner,
        )

    rendered = json.dumps(exc_info.value.as_mapping(), sort_keys=True)
    assert "fly-topology-audit.credential-in-ordinary-env" in rendered
    assert "POLYARB_SUPABASE_DB_DSN_V2" in rendered
    assert sentinel not in rendered


@pytest.mark.parametrize(
    ("runner", "reason_code"),
    (
        (
            FakeFlyRunner(
                status_payload={},
                returncode=1,
                stderr="provider exploded postgresql://admin:secret@example.test/postgres",
            ),
            "fly-topology-audit.provider-failure",
        ),
        (
            FakeFlyRunner(status_payload={"Machines": "not-a-list"}),
            "fly-topology-audit.provider-shape-invalid",
        ),
    ),
)
def test_fly_topology_audit_sanitizes_provider_failures(
    runner: FakeFlyRunner,
    reason_code: str,
) -> None:
    from polyarb.control_plane.fly_topology_audit import (
        FlyTopologyAuditError,
        audit_fly_topology,
    )

    with pytest.raises(FlyTopologyAuditError) as exc_info:
        audit_fly_topology(
            targets=("writer-app/28654e35a73d08",),
            required_secrets=(),
            runner=runner,
        )

    rendered = json.dumps(exc_info.value.as_mapping(), sort_keys=True)
    assert reason_code in rendered
    assert "postgresql://" not in rendered
    assert "secret" not in rendered.lower().replace("provider", "")


def test_fly_topology_audit_rejects_unexpected_machine_without_disclosing_it() -> None:
    from polyarb.control_plane.fly_topology_audit import (
        FlyTopologyAuditError,
        audit_fly_topology,
    )

    payload = _status_payload()
    payload["Machines"].append(  # type: ignore[union-attr]
        {
            "id": "unexpected-machine",
            "state": "started",
            "config": {"env": {"LEAK": "must-never-appear"}},
        }
    )
    runner = FakeFlyRunner(status_payload=payload)

    with pytest.raises(FlyTopologyAuditError) as exc_info:
        audit_fly_topology(
            targets=("writer-app/28654e35a73d08",),
            required_secrets=(),
            runner=runner,
        )

    rendered = json.dumps(exc_info.value.as_mapping(), sort_keys=True)
    assert "fly-topology-audit.topology-mismatch" in rendered
    assert "unexpected-machine" not in rendered
    assert "must-never-appear" not in rendered


def test_fly_topology_audit_requires_declared_secret_name_presence() -> None:
    from polyarb.control_plane.fly_topology_audit import (
        FlyTopologyAuditError,
        audit_fly_topology,
    )

    runner = FakeFlyRunner(status_payload=_status_payload(), secrets_payload=[])

    with pytest.raises(FlyTopologyAuditError) as exc_info:
        audit_fly_topology(
            targets=("writer-app/28654e35a73d08",),
            required_secrets=("writer-app/POLYARB_SUPABASE_DB_DSN",),
            runner=runner,
        )

    assert exc_info.value.as_mapping() == {
        "status": "fail",
        "reason_code": "fly-topology-audit.required-secret-missing",
        "app": "writer-app",
        "object": "POLYARB_SUPABASE_DB_DSN",
    }


def test_fly_topology_subprocess_forces_keychain_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from polyarb.control_plane.fly_topology_audit import run_flyctl_read_only

    observed: dict[str, object] = {}

    def fake_run(argv: Sequence[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
        observed.update({"argv": tuple(argv), **kwargs})
        return subprocess.CompletedProcess(argv, 0, stdout="{}", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run_flyctl_read_only(("flyctl", "status", "-a", "writer-app", "--json"))

    assert observed["argv"] == ("flyctl", "status", "-a", "writer-app", "--json")
    assert observed["capture_output"] is True
    assert observed["text"] is True
    assert observed["timeout"] == 30
    assert observed["check"] is False
    assert observed["env"]["FLY_API_TOKEN"] == ""  # type: ignore[index]
