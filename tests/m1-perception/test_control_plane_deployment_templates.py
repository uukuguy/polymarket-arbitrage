"""Static deployment contracts for isolated transactional control-plane apps."""

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_control_api_template_has_only_postgres_read_process_and_http_health() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-api.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_API_APP__"
    assert payload["env"]["POLYARB_RUNTIME_ROLE"] == "control-plane"
    assert payload["processes"] == {"api": "python -m polyarb.control_plane.api"}
    assert payload["http_service"]["processes"] == ["api"]
    assert payload["http_service"]["checks"][0]["path"] == "/healthz"
    assert "mounts" not in payload


def test_control_worker_template_has_three_fixed_transactional_roles() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-worker.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_WORKER_APP__"
    assert payload["env"]["POLYARB_ALERT_CHANNELS"] == "dashboard,telegram"
    assert payload["env"]["POLYARB_RUNTIME_ROLE"] == "control-plane"
    assert payload["processes"] == {
        "coordinator": (
            "python -m polyarb.cli_control_plane serve --enable "
            "--worker-id fly-control-plane-coordinator --worker-role coordinator "
            "--max-turns 8 --structure-materializer-turns 8 --interval-seconds 2 --json"
        ),
        "structure_range": (
            "/bin/sh -ec 'exec python -m polyarb.cli_control_plane serve --enable "
            "--worker-id \"fly-control-plane-structure-range:${FLY_MACHINE_ID:?}\" "
            "--worker-role structure-range --pool-turns 2 --interval-seconds 2 --json'"
        ),
        "quote_batch": (
            "python -m polyarb.cli_control_plane serve --enable "
            "--worker-id fly-control-plane-quote-batch --worker-role quote-batch "
            "--pool-turns 1 --interval-seconds 5 --json"
        ),
    }
    assert payload["vm"][0]["processes"] == [
        "coordinator",
        "structure_range",
        "quote_batch",
    ]
    assert payload["vm"][0]["memory"] == "1024mb"
    assert "http_service" not in payload
    assert "mounts" not in payload


def test_alert_delivery_template_isolated_from_runtime_watchdog() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-alert-delivery.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_ALERT_DELIVERY_APP__"
    assert payload["processes"] == {
        "delivery": (
            "python -m polyarb.cli_control_plane alert-serve --enable "
            "--worker-id fly-control-plane-alert-delivery "
            "--interval-seconds 5 --json"
        )
    }
    assert payload["restart"] == [{"policy": "always"}]
    assert "http_service" not in payload
    assert "mounts" not in payload


def test_control_alert_template_is_a_database_independent_runtime_watchdog() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-alert.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_ALERT_APP__"
    watchdog = payload["processes"]["watchdog"]
    assert '"$POLYARB_CONTROL_API_URL"' in watchdog
    assert '"$POLYARB_COORDINATOR_MACHINE_ID"' in watchdog
    assert '"$POLYARB_STRUCTURE_RANGE_MACHINE_ID"' in watchdog
    assert '"$POLYARB_QUOTE_BATCH_MACHINE_ID"' in watchdog
    assert '"$POLYARB_EVIDENCE_APP"' in watchdog
    assert '"$POLYARB_EVIDENCE_MACHINE_ID"' in watchdog
    assert '"$POLYARB_WATCHDOG_SOAK_RUN_ID_V2"' in watchdog
    assert (
        '--secondary-target '
        '"$POLYARB_RUNTIME_EVENT_WRITER_APP/$POLYARB_RUNTIME_EVENT_WRITER_MACHINE_ID"'
        in watchdog
    )
    assert payload["vm"][0]["processes"] == ["watchdog"]
    assert payload["restart"] == [{"policy": "always"}]
    assert "http_service" not in payload
    assert "mounts" not in payload


def test_runtime_controller_template_is_private_observe_only_recovery_topology() -> None:
    template = ROOT / "deploy/control-plane/fly-runtime-controller.toml.template"
    text = template.read_text()
    payload = tomllib.loads(text)

    assert payload["app"] == "__RUNTIME_CONTROLLER_APP__"
    assert payload["env"] == {
        "POLYARB_RUNTIME_ROLE": "control-plane",
        "POLYARB_RUNTIME_RECOVERY_ALLOWED_TARGETS": "__RUNTIME_RECOVERY_ALLOWED_TARGETS__",
        "POLYARB_RUNTIME_RECOVERY_MODE": "observe-only",
    }
    assert payload["processes"] == {
        "controller": (
            "python -m polyarb.cli_control_plane runtime-reconcile-serve "
            "--enable --interval-seconds 30 --json"
        )
    }
    assert payload["restart"] == [{"policy": "always"}]
    assert payload["vm"][0]["processes"] == ["controller"]
    assert "http_service" not in payload
    assert "mounts" not in payload

    env_and_process_text = "\n".join(
        [
            *(f"{key}={value}" for key, value in payload["env"].items()),
            *payload["processes"].values(),
        ]
    )
    for forbidden in (
        "POLYARB_R2",
        "R2_BUCKET",
        "GAMMA",
        "CLOB",
        "TELEGRAM",
        "POLYARB_ALERT_CHANNELS",
        "POLYARB_CONTROL_WORKER",
        "POLYARB_SOAK",
    ):
        assert forbidden not in env_and_process_text
    assert "FLY_API_TOKEN" not in env_and_process_text
    assert "scoped runtime-controller Postgres DSN" in text
    assert "optional exact Fly recovery token" in text


def test_qualification_worker_template_has_only_scoped_database_and_no_recovery_authority() -> None:
    template = ROOT / "deploy/control-plane/fly-qualification-worker.toml.template"
    text = template.read_text()
    payload = tomllib.loads(text)

    assert payload["app"] == "__QUALIFICATION_WORKER_APP__"
    assert payload["env"] == {"POLYARB_RUNTIME_ROLE": "control-plane"}
    assert payload["processes"] == {
        "qualification": (
            "python -m polyarb.cli_control_plane qualification-serve "
            "--enable --interval-seconds 30 --json"
        )
    }
    assert payload["restart"] == [{"policy": "always"}]
    assert payload["vm"][0]["processes"] == ["qualification"]
    assert "http_service" not in payload
    assert "mounts" not in payload

    env_and_process_text = "\n".join(
        [
            *(f"{key}={value}" for key, value in payload["env"].items()),
            *payload["processes"].values(),
        ]
    )
    for forbidden in (
        "FLY_API_TOKEN",
        "POLYARB_R2",
        "R2_BUCKET",
        "GAMMA",
        "CLOB",
        "TELEGRAM",
        "POLYARB_ALERT_CHANNELS",
        "POLYARB_RUNTIME_RECOVERY",
        "POLYARB_CONTROL_WORKER",
        "POLYARB_SOAK",
    ):
        assert forbidden not in env_and_process_text
    assert "scoped qualification Postgres DSN" in text
