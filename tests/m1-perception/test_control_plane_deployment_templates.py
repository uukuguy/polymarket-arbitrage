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
            "python -m polyarb.cli_control_plane serve --enable "
            "--worker-id fly-control-plane-structure-range --worker-role structure-range "
            "--pool-turns 1 --interval-seconds 5 --json"
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
    assert '"$POLYARB_EVIDENCE_SOAK_RUN_ID"' in watchdog
    assert (
        '--secondary-target '
        '"$POLYARB_RUNTIME_EVENT_WRITER_APP/$POLYARB_RUNTIME_EVENT_WRITER_MACHINE_ID"'
        in watchdog
    )
    assert payload["vm"][0]["processes"] == ["watchdog"]
    assert payload["restart"] == [{"policy": "always"}]
    assert "http_service" not in payload
    assert "mounts" not in payload
