"""Static deployment contracts for isolated transactional control-plane apps."""

import tomllib
from pathlib import Path

import pytest

from polyarb.control_plane.db_role_admin import PROFILE_DSN_ENV

ROOT = Path(__file__).resolve().parents[2]

FORMAL_LONG_RUNNING_TEMPLATES = (
    "fly-control-alert-delivery.toml.template",
    "fly-control-alert.toml.template",
    "fly-control-api.toml.template",
    "fly-control-worker.toml.template",
    "fly-qualification-worker.toml.template",
    "fly-runtime-controller.toml.template",
    "fly-runtime-event-writer.toml.template",
)


@pytest.mark.parametrize("template_name", FORMAL_LONG_RUNNING_TEMPLATES)
def test_formal_service_template_declares_platform_shutdown_backstop(template_name: str) -> None:
    payload = tomllib.loads((ROOT / "deploy/control-plane" / template_name).read_text())

    assert payload["kill_signal"] == "SIGTERM"
    assert payload["kill_timeout"] == 40


def test_docker_build_context_excludes_local_distribution_artifacts() -> None:
    ignored = {
        line.strip()
        for line in (ROOT / ".dockerignore").read_text().splitlines()
        if line.strip() and not line.lstrip().startswith("#")
    }

    assert "dist/" in ignored


def test_control_api_template_has_only_postgres_read_process_and_http_health() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-api.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_API_APP__"
    assert payload["env"]["POLYARB_RUNTIME_ROLE"] == "control-plane"
    assert payload["processes"] == {"api": "python -m polyarb.control_plane.api"}
    assert payload["http_service"]["processes"] == ["api"]
    assert payload["http_service"]["checks"][0]["path"] == "/healthz"
    assert payload["http_service"]["checks"][0]["timeout"] == "5s"
    assert "mounts" not in payload


def test_control_worker_template_has_three_fixed_transactional_roles() -> None:
    payload = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-worker.toml.template").read_text()
    )

    assert payload["app"] == "__CONTROL_PLANE_WORKER_APP__"
    assert payload["env"]["POLYARB_ALERT_CHANNELS"] == "dashboard,telegram"
    assert payload["env"]["POLYARB_RUNTIME_ROLE"] == "control-plane"
    # Each quote lane owns a synchronous database write path.  Keep the lane
    # count within the worker's two-session pool so a normal publication wave
    # cannot starve itself with PoolTimeout while committing its receipts.
    assert payload["env"]["POLYARB_CLOB_BATCH_MAX_CONCURRENCY"] == "2"
    # Structure normalization stages its research rows through the same pool,
    # so its lease lanes must obey the identical database-session budget.
    assert payload["env"]["POLYARB_STRUCTURE_RANGE_MAX_CONCURRENCY"] == "2"
    assert payload["processes"] == {
        "coordinator": (
            "python -m polyarb.cli_control_plane serve --enable "
            "--worker-id fly-control-plane-coordinator --worker-role coordinator "
            "--max-turns 8 --structure-materializer-turns 8 --interval-seconds 2 --json"
        ),
        "structure_range": (
            "/bin/sh -ec 'exec python -m polyarb.cli_control_plane serve --enable "
            '--worker-id "fly-control-plane-structure-range:${FLY_MACHINE_ID:?}" '
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
    assert '"$POLYARB_EVIDENCE_APP"' not in watchdog
    assert '"$POLYARB_EVIDENCE_MACHINE_ID"' not in watchdog
    # A fresh database commissioning does not inherit the retired sampler's
    # formal-run identifier.  Runtime liveness is checked from the live API
    # and exact Machines; a formal soak is an explicit, opt-in acceptance gate.
    assert '"$POLYARB_WATCHDOG_SOAK_RUN_ID_V2"' not in watchdog
    assert (
        "--secondary-target "
        '"$POLYARB_RUNTIME_EVENT_WRITER_APP/$POLYARB_RUNTIME_EVENT_WRITER_MACHINE_ID"' in watchdog
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
        "POLYARB_DB_EXPECTED_DATABASE": "__EXPECTED_DATABASE__",
        "POLYARB_DB_POOL_MAX_SIZE": "1",
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


def test_database_session_budgets_leave_recovery_capacity() -> None:
    api = tomllib.loads((ROOT / "deploy/control-plane/fly-control-api.toml.template").read_text())
    worker = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-worker.toml.template").read_text()
    )
    controller = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-runtime-controller.toml.template").read_text()
    )
    qualification = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-qualification-worker.toml.template").read_text()
    )
    alert_delivery = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-control-alert-delivery.toml.template").read_text()
    )
    runtime_writer = tomllib.loads(
        (ROOT / "deploy/control-plane/fly-runtime-event-writer.toml.template").read_text()
    )

    assert api["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "2"
    assert worker["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "2"
    assert controller["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "1"
    assert qualification["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "1"
    assert alert_delivery["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "1"
    assert runtime_writer["env"]["POLYARB_DB_POOL_MAX_SIZE"] == "1"
    all_deployed_session_owners = (
        int(api["env"]["POLYARB_DB_POOL_MAX_SIZE"])
        + 3 * int(worker["env"]["POLYARB_DB_POOL_MAX_SIZE"])
        + int(controller["env"]["POLYARB_DB_POOL_MAX_SIZE"])
        + int(qualification["env"]["POLYARB_DB_POOL_MAX_SIZE"])
        + int(alert_delivery["env"]["POLYARB_DB_POOL_MAX_SIZE"])
        + int(runtime_writer["env"]["POLYARB_DB_POOL_MAX_SIZE"])
    )
    assert all_deployed_session_owners == 12
    assert 15 - all_deployed_session_owners == 3


def test_qualification_worker_template_has_only_scoped_database_and_no_recovery_authority() -> None:
    template = ROOT / "deploy/control-plane/fly-qualification-worker.toml.template"
    text = template.read_text()
    payload = tomllib.loads(text)

    assert payload["app"] == "__QUALIFICATION_WORKER_APP__"
    assert payload["env"] == {
        "POLYARB_DB_EXPECTED_DATABASE": "__EXPECTED_DATABASE__",
        "POLYARB_DB_POOL_MAX_SIZE": "1",
        "POLYARB_QUALIFICATION_CONFIG_ID": "__QUALIFICATION_CONFIG_ID__",
        "POLYARB_QUALIFICATION_RELEASE_ID": "__QUALIFICATION_RELEASE_ID__",
        "POLYARB_QUALIFICATION_ROLE_IDENTITY": "opportunity,quote,structure",
        "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_ALLOWED_TARGETS": (
            "__RUNTIME_RECOVERY_ALLOWED_TARGETS__"
        ),
        "POLYARB_QUALIFICATION_RUNTIME_RECOVERY_MODE": "observe-only",
        "POLYARB_RUNTIME_ROLE": "control-plane",
    }
    assert payload["processes"] == {
        "qualification": (
            "python -m polyarb.cli_control_plane qualification-serve "
            "--enable --interval-seconds 30 --batch-size 100 --json"
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


def test_daemon_dsn_names_are_one_source_across_cli_templates_and_runbook() -> None:
    runtime_name = "POLYARB_SUPABASE_DB_DSN"
    qualification_name = "POLYARB_QUALIFICATION_DB_DSN"
    deprecated_names = {
        "POLYARB_RUNTIME_CONTROLLER_DB_DSN",
        "POLYARB_QUALIFICATION_WORKER_DB_DSN",
    }

    assert PROFILE_DSN_ENV == {
        "runtime-controller": runtime_name,
        "qualification-worker": qualification_name,
    }

    cli_source = (ROOT / "src/polyarb/cli_control_plane.py").read_text()
    runtime_template = (
        ROOT / "deploy/control-plane/fly-runtime-controller.toml.template"
    ).read_text()
    qualification_template = (
        ROOT / "deploy/control-plane/fly-qualification-worker.toml.template"
    ).read_text()
    runbook = (ROOT / "docs/dev/control-plane-runbook.md").read_text()

    assert f'os.environ.get("{runtime_name}"' in cli_source
    assert f'os.environ.get("{qualification_name}"' in cli_source
    assert runtime_name in runtime_template
    assert qualification_name not in runtime_template
    assert qualification_name in qualification_template
    assert runtime_name not in qualification_template
    assert runtime_name in runbook
    assert qualification_name in runbook
    for deprecated in deprecated_names:
        assert deprecated not in runtime_template
        assert deprecated not in qualification_template
        assert deprecated not in runbook
