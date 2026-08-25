"""Pure-local rendering of explicit transactional control-plane rollout artifacts."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from pathlib import Path


class RolloutArtifactError(ValueError):
    """An operator supplied an ambiguous or unsafe rollout identity."""


_APP_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
_RECOVERY_TARGET = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}/[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_LEGACY_APP = "polyarb-l1"
_ROOT = Path(__file__).resolve().parents[3]
_TEMPLATE_DIR = _ROOT / "deploy" / "control-plane"


def render_rollout_artifacts(
    *,
    api_app: str,
    worker_app: str,
    alert_app: str,
    runtime_event_writer_app: str,
    runtime_controller_app: str | None = None,
    qualification_worker_app: str | None = None,
    runtime_recovery_allowed_targets: Sequence[str] = (),
    expected_database: str,
    output_dir: Path,
) -> dict[str, str]:
    """Render non-deployable staged rollout inputs without cloud access."""
    _validate_app("api_app", api_app)
    _validate_app("worker_app", worker_app)
    _validate_app("alert_app", alert_app)
    _validate_app("runtime_event_writer_app", runtime_event_writer_app)
    render_runtime_topology = (
        runtime_controller_app is not None or qualification_worker_app is not None
    )
    app_names = [api_app, worker_app, alert_app, runtime_event_writer_app]
    runtime_apps: tuple[str, str] | None = None
    if render_runtime_topology:
        if runtime_controller_app is None or qualification_worker_app is None:
            raise RolloutArtifactError(
                "runtime controller and qualification worker apps must be rendered together"
            )
        runtime_apps = (runtime_controller_app, qualification_worker_app)
        _validate_app("runtime_controller_app", runtime_apps[0])
        _validate_app("qualification_worker_app", runtime_apps[1])
        app_names.extend(runtime_apps)
    if len(set(app_names)) != len(app_names):
        raise RolloutArtifactError(
            "API, data worker, alert worker, runtime writer, runtime controller "
            "and qualification worker must use different Fly apps"
        )
    allowed_targets = _validate_recovery_targets(runtime_recovery_allowed_targets)
    if not expected_database.strip():
        raise RolloutArtifactError("expected_database must be non-empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    destinations = {
        "api_config": output_dir / "fly-control-api.toml",
        "worker_config": output_dir / "fly-control-worker.toml",
        "alert_config": output_dir / "fly-control-alert.toml",
        "runtime_event_writer_config": output_dir / "fly-runtime-event-writer.toml",
        "checklist": output_dir / "rollout-checklist.json",
    }
    if render_runtime_topology:
        destinations["runtime_controller_config"] = output_dir / "fly-runtime-controller.toml"
        destinations["qualification_worker_config"] = output_dir / "fly-qualification-worker.toml"
    if any(path.exists() for path in destinations.values()):
        raise RolloutArtifactError("rollout artifact output already exists")
    api_config = _render_template(
        "fly-control-api.toml.template", "__CONTROL_PLANE_API_APP__", api_app
    )
    worker_config = _render_template(
        "fly-control-worker.toml.template", "__CONTROL_PLANE_WORKER_APP__", worker_app
    )
    alert_config = _render_template(
        "fly-control-alert.toml.template", "__CONTROL_PLANE_ALERT_APP__", alert_app
    )
    runtime_event_writer_config = _render_template(
        "fly-runtime-event-writer.toml.template",
        "__RUNTIME_EVENT_WRITER_APP__",
        runtime_event_writer_app,
    )
    runtime_controller_config = ""
    qualification_worker_config = ""
    if runtime_apps is not None:
        runtime_controller_config = _render_template(
            "fly-runtime-controller.toml.template",
            "__RUNTIME_CONTROLLER_APP__",
            runtime_apps[0],
            replacements={
                "__RUNTIME_RECOVERY_ALLOWED_TARGETS__": ",".join(allowed_targets),
            },
        )
        qualification_worker_config = _render_template(
            "fly-qualification-worker.toml.template",
            "__QUALIFICATION_WORKER_APP__",
            runtime_apps[1],
        )
    checklist = {
        "artifact_version": 9 if render_runtime_topology else 7,
        "api_app": api_app,
        "worker_app": worker_app,
        "alert_app": alert_app,
        "runtime_event_writer_app": runtime_event_writer_app,
        "expected_database": expected_database,
        "steps": [
            "preflight",
            "revisions-022-through-025-migration",
            (
                "isolated-api-data-worker-alert-worker-runtime-event-writer-"
                "runtime-controller-and-qualification-deploy"
                if render_runtime_topology
                else "isolated-api-data-worker-alert-worker-and-runtime-event-writer-deploy"
            ),
            "three-fresh-source-window-structure-quote-shadows",
            "source-and-quote-admitter-worker-loss-circuit-probe-and-api-readability",
            *(
                [
                    "runtime-controller-observe-only-dry-run",
                    "qualification-worker-read-only-replay",
                ]
                if render_runtime_topology
                else []
            ),
            "continuous-24-hour-soak",
            "one-way-formal-transactional-promotion",
        ],
        "source_window_admission": "explicit-operator-command",
        "cloud_actions_performed": False,
        "rendered_secret_values": False,
    }
    if runtime_apps is not None:
        checklist.update(
            {
                "runtime_controller_app": runtime_apps[0],
                "qualification_worker_app": runtime_apps[1],
                "runtime_recovery_mode": "observe-only",
                "runtime_recovery_allowed_targets": list(allowed_targets),
            }
        )
    destinations["api_config"].write_text(api_config)
    destinations["worker_config"].write_text(worker_config)
    destinations["alert_config"].write_text(alert_config)
    destinations["runtime_event_writer_config"].write_text(runtime_event_writer_config)
    if runtime_apps is not None:
        destinations["runtime_controller_config"].write_text(runtime_controller_config)
        destinations["qualification_worker_config"].write_text(qualification_worker_config)
    destinations["checklist"].write_text(json.dumps(checklist, sort_keys=True, indent=2) + "\n")
    return {name: str(path) for name, path in destinations.items()}


def _validate_app(name: str, value: str) -> None:
    if value == _LEGACY_APP:
        raise RolloutArtifactError(f"{name} must not reuse legacy app {_LEGACY_APP}")
    if not _APP_NAME.fullmatch(value):
        raise RolloutArtifactError(f"{name} is not a valid Fly app name")


def _validate_recovery_targets(targets: Sequence[str]) -> tuple[str, ...]:
    normalized: list[str] = []
    for target in targets:
        if not isinstance(target, str) or not _RECOVERY_TARGET.fullmatch(target):
            raise RolloutArtifactError(
                "runtime recovery targets must be exact <app>/<machine-or-process> identities"
            )
        if any(marker in target for marker in ("$", "{", "}", ":", "?")):
            raise RolloutArtifactError(
                "runtime recovery targets must not contain environment placeholders"
            )
        normalized.append(target)
    if len(set(normalized)) != len(normalized):
        raise RolloutArtifactError("runtime recovery targets must be unique")
    return tuple(normalized)


def _render_template(
    template_name: str,
    placeholder: str,
    app_name: str,
    *,
    replacements: dict[str, str] | None = None,
) -> str:
    rendered = (_TEMPLATE_DIR / template_name).read_text().replace(placeholder, app_name)
    for key, value in (replacements or {}).items():
        rendered = rendered.replace(key, value)
    if placeholder in rendered or "__CONTROL_PLANE_" in rendered:
        raise RolloutArtifactError("rollout template contains an unresolved identity")
    if "__RUNTIME_" in rendered or "__QUALIFICATION_" in rendered:
        raise RolloutArtifactError("rollout template contains an unresolved identity")
    return rendered
