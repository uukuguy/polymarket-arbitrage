"""Pure-local rendering of explicit transactional control-plane rollout artifacts."""

from __future__ import annotations

import json
import re
from pathlib import Path


class RolloutArtifactError(ValueError):
    """An operator supplied an ambiguous or unsafe rollout identity."""


_APP_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{1,62}$")
_LEGACY_APP = "polyarb-l1"
_ROOT = Path(__file__).resolve().parents[3]
_TEMPLATE_DIR = _ROOT / "deploy" / "control-plane"


def render_rollout_artifacts(
    *,
    api_app: str,
    worker_app: str,
    expected_database: str,
    output_dir: Path,
) -> dict[str, str]:
    """Render non-deployable staged rollout inputs without cloud access."""
    _validate_app("api_app", api_app)
    _validate_app("worker_app", worker_app)
    if api_app == worker_app:
        raise RolloutArtifactError("API and worker must use different Fly apps")
    if not expected_database.strip():
        raise RolloutArtifactError("expected_database must be non-empty")
    output_dir.mkdir(parents=True, exist_ok=True)
    destinations = {
        "api_config": output_dir / "fly-control-api.toml",
        "worker_config": output_dir / "fly-control-worker.toml",
        "checklist": output_dir / "rollout-checklist.json",
    }
    if any(path.exists() for path in destinations.values()):
        raise RolloutArtifactError("rollout artifact output already exists")
    api_config = _render_template(
        "fly-control-api.toml.template", "__CONTROL_PLANE_API_APP__", api_app
    )
    worker_config = _render_template(
        "fly-control-worker.toml.template", "__CONTROL_PLANE_WORKER_APP__", worker_app
    )
    checklist = {
        "artifact_version": 2,
        "api_app": api_app,
        "worker_app": worker_app,
        "expected_database": expected_database,
        "steps": [
            "preflight",
            "revision-011-migration",
            "isolated-api-and-worker-deploy",
            "three-fresh-source-window-shadows",
            "source-worker-loss-and-api-readability",
            "continuous-24-hour-soak",
            "authorized-reversible-switch",
        ],
        "source_window_admission": "explicit-operator-command",
        "cloud_actions_performed": False,
    }
    destinations["api_config"].write_text(api_config)
    destinations["worker_config"].write_text(worker_config)
    destinations["checklist"].write_text(json.dumps(checklist, sort_keys=True, indent=2) + "\n")
    return {name: str(path) for name, path in destinations.items()}


def _validate_app(name: str, value: str) -> None:
    if value == _LEGACY_APP:
        raise RolloutArtifactError(f"{name} must not reuse legacy app {_LEGACY_APP}")
    if not _APP_NAME.fullmatch(value):
        raise RolloutArtifactError(f"{name} is not a valid Fly app name")


def _render_template(template_name: str, placeholder: str, app_name: str) -> str:
    rendered = (_TEMPLATE_DIR / template_name).read_text().replace(placeholder, app_name)
    if placeholder in rendered or "__CONTROL_PLANE_" in rendered:
        raise RolloutArtifactError("rollout template contains an unresolved identity")
    return rendered
