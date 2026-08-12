"""Pure-local rollout artifact contracts for transactional M1 control plane."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_rollout_renderer_writes_two_named_apps_and_staged_checklist(tmp_path: Path) -> None:
    from polyarb.control_plane.rollout import render_rollout_artifacts

    rendered = render_rollout_artifacts(
        api_app="polyarb-control-api-staging",
        worker_app="polyarb-control-worker-staging",
        expected_database="control_plane_staging",
        output_dir=tmp_path,
    )

    assert rendered == {
        "api_config": str(tmp_path / "fly-control-api.toml"),
        "worker_config": str(tmp_path / "fly-control-worker.toml"),
        "checklist": str(tmp_path / "rollout-checklist.json"),
    }
    assert 'app = "polyarb-control-api-staging"' in (tmp_path / "fly-control-api.toml").read_text()
    assert (
        'app = "polyarb-control-worker-staging"'
        in (tmp_path / "fly-control-worker.toml").read_text()
    )
    checklist = json.loads((tmp_path / "rollout-checklist.json").read_text())
    assert checklist["expected_database"] == "control_plane_staging"
    assert checklist["steps"] == [
        "preflight",
        "revision-011-migration",
        "isolated-api-and-worker-deploy",
        "three-fresh-source-window-shadows",
        "source-worker-loss-and-api-readability",
        "continuous-24-hour-soak",
        "authorized-reversible-switch",
    ]
    assert checklist["source_window_admission"] == "explicit-operator-command"


@pytest.mark.parametrize("api_app,worker_app", [("polyarb-l1", "worker"), ("api", "polyarb-l1")])
def test_rollout_renderer_rejects_legacy_app_reuse(
    tmp_path: Path, api_app: str, worker_app: str
) -> None:
    from polyarb.control_plane.rollout import RolloutArtifactError, render_rollout_artifacts

    with pytest.raises(RolloutArtifactError, match="legacy app"):
        render_rollout_artifacts(
            api_app=api_app,
            worker_app=worker_app,
            expected_database="control_plane_staging",
            output_dir=tmp_path,
        )
