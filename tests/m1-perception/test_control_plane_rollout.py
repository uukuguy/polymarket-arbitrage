"""Pure-local rollout artifact contracts for transactional M1 control plane."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_rollout_renderer_writes_three_isolated_apps_and_staged_checklist(tmp_path: Path) -> None:
    from polyarb.control_plane.rollout import render_rollout_artifacts

    rendered = render_rollout_artifacts(
        api_app="polyarb-control-api-staging",
        worker_app="polyarb-control-worker-staging",
        alert_app="polyarb-control-alert-staging",
        expected_database="control_plane_staging",
        output_dir=tmp_path,
    )

    assert rendered == {
        "api_config": str(tmp_path / "fly-control-api.toml"),
        "worker_config": str(tmp_path / "fly-control-worker.toml"),
        "alert_config": str(tmp_path / "fly-control-alert.toml"),
        "checklist": str(tmp_path / "rollout-checklist.json"),
    }
    assert 'app = "polyarb-control-api-staging"' in (tmp_path / "fly-control-api.toml").read_text()
    assert (
        'app = "polyarb-control-worker-staging"'
        in (tmp_path / "fly-control-worker.toml").read_text()
    )
    assert (
        'app = "polyarb-control-alert-staging"' in (tmp_path / "fly-control-alert.toml").read_text()
    )
    worker_config = (tmp_path / "fly-control-worker.toml").read_text()
    alert_config = (tmp_path / "fly-control-alert.toml").read_text()
    assert "soak_sampler" not in worker_config
    assert "structure_range_a" not in worker_config
    assert "structure_range_b" not in worker_config
    assert "quote_batch_a" not in worker_config
    assert "quote_batch_b" not in worker_config
    assert "coordinator =" in worker_config
    assert "structure_range =" in worker_config
    assert "quote_batch =" in worker_config
    assert 'memory = "1024mb"' in worker_config
    for stale_machine_id in (
        "3d8d0e29c7d589",
        "080d3ddbe66068",
        "4d895231f7d987",
        "85e990c43533e8",
        "86ed91bee33608",
    ):
        assert stale_machine_id not in worker_config
        assert stale_machine_id not in alert_config
    checklist = json.loads((tmp_path / "rollout-checklist.json").read_text())
    assert checklist["expected_database"] == "control_plane_staging"
    assert checklist["steps"] == [
        "preflight",
        "revision-014-migration",
        "isolated-api-data-worker-and-alert-worker-deploy",
        "three-fresh-source-window-structure-quote-shadows",
        "source-and-quote-admitter-worker-loss-circuit-probe-and-api-readability",
        "continuous-24-hour-soak",
        "one-way-formal-transactional-promotion",
    ]
    assert checklist["source_window_admission"] == "explicit-operator-command"


@pytest.mark.parametrize(
    "api_app,worker_app,alert_app",
    [
        ("polyarb-l1", "worker", "alert"),
        ("api", "polyarb-l1", "alert"),
        ("api", "worker", "polyarb-l1"),
    ],
)
def test_rollout_renderer_rejects_legacy_app_reuse(
    tmp_path: Path, api_app: str, worker_app: str, alert_app: str
) -> None:
    from polyarb.control_plane.rollout import RolloutArtifactError, render_rollout_artifacts

    with pytest.raises(RolloutArtifactError, match="legacy app"):
        render_rollout_artifacts(
            api_app=api_app,
            worker_app=worker_app,
            alert_app=alert_app,
            expected_database="control_plane_staging",
            output_dir=tmp_path,
        )
