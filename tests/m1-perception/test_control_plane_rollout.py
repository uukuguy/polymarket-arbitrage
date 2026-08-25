"""Pure-local rollout artifact contracts for transactional M1 control plane."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


def test_rollout_renderer_writes_six_isolated_apps_and_staged_checklist(tmp_path: Path) -> None:
    from polyarb.control_plane.rollout import render_rollout_artifacts

    rendered = render_rollout_artifacts(
        api_app="polyarb-control-api-staging",
        worker_app="polyarb-control-worker-staging",
        alert_app="polyarb-control-alert-staging",
        runtime_event_writer_app="polyarb-control-runtime-event-writer-staging",
        runtime_controller_app="polyarb-runtime-controller-staging",
        qualification_worker_app="polyarb-qualification-worker-staging",
        runtime_recovery_allowed_targets=(
            "polyarb-control-worker-staging/fly-control-plane-coordinator",
            "polyarb-control-worker-staging/fly-control-plane-quote-batch",
        ),
        expected_database="control_plane_staging",
        output_dir=tmp_path,
    )

    assert rendered == {
        "api_config": str(tmp_path / "fly-control-api.toml"),
        "worker_config": str(tmp_path / "fly-control-worker.toml"),
        "alert_config": str(tmp_path / "fly-control-alert.toml"),
        "runtime_event_writer_config": str(tmp_path / "fly-runtime-event-writer.toml"),
        "runtime_controller_config": str(tmp_path / "fly-runtime-controller.toml"),
        "qualification_worker_config": str(tmp_path / "fly-qualification-worker.toml"),
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
    assert 'app = "polyarb-control-runtime-event-writer-staging"' in (
        tmp_path / "fly-runtime-event-writer.toml"
    ).read_text()
    runtime_controller_config = (tmp_path / "fly-runtime-controller.toml").read_text()
    qualification_config = (tmp_path / "fly-qualification-worker.toml").read_text()
    assert 'app = "polyarb-runtime-controller-staging"' in runtime_controller_config
    assert 'app = "polyarb-qualification-worker-staging"' in qualification_config
    assert 'POLYARB_RUNTIME_RECOVERY_MODE = "observe-only"' in runtime_controller_config
    assert (
        'POLYARB_RUNTIME_RECOVERY_ALLOWED_TARGETS = '
        '"polyarb-control-worker-staging/fly-control-plane-coordinator,'
        'polyarb-control-worker-staging/fly-control-plane-quote-batch"'
        in runtime_controller_config
    )
    assert "FLY_API_TOKEN" not in qualification_config
    assert "http_service" not in runtime_controller_config
    assert "http_service" not in qualification_config
    worker_config = (tmp_path / "fly-control-worker.toml").read_text()
    api_config = (tmp_path / "fly-control-api.toml").read_text()
    alert_config = (tmp_path / "fly-control-alert.toml").read_text()
    assert 'memory = "256mb"' in api_config
    assert "min_machines_running = 0" in api_config
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
        assert stale_machine_id not in runtime_controller_config
        assert stale_machine_id not in qualification_config
    checklist = json.loads((tmp_path / "rollout-checklist.json").read_text())
    assert checklist["expected_database"] == "control_plane_staging"
    assert checklist["runtime_controller_app"] == "polyarb-runtime-controller-staging"
    assert checklist["qualification_worker_app"] == "polyarb-qualification-worker-staging"
    assert checklist["runtime_recovery_mode"] == "observe-only"
    assert checklist["runtime_recovery_allowed_targets"] == [
        "polyarb-control-worker-staging/fly-control-plane-coordinator",
        "polyarb-control-worker-staging/fly-control-plane-quote-batch",
    ]
    assert checklist["rendered_secret_values"] is False
    assert checklist["steps"] == [
        "preflight",
        "revisions-022-through-024-migration",
        "isolated-api-data-worker-alert-worker-runtime-event-writer-runtime-controller-and-qualification-deploy",
        "three-fresh-source-window-structure-quote-shadows",
        "source-and-quote-admitter-worker-loss-circuit-probe-and-api-readability",
        "runtime-controller-observe-only-dry-run",
        "qualification-worker-read-only-replay",
        "continuous-24-hour-soak",
        "one-way-formal-transactional-promotion",
    ]
    assert checklist["source_window_admission"] == "explicit-operator-command"


@pytest.mark.parametrize(
    (
        "api_app,worker_app,alert_app,runtime_event_writer_app,"
        "runtime_controller_app,qualification_worker_app"
    ),
    [
        ("polyarb-l1", "worker", "alert", "writer", "controller", "qualification"),
        ("api", "polyarb-l1", "alert", "writer", "controller", "qualification"),
        ("api", "worker", "polyarb-l1", "writer", "controller", "qualification"),
        ("api", "worker", "alert", "polyarb-l1", "controller", "qualification"),
        ("api", "worker", "alert", "writer", "polyarb-l1", "qualification"),
        ("api", "worker", "alert", "writer", "controller", "polyarb-l1"),
    ],
)
def test_rollout_renderer_rejects_legacy_app_reuse(
    tmp_path: Path,
    api_app: str,
    worker_app: str,
    alert_app: str,
    runtime_event_writer_app: str,
    runtime_controller_app: str,
    qualification_worker_app: str,
) -> None:
    from polyarb.control_plane.rollout import RolloutArtifactError, render_rollout_artifacts

    with pytest.raises(RolloutArtifactError, match="legacy app"):
        render_rollout_artifacts(
            api_app=api_app,
            worker_app=worker_app,
            alert_app=alert_app,
            runtime_event_writer_app=runtime_event_writer_app,
            runtime_controller_app=runtime_controller_app,
            qualification_worker_app=qualification_worker_app,
            expected_database="control_plane_staging",
            output_dir=tmp_path,
        )


def test_rollout_renderer_rejects_app_reuse_for_runtime_controller_and_qualification(
    tmp_path: Path,
) -> None:
    from polyarb.control_plane.rollout import RolloutArtifactError, render_rollout_artifacts

    with pytest.raises(RolloutArtifactError, match="different Fly apps"):
        render_rollout_artifacts(
            api_app="api",
            worker_app="worker",
            alert_app="alert",
            runtime_event_writer_app="writer",
            runtime_controller_app="worker",
            qualification_worker_app="qualification",
            expected_database="control_plane_staging",
            output_dir=tmp_path,
        )

    with pytest.raises(RolloutArtifactError, match="different Fly apps"):
        render_rollout_artifacts(
            api_app="api",
            worker_app="worker",
            alert_app="alert",
            runtime_event_writer_app="writer",
            runtime_controller_app="controller",
            qualification_worker_app="controller",
            expected_database="control_plane_staging",
            output_dir=tmp_path,
        )
