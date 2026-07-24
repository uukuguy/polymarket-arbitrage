"""Structural tests for fly-l2.toml + deploy-l2.yml + fly_secrets_sync.sh.

Phase 03 Plan 02 — bootstrap polyarb-l2 Fly app per D-06 (single binary,
two deployments). These tests assert the 8 documented diffs from fly.toml
that are required for the L2 deployment to be correct.

Threat references:
- T-03-02-01: scripts must not contain `set -x` (leaks secret values).
- Phase 02.1 BUG-6 invariant: /healthz probe path is required; /health
  is forbidden (returns 503 under L1 partial-degradation states).
- Phase 02 LEARNINGS L8: flyctl-actions must be pinned to @1.6 (NOT @v1.5).
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FLY_L2_TOML = ROOT / "fly-l2.toml"
SECRETS_SYNC_SH = ROOT / "scripts/fly_secrets_sync.sh"
DEPLOY_L2_YML = ROOT / ".github/workflows/deploy-l2.yml"


def _load() -> dict:
    assert FLY_L2_TOML.exists(), f"missing {FLY_L2_TOML}"
    return tomllib.loads(FLY_L2_TOML.read_text())


def test_fly_l2_config_exists() -> None:
    assert FLY_L2_TOML.exists(), f"fly-l2.toml must exist at repo root ({FLY_L2_TOML})"


def test_fly_l2_config_app_name() -> None:
    assert _load()["app"] == "polyarb-l2"


def test_fly_l2_config_primary_region_ams() -> None:
    # Same region as polyarb-l1 — private network latency for event bus (D-06).
    assert _load()["primary_region"] == "ams"


def test_fly_l2_config_no_cron_process() -> None:
    procs = _load()["processes"]
    assert (
        "cron" not in procs
    ), f"cron process must not exist (D-06 — WS-driven single loop); got {list(procs.keys())}"
    assert set(procs.keys()) == {
        "app"
    }, f"expected only 'app' process group; got {set(procs.keys())}"


def test_fly_l2_config_healthz_probe() -> None:
    config = _load()
    checks = config["http_service"]["checks"]
    # tomllib parses [[http_service.checks]] as list-of-dicts.
    assert any(
        c.get("path") == "/healthz" for c in checks
    ), "no /healthz probe (Phase 02.1 BUG-6 invariant)"
    assert not any(
        c.get("path") == "/health" for c in checks
    ), "/health probe FORBIDDEN (BUG-6 — 503 blocks Fly proxy)"


def test_fly_l2_config_volume_sized_down() -> None:
    size = _load()["mounts"]["initial_size"]
    assert re.match(
        r"^1[gG][bB]?$", size
    ), f"volume should be 1gb (no parquet archive); got {size!r}"


def test_fly_l2_config_single_vm_group() -> None:
    config = _load()
    vm = config["vm"]
    vm_blocks = vm if isinstance(vm, list) else [vm]
    groups = {p for block in vm_blocks for p in block.get("processes", [])}
    assert groups == {"app"}, f"expected only 'app' VM group; got {groups}"


def test_fly_l2_config_daemon_variant_env() -> None:
    env = _load()["env"]
    assert (
        env.get("POLYARB_DAEMON_VARIANT") == "l2"
    ), f"POLYARB_DAEMON_VARIANT must be 'l2'; got {env.get('POLYARB_DAEMON_VARIANT')!r}"


def test_fly_l2_config_separate_db_path() -> None:
    db_path = _load()["env"]["POLYARB_DB_PATH"]
    assert "l2" in db_path, f"L2 SQLite path must differ from L1; got {db_path!r}"


def test_secrets_sync_script_no_set_x() -> None:
    assert SECRETS_SYNC_SH.exists(), f"missing {SECRETS_SYNC_SH}"
    text = SECRETS_SYNC_SH.read_text()
    assert "set -x" not in text, "set -x leaks secret values (T-03-02-01)"
    assert "set -ex" not in text, "set -ex leaks secret values"


def test_deploy_l2_workflow_uses_correct_config() -> None:
    assert DEPLOY_L2_YML.exists(), f"missing {DEPLOY_L2_YML}"
    text = DEPLOY_L2_YML.read_text()
    assert "--config fly-l2.toml" in text, "deploy-l2 workflow must use fly-l2.toml"
    assert "polyarb-l2" in text, "deploy-l2 workflow must reference polyarb-l2 app"
    assert (
        "superfly/flyctl-actions/setup-flyctl@1.6" in text
    ), "must pin @1.6 (Phase 02 L8)"
    assert "/healthz" in text, "smoke path must be /healthz (BUG-6)"
    assert "@v1.5" not in text, "@v1.5 is non-existent (Phase 02 L8)"


def test_deploy_l2_workflow_covers_every_phase_054_runtime_and_migration_path() -> None:
    text = DEPLOY_L2_YML.read_text()
    required_paths = {
        "src/polyarb/observation/l3_*.py",
        "src/polyarb/storage/l3_evidence_store.py",
        "src/polyarb/config.py",
        "scripts/l3_evidence.py",
        "src/polyarb/daemon/**",
        "src/polyarb/http/l2_*.py",
        "alembic/versions/**",
    }

    for path in sorted(required_paths):
        assert f"- '{path}'" in text, f"deploy-l2 workflow does not cover {path}"


def test_deploy_l2_workflow_keeps_manual_dispatch_and_secret_boundary() -> None:
    text = DEPLOY_L2_YML.read_text()

    assert "workflow_dispatch: {}" in text
    assert "actions/checkout@v4" in text
    assert "superfly/flyctl-actions/setup-flyctl@1.6" in text
    assert "APP: polyarb-l2" in text
    assert "FLY_API_TOKEN: ${{ secrets.FLY_API_TOKEN }}" in text


def test_deploy_l2_workflow_binds_release_id_to_dispatched_sha() -> None:
    text = DEPLOY_L2_YML.read_text()

    assert '--env POLYARB_RELEASE_ID="${GITHUB_SHA}"' in text


def test_deploy_l2_workflow_redeploys_the_built_image_by_external_digest() -> None:
    text = DEPLOY_L2_YML.read_text()

    assert "flyctl image show" in text
    assert "PINNED_IMAGE_REF" in text
    assert '@sha256:' in text
    assert '--image "${PINNED_IMAGE_REF}"' in text
    assert '--env POLYARB_IMAGE_REF="${PINNED_IMAGE_REF}"' in text


def test_deploy_l2_push_event_cannot_execute_deploy_job() -> None:
    text = DEPLOY_L2_YML.read_text()

    assert "push:" in text, "push path filters remain required as reachability proof"
    assert (
        "if: github.event_name == 'workflow_dispatch'" in text
    ), "the production deploy job must be executable only by explicit manual dispatch"
