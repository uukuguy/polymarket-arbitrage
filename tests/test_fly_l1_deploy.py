"""Structural contracts for the L1 production deployment workflow."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_L1_YML = ROOT / ".github/workflows/deploy.yml"


def test_deploy_l1_workflow_binds_release_id_to_deployed_sha() -> None:
    text = DEPLOY_L1_YML.read_text()

    assert '--env POLYARB_RELEASE_ID="${GITHUB_SHA}"' in text
