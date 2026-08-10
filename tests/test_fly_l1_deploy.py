"""Structural contracts for the L1 production deployment workflow."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_L1_YML = ROOT / ".github/workflows/deploy.yml"


def test_deploy_l1_workflow_binds_release_id_to_deployed_sha() -> None:
    text = DEPLOY_L1_YML.read_text()

    assert '--env POLYARB_RELEASE_ID="${GITHUB_SHA}"' in text
    assert "--ha=false --max-concurrent 1" in text


def test_deploy_l1_workflow_ignores_documentation_only_pushes() -> None:
    """Closure evidence commits must not restart the production quote window."""
    text = DEPLOY_L1_YML.read_text()

    assert "paths-ignore:" in text
    assert "- '.planning/**'" in text
    assert "- 'docs/**'" in text
    assert "- '**/*.md'" in text


def test_deploy_l1_workflow_requires_resident_polywatch_before_smoke() -> None:
    text = DEPLOY_L1_YML.read_text()

    assert "Verify and repair resident Polywatch" in text
    assert "scripts/polywatch/resident_watchdog.py --repair" in text
    assert text.index("Verify and repair resident Polywatch") < text.index("Smoke test /health")
