from __future__ import annotations

from pathlib import Path


def test_make_exposes_bounded_cloud_perception_contract() -> None:
    text = Path("Makefile").read_text()
    for target in (
        "perception-status",
        "perception-groups",
        "perception-incidents",
        "queue-discovery",
        "queue-reconciliation",
    ):
        assert f"## {target}:" in text
        assert f"\n{target}:" in text
    assert "--max-time" in text
    assert "POLYARB_SCAN_SHARED_SECRET" in text


def test_make_watchdog_verification_requires_live_identity_facts() -> None:
    text = Path("Makefile").read_text()

    assert "## control-plane-render-rollout:" in text
    assert "\ncontrol-plane-render-rollout:" in text
    assert "## control-plane-watchdog-verify:" in text
    assert "\ncontrol-plane-watchdog-verify:" in text
    assert "machine_ids" in text
    assert "--watchdog-once" in text
    for stale_machine_id in (
        "3d8d0e29c7d589",
        "080d3ddbe66068",
        "4d895231f7d987",
        "85e990c43533e8",
        "86ed91bee33608",
    ):
        assert stale_machine_id not in text
