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
