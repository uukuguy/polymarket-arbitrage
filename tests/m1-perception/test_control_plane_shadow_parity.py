"""Offline acceptance contract for three transactional Structure/Quote shadow runs."""

from __future__ import annotations

import pytest


def _sample(run_id: str) -> dict[str, object]:
    digest = "a" * 64
    counts = {
        "events": 2,
        "event_tags": 1,
        "memberships": 2,
        "group_truth": 1,
        "markets": 2,
        "issues": 0,
    }
    source = {
        "publication_id": "publication-1",
        "window_id": "window-1",
        "snapshot_id": 42,
        "comparison_receipt_digest": digest,
    }
    return {
        "run_id": run_id,
        "legacy": {
            "source_identity": source,
            "bundle_digest": digest,
            "component_counts": counts,
            "quote_universe_hash": digest,
        },
        "transactional": {
            "source_identity": source,
            "bundle_digest": digest,
            "manifest_digest": "b" * 64,
            "component_counts": counts,
            "quote_universe_hash": digest,
            "legacy_pointer_mutations": 0,
        },
    }


def test_shadow_parity_verdict_requires_three_complete_matching_runs() -> None:
    from polyarb.control_plane.shadow_parity import verify_shadow_parity

    verdict = verify_shadow_parity({"runs": [_sample("run-1"), _sample("run-2"), _sample("run-3")]})

    assert verdict == {
        "status": "PASS",
        "required_runs": 3,
        "verified_runs": ["run-1", "run-2", "run-3"],
        "legacy_pointer_mutations": 0,
    }


@pytest.mark.parametrize(
    "evidence,reason",
    [
        ({"runs": [_sample("run-1"), _sample("run-2")]}, "requires exactly three"),
        (
            {
                "runs": [
                    _sample("run-1"),
                    _sample("run-2"),
                    {
                        **_sample("run-3"),
                        "transactional": {
                            **_sample("run-3")["transactional"],
                            "legacy_pointer_mutations": 1,
                        },
                    },
                ]
            },
            "legacy pointer mutation",
        ),
    ],
)
def test_shadow_parity_verdict_rejects_incomplete_or_mutating_evidence(
    evidence: dict[str, object], reason: str
) -> None:
    from polyarb.control_plane.shadow_parity import ShadowParityError, verify_shadow_parity

    with pytest.raises(ShadowParityError, match=reason):
        verify_shadow_parity(evidence)
