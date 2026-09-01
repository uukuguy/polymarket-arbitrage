"""Capacity verdict contracts for the M1 production recovery closure."""

from __future__ import annotations

import pytest


@pytest.mark.parametrize(
    ("used_bytes", "expected_state"),
    [
        (599, "healthy"),
        (600, "warning"),
        (749, "warning"),
        (750, "critical"),
        (849, "critical"),
        (850, "exhausted"),
    ],
)
def test_database_capacity_state_uses_fixed_budget_thresholds(
    used_bytes: int, expected_state: str
) -> None:
    from polyarb.control_plane.capacity import classify_database_capacity

    verdict = classify_database_capacity(
        used_bytes=used_bytes,
        budget_bytes=1_000,
        provider_read_only=False,
    )

    assert verdict.state == expected_state
    assert verdict.used_percent == used_bytes * 100 // 1_000


def test_database_capacity_read_only_is_exhausted_even_below_budget() -> None:
    from polyarb.control_plane.capacity import classify_database_capacity

    verdict = classify_database_capacity(
        used_bytes=1,
        budget_bytes=1_000,
        provider_read_only=True,
    )

    assert verdict.state == "exhausted"
    assert verdict.reason_code == "provider-read-only"


@pytest.mark.parametrize("used_bytes,budget_bytes", [(0, 0), (-1, 1), (1, -1)])
def test_database_capacity_rejects_invalid_byte_values(
    used_bytes: int, budget_bytes: int
) -> None:
    from polyarb.control_plane.capacity import classify_database_capacity

    with pytest.raises(ValueError, match="bytes"):
        classify_database_capacity(
            used_bytes=used_bytes,
            budget_bytes=budget_bytes,
            provider_read_only=False,
        )
