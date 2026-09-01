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


def test_control_plane_capacity_probe_returns_a_bounded_relation_breakdown() -> None:
    """Capacity must be independently observable without exposing row data."""
    from polyarb.control_plane.postgres import PostgresControlPlane

    class Cursor:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def execute(self, statement: str) -> None:
            self.calls.append(statement)

        def fetchone(self) -> dict[str, int]:
            return {"used_bytes": 300}

        def fetchall(self) -> list[dict[str, object]]:
            return [
                {"relation": "m1_job_attempts", "used_bytes": 120},
                {"relation": "m1_incident_events", "used_bytes": 80},
            ]

        def __enter__(self) -> "Cursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    class Connection:
        def __init__(self, cursor: Cursor) -> None:
            self.cursor_instance = cursor

        def cursor(self, *, row_factory: object) -> Cursor:
            assert row_factory is not None
            return self.cursor_instance

        def __enter__(self) -> "Connection":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

    cursor = Cursor()
    control_plane = PostgresControlPlane(lambda: Connection(cursor), database_capacity_budget_bytes=500)

    assert control_plane.database_capacity() == {
        "state": "warning",
        "used_bytes": 300,
        "budget_bytes": 500,
        "used_percent": 60,
        "reason_code": "budget-warning",
        "largest_relations": [
            {"relation": "m1_job_attempts", "used_bytes": 120},
            {"relation": "m1_incident_events", "used_bytes": 80},
        ],
    }
    assert any("pg_database_size" in statement for statement in cursor.calls)
    assert any("LIMIT 10" in statement for statement in cursor.calls)
