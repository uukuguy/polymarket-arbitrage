from __future__ import annotations

import math
from dataclasses import FrozenInstanceError

import pytest

from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)


def test_group_revision_hash_covers_ordered_complete_leg_identity() -> None:
    legs = (
        GroupLeg("m-1", "c-1", "yes-1", "First"),
        GroupLeg("m-2", "c-2", "yes-2", "Second"),
    )
    revision = GroupRevision.certified(
        group_id="g-1",
        event_id="e-1",
        revision=7,
        started_at_ms=1_000,
        observed_at_ms=2_000,
        source_cursor="cursor-2",
        legs=legs,
    )

    assert revision.status == "certified"
    assert revision.membership_hash == GroupRevision.membership_digest(legs)
    assert revision.membership_hash != GroupRevision.membership_digest(tuple(reversed(legs)))


def test_group_revision_requires_complete_timestamp_ordered_membership() -> None:
    leg = GroupLeg("m-1", "c-1", "yes-1", "First")

    with pytest.raises(ValueError, match="incomplete-group-membership"):
        GroupRevision.certified(
            group_id="g-1",
            event_id="e-1",
            revision=1,
            started_at_ms=1_000,
            observed_at_ms=2_000,
            source_cursor="cursor-1",
            legs=(leg,),
        )
    with pytest.raises(ValueError, match="invalid-timestamp-order"):
        GroupRevision.certified(
            group_id="g-1",
            event_id="e-1",
            revision=1,
            started_at_ms=2_001,
            observed_at_ms=2_000,
            source_cursor="cursor-1",
            legs=(leg, GroupLeg("m-2", "c-2", "yes-2", "Second")),
        )


def test_group_revision_models_are_immutable() -> None:
    leg = GroupLeg("m-1", "c-1", "yes-1", "First")

    with pytest.raises(FrozenInstanceError):
        leg.title = "Changed"  # type: ignore[misc]


def test_quote_batch_rejects_a_leg_from_another_membership() -> None:
    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_000,
            quoted_at_ms=3_100,
            legs=(
                GroupQuoteLeg("yes-1", "hash-b", 0.40, 10.0, "executable"),
                GroupQuoteLeg("yes-2", "hash-a", 0.50, 12.0, "executable"),
            ),
        )


@pytest.mark.parametrize("value", [0.0, -0.1, math.inf, -math.inf, math.nan])
@pytest.mark.parametrize("field", ["best_ask_price", "best_ask_size"])
def test_quote_batch_requires_positive_finite_ask_values(
    field: str, value: float
) -> None:
    values = {"best_ask_price": 0.40, "best_ask_size": 10.0}
    values[field] = value

    with pytest.raises(ValueError, match=f"invalid-{field.replace('_', '-')}"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_000,
            quoted_at_ms=3_100,
            legs=(
                GroupQuoteLeg(
                    "yes-1",
                    "hash-a",
                    values["best_ask_price"],
                    values["best_ask_size"],
                    "executable",
                ),
                GroupQuoteLeg("yes-2", "hash-a", 0.50, 12.0, "executable"),
            ),
        )


def test_quote_batch_requires_unique_all_leg_identity_and_timestamp_order() -> None:
    duplicate_legs = (
        GroupQuoteLeg("yes-1", "hash-a", 0.40, 10.0, "executable"),
        GroupQuoteLeg("yes-1", "hash-a", 0.50, 12.0, "executable"),
    )

    with pytest.raises(ValueError, match="duplicate-quote-leg"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_000,
            quoted_at_ms=3_100,
            legs=duplicate_legs,
        )
    with pytest.raises(ValueError, match="invalid-timestamp-order"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_101,
            quoted_at_ms=3_100,
            legs=(
                GroupQuoteLeg("yes-1", "hash-a", 0.40, 10.0, "executable"),
                GroupQuoteLeg("yes-2", "hash-a", 0.50, 12.0, "executable"),
            ),
        )


def test_complete_quote_batch_contains_only_executable_legs() -> None:
    with pytest.raises(ValueError, match="incomplete-quote-leg"):
        GroupQuoteBatch.complete(
            group_id="g-1",
            membership_hash="hash-a",
            quote_batch_id="qb-1",
            started_at_ms=3_000,
            quoted_at_ms=3_100,
            legs=(
                GroupQuoteLeg("yes-1", "hash-a", 0.40, 10.0, "failed"),
                GroupQuoteLeg("yes-2", "hash-a", 0.50, 12.0, "executable"),
            ),
        )
