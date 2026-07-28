from __future__ import annotations

import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from polyarb.perception.models import (
    GroupLeg,
    GroupQuoteBatch,
    GroupQuoteLeg,
    GroupRevision,
)
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.storage.sqlite_store import SQLiteStore


def revision(
    *,
    group_id: str,
    revision: int,
    token_suffix: str,
    observed_at_ms: int = 2_000,
) -> GroupRevision:
    return GroupRevision.certified(
        group_id=group_id,
        event_id=f"event-{group_id}",
        revision=revision,
        started_at_ms=1_000,
        observed_at_ms=observed_at_ms,
        source_cursor=f"cursor-{revision}",
        legs=(
            GroupLeg(
                f"market-1-{token_suffix}",
                f"condition-1-{token_suffix}",
                f"yes-1-{token_suffix}",
                "First",
            ),
            GroupLeg(
                f"market-2-{token_suffix}",
                f"condition-2-{token_suffix}",
                f"yes-2-{token_suffix}",
                "Second",
            ),
        ),
    )


def batch_for(
    group: GroupRevision,
    *,
    quote_batch_id: str,
    quoted_at_ms: int = 3_100,
) -> GroupQuoteBatch:
    return GroupQuoteBatch.complete(
        group_id=group.group_id,
        membership_hash=group.membership_hash,
        quote_batch_id=quote_batch_id,
        started_at_ms=3_000,
        quoted_at_ms=quoted_at_ms,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.40 + index * 0.10,
                10.0 + index,
                "executable",
            )
            for index, leg in enumerate(group.legs)
        ),
    )


def test_sqlite_schema_initialization_adds_perception_tables(tmp_path: Path) -> None:
    db_path = tmp_path / "state.db"

    SQLiteStore(db_path).init_schema()

    with sqlite3.connect(db_path) as con:
        tables = {
            row[0]
            for row in con.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "neg_risk_group_revisions" in tables
    assert "neg_risk_group_quote_batches" in tables


def test_membership_change_invalidates_previous_quote_atomically(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(first)
    store.publish_quote_batch(batch_for(first, quote_batch_id="qb-1"))

    changed = revision(group_id="g-1", revision=2, token_suffix="b")
    store.publish_group_revision(changed)

    assert store.current_group("g-1") == changed
    assert (
        store.current_quote_batch("g-1", now_ms=10_000, max_age_ms=60_000)
        is None
    )
    with sqlite3.connect(db_path) as con:
        status = con.execute(
            "SELECT status FROM neg_risk_group_quote_batches WHERE id='qb-1'"
        ).fetchone()[0]
    assert status == "superseded"


def test_current_quote_batch_requires_current_complete_all_leg_identity(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)

    wrong_group = replace(batch_for(group, quote_batch_id="qb-wrong-group"), group_id="g-2")
    wrong_hash = replace(
        batch_for(group, quote_batch_id="qb-wrong-hash"),
        membership_hash="other-hash",
    )
    missing_leg = replace(
        batch_for(group, quote_batch_id="qb-missing"),
        legs=batch_for(group, quote_batch_id="ignored").legs[:1],
    )

    with pytest.raises(ValueError, match="group-identity-mismatch"):
        store.publish_quote_batch(wrong_group)
    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        store.publish_quote_batch(wrong_hash)
    with pytest.raises(ValueError, match="quote-leg-identity-mismatch"):
        store.publish_quote_batch(missing_leg)

    expected = batch_for(group, quote_batch_id="qb-1")
    assert store.publish_quote_batch(expected) == expected
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == expected


def test_quote_batch_fails_closed_when_no_certified_group_exists(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")

    with pytest.raises(ValueError, match="certified-group-not-found"):
        store.publish_quote_batch(batch_for(group, quote_batch_id="qb-1"))


def test_current_quote_batch_excludes_stale_and_future_observations(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    store.publish_quote_batch(
        batch_for(group, quote_batch_id="qb-1", quoted_at_ms=3_100)
    )

    assert store.current_quote_batch("g-1", now_ms=4_101, max_age_ms=1_000) is None
    assert store.current_quote_batch("g-1", now_ms=3_000, max_age_ms=1_000) is None


def test_same_membership_revision_keeps_current_quote_authority(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(first, quote_batch_id="qb-1")
    store.publish_group_revision(first)
    store.publish_quote_batch(quote)
    unchanged = replace(
        first,
        revision=2,
        observed_at_ms=2_500,
        source_cursor="cursor-2",
    )

    store.publish_group_revision(unchanged)

    assert store.current_group("g-1") == unchanged
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == quote


def test_revision_numbers_are_append_only_and_monotonic(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    current = revision(group_id="g-1", revision=2, token_suffix="a")
    store.publish_group_revision(current)

    with pytest.raises(ValueError, match="group-revision-not-monotonic"):
        store.publish_group_revision(
            revision(group_id="g-1", revision=1, token_suffix="b")
        )

    assert store.current_group("g-1") == current


def test_publish_revision_rejects_forged_membership_hash(tmp_path: Path) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    first = revision(group_id="g-1", revision=1, token_suffix="a")
    quote = batch_for(first, quote_batch_id="qb-1")
    store.publish_group_revision(first)
    store.publish_quote_batch(quote)
    changed = revision(group_id="g-1", revision=2, token_suffix="b")
    forged = replace(changed, membership_hash=first.membership_hash)

    with pytest.raises(ValueError, match="membership-hash-mismatch"):
        store.publish_group_revision(forged)

    assert store.current_group("g-1") == first
    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) == quote


def test_publish_quote_batch_revalidates_complete_model_invariants(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    group = revision(group_id="g-1", revision=1, token_suffix="a")
    store.publish_group_revision(group)
    valid = batch_for(group, quote_batch_id="qb-1")
    forged_leg = replace(valid.legs[0], best_ask_price=float("nan"))
    forged = replace(valid, legs=(forged_leg, *valid.legs[1:]))

    with pytest.raises(ValueError, match="invalid-best-ask-price"):
        store.publish_quote_batch(forged)

    assert store.current_quote_batch("g-1", now_ms=3_200, max_age_ms=1_000) is None


def test_publish_certified_revision_revalidates_model_invariants(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    valid = revision(group_id="g-1", revision=1, token_suffix="a")
    one_leg = valid.legs[:1]
    forged = replace(
        valid,
        membership_hash=GroupRevision.membership_digest(one_leg),
        legs=one_leg,
    )

    with pytest.raises(ValueError, match="incomplete-group-membership"):
        store.publish_group_revision(forged)

    assert store.current_group("g-1") is None
