"""Executable contracts for invisible, atomic Structure generations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from polyarb.storage.sqlite_store import SQLiteStore

COMPONENT_COUNTS = {
    "events": 1,
    "event_tags": 0,
    "memberships": 1,
    "group_truth": 1,
    "markets": 1,
    "issues": 0,
}


def _event(snapshot_id: int) -> dict[str, object]:
    return {
        "id": "event-1",
        "slug": "event-1",
        "title": "Generation publication event",
        "active": 1,
        "closed": 0,
        "fetched_at_ms": snapshot_id * 1_000,
        "snapshot_id": snapshot_id,
    }


def _membership(snapshot_id: int, market_id: str) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "event_id": "event-1",
        "neg_risk_market_id": "group-1",
        "market_id": market_id,
        "member_kind": "named",
        "active": 1,
        "closed": 0,
    }


def _group_truth(
    snapshot_id: int,
    *,
    expected_member_count: int = 1,
) -> dict[str, object]:
    return {
        "snapshot_id": snapshot_id,
        "event_id": "event-1",
        "neg_risk_market_id": "group-1",
        "neg_risk_type": "standard",
        "expected_member_count": expected_member_count,
        "active_named_count": 1,
        "membership_hash": "membership-1",
        "quality": "complete-supported",
        "reason": None,
    }


def _market(market_id: str, snapshot_id: int) -> dict[str, object]:
    return {
        "market_id": market_id,
        "condition_id": f"condition-{market_id}",
        "slug": market_id,
        "question": f"Will {market_id} publish?",
        "yes_token_id": f"yes-{market_id}",
        "no_token_id": f"no-{market_id}",
        "active": 1,
        "closed": 0,
        "neg_risk": 1,
        "neg_risk_market_id": "group-1",
        "fetched_at_ms": snapshot_id * 1_000,
        "snapshot_id": snapshot_id,
        "incomplete": 0,
        "event_id": "event-1",
    }


def _complete_window(store: SQLiteStore, market_id: str, *, now_ms: int) -> str:
    window = store.begin_or_resume_structure_sync(started_at_ms=now_ms)
    window_id = str(window["id"])
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[{"id": "event-1", "active": True, "closed": False}],
        finished_at_ms=now_ms + 1,
    )
    store.commit_structure_market_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[
            {
                "id": market_id,
                "event_id": "event-1",
                "negRisk": True,
                "negRiskMarketID": "group-1",
                "active": True,
                "closed": False,
            }
        ],
        finished_at_ms=now_ms + 2,
    )
    return window_id


def _begin_generation(
    store: SQLiteStore,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
):
    window_id = _complete_window(store, market_id, now_ms=now_ms)
    return store.begin_structure_publication(
        window_id=window_id,
        snapshot_metadata={
            "snapshot_id": snapshot_id,
            "taken_at_ms": now_ms,
            "mode": "full",
            "data_product": "structure",
            "expected_counts": COMPONENT_COUNTS,
        },
        now_ms=now_ms + 3,
    )


def _append_market(
    store: SQLiteStore,
    publication,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
) -> None:
    store.append_structure_publication_chunk(
        publication_id=publication.publication_id,
        component="markets",
        rows=(_market(market_id, snapshot_id),),
        next_cursor=market_id,
        now_ms=now_ms,
    )


def _append_generation_truth(
    store: SQLiteStore,
    publication,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
    expected_member_count: int = 1,
) -> None:
    chunks = (
        ("events", (_event(snapshot_id),), "event-1"),
        (
            "memberships",
            (_membership(snapshot_id, market_id),),
            f"event-1:{market_id}",
        ),
        (
            "group_truth",
            (
                _group_truth(
                    snapshot_id,
                    expected_member_count=expected_member_count,
                ),
            ),
            "group-1",
        ),
        ("markets", (_market(market_id, snapshot_id),), market_id),
    )
    for offset, (component, rows, next_cursor) in enumerate(chunks):
        store.append_structure_publication_chunk(
            publication_id=publication.publication_id,
            component=component,
            rows=rows,
            next_cursor=next_cursor,
            now_ms=now_ms + offset,
        )


def _certify(
    store: SQLiteStore,
    publication,
    *,
    now_ms: int,
    coverage_completed: bool = True,
) -> None:
    store.certify_structure_generation(
        publication_id=publication.publication_id,
        receipt={
            "component_counts": COMPONENT_COUNTS,
            "source_coverage": {
                "completed": coverage_completed,
                "event_items": 1,
                "market_items": 1,
            },
            "membership_validation": {
                "valid": True,
                "expected_member_count": 1,
                "actual_member_count": 1,
            },
            "validation_hash": "a" * 64,
            "certified_at_ms": now_ms,
        },
    )


def _publish_generation(
    store: SQLiteStore,
    *,
    snapshot_id: int,
    market_id: str,
    now_ms: int,
):
    publication = _begin_generation(
        store,
        snapshot_id=snapshot_id,
        market_id=market_id,
        now_ms=now_ms,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=snapshot_id,
        market_id=market_id,
        now_ms=now_ms + 4,
    )
    _certify(store, publication, now_ms=now_ms + 8)
    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=now_ms + 9,
        )
        == snapshot_id
    )
    return publication


def test_generation_publication_attempt_is_invisible_until_pointer_switch(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )

    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_switches_all_reads_after_terminal_receipt(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_008)

    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )
        == 11
    )

    assert store.current_structure_generation()["snapshot_id"] == 11
    assert store.current_generation_market_ids() == ("new-market",)


def test_generation_publication_attempt_rolls_back_pointer_switch_exception(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_008)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "CREATE TRIGGER reject_structure_pointer_switch "
            "BEFORE UPDATE OF snapshot_id ON current_structure_generation "
            "BEGIN SELECT RAISE(ABORT, 'injected-pointer-switch-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-pointer-switch-failure"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_rejects_incomplete_source_coverage(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )

    with pytest.raises(ValueError, match="source-coverage-incomplete"):
        _certify(
            store,
            publication,
            now_ms=11_008,
            coverage_completed=False,
        )
    with pytest.raises(ValueError, match="not-ready"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)


def test_generation_publication_attempt_rejects_invalid_membership_truth(
    tmp_path: Path,
) -> None:
    store = SQLiteStore(tmp_path / "state.db")
    store.init_schema()
    _publish_generation(
        store,
        snapshot_id=10,
        market_id="old-market",
        now_ms=10_000,
    )
    publication = _begin_generation(
        store,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_000,
    )
    _append_generation_truth(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
        expected_member_count=2,
    )

    # The receipt claims 1/1 valid membership. Certification must re-read the
    # stored group truth (expected=2) instead of trusting caller-supplied counts.
    with pytest.raises(ValueError, match="membership-invalid"):
        _certify(
            store,
            publication,
            now_ms=11_008,
        )
    with pytest.raises(ValueError, match="not-ready"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_009,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)
