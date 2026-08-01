"""Executable contracts for invisible, atomic Structure generations."""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from polyarb.storage.sqlite_store import SQLiteStore

COMPONENT_COUNTS = {
    "events": 0,
    "event_tags": 0,
    "memberships": 0,
    "group_truth": 0,
    "markets": 1,
    "issues": 0,
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
        "neg_risk": 0,
        "fetched_at_ms": snapshot_id * 1_000,
        "snapshot_id": snapshot_id,
        "incomplete": 0,
    }


def _complete_window(store: SQLiteStore, market_id: str, *, now_ms: int) -> str:
    window = store.begin_or_resume_structure_sync(started_at_ms=now_ms)
    window_id = str(window["id"])
    store.commit_structure_event_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        events=[],
        finished_at_ms=now_ms + 1,
    )
    store.commit_structure_market_page(
        window_id=window_id,
        requested_cursor=None,
        next_cursor=None,
        completed=True,
        markets=[{"id": market_id, "active": True, "closed": False}],
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


def _certify(store: SQLiteStore, publication, *, now_ms: int) -> None:
    store.certify_structure_generation(
        publication_id=publication.publication_id,
        receipt={
            "component_counts": COMPONENT_COUNTS,
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
    _append_market(
        store,
        publication,
        snapshot_id=snapshot_id,
        market_id=market_id,
        now_ms=now_ms + 4,
    )
    _certify(store, publication, now_ms=now_ms + 5)
    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=now_ms + 6,
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
    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_005)

    assert (
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_006,
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
    _append_market(
        store,
        publication,
        snapshot_id=11,
        market_id="new-market",
        now_ms=11_004,
    )
    _certify(store, publication, now_ms=11_005)
    with sqlite3.connect(store.db_path) as con:
        con.execute(
            "CREATE TRIGGER reject_structure_pointer_switch "
            "BEFORE UPDATE OF snapshot_id ON current_structure_generation "
            "BEGIN SELECT RAISE(ABORT, 'injected-pointer-switch-failure'); END"
        )

    with pytest.raises(sqlite3.IntegrityError, match="injected-pointer-switch-failure"):
        store.publish_structure_generation(
            publication_id=publication.publication_id,
            now_ms=11_006,
        )

    assert store.current_structure_generation()["snapshot_id"] == 10
    assert store.current_generation_market_ids() == ("old-market",)
