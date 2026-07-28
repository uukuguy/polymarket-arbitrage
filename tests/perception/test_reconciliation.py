from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from polyarb.clients.gamma_client import EventPage
from polyarb.config import Settings
from polyarb.perception.models import GroupLeg, GroupRevision
from polyarb.perception.reconciliation import (
    ReconciliationIncompleteError,
    ReconciliationWorker,
)
from polyarb.perception.store import OpportunityPerceptionStore


def _event(event_id: str, group_id: str, *, suffix: str = "a") -> dict:
    return {
        "id": event_id,
        "slug": event_id,
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": False,
        "negRiskMarketID": group_id,
        "liquidity": "100",
        "volume": "200",
        "markets": [
            {
                "id": f"{group_id}-m1-{suffix}",
                "conditionId": f"{group_id}-c1-{suffix}",
                "clobTokenIds": json.dumps(
                    [f"{group_id}-yes1-{suffix}", f"{group_id}-no1-{suffix}"]
                ),
                "question": "One?",
                "active": True,
                "closed": False,
                "negRiskOther": False,
                "groupItemTitle": "One",
            },
            {
                "id": f"{group_id}-m2-{suffix}",
                "conditionId": f"{group_id}-c2-{suffix}",
                "clobTokenIds": json.dumps(
                    [f"{group_id}-yes2-{suffix}", f"{group_id}-no2-{suffix}"]
                ),
                "question": "Two?",
                "active": True,
                "closed": False,
                "negRiskOther": False,
                "groupItemTitle": "Two",
            },
        ],
    }


def _page(
    *events: dict,
    requested: str | None,
    next_cursor: str | None,
    started: int,
    finished: int,
) -> EventPage:
    return EventPage(
        events=events,
        requested_cursor=requested,
        next_cursor=next_cursor,
        completed=next_cursor is None,
        started_at_ms=started,
        finished_at_ms=finished,
    )


class FakeGamma:
    def __init__(self, pages: list[EventPage]) -> None:
        self.pages = pages
        self.calls: list[tuple[str | None, int]] = []

    async def fetch_active_event_page(self, cursor: str | None, limit: int) -> EventPage:
        self.calls.append((cursor, limit))
        return self.pages.pop(0)


def _store(path: Path) -> OpportunityPerceptionStore:
    store = OpportunityPerceptionStore(path)
    store.init_schema()
    return store


def _revision(
    group_id: str,
    *,
    revision: int,
    observed_at_ms: int,
    suffix: str = "a",
) -> GroupRevision:
    legs = (
        GroupLeg(f"m1-{suffix}", f"c1-{suffix}", f"yes1-{suffix}", "One"),
        GroupLeg(f"m2-{suffix}", f"c2-{suffix}", f"yes2-{suffix}", "Two"),
    )
    return GroupRevision.certified(
        group_id=group_id,
        event_id=f"e-{group_id}",
        revision=revision,
        started_at_ms=observed_at_ms - 10,
        observed_at_ms=observed_at_ms,
        source_cursor="online",
        legs=legs,
    )


@pytest.mark.asyncio
async def test_restart_resumes_after_last_committed_cursor(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = _store(path)
    first_gamma = FakeGamma(
        [_page(_event("e-1", "g-1"), requested=None, next_cursor="c-2", started=1, finished=2)]
    )

    first = await ReconciliationWorker(gamma=first_gamma, store=store, page_limit=1).run_batch()
    restarted_gamma = FakeGamma(
        [_page(_event("e-2", "g-2"), requested="c-2", next_cursor="c-3", started=3, finished=4)]
    )
    second = await ReconciliationWorker(
        gamma=restarted_gamma, store=_store(path), page_limit=1
    ).run_batch()

    assert first.next_cursor == "c-2"
    assert second.requested_cursor == "c-2"
    assert restarted_gamma.calls == [("c-2", 1)]
    assert store.current_reconciliation().pages_completed == 2


def test_incomplete_window_cannot_replace_online_group_revision(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "state.db")
    store.publish_group_revision(_revision("g-1", revision=3, observed_at_ms=3_000))
    window = store.begin_reconciliation(started_at_ms=4_000)
    store.stage_reconciliation_group(
        window.id,
        _revision("g-1", revision=1, observed_at_ms=4_100, suffix="b"),
        quality="complete-supported",
        reason=None,
    )

    with pytest.raises(ReconciliationIncompleteError):
        store.apply_reconciliation_diff(window.id)

    assert store.current_group("g-1").revision == 3


@pytest.mark.asyncio
async def test_terminal_batch_applies_one_atomic_diff_and_is_idempotent(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.db"
    store = _store(path)
    store.publish_group_revision(_revision("changed", revision=1, observed_at_ms=50, suffix="old"))
    store.publish_group_revision(_revision("closed", revision=1, observed_at_ms=50))
    gamma = FakeGamma(
        [
            _page(
                _event("e-changed", "changed", suffix="new"),
                _event("e-added", "added"),
                requested=None,
                next_cursor="c-2",
                started=100,
                finished=110,
            ),
            _page(
                requested="c-2",
                next_cursor=None,
                started=120,
                finished=130,
            ),
        ]
    )
    worker = ReconciliationWorker(gamma=gamma, store=store, page_limit=2)

    await worker.run_batch()
    terminal = await worker.run_batch()
    again = store.apply_reconciliation_diff(terminal.window_id)

    assert terminal.completed is True
    assert terminal.diff is not None
    assert terminal.diff.added == 1
    assert terminal.diff.changed == 1
    assert terminal.diff.closed == 1
    assert terminal.diff.unchanged == 0
    assert terminal.diff.rejected == 0
    assert terminal.diff.started_at_ms == 100
    assert terminal.diff.finished_at_ms == 130
    assert again == terminal.diff
    assert store.current_group("changed").revision == 2
    assert store.current_group("closed").status == "closed"
    applied = store.current_reconciliation()
    assert (
        applied.added_count,
        applied.changed_count,
        applied.closed_count,
        applied.unchanged_count,
        applied.applied_rejected_count,
    ) == (1, 1, 1, 0, 0)


@pytest.mark.asyncio
async def test_concurrent_online_revision_wins_over_older_reconciliation(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "state.db")
    store.publish_group_revision(_revision("g-1", revision=1, observed_at_ms=50, suffix="old"))
    gamma = FakeGamma(
        [
            _page(
                _event("e-1", "g-1", suffix="recon"),
                requested=None,
                next_cursor="c-2",
                started=100,
                finished=110,
            ),
            _page(requested="c-2", next_cursor=None, started=120, finished=130),
        ]
    )
    worker = ReconciliationWorker(gamma=gamma, store=store)
    await worker.run_batch()
    online = _revision("g-1", revision=2, observed_at_ms=115, suffix="online")
    store.publish_group_revision(online)

    terminal = await worker.run_batch()

    assert terminal.diff.changed == 0
    assert terminal.diff.unchanged == 1
    assert store.current_group("g-1") == online


@pytest.mark.asyncio
async def test_rejected_identity_is_reported_but_never_closes_known_group(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path / "state.db")
    store.publish_group_revision(_revision("g-1", revision=1, observed_at_ms=50))
    invalid = _event("e-1", "g-1")
    invalid["markets"][1]["conditionId"] = ""
    gamma = FakeGamma(
        [
            _page(
                invalid,
                requested=None,
                next_cursor="c-2",
                started=100,
                finished=110,
            ),
            _page(requested="c-2", next_cursor=None, started=120, finished=130),
        ]
    )
    worker = ReconciliationWorker(gamma=gamma, store=store)

    await worker.run_batch()
    terminal = await worker.run_batch()

    assert terminal.diff.rejected == 1
    assert terminal.diff.closed == 0
    assert store.current_group("g-1").status == "certified"


@pytest.mark.asyncio
async def test_page_receipt_cursor_and_staging_commit_atomically(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "state.db")
    gamma = FakeGamma(
        [_page(_event("e-1", "g-1"), requested=None, next_cursor="c-2", started=1, finished=2)]
    )
    worker = ReconciliationWorker(gamma=gamma, store=store)
    original = store._stage_reconciliation_sample

    def fail_after_stage(*args, **kwargs):
        original(*args, **kwargs)
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(store, "_stage_reconciliation_sample", fail_after_stage)

    with pytest.raises(sqlite3.OperationalError, match="injected"):
        await worker.run_batch()

    window = store.current_reconciliation()
    assert window is None or window.pages_completed == 0


@pytest.mark.asyncio
async def test_apply_diff_rolls_back_every_group_on_mid_apply_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = _store(tmp_path / "state.db")
    gamma = FakeGamma(
        [
            _page(
                _event("e-1", "g-1"),
                _event("e-2", "g-2"),
                requested=None,
                next_cursor="c-2",
                started=100,
                finished=110,
            ),
            _page(requested="c-2", next_cursor=None, started=120, finished=130),
        ]
    )
    worker = ReconciliationWorker(gamma=gamma, store=store)
    await worker.run_batch()
    original = store._insert_group_revision
    calls = 0

    def fail_second(*args, **kwargs):
        nonlocal calls
        calls += 1
        original(*args, **kwargs)
        if calls == 2:
            raise sqlite3.OperationalError("mid-apply")

    monkeypatch.setattr(store, "_insert_group_revision", fail_second)

    with pytest.raises(sqlite3.OperationalError, match="mid-apply"):
        await worker.run_batch()

    assert store.current_group("g-1") is None
    assert store.current_group("g-2") is None
    assert store.current_reconciliation().status == "complete"


def test_reconciliation_and_legacy_structure_are_default_off() -> None:
    settings = Settings(_env_file=None)

    assert settings.opportunity_reconciliation_enabled is False
    assert settings.legacy_structure_reconciliation_enabled is False
