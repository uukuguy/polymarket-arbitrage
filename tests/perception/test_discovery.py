from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path

import pytest

from polyarb.clients.gamma_client import EventPage
from polyarb.perception.candidate_watcher import (
    CandidateWatcherRuntime,
    CandidateWatcherScheduler,
)
from polyarb.perception.discovery import (
    CandidateFreshness,
    DiscoveryLoadController,
    DiscoveryWorker,
    compose_candidate_group_ids,
)
from polyarb.perception.store import OpportunityPerceptionStore


def _event(
    *,
    event_id: str,
    group_id: str,
    liquidity: str = "100",
    volume: str = "200",
    valid: bool = True,
    augmented: bool = False,
) -> dict:
    markets = [
        {
            "id": f"{group_id}-m1",
            "conditionId": f"{group_id}-c1",
            "clobTokenIds": json.dumps([f"{group_id}-yes1", f"{group_id}-no1"]),
            "question": "One?",
            "active": True,
            "closed": False,
            "negRiskOther": False,
            "groupItemTitle": "One",
        },
        {
            "id": f"{group_id}-m2",
            "conditionId": f"{group_id}-c2",
            "clobTokenIds": json.dumps([f"{group_id}-yes2", f"{group_id}-no2"]),
            "question": "Two?",
            "active": True,
            "closed": False,
            "negRiskOther": False,
            "groupItemTitle": "Two",
        },
    ]
    if not valid:
        markets[1]["active"] = "unknown"
    return {
        "id": event_id,
        "slug": event_id,
        "active": True,
        "closed": False,
        "negRisk": True,
        "enableNegRisk": True,
        "negRiskAugmented": augmented,
        "negRiskMarketID": group_id,
        "liquidity": liquidity,
        "volume": volume,
        "markets": markets,
    }


class FakeGamma:
    def __init__(self, page: EventPage | BaseException) -> None:
        self.page = page
        self.calls: list[tuple[str | None, int]] = []

    async def fetch_active_event_page(
        self,
        cursor: str | None,
        limit: int,
    ) -> EventPage:
        self.calls.append((cursor, limit))
        if isinstance(self.page, BaseException):
            raise self.page
        return self.page


def _page(
    *events: dict,
    requested_cursor: str | None = "c-1",
    next_cursor: str | None = "c-2",
    completed: bool = False,
) -> EventPage:
    return EventPage(
        events=events,
        requested_cursor=requested_cursor,
        next_cursor=next_cursor,
        completed=completed,
        started_at_ms=9_900,
        finished_at_ms=10_000,
    )


def _store(tmp_path: Path) -> OpportunityPerceptionStore:
    db_path = tmp_path / "state.db"
    store = OpportunityPerceptionStore(db_path)
    store.init_schema()
    with sqlite3.connect(db_path) as con:
        con.execute(
            "INSERT INTO neg_risk_discovery_state("
            "id,next_cursor,completed,last_started_at_ms,last_finished_at_ms,"
            "page_event_count,groups_seen,promoted_count"
            ") VALUES (1,'c-1',0,0,0,0,0,0)"
        )
    return store


@pytest.mark.asyncio
async def test_discovery_commits_rows_promotions_coverage_and_cursor_atomically(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(
        _page(
            _event(event_id="e-1", group_id="g-1", liquidity="100"),
            _event(event_id="e-2", group_id="g-2", liquidity="300"),
        )
    )
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        page_limit=2,
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.groups_seen == 2
    assert result.promoted_group_ids == ("g-2", "g-1")
    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1").last_discovered_at_ms == result.finished_at_ms
    assert store.current_group("g-1").status == "certified"
    assert tuple(
        leg.yes_token_id for leg in store.current_group("g-1").legs
    ) == ("g-1-yes1", "g-1-yes2")
    assert store.promoted_group_ids() == ("g-2", "g-1")
    coverage = store.coverage_windows(now_ms=10_000)
    assert coverage.by_minutes[15].raw_fraction == Decimal("1")
    assert coverage.by_minutes[15].liquidity_weighted_fraction == Decimal("1")


@pytest.mark.asyncio
async def test_discovery_rollback_never_advances_cursor_on_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    original = store._insert_discovery_schedule

    def fail_after_row(*args, **kwargs):
        original(*args, **kwargs)
        raise sqlite3.OperationalError("injected")

    monkeypatch.setattr(store, "_insert_discovery_schedule", fail_after_row)
    worker = DiscoveryWorker(gamma=gamma, store=store, clock_ms=lambda: 10_000)

    with pytest.raises(sqlite3.OperationalError, match="injected"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-1"
    assert store.group_schedule("g-1") is None
    assert store.coverage_windows(now_ms=10_000).known_groups == 0


@pytest.mark.asyncio
async def test_upstream_or_normalization_failure_never_advances_cursor(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    upstream = DiscoveryWorker(
        gamma=FakeGamma(RuntimeError("upstream")),
        store=store,
        clock_ms=lambda: 10_000,
    )
    with pytest.raises(RuntimeError, match="upstream"):
        await upstream.run_batch()
    assert store.discovery_cursor() == "c-1"

    invalid = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-1"),
                _event(event_id="e-1", group_id="g-different"),
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
    )
    with pytest.raises(RuntimeError, match="conflict"):
        await invalid.run_batch()
    assert store.discovery_cursor() == "c-1"


@pytest.mark.asyncio
async def test_duplicate_group_identity_in_one_page_fails_batch_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-1"),
                _event(event_id="e-2", group_id="g-1"),
            )
        ),
        store=store,
    )

    with pytest.raises(ValueError, match="duplicate-discovery-group"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-1"
    assert store.group_schedule("g-1") is None


@pytest.mark.asyncio
async def test_restart_uses_durable_cursor_and_terminal_page_restarts_sweep(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    terminal_gamma = FakeGamma(
        _page(
            _event(event_id="e-1", group_id="g-1"),
            requested_cursor="c-1",
            next_cursor=None,
            completed=True,
        )
    )
    await DiscoveryWorker(
        gamma=terminal_gamma,
        store=store,
        clock_ms=lambda: 10_000,
    ).run_batch()

    restart_gamma = FakeGamma(
        _page(requested_cursor=None, next_cursor="new-cursor")
    )
    await DiscoveryWorker(
        gamma=restart_gamma,
        store=store,
        clock_ms=lambda: 20_000,
    ).run_batch()

    assert terminal_gamma.calls == [("c-1", 100)]
    assert restart_gamma.calls == [(None, 100)]
    assert store.discovery_cursor() == "new-cursor"


@pytest.mark.asyncio
async def test_incomplete_and_unsupported_membership_fail_closed(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(
                    event_id="e-1",
                    group_id="g-1",
                    augmented=True,
                )
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.promoted_group_ids == ()
    assert store.group_schedule("g-1").quality == "complete-unsupported"
    assert store.promoted_group_ids() == ()


@pytest.mark.asyncio
async def test_coverage_windows_use_exact_discovery_samples_and_liquidity_weights(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-light", liquidity="100"),
                _event(event_id="e-2", group_id="g-heavy", liquidity="300"),
            )
        ),
        store=store,
    ).run_batch()
    await DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-2",
                        group_id="g-heavy",
                        liquidity="300",
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=999_900,
                finished_at_ms=1_000_000,
            )
        ),
        store=store,
    ).run_batch()

    coverage = store.coverage_windows(now_ms=1_000_000)

    assert coverage.by_minutes[15].visited_groups == 1
    assert coverage.by_minutes[15].raw_fraction == Decimal("0.5")
    assert coverage.by_minutes[15].liquidity_weighted_fraction == Decimal("0.75")
    assert coverage.by_minutes[30].raw_fraction == Decimal("1")
    assert coverage.by_minutes[60].raw_fraction == Decimal("1")


def test_candidate_source_composes_legacy_seed_with_discovery_promotions(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-new")))
    asyncio.run(
        DiscoveryWorker(
            gamma=gamma,
            store=store,
            clock_ms=lambda: 10_000,
        ).run_batch()
    )

    source = compose_candidate_group_ids(lambda: ("g-legacy", "g-new"), store)

    assert source() == ("g-legacy", "g-new")


@pytest.mark.asyncio
async def test_new_promotions_enter_candidate_scheduler_in_discovery_score_order(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="a-low", liquidity="100"),
                _event(event_id="e-2", group_id="z-high", liquidity="300"),
            )
        ),
        store=store,
        clock_ms=lambda: 10_000,
    ).run_batch()
    calls: list[str] = []

    class Watcher:
        async def run_once(self, group_id: str, *, priority_hint: str) -> None:
            calls.append(group_id)

    scheduler = CandidateWatcherScheduler(
        watcher=Watcher(),
        store=store,
        candidate_group_ids=lambda: store.promoted_group_ids(),
        runtime=CandidateWatcherRuntime(),
        clock_ms=lambda: 10_000,
        cycle_max_groups=2,
        reserved_non_high_slots=1,
    )

    await scheduler.run_due_once()

    assert calls == ["z-high", "a-low"]


@pytest.mark.asyncio
async def test_discovery_yields_before_gamma_when_hot_path_is_stale(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    controller = DiscoveryLoadController(candidate_hard_stale_ms=90_000)
    worker = DiscoveryWorker(
        gamma=gamma,
        store=store,
        load_controller=controller,
        candidate_freshness=lambda: CandidateFreshness(
            candidate_count=2,
            quote_p95_age_ms=91_000,
        ),
        clock_ms=lambda: 10_000,
    )

    result = await worker.run_batch()

    assert result.yielded is True
    assert result.yield_reason == "candidate-quote-stale"
    assert gamma.calls == []
    assert store.discovery_cursor() == "c-1"


@pytest.mark.asyncio
async def test_cancellation_during_commit_finishes_one_atomic_batch(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))
    entered = threading.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    entered_async = asyncio.Event()
    original = store.publish_discovery_batch

    def delayed(*args, **kwargs):
        entered.set()
        loop.call_soon_threadsafe(entered_async.set)
        release.wait(timeout=5)
        return original(*args, **kwargs)

    monkeypatch.setattr(store, "publish_discovery_batch", delayed)
    worker = DiscoveryWorker(gamma=gamma, store=store, clock_ms=lambda: 10_000)
    task = asyncio.create_task(worker.run_batch())
    await entered_async.wait()
    task.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert store.discovery_cursor() == "c-2"
    assert store.group_schedule("g-1") is not None
