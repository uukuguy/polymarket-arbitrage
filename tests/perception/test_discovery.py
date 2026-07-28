from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
from decimal import Decimal
from pathlib import Path

import pytest

from polyarb.cli_discovery import main as discovery_status_main
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
from polyarb.perception.group_structure import (
    GroupStructureReader,
    GroupStructureUnavailableError,
)
from polyarb.perception.models import GroupQuoteBatch, GroupQuoteLeg
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


def _publish_quote(
    store: OpportunityPerceptionStore,
    group_id: str,
    *,
    quoted_at_ms: int,
) -> None:
    group = store.current_group(group_id)
    store.publish_quote_batch(
        GroupQuoteBatch.complete(
            group_id=group_id,
            membership_hash=group.membership_hash,
            quote_batch_id=f"qb-{group_id}-{quoted_at_ms}",
            started_at_ms=quoted_at_ms - 1,
            quoted_at_ms=quoted_at_ms,
            legs=tuple(
                GroupQuoteLeg(
                    leg.yes_token_id,
                    group.membership_hash,
                    0.4,
                    10,
                    "executable",
                )
                for leg in group.legs
            ),
        )
    )


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
async def test_same_group_and_membership_cannot_migrate_event_identity(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    before = store.current_group("g-1")
    schedule_before = store.group_schedule("g-1")
    migrated = _event(event_id="e-2", group_id="g-1")
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(migrated,),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    with pytest.raises(ValueError, match="event-identity-conflict"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-2"
    assert store.current_group("g-1") == before
    assert store.group_schedule("g-1") == schedule_before


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
async def test_incomplete_rediscovery_revokes_prior_group_and_quote_authority(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    group = store.current_group("g-1")
    quote = GroupQuoteBatch.complete(
        group_id="g-1",
        membership_hash=group.membership_hash,
        quote_batch_id="qb-1",
        started_at_ms=10_001,
        quoted_at_ms=10_002,
        legs=tuple(
            GroupQuoteLeg(
                leg.yes_token_id,
                group.membership_hash,
                0.4,
                10,
                "executable",
            )
            for leg in group.legs
        ),
    )
    store.publish_quote_batch(quote)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-1",
                        group_id="g-1",
                        augmented=True,
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    await worker.run_batch()

    assert store.discovery_cursor() == "c-3"
    assert store.group_schedule("g-1").quality == "complete-unsupported"
    assert store.promoted_group_ids() == ()
    assert store.current_group("g-1").status == "invalidated"
    assert store.current_quote_batch("g-1", 20_100, 60_000) is None
    with pytest.raises(GroupStructureUnavailableError):
        await GroupStructureReader(store).read_group("g-1")


@pytest.mark.asyncio
async def test_failed_revocation_batch_rolls_back_authority_and_cursor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    _publish_quote(store, "g-1", quoted_at_ms=10_100)
    original = store._insert_discovery_schedule

    def fail_after_revocation(*args, **kwargs):
        original(*args, **kwargs)
        raise sqlite3.OperationalError("injected-revocation")

    monkeypatch.setattr(store, "_insert_discovery_schedule", fail_after_revocation)
    worker = DiscoveryWorker(
        gamma=FakeGamma(
            EventPage(
                events=(
                    _event(
                        event_id="e-1",
                        group_id="g-1",
                        augmented=True,
                    ),
                ),
                requested_cursor="c-2",
                next_cursor="c-3",
                completed=False,
                started_at_ms=20_000,
                finished_at_ms=20_100,
            )
        ),
        store=store,
    )

    with pytest.raises(sqlite3.OperationalError, match="injected-revocation"):
        await worker.run_batch()

    assert store.discovery_cursor() == "c-2"
    assert store.current_group("g-1").status == "certified"
    assert store.current_quote_batch("g-1", 20_100, 60_000) is not None


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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "corruption",
    [
        "UPDATE neg_risk_discovery_state SET groups_seen=999",
        "UPDATE neg_risk_group_schedule SET priority_score='999'",
        "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=999999",
        "UPDATE neg_risk_group_schedule SET activity_rank='NaN'",
        "UPDATE neg_risk_group_schedule SET promoted_at_ms=NULL",
        "INSERT INTO neg_risk_discovery_load_state("
        "id,degraded_streak,last_reason,last_decision,updated_at_ms"
        ") VALUES (1,1,'candidate-quote-stale','fresh',1)",
    ],
)
async def test_status_rejects_direct_semantic_corruption_without_leak(
    tmp_path: Path,
    capsys,
    corruption: str,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    db_path = tmp_path / "state.db"
    with sqlite3.connect(db_path) as con:
        con.execute(corruption)

    assert discovery_status_main(["--db-path", str(db_path)]) == 2
    captured = capsys.readouterr()
    assert str(db_path) not in captured.err
    assert "Traceback" not in captured.err


@pytest.mark.asyncio
async def test_durable_candidate_freshness_covers_all_promoted_certified_groups(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="g-fresh"),
                _event(event_id="e-2", group_id="g-stale"),
            )
        ),
        store=store,
    ).run_batch()
    _publish_quote(store, "g-fresh", quoted_at_ms=99_000)
    _publish_quote(store, "g-stale", quoted_at_ms=10_000)
    stale = store.current_group("g-stale")
    store.record_candidate_watch_fact(
        group_id="g-stale",
        membership_hash=stale.membership_hash,
        quote_batch_id=None,
        observed_at_ms=99_500,
        last_result="unavailable",
        reason="fixture",
        bundle_cost=None,
        gross_edge_bps=None,
        max_bundle_size=None,
        priority_class="high",
        consecutive_failures=1,
        effective_interval_s=1,
        schedule_reason="fixture",
        next_due_at_ms=100_500,
    )

    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)
    restarted = OpportunityPerceptionStore(tmp_path / "state.db")

    assert snapshot.candidate_count == 2
    assert snapshot.missing_quote_count == 0
    assert snapshot.quote_p95_age_ms == 90_000
    assert restarted.candidate_freshness_snapshot(now_ms=100_000) == snapshot


def test_empty_durable_candidate_set_allows_discovery_bootstrap(
    tmp_path: Path,
) -> None:
    store = OpportunityPerceptionStore(tmp_path / "state.db")
    store.init_schema()
    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)

    assert snapshot.candidate_count == 0
    assert snapshot.missing_quote_count == 0
    assert snapshot.quote_p95_age_ms is None


@pytest.mark.asyncio
async def test_missing_durable_quote_yields_discovery_but_empty_set_does_not(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(_page(_event(event_id="e-1", group_id="g-1"))),
        store=store,
    ).run_batch()
    snapshot = store.candidate_freshness_snapshot(now_ms=100_000)
    controller = DiscoveryLoadController(candidate_hard_stale_ms=90_000)

    assert snapshot.missing_quote_count == 1
    assert controller.yield_reason(
        CandidateFreshness(
            candidate_count=snapshot.candidate_count,
            quote_p95_age_ms=snapshot.quote_p95_age_ms,
            missing_quote_count=snapshot.missing_quote_count,
        )
    ) == "candidate-quote-missing"
    assert controller.yield_reason(
        CandidateFreshness(candidate_count=0, quote_p95_age_ms=None)
    ) is None


@pytest.mark.asyncio
async def test_degraded_duty_cycle_persists_probe_phase_across_restart(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    gamma = FakeGamma(_page(_event(event_id="e-1", group_id="g-1")))

    def freshness() -> CandidateFreshness:
        return CandidateFreshness(
            candidate_count=1,
            quote_p95_age_ms=None,
            missing_quote_count=1,
        )

    first = DiscoveryWorker(
        gamma=gamma,
        store=store,
        load_controller=DiscoveryLoadController(candidate_hard_stale_ms=90_000),
        candidate_freshness=freshness,
        degraded_probe_every_cycles=3,
    )
    assert (await first.run_batch()).yielded is True
    assert (await first.run_batch()).yielded is True
    restarted = DiscoveryWorker(
        gamma=gamma,
        store=OpportunityPerceptionStore(tmp_path / "state.db"),
        load_controller=DiscoveryLoadController(candidate_hard_stale_ms=90_000),
        candidate_freshness=freshness,
        degraded_probe_every_cycles=3,
    )

    result = await restarted.run_batch()

    assert result.yielded is False
    assert gamma.calls == [("c-1", 100)]
    assert store.discovery_load_state().degraded_streak == 3
    assert store.discovery_load_state().last_decision == "probe"
    recovered = store.record_discovery_load_decision(
        degraded_reason=None,
        probe_every_cycles=3,
        now_ms=20_000,
    )
    assert recovered.degraded_streak == 0
    assert recovered.last_decision == "fresh"
    assert OpportunityPerceptionStore(
        tmp_path / "state.db"
    ).discovery_load_state() == recovered


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
async def test_overdue_factless_promotion_beats_repeatedly_new_higher_score(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    await DiscoveryWorker(
        gamma=FakeGamma(
            _page(
                _event(event_id="e-1", group_id="a-old", liquidity="1"),
                _event(event_id="e-2", group_id="z-new", liquidity="999"),
            )
        ),
        store=store,
    ).run_batch()
    with sqlite3.connect(tmp_path / "state.db") as con:
        con.execute(
            "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=0,"
            "priority_score='0' WHERE group_id='a-old'"
        )
        con.execute(
            "UPDATE neg_risk_group_schedule SET first_discovered_at_ms=999000,"
            "priority_score='999' WHERE group_id='z-new'"
        )
    calls: list[str] = []

    class Watcher:
        async def run_once(self, group_id: str, *, priority_hint: str) -> None:
            calls.append(group_id)

    def scheduler() -> CandidateWatcherScheduler:
        return CandidateWatcherScheduler(
            watcher=Watcher(),
            store=OpportunityPerceptionStore(tmp_path / "state.db"),
            candidate_group_ids=lambda: ("z-new", "a-old"),
            runtime=CandidateWatcherRuntime(),
            clock_ms=lambda: 1_000_000,
            cycle_max_groups=2,
            reserved_non_high_slots=1,
            discovery_candidate_max_wait_s=500,
        )

    await scheduler().run_due_once()
    await scheduler().run_due_once()

    assert calls == ["a-old", "z-new", "a-old", "z-new"]


def test_overdue_promotions_use_only_reserved_capacity_after_genuine_high(
    tmp_path: Path,
) -> None:
    store = _store(tmp_path)
    scheduler = CandidateWatcherScheduler(
        watcher=object(),
        store=store,
        candidate_group_ids=lambda: (),
        runtime=CandidateWatcherRuntime(),
        cycle_max_groups=5,
        reserved_non_high_slots=2,
    )
    due = [
        (0, 100, "hot-1"),
        (0, 101, "hot-2"),
        (0, 102, "hot-3"),
    ] + [
        (1, -(10**18) + index, f"overdue-{index}")
        for index in range(5)
    ]

    selected = scheduler._select_cycle(due)

    assert selected[0][2] == "hot-1"
    assert sum(item[2].startswith("overdue") for item in selected) == 2
    assert {item[2] for item in selected if item[2].startswith("hot")} == {
        "hot-1",
        "hot-2",
        "hot-3",
    }


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
