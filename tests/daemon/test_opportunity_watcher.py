"""Global observer-only neg-risk opportunity reconciliation."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.perception.store import OpportunityPerceptionStore
from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    PersistedQuote,
    UniverseLeg,
    VerifiedQuoteUniverse,
)
from polyarb.routing.opportunity_ledger import OpportunityLedger
from polyarb.routing.opportunity_scanner import GroupAssessment, OpportunityLeg
from polyarb.storage.sqlite_store import SQLiteStore


def _projection(*, run_id: int = 1, first_ask: float = 0.45) -> CompleteQuoteProjection:
    now_ms = int(time.time() * 1000)
    legs = (
        UniverseLeg(
            "group-1", "market-1", "condition-1", "alpha", "token-1", "event-1", "membership-1"
        ),
        UniverseLeg(
            "group-1", "market-2", "condition-2", "beta", "token-2", "event-1", "membership-1"
        ),
    )
    return CompleteQuoteProjection(
        run_id=run_id,
        universe_snapshot_id=10,
        universe_taken_at_ms=now_ms,
        quoted_at_ms=now_ms,
        requested_token_count=len(legs),
        successful_response_count=len(legs),
        run_legs=legs,
        quotes=tuple(
            PersistedQuote(
                leg.neg_risk_market_id,
                leg.market_id,
                leg.condition_id,
                leg.slug,
                leg.yes_token_id,
                "executable",
                ask,
                size,
                leg.event_id,
                leg.membership_hash,
            )
            for leg, ask, size in (
                (legs[0], first_ask, 42.0),
                (legs[1], 0.52, 40.0),
            )
        ),
        source_universe=VerifiedQuoteUniverse(
            snapshot_id=10,
            taken_at_ms=now_ms,
            universe_hash="universe-1",
            legs=legs,
            rejections=(),
        ),
        universe_hash="universe-1",
        source_truth_hash="truth-1",
    )


def _observe_assessment_fixture() -> GroupAssessment:
    return GroupAssessment(
        group_id="group-1",
        event_id="event-1",
        membership_hash="membership-1",
        status="observe",
        reason=None,
        bundle_cost=0.97,
        gross_edge_bps=300.0,
        max_bundle_size=42.0,
        legs=(
            OpportunityLeg("market-1", "condition-1", "alpha", "token-1", 0.45, 42.0),
            OpportunityLeg("market-2", "condition-2", "beta", "token-2", 0.52, 40.0),
        ),
        structure_revision=10,
        quote_run_id=42,
        quoted_at_ms=int(time.time() * 1000),
    )


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        db_path=tmp_path / "state.db",
        parquet_root=tmp_path / "snapshots",
        cache_root=tmp_path / "cache",
    )


@pytest.fixture
def ledger(settings: Settings) -> OpportunityLedger:
    SQLiteStore(settings.db_path).init_schema()
    return OpportunityLedger(settings.db_path)


@pytest.fixture
def complete_projection() -> CompleteQuoteProjection:
    return _projection()


async def test_global_reconciliation_runs_only_after_certification(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    watcher = OpportunityWatcher.for_test(settings, ledger=ledger)

    await watcher.reconcile_global_projection(complete_projection)

    assert ledger.current_opportunities()[0]["status"] == "observe"


async def test_global_reconciliation_skips_no_edge_groups_without_active_master(
    settings: Settings,
    ledger: OpportunityLedger,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    reconcile = Mock(wraps=ledger.reconcile_global)
    ledger.reconcile_global = reconcile  # type: ignore[method-assign]

    await OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=AsyncMock(),
    ).reconcile_global_projection(_projection(first_ask=0.55))

    reconcile.assert_not_called()


async def test_telegram_failure_is_retryable_without_losing_observation(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=AsyncMock(side_effect=OSError("telegram unavailable")),
    )

    await watcher.reconcile_global_projection(complete_projection)

    assert ledger.current_opportunities()[0]["status"] == "observe"
    pending = ledger.pending_notifications(now_ms=int(time.time() * 1000))
    assert len(pending) == 1
    assert pending[0].attempt_count == 1
    incident = OpportunityPerceptionStore(settings.db_path).open_incidents()[0]
    assert incident.scope == f"notification:{pending[0].id}"
    assert incident.kind == "telegram-delivery-failed"
    assert incident.state == "recovering"


async def test_telegram_retry_closes_exact_notification_incident(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    send_telegram = AsyncMock(
        side_effect=[OSError("telegram unavailable"), None]
    )
    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=send_telegram,
    )

    await watcher.reconcile_global_projection(complete_projection)
    await watcher.deliver_pending_notifications()

    attempts = ledger.notification_attempts(1)
    assert [attempt.outcome for attempt in attempts] == [
        "failed",
        "delivered",
    ]
    assert OpportunityPerceptionStore(settings.db_path).open_incidents() == ()


async def test_delivery_batch_is_bounded_and_paced(
    settings: Settings,
    ledger: OpportunityLedger,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    now_ms = int(time.time() * 1000)
    ledger.reconcile_global(
        _observe_assessment_fixture(),
        observed_at_ms=now_ms,
    )
    ledger.reconcile_global(
        replace(_observe_assessment_fixture(), gross_edge_bps=325.0, quote_run_id=43),
        observed_at_ms=now_ms + 1,
    )
    ledger.reconcile_global(
        replace(
            _observe_assessment_fixture(),
            status="no-edge",
            bundle_cost=1.01,
            gross_edge_bps=-100.0,
            quote_run_id=44,
        ),
        observed_at_ms=now_ms + 2,
    )
    delays: list[float] = []

    async def delivery_sleep(delay_s: float) -> None:
        delays.append(delay_s)

    send_telegram = AsyncMock()
    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=send_telegram,
        clock_ms=lambda: now_ms + 2,
        notification_batch_limit=2,
        notification_min_interval_s=1.1,
        delivery_sleep=delivery_sleep,
    )

    await watcher.deliver_pending_notifications()

    assert send_telegram.await_count == 2
    assert delays == [1.1]
    assert len(ledger.pending_notifications(now_ms=now_ms + 2)) == 1


async def test_opportunity_alert_without_telegram_configuration_stays_retryable(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.alerts import TelegramUnavailableError, send_opportunity_alert
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    with pytest.raises(TelegramUnavailableError):
        await send_opportunity_alert(settings, "observer-only card")

    await OpportunityWatcher.for_test(settings, ledger=ledger).reconcile_global_projection(
        complete_projection
    )

    notification = ledger.pending_notifications(now_ms=int(time.time() * 1000))[0]
    assert notification.attempt_count == 1
    assert ledger.notification_attempts(notification.id)[0].error_kind == "TelegramUnavailableError"


async def test_stable_observation_sends_no_duplicate_card(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    send_telegram = AsyncMock()
    watcher = OpportunityWatcher.for_test(settings, ledger=ledger, send_telegram=send_telegram)

    await watcher.reconcile_global_projection(complete_projection)
    await watcher.reconcile_global_projection(complete_projection)

    assert send_telegram.await_count == 1


async def test_close_sends_one_card_with_observer_only_warning(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    send_telegram = AsyncMock()
    watcher = OpportunityWatcher.for_test(settings, ledger=ledger, send_telegram=send_telegram)

    await watcher.reconcile_global_projection(complete_projection)
    await watcher.reconcile_global_projection(
        replace(_projection(run_id=2, first_ask=0.55), source_truth_hash="truth-2")
    )

    assert send_telegram.await_count == 2
    for call in send_telegram.await_args_list:
        card = call.args[1]
        assert "execution_status=not-verified" in card
        assert "仅观察，未扣手续费、滑点和多腿成交风险" in card
        assert "event_id=event-1" in card
        assert "group_id=group-1" in card
        assert "membership_hash=membership-1" in card
        assert "structure_revision=10" in card
        assert "quote_run_id=" in card
        assert '"token_id":"token-1"' in card
        assert '"token_id":"token-2"' in card
    assert "status=closed" in send_telegram.await_args_list[-1].args[1]
    assert "unknown" not in send_telegram.await_args_list[-1].args[1]


def test_large_opportunity_card_fits_telegram_limit_without_losing_identity() -> None:
    from polyarb.daemon.opportunity_watcher import _format_card
    from polyarb.routing.opportunity_ledger import PendingNotification

    legs = [
        {
            "market_id": f"market-{index}-" + ("m" * 80),
            "condition_id": f"condition-{index}-" + ("c" * 80),
            "slug": f"slug-{index}-" + ("s" * 80),
            "token_id": f"token-{index}-" + ("t" * 80),
            "ask": 0.5,
            "ask_size": 10,
        }
        for index in range(40)
    ]
    card = _format_card(
        PendingNotification(
            id=1,
            opportunity_id="opportunity-1",
            reason="entered-gross-edge-threshold",
            payload={
                "status": "observe",
                "strategy": "neg-risk-buy-all",
                "event_id": "event-1",
                "group_id": "group-1",
                "membership_hash": "membership-exact",
                "legs": legs,
                "bundle_cost": 0.9,
                "gross_edge_bps": 1000,
                "max_bundle_size": 10,
                "structure_revision": 767,
                "quote_run_id": 1328,
                "quoted_at_ms": 1_800_000_000_000,
            },
            attempt_count=0,
        )
    )

    assert len(card) <= 4_000
    assert "membership_hash=membership-exact" in card
    assert "legs_count=40" in card
    assert "legs_truncated=" in card


async def test_focused_loop_persists_one_top_of_book_observation(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher
    from polyarb.routing.focused_quote_collector import StructureGroup, StructureLeg

    class Reader:
        requests: list[list[str]] = []
        projections: list[str] = []

        async def get_books(
            self,
            token_ids: list[str],
            *,
            projection: str = "full",
        ) -> list[object]:
            self.requests.append(token_ids)
            self.projections.append(projection)
            return [
                {"asset_id": "token-1", "asks": [{"price": "0.45", "size": "42"}]},
                {"asset_id": "token-2", "asks": [{"price": "0.52", "size": "40"}]},
            ]

    class MembershipReader:
        def current_group(self, event_id: str, group_id: str) -> StructureGroup:
            assert (event_id, group_id) == ("event-1", "group-1")
            return StructureGroup(
                structure_revision=11,
                event_id=event_id,
                group_id=group_id,
                membership_hash="membership-1",
                legs=(
                    StructureLeg("market-1", "condition-1", "alpha", "token-1"),
                    StructureLeg("market-2", "condition-2", "beta", "token-2"),
                ),
            )

    async def stop_after_one(_: object, __: float) -> bool:
        return True

    await OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=AsyncMock(),
        focused_reader=Reader(),
        membership_reader=MembershipReader(),
        wait_for_stop=stop_after_one,
    ).reconcile_global_projection(complete_projection)
    reader = Reader()
    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=AsyncMock(),
        focused_reader=reader,
        membership_reader=MembershipReader(),
        wait_for_stop=stop_after_one,
    )

    await watcher.run(asyncio.Event())

    assert reader.requests == [["token-1", "token-2"]]
    assert reader.projections == ["top"]
    with sqlite3.connect(settings.db_path) as con:
        observation = con.execute(
            "SELECT source,status,quote_run_id FROM neg_risk_opportunity_observations "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert observation == ("focused", "observe", 1)


async def test_focused_loop_empty_watchlist_makes_no_clob_request(
    settings: Settings,
    ledger: OpportunityLedger,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher

    reader = AsyncMock()

    async def stop_after_one(_: object, __: float) -> bool:
        return True

    watcher = OpportunityWatcher.for_test(
        settings,
        ledger=ledger,
        send_telegram=AsyncMock(),
        focused_reader=reader,
        membership_reader=object(),
        wait_for_stop=stop_after_one,
    )

    await watcher.run(asyncio.Event())

    reader.get_books.assert_not_awaited()


async def test_focused_loop_cancellation_preserves_committed_observation(
    settings: Settings,
    ledger: OpportunityLedger,
    complete_projection: CompleteQuoteProjection,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher
    from polyarb.routing.focused_quote_collector import StructureGroup, StructureLeg

    class Reader:
        async def get_books(self, _: list[str], *, projection: str = "full") -> list[object]:
            assert projection == "top"
            return [
                {"asset_id": "token-1", "asks": [{"price": "0.45", "size": "42"}]},
                {"asset_id": "token-2", "asks": [{"price": "0.52", "size": "40"}]},
            ]

    class MembershipReader:
        def current_group(self, event_id: str, group_id: str) -> StructureGroup:
            return StructureGroup(
                structure_revision=11,
                event_id=event_id,
                group_id=group_id,
                membership_hash="membership-1",
                legs=(
                    StructureLeg("market-1", "condition-1", "alpha", "token-1"),
                    StructureLeg("market-2", "condition-2", "beta", "token-2"),
                ),
            )

    wait_started = asyncio.Event()
    never = asyncio.Event()

    async def block_after_commit(_: object, __: float) -> bool:
        wait_started.set()
        await never.wait()
        return False

    await OpportunityWatcher.for_test(settings, ledger=ledger).reconcile_global_projection(
        complete_projection
    )
    task = asyncio.create_task(
        OpportunityWatcher.for_test(
            settings,
            ledger=ledger,
            send_telegram=AsyncMock(),
            focused_reader=Reader(),
            membership_reader=MembershipReader(),
            wait_for_stop=block_after_commit,
        ).run(asyncio.Event())
    )
    await asyncio.wait_for(wait_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    with sqlite3.connect(settings.db_path) as con:
        observation = con.execute(
            "SELECT source,status FROM neg_risk_opportunity_observations "
            "ORDER BY id DESC LIMIT 1"
        ).fetchone()
    assert observation == ("focused", "observe")


async def test_focused_loop_drops_stale_master_and_runs_the_next_poll(
    settings: Settings,
) -> None:
    from polyarb.daemon.opportunity_watcher import OpportunityWatcher
    from polyarb.routing.focused_quote_collector import (
        ActiveOpportunity,
        StructureGroup,
        StructureLeg,
    )
    from polyarb.routing.opportunity_ledger import FocusedObservationStaleError

    master = ActiveOpportunity(
        id="opportunity-1",
        event_id="event-1",
        group_id="group-1",
        membership_hash="membership-1",
        structure_revision=10,
        quote_run_id=42,
        legs=(
            StructureLeg("market-1", "condition-1", "alpha", "token-1"),
            StructureLeg("market-2", "condition-2", "beta", "token-2"),
        ),
    )

    class RacingLedger:
        def __init__(self) -> None:
            self.poll_count = 0
            self.record_count = 0
            self.committed: list[object] = []

        def active_masters(self) -> tuple[ActiveOpportunity, ...]:
            self.poll_count += 1
            return (master,) if self.poll_count <= 2 else ()

        def record_focused(self, observation: object) -> None:
            self.record_count += 1
            if self.record_count == 1:
                raise FocusedObservationStaleError()
            self.committed.append(observation)

        def pending_notifications(
            self,
            *,
            now_ms: int,
            limit: int = 100,
        ) -> tuple[object, ...]:
            return ()

    class Reader:
        async def get_books(self, _: list[str], *, projection: str = "full") -> list[object]:
            assert projection == "top"
            return [
                {"asset_id": "token-1", "asks": [{"price": "0.45", "size": "42"}]},
                {"asset_id": "token-2", "asks": [{"price": "0.52", "size": "40"}]},
            ]

    class MembershipReader:
        def current_group(self, event_id: str, group_id: str) -> StructureGroup:
            return StructureGroup(
                structure_revision=11,
                event_id=event_id,
                group_id=group_id,
                membership_hash="membership-1",
                legs=master.legs,
            )

    delays: list[float] = []

    async def wait_two_polls(_: object, delay_s: float) -> bool:
        delays.append(delay_s)
        return len(delays) == 2

    ledger = RacingLedger()
    await OpportunityWatcher.for_test(
        settings,
        ledger=ledger,  # type: ignore[arg-type]
        send_telegram=AsyncMock(),
        focused_reader=Reader(),
        membership_reader=MembershipReader(),
        wait_for_stop=wait_two_polls,
    ).run(asyncio.Event())

    assert ledger.poll_count == 2
    assert ledger.record_count == 2
    assert len(ledger.committed) == 1
    assert delays == [15.0, 15.0]
