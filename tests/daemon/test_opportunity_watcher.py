"""Global observer-only neg-risk opportunity reconciliation."""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from dataclasses import replace
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("POLYARB_ALLOW_EXTERNAL_PATHS", "1")
os.environ.setdefault("POLYARB_ALLOW_EMPTY_SECRET", "1")

from polyarb.config import Settings
from polyarb.routing.neg_risk_quote_store import (
    CompleteQuoteProjection,
    PersistedQuote,
    UniverseLeg,
    VerifiedQuoteUniverse,
)
from polyarb.routing.opportunity_ledger import OpportunityLedger
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
