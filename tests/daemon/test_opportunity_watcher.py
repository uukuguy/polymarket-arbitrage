"""Global observer-only neg-risk opportunity reconciliation."""

from __future__ import annotations

import os
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
    assert "status=closed" in send_telegram.await_args_list[-1].args[1]
