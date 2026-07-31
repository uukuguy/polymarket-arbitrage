"""Durable lifecycle facts for observer-only neg-risk opportunities."""

from __future__ import annotations

import sqlite3
from dataclasses import replace

from polyarb.routing.focused_quote_collector import (
    ActiveOpportunity,
    FocusedObservation,
    StructureLeg,
)
from polyarb.routing.opportunity_ledger import OpportunityLedger
from polyarb.routing.opportunity_scanner import GroupAssessment, OpportunityLeg
from polyarb.storage.sqlite_store import SQLiteStore

NOW_MS = 1_800_000_000_000


def _observe_assessment() -> GroupAssessment:
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
            OpportunityLeg("market-a", "condition-a", "alpha", "token-a", 0.45, 50),
            OpportunityLeg("market-b", "condition-b", "beta", "token-b", 0.52, 42),
        ),
        structure_revision=17,
        quote_run_id=42,
        quoted_at_ms=NOW_MS,
    )


def test_first_crossing_persists_master_observation_and_notification(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)

    transition = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    assert transition.kind == "entered"
    assert transition.opportunity_id
    assert ledger.current_opportunities() == [
        {
            "id": transition.opportunity_id,
            "status": "observe",
            "event_id": "event-1",
            "group_id": "group-1",
            "membership_hash": "membership-1",
            "bundle_cost": 0.97,
            "gross_edge_bps": 300.0,
            "max_bundle_size": 42.0,
            "structure_revision": 17,
            "quote_run_id": 42,
        }
    ]
    pending = ledger.pending_notifications(now_ms=NOW_MS)
    assert len(pending) == 1
    assert pending[0].reason == "entered-gross-edge-threshold"
    assert pending[0].opportunity_id == transition.opportunity_id


def test_complete_below_threshold_observation_closes_existing_opportunity(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    no_edge = replace(
        _observe_assessment(),
        status="no-edge",
        bundle_cost=1.01,
        gross_edge_bps=-100.0,
        max_bundle_size=38.0,
        quote_run_id=43,
    )

    transition = ledger.reconcile_global(no_edge, observed_at_ms=NOW_MS + 120_000)

    assert transition == replace(opened, kind="closed")
    assert ledger.current_opportunities() == []
    pending = ledger.pending_notifications(now_ms=NOW_MS + 120_000)
    assert [item.reason for item in pending] == [
        "entered-gross-edge-threshold",
        "closed-gross-edge-threshold",
    ]


def test_material_edge_change_enqueues_one_follow_up_notification(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    changed = replace(
        _observe_assessment(),
        gross_edge_bps=325.0,
        bundle_cost=0.9675,
        quote_run_id=43,
    )

    transition = ledger.reconcile_global(changed, observed_at_ms=NOW_MS + 120_000)

    assert transition == replace(opened, kind="edge-changed")
    assert [item.reason for item in ledger.pending_notifications(now_ms=NOW_MS)] == [
        "entered-gross-edge-threshold",
        "edge-changed",
    ]


def test_notification_delivery_is_audited_without_changing_market_fact(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    notification = ledger.pending_notifications(now_ms=NOW_MS)[0]

    ledger.mark_notification_delivered(notification.id, delivered_at_ms=NOW_MS + 1)

    assert ledger.pending_notifications(now_ms=NOW_MS + 1) == ()
    assert ledger.current_opportunities()[0]["id"] == opened.opportunity_id


def test_notification_attempts_are_append_only_and_retry_state_is_derived(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    notification = ledger.pending_notifications(now_ms=NOW_MS)[0]

    ledger.mark_notification_failed(
        notification.id,
        attempted_at_ms=NOW_MS + 1,
        error_kind="TelegramUnavailableError",
    )
    assert ledger.pending_notifications(now_ms=NOW_MS + 1)[0].attempt_count == 1
    ledger.mark_notification_delivered(notification.id, delivered_at_ms=NOW_MS + 2)

    assert ledger.pending_notifications(now_ms=NOW_MS + 2) == ()
    assert ledger.current_opportunities()[0]["id"] == opened.opportunity_id
    attempts = ledger.notification_attempts(notification.id)
    assert [(item.outcome, item.error_kind) for item in attempts] == [
        ("failed", "TelegramUnavailableError"),
        ("delivered", None),
    ]


def test_permanent_telegram_http_failure_uses_bounded_retry_backoff(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    notification = ledger.pending_notifications(now_ms=NOW_MS)[0]

    ledger.mark_notification_failed(
        notification.id,
        attempted_at_ms=NOW_MS + 1,
        error_kind="HTTPStatusError",
    )

    assert ledger.pending_notifications(now_ms=NOW_MS + 5_000) == ()
    assert ledger.pending_notifications(now_ms=NOW_MS + 5_001)[0].attempt_count == 1


def test_active_masters_rebuild_the_original_all_leg_identity(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    masters = ledger.active_masters()

    assert masters == (
        ActiveOpportunity(
            id=opened.opportunity_id,
            event_id="event-1",
            group_id="group-1",
            membership_hash="membership-1",
            structure_revision=17,
            quote_run_id=42,
            legs=(
                StructureLeg("market-a", "condition-a", "alpha", "token-a"),
                StructureLeg("market-b", "condition-b", "beta", "token-b"),
            ),
        ),
    )


def test_record_focused_no_edge_closes_master_without_a_quote_run_write(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    ledger.record_focused(
        FocusedObservation(
            opportunity_id=opened.opportunity_id,
            status="no-edge",
            reason=None,
            bundle_cost=1.03,
            gross_edge_bps=-300.0,
            max_bundle_size=40.0,
            legs=(
                OpportunityLeg("market-a", "condition-a", "alpha", "token-a", 0.51, 50),
                OpportunityLeg("market-b", "condition-b", "beta", "token-b", 0.52, 40),
            ),
            structure_revision=18,
            quote_run_id=42,
            observed_at_ms=NOW_MS + 15_000,
        )
    )

    assert ledger.current_opportunities() == []
    with sqlite3.connect(db_path) as con:
        master = con.execute(
            "SELECT status,transition_reason,quote_run_id FROM neg_risk_opportunities WHERE id=?",
            (opened.opportunity_id,),
        ).fetchone()
        observation = con.execute(
            "SELECT source,status,reason,quote_run_id FROM neg_risk_opportunity_observations "
            "WHERE opportunity_id=? ORDER BY id DESC LIMIT 1",
            (opened.opportunity_id,),
        ).fetchone()
        quote_runs = con.execute("SELECT COUNT(*) FROM neg_risk_quote_runs").fetchone()[0]
    assert master == ("closed", "closed-gross-edge-threshold", 42)
    assert observation == ("focused", "closed", "closed-gross-edge-threshold", 42)
    assert quote_runs == 0


def test_record_focused_unavailable_preserves_a_retriable_master(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    ledger.record_focused(
        FocusedObservation(
            opportunity_id=opened.opportunity_id,
            status="unavailable",
            reason="incomplete-quotes",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            legs=(),
            structure_revision=18,
            quote_run_id=42,
            observed_at_ms=NOW_MS + 15_000,
        )
    )

    assert ledger.active_masters()[0].id == opened.opportunity_id
    with sqlite3.connect(db_path) as con:
        observation = con.execute(
            "SELECT source,status,reason FROM neg_risk_opportunity_observations "
            "WHERE opportunity_id=? ORDER BY id DESC LIMIT 1",
            (opened.opportunity_id,),
        ).fetchone()
    assert observation == ("focused", "unavailable", "incomplete-quotes")


def test_record_focused_membership_invalidation_closes_master(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)

    ledger.record_focused(
        FocusedObservation(
            opportunity_id=opened.opportunity_id,
            status="invalidated",
            reason="structure-membership-changed",
            bundle_cost=None,
            gross_edge_bps=None,
            max_bundle_size=None,
            legs=(),
            structure_revision=18,
            quote_run_id=42,
            observed_at_ms=NOW_MS + 15_000,
        )
    )

    assert ledger.active_masters() == ()
    with sqlite3.connect(db_path) as con:
        master = con.execute(
            "SELECT status,transition_reason FROM neg_risk_opportunities WHERE id=?",
            (opened.opportunity_id,),
        ).fetchone()
    assert master == ("invalidated", "structure-membership-changed")


def test_focused_records_keep_the_opening_global_quote_run_after_refresh(tmp_path) -> None:
    db_path = tmp_path / "state.db"
    SQLiteStore(db_path).init_schema()
    ledger = OpportunityLedger(db_path)
    opened = ledger.reconcile_global(_observe_assessment(), observed_at_ms=NOW_MS)
    refreshed = replace(
        _observe_assessment(),
        quote_run_id=43,
        bundle_cost=0.965,
        gross_edge_bps=350.0,
    )
    ledger.reconcile_global(refreshed, observed_at_ms=NOW_MS + 120_000)

    master = ledger.active_masters()[0]
    ledger.record_focused(
        FocusedObservation(
            opportunity_id=opened.opportunity_id,
            status="observe",
            reason=None,
            bundle_cost=0.96,
            gross_edge_bps=400.0,
            max_bundle_size=40.0,
            legs=(
                OpportunityLeg("market-a", "condition-a", "alpha", "token-a", 0.44, 50),
                OpportunityLeg("market-b", "condition-b", "beta", "token-b", 0.52, 40),
            ),
            structure_revision=18,
            quote_run_id=master.quote_run_id,
            observed_at_ms=NOW_MS + 135_000,
        )
    )

    assert master.quote_run_id == 42
    with sqlite3.connect(db_path) as con:
        current_run_id = con.execute(
            "SELECT quote_run_id FROM neg_risk_opportunities WHERE id=?",
            (opened.opportunity_id,),
        ).fetchone()[0]
        focused_run_id = con.execute(
            "SELECT quote_run_id FROM neg_risk_opportunity_observations "
            "WHERE opportunity_id=? AND source='focused' ORDER BY id DESC LIMIT 1",
            (opened.opportunity_id,),
        ).fetchone()[0]
    assert current_run_id == 43
    assert focused_run_id == 42
